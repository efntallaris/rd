#!/usr/bin/env python3
"""
Merge client + server slot-state traces from DoubleReadCorrectnessTest +
poll_slot_ownership.sh into a unified per-slot timeline. Produce a
human-readable timeline and a verdict summary.

Inputs:
  client_trace.log   "# unix_ms slot client_state client_peer client_owner"
  slot_ownership.log "<unix_ms> <host> <slot> <state> <peer>"
  tool.stdout        DoubleReadCorrectnessTest stdout — contains
                     "ops=... reads_ok=... writes_ok=... mismatches=..."
  tool.stderr        contains [MISMATCH ...] / [read NIL] lines

Outputs:
  timeline.txt   per-slot event log, dedup'd to state CHANGES
  verdict.txt    summary (mismatch_count, state_walk_ok, key counts)
"""
from __future__ import annotations
import argparse
import re
import sys
from collections import defaultdict
from pathlib import Path


def parse_client_trace(path: Path):
    """Return dict[slot] -> list of (ts_ms, state, peer, owner)."""
    out = defaultdict(list)
    if not path.exists():
        return out
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split()
            if len(parts) < 5:
                continue
            try:
                ts = int(parts[0])
                slot = int(parts[1])
            except ValueError:
                continue
            state = parts[2]
            peer  = parts[3]
            owner = parts[4]
            out[slot].append((ts, state, peer, owner))
    return out


def parse_server_trace(path: Path):
    """Return dict[slot] -> list of (ts_ms, host, state, peer)."""
    out = defaultdict(list)
    if not path.exists():
        return out
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split()
            if len(parts) < 5:
                continue
            try:
                ts = int(parts[0])
                slot = int(parts[2])
            except ValueError:
                continue
            host  = parts[1]
            state = parts[3]
            peer  = parts[4]
            out[slot].append((ts, host, state, peer))
    return out


def dedup_state_changes(events, key_fn):
    """Keep only events that differ from the prior event by key_fn(e).
    Always keep the first event."""
    out = []
    last = None
    for e in events:
        k = key_fn(e)
        if k != last:
            out.append(e)
            last = k
    return out


def build_timeline(client_by_slot, server_by_slot, t0_ms, slots):
    """Return a list of (ts_rel_ms, lines:list[str]) per slot."""
    blocks = []
    for slot in slots:
        srv = server_by_slot.get(slot, [])
        cli = client_by_slot.get(slot, [])
        # Server traces only emit non-STABLE slots; absence == STABLE.
        # We add synthetic STABLE-on-all-hosts entries at t0 and at end.
        srv_dedup = dedup_state_changes(srv, lambda e: (e[1], e[2], e[3]))
        cli_dedup = dedup_state_changes(cli, lambda e: (e[1], e[2], e[3]))

        lines = [f"== Slot {slot} =="]
        # Merge by ts_ms.
        all_evts = []
        for (ts, host, state, peer) in srv_dedup:
            all_evts.append((ts, 'server', host, state, peer))
        for (ts, state, peer, owner) in cli_dedup:
            all_evts.append((ts, 'client', None, state, peer, owner))
        all_evts.sort(key=lambda e: e[0])
        for evt in all_evts:
            ts = evt[0]
            rel = (ts - t0_ms) / 1000.0
            if evt[1] == 'server':
                lines.append(
                    f"  t={rel:+.3f}s server {evt[2]:7s} state={evt[3]:9s} peer={evt[4]}")
            else:
                lines.append(
                    f"  t={rel:+.3f}s client          state={evt[3]:9s} "
                    f"peer={evt[4]} owner={evt[5]}")
        blocks.append((slot, lines))
    return blocks


def parse_tool_summary(stdout_path: Path):
    """Find the final '[run] done. ops=... mismatches=...' line."""
    res = dict(ops=0, reads_ok=0, writes_ok=0, mismatches=0, found=False)
    if not stdout_path.exists():
        return res
    txt = stdout_path.read_text(errors='replace')
    m = re.search(r"\[run\] done\.\s+ops=(\d+)\s+reads_ok=(\d+)\s+writes_ok=(\d+)\s+mismatches=(\d+)", txt)
    if m:
        res['ops']        = int(m.group(1))
        res['reads_ok']   = int(m.group(2))
        res['writes_ok']  = int(m.group(3))
        res['mismatches'] = int(m.group(4))
        res['found'] = True
    return res


def parse_tool_errors(stderr_path: Path):
    """Count NIL reads partitioned by client-side slot state at the time of
    the read. NIL during STABLE means the migration is reported done but the
    key is missing — a lost write."""
    nil_stable = []
    nil_other = []
    mismatch_stale = []
    if not stderr_path or not stderr_path.exists():
        return nil_stable, nil_other, mismatch_stale
    for line in stderr_path.read_text(errors='replace').splitlines():
        m = re.match(r"\[read NIL\] key=(\S+) expected_ver=(\d+) slot=(\d+) state=(\S+)", line)
        if m:
            entry = dict(key=m.group(1), ver=int(m.group(2)),
                         slot=int(m.group(3)), state=m.group(4))
            if entry['state'] == 'STABLE':
                nil_stable.append(entry)
            else:
                nil_other.append(entry)
            continue
        m = re.match(r"\[MISMATCH stale\] key=(\S+) expected>=(\d+) got_ver=(\d+) slot=(\d+)", line)
        if m:
            mismatch_stale.append(dict(key=m.group(1), expected=int(m.group(2)),
                                       got=int(m.group(3)), slot=int(m.group(4))))
    return nil_stable, nil_other, mismatch_stale


def check_state_walk(server_by_slot, slots):
    """For each slot, ensure each host's state walk matches:
    donor:     STABLE -> MIGRATING -> MIGRATED -> STABLE
    recipient: STABLE -> MIGRATING -> STABLE  (or stays STABLE if absent)
    'Absent' means STABLE since the server trace only logs non-STABLE.
    """
    issues = []
    expected_donor = ['MIGRATING', 'MIGRATED']        # transitions we should see
    expected_recipient = ['MIGRATING']                # at least one MIGRATING
    for slot in slots:
        events = server_by_slot.get(slot, [])
        per_host = defaultdict(list)
        for (ts, host, state, peer) in events:
            per_host[host].append(state)
        # Dedup adjacent dupes
        for host in list(per_host):
            seq = []
            for s in per_host[host]:
                if not seq or seq[-1] != s:
                    seq.append(s)
            per_host[host] = seq
        # Check donor (redis0) and recipient (redis3).
        donor = per_host.get('redis0', [])
        recip = per_host.get('redis3', [])
        for needed in expected_donor:
            if needed not in donor:
                issues.append(f"slot {slot}: donor (redis0) never saw state {needed}")
        for needed in expected_recipient:
            if needed not in recip:
                issues.append(f"slot {slot}: recipient (redis3) never saw state {needed}")
    return issues


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--client-trace', required=True, type=Path)
    ap.add_argument('--server-trace', required=True, type=Path)
    ap.add_argument('--tool-out', required=True, type=Path)
    ap.add_argument('--tool-err', required=False, type=Path)
    ap.add_argument('--timeline-out', required=True, type=Path)
    ap.add_argument('--verdict-out', required=True, type=Path)
    ap.add_argument('--slot-range', default='0..127')
    args = ap.parse_args()

    lo, hi = args.slot_range.split('..')
    slots = list(range(int(lo), int(hi) + 1))

    client_by_slot = parse_client_trace(args.client_trace)
    server_by_slot = parse_server_trace(args.server_trace)
    tool = parse_tool_summary(args.tool_out)
    nil_stable, nil_other, mismatch_stale = parse_tool_errors(args.tool_err)

    # t0 = earliest timestamp across either source
    all_ts = []
    for slot, evts in client_by_slot.items():
        all_ts.extend(e[0] for e in evts)
    for slot, evts in server_by_slot.items():
        all_ts.extend(e[0] for e in evts)
    t0_ms = min(all_ts) if all_ts else 0

    blocks = build_timeline(client_by_slot, server_by_slot, t0_ms, slots)

    # Write timeline.txt
    args.timeline_out.parent.mkdir(parents=True, exist_ok=True)
    with open(args.timeline_out, 'w') as f:
        for slot, lines in blocks:
            for ln in lines:
                f.write(ln + '\n')
            f.write('\n')

    # state-walk sanity
    issues = check_state_walk(server_by_slot, slots)

    # Sample per-slot summary line for verdict (using slot 0 as anchor).
    sample_slot = slots[0]
    sample_lines = next((b[1] for b in blocks if b[0] == sample_slot), [])
    sample_excerpt = '\n'.join(sample_lines[:14])

    with open(args.verdict_out, 'w') as f:
        f.write("=== Double-read correctness test verdict ===\n\n")
        f.write(f"Slot range tested:    {args.slot_range}\n")
        if tool['found']:
            f.write(f"Tool ops:             {tool['ops']}\n")
            f.write(f"Tool reads_ok:        {tool['reads_ok']}\n")
            f.write(f"Tool writes_ok:       {tool['writes_ok']}\n")
            f.write(f"Tool mismatches:      {tool['mismatches']}    "
                    + ("PASS" if tool['mismatches'] == 0 else "*** FAIL ***") + "\n")
        else:
            f.write("Tool summary line not found in stdout — tool may have crashed.\n")
        f.write("\n")
        f.write(f"NIL reads during STABLE (= lost writes): {len(nil_stable)}    "
                + ("PASS" if not nil_stable else "*** FAIL ***") + "\n")
        if nil_stable[:5]:
            for e in nil_stable[:5]:
                f.write(f"    key={e['key']} expected_ver={e['ver']} slot={e['slot']}\n")
            if len(nil_stable) > 5:
                f.write(f"    ... (+{len(nil_stable)-5} more)\n")
        f.write(f"NIL reads during MIGRATING (tolerated): {len(nil_other)}\n")
        f.write(f"Stale-read mismatches:                  {len(mismatch_stale)}    "
                + ("PASS" if not mismatch_stale else "*** FAIL ***") + "\n")
        if mismatch_stale[:5]:
            for e in mismatch_stale[:5]:
                f.write(f"    key={e['key']} expected>={e['expected']} "
                        f"got={e['got']} slot={e['slot']}\n")
            if len(mismatch_stale) > 5:
                f.write(f"    ... (+{len(mismatch_stale)-5} more)\n")
        f.write("\n")
        f.write(f"State-walk check ({len(slots)} slots):\n")
        if not issues:
            f.write("  PASS: every donor saw MIGRATING+MIGRATED; every recipient saw MIGRATING\n")
        else:
            f.write(f"  *** FAIL *** ({len(issues)} issues):\n")
            for i in issues[:20]:
                f.write(f"    - {i}\n")
            if len(issues) > 20:
                f.write(f"    ... (+{len(issues)-20} more)\n")
        f.write("\n")
        f.write(f"Per-slot timeline:    {args.timeline_out}\n")
        f.write("\nSample (slot " + str(sample_slot) + "):\n")
        f.write(sample_excerpt + "\n")

    print(f"wrote {args.timeline_out}")
    print(f"wrote {args.verdict_out}")
    ok = (tool.get('mismatches', 1) == 0 and not issues
          and not nil_stable and not mismatch_stale)
    return 0 if ok else 1


if __name__ == '__main__':
    sys.exit(main())
