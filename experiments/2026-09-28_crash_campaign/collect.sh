#!/usr/bin/env bash
# collect.sh — after the campaign: copy raw output into logs/, build figures, and write
# RESULTS_AUTO.md (per-run counters pulled from the logs). Run on the controller as root.
#   --wait   block until run_all_scenarios.sh has finished first
set -uo pipefail
here=$(cd "$(dirname "$0")" && pwd)
CAMP=/tmp/crash_inject/campaign
RUNS="campaign_heal crash_s1 crash_s2 crash_s3 crash_s4 crash_s5"
say(){ echo "[collect $(date -u +%H:%M:%S)] $*"; }

if [ "${1:-}" = "--wait" ]; then
  say "waiting for the campaign to finish"
  while pgrep -f run_all_scenarios.sh >/dev/null; do sleep 60; done
  sleep 30
fi

# ---- 1. raw output -> logs/ ------------------------------------------------
say "copying $CAMP -> logs/campaign"
mkdir -p "$here/logs/campaign/inject"
cp -a "$CAMP"/. "$here/logs/campaign/"
cp -a /tmp/crash_inject/*.txt /tmp/crash_inject/*.snap "$here/logs/campaign/inject/" 2>/dev/null
for r in $RUNS; do
  if [ -d "/tmp/experiments/$r" ]; then
    say "copying /tmp/experiments/$r -> logs/$r"
    rm -rf "$here/logs/$r"; cp -a "/tmp/experiments/$r" "$here/logs/$r"
  else
    say "missing /tmp/experiments/$r"
  fi
done

# The recipient-leader log on redis3 is wiped at end of run (and redis3 is the kill target in
# S1). The Gantt reads logs/redis3/.../redis3_sg4.log, so fill it from the campaign copy (which
# prefers the injector's snapshot) when the collected one is empty or missing.
for r in $RUNS; do
  lab=$(echo "${r#crash_}" | tr a-z A-Z); [ "$r" = campaign_heal ] && lab=HEALTHY
  dst="$here/logs/$r/logs/redis3/tmp/redis_logs/redis3_sg4.log"
  src="$here/logs/campaign/$lab/redis3_sg4.log"
  # The injector snapshots the ARM host's log, and run_all_scenarios.sh saves it as
  # redis3_sg4.log whatever it is (S2/S3/S5 arm on redis0_sg1). Only use it if it is
  # really a recipient log.
  if [ -d "$here/logs/$r" ] && [ ! -s "$dst" ] && [ -s "$src" ] && grep -aq "RECP_TXN_START logged" "$src"; then
    mkdir -p "$(dirname "$dst")"; cp "$src" "$dst"; say "$r: redis3_sg4.log filled from campaign copy"
  fi
  # sg4 followers are not in the base collection; add them for completeness
  for h in redis4 redis5; do
    f="$here/logs/campaign/$lab/${h}_sg4.log"; d="$here/logs/$r/logs/$h/tmp/redis_logs"
    [ -d "$here/logs/$r" ] && [ -s "$f" ] && [ ! -s "$d/${h}_sg4.log" ] && { mkdir -p "$d"; cp "$f" "$d/"; }
  done
done

# Full per-host logs saved by grab_logs.sh (all sgs on every host). Use them for any host whose
# logs the base collection did not keep (a failed playbook skips it; donors sg2/sg3 are never
# kept by the campaign script).
for r in $RUNS; do
  lab=$(echo "${r#crash_}" | tr a-z A-Z); [ "$r" = campaign_heal ] && lab=HEALTHY
  full="$here/logs/campaign/$lab/full"; [ -d "$full" ] || continue
  for hd in "$full"/*/; do
    h=$(basename "$hd"); d="$here/logs/$r/logs/$h/tmp/redis_logs"
    for f in "$hd"*.log; do
      [ -s "$f" ] || continue
      [ -s "$d/$(basename "$f")" ] && continue
      mkdir -p "$d"; cp "$f" "$d/"; say "$r: added $h/$(basename "$f") from full logs"
    done
  done
done

# A failed playbook skips the base collection entirely (S1): take YCSB output and the sg1 donor
# logs from the campaign copy so the throughput figures and a partial Gantt can still be drawn.
for r in $RUNS; do
  lab=$(echo "${r#crash_}" | tr a-z A-Z); [ "$r" = campaign_heal ] && lab=HEALTHY
  c="$here/logs/campaign/$lab"; [ -d "$here/logs/$r" ] || continue
  for y in ycsb0 ycsb1; do
    dst="$here/logs/$r/ycsb/$y/tmp/ycsb_output_$y"
    if [ ! -s "$dst" ] && [ -s "$c/ycsb_$y.txt" ]; then
      mkdir -p "$(dirname "$dst")"; cp "$c/ycsb_$y.txt" "$dst"; say "$r: ycsb $y from campaign copy"
    fi
  done
  for h in redis0 redis1 redis2; do
    f="$c/${h}_sg1.log"; d="$here/logs/$r/logs/$h/tmp/redis_logs"
    if [ -s "$f" ] && [ ! -s "$d/${h}_sg1.log" ]; then mkdir -p "$d"; cp "$f" "$d/"; say "$r: $h sg1 log from campaign copy"; fi
  done
done

# ---- 2. figures --------------------------------------------------------------
say "building figures"
"$here/figures.sh" > "$here/logs/figures.log" 2>&1 || say "figures.sh rc=$? (see logs/figures.log)"

# ---- 3. counters -> RESULTS_AUTO.md ----------------------------------------
cnt(){ grep -ahcE "$1" "${@:2}" 2>/dev/null | awk '{s+=$1} END{print s+0}'; }
out="$here/RESULTS_AUTO.md"
{
  echo "# Crash campaign — auto-collected results"
  echo
  echo "Generated $(date -u '+%Y-%m-%d %H:%M UTC') by \`collect.sh\` from \`logs/\`. Counts are grep hits"
  echo "summed over the recipient (sg4) and sg1 logs of each run."
  echo
  echo "| run | reshard rc | kill | crash sigs | leader elections (incl. startup) | faked INDX_UPD | chain-ack observed | INDX_UPD applied | RE-FORM | resume-plan | RESUME-STATUS | recipient DBSIZE |"
  echo "|---|---|---|---|---|---|---|---|---|---|---|---|"
  for r in $RUNS; do
    lab=$(echo "${r#crash_}" | tr a-z A-Z); [ "$r" = campaign_heal ] && lab=HEALTHY
    d="$here/logs/campaign/$lab"; [ -d "$d" ] || { echo "| $lab | not run | | | | | | | | | | |"; continue; }
    # full collected logs (every sg on every host); the campaign copies of redis0-3 are empty
    # because those logs are wiped at end of run
    logs=$(ls "$here/logs/$r"/logs/*/tmp/redis_logs/*.log 2>/dev/null)
    [ -z "$logs" ] && logs=$(ls "$d"/*_sg4.log "$d"/*_sg1.log 2>/dev/null)
    rc=$(grep -aoE 'rc=[0-9]+' "$d/run.log" 2>/dev/null | tail -1); [ -z "$rc" ] && rc="see run.log"
    grep -aq 'failed=[1-9]' "$d/run.log" 2>/dev/null && rc="$rc, play failed"
    kill=$(grep -ahoE 'result=[A-Z_]+' "$here"/logs/campaign/inject/${lab}-*.txt 2>/dev/null | tail -1); [ -z "$kill" ] && kill="—"
    db=$(grep -aoE 'DBSIZE[^0-9]{0,20}[0-9,]{4,}' "$d/run.log" 2>/dev/null | tail -1 | grep -oE '[0-9,]{4,}$'); [ -z "$db" ] && db="?"
    echo "| $lab | $rc | ${kill#result=} | $(cnt 'ASSERTION FAILED|REDIS BUG|Crashed by signal|SIGSEGV' $logs) | $(cnt 'Node is now a leader' $logs) | $(cnt 'firing MGN_INDX_UPD (anyway|immediately)' $logs) | $(cnt 'chain-ack observed' $logs) | $(cnt 'MGN_INDX_UPD applied' $logs) | $(cnt 'RE-FORM' $logs) | $(cnt 'S2 resume-plan' $logs) | $(cnt 'AqRaft MGN-RESUME-STATUS' $logs) | $db |"
  done
  echo
  if [ -f "$here/figures/metrics.json" ]; then
    echo "## metrics.json (analyze_crash.py)"; echo; echo '```json'; cat "$here/figures/metrics.json"; echo '```'; echo
  fi
  echo "## Verdicts (tail of each run log)"
  for r in $RUNS; do
    lab=$(echo "${r#crash_}" | tr a-z A-Z); [ "$r" = campaign_heal ] && lab=HEALTHY
    f="$here/logs/campaign/$lab/run.log"; [ -s "$f" ] || continue
    echo; echo "### $lab"; echo; echo '```'; tail -40 "$f"; echo '```'
  done
  echo; echo "## Figures"; echo
  ls "$here/figures" | sed 's/^/- /'
  echo; echo "## Resume / recovery lines"
  for r in $RUNS; do
    lab=$(echo "${r#crash_}" | tr a-z A-Z); [ "$r" = campaign_heal ] && lab=HEALTHY
    d="$here/logs/campaign/$lab"; [ -d "$d" ] || continue
    l=$(grep -ahE 'resume-plan|MGN-RESUME-STATUS:|become-leader|DONOR-REHOME|RE-FORM|STALL-ABORT|fail loud' $(ls "$here/logs/$r"/logs/*/tmp/redis_logs/*.log "$d"/*_sg4.log 2>/dev/null) 2>/dev/null | head -25)
    [ -n "$l" ] && { echo; echo "### $lab"; echo; echo '```'; echo "$l" | cut -c1-260; echo '```'; }
  done
} > "$out"
chown -R entall "$here" 2>/dev/null
say "done -> $out"
