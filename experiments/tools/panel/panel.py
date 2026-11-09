#!/usr/bin/env python3
"""Experiment panel: launch, queue, stop and watch the lincheck / crash-suite runs from a browser.

Run ON THE CONTROLLER (redis0), as your user (the runners call sudo themselves):

    python3 experiments/tools/panel/panel.py            # http://127.0.0.1:8765
    python3 experiments/tools/panel/panel.py --port 9000 --host 0.0.0.0

From your laptop: VS Code forwards the port by itself, or `ssh -L 8765:localhost:8765 redis0`.

Standard library only. What it does:
  * runners = the scripts of the runbooks: ansible/experiments/custom_scaleout_3to6/run_all.sh
    (3 -> 6), ansible/lincheck/run_all_lin.sh (3 -> 4) and every experiments/*/run.sh; the scenario
    lists come from the scenarios.env files and the SUITE=(...) of each run.sh;
  * a launch is `setsid <runner> <scenarios>` with OUT_ROOT and the chosen env, detached, so it keeps
    going if the panel stops; the runner's stdout goes to $OUT_ROOT/panel_driver.log and the
    parameters to $OUT_ROOT/panel_job.json (the "rerun" buttons read it back);
  * a queue: the next job starts once the cluster is free (no /tmp/cluster_in_use, no runner,
    ansible-playbook or crash_inject process, launched from here or from a terminal);
  * live view over server-sent events: run.log / playbook.log of the scenario in progress, the
    summary lines, the playbook task, crash_inject events, YCSB throughput and read latency of
    ycsb0/ycsb1 (tail -F of /tmp/ycsb_output_<host> over ssh), redis-server processes per host;
  * history: every $OUT_ROOT under /tmp/lincheck with its summary, the files of each run, rerun.
State (jobs, queue, parameters of runs started elsewhere): ~/.experiment_panel/.
"""
import argparse, calendar, collections, glob, json, os, re, shlex, signal, subprocess, sys, threading, time
import queue as queuemod
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.environ.get("PANEL_REPO") or os.path.abspath(os.path.join(HERE, "..", "..", ".."))
LIN_ROOT = "/tmp/lincheck"
STATE = os.path.expanduser("~/.experiment_panel")
LOCK = "/tmp/cluster_in_use"
REDIS_HOSTS = ["redis0", "redis1", "redis2", "redis3", "redis4", "redis5"]
YCSB_HOSTS = ["ycsb0", "ycsb1"]
SSH = ["sudo", "-n", "ssh", "-o", "StrictHostKeyChecking=accept-new", "-o", "ConnectTimeout=5",
       "-o", "BatchMode=yes", "-o", "ControlMaster=auto", "-o", "ControlPath=/tmp/.panel_ssh_%h",
       "-o", "ControlPersist=120", "-o", "ServerAliveInterval=5"]
READ_ROOTS = [LIN_ROOT, "/tmp/experiments", os.path.join(REPO, "experiments")]
BUSY_RE = re.compile(r"(^|/)(ansible-playbook|crash_inject\.sh|fault_steps\.sh|run_all_lin\.sh|"
                     r"run_lin_scenario\.sh|run_crash_scenario\.sh|scaleout_pairs\.py)$")
# Env variables shown in the form (others go in the free "more env" box).
ENV_FIELDS = [
    ("ROUNDS", "rounds per pair"), ("SCALEOUT_MODE", "pairwise / sequential / pipelined / parallel"),
    ("CHUNK_SLOTS", "slots per announced chunk"), ("CHUNK_ARM_COUNT", "S1/S6 arm count"),
    ("ELECTION_TIMEOUT_MS", "Raft election timeout"), ("PROFILE", "quick (300k) / full (snapshot)"),
    ("DATASET_SNAPSHOT", "PROFILE=full only"), ("WORKLOAD", "YCSB workload file"),
    ("YCSB_THREADS", "threads per client"), ("MERGE_THREADS", "rdma_backpatch_pool_size"),
    ("REPS", "repetitions of the list"), ("QUORUM_READS_LIST", "yes / no / 'yes no'"),
    ("SCALEOUT_EXTRA_ARGS", "more -e flags (3->6)"), ("EXTRA_ANSIBLE_ARGS", "replaces defaults (3->4)"),
]
ENV_NAMES = {n for n, _ in ENV_FIELDS}


def now():
    return time.time()


def read(path, default=""):
    try:
        with open(path, errors="replace") as f:
            return f.read()
    except OSError:
        return default


def tail_lines(path, n=400, max_bytes=2_000_000):
    try:
        with open(path, "rb") as f:
            f.seek(0, 2)
            size = f.tell()
            f.seek(max(0, size - max_bytes))
            data = f.read().decode(errors="replace")
    except OSError:
        return []
    lines = data.splitlines()
    if size > max_bytes and lines:
        lines = lines[1:]
    return lines[-n:]


def write_json(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(obj, f, indent=1)
    os.replace(tmp, path)


def allowed_path(p):
    p = os.path.realpath(p)
    return any(p == r or p.startswith(r + "/") for r in map(os.path.realpath, READ_ROOTS))


# --------------------------------------------------------------------------- runners / scenarios
SCEN_CASE = re.compile(r"^\s{0,4}([A-Z][A-Z0-9_]*)\)\s*(?:#\s*(.*))?$")


def parse_scenarios(env_file):
    out, cur = [], None
    for line in read(env_file).splitlines():
        m = SCEN_CASE.match(line)
        if m:
            cur = {"id": m.group(1), "desc": (m.group(2) or "").strip(), "label": ""}
            out.append(cur)
            continue
        if cur is not None:
            lm = re.search(r'LABEL="([^"]+)"', line)
            if lm and not cur["label"]:
                cur["label"] = lm.group(1)
            elif not cur["label"] and re.match(r"^\s+#\s", line):
                cur["desc"] += " " + line.strip().lstrip("#").strip()
    seen, res = set(), [{"id": "HEALTHY", "desc": "no fault", "label": "HEALTHY"}]
    for s in out:
        if s["id"] not in seen and s["id"] != "HEALTHY":
            seen.add(s["id"])
            res.append(s)
    return res


def parse_defaults(script):
    """`export X="${X:-default}"` and `X="${X:-default}"` lines -> {X: default}."""
    d = {}
    for m in re.finditer(r'\$\{([A-Z_][A-Z0-9_]*):-([^}]*)\}', read(script)):
        d.setdefault(m.group(1), m.group(2))
    return d


def runners():
    s36 = os.path.join(REPO, "ansible/experiments/custom_scaleout_3to6")
    env36 = os.path.join(s36, "scenarios.env")
    env34 = os.path.join(REPO, "ansible/crash/scenarios.env")
    scen36, scen34 = parse_scenarios(env36), parse_scenarios(env34)
    d36 = parse_defaults(os.path.join(s36, "env.sh"))
    d36.update({k: v for k, v in parse_defaults(os.path.join(REPO, "ansible/lincheck/run_lin_scenario.sh")).items()
                if k not in d36})
    d34 = parse_defaults(os.path.join(REPO, "ansible/lincheck/run_lin_scenario.sh"))
    res = [
        {"id": "3to6", "name": "3 -> 6 scale-out (run_all.sh, raw run, no data/ copy)",
         "script": os.path.join(s36, "run_all.sh"), "scenarios": scen36, "suite": ["HEALTHY", "S1", "S2", "S5", "S9", "S10", "S11", "S12"],
         "defaults": d36, "copies_data": False},
        {"id": "3to4", "name": "3 -> 4 single recipient (run_all_lin.sh)",
         "script": os.path.join(REPO, "ansible/lincheck/run_all_lin.sh"), "scenarios": scen34,
         "suite": ["HEALTHY", "S1", "S2", "S3", "S4", "S5"], "defaults": d34, "copies_data": False},
    ]
    exps = []
    for rs in glob.glob(os.path.join(REPO, "experiments/*/run.sh")):
        txt = read(rs)
        d = os.path.basename(os.path.dirname(rs))
        is36 = "custom_scaleout_3to6" in txt
        if not is36 and "run_all_lin" not in txt:
            continue
        m = re.search(r"SUITE=\(([^)]*)\)", txt)
        suite = m.group(1).split() if m else []
        defs = dict(d36 if is36 else d34)
        defs.update(parse_defaults(rs))
        exps.append({"id": "exp:" + d, "name": d + "/run.sh (copies into data/, draws figures)",
                     "script": rs, "scenarios": scen36 if is36 else scen34, "suite": suite,
                     "defaults": defs, "copies_data": True, "dir": os.path.dirname(rs)})
    exps.sort(key=lambda r: r["id"], reverse=True)
    return res + exps


def runner_by_id(rid):
    for r in runners():
        if r["id"] == rid:
            return r
    return None


# --------------------------------------------------------------------------- processes
def proc_table():
    """pid -> (ppid, argv list, uid)."""
    tab = {}
    for p in os.listdir("/proc"):
        if not p.isdigit():
            continue
        try:
            with open(f"/proc/{p}/stat") as f:
                st = f.read()
            ppid = int(st[st.rindex(")") + 2:].split()[1])
            with open(f"/proc/{p}/cmdline", "rb") as f:
                argv = [a.decode(errors="replace") for a in f.read().split(b"\0") if a]
            tab[int(p)] = (ppid, argv)
        except (OSError, ValueError):
            pass
    return tab


def descendants(pid, tab=None):
    tab = tab or proc_table()
    kids = collections.defaultdict(list)
    for p, (pp, _) in tab.items():
        kids[pp].append(p)
    out, stack = [], [pid]
    while stack:
        p = stack.pop()
        for c in kids.get(p, []):
            out.append(c)
            stack.append(c)
    return out


def alive(pid):
    try:
        with open(f"/proc/{pid}/stat") as f:
            return f.read().split(")")[-1].split()[0] != "Z"
    except OSError:
        return False


def proc_env(pid):
    try:
        with open(f"/proc/{pid}/environ", "rb") as f:
            return dict(kv.decode(errors="replace").split("=", 1) for kv in f.read().split(b"\0") if b"=" in kv)
    except OSError:
        return {}


def busy_processes(tab=None):
    tab = tab or proc_table()
    res = []
    for p, (_, argv) in tab.items():
        if any(BUSY_RE.search(a) for a in argv[:3]):
            res.append((p, argv))
    return res


# --------------------------------------------------------------------------- jobs
class Jobs:
    def __init__(self):
        self.lock = threading.RLock()
        self.jobs = {}
        self.queue = []
        self.popen = {}
        self.hold = None
        os.makedirs(os.path.join(STATE, "jobs"), exist_ok=True)
        os.makedirs(os.path.join(STATE, "external"), exist_ok=True)
        for f in glob.glob(os.path.join(STATE, "jobs", "*.json")):
            try:
                j = json.load(open(f))
                self.jobs[j["id"]] = j
            except (OSError, ValueError, KeyError):
                pass
        try:
            self.queue = json.load(open(os.path.join(STATE, "queue.json")))
        except (OSError, ValueError):
            self.queue = []
        self.refresh()

    def save(self, j):
        write_json(os.path.join(STATE, "jobs", j["id"] + ".json"), j)

    def save_queue(self):
        write_json(os.path.join(STATE, "queue.json"), self.queue)

    def make(self, spec):
        r = runner_by_id(spec.get("runner", ""))
        if not r:
            raise ValueError("unknown runner %r" % spec.get("runner"))
        scen = [s for s in spec.get("scenarios", []) if re.fullmatch(r"[A-Za-z0-9_]+", s)]
        if not scen:
            raise ValueError("choose at least one scenario")
        env = {}
        for k, v in (spec.get("env") or {}).items():
            k, v = k.strip(), str(v).strip()
            if v == "":
                continue
            if not re.fullmatch(r"[A-Z_][A-Z0-9_]*", k):
                raise ValueError("bad env name %r" % k)
            env[k] = v
        for k in ("ROUNDS", "CHUNK_SLOTS", "CHUNK_ARM_COUNT", "ELECTION_TIMEOUT_MS", "YCSB_THREADS", "MERGE_THREADS", "REPS"):
            if k in env and not env[k].isdigit():
                raise ValueError(f"{k} must be a number (got {env[k]!r}); leave it blank for the default")
        for line in (spec.get("more_env") or "").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                raise ValueError("more env: expected KEY=VALUE, got %r" % line)
            k, v = line.split("=", 1)
            k = k.strip().removeprefix("export ").strip()
            if not re.fullmatch(r"[A-Z_][A-Z0-9_]*", k):
                raise ValueError("bad env name %r" % k)
            env[k] = v.strip().strip('"').strip("'")
        name = re.sub(r"[^A-Za-z0-9_.-]+", "_", spec.get("name") or "").strip("_") or "panel"
        ts = time.strftime("%Y%m%d_%H%M%S", time.gmtime())
        jid = f"{ts}_{name}"
        out_root = spec.get("out_root") or env.pop("OUT_ROOT", None) or os.path.join(LIN_ROOT, jid)
        env.pop("OUT_ROOT", None)
        return {"id": jid, "name": name, "runner": r["id"], "runner_name": r["name"], "script": r["script"],
                "scenarios": scen, "env": env, "out_root": out_root, "status": "queued",
                "created": now(), "started": None, "ended": None, "pid": None, "rc": None,
                "note": spec.get("note", "")}

    def start(self, j):
        os.makedirs(j["out_root"], exist_ok=True)
        rcf = os.path.join(j["out_root"], "panel_rc")
        try:
            os.unlink(rcf)
        except OSError:
            pass
        env = {k: v for k, v in os.environ.items() if k not in ENV_NAMES and k not in ("OUT_ROOT", "SCENARIOS_ENV", "INVENTORY")}
        env.update(j["env"])
        env["OUT_ROOT"] = j["out_root"]
        env["PANEL_RCFILE"] = rcf
        logf = open(os.path.join(j["out_root"], "panel_driver.log"), "ab")
        hdr = (f"=== panel job {j['id']} started {time.strftime('%FT%TZ', time.gmtime())}\n"
               f"=== {' '.join(f'{k}={shlex.quote(v)}' for k, v in sorted(j['env'].items()))} "
               f"OUT_ROOT={j['out_root']} {j['script']} {' '.join(j['scenarios'])}\n").encode()
        logf.write(hdr)
        logf.flush()
        p = subprocess.Popen(["bash", "-c", '"$@"; echo $? > "$PANEL_RCFILE"', "panel-job", j["script"], *j["scenarios"]],
                             cwd=os.path.dirname(j["script"]), env=env, stdin=subprocess.DEVNULL,
                             stdout=logf, stderr=subprocess.STDOUT, start_new_session=True)
        logf.close()
        self.popen[p.pid] = p
        j.update(status="running", started=now(), pid=p.pid)
        self.save(j)
        write_json(os.path.join(j["out_root"], "panel_job.json"),
                   {k: j[k] for k in ("id", "name", "runner", "scenarios", "env", "out_root", "created", "note")})
        return j

    def submit(self, spec, mode):
        j = self.make(spec)                  # validate the form first
        j["force"] = bool(spec.get("force"))
        if mode == "now":
            busy = cluster_busy(self)
            if busy:
                raise RuntimeError("cluster busy: " + busy)
            if not j["force"]:
                bad, _ = judge(check_hosts())   # up to 20 s; outside the lock so the page keeps updating
                if WATCHDOG:
                    WATCHDOG.kick.set()
                if bad:
                    raise RuntimeError("health check failed, not started:\n  " + "\n  ".join(bad)
                                       + "\nfix the host(s), or tick \u201cskip health check\u201d to start anyway")
        with self.lock:
            if mode == "now":
                busy = cluster_busy(self)
                if busy:
                    raise RuntimeError("cluster busy: " + busy)
                self.jobs[j["id"]] = j
                return self.start(j)
            self.jobs[j["id"]] = j
            self.save(j)
            self.queue.append(j["id"])
            self.save_queue()
            return j

    def refresh(self):
        with self.lock:
            for pid, p in list(self.popen.items()):
                if p.poll() is not None:
                    del self.popen[pid]
            for j in self.jobs.values():
                if j["status"] == "running" and not alive(j["pid"] or -1):
                    rc = read(os.path.join(j["out_root"], "panel_rc")).strip()
                    j["rc"] = int(rc) if rc.lstrip("-").isdigit() else None
                    j["ended"] = now()
                    if j.get("stopping"):
                        j["status"] = "stopped"
                    else:
                        j["status"] = "done" if j["rc"] == 0 else "failed"
                    self.save(j)

    def running(self):
        return [j for j in self.jobs.values() if j["status"] == "running"]

    def tick(self):
        self.refresh()
        with self.lock:
            while self.queue and self.queue[0] not in self.jobs:
                self.queue.pop(0)
            self.hold = None
            if self.queue and not cluster_busy(self):
                j = self.jobs[self.queue[0]]
                ok, why = (True, "") if j.get("force") or WATCHDOG is None else WATCHDOG.gate()
                if not ok:
                    self.hold = why   # stays first in the queue; starts once the hosts are fine
                    return
                self.queue.pop(0)
                self.save_queue()
                self.start(j)

    def stop(self, pid, job=None):
        tab = proc_table()
        pids = [pid] + descendants(pid, tab)
        if job:
            job["stopping"] = True
            self.save(job)
        # The runner's descendants run partly as root (sudo ansible-playbook, crash_inject).
        subprocess.run(["sudo", "-n", "kill", "-TERM", *map(str, pids)], capture_output=True)
        def finish():
            time.sleep(6)
            left = [p for p in pids if alive(p)]
            if left:
                subprocess.run(["sudo", "-n", "kill", "-KILL", *map(str, left)], capture_output=True)
            for h in YCSB_HOSTS:  # the YCSB run phase is async on the clients and outlives the playbook
                subprocess.run(SSH + [h, "pkill -9 -f 'ycs[b].sh'; pkill -9 -f 'site[.]ycsb[.]Client'; pkill -f '[L]inHistoryClient'; true"], capture_output=True, timeout=20)
            lk = read(LOCK)
            m = re.search(r"pid=(\d+)", lk)
            if lk and (not m or not alive(int(m.group(1)))):
                subprocess.run(["sudo", "-n", "rm", "-f", LOCK], capture_output=True)
            self.refresh()
        threading.Thread(target=finish, daemon=True).start()
        return pids


def external_runs(jobs):
    """Runner processes not started by this panel (a terminal, a Claude session)."""
    tab = proc_table()
    mine = set()
    for j in jobs.running():
        mine.add(j["pid"])
        mine.update(descendants(j["pid"], tab))
    res = []
    for p, (pp, argv) in tab.items():
        if p in mine or pp in mine:
            continue
        if any(a.endswith("run_all_lin.sh") for a in argv[:3]) and not any(
                pp == q for q, (_, a2) in tab.items() if any(x.endswith("run_all_lin.sh") for x in a2[:3])):
            env = proc_env(p)
            # the top-most ancestor that is a runner script (run.sh / run_all.sh) -> its args
            top, cur = p, pp
            while cur in tab and cur > 1 and any(x.endswith(".sh") for x in tab[cur][1][:3]):
                top, cur = cur, tab[cur][0]
            out_root = env.get("OUT_ROOT", "/tmp/lincheck/runs")
            info = {"pid": top, "inner_pid": p, "out_root": out_root, "argv": tab[top][1],
                    "scenarios": [a for a in argv[2:] if re.fullmatch(r"[A-Z][A-Z0-9_]*", a)],
                    "env": {k: v for k, v in env.items() if k in ENV_NAMES}}
            res.append(info)
            f = os.path.join(STATE, "external", os.path.basename(out_root.rstrip("/")) + ".json")
            old = None
            try:
                old = json.load(open(f))
            except (OSError, ValueError):
                pass
            if not old or old.get("scenarios") != info["scenarios"]:
                guess = "3to6" if env.get("EXP_PREFIX") == "scaleout3to6_" else "3to4"
                for a in info["argv"]:
                    if a.endswith("/run.sh") and "/experiments/" in a:
                        guess = "exp:" + os.path.basename(os.path.dirname(a))
                write_json(f, {"runner": guess, "scenarios": info["scenarios"], "out_root": out_root,
                               "env": {k: v for k, v in info["env"].items()
                                       if k not in ("EXTRA_ANSIBLE_ARGS",) or guess == "3to4"}})
    return res


def cluster_busy(jobs):
    if jobs.running():
        return "panel job %s running" % jobs.running()[0]["id"]
    if os.path.exists(LOCK):
        return "lock " + LOCK + ": " + read(LOCK).strip()
    b = busy_processes()
    if b:
        return "process %d %s" % (b[0][0], " ".join(b[0][1][:3])[:120])
    return ""


# --------------------------------------------------------------------------- runs on disk
SUMMARY_RE = re.compile(r"^(\S+) (\S+) q=(\S+) rc=(\S+) (OVERALL: \S+|OVERALL: \?) \| (.*?) \| (.*?)\s*(/\S+)?\s*$")


def parse_summary(out_root):
    rows = []
    for line in read(os.path.join(out_root, "summary.txt")).splitlines():
        m = SUMMARY_RE.match(line)
        if not m:
            continue
        ts, scen, q, rc, ov, lin, pc, d = m.groups()
        rows.append({"ts": ts, "scenario": scen, "q": q, "rc": rc, "overall": ov.split(": ")[1],
                     "lin": lin, "post": pc, "dir": d or ""})
    return rows


def run_dirs(out_root):
    ds = [d for d in glob.glob(os.path.join(out_root, "*_q*_*")) if os.path.isdir(d)]
    ds.sort(key=os.path.getmtime)
    return ds


def job_meta(out_root):
    for f in (os.path.join(out_root, "panel_job.json"),
              os.path.join(STATE, "external", os.path.basename(out_root.rstrip("/")) + ".json")):
        try:
            return json.load(open(f))
        except (OSError, ValueError):
            pass
    return None


def active_run(jobs):
    """The run in progress (panel or external) and where it is."""
    rj = jobs.running()
    ext = [] if rj else external_runs(jobs)
    if not rj and not ext:
        return None
    if rj:
        j = rj[0]
        a = {"kind": "panel", "job_id": j["id"], "pid": j["pid"], "out_root": j["out_root"],
             "scenarios": j["scenarios"], "started": j["started"], "runner": j["runner"], "name": j["name"]}
    else:
        e = ext[0]
        a = {"kind": "external", "pid": e["pid"], "out_root": e["out_root"], "scenarios": e["scenarios"],
             "started": None, "argv": " ".join(e["argv"])[:300], "name": os.path.basename(e["out_root"])}
        try:
            a["started"] = os.stat(f"/proc/{e['pid']}").st_mtime
        except OSError:
            pass
    a["setup"] = "3to6"
    if a["kind"] == "panel":
        r = runner_by_id(a["runner"]) or {}
        if a["runner"] == "3to4" or (r.get("script") and "custom_scaleout_3to6" not in read(r["script"]) and a["runner"] != "3to6"):
            a["setup"] = "3to4"
    elif proc_env(a["pid"]).get("EXP_PREFIX", "scaleout3to6_") != "scaleout3to6_" or "run_all_lin" in a.get("argv", "") and "3to6" not in a.get("argv", ""):
        a["setup"] = "3to6" if "scaleout3to6" in read(os.path.join(a["out_root"], "summary.txt")) else a["setup"]
    rows = parse_summary(a["out_root"])
    a["rows"] = rows
    done_dirs = {r["dir"].rstrip("/") for r in rows}
    started = a["started"] or 0
    cur = [d for d in run_dirs(a["out_root"]) if d not in done_dirs and os.path.getmtime(d) >= started - 5]
    a["current_dir"] = cur[-1] if cur else None
    a["current"] = None
    if a["current_dir"]:
        base = os.path.basename(a["current_dir"])
        a["current"] = base.split("_q")[0]
        a["current_started"] = run_started(a["current_dir"])
        pb = tail_lines(os.path.join(a["current_dir"], "playbook.log"), 3000)
        a["task"] = next((l.strip(" *") for l in reversed(pb) if l.startswith(("TASK [", "PLAY ["))), "")
        a["events"] = [l for l in pb if l.startswith("[crash_inject") and re.search(
            r"MARKER HIT|KILLED|RESTARTED|STOPPED|CONTINUED|PARTITIONED|HEALED|IN SYNC|CMD|TIMEOUT|gave up", l)]
    return a


def run_started(d):
    m = re.search(r"_(\d{8}_\d{6})$", d)
    if m:
        try:
            return calendar.timegm(time.strptime(m.group(1), "%Y%m%d_%H%M%S"))
        except ValueError:
            pass
    return os.path.getmtime(d)


def history(limit=80):
    roots = [d for d in glob.glob(os.path.join(LIN_ROOT, "*")) if os.path.isdir(d)]
    roots.sort(key=os.path.getmtime, reverse=True)
    res = []
    # experiment folders (experiments/<e>/data): rows point at their saved copies, data/<S>/lincheck
    for f in glob.glob(os.path.join(REPO, "experiments", "*", "data", "summary.txt")):
        data = os.path.dirname(f); e = os.path.basename(os.path.dirname(data))
        rows = parse_summary(data)
        for x in rows:
            c = os.path.join(data, x["scenario"], "lincheck")
            x["orig"] = x["dir"]; x["dir"] = c if os.path.isdir(c) else ""
        res.append({"out_root": data, "name": e, "mtime": os.path.getmtime(f), "rows": rows, "n_dirs": len(rows),
                    "meta": {"runner": "exp:" + e, "env": {}, "scenarios": [x["scenario"] for x in rows]}, "kind": "experiment",
                    "pass": sum(1 for x in rows if x["overall"] == "PASS"), "fail": sum(1 for x in rows if x["overall"] != "PASS")})
    for r in roots[:limit]:
        rows = parse_summary(r)
        meta = job_meta(r)
        res.append({"out_root": r, "name": os.path.basename(r), "mtime": os.path.getmtime(r), "rows": rows,
                    "n_dirs": len(run_dirs(r)), "meta": meta,
                    "pass": sum(1 for x in rows if x["overall"] == "PASS"),
                    "fail": sum(1 for x in rows if x["overall"] != "PASS"), "kind": "run"})
    res.sort(key=lambda r: r["mtime"], reverse=True)
    return res


def run_detail(d):
    files = []
    for f in sorted(os.listdir(d)) if os.path.isdir(d) else []:
        p = os.path.join(d, f)
        if os.path.isfile(p):
            files.append({"name": f, "size": os.path.getsize(p)})
    logs = sorted(glob.glob(os.path.join(d, "logs", "*", "*")))
    return {"dir": d, "files": files, "logs": [{"name": os.path.relpath(p, d), "size": os.path.getsize(p)}
                                                for p in logs if os.path.isfile(p)]}


# --------------------------------------------------------------------------- figures
FIG_SCRIPT = os.path.join(HERE, "figures_for_run.sh")
FIG_RUNNING = set()
FIG_LOCK = threading.Lock()


def fig_status(run_dir):
    out = os.path.dirname(run_dir.rstrip("/"))
    return read(os.path.join(out, "figures", ".status_" + os.path.basename(run_dir.rstrip("/")))).strip()


def make_figures(run_dir):
    """House-style figures of one finished scenario run, in the background (figures_for_run.sh)."""
    run_dir = run_dir.rstrip("/")
    with FIG_LOCK:
        if run_dir in FIG_RUNNING:
            return False
        FIG_RUNNING.add(run_dir)
    def go():
        try:
            env = dict(os.environ)
            c = exp_copies().get(run_dir)
            if c and os.path.isdir(os.path.join(c, "exp", "ycsb")):
                env["FIG_EXP_SRC"] = os.path.join(c, "exp")   # the experiment's copy (run.sh) of this run
            subprocess.run([FIG_SCRIPT, run_dir], stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL, timeout=900, env=env)
        except Exception as e:
            print("figures", run_dir, repr(e), file=sys.stderr)
        finally:
            with FIG_LOCK:
                FIG_RUNNING.discard(run_dir)
    threading.Thread(target=go, daemon=True).start()
    return True


FIG_KINDS = (("tputlat_{s}.png", "throughput + latency"), ("{s}_tput_lat.png", "throughput + latency (older style)"),
             ("paper_{s}_recovery_wide.png", "recovery (7 in)"), ("gantt_{s}.png", "migration Gantt"),
             ("paper_{s}_recovery.png", "recovery (3.33 in)"), ("{s}_recovery.png", "recovery, detailed"))


def fig_files(fig, s):
    files = []
    for pat, label in FIG_KINDS:
        p = os.path.join(fig, pat.format(s=s))
        if os.path.isfile(p):
            pdf = p[:-4] + ".pdf"
            files.append({"path": p, "name": os.path.basename(p), "label": label, "mtime": os.path.getmtime(p),
                          "pdf": pdf if os.path.isfile(pdf) else None})
    return files


def run_figures(out_root, only=None):
    """Per scenario run: figure status, the PNGs (paper figures first) and their PDFs. For an experiment
    folder (experiments/<e>/data) the figures are the ones its figures.sh drew into experiments/<e>/figures."""
    out_root = out_root.rstrip("/")
    if os.path.basename(out_root) == "data" and os.path.isfile(os.path.join(out_root, "summary.txt")):
        fig = os.path.join(os.path.dirname(out_root), "figures")
        res = []
        for r in parse_summary(out_root):
            s = r["scenario"]
            d = os.path.join(out_root, s, "lincheck")
            if only and d.rstrip("/") != only.rstrip("/"):
                continue
            t = os.path.join(fig, f"{s}_table.txt")
            res.append({"dir": d, "base": s, "scenario": s, "status": "done", "files": fig_files(fig, s), "overall": r["overall"],
                        "table": t if os.path.isfile(t) else None, "log": None, "readonly": True})
        if not only:
            extra = [{"path": p, "name": os.path.basename(p), "label": "whole suite", "mtime": os.path.getmtime(p),
                      "pdf": p[:-4] + ".pdf" if os.path.isfile(p[:-4] + ".pdf") else None}
                     for p in sorted(glob.glob(os.path.join(fig, "tputlat_all.png")) + glob.glob(os.path.join(fig, "suite*.png")))]
            if extra:
                res.append({"dir": out_root, "base": "suite", "scenario": "suite", "status": "done", "files": extra, "overall": None,
                            "table": os.path.join(fig, "suite_table.md") if os.path.isfile(os.path.join(fig, "suite_table.md")) else None,
                            "log": None, "readonly": True})
        return res
    fig = os.path.join(out_root, "figures")
    rows = {r["dir"].rstrip("/"): r for r in parse_summary(out_root)}
    res = []
    for d in run_dirs(out_root):
        base = os.path.basename(d)
        if only and d.rstrip("/") != only.rstrip("/"):
            continue
        s = base.split("_q")[0]
        st = "running" if d in FIG_RUNNING else fig_status(d)
        r = rows.get(d.rstrip("/"))
        res.append({"dir": d, "base": base, "scenario": s, "status": st, "files": fig_files(fig, s),
                    "overall": r["overall"] if r else None,
                    "table": os.path.join(fig, f"{s}_table.txt") if os.path.isfile(os.path.join(fig, f"{s}_table.txt")) else None,
                    "log": os.path.join(fig, f"{s}_figlog.txt") if os.path.isfile(os.path.join(fig, f"{s}_figlog.txt")) else None})
    return res


def figures_zip(out_root, only=None):
    """All PNG + PDF figures of a run / experiment folder (or of one scenario) as one zip."""
    import io, zipfile
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_STORED) as z:
        for r in run_figures(out_root, only):
            for f in r["files"]:
                z.write(f["path"], f["name"])
                if f["pdf"]:
                    z.write(f["pdf"], os.path.basename(f["pdf"]))
            if r.get("table"):
                z.write(r["table"], os.path.basename(r["table"]))
    return buf.getvalue()


# --------------------------------------------------------------------------- live feeds
class Hub:
    """Fan-out of server-sent events; keeps a backlog per stream for clients that connect late."""

    def __init__(self):
        self.lock = threading.Lock()
        self.clients = set()
        self.backlog = {}          # stream -> deque of lines
        self.tput = collections.OrderedDict()   # (host, ts) -> sample
        self.topo = collections.deque(maxlen=20000)
        self.topo_run = None
        self.state = {}

    def subscribe(self):
        q = queuemod.Queue(maxsize=5000)
        with self.lock:
            self.clients.add(q)
            init = [("state", self.state)]
            for s, dq in self.backlog.items():
                init.append(("log", {"stream": s, "reset": True, "lines": list(dq)}))
            init.append(("tput", {"reset": True, "samples": list(self.tput.values())}))
            init.append(("topo", {"reset": True, "events": list(self.topo)}))
        for ev in init:
            q.put(ev)
        return q

    def unsubscribe(self, q):
        with self.lock:
            self.clients.discard(q)

    def n(self):
        return len(self.clients)

    last_data = 0.0   # last log line / YCSB sample / server-log event: the watchdog's sign of life

    def send(self, kind, data):
        if kind in ("log", "tput", "topo"):
            self.last_data = now()
        with self.lock:
            for q in list(self.clients):
                try:
                    q.put_nowait((kind, data))
                except queuemod.Full:
                    self.clients.discard(q)

    def topo_event(self, ev):
        with self.lock:
            self.topo.append(ev)
        self.send("topo", {"events": [ev]})

    def topo_reset(self):
        with self.lock:
            self.topo.clear()
            TOPO_PARSER.reset()
        self.send("topo", {"reset": True, "events": []})

    def log(self, stream, lines, reset=False):
        with self.lock:
            dq = self.backlog.setdefault(stream, collections.deque(maxlen=1500))
            if reset:
                dq.clear()
            dq.extend(lines)
        self.send("log", {"stream": stream, "reset": reset, "lines": lines})


class FileFollower(threading.Thread):
    """Follows the driver log, run.log and playbook.log of the active run."""

    def __init__(self, hub, jobs):
        super().__init__(daemon=True)
        self.hub, self.jobs = hub, jobs
        self.files = {}   # stream -> [path, offset, partial]

    def follow(self, stream, path):
        cur = self.files.get(stream)
        if cur is None or cur[0] != path:
            if path is None:
                if cur is not None:
                    self.hub.log(stream, [], reset=True)
                self.files[stream] = [None, 0, ""]
                return
            lines = tail_lines(path, 600)
            try:
                off = os.path.getsize(path)
            except OSError:
                off = 0
            self.files[stream] = [path, off, ""]
            self.hub.log(stream, [f"--- {path}"] + lines, reset=True)
            return
        if path is None:
            return
        try:
            size = os.path.getsize(path)
        except OSError:
            return
        if size < cur[1]:
            cur[1] = 0
        if size == cur[1]:
            return
        with open(path, "rb") as f:
            f.seek(cur[1])
            data = f.read(min(size - cur[1], 4_000_000)).decode(errors="replace")
            cur[1] = f.tell()
        data = cur[2] + data
        parts = data.split("\n")
        cur[2] = parts.pop()
        if parts:
            self.hub.log(stream, parts)

    def run(self):
        last_state, a = 0, None
        while True:
            try:
                fresh = now() - last_state > 1.5
                if fresh:
                    self.jobs.tick()
                    a = active_run(self.jobs)
                if fresh and a:
                    for r in a["rows"]:
                        rd = (r.get("dir") or "").rstrip("/")
                        if rd and os.path.isdir(rd) and not fig_status(rd) and rd not in FIG_RUNNING:
                            make_figures(rd)
                d = a and a.get("current_dir")
                if d and d != self.hub.topo_run:
                    if self.hub.topo_run is not None:
                        self.hub.topo_reset()
                    self.hub.topo_run = d
                drv = a and os.path.join(a["out_root"], "panel_driver.log")
                self.follow("driver", drv if drv and os.path.exists(drv) else None)
                self.follow("run", os.path.join(d, "run.log") if d else None)
                self.follow("playbook", os.path.join(d, "playbook.log") if d else None)
                if fresh:
                    last_state = now()
                    st = {"active": a, "busy": cluster_busy(self.jobs), "lock": read(LOCK).strip(),
                          "queue": [self.jobs.jobs[q] for q in self.jobs.queue if q in self.jobs.jobs],
                          "jobs": sorted(self.jobs.jobs.values(), key=lambda j: j["created"], reverse=True)[:30],
                          "nodes": NODES.snapshot(), "time": now(), "hold": self.jobs.hold,
                          "health": WATCHDOG.snapshot() if WATCHDOG else None}
                    self.hub.state = st
                    self.hub.send("state", st)
            except Exception as e:  # keep the panel alive whatever a file looks like
                print("follower:", repr(e), file=sys.stderr)
            time.sleep(0.7)


YCSB_RE = re.compile(r"^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d):\d+ (\d+) sec: (\d+) operations;(?: ([\d.]+) current ops/sec;)?(.*)$")


def parse_ycsb(line, host, phase):
    m = YCSB_RE.match(line)
    if not m:
        return None
    ts = calendar.timegm(time.strptime(m.group(1), "%Y-%m-%d %H:%M:%S"))
    rest = m.group(5)
    lat = {k.lower(): float(v) for k, v in re.findall(r"\[(READ|UPDATE|INSERT) AverageLatency\(us\)=([\d.]+)\]", rest)}
    for k, v in re.findall(r"\[(READ|UPDATE|INSERT): Count=\d+, Max=\d+, Min=\d+, Avg=([\d.]+)", rest):
        lat.setdefault(k.lower(), float(v))
    return {"host": host, "phase": phase, "ts": ts, "sec": int(m.group(2)),
            "ops": int(m.group(3)), "tput": float(m.group(4) or 0), "lat": lat}


def mig_window(d):
    """[first TXN_START logged, last round done] over all donors, from the run dir's server logs."""
    P, t1, t2 = TopoParser(), None, None
    for f in glob.glob(os.path.join(d, "logs", "redis*", "*.log")):
        host, sg = os.path.basename(f)[:-4].split("_", 1)
        for line in read(f).splitlines():
            if "TXN_START logged" not in line and "TXN_DONE logged" not in line:
                continue
            e = P.parse(host, sg, line)
            if not e:
                continue
            if e["k"] == "d_round":
                t1 = e["t"] if t1 is None else min(t1, e["t"])
            elif e["k"] == "d_commit":
                t2 = e["t"] if t2 is None else max(t2, e["t"])
    return [t1, t2] if t1 else None


def run_topo(d):
    """The topology events of a finished run, from the server logs copied into its run dir."""
    pat, P, lines = re.compile(TOPO_GREP), TopoParser(), []
    for f in glob.glob(os.path.join(d, "logs", "redis*", "*.log")):
        host, sg = os.path.basename(f)[:-4].split("_", 1)
        for line in read(f).splitlines():
            if pat.search(line):
                m = LOGLINE.match(line)
                if m:
                    lines.append((m.group(1)[-8:] + "." + m.group(2), host, sg, line))
    lines.sort(key=lambda x: x[0])
    return [e for _, h, sg, l in lines for e in [P.parse(h, sg, l)] if e]


EXP_COPIES = {"t": 0, "map": {}}


def exp_copies():
    """original /tmp/lincheck run dir -> experiments/<e>/data/<S> (the copies run.sh makes), from
    each experiment's data/summary.txt; refreshed every 60 s."""
    if now() - EXP_COPIES["t"] > 60:
        m = {}
        for f in glob.glob(os.path.join(REPO, "experiments", "*", "data", "summary.txt")):
            data = os.path.dirname(f)
            for r in parse_summary(data):
                c = os.path.join(data, r["scenario"])
                if r["dir"] and os.path.isdir(c):
                    m[r["dir"].rstrip("/")] = c
        EXP_COPIES.update(t=now(), map=m)
    return EXP_COPIES["map"]


def run_scenario(d):
    base = os.path.basename(d.rstrip("/"))
    return os.path.basename(os.path.dirname(d.rstrip("/"))) if base == "lincheck" else base.split("_q")[0]


def run_ycsb(d):
    """YCSB samples, crash events, migration window and topology events of a finished scenario run.
    YCSB from (in order): the run's own copy (experiments/<e>/data/<S>/exp next to its lincheck dir),
    the experiment copy made from this /tmp/lincheck run, the panel's copy, /tmp/experiments (only
    while no later run of the scenario overwrote it). Without any, the replay still has the topology
    and the timeline (server logs are in every run dir)."""
    d = d.rstrip("/")
    base, scen = os.path.basename(d), run_scenario(d)
    in_exp = base == "lincheck"
    t0 = run_started(d) if not in_exp else 0
    t1 = max([os.path.getmtime(os.path.join(d, f)) for f in os.listdir(d)] + [t0]) + 60
    names = ["lin_healthy"] if scen == "HEALTHY" else ["crash_" + scen.lower()]
    cands = []
    if in_exp:
        cands.append((os.path.join(os.path.dirname(d), "exp"), False))
    if d in exp_copies():
        cands.append((os.path.join(exp_copies()[d], "exp"), False))
    cands.append((os.path.join(os.path.dirname(d), "panel_data", base, "exp"), False))
    cands += [(os.path.join("/tmp/experiments", pre + n), True) for pre in ("scaleout3to6_", "") for n in names]
    pb = tail_lines(os.path.join(d, "playbook.log"), 100000, 50_000_000) + tail_lines(os.path.join(d, "run.log"), 5000)
    seen, events = set(), []
    for l in pb:
        if l.startswith("[crash_inject") and re.search(r"KILLED|RESTARTED|STOPPED|CONTINUED|PARTITIONED|HEALED", l) and l not in seen:
            seen.add(l); events.append(l)
    topo, mig = run_topo(d), mig_window(d)
    note = f"no YCSB data left for this {scen} run (only the latest run of a scenario stays in /tmp/experiments): topology and timeline only"
    for c, check in cands:
        files = glob.glob(os.path.join(c, "ycsb", "*", "tmp", "ycsb_output_*"))
        if not files:
            continue
        samples = []
        for f in files:
            host = os.path.basename(f).split("_")[-1]
            phase = "load" if "_load_" in f else "run"
            for line in read(f).splitlines():
                smp = parse_ycsb(line, host, phase)
                if smp:
                    samples.append(smp)
        run = [x for x in samples if x["phase"] == "run"]
        if run and (not check or t0 - 120 <= min(x["ts"] for x in run) <= t1):
            return {"samples": samples, "events": events, "source": c, "t0": t0, "mig": mig, "topo": topo}
    return {"samples": [], "events": events, "note": note, "t0": t0, "mig": mig, "topo": topo}


class YcsbTail(threading.Thread):
    def __init__(self, hub, host):
        super().__init__(daemon=True)
        self.hub, self.host = hub, host

    def run(self):
        files = f"/tmp/ycsb_output_{self.host} /tmp/ycsb_output_load_{self.host}"
        while True:
            try:
                p = subprocess.Popen(SSH + [self.host, f"tail -n 400 -F {files} 2>&1"], stdout=subprocess.PIPE,
                                     stderr=subprocess.DEVNULL, stdin=subprocess.DEVNULL, text=True, errors="replace")
                phase = "run"
                for line in p.stdout:
                    line = line.rstrip("\n")
                    if line.startswith("==> "):
                        phase = "load" if "_load_" in line else "run"
                        continue
                    s = parse_ycsb(line, self.host, phase)
                    if not s:
                        continue
                    ts = s["ts"]
                    key = (self.host, phase, ts)
                    with self.hub.lock:
                        dup = key in self.hub.tput
                        self.hub.tput[key] = s
                        while len(self.hub.tput) > 8000:
                            self.hub.tput.popitem(last=False)
                    if not dup:
                        self.hub.send("tput", {"samples": [s]})
                p.wait()
            except Exception as e:
                print("ycsb tail", self.host, repr(e), file=sys.stderr)
            time.sleep(5)


# --------------------------------------------------------------------------- host health / watchdog
# Hosts that hang (no SSH, redis not answering) used to leave a job stuck forever on its next task. The
# panel now (1) checks every host before a job starts and refuses / holds the queue when one is not
# usable, (2) keeps checking while a job runs and stops it when a host is gone, memory runs out, or
# nothing has happened for a long time, (3) shows it all on the page. Limits: PANEL_<NAME> env vars.
LIMITS = {
    "block_mem_gb": 16,        # launch needs this much memory free (+ what old redis-servers would free) per redis host
    "warn_mem_gb": 24,
    "block_disk_pct": 95,      # /tmp's filesystem
    "warn_disk_pct": 85,
    "run_low_mem_gb": 4,       # while running: warn below this ...
    "run_kill_mem_gb": 1.5,    # ... stop the job and kill redis on that host below this (before the host locks up)
    "unreachable_stop_s": 300, # stop a job when a host has not answered SSH for this long
    "stall_warn_s": 420,       # no log line / YCSB sample / server-log event / file write for this long
    "stall_stop_s": 1200,
    "period_run_s": 30,
    "period_idle_s": 120,
}
for _k in LIMITS:
    try:
        LIMITS[_k] = float(os.environ["PANEL_" + _k.upper()])
    except (KeyError, ValueError):
        pass

HEALTH_CMD = ("grep -E '^(MemTotal|MemAvailable|SwapTotal|SwapFree):' /proc/meminfo; echo load $(cut -d' ' -f1 /proc/loadavg);"
              " echo ncpu $(nproc); echo disk $(df -P /tmp | tail -1 | awk '{print $5}' | tr -d %);"
              " echo redis $(ps -C redis-server -o rss= | awk '{s+=$1; n++} END {print n+0, s+0}')")


def host_health(h):
    t0 = time.time()
    try:
        if h == REDIS_HOSTS[0] and os.uname().nodename.startswith(h + "."):
            p = subprocess.run(["bash", "-c", HEALTH_CMD], capture_output=True, text=True, timeout=15)
        else:
            p = subprocess.run(SSH + [h, HEALTH_CMD], capture_output=True, text=True, timeout=15)
    except subprocess.TimeoutExpired:
        return {"host": h, "ok": False, "err": "no answer in 15 s (host hung?)", "t": now()}
    if p.returncode == 255 or "MemTotal" not in p.stdout:
        err = (p.stderr.strip().splitlines() or ["ssh failed"])[-1]
        return {"host": h, "ok": False, "err": "SSH: " + err[-120:], "t": now()}
    kv = {}
    for line in p.stdout.splitlines():
        parts = line.replace(":", " ").split()
        if parts:
            kv[parts[0]] = parts[1:]
    g = lambda k, i=0: float(kv.get(k, [0] * (i + 1))[i] or 0)
    return {"host": h, "ok": True, "ms": int((time.time() - t0) * 1000), "t": now(),
            "mem_total_gb": round(g("MemTotal") / 2**20, 1), "mem_avail_gb": round(g("MemAvailable") / 2**20, 1),
            "swap_used_gb": round((g("SwapTotal") - g("SwapFree")) / 2**20, 1),
            "load1": g("load"), "ncpu": int(g("ncpu")), "disk_pct": int(g("disk")),
            "redis_n": int(g("redis")), "redis_rss_gb": round(g("redis", 1) / 2**20, 1)}


def check_hosts(hosts=None):
    hosts = hosts or REDIS_HOSTS + YCSB_HOSTS
    res, ths = {}, []
    for h in hosts:
        t = threading.Thread(target=lambda h=h: res.__setitem__(h, host_health(h)), daemon=True)
        t.start()
        ths.append(t)
    end = time.time() + 20
    for t in ths:
        t.join(max(0.1, end - time.time()))
    for h in hosts:
        res.setdefault(h, {"host": h, "ok": False, "err": "no answer in 20 s (host hung?)", "t": now()})
    return res


def judge(health):
    """-> (problems that block a launch, warnings)"""
    bad, warn = [], []
    for h, x in sorted(health.items()):
        if not x.get("ok"):
            bad.append(f"{h}: {x.get('err', 'not reachable')}")
            continue
        if h in REDIS_HOSTS:
            free = x["mem_avail_gb"] + x["redis_rss_gb"]   # the playbook restarts any redis-server left over
            if free < LIMITS["block_mem_gb"]:
                bad.append(f"{h}: only {free:.0f} GB memory free (needs {LIMITS['block_mem_gb']:.0f}); something else is using it")
            elif free < LIMITS["warn_mem_gb"]:
                warn.append(f"{h}: {free:.0f} GB memory free")
        if x["disk_pct"] >= LIMITS["block_disk_pct"]:
            bad.append(f"{h}: /tmp filesystem {x['disk_pct']}% full")
        elif x["disk_pct"] >= LIMITS["warn_disk_pct"]:
            warn.append(f"{h}: /tmp filesystem {x['disk_pct']}% full")
        if x["swap_used_gb"] >= 2:
            warn.append(f"{h}: {x['swap_used_gb']} GB swap in use")
        if x["ncpu"] and x["load1"] > 2 * x["ncpu"]:
            warn.append(f"{h}: load {x['load1']:.0f} on {x['ncpu']} cpus")
    return bad, warn


class Watchdog(threading.Thread):
    def __init__(self, hub, jobs):
        super().__init__(daemon=True)
        self.hub, self.jobs = hub, jobs
        self.health, self.t = {}, 0
        self.down_since = {}
        self.alerts = []          # what is wrong right now: [{sev, msg}]
        self.events = []          # what the watchdog did: [{t, msg}]
        self.lock = threading.Lock()
        self.kick = threading.Event()
        try:
            self.events = json.load(open(os.path.join(STATE, "watchdog.json")))[-30:]
        except (OSError, ValueError):
            pass

    def note(self, msg):
        print("watchdog:", msg, file=sys.stderr)
        with self.lock:
            self.events = (self.events + [{"t": now(), "msg": msg}])[-30:]
            write_json(os.path.join(STATE, "watchdog.json"), self.events)

    def snapshot(self):
        with self.lock:
            bad, warn = judge(self.health) if self.health else ([], [])
            return {"t": self.t, "hosts": self.health, "alerts": list(self.alerts), "events": self.events[-10:],
                    "blockers": bad, "warnings": warn, "limits": LIMITS}

    def gate(self, max_age=90):
        """May a queued job start? -> (ok, reason). Uses the last check; asks for a new one when old."""
        if now() - self.t > max_age:
            self.kick.set()
            return False, "checking the hosts first"
        bad, _ = judge(self.health)
        return (not bad), "; ".join(bad)

    def activity(self, job):
        t = self.hub.last_data
        root = job.get("out_root") if job else None
        if root and os.path.isdir(root):
            for dp, dn, fn in os.walk(root):
                if dp.count(os.sep) - root.count(os.sep) >= 3:
                    dn[:] = []
                for f in fn:
                    try:
                        t = max(t, os.path.getmtime(os.path.join(dp, f)))
                    except OSError:
                        pass
        return t

    def step(self):
        res = check_hosts()
        t = now()
        run = self.jobs.running()
        job = run[0] if run else None
        alerts = []
        for h, x in res.items():
            if x.get("ok"):
                if h in self.down_since:
                    self.note(f"{h} answers again after {int(t - self.down_since[h])} s")
                self.down_since.pop(h, None)
            else:
                if h not in self.down_since:
                    self.down_since[h] = t
                    self.note(f"{h} stopped answering: {x.get('err')}")
                alerts.append({"sev": "bad", "since": self.down_since[h], "msg": f"{h} not reachable — {x.get('err')}",
                               "stop_at": self.down_since[h] + LIMITS["unreachable_stop_s"]})
        bad, warn = judge(res)
        for w in warn:
            alerts.append({"sev": "warn", "msg": w})
        for h, x in res.items():
            if x.get("ok") and h in REDIS_HOSTS and job and x["mem_avail_gb"] < LIMITS["run_low_mem_gb"]:
                alerts.append({"sev": "bad", "msg": f"{h}: only {x['mem_avail_gb']} GB memory left during the run"})
        reason, rescue = None, []
        if job and not job.get("stopping"):
            for h, s in self.down_since.items():
                if t - s >= LIMITS["unreachable_stop_s"]:
                    reason = f"{h} has not answered for {int(t - s)} s"
            for h, x in res.items():
                if x.get("ok") and h in REDIS_HOSTS and x["mem_avail_gb"] < LIMITS["run_kill_mem_gb"]:
                    reason = f"{h} ran out of memory ({x['mem_avail_gb']} GB left)"
                    rescue.append(h)
            idle = t - max(self.activity(job), job.get("started") or 0)
            if idle >= LIMITS["stall_warn_s"]:
                alerts.append({"sev": "bad" if idle >= LIMITS["stall_stop_s"] * 0.75 else "warn",
                               "msg": f"no progress for {int(idle // 60)} min (no log line, YCSB sample or file write)"
                                      f" — auto-stop at {int(LIMITS['stall_stop_s'] // 60)} min"})
            if not reason and idle >= LIMITS["stall_stop_s"]:
                reason = f"no progress for {int(idle // 60)} min"
        with self.lock:
            self.health, self.t, self.alerts = res, t, alerts
        if reason:
            self.note(f"auto-stopped job {job['id']}: {reason}")
            job["stop_reason"] = "auto-stopped: " + reason
            self.jobs.stop(job["pid"], job)
            for h in rescue:   # free the host before it locks up; the next run restarts redis anyway
                subprocess.run(SSH + [h, "sudo -n pkill -9 redis-server; true"], capture_output=True, timeout=20)
                self.note(f"killed redis-server on {h} to free its memory")

    def run(self):
        while True:
            try:
                self.step()
            except Exception as e:
                print("watchdog:", repr(e), file=sys.stderr)
            busy = self.jobs.running() or self.jobs.queue
            self.kick.wait(LIMITS["period_run_s"] if busy else LIMITS["period_idle_s"])
            self.kick.clear()


class Nodes(threading.Thread):
    """redis-server processes per host, polled while somebody watches."""

    def __init__(self, hub):
        super().__init__(daemon=True)
        self.hub = hub
        self.data = {}
        self.lock = threading.Lock()

    def snapshot(self):
        with self.lock:
            return dict(self.data)

    def poll(self, h):
        cmd = "ps -C redis-server -o pid=,etimes=,args= 2>/dev/null; true"
        try:
            if h == REDIS_HOSTS[0] and os.uname().nodename.startswith(h + "."):
                out = subprocess.run(["bash", "-c", cmd], capture_output=True, text=True, timeout=8).stdout
            else:
                p = subprocess.run(SSH + [h, cmd], capture_output=True, text=True, timeout=8)
                if p.returncode == 255:
                    return {"error": "unreachable", "t": now()}
                out = p.stdout
        except subprocess.TimeoutExpired:
            return {"error": "unreachable", "t": now()}
        inst = []
        hi = REDIS_HOSTS.index(h)
        for line in out.splitlines():
            parts = line.split(None, 2)
            if len(parts) < 3:
                continue
            m = re.search(r":(\d{4,5})\b", parts[2])
            port = int(m.group(1)) if m else 0
            sg = (port - 8000 + 1 + (3 if hi >= 3 else 0)) if 8000 <= port < 8003 else None
            inst.append({"pid": int(parts[0]), "up": int(parts[1]), "port": port, "sg": sg})
        inst.sort(key=lambda i: i["port"])
        return {"inst": inst, "t": now()}

    def run(self):
        while True:
            if self.hub.n() > 0:
                ths, res = [], {}
                for h in REDIS_HOSTS:
                    t = threading.Thread(target=lambda h=h: res.__setitem__(h, self.poll(h)), daemon=True)
                    t.start()
                    ths.append(t)
                for t in ths:
                    t.join(10)
                with self.lock:
                    self.data.update(res)
            time.sleep(2)


# --------------------------------------------------------------------------- live topology
# One `tail -F | grep` per redis host over the server logs (/tmp/redis_logs/<host>_sg<N>.log), turned
# into small events: elections, donor rounds / copy / chunks, chunks landing on the recipient leader,
# the chain order, leader -> F1 -> F2 forwarding, index update, RECP_DURABLE, TXN_DONE, test hooks,
# recovery lines. The page replays them into the topology picture. Logs are recreated per scenario.
def topology(setup):
    import yaml
    total = 2730 if setup == "3to6" else 1365
    try:
        if setup == "3to6":
            y = yaml.safe_load(open(os.path.join(REPO, "ansible/experiments/custom_scaleout_3to6/topology.yml")))
            total = int(y.get("scaleout_slots_per_source", total))
        else:
            y = yaml.safe_load(open(os.path.join(REPO, "ansible/group_vars/all.yml")))
        groups = []
        for g in y["redisraft_shardgroups"]:
            role = g.get("role") or ("recipient" if g["id"] == "sg4" else "donor")
            groups.append({"id": g["id"], "port": g.get("port"), "role": role,
                           "recipient": (g.get("recipient") or "sg4") if role == "donor" else None,
                           "slots": g.get("slot_config", ""), "hosts": [n["host"] for n in g["nodes"]]})
    except Exception as e:
        return {"setup": setup, "error": repr(e), "groups": [], "total": total}
    return {"setup": setup, "groups": groups, "total": total}


TOPO_GREP = ("^==> |now a |just started|RDMA MIGRATE worker: (started|id=)|TRANSFER chunk seq|TXN_START logged|"
             "TXN_DONE logged|MGN_RECP_TXN_START applied|DONE-SLOTS-CHUNK|CHAIN view|forward FIRST-POST|"
             "CHAIN-FORWARDED: sess=[0-9]+ length|pipelined forward complete|INDEX-UPDATE done|merge_done|"
             "RECP_DURABLE logged|MGN_RECP_DURABLE applied|TEST HOOK|MGN-RECOVER|recipient-closed|refetch|resend|chain: batch|"
             "recipient-recover:|re-dispatched|re-shipped|resume-plan")
LOGLINE = re.compile(r"^\d+:\w (\d\d \w\w\w \d{4} \d\d:\d\d:\d\d)\.(\d+) [*#] (.*)$")
RNG = re.compile(r"slots=(\d+-\d+)")


class TopoParser:
    def __init__(self):
        self.reset()

    def reset(self):
        self.dmap, self.cmap = {}, {}   # donor (sg, sess|mig id) -> slot range; recipient (sg, chain sess) -> range

    def parse(self, host, sg, line):
        m = LOGLINE.match(line)
        if not m:
            return None
        try:
            t = time.mktime(time.strptime(m.group(1), "%d %b %Y %H:%M:%S")) + float("0." + m.group(2))
        except ValueError:
            return None
        x = m.group(3)
        ev = {"t": t, "h": host, "g": sg, "r": "", "x": {}}
        rm = RNG.search(x)
        rng = rm.group(1) if rm else ""
        def num(p):
            mm = re.search(p, x)
            return mm.group(1) if mm else None
        if "now a " in x:
            mm = re.search(r"now a (\w+)(?:, term (\d+))?", x)
            ev.update(k="role", x={"role": mm.group(1), "term": int(mm.group(2) or 0)})
        elif "just started" in x:
            ev.update(k="start", x={"pid": line.split(":", 1)[0]})
        elif "RDMA MIGRATE worker:" in x:
            i = num(r"(?:started id|id)=(\d+)")
            if rng and i:
                self.dmap[(sg, i)] = rng
            r = self.dmap.get((sg, i), "")
            if "started id=" in x:
                ev.update(k="d_start", x={"n": int(num(r"n_slots=(\d+)") or 0)})
            elif "state=TRANSFER" in x:
                ev.update(k="d_copy", r=r)
            elif "TRANSFER chunk seq" in x:
                ev.update(k="d_chunk", x={"seq": int(num(r"seq=(\d+)") or 0), "n": int(num(r" slots=(\d+)") or 0)})
            elif "state=BACKPATCH" in x:
                ev.update(k="d_copied", r=r)
            elif re.search(r"id=\d+ DONE", x):
                ev.update(k="d_done", r=r)
            elif re.search(r"id=\d+ FAILED", x):
                ev.update(k="d_fail", r=r)
            else:
                return None
        elif "RECP_TXN_START logged" in x:
            ev.update(k="r_open", r=rng)
        elif "TXN_START logged" in x:
            i = num(r"sess=(\d+)")
            self.dmap[(sg, i)] = rng
            ev.update(k="d_round", r=rng, x={"n": int(num(r" n=(\d+)") or 0)})
        elif "TXN_DONE logged" in x:
            ev.update(k="d_commit", r=self.dmap.get((sg, num(r"sess=(\d+)")), ""))
        elif "MGN_RECP_TXN_START applied" in x:
            ev.update(k="r_open", r=rng)
        elif "chain: batch" in x:
            mig, cs = num(r"mig_id=(\d+)"), num(r"chain_sess=(\d+)")
            for (dsg, i), r in self.dmap.items():
                if i == mig:
                    self.cmap[(sg, cs)] = r
            return None
        elif "DONE-SLOTS-CHUNK" in x:
            mig = num(r"mig_id=(\d+)")
            r = next((r for (dsg, i), r in self.dmap.items() if i == mig), "")
            ev.update(k="r_chunk", r=r, x={"seq": int(num(r"seq=(\d+)") or 0), "n": int(num(r"n_slots=(\d+)") or 0)})
        elif "CHAIN view" in x:
            cs = num(r"sess=(\d+)")
            if rng:
                self.cmap[(sg, cs)] = rng
            ch = num(r"chain=(\S+)") or ""
            ev.update(k="c_view", r=rng, x={"chain": [c.split(":")[0] for c in ch.split(",") if c and c != "-"]})
        elif "recipient-recover:" in x or "re-dispatched migration" in x or "re-shipped slots" in x or "resume-plan:" in x:
            # where the interrupted round comes from after a crash (cf. recovery_breakdown.py "refetch")
            rr = num(r"range=(\d+-\d+)") or rng
            if "probing" in x and "I hold" in x:
                ev.update(k="rec_probe", r=rr, x={"have": int(num(r"I hold (\d+)/") or 0), "n": int(num(r"I hold \d+/(\d+)") or 0),
                                                 "peers": int(num(r"probing (\d+) peers") or 0)})
            elif re.search(r"I (hold|lack) the range", x):
                mm = re.search(r"I (hold|lack) the range, (\d+)/(\d+) peers hold all of it", x)
                kind = "self" if mm and mm.group(1) == "hold" else ("peer" if mm and int(mm.group(2)) > 0 else "none")
                ev.update(k="rec_found", r=rr, x={"kind": kind, "holders": int(mm.group(2)) if mm else 0})
            elif "asked holder" in x:
                ev.update(k="rec_ask", r=rr, x={"holder": num(r"asked holder (redis\d+)")})
            elif "held here as a follower was applied" in x:
                ev.update(k="rec_adopt", r=rr)
            elif "told donor" in x:
                ev.update(k="rec_told", r=rng or rr, x={"leader": num(r"is now (redis\d+)")})
            elif "re-dispatched migration" in x:
                ip = num(r"to (\d+\.\d+\.\d+\.\d+):")
                ev.update(k="d_reship", r=rr, x={"to": f"redis{int(ip.split('.')[-1]) - 1}" if ip else None})
            elif "re-shipped slots" in x:
                lo, n = num(r"lo=(\d+)"), num(r" n=(\d+)")
                ev.update(k="d_reship", r=f"{lo}-{int(lo) + int(n) - 1}" if lo and n else rr, x={"to": num(r"NEW leader (redis\d+)")})
            elif "resume-plan:" in x and "recipient reports" in x:
                ev.update(k="d_plan", r=rr, x={"durable": int(num(r"durable=(\d+)") or 0), "pending": int(num(r"pending=(\d+)") or 0),
                                               "missing": int(num(r"missing=(\d+)") or 0)})
            else:
                return None
        else:
            cs = num(r"sess=(\d+)")
            if rng and cs:
                self.cmap[(sg, cs)] = rng
            r = rng or self.cmap.get((sg, cs), "")
            if "forward FIRST-POST" in x:
                ev.update(k="c_fwd", r=r)
            elif "CHAIN-FORWARDED" in x:
                ev.update(k="c_recv", r=r, x={"tail": int(num(r"tail=(\d+)") or 0)})
            elif "pipelined forward complete" in x:
                ev.update(k="c_fwd_done", r=r)
            elif "INDEX-UPDATE done" in x:
                ev.update(k="r_index", r=r, x={"role": num(r"role=(\w+)")})
            elif "merge_done" in x:
                mig = num(r"mig_id=(\d+)")
                r = r or next((r2 for (dsg, i), r2 in self.dmap.items() if i == mig), "")
                ev.update(k="r_merged", r=r)
            elif "RECP_DURABLE logged" in x:
                ev.update(k="r_durable", r=r)
            elif "MGN_RECP_DURABLE applied" in x:
                ev.update(k="r_durable_f", r=r)
            elif "TEST HOOK" in x:
                ev.update(k="hook", x={"name": num(r"TEST HOOK (\S+)")})
            else:
                ev.update(k="recover", r=r, x={"msg": re.sub(r"\s+", " ", x)[:160]})
        return ev


class TopoTail(threading.Thread):
    def __init__(self, hub, host, parser):
        super().__init__(daemon=True)
        self.hub, self.host, self.p = hub, host, parser

    def run(self):
        hi = REDIS_HOSTS.index(self.host)
        sgs = (1, 2, 3) if hi < 3 else (4, 5, 6)
        files = " ".join(f"/tmp/redis_logs/{self.host}_sg{n}.log" for n in sgs)
        cmd = f"tail -n 20000 -F {files} 2>/dev/null | grep --line-buffered -aE {shlex.quote(TOPO_GREP)}"
        while True:
            try:
                p = subprocess.Popen(SSH + [self.host, cmd], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                                     stdin=subprocess.DEVNULL, text=True, errors="replace")
                sg = None
                for line in p.stdout:
                    line = line.rstrip("\n")
                    if line.startswith("==> "):
                        mm = re.search(r"_(sg\d+)\.log", line)
                        sg = mm.group(1) if mm else None
                        continue
                    if not sg:
                        continue
                    with self.hub.lock:
                        ev = self.p.parse(self.host, sg, line)
                    if ev:
                        self.hub.topo_event(ev)
                p.wait()
            except Exception as e:
                print("topo tail", self.host, repr(e), file=sys.stderr)
            time.sleep(4)


# --------------------------------------------------------------------------- HTTP
TOPO_PARSER = TopoParser()
HUB = Hub()
NODES = Nodes(HUB)
JOBS = None
WATCHDOG = None
TOKEN = ""   # when set, every request needs ?token=... once (then a cookie)


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def reply(self, code, body, ctype="application/json"):
        if not isinstance(body, (bytes, bytearray)):
            body = json.dumps(body, default=str).encode() if ctype == "application/json" else body.encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def authed(self, q):
        if not TOKEN:
            return True
        ck = self.headers.get("Cookie") or ""
        if f"panel_token={TOKEN}" in [c.strip() for c in ck.split(";")]:
            return True
        return q.get("token") == TOKEN

    def do_GET(self):
        u = urllib.parse.urlparse(self.path)
        q = dict(urllib.parse.parse_qsl(u.query))
        if not self.authed(q):
            return self.reply(401, "<h3>401: open the panel with ?token=... (printed when panel.py starts)</h3>", "text/html")
        if TOKEN and q.get("token") == TOKEN:
            self.send_response(302)
            self.send_header("Set-Cookie", f"panel_token={TOKEN}; Path=/; HttpOnly; SameSite=Strict; Max-Age=31536000")
            self.send_header("Location", u.path or "/")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        try:
            if u.path in ("/", "/index.html"):
                return self.reply(200, read(os.path.join(HERE, "index.html")), "text/html; charset=utf-8")
            if u.path == "/api/stream":
                return self.stream()
            if u.path == "/api/health":
                return self.reply(200, WATCHDOG.snapshot() if WATCHDOG else {})
            if u.path == "/api/runners":
                return self.reply(200, {"runners": runners(), "env_fields": ENV_FIELDS})
            if u.path == "/api/topology":
                return self.reply(200, topology(q.get("setup", "3to6")))
            if u.path == "/api/history":
                return self.reply(200, history())
            if u.path == "/api/run":
                d = q.get("dir", "")
                if not allowed_path(d):
                    return self.reply(403, {"error": "path not allowed"})
                return self.reply(200, run_detail(d))
            if u.path == "/api/ycsb":
                d = q.get("dir", "")
                if not allowed_path(d) or not os.path.isdir(d):
                    return self.reply(403, {"error": "path not allowed"})
                return self.reply(200, run_ycsb(d))
            if u.path == "/api/runfigs":
                d = q.get("dir", "")
                if not allowed_path(d) or not os.path.isdir(d):
                    return self.reply(403, {"error": "path not allowed"})
                return self.reply(200, run_figures(d, q.get("run")))
            if u.path == "/api/figzip":
                d = q.get("dir", "")
                if not allowed_path(d) or not os.path.isdir(d):
                    return self.reply(403, {"error": "path not allowed"})
                data = figures_zip(d, q.get("run"))
                name = re.sub(r"[^A-Za-z0-9_.-]+", "_", "figures_" + os.path.basename(d.rstrip("/").removesuffix("/data")) + ("_" + run_scenario(q["run"]) if q.get("run") else "")) + ".zip"
                self.send_response(200)
                self.send_header("Content-Type", "application/zip")
                self.send_header("Content-Disposition", f'attachment; filename="{name}"')
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
                return
            if u.path == "/api/file":
                p = q.get("path", "")
                if not allowed_path(p) or not os.path.isfile(p):
                    return self.reply(403, {"error": "path not allowed"})
                n = int(q.get("tail", "3000"))
                if p.endswith(".gz"):
                    return self.reply(200, {"path": p, "lines": ["(compressed file)"]})
                return self.reply(200, {"path": p, "size": os.path.getsize(p), "lines": tail_lines(p, n, 8_000_000)})
            if u.path == "/api/raw":
                p = q.get("path", "")
                if not allowed_path(p) or not os.path.isfile(p):
                    return self.reply(403, {"error": "path not allowed"})
                ext = p.rsplit(".", 1)[-1].lower()
                ct = {"png": "image/png", "pdf": "application/pdf", "svg": "image/svg+xml",
                      "html": "text/html", "csv": "text/plain", "md": "text/plain", "txt": "text/plain",
                      "json": "application/json"}.get(ext, "application/octet-stream")
                with open(p, "rb") as f:
                    return self.reply(200, f.read(), ct)
            if u.path == "/api/figures":
                d = q.get("dir", "")
                if not allowed_path(d):
                    return self.reply(403, {"error": "path not allowed"})
                figs = sorted(glob.glob(os.path.join(d, "figures", "*.png")), key=os.path.getmtime, reverse=True)
                return self.reply(200, [{"path": f, "name": os.path.basename(f), "mtime": os.path.getmtime(f)} for f in figs])
            return self.reply(404, {"error": "not found"})
        except Exception as e:
            return self.reply(500, {"error": repr(e)})

    def do_POST(self):
        u = urllib.parse.urlparse(self.path)
        n = int(self.headers.get("Content-Length") or 0)
        try:
            body = json.loads(self.rfile.read(n) or b"{}")
        except ValueError:
            return self.reply(400, {"error": "bad json"})
        if not self.authed({}):
            return self.reply(401, {"error": "not authorised"})
        if self.headers.get("X-Panel") != "1":   # a plain cross-site form cannot set this header
            return self.reply(403, {"error": "missing X-Panel header"})
        try:
            if u.path == "/api/health/check":
                res = check_hosts()
                bad, warn = judge(res)
                if WATCHDOG:
                    with WATCHDOG.lock:
                        WATCHDOG.health, WATCHDOG.t = res, now()
                return self.reply(200, {"ok": not bad, "blockers": bad, "warnings": warn, "hosts": res})
            if u.path == "/api/launch":
                j = JOBS.submit(body, body.get("mode", "queue"))
                return self.reply(200, j)
            if u.path == "/api/stop":
                jid, pid = body.get("job_id"), body.get("pid")
                job = JOBS.jobs.get(jid) if jid else None
                if job:
                    pid = job["pid"]
                if not pid or not alive(int(pid)):
                    return self.reply(400, {"error": "nothing to stop"})
                # Only runner trees: a panel job, or an external runner found by external_runs().
                ok = job or any(e["pid"] == int(pid) for e in external_runs(JOBS))
                if not ok:
                    return self.reply(403, {"error": "pid is not a runner"})
                pids = JOBS.stop(int(pid), job)
                return self.reply(200, {"killed": pids})
            if u.path == "/api/figures/make":
                d = body.get("dir", "")
                if not allowed_path(d) or not os.path.isdir(d) or not d.startswith(LIN_ROOT + "/"):
                    return self.reply(403, {"error": "path not allowed"})
                return self.reply(200, {"started": make_figures(d)})
            if u.path == "/api/queue/remove":
                with JOBS.lock:
                    jid = body.get("job_id")
                    if jid in JOBS.queue:
                        JOBS.queue.remove(jid)
                        JOBS.jobs[jid]["status"] = "cancelled"
                        JOBS.save(JOBS.jobs[jid])
                        JOBS.save_queue()
                return self.reply(200, {"queue": JOBS.queue})
            if u.path == "/api/queue/move":
                with JOBS.lock:
                    jid, d = body.get("job_id"), int(body.get("delta", 0))
                    if jid in JOBS.queue:
                        i = JOBS.queue.index(jid)
                        k = max(0, min(len(JOBS.queue) - 1, i + d))
                        JOBS.queue.insert(k, JOBS.queue.pop(i))
                        JOBS.save_queue()
                return self.reply(200, {"queue": JOBS.queue})
            return self.reply(404, {"error": "not found"})
        except (ValueError, RuntimeError) as e:
            return self.reply(409, {"error": str(e)})
        except Exception as e:
            return self.reply(500, {"error": repr(e)})

    def stream(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "keep-alive")
        self.end_headers()
        q = HUB.subscribe()
        try:
            while True:
                try:
                    kind, data = q.get(timeout=15)
                    msg = f"event: {kind}\ndata: {json.dumps(data, default=str)}\n\n"
                except queuemod.Empty:
                    msg = ": ping\n\n"
                self.wfile.write(msg.encode())
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass
        finally:
            HUB.unsubscribe(q)
            self.close_connection = True


def main():
    global JOBS, TOKEN, WATCHDOG
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--host", default="127.0.0.1", help="0.0.0.0 to reach it on the public IP (a token is then required)")
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--token", default=None, help="access token (default: kept in ~/.experiment_panel/token when --host is not 127.0.0.1)")
    a = ap.parse_args()
    if a.token is not None:
        TOKEN = a.token
    elif a.host not in ("127.0.0.1", "localhost"):
        tf = os.path.join(STATE, "token")
        TOKEN = read(tf).strip()
        if not TOKEN:
            import secrets
            TOKEN = secrets.token_urlsafe(16)
            os.makedirs(STATE, exist_ok=True)
            with open(os.open(tf, os.O_WRONLY | os.O_CREAT, 0o600), "w") as f:
                f.write(TOKEN)
    signal.signal(signal.SIGCHLD, signal.SIG_DFL)
    JOBS = Jobs()
    WATCHDOG = Watchdog(HUB, JOBS)
    WATCHDOG.start()
    FileFollower(HUB, JOBS).start()
    NODES.start()
    for h in YCSB_HOSTS:
        YcsbTail(HUB, h).start()
    for h in REDIS_HOSTS:
        TopoTail(HUB, h, TOPO_PARSER).start()
    srv = ThreadingHTTPServer((a.host, a.port), Handler)
    srv.daemon_threads = True
    shown = a.host if a.host not in ("0.0.0.0", "") else os.uname().nodename
    print(f"experiment panel on http://{shown}:{a.port}/{'?token=' + TOKEN if TOKEN else ''}  (repo {REPO}, state {STATE})", flush=True)
    srv.serve_forever()


if __name__ == "__main__":
    main()
