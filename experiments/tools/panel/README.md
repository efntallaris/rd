# Experiment panel

A web page on the controller (redis0) to launch, queue, stop, watch and replay the lincheck /
crash-suite / scale-out experiments, and to get their figures. It is one Python file using only
the standard library (`panel.py`), one page (`index.html`), and the figure script
(`figures_for_run.sh`).

## Run it

### As a service (starts at boot, restarts if it dies)

```bash
cd /users/entall/rd
experiments/tools/panel/install_service.sh          # install, enable, start; prints the URL
```

Run it as the user that runs the experiments, not with sudo. The runners call `sudo` themselves,
and the script asks for it only to install the unit. It writes
`/etc/systemd/system/experiment-panel.service` from `experiment-panel.service`, filling in the
user and the repo path. Anything listening on :8765 that was started by hand is stopped first.

```bash
systemctl status experiment-panel          # is it up
sudo systemctl restart experiment-panel    # after editing panel.py / index.html
tail -f ~/.experiment_panel/panel.log      # its log
experiments/tools/panel/install_service.sh --remove   # uninstall
```

Restarting or stopping the panel **does not stop a running experiment**: jobs run detached, and the
unit uses `KillMode=process`. The panel picks running jobs up again when it comes back. After
editing `index.html`, a reload in the browser is enough; edits to `panel.py` need the restart.

### On a new CloudLab cluster (automatic)

The profile's startup service runs `scripts_copy/install_preqs.sh` (from `master`) on every node,
as root, at every boot. Its last step checks out this branch in `/users/entall/rd`, then calls
`experiments/tools/panel/cloudlab_setup.sh`. On **redis0** that script installs and starts the
service for the owner of the checkout (`entall`). On the other nodes it does nothing.

The URL with the token is printed in `/var/tmp/startup-2.txt` and saved in
`~/.experiment_panel/url`. Because the startup service runs at every boot, the service is
reinstalled each time, which is harmless. To use another controller or user, set
`PANEL_CONTROLLER` / `PANEL_USER` for the script.

### By hand

```bash
cd /users/entall/rd
python3 experiments/tools/panel/panel.py                   # http://localhost:8765 only
python3 experiments/tools/panel/panel.py --host 0.0.0.0    # public, with a token (below)
```

Options: `--port` (default 8765) and `--token` (default: the token kept in
`~/.experiment_panel/token`, created the first time it binds to a non-local address).

### Open it

- **Public**: `http://<redis0 public IP>:8765/?token=<token>`. The service prints this URL, and the
  token is also in `~/.experiment_panel/token`. After the first visit the browser keeps the token in
  a cookie.
- **Local**: VS Code forwards the port, or use `ssh -L 8765:localhost:8765 redis0` and open
  `http://localhost:8765`.

Every request needs the token. A page that only shows data still lets anyone with the link start
and stop runs, so do not paste the link in public places.

## Use it

**Dashboard**: the run in progress, or the last run when nothing is running:
- the scenario chips (✓ / ✗), the playbook task in progress, and the crash_inject events;
- throughput (ycsb0 + ycsb1), read/update latency including failed reads, and a per-pair migration
  timeline (copy / chain / commit) from the server logs, all on one shared time axis;
- **zoom**: drag across any chart to zoom all three; double-click or "reset" to go back;
- **replay**: the player plays the run back (0.05×–2×), and click a chart to jump to that time;
- **Topology** (collapsed, "▸ Topology"): who holds which slots, the RDMA copy and chain hops as
  moving dots, leaders (★), crashed nodes (red ✕), the rounds each replica holds, and where an
  interrupted round comes from after a crash. Below it are the run.log / playbook.log / driver log.

**Experiments**: every run in `/tmp/lincheck/*` and every `experiments/*/data` folder, newest first,
with a search box and a kind filter:
- click a row for its scenarios, its figures (PNG + PDF, or all as a zip), and the files and node
  logs of each scenario;
- **replay** opens the same charts, timeline and topology for that run;
- **rerun / rerun all / rerun failed** fill the launch form with the same parameters.

**+ New experiment**:
- **Runner**: `custom_scaleout_3to6/run_all.sh`, `lincheck/run_all_lin.sh`, or an
  `experiments/*/run.sh` (that one also copies into `data/` and draws figures).
- **Scenarios** run in the order you click them.
- **Env** fields: blank uses the script's default, which is shown greyed.
- **Run now** refuses while the cluster is busy (`/tmp/cluster_in_use`, or any runner,
  ansible-playbook or crash_inject process, also ones started from a terminal). **Add to queue**
  starts the job when the cluster is free.
- Each job gets `OUT_ROOT=/tmp/lincheck/<utc ts>_<name>`, with the runner output in
  `panel_driver.log` and the parameters in `panel_job.json`.
- **Figures** are drawn after each scenario, in the house style, into `$OUT_ROOT/figures/`.
- **Stop** kills the runner tree (with sudo) and the YCSB / history clients on ycsb0/1, and clears
  a stale lock. The scenario in progress is lost, and redis instances are left running (the next
  run restarts them).

The 3->6 runner ignores `ELECTION_TIMEOUT_MS`. Pass `-e raft_election_timeout=500` in
`SCALEOUT_EXTRA_ARGS` instead.

## Host health and the watchdog

When a host hangs (SSH and redis stop answering), a job used to wait forever on its next ansible
task. The panel now checks every redis and YCSB host: SSH, free memory, swap, /tmp disk, and load.

- **Before a job starts**, it blocks the launch when a host does not answer SSH, has less than 16 GB
  of memory free (counting what old redis-servers would free), or has /tmp at least 95% full.
  "Run now" refuses with the reason, and the queue stays on hold until the hosts are fine.
  "skip health check" in the form overrides this.
- **While a job runs**, it checks every 30 s (every 2 min when idle), and stops the job by itself when:
  - a host has not answered for 5 min;
  - a redis host has less than 1.5 GB of memory left. It also kills redis-server there, so the host
    does not lock up;
  - nothing has happened for 20 min: no log line, YCSB sample, server-log event or file write.

  The reason is shown in Recent jobs and in the banner.

Problems show in a banner on every tab and as red header chips; hover a chip for its numbers.

The limits are env vars, set as `Environment=` lines in the unit or before `panel.py`. Examples:
`PANEL_UNREACHABLE_STOP_S=600`, `PANEL_STALL_STOP_S=1800`, `PANEL_BLOCK_MEM_GB=12`. See `LIMITS`
in `panel.py` for the full list. What the watchdog did is logged in
`~/.experiment_panel/watchdog.json`.

## Files and state

| | |
|---|---|
| `panel.py` | server: jobs and queue, SSE stream, log / YCSB / server-log tails, health watchdog, API |
| `index.html` | the whole page (no build step, no external scripts) |
| `figures_for_run.sh` | house-style figures of one scenario run: `figures_for_run.sh <run dir> [out root]` |
| `experiment-panel.service`, `install_service.sh` | start at boot (`--user <name>` when run as root) |
| `cloudlab_setup.sh` | CloudLab startup hook: runs `install_service.sh` on the controller only |
| `~/.experiment_panel/` | `jobs/*.json`, `queue.json`, `token`, `url`, `panel.log`, `watchdog.json` |
| `$OUT_ROOT/panel_data/` | snapshot of each scenario's `/tmp/experiments/<exp>`, which the next run would overwrite |

## Troubleshooting

- **Page does not load**: check `systemctl status experiment-panel` and the end of
  `~/.experiment_panel/panel.log`, and that port 8765 is reachable from where you are.
- **"health check failed"**: a host is down or full. The banner names it. A host that answers ping
  but not SSH is hung and needs a power cycle from the testbed console.
- **"cluster busy"**: something else holds the cluster. The status pill says what (lock file or
  process); stop it, or queue the job.
- **No throughput in a replay**: an old run whose `/tmp/experiments` copy was overwritten before
  the panel snapshotted it. The timeline and topology still come from the server logs.
