# <Title> (<YYYY-MM-DD>)

**Question:** what this experiment answers, in one sentence.

**Setup:** keys, workload, clients/threads, topology, relevant config flags
(`-e ...`), commit on `aqueduct_broken`.

**Result:** the headline number(s), with the figure that shows them.

## Layout
- `run.sh` — exact command that produced the runs (runs on the controller).
- `logs/<run>/` — raw output, fetched with `experiments/tools/fetch_logs.sh`
  (same layout as `/tmp/experiments/<run>` on the controller: `ycsb/`, `logs/<host>/`).
- `figures.sh` — rebuilds `figures/` from `logs/`.
- `figures/` — generated figures (do not edit by hand).
