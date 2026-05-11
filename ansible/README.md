# ansible/

Experiment harness for the aqueduct Redis project. Four experiment variants
form a 2×2 matrix on two axes — the Redis under test and whether a slot
reshard runs concurrently with the YCSB workload.

|                    | **baseline** (no reshard)        | **reshard** (concurrent slot migration) |
|--------------------|----------------------------------|-----------------------------------------|
| **vanilla redis**  | `experiments/vanilla_baseline/`  | `experiments/vanilla_reshard/`          |
| **aqueduct fork**  | `experiments/custom_baseline/`   | `experiments/custom_reshard/`           |

Each variant is one directory with the same four files:

```
experiments/<variant>/
  setup.yml      # one-time: kill, clean, build redis, build ycsb
  workload.yml   # per-workload: start cluster, load, monitor, run ycsb, collect
  teardown.yml   # one-time: kill + clean
  run_sweep.sh   # driver: loops setup → workload×N → teardown across YCSB workloads
```

## Running a variant

All ansible invocations need sudo (cluster SSH keys live in /root/.ssh).

```bash
cd /users/entall/rd/ansible

# Baseline against upstream Redis 8.6.2 across YCSB workloads a/b/c/f:
sudo ./experiments/vanilla_baseline/run_sweep.sh

# Just one workload:
sudo ./experiments/custom_reshard/run_sweep.sh workloada

# Override upstream Redis version (vanilla variants only):
sudo VANILLA_REDIS_TAG=8.6.3 ./experiments/vanilla_baseline/run_sweep.sh
```

Per-run results land in `/tmp/experiments/<variant>_<workload>/` on the
controller (YCSB output + mpstat/iostat/ifstat logs from each redis node).
Per-sweep ansible driver logs land in `/tmp/<variant>_sweep_<timestamp>/`.

## Variants in detail

### `vanilla_baseline`
Clones `https://github.com/redis/redis` at `vanilla_redis_tag` (default 8.6.2,
matches the fork's `REDIS_VERSION`) into `/users/entall/rd/redis-vanilla` and
installs into `/users/entall/rd/redis_bin_vanilla`. Stable 3-master cluster
(redis0/1/2); YCSB runs synchronously. Use this as the apples-to-apples
baseline for the aqueduct fork.

### `vanilla_reshard`
Same source/build as `vanilla_baseline`. After cluster create + YCSB load,
`add_migrate_nodes` brings `redis3` into the cluster and `reshard_cluster`
moves slots into it concurrently with an **async** YCSB run. Quantifies the
cost of upstream's reshard against the same workload.

### `custom_baseline`
Builds the aqueduct fork in place (`/users/entall/rd/redis/`). Stable cluster,
sync YCSB. Use this to confirm the fork doesn't regress steady-state
performance vs. vanilla.

### `custom_reshard`
The primary aqueduct experiment — fork redis with concurrent reshard,
async YCSB. Equivalent to the legacy `experiment.yml`.

## tasks/

Shared task playbooks, grouped by concern:

```
tasks/
  build/      build_redis_custom, build_redis_vanilla, clone_vanilla_redis, build_ycsb
  cluster/    start_redis_instances, create_cluster, add_migrate_nodes, reshard_cluster
  ycsb/       load_ycsb, run_ycsb_sync, start_ycsb_async, wait_ycsb
  monitor/    start_monitoring
  results/    collect_results
  teardown/   kill_all, kill_processes, clean_cluster, clean_runtime
```

`kill_processes` and `clean_runtime` preserve the built binaries (used
between iterations of a sweep). `kill_all` and `clean_cluster` wipe them
(used at sweep boundaries).

## templates/

`redis_cluster.conf.j2` is shared across variants. The fork-only
`cluster-rdma-allocator-*` directives are guarded by
`{% if redis_variant == 'custom' %}` — every sweep script passes
`-e redis_variant=...` so the right config renders for both variants.

## legacy/

Files superseded by the new structure. Kept for reference / git-history
convenience. Not maintained:

- `experiment.yml` — old monolithic orchestrator. Use `experiments/custom_reshard/`.
- `vanilla_{setup,workload,teardown}.yml`, `run_vanilla_sweep.sh` — use `experiments/vanilla_baseline/`.
- `playbook.yml` — abandoned bare-metal installer.
- `hello_world.yml` — debug stub.
