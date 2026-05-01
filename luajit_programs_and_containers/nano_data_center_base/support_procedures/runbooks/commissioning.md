# Commissioning Runbook — nano_data_center_base

Three modes:

1. **First-time** — brand-new node, no infra, no images, never run before.
2. **Subsequent boot** — clean restart after a stop. Nothing changed.
3. **Re-commission** — KB schema changed; rebuild KB and re-slice.

All three modes assume `NDC_BASE` is set (or that scripts climb correctly from
their location). The default for the dev laptop is:

```
export NDC_BASE=/home/gedgar/knowledge_base_assembly/luajit_programs_and_containers/nano_data_center_base
```

---

## Mode 1: First-time

Use this when the node has nothing yet. After this, you have a green cluster.

### 1a. Operating system + Docker

- Pi (target): Debian 12 / 64-bit booted off USB3 SSD.
- Dev laptop: WSL2 Ubuntu, Docker Desktop running.

### 1b. Operator secrets file (one-time per host)

```bash
mkdir -p ~/.config/nanodatacenter
cat > ~/.config/nanodatacenter/secrets.env <<'EOF'
PG_PASSWORD=<your pg-vector password>
EOF
chmod 600 ~/.config/nanodatacenter/secrets.env
```

This file is sourced by `build_kb.sh` and `slice_bootstrap.sh` so the
password never goes on the command line.

### 1c. Infra containers (pg-vector, nats, mosquitto, kv-bridge)

```bash
cd $NDC_BASE/commissioning_software/system_node_control/construction
bash install_infra.sh
```

Verify:

```bash
docker ps --format "table {{.Names}}\t{{.Status}}" | grep -E "pg-vector|nats|mosquitto|kv-bridge"
```

All four must be `Up`. The infra containers are managed by
`install_infra.sh`, NOT by `node_control` — they pre-date the chain-tree
supervisor by design (chicken-and-egg avoidance).

### 1d. Build all 6 platform images

```bash
$NDC_BASE/support_procedures/runbooks/rebuild_and_start.sh --full
```

This builds:
- `nanodatacenter/luajit-base:latest`
- `nanodatacenter/openresty-base:latest`
- `nanodatacenter/docker-host-broker:latest`
- `nanodatacenter/test-app:latest`
- `nanodatacenter/dcs-console:latest`
- `nanodatacenter/observability:latest`

…starts the broker, runs `build_kb.sh` + `slice_bootstrap.sh` +
`stage_deploy.sh`, seeds `POSTGRES_PASSWORD` into each per-CPU `env.sh`,
and boots `dcs.lua` on `cpu_01` (master) and `cpu_02` (slave).

Convergence target (visible in `tail -f $DEP/cpu_01/error.log`):

```
master cpu_01:  sys_ready=true   node_op=true
slave  cpu_02:  sys_ready=false  node_op=true   (sys_ready is master-only)
```

Plus 5 DCS-managed containers up: `test_app_01`, `observability_01`,
`dcs_console_01`, `robot_manager_01`, `ros_mission_planner_ii_01`.

### 1e. Smoke check

```bash
docker ps --format "table {{.Names}}\t{{.Status}}"
curl -sS http://127.0.0.1:9100/v1/health
```

Health endpoint should return `"status":"healthy"` with `"docker_socket_ok":true`.

---

## Mode 2: Subsequent boot

Use this when nothing changed and you just want the cluster running again
after a stop or reboot.

### 2a. Verify infra is up

```bash
docker ps --format '{{.Names}}' | grep -E "pg-vector|nats|mosquitto|kv-bridge"
```

If any are missing: `docker start <name>`. If they don't exist at all,
you're in Mode 1, not Mode 2.

### 2b. Start the cluster

```bash
$NDC_BASE/support_procedures/runbooks/rebuild_and_start.sh --start
```

Default mode. Ensures the broker is up (starts it if not), then boots
`dcs.lua` on both CPUs. If `bootstrap.db` is missing it will fall through
to the `--kb` path automatically.

### 2c. Stop the cluster

```bash
$NDC_BASE/support_procedures/runbooks/rebuild_and_start.sh --stop
```

Or directly:

```bash
$NDC_BASE/commissioning_software/system_node_control/deployment/cpu_01/stop.sh
$NDC_BASE/commissioning_software/system_node_control/deployment/cpu_02/stop.sh
```

⚠ Use `stop.sh`, NOT `pkill -f`. See `feedback_pkill_pid_match` —
`pkill -f` patterns containing path components silently no-op against
`bash ./start.sh`.

---

## Mode 3: Re-commission

Use this when the KB schema changed (catalogs/definitions.lua,
catalogs/topology.lua, or any subsystem in
`construction/subsystems/*.lua`). The KB needs to be rebuilt, sliced,
and re-staged. Image rebuilds are NOT required unless you also touched
chain-tree DSL or runtime Lua libs.

### 3a. Stop the cluster first

⚠ Critical. The `build_kb.sh` re-creates the
`knowledge_base_sync_msg__*` tables. Doing this with the cluster up
breaks the live cluster's prepared statements
(`feedback_test_db_isolation`).

```bash
$NDC_BASE/support_procedures/runbooks/rebuild_and_start.sh --stop
```

### 3b. Rebuild + restart

```bash
$NDC_BASE/support_procedures/runbooks/rebuild_and_start.sh --kb
```

This: stops cluster (idempotent), runs `build_kb.sh`, runs
`slice_bootstrap.sh`, runs `stage_deploy.sh --mode=dev`, restarts
`dcs.lua` on both CPUs.

### 3c. If you also rebuilt images

If you also touched `runtime/dcs_host/*.lua`,
`engines/chain_tree/runtime/*.lua`, or anything baked into `luajit_base`
via its `docker_build.sh`, do the full path:

```bash
$NDC_BASE/support_procedures/runbooks/rebuild_and_start.sh --full
```

This is Mode 1d minus the install_infra step.

---

## Useful runtime checks

Live tail master:

```bash
tail -f $NDC_BASE/commissioning_software/system_node_control/deployment/cpu_01/error.log
```

Master state-machine and chain-tree diagnostics in pg:

```bash
PG_PW=$(docker exec pg-vector printenv POSTGRES_PASSWORD)
PGPASSWORD=$PG_PW psql -h localhost -U gedgar -d knowledge_base \
    -c "SELECT path FROM knowledge_base_status WHERE path::text LIKE '%docker_broker%' LIMIT 5;"
```

Broker health + container snapshot:

```bash
curl -sS http://127.0.0.1:9100/v1/health
curl -sS http://127.0.0.1:9100/v1/state/containers | head -50
```

Pre-flight before a `build_kb.sh` (catches stale staging, missing modules,
prepared-statement issues):

```bash
bash $NDC_BASE/commissioning_software/system_node_control/construction/phase6_preflight.sh
```

---

## Recovering from common failure modes

### "relation knowledge_base_sync_msg__master_q does not exist"

Live cluster's prepared statements are pinned to dropped table OIDs
(usually because someone ran `test_sync_rpc.lua` or some other smoke
test against the live pg). Recovery:

```bash
$NDC_BASE/support_procedures/runbooks/rebuild_and_start.sh --kb
```

### Master cycling on `ERR_INFRA_FAIL: snapshot read ... row not found`

The docker-host-broker isn't writing to the path the master is reading.
Cause is almost always a `SITE` env var mismatch. Required broker env
(see `feedback_broker_bootstrap_env`):

- `SITE=moonbase.alpha.dcs` — must match `topology.site` exactly.
- `HTTP_ADDR=0.0.0.0:9100` — default `127.0.0.1` won't route from host.
- `PG_DSN=...` — without it, broker is NATS-only, master can't read.

The `rebuild_and_start.sh --start` path uses the canonical run command;
if you're starting the broker manually, copy from there.

### `docker exec pg-vector printenv POSTGRES_PASSWORD` returns empty

You're on a fresh host where pg-vector was started without
`POSTGRES_PASSWORD`. Fix the password before retrying — see
`install_infra.sh` and `setup_secrets.sh`.

---

## What's NOT in this runbook (yet)

- **Air-gapped Pi deployment** via `docker save`/`docker load` over USB.
  Deferred until first real Pi deploy approaches (v3 step 3-4); covered
  in `project_directory_restructure` memory's Q4 answer and the v3
  roadmap.
- **Multi-node distribution** of bootstrap.db (master ships per-node
  bootstrap.db to each slave). Currently dev cluster runs both CPUs on
  the same host so `slice_bootstrap.sh` puts both bootstrap.dbs in
  `commissioning_software/system_node_control/deployment/cpu_*/`.
  Distribution mechanism (scp from master? HTTP? second USB?) not yet
  decided — call out at first multi-host commissioning.
- **Site-tier KB rows** from `nano_data_center_instance/`. Currently
  `build_kb.sh` only walks base; instance-tier integration lands with
  Phase B (first app port).
