# Nanodatacenter DCS — Continuation Plan

## State at end of 2026-05-01 session — Phase A DONE

The directory restructure (base port) is **complete and live**. The cluster
runs entirely from `nano_data_center_base/`. The previous tree at
`building_blocks/nanodatacenter_dcs/` is retained for historical reference
and is no longer used in production.

### Phase A acceptance results (green)

| Check | Result |
|---|---|
| 6 docker images built from new tree | luajit_base, openresty_base, observability, dcs_console, test_app, docker-host-broker |
| `build_kb.sh` against pg-vector | defs=9, cpus=2, instances=9, subsystems=16 |
| `slice_bootstrap.sh` per-CPU bootstrap.db | cpu_01=2014 rows, cpu_02=581 rows |
| `stage_deploy.sh --mode=dev` | runtime symlinks + env.sh seeded |
| `phase6_preflight.sh` | all checks passed |
| Master `cpu_01` boot | `sys_ready=true node_op=true` |
| Slave `cpu_02` boot + join | `node_op=true` |
| 5 DCS-managed containers up | test_app_01, observability_01, dcs_console_01, robot_manager_01, ros_mission_planner_ii_01 |
| Phase 6 `test_sync_rpc.lua` (14 tests) | ALL PASSED |
| docker-host-broker | heartbeat advancing, docker_socket_ok=true |

### Path edits made (Phase 2 final)

6 files, ~35 lines total:

| File | Change |
|---|---|
| `construction/build_kb.sh` | `REPO_ROOT/building_blocks/...` → `NDC_BASE/commissioning_software/kb/postgres/construct_kb` |
| `construction/slice_bootstrap.sh` | same pattern, `PG_KB_DIR` and `SQLITE_KB_DIR` |
| `construction/start.sh.template` | `BB_ROOT` deleted; replaced with `NDC_BASE`; subdir paths rewritten |
| `construction/phase6_preflight.sh` | 2 path refs to `NDC_BASE/commissioning_software/kb/postgres/...` |
| `luajit/luajit_base/container/docker_build.sh` | `BB_DIR` → `NDC_BASE`; `chain_tree_luajit/` → `engines/chain_tree/`; `nanodatacenter_dcs/` → `system_node_control/`; `knowledge_base/` → `kb/` |
| `platform_containers/observability/container/docker_build.sh` | same |

### Operational gotchas surfaced during smoke

1. **docker-host-broker bootstrap env vars** — three env vars are required, not
   all of them are in `WIRE_PROTOCOL.md`'s `docker run` example:
   - `SITE=moonbase.alpha.dcs` — must match `topology.site` EXACTLY (not
     `moonbase.alpha`); broker writes pg paths as
     `system.site.<SITE>.docker_broker.…` and dcs_host's `broker_client.lua`
     reads from the same path with `ctx.cfg.site` as the site segment.
   - `HTTP_ADDR=0.0.0.0:9100` — default `127.0.0.1:9100` only listens
     inside the container; host port mapping needs `0.0.0.0`.
   - `PG_DSN=host=pg-vector port=5432 dbname=knowledge_base user=… password=…` —
     without it, broker is NATS-only and master can't read the snapshot row.
2. **Test/live-cluster pg collision** — `test_sync_rpc.lua` operates on the
   same `knowledge_base_sync_msg__*` tables the live cluster uses. Running
   it against the live pg drops/recreates those tables and breaks the live
   cluster's prepared statements. Recovery: stop dcs.lua, re-run
   `build_kb.sh`, restart dcs.lua.

---

## What's next: Phase B — first app port (DESIGN LOCKED 2026-05-01)

The Phase B design session at end of 2026-05-01 locked the architecture
for app-container ports under the new tree. Concrete execution plan
below; full design rationale in `project_phase_b_design.md` memory.

### What Phase B builds

A general-purpose mechanism for app containers (planner, irrigation,
ETO, scheduler, future apps) to exist under a stable KB anchor that's
**independent of which CPU they run on**. CPU-independent namespace
enables load balancing — a container can move from cpu_02 to cpu_03
without changing its KB anchor.

### Locked architecture

**Canonical app anchor (fixed shape):**
```
system.<system_name>.site.<site_name>.app_containers.<container_name>.*
```

For this cluster: `system_name = moon_base`, `site_name = moon_base_alpha`.
Replaces the existing `system.site.moonbase.alpha.dcs.*` paths
cluster-wide. The `system.<sys>.site.<site>` shape is what gets
federated to the cloud, so it's a foundational migration, not Phase
B-only.

**Three sub-paths under each container's anchor:**

| Sub-path | Owner | When written |
|---|---|---|
| `app_containers.<c>.spec.*` | apps-builder one-shot at commission | once per recommission |
| `app_containers.<c>.placement.*` | site config (build_kb stage 2) | once per recommission; rewritten by load balancer |
| `app_containers.<c>.runtime.*` | the container itself at startup | every container start; heartbeat updated each tick |

**KB construction = one-shot apps-builder container (NOT laptop-side scripts):**

Each app ships a single `kb_build(ctx)` function in
`nano_data_center_instance/app_containers/<app>/kb_build.lua`.
Function declares schema scope-relatively (DSL has scope inheritance;
driver pushes `app_containers.<container_name>` scope before invoking).

A driver framework lives in
`nano_data_center_base/commissioning_software/apps_builder_framework/`.
The apps-builder image lives in
`nano_data_center_instance/commissioning_software/apps_builder/` and
composes the framework + per-app kb_build.lua functions at image
build time. It runs as a one-shot container at commission time,
walks the placement table, calls each declared container's kb_build,
and exits.

**Driver context (slim):**
```lua
ctx = {
  instance_id = "irrigation_controller_01",
  app_class   = "irrigation_controller",
  cpu         = "cpu_02",
  role        = "active",
  kb          = <DSL handle; scope already set to container's anchor>,
  read_kb     = <unscoped reader for cross-app discovery during build>,
}
```
No anchor string passed — the DSL inherits scope from the driver.
Function returns `ok, err`; driver fails atomically on first error.

**File tables (cross-container discovery):**
```
system.<sys>.site.<s>.app_containers.<c>.KB_FILE_TABLE.<table_name>.*
```
`KB_FILE_TABLE` is the class marker (analogous to `KB_STATUS_FIELD`,
`KB_STREAM_FIELD`). Other containers find file tables by ltree class
navigation: `WHERE path ~ '*.KB_FILE_TABLE.*'`.

**Web server routing under load balancing:**

Containers write `runtime.host`, `runtime.ui_port`, `runtime.cpu`,
`runtime.heartbeat_at` to their own anchor at startup and on every
chain-tree tick. `dcs_console`'s nginx shared_dict poller reads
`app_containers.*.runtime` every N seconds and populates a routing
dict keyed by container_name. Reverse-proxy directives look up the
dict at request time. When a container moves CPUs: old node stops it,
heartbeat goes stale (dict drops), new node starts it, container
writes new runtime row, dict picks it up next poll. Brief 503 window
during the gap; client retry succeeds on the new route.

`container_registry`'s REGISTER/DEREGISTER/RECONCILE verbs continue
to exist as semantic operations but write into the unified anchor's
`runtime.*` sub-path instead of a separate top-level namespace.

**Apps don't depend on each other at build time. Schema vs data are
decoupled:** apps' kb_build writes structure (schema, capabilities);
site config writes per-deployment data (specific zones, instance
counts, tunables). Different sub-paths; no coordination needed at
build time. Cross-app reads at runtime are the integration path.

### Phase B execution layers (in dependency order)

| Layer | Goal | Checkpoint |
|---|---|---|
| **M** — namespace migration | Add `system` segment everywhere; rename `moonbase.alpha.dcs` → `moon_base_alpha`. ~50–80 lines across topology.lua, path composers in dcs_host/, broker env, site.json. | Cluster boots green on `system.moon_base.site.moon_base_alpha.*`. |
| **F** — apps-builder framework (base) | `nano_data_center_base/commissioning_software/apps_builder_framework/` with driver.lua + dsl.lua + scoped_writer.lua + container_spec_validator.lua. Unit tests for scope confinement and atomic failure. | Framework + tests green; nothing wired into runtime yet. |
| **A** — port the planner (instance) | `git mv` planner from `building_blocks/` to `nano_data_center_instance/app_containers/ros_mission_planner_ii/`. Author its `kb_build.lua` + `container_spec.lua`. Rebuild planner image. | Planner image builds green; not yet started. |
| **I** — instance plumbing | `nano_data_center_instance/commissioning_software/apps_builder/` Dockerfile that composes framework + per-app functions. Build apps-builder image. Author placement row in site config. Wire into `build_kb.sh` and `rebuild_and_start.sh`. | Full pipeline green; pg has planner anchor populated under `app_containers`. |
| **N** — node_control reads placement from KB | RECONCILE compares `placement.cpu` against `runtime.cpu`; stop+start on mismatch. Remove per-CPU container list from topology.lua; bootstrap.db slicer simplifies. | Cluster boots; node_control on cpu_02 reads placement, finds planner assigned, starts it. |
| **V** — verify | Boot from scratch; verify anchor populated; verify UI proxied; verify cross-discovery query works. | Phase B done. Mark in continue.md, queue Phase B.2. |

**Locked execution choices:**
- **Compatible mode for A vs N**: A first (planner ports while node_control still reads bootstrap.db topology); N second (cut over to KB-driven placement after A is green). Smaller blast radius per checkpoint.
- **File-store loader deferred to Phase B.2**: file store has its own design surface (sha256 blobs, ltree paths, class registry). Phase B is already large.
- **Apps-builder rebuild is opt-in**: `rebuild_and_start.sh --apps-builder` rebuilds the image; not part of `--full` because apps don't change every run.
- **Audit bootstrap.db per-CPU container list during N1**: anything beyond container names (port allocations, dependencies) needs to migrate to placement KB rows too.

### Sequencing within a session

- Layer M is its own session (or first half of a session). Migration touches lots of files but is mechanical; finish + verify before opening F.
- Layers F + A + I can be a single session if the planner port goes smoothly; expect it to take 1–2 sessions.
- Layer N is its own session because of node_control refactor risk.
- Layer V is half a session (build + smoke).

Total: 3–5 sessions for Phase B as scoped above.

### Other pending items (unchanged from earlier)

- `kb/dsl/scripts/site_config.lua` line 52 — hardcoded fallback at
  `building_blocks/system_api/shell`. Benign; defer until `system_api/`
  has real content.
- `commissioning_software/orchestrator/` copied as-is from
  `building_blocks/orchestrator/`. Not wired into runtime; review for
  removal during Layer F or N.

---

## v3 7-step roadmap (where this lands)

| Step | Theme | Status |
|---|---|---|
| 1 | Solidify system/node-control RPC (Phase 6.1+6.2+6.3) | ✅ Done |
| 2 | Container base + RPC methods (Phase 6.4) | ✅ Done |
| 3 | **Condense for build (this restructure)** | ✅ DONE 2026-05-01 |
| 4 | KB-driven everything (file store, three-tier config, catalog hydration) | 🔲 |
| 5 | App-container build documentation | 🔲 (first app port = ros_mission_planner_ii) |
| 6 | Log-analysis web UI by KB namespace tree | 🔲 |
| 7 | v1 done = soak-node + 30-day adversarial soak | 🔲 |

---

## Quick boot/teardown reference (NEW tree)

```bash
export NDC_BASE=/home/gedgar/knowledge_base_assembly/luajit_programs_and_containers/nano_data_center_base
DEP=$NDC_BASE/commissioning_software/system_node_control/deployment

# Boot cluster
( cd $DEP/cpu_01 && setsid nohup ./start.sh </dev/null >/dev/null 2>&1 & disown )
sleep 3
( cd $DEP/cpu_02 && setsid nohup ./start.sh </dev/null >/dev/null 2>&1 & disown )

# Stop cluster (works correctly; pkill -f does NOT — see feedback_pkill_pid_match memory)
$DEP/cpu_01/stop.sh
$DEP/cpu_02/stop.sh

# Rebuild after KB schema change
cd $NDC_BASE/commissioning_software/system_node_control/construction
POSTGRES_PASSWORD=$(docker exec pg-vector printenv POSTGRES_PASSWORD) bash build_kb.sh
POSTGRES_PASSWORD=$(docker exec pg-vector printenv POSTGRES_PASSWORD) bash slice_bootstrap.sh
bash stage_deploy.sh --mode=dev

# Phase 6 smoke tests — WARNING: do not run against live cluster's pg.
# Tests drop/recreate knowledge_base_sync_msg__* tables and will break
# the running cluster's prepared statements.
```

---

## End of 2026-05-01 session

Phase A complete. Cluster running from `nano_data_center_base/`.
`building_blocks/nanodatacenter_dcs/` retained for historical reference,
no longer used. Next session: Phase B (first app port — `ros_mission_planner_ii`
+ `thread_bridge`).
