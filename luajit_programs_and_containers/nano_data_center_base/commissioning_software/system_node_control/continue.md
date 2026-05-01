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

## What's next: Phase B — first app port

Phase A's plan called Phase B "live cutover + first app port"; the cutover
happened in this session, so Phase B is now just the first app port.

### Phase B target: ros_mission_planner_ii (ONE container)

CORRECTION 2026-05-01: planner + UI are a SINGLE container, not two.
`building_blocks/ros_mission_planner_ii/container/` has one `Dockerfile`
(`FROM openresty-base`), one image
(`nanodatacenter/ros-mission-planner-ii:latest`), and TWO processes
inside it (`planner/` Lua worker + `planner_ui/` openresty on internal
:8080), both supervised by the chain-tree controller baked into
luajit-base. The gateway reverse-proxies the UI's port; the planner
process runs alongside but is not directly reachable from outside.

`thread_bridge/` in building_blocks is a Lua/C library (fnv1a hashing),
NOT a containerized service. Per `project_thread_bridge.md` memory, an
actual Thread-mesh NATS bridge container is planned separately; the
fnv1a code is scaffolding for that future container, not part of the
planner port.

So Phase B = port one container (`ros_mission_planner_ii`) from
`building_blocks/` to `nano_data_center_instance/app_containers/`.

This is the first real test of the base/instance split: the app needs its
KB-namespace root registered dynamically (per
`project_dcs_registry_integration.md` — REGISTER/DEREGISTER/RECONCILE
verbs already work) and its build script needs to source from base for
luajit_base/openresty_base while sourcing its own code from instance.

Open decisions before starting:
- Where does the app's KB-construction script live? Three options:
  1. In `nano_data_center_instance/app_containers/<app>/kb_script/` — collocated with the app.
  2. In `nano_data_center_instance/configurations/moon_base_alpha/kb_script/` — collocated with the site.
  3. Split: app-generic rows in option 1, site-overrides in option 2.
  Option 3 is the future-proof answer but option 1 is simpler if no site
  overrides exist yet.
- Does `build_kb.sh` need to accept `NDC_INSTANCE` (third source tree)
  and walk it as a third stage? Yes, and that is when option 3 above
  starts to matter.
- Is the bake-in vs volume-mount one-shot loader question still moot,
  or does Phase B force the decision? Probably forces it.

### Other pending items

- `support_procedures/runbooks/commissioning.md` — unwritten. Three modes:
  first-time, subsequent boot, re-commission.
- `kb/dsl/scripts/site_config.lua` — line 52 has a hardcoded fallback path
  pointing at `building_blocks/system_api/shell`. Defer until `system_api/`
  actually has content; currently a benign fallback that only fires when
  `SHELL_DIR` env is unset.
- `commissioning_software/orchestrator/` — copied as-is from
  `building_blocks/orchestrator/`. Not yet wired into anything in the new
  tree's runtime path; review whether it is still needed or superseded by
  `system_node_control/runtime/`.
- `nano_data_center_instance/` placeholders need real content before they
  are useful: `app_containers/`, `configurations/moon_base_alpha/kb_script/`.

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
