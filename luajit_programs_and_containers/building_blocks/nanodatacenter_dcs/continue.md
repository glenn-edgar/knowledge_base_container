# Nanodatacenter DCS — Continuation Plan

## State at end of 2026-04-28 session

**Phase 6 (sync layer rewrite + per-node restart + container RPC) is code-complete and pushed to origin/master.** 8 commits, range `42a9a09..66159e1f`:

```
66159e1f phase 6.4b: container-side client + supervisor integration -- closes Phase 6
09ba314d phase 6.4a: container-layer RPC -- master-side infra + smoke test (Option A, pg queues)
7073dbc8 phase 6.2: per-node infra restart -- recover pg-vector outage without teardown
25c727ec phase 6.1 cleanup: missed-ACK math + master self-row + stop.sh helper
a7feeb22 phase 6.1 acceptance fixes: LUA_PATH + slave first-HB nudge + steady-state keepalive
08a49b83 phase 6.1 followups: drop dead cluster_sync_bits helpers + add preflight
0387d719 phase 6.1: inter-CPU sync via pg-backed RPC queues
b4aff119 file-store smoke: add 3-case dedup test + fix cleanup gap
```

Acceptance results:
- 6.1 Tests 1–4: fresh boot ~10s, master kill+rejoin ~24s, slave kill+rejoin ~5s, both kill+rejoin ~8s.
- 6.2: `docker restart pg-vector` recovery ~8s, no slave fail-stop, no teardown.
- 6.3: free fallout of 6.1 + steady-state keepalive (verified via Test 2 with new missed-ACK math).
- 6.4a smoke: synthetic-client end-to-end PASS, budget max 0.057ms (878× headroom).
- 6.4b e2e: client transitions JOINING→ACTIVE in 2 ticks, KB row reaches ACTIVE, missed_acks stays 0.

## Operational follow-up (not blocking next session, but real)

1. **Rebuild `luajit-base` + apps** so live `test_app_01` / `dcs_console_01` / `observability_01` / `robot_manager_01` / `ros_mission_planner_ii_01` actually speak the 6.4b protocol. Until rebuilt, master's `container_state_<name>` rows for them stay `UNKNOWN`. NOT a cascade — master patiently waits forever (verified during 6.4a synthetic testing).
2. Real-cluster acceptance scenarios from `PHASE6_DESIGN.md §7.5` steps 2–5 (PAUSE-during-infra-restart, DRAIN-then-rm, controller-wedge → RESET_HINT). Need actual containers running the 6.4b client.
3. App-side hooks for PAUSE/RESUME — client tracks state, doesn't propagate to child processes; per-app policy.

## Next session: directory restructure (PRIMARY FOCUS)

Goal: replace flat `building_blocks/` with role-organized hierarchy. Memory: `project_directory_restructure.md`.

### Locked

- `ros_planner_ii_mqtt_robot/` is OUT of nano_data_center (separate effort).
- `t/` is stray; delete.
- `system_node_control` (= dcs.lua supervisor) is pure source under `commissioning_software/`. Stays host-process, NOT containerized.
- `docker_host_broker` belongs under `commissioning_software/infrastructure/`.

### Proposed top-level

```
nano_data_center/
├── app_containers/             # runtime Docker artifacts, one dir per image
│   ├── ros_planner_ii/
│   ├── ros_mission_planner_ii/
│   ├── ros_fleet_manager/
│   ├── robot_manager/
│   ├── ops_container/          # was dcs_console + nanodatacenter_gateway (merge?)
│   ├── observability/
│   ├── system_api/
│   └── thread_bridge/
├── commissioning_software/     # everything that BUILDS or PROVISIONS
│   ├── infrastructure/         # postgres, nats, mqtt, kv_bridge, docker_host_broker
│   ├── base_images/            # luajit_base, openresty_base
│   ├── engines/                # chain_tree, s_expression (was *_luajit)
│   ├── kb/                     # KB substrate + DSL (was knowledge_base/ + kb_dsl/)
│   ├── system_node_control/    # was nanodatacenter_dcs/ source
│   ├── orchestrator/
│   └── configurations/
│       └── <site_or_role>/     # NOT config_1 -- lock naming at start
│           ├── kb_script/      # KB construction inputs
│           ├── file_scripts/   # file-store seed
│           ├── master_node_data/
│           └── slave_node_data/
├── support_procedures/         # ops, runbooks, tests, monitoring
└── development/                # local-dev scratch (gitignored)
    ├── master_1/               # was deployment/cpu_01/
    └── slave_node_1/           # was deployment/cpu_02/
```

### Open decisions to lock at session start (before any `git mv`)

1. **Top-level**: rename `building_blocks` → `nano_data_center`, or nest? Recommend rename.
2. **`ops_container`**: merge dcs_console + gateway into one image, or keep separate? Recommend keep separate (different deps).
3. **Embedded targets** (`ros_planner_ii_c_cbor_robot`): new top-level `embedded_targets/`, lump under `app_containers/embedded/`, or under `commissioning_software/embedded/`? Recommend new top-level if more embedded coming.
4. **`configurations/` naming**: site-name (`moonbase_alpha/`), role (`dev_cluster/`), or sequence (`config_1/`)? Recommend site/role; `config_1` will rot.
5. **`scan_tree_luajit/`**: still alive or dead code? Verify before assigning a slot.
6. **`ros_planner_s_expression/`**: app or engine? Probably app since it's planner-domain; verify.

### Migration cost (real)

~50–100 path edits across:
- `docker_build.sh` files (`SCRIPT_DIR/../..` math)
- Construction subsystems (`BB_DIR/knowledge_base/postgres/data_structures/...`)
- `start.sh.template` `BB_ROOT` math (just landed in `25c727ec`)
- Dockerfile COPY directives
- Cross-directory `require()` statements
- LUA_PATH constructions

Plus a full `build_kb.sh` + `slice_bootstrap.sh` + cluster smoke pass to verify nothing dropped.

### Phasing recommendation

- **Step A (next session):** tree skeleton + `git mv` + path fixups + smoke pass on new layout. The session's work IS the restructure; no other goals.
- **Step B (session after):** luajit-base rebuild, real-container Phase 6.4 acceptance per design §7.5, closing operational follow-up #1 + #2 above.

Don't entangle restructure with rebuild work; both are individually large enough.

### Mapping reference (old → new)

| Current | Proposed location |
|---|---|
| `nanodatacenter_dcs/` | `commissioning_software/system_node_control/` |
| `nanodatacenter_gateway/` | `app_containers/ops_container/` (merged?) |
| `dcs_console/` | `app_containers/ops_container/` (merged?) |
| `docker_host_broker/` | `commissioning_software/infrastructure/docker_host_broker/` |
| `luajit_base/`, `openresty_base/` | `commissioning_software/base_images/` |
| `chain_tree_luajit/`, `s_expression_luajit/`, `scan_tree_luajit/` | `commissioning_software/engines/` |
| `knowledge_base/`, `kb_dsl/` | `commissioning_software/kb/` (kb_dsl/ → kb/dsl/) |
| `observability/`, `system_api/`, `thread_bridge/` | `app_containers/` |
| `orchestrator/` | `commissioning_software/orchestrator/` |
| `ros_planner_*/`, `ros_mission_*/`, `ros_fleet_*/`, `robot_manager/` | `app_containers/` |
| `ros_planner_ii_c_cbor_robot/` | embedded slot (TBD per Q3) |
| `ros_scripts/` | `commissioning_software/configurations/<config>/scripts/` |
| `start_planner_system.sh` | `support_procedures/runbooks/` |
| `ros_planner_ii_mqtt_robot/` | `../experiments/` (out of nano_data_center) |
| `t/` | delete |

## v3 7-step roadmap (where this fits)

| Step | Theme | Status |
|---|---|---|
| 1 | Solidify system/node-control RPC (Phase 6.1+6.2+6.3) | ✅ Done |
| 2 | Container base + RPC methods (Phase 6.4) | ✅ Code-complete; deployment is op follow-up |
| 3 | **Condense for build (this restructure + Pi target)** | 🎯 Next |
| 4 | KB-driven everything (file store, three-tier config, catalog hydration) | 🔲 |
| 5 | App-container build documentation | 🔲 |
| 6 | Log-analysis web UI by KB namespace tree | 🔲 |
| 7 | v1 done = soak-node + 30-day adversarial soak | 🔲 (the goal post) |

## Quick boot/teardown reference

```bash
# Boot cluster
( cd ~/.../nanodatacenter_dcs/deployment/cpu_01 && setsid nohup ./start.sh </dev/null >/dev/null 2>&1 & disown )
sleep 3
( cd ~/.../nanodatacenter_dcs/deployment/cpu_02 && setsid nohup ./start.sh </dev/null >/dev/null 2>&1 & disown )

# Stop cluster (works correctly; pkill -f does NOT — see feedback_pkill_pid_match memory)
~/.../nanodatacenter_dcs/deployment/cpu_01/stop.sh
~/.../nanodatacenter_dcs/deployment/cpu_02/stop.sh

# Rebuild after KB schema change
cd ~/.../nanodatacenter_dcs/construction
POSTGRES_PASSWORD=$(docker exec pg-vector printenv POSTGRES_PASSWORD) ./build_kb.sh
POSTGRES_PASSWORD=$(docker exec pg-vector printenv POSTGRES_PASSWORD) ./slice_bootstrap.sh
./stage_deploy.sh --mode=dev

# Phase 6 smoke tests (cluster must be up)
POSTGRES_PASSWORD=... luajit construction/tests/test_container_rpc_smoke.lua
CONTAINER_NAME=test_app_01 APP_CPU_ID=cpu_01 APP_SITE=moonbase.alpha.dcs \
  POSTGRES_PASSWORD=... luajit construction/tests/test_container_rpc_client_e2e.lua
```

## Memory pointers (relevant for next session)

- `project_phase6_complete.md` — what's done, what's deferred.
- `project_directory_restructure.md` — next-session plan (this doc's primary reference).
- `project_v3_platform_roadmap.md` — North Star.
- `feedback_pkill_pid_match.md` — kill via stop.sh, not pkill -f.
- `feedback_first_hb_nudge_pattern.md` — pattern reused in sync_rpc + container_rpc_client.
- `feedback_no_band_aid_over_architecture.md` — discipline guiding "lock the design first" sessions.
- `feedback_design_session_rhythm.md` — multi-question session rhythm.

---

*Previous continue.md (the 2026-04-20 directory reorg sketch for construction/runtime/deployment within `nanodatacenter_dcs/`) was superseded by the actual reorg landed in `8c3e08f9` and earlier commits. The next reorg is at the higher `building_blocks/` level — see "Next session" above.*
