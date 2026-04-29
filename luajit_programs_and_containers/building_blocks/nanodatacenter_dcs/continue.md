# Nanodatacenter DCS — Continuation Plan

## State at end of 2026-04-28 PM session

Phase 6 (sync layer rewrite + per-node restart + container RPC) is **code-complete and pushed to origin/master**. 8 commits, range `42a9a09..66159e1f`. Acceptance results green across 6.1 (4 tests), 6.2, 6.3, 6.4a, 6.4b. See `project_phase6_complete.md` memory for the detailed validation log.

**Operational follow-up still open**: rebuild `luajit-base` Docker image + apps so live containers actually speak the new 6.4b protocol. Until then, master's `container_state_<name>` rows for them sit at `UNKNOWN` (no cascade — verified during 6.4a synthetic testing). Real-cluster acceptance scenarios from `PHASE6_DESIGN §7.5` 2–5 also need real containers running the client.

This session was a **design session** (no code touched). Locked the directory restructure plan and answered five impact questions.

---

## PRIMARY FOCUS NEXT SESSION: directory restructure (base port)

### Top-level shape — TWO sibling repos (LOCKED)

Anchor: `~/knowledge_base_assembly/luajit_programs_and_containers/`

```
luajit_programs_and_containers/
├── building_blocks/                  # ORIGINAL — UNTOUCHED this port
├── nano_data_center_base/            # NEW repo #1 (platform, app-agnostic)
└── nano_data_center_instance/        # NEW repo #2 (site-specific)
```

**Locks:**
- Originals in `building_blocks/` are NOT moved or deleted. Live cluster keeps running off `building_blocks/` paths through the entire base port.
- Method = COPY (not `git mv`). `cp -a` from source into the new tree, then path-edit the copies.
- Cutover from `building_blocks/` to `nano_data_center_base/` is a separate, deliberate step in a later session, per-container.
- **First instance / site name = `moon_base_alpha`.**

### Wire-up: sibling-dirs + `NDC_BASE` env var (LOCKED)

| Option | Decision | Reason |
|---|---|---|
| Sibling dirs + `NDC_BASE` env var | **CHOSEN** | Simple, debuggable, no git complexity |
| Git submodule of base inside instance | Rejected | Submodule UX is rough; user is not a git expert |
| Symlink instance → base | Rejected | Breaks on Pi deploy unless careful |

`start.sh.template`, `docker_build.sh`, `LUA_PATH`, `BB_DIR`/`BB_ROOT` math all consume `NDC_BASE`.

### Base repo target layout (LOCKED)

```
nano_data_center_base/
├── luajit/
│   ├── luajit_base/                # was building_blocks/luajit_base/
│   └── openresty_base/             # was building_blocks/openresty_base/
├── commissioning_software/
│   ├── infrastructure/             # postgres, nats, mqtt, kv_bridge, docker_host_broker
│   ├── engines/                    # chain_tree (was chain_tree_luajit), s_expression (was s_expression_luajit)
│   ├── kb/                         # was knowledge_base/ + kb_dsl/  (kb_dsl/ → kb/dsl/)
│   ├── system_node_control/        # was nanodatacenter_dcs/  (host-process, NOT containerized)
│   ├── orchestrator/
│   └── validation/
│       └── test_app/               # was test_app/  (Phase 6.4b smoke target)
├── platform_containers/            # FLAT, 5 entries — core containers that control/analyze the DCS itself
│   ├── ops_container/              # was nanodatacenter_gateway/ (CONFIRMED separate from gateway/)
│   ├── dcs_console/
│   ├── gateway/                    # reverse proxy for federated microservice arch (CONFIRMED 2 containers, not 1)
│   ├── observability/              # core: analyze the DCS itself
│   └── system_api/                 # core: control the DCS itself
├── support_procedures/
│   └── runbooks/
│       ├── start_planner_system.sh # was building_blocks/start_planner_system.sh
│       └── commissioning.md        # NEW — write during base port (3 modes: first-time, subsequent boot, re-commission)
└── development/                    # gitignored
    ├── master_1/                   # was nanodatacenter_dcs/deployment/cpu_01/
    └── slave_node_1/               # was nanodatacenter_dcs/deployment/cpu_02/
```

### Instance repo target layout (LOCKED)

```
nano_data_center_instance/
├── app_containers/                 # EMPTY at end of base port
│                                   # First app port = ros_mission_planner_ii + thread_bridge (paired) — separate session
├── configurations/
│   └── moon_base_alpha/            # FIRST SITE NAME LOCKED
│       ├── kb_script/              # KB construction inputs
│       ├── file_scripts/           # file-store seed
│       ├── master_node_data/       # per-node site.json + secrets.env + bootstrap.db
│       └── slave_node_data/
└── development/                    # gitignored
```

### Excluded from this port (stay in `building_blocks/` untouched)

| Directory | Reason |
|---|---|
| `ros_planner_ii/` | Old platform; not coming back |
| `ros_planner_s_expression/` | Old platform; not coming back |
| `ros_planner_ii_c_cbor_robot/` | Old platform (embedded); not coming back |
| `scan_tree_luajit/` | Old platform; not coming back |
| `ros_planner_ii_mqtt_robot/` | Out of nano_data_center entirely (separate effort) |
| `ros_fleet_manager/` | App-tier — part of ros_mission_planner family; deferred to that app port |
| `ros_scripts/` | Status TBD — flag for verify, default = leave alone |
| `robot_manager/` | App-tier; deferred to its own per-app port |
| `thread_bridge/` | App-tier (Go MQTT→NATS bridge); ports paired with `ros_mission_planner_ii` |
| `ros_mission_planner_ii/` | App-tier; first app to port (with thread_bridge), separate session |
| `nanodatacenter_dcs/` | Source already covered — `system_node_control/` copies FROM here |

---

## Five impact-question answers from this session (architectural decisions)

### Q1: KB namespace IDs

**Answer**: namespace IDs are **logical strings in pg, unaffected by directory moves.** What the split does require:

- **Namespace ownership contract** (NEW: write `commissioning_software/kb/NAMESPACE_CONTRACT.md` during base port):
  - Base owns: `system_control.*`, `local_system_monitor.*`, `node_control.*`, `cluster_sync.*`, `kb_log.*`, `kb_rule.*`, `sys_exception.*`, `container_registry.*`.
  - Apps each own: their container-named root (`ros_mission_planner_ii.*`, etc.). One root per app.
  - Site/instance owns: `site.<name>.*` (e.g., `site.moon_base_alpha.*`).
- **Construction order**: `build_kb.sh` lives in base, takes `NDC_BASE` + `NDC_INSTANCE` env vars, walks both trees (option A — base orchestrates). Apps later add a third stage when ported.
- **Bootstrap.db slicing**: unchanged — slicer reads pg, repo-agnostic.
- **App-port collision**: prefer **dynamic** registration (apps register themselves at startup via container_registry — most infra already exists per `project_dcs_registry_integration.md`) over static container roster baked into base.

### Q2: Observability tree viewer folding namespaces

**Answer**: no fundamental problem, but forces **one design rule**:

> **Observability must use DYNAMIC namespace discovery, not a hardcoded list.**

Viewer queries pg with `SELECT DISTINCT subltree(path, 0, 1) FROM kb_log UNION ... FROM kb_rule UNION ... FROM sys_exception` and builds the tree from the result. Never ships with `local roots = {"system_control", "node_control", ...}`.

Phase-0 audit task: `grep -r` `building_blocks/observability/` for hardcoded namespace lists. If found → small refactor lands during base port.

Cosmetic: top-level groups (`Platform/`, `Site/`, `Apps/`) collapse the tree sensibly when 10+ namespaces accumulate.

### Q3: One-shot containers (file-store + KB loaders)

**Answer**: not a problem; **design rule = volume-mount, not bake-in.**

- **Bake-in (today, probably)**: scripts COPY'd into image. Two-repo split breaks this — one Dockerfile in base needs scripts from instance.
- **Volume-mount (recommended)**: image ships with loader binary only. At run time:
  ```
  -v $NDC_BASE/commissioning_software/kb/scripts:/scripts/base
  -v $NDC_INSTANCE/configurations/moon_base_alpha/kb_script:/scripts/site
  ```
  Container walks `/scripts/base/` first, `/scripts/site/` second, exits. Same image works any site, any base version. Re-runs after script edits = `docker run` again, no rebuild.

Phase-0 audit task: check `nanodatacenter_dcs/construction/Dockerfile*` and companion loaders. If bake-in → small Dockerfile refactor (15 lines) during base port.

### Q4: Air-gapped Docker via USB

**Answer**: yes, fully supported. Standard pattern is `docker save` → USB → `docker load`. Two-repo split makes air-gap deployment **easier** because incremental updates ship just one repo (e.g., new instance scripts) rather than the whole tree.

Five things to watch:
1. **USB stick ≠ USB3 SSD**. Pi runs from SSD; USB stick is just transport. Never put `/var/lib/docker` on a USB stick.
2. Bundle size ~2–5 GB total; modern USBs handle fine.
3. Image references must use **explicit local tags**, never `:latest` with implicit pull.
4. Single `bundle.sh` reading an `images.txt` manifest avoids missed images.
5. Version coherence: ship `VERSION` / `MANIFEST.txt` so target verifies it loaded the right snapshot.

Defer: `bundle.sh` + `air_gap_install.md` runbook land in `support_procedures/` when real Pi deployment approaches (v3 step 3-4). NOT next session's work.

### Q5: Commissioning workflow

**Answer**: user's mental model directionally correct. Three refinements + two missing steps:

**Refinements:**
1. **One-shot containers run on MASTER ONLY**, not every node. v1 is single-pg; slaves connect to master's pg over the network.
2. `system_control` is master-only at runtime; `node_control` runs everywhere. **Same source ships to every node**; `site.json` determines role.
3. PG data dir is master-only (`/opt/ndc/pg_data` bind-mounted on master's USB3 SSD).

**Missing steps:**
A. **Bootstrap.db slicing + per-node distribution**. After one-shots populate master's pg, run `slice_bootstrap.sh` on master → per-CPU bootstrap.db. Each node needs its sliced bootstrap.db delivered before its dcs.lua starts. Distribution mechanism (scp from master? HTTP? second USB pass?) — decision deferred.
B. **Network wiring**. Each slave's `site.json` must know master's pg endpoint; `/etc/hosts` entries or fixed IPs.

**Full commissioning order (lands in `support_procedures/runbooks/commissioning.md`):**

```
PER NODE (parallel):
  1. Boot Pi off USB3 SSD (Debian)
  2. Install Docker
  3. Plug in deployment USB; verify sha256
  4. docker load -i /mnt/usb/ndc-images.tar
  5. cp -a base/instance repos to /opt/ndc_*
  6. mkdir /opt/ndc/pg_data  (master only)
  7. Place per-node site.json (role, master_addr, NDC_BASE)
  8. Place secrets.env

ON MASTER ONLY:
  9. Bring up infrastructure: pg, nats, mqtt, kv_bridge, broker
  10. Run one-shot containers: kb_loader, file_store_loader (volume-mount scripts)
  11. slice_bootstrap.sh → per-CPU bootstrap.db files
  12. Distribute bootstrap.db to slaves

ON EACH SLAVE (after master ready):
  13. Bring up docker_host_broker (per-host)
  14. Place sliced bootstrap.db
  15. Start dcs.lua → reads bootstrap.db, JOIN_REQ via Phase 6 sync_rpc

ON MASTER:
  16. Start dcs.lua → master state machine, accepts JOIN_REQs

VERIFY:
  17. sys_ready=true, node_op=true cluster-wide
  18. All N nodes ACTIVE
  19. Platform containers up (5 of them)
```

Three commissioning modes for the runbook: first-time, subsequent boot (read existing bootstrap.db, no one-shot rerun), re-commission (KB schema change).

---

## Five-phase execution plan (next session)

### Phase 0 — pre-flight (BEFORE any cp)

1. **Resolve known unknowns:**
   - `gateway/` vs `ops_container/` — confirmed two containers (gateway = reverse proxy for federated microservice arch). ✅
   - `ros_fleet_manager/` — confirmed app-tier (part of ros_mission_planner family). ✅
   - `ros_scripts/` — alive-or-dead check; default = leave alone.
   - `infrastructure/postgres|nats|mqtt|kv_bridge` source location — likely embedded in `nanodatacenter_dcs/construction/`, NOT separate top-level dirs. **Verify before mapping.**
2. **Three audit checks:**
   - Are construction scripts split per-namespace, or one big lump? (Determines whether instance-tier rows split cleanly out of base.)
   - Does `building_blocks/observability/` hardcode namespace lists?
   - Do current one-shot Dockerfiles bake scripts in, or volume-mount?
3. **Tag** `pre-restructure-base-port` on `building_blocks/`'s git for rollback.
4. **Snapshot live cluster state**: `docker ps`, `docker images | grep nanodatacenter`. Save to `~/restructure_snapshot_<date>.txt`.
5. Decide dual-cluster strategy for Phase 4 smoke (`ndc_*` prefix on dev cluster vs prod-down).
6. Create the two empty roots; init each as its own git repo.

### Phase 1 — copy base tree (no edits)

`cp -a` per the source→target table:

| Source (under `building_blocks/`) | Target (under `nano_data_center_base/`) |
|---|---|
| `luajit_base/` | `luajit/luajit_base/` |
| `openresty_base/` | `luajit/openresty_base/` |
| `chain_tree_luajit/` | `commissioning_software/engines/chain_tree/` |
| `s_expression_luajit/` | `commissioning_software/engines/s_expression/` |
| `knowledge_base/` | `commissioning_software/kb/` |
| `kb_dsl/` | `commissioning_software/kb/dsl/` |
| `nanodatacenter_dcs/` | `commissioning_software/system_node_control/` |
| `orchestrator/` | `commissioning_software/orchestrator/` |
| `docker_host_broker/` | `commissioning_software/infrastructure/docker_host_broker/` |
| `test_app/` | `commissioning_software/validation/test_app/` |
| `nanodatacenter_gateway/` | `platform_containers/ops_container/` |
| `dcs_console/` | `platform_containers/dcs_console/` |
| (gateway source) | `platform_containers/gateway/` (locate; may be inside nanodatacenter_gateway/) |
| `observability/` | `platform_containers/observability/` |
| `system_api/` | `platform_containers/system_api/` |
| `start_planner_system.sh` | `support_procedures/runbooks/start_planner_system.sh` |

### Phase 2 — path edits in COPIES (originals untouched)

Estimate ~50–100 edit sites, but FUZZY (could be 30, could be 200; discover during execution). Concentrated in:

1. **Dockerfiles** — `COPY` directives rewritten against new tree.
2. **`docker_build.sh`** — `SCRIPT_DIR/../..` math → `NDC_BASE="${NDC_BASE:-$(cd "$SCRIPT_DIR/../../.." && pwd)}"`.
3. **`start.sh.template`** — add `NDC_BASE`; `BB_ROOT` becomes alias temporarily; delete later.
4. **LUA_PATH** — `building_blocks/knowledge_base/...` → `$NDC_BASE/commissioning_software/kb/...`.
5. **`require()` strings** — `kb_sync_queue` and friends.
6. **Construction subsystems** — `BB_DIR/knowledge_base/...` → `$NDC_BASE/commissioning_software/kb/...`.
7. **`build_kb.sh` / `slice_bootstrap.sh`** — read from `NDC_BASE` + `NDC_INSTANCE`.
8. **One-shot Dockerfiles** — refactor from bake-in to volume-mount IF current pattern bakes in.
9. **observability** — refactor to dynamic namespace discovery IF current pattern hardcodes.

### Phase 3 — instance stub

```
nano_data_center_instance/
├── app_containers/                 # README placeholder
├── configurations/
│   └── moon_base_alpha/
│       ├── kb_script/              # placeholder
│       ├── file_scripts/           # placeholder
│       ├── master_node_data/
│       │   └── site.json           # NDC_BASE + role=master + master_addr
│       └── slave_node_data/
│           └── site.json           # NDC_BASE + role=slave + master_addr
└── development/                    # gitignored
```

`site.json` follows three-tier-config pattern from `feedback_three_tier_config.md`.

### Phase 4 — validation smoke pass

1. Build base images from new tree (luajit_base, openresty_base, broker, validation/test_app, 5 platform containers).
2. Build KB from new tree: `NDC_BASE=... NDC_INSTANCE=... ./build_kb.sh && ./slice_bootstrap.sh`.
3. Stage to `development/master_1` + `slave_node_1`. Verify `start.sh.template` resolves all paths under `NDC_BASE`.
4. Run dev cluster from new tree. Acceptance: `sys_ready=true`, `node_op=true`, all 5 platform containers up, `test_app_01` healthy.
5. Live cluster (still on `building_blocks/`) keeps running unchanged. Two-cluster sanity: dev from `nano_data_center_base/development/`, prod from `building_blocks/nanodatacenter_dcs/deployment/`.
6. Phase 6 acceptance suite re-run against new layout. All green = base port done.

**Note**: per `feedback_user_driven_testing.md`, user runs DCS tests; assistant analyzes pasted logs. Phase 4 is user-paced.

### Phase 5 — commit + handoff (no live cutover)

1. Commit base repo: `restructure: initial base repo from building_blocks/`.
2. Commit instance repo: `restructure: initial instance stub with moon_base_alpha skeleton`.
3. Update memory + this continue.md → mark Phase A done; queue Phase B (live cutover + app ports).
4. Live cluster still on `building_blocks/`. Cutover is NEXT session's question, not this one.

---

## Honest plan assessment (end-of-session 2026-04-28)

### Confidence

| Dimension | Confidence | Note |
|---|---|---|
| Architecture correctness | 95% | Two-repo split is a clear improvement |
| Execution-as-one-session | 60% | Path edits + construction-script triage may push it |
| Execution-in-two-sessions | 90% | Checkpoint at "copy done, edits in progress" enables clean wrap-and-continue |
| First app port goes smoothly afterward | 70% | Depends on whether base port surfaces uncomfortable coupling |

### Two real execution risks

1. **"~50–100 path edits" estimate is fuzzy.** Real number could be 30 or 200. Discover during Phase 2. Mitigation: budget 1–2 sessions for Phase A, not strictly 1.
2. **Construction-script directory boundaries may not align cleanly with base/instance.** If a single script file holds both base-tier and (eventual) app-tier rows, splitting becomes per-statement. Won't know until Phase 0 audit. **Biggest unknown.**

### Three soft risks

3. Dual-cluster smoke (old + new on same dev box) — port/name/DB collisions. Mitigation: prefix dev cluster with `ndc_*` OR keep prod down. Decide at Phase 0.
4. observability refactor scope (dynamic namespace discovery) — could be small or larger.
5. Pi hardware vs WSL2 — first run on real Pi may surface platform bugs, but that's a later session.

### Pre-session optional checks (30 min)

If you have time before next session:
1. Spot-check `building_blocks/nanodatacenter_dcs/construction/` — script tree split by namespace/role, or one big lump? **This single answer changes Phase 0 difficulty from "20 min" to "2 hours."**
2. `grep -rn 'system_control\|node_control\|kb_log' building_blocks/observability/` — does observability hardcode namespace lists?
3. Confirm `nanodatacenter_gateway/` and `dcs_console/` are genuinely two separate Dockerfiles.

### Bottom line

**Plan is solid enough to execute against. Known unknowns are bounded. No hidden architectural traps. Worst realistic case: Phase A takes 2 sessions instead of 1.**

---

## v3 7-step roadmap (where this fits)

| Step | Theme | Status |
|---|---|---|
| 1 | Solidify system/node-control RPC (Phase 6.1+6.2+6.3) | ✅ Done |
| 2 | Container base + RPC methods (Phase 6.4) | ✅ Code-complete; deployment is op follow-up |
| 3 | **Condense for build (this restructure + Pi target)** | 🎯 IN PROGRESS — base port next session |
| 4 | KB-driven everything (file store, three-tier config, catalog hydration) | 🔲 |
| 5 | App-container build documentation | 🔲 (first app port = ros_mission_planner_ii defines the template) |
| 6 | Log-analysis web UI by KB namespace tree | 🔲 |
| 7 | v1 done = soak-node + 30-day adversarial soak | 🔲 |

---

## Quick boot/teardown reference (CURRENT building_blocks tree)

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

---

## Memory pointers (relevant for next session)

- `project_directory_restructure.md` — UPDATED 2026-04-28 PM with full two-repo design + 5 impact answers + risks.
- `project_phase6_complete.md` — what's done, what's deferred.
- `project_v3_platform_roadmap.md` — North Star.
- `feedback_pkill_pid_match.md` — kill via stop.sh, not pkill -f.
- `feedback_first_hb_nudge_pattern.md` — pattern reused in sync_rpc + container_rpc_client.
- `feedback_no_band_aid_over_architecture.md` — discipline guiding "lock the design first" sessions.
- `feedback_design_session_rhythm.md` — multi-question session rhythm.
- `feedback_three_tier_config.md` — JSON file (boot) / KB (live) / code (protocol). Drives `moon_base_alpha/site.json`.
- `feedback_user_driven_testing.md` — user runs tests; assistant analyzes pasted logs. Phase 4 smoke is user-paced.

---

## Bring-up checklist for next session

1. **Read in this order:**
   - This file (`nanodatacenter_dcs/continue.md`).
   - `project_directory_restructure.md` memory.
   - `feedback_three_tier_config.md` memory.

2. **Resolve open Phase-0 items BEFORE any cp** (see Phase 0 list above).

3. **Tag `pre-restructure-base-port`** on `building_blocks/`'s git for rollback.

4. **Verify cluster state still green:**
   ```bash
   docker ps --format "table {{.Names}}\t{{.Status}}"
   curl -sS http://127.0.0.1:9100/v1/health
   ```

5. **Then execute Phase 1 → 5 of the copy-and-fixup plan above.**

---

## End of 2026-04-28 PM session

Design fully locked. Plan written + risks named. No code touched. Originals safe. Next session: execute.
