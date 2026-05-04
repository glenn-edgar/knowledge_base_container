# Nanodatacenter DCS — Continuation Plan

## State at end of 2026-05-03 session — Layers O / F / A-pre DONE; Layer A planning locked

Three holding commits today, all independently revertible per the
holding-commit discipline (`feedback_holding_commits` memory). Cluster
ACTIVE/ACTIVE on the new namespace; 4/4 infra services advertised
through KB-driven discovery.

| Commit | Layer | Scope |
|---|---|---|
| `226b17c1` | **O** | observability tree control: log_web `/tree` + exception_web `/tree`. Recursive `<details>/<summary>` over the KB ltree namespace; pure CSS. log_web 71 leaves, exception_web 163 leaves. |
| `9486e8d3` | **F** | apps-builder framework (driver + ctx + spec validator) + 42/42 unit tests. Sqlite construct_kb gained `with_header` / `with_kb` parity with pg. No cluster impact yet. |
| `2298f0ae` | **A-pre** | KB-driven infra discovery for NATS/MQTT/pg/kv-bridge. `service_contract` block on each infra def; `infrastructure_registry` subsystem pre-allocates schema; system_control's `INFRA_PUBLISH` chain-tree column writes runtime addressing every 5s; `infra_discovery.lua` helper in luajit_base provides `lookup()` / `nats_url()` / `mqtt_addr()`. e2e green from a fresh container. **Bundled fix:** stale `build_dsl.sh` path post-Phase-A restructure. |

### Layer A planning (in progress; spec contents fully decided)

**Mission planner port** — replaces the existing `ros_mission_planner_ii_01`
shell with a real port under the new framework. Class = `mission_planner`.
ONE container, TWO supervised processes (planner Lua worker + planner_ui
OpenResty), running under the standard `luajit_base` supervisor — same
base every other app uses, no bespoke startup.

**`manifest.lua` LOCKED** (file lives at
`nano_data_center_instance/app_containers/mission_planner/manifest.lua`,
mirrored to `app_containers.mission_planner_01.spec.*` by `kb_build.lua`):

| KB row class | Field | Value |
|---|---|---|
| KB_STATUS_FIELD | `version` | `"1.0"` |
| KB_STATUS_FIELD | `class`   | `"mission_planner"` |
| KB_JSONB_FIELD  | `capabilities`  | `[path_planning, energy_budget, transit, drive_base]` |
| KB_JSONB_FIELD  | `virtual_nodes` | `[init_check, path_spline, path_line, operation, idle, error_recovery]` |
| KB_JSONB_FIELD  | `wire_formats`  | `[json, cbor]` |
| KB_JSONB_FIELD  | `ui_protocol`   | port_internal=`8090`, scheme=`http`, 6 endpoints (landing + submit_mission + list_missions + mission_status + list_robots + blackboard_view) |
| KB_JSONB_FIELD  | `nats_protocol` | port=`4222`, 6 subjects per audit (mission_submit/status/result/log + robot_status_kv + blackboard_kv) |
| KB_JSONB_FIELD  | `mqtt_protocol` | port=`1883`, 7 topics per audit (rpc + stream_bus + link + 4 status sub-topics) |

**Storage rationale:** scalars use KB_STATUS_FIELD (single value, queryable
individually); structured catalogs use KB_JSONB_FIELD (knowledge_base_document
table) — querying via JSONB ops (`data ? 'energy_budget'` for set membership)
is cleaner than fanning ~30 fields into individual status rows.

**Locked design choices for the port:**
- `ui_protocol` is a **first-class peer** of NATS/MQTT — external schedulers /
  monitors can use HTTP without a NATS client. Six endpoints; every NATS subject
  has an HTTP equivalent where reasonable (`mission_submit` ↔ POST
  `/api/missions`).
- WebSocket / SSE / auth deferred (auth requires sidecar/identity design).
- `streams = {}` for v1.0 — planner stays NATS-only for runtime state. Adding
  KB-resident streams later bumps to v1.1 once we know what's worth persisting.
- Robot classes deferred to future robot_manager port — planner cross-discovers
  via `WHERE path ~ '*.spec.KB_JSONB_FIELD.robot_classes'` once that lands.
  For Layer A, planner uses a stop-gap stub (Q2.2 below).
- All NATS/MQTT broker addressing goes through `infra_discovery.lookup()`
  (Layer A-pre) — no env-var injection chain for these.

### Open Layer A planning items (resolve before code)

**Q2.1 — `kb_query.lua` strategy. LOCKED 2026-05-04: refactor in place.**
Source is `third_party_containers/ros_planner/lua/kb_construct/kb_query.lua`
(338 lines; the `building_blocks/ros_planner_ii/hub_dsl/...` copy is
byte-identical and goes away with the planner port). Original estimate
of "20–30% changes" undersold the work — actual scope is ~50% diff once
the data-layout shift (scattered per-node rows → JSONB blobs under
`spec.KB_JSONB_FIELD.*`) is counted, but refactor still beats fresh
because `parse_row`'s pcall+fallback, the KBM read-only ctor
(`upload_flag=true`), and the `get_site_config` aggregation contract
are soak-tested and worth keeping. `git mv` to preserve blame.

**Concrete plan:**
- `git mv` source → `nano_data_center_instance/app_containers/mission_planner/lua/kb_query.lua`.
- **Keep** (~15%): `parse_row`, KBM ctor, `close`, `get_site_config` aggregation skeleton.
- **Simplify** `get_site()` (lines 78–94): drop depth-0 / domain auto-detect; read `bootstrap.config.system_name` + `bootstrap.config.site` directly.
- **Delete** three v2-only helpers, no replacement needed:
  - `get_infrastructure()` (212–225) — superseded by A-pre's `infra_discovery.lookup()`.
  - `get_container_config()` (230–237) — no equivalent under `app_containers.*`.
  - `get_domain()` (243–249) — subsystems/domain row obsolete; system_name/site live in bootstrap.config.
- **Add** generic reader `get_app_spec(container_name, key)` that reads `app_containers.<c>.spec.KB_JSONB_FIELD.<key>` and returns the decoded JSONB blob. Cached.
- **Rewire surviving getters** through the new shape:
  - `get_planner_state()` → `app_containers.ros_mission_planner_ii.runtime.planner_state`.
  - `get_virtual_node*` → `get_app_spec("ros_mission_planner_ii", "virtual_nodes")` (manifest already declares this).
  - `get_class_*` / `get_robot_infra` / `list_robot_classes` → thin facade over `get_app_spec("robot_manager", "robot_classes")`. Actual source (real spec vs. stub) decided by Q2.2.
- **Drop** `boards` methods entirely — no v3 owner; re-add when one ports.
- All path strings go through `ndc_paths` composer; no literals (M-1 discipline).

**Q2.2 — robot classes during the port. LOCKED 2026-05-04: option (i) —
hardcode fallback inside `kb_query.lua`.** A single `FALLBACK_ROBOT_CLASSES`
constant in the file, engaged only when `get_app_spec("robot_manager",
"robot_classes")` returns nil. One class entry: `drive_base`, with
capabilities mirroring the drive-base app catalogue (memory
`project_drive_base_catalogue`) — not fictional data, a frozen snapshot
of what robot_manager will eventually publish for drive_base.

Rejected (ii) `manifest_robots.lua`: separate file adds build-script
awareness + multi-place removal for zero functional gain over a constant.
Rejected (iii) port robot_manager: doubles Layer A scope, out per design.

**Why one class is enough for Layer V:** `rejected_no_robot` is determined
by the runtime registry being empty (no robot announced via link protocol),
NOT by class lookup. Stub exists only so `list_robot_classes()` and
startup logging aren't empty during boot.

**Removal path:** when robot_manager ports under
`app_containers.robot_manager.spec.KB_JSONB_FIELD.robot_classes`, the real
path starts succeeding, fallback becomes unreachable, single-place
deletion in `kb_query.lua`. TODO comment in code names the future path
verbatim so the link is searchable.

**Q2.3 — planner_ui ↔ planner_worker decoupling. LOCKED 2026-05-04: yes,
fully decoupled. The two processes are bundled in one container for
operational convenience only — they have no IPC and no shared state at
the process level.** Both read pg; both speak NATS. UI is the HTTP
surface; worker is the runtime engine. Mission submit flow: UI POST
`/api/missions` → NATS publish to `{site}.action_server.missions` →
worker consumes → writes status to pg → UI displays via pg read or NATS
subscription. No localhost reverse-proxy from UI to worker, no Unix
sockets, no FIFOs, no shared memory, no signal coordination.

**Concrete porting obligation:** the planner port's `nginx.conf` listens
on its own port (`ui_protocol.port_internal=8090` per manifest) and
references no other internal port. Future "UI needs worker data" cases
go through pg or NATS, not a sidecar HTTP call. This keeps the option
open to split UI into its own container later with zero protocol change
(both pg and NATS are already discovered via `infra_discovery.lua`).

**Q3 — Layer V acceptance test. LOCKED 2026-05-04: tests BOTH rejection
and completion paths, using the existing
`building_blocks/ros_planner_ii_mqtt_robot/` as a Layer V fixture.**
Robot is KB-independent (config = local JSON file; talks MQTT topics
+ link protocol; no pg reads, no NATS KV consultation), so it runs
from `building_blocks/` with only `site = "moon_base_alpha"` +
mqtt_host/port pointing at the v3 broker. No port required.

**Pre-conditions (programmatic, run before NATS scenario):**
1. pg row exists at `app_containers.ros_mission_planner_ii.spec.KB_STATUS_FIELD.version = "1.0"` AND ≥2 manifest KB_JSONB_FIELD blobs (`capabilities`, `nats_protocol`) decode cleanly. Proves apps-builder ran + manifest landed + spec validator passed.
2. `app_containers.ros_mission_planner_ii.runtime.heartbeat_at` <10s old. Proves planner_worker is alive and writing the runtime row dcs_console's routing dict consumes.
3. `GET http://localhost:<ui_port>/` returns 200. Proves planner_ui process started under the supervisor (luajit_base two-process pattern works for this app).

**Phase 1 — rejection (robot offline):**
- Test client publishes mock mission for class `drive_base` to `{site}.action_server.missions`.
- Observe status KV: `submitted` → `rejected_no_robot`.
- Proves: NATS subscribe works, capability lookup finds drive_base via Q2.2 fallback, runtime registry consulted and empty.

**Phase 2 — completion (robot online; the headline integration test):**
- Start `ros_planner_ii_mqtt_robot` from `building_blocks/` with config: `site=moon_base_alpha`, `mqtt_host`/`mqtt_port` from `infra_discovery.lookup("mqtt_broker")`, `robot_class=drive_base`, capabilities matching the FALLBACK stub.
- Robot connects → MQTT retained `{site_path}/robots/{id}/status/state` shows `connected=true`.
- Test client publishes another mission.
- Observe: `submitted` → `dispatched` → `in_progress` → `completed`.
- Proves: planner→MQTT→robot dispatch, status state machine progresses, runtime registry populated by link-protocol announce, no v2→v3 behavioral regression.

**Optional (cheap, strong signal):**
- Rejection-on-unknown-class: submit mission for `nonexistent_class` while robot online. Expected: `unknown_class`, NOT `rejected_no_robot`. Distinguishes the two failure modes.

**Out of scope (defer to Phase B.2 or later):**
- Manual web-UI testing. Historical surface_ops_ui mission-launcher methodology is not portable to v3 (current `planner_ui` is a placeholder shell). UI smoke goes in a separate layer when real endpoints land.
- HTTP `POST /api/missions` and the other 5 UI endpoints.
- Hardware robot, multi-robot contention, energy edge cases (v2 already proved planner internals).

### **First action next session — soak verification + decide Q2.1/Q2.2**

```bash
export NDC_BASE=/home/gedgar/knowledge_base_assembly/luajit_programs_and_containers/nano_data_center_base
DEP=$NDC_BASE/commissioning_software/system_node_control/deployment

# soak verification — 5-min check (per feedback_soak_cadence)
pgrep -af "dcs\.lua" | grep -v claude
docker exec pg-vector psql -U gedgar -d knowledge_base -tAc \
  "SELECT data->>'state' FROM knowledge_base_status WHERE path::text LIKE '%peer_state%' ORDER BY path"
docker exec pg-vector psql -U gedgar -d knowledge_base -tAc \
  "SELECT count(*) FROM knowledge_base_status WHERE path::text ~ 'SYS_EXCEPTION' AND (data->>'status')::boolean = true"
# infra registry should still be advertising 4/4 healthy
docker exec pg-vector psql -U gedgar -d knowledge_base -tAc \
  "SELECT path::text, data FROM knowledge_base_status \
   WHERE path::text LIKE '%infrastructure.registry.%KB_STATUS_FIELD.healthy' ORDER BY path"
# tail INFRA_PUBLISH log
grep "INFRA_PUBLISH:" $DEP/cpu_01/error.log | tail -5
```

If green: resolve Q2.1 (refactor vs. fresh) + Q2.2 (which stub strategy)
+ Q3 confirm, then start Layer A code.

Rollback recipe (any of today's holding commits): `git revert <hash>` —
each is independently revertible.

### Layer ordering

```
M-2 ✅ → soak ✅ → O ✅ → F ✅ → A-pre ✅ → A → I → N → V
                                        ↑ next
```

Phase B remaining: 3.5 sessions (A, I together; N standalone; V smoke).
After Phase B: v3 roadmap step 4 (KB-driven file store, three-tier
config) onward.

---

## State at end of 2026-05-02 session — Phase B Layer M-2 DONE — namespace migration complete

Phase B Layer M (namespace migration) is **fully done**. M-1 centralized path
composition; M-2 today flipped the path shape from `system.site.<S>.*` →
`system.<sys>.site.<S>.*` and renamed `moonbase.alpha.dcs` → `moon_base /
moon_base_alpha`. Cluster is live on the new shape and 5-min soak is green.

### What landed today (1 commit, 28 files, +375/-115)

| Commit | Scope |
|---|---|
| `9c2daee1` | Phase B Layer M-2: rename + system_name segment + KB DSL `path_prefix` (the structural enabling change discovered mid-session) |

**Headline structural change beyond the original M-2 scope.** The KB DSL
walker emits paths starting at the kb_name; with kb_name = `"system"` it
produced `system.site.<S>.*` (legacy), bypassing `ndc_paths`. Solution:
`construct_kb.lua:add_kb()` (postgres + sqlite) gained an optional
`path_prefix` argument that initializes the path stack to a multi-segment
root. `build_kb.lua` calls `add_kb("system", "...", { "system", SYSTEM_NAME })`
so paths now root at `system.<sys>.*` while the logical kb_name stays
`"system"` — every `WHERE knowledge_base = 'system'` filter in production
code keeps matching. `check_installation` was updated to compare against the
remembered prefix instead of the legacy `{ kb_name }` baseline.

Containers also gained an `APP_SYSTEM` env var (parallel to `APP_SITE`).
node_control's container launcher injects it; the supervisor and platform
containers (observability + dcs_console) configure `ndc_paths` from it at
module load. `nginx.conf` adds `env APP_SYSTEM;` in the admin + gateway
slots.

### Today's rebuild + soak

| Step | Result |
|---|---|
| 5 platform images rebuilt (luajit_base, openresty_base, observability, dcs_console, docker_host_broker) | green |
| First `build_kb.sh` attempt | rows still at legacy shape — discovered DSL walker bypasses `ndc_paths` |
| `add_kb` `path_prefix` fix + re-build | 4477 rows under `system.moon_base.site.*`, 18 under `system.moon_base.container_definition.*`, **0 rows at legacy `system.site.*`** |
| `slice_bootstrap.sh` | cpu_01=2014 rows, cpu_02=581 rows (matches Phase A baseline) |
| `stage_deploy.sh --mode=dev` | runtime symlinks + env.sh preserved |
| `phase6_preflight.sh` | all checks PASS |
| Broker re-run with `-e SYSTEM=moon_base -e SITE=moon_base_alpha` | pgwriter connected, healthcheck OK |
| Boot cpu_01 master + cpu_02 slave | identity log = `system=moon_base site=moon_base_alpha`, exactly 1 dcs.lua per CPU |
| 5-min soak health check | `system_ready={"value":1}`, both peers ACTIVE under new path shape, heartbeats ~2-3s old, zero SYS_EXCEPTIONs, zero panics in error.log |

### **First action next session — verify soak survived overnight**

```bash
export NDC_BASE=/home/gedgar/knowledge_base_assembly/luajit_programs_and_containers/nano_data_center_base
export PGPASSWORD=$(docker exec pg-vector printenv POSTGRES_PASSWORD)
DEP=$NDC_BASE/commissioning_software/system_node_control/deployment

# 1. processes still 1 dcs.lua per CPU?
pgrep -af "dcs\.lua" | grep -v claude

# 2. peers still ACTIVE under NEW path shape?
docker exec pg-vector psql -U gedgar -d knowledge_base -tAc \
  "SELECT path::text, data FROM knowledge_base_status \
   WHERE path::text LIKE '%peer_state%' OR path::text LIKE '%system_ready' \
   ORDER BY path"

# 3. heartbeat freshness (age_s should be <15s)
docker exec pg-vector psql -U gedgar -d knowledge_base -tAc \
  "SELECT node_id, extract(epoch from now())-bit_mask/1e9 AS age_s \
   FROM bit_mask_table WHERE node_id LIKE '%heartbeat%' ORDER BY node_id"

# 4. any SYS_EXCEPTIONs raised overnight?
docker exec pg-vector psql -U gedgar -d knowledge_base -tAc \
  "SELECT path::text, data->>'last_error' \
   FROM knowledge_base_status \
   WHERE path::text ~ 'SYS_EXCEPTION' AND (data->>'status')::boolean = true"

# 5. zero rows at legacy shape (sanity)
docker exec pg-vector psql -U gedgar -d knowledge_base -tAc \
  "SELECT count(*) FROM knowledge_base WHERE path::text LIKE 'system.site.%'"
# expect: 0
```

If all-clean: open Layer **O** (observability tree-by-namespace). If any
check regresses, rollback `9c2daee1`:

```bash
cd $NDC_BASE
git revert --no-edit 9c2daee1
# Then rebuild + reslice + restage + reboot.
```

### Layer ordering after M-2 (unchanged from prior plan)

```
M-2 (DONE)  →  short soak  →  O (observability tree-by-namespace)  →  F + A + I  →  N  →  V
```

### **Next session — Layer O (observability tree-by-namespace)**

Lands BEFORE F so app-container logs (`app_containers.<c>.runtime.*`,
`app_containers.<c>.KB_LOG.*`) are visible in the tree from day one of app
onboarding. Single session — backend + frontend live in the same container
(`platform_containers/observability/container/{log_web,exception_web}`).

**Scope:**
- Server: extend `log_web/lua/helpers.lua` + `exception_web/lua/helpers.lua`
  with a tree-build query (`SELECT path, count(*) FROM knowledge_base_stream
  GROUP BY path` etc.) returning a node tree keyed by ltree segments.
  Each segment becomes a tree node; leaves are the actual log/exception
  streams.
- Client: htmx-driven expand/collapse view per node. Clicking a leaf opens
  the existing strip-chart / detail pane.
- Path math now reads `system.<sys>.site.<s>.cpu.<id>.container.<c>.KB_LOG.<sample>`
  (stable thanks to M-2).

**Checkpoint:** operator can navigate the full KB log/exception namespace as
a tree.

---

## Historical: end of 2026-05-01 EVENING session — Phase B Layer M-1 DONE

Phase B Layer M (namespace migration) is split into M-1 (path-composition
centralization, finished tonight) and M-2 (the actual rename, still pending).
Cluster is in overnight soak as of ~22:40 local against the M-1 code path.

### What landed tonight (5 commits)

| Commit | Layer | Scope |
|---|---|---|
| `bfeb2afb` | M-1a | dcs_host runtime: new `ndc_paths.lua` (Lua path composer) + 8 modules refactored to use it. `luajit_base/container/docker_build.sh` ships `ndc_paths.lua` into the base layer's `prebuilt_lua_share/`. |
| `0d248831` | M-1b | Container processes: `luajit_base/supervisor/user_functions.lua`, `dcs_console/{admin,gateway}/lua/`, `observability/{exception,log}_analyzer/main.lua`. observability stages `ndc_paths.lua` into `_staged_lib/` for self-contained image rebuilds. |
| `232183c9` | M-1c | Go broker: new `internal/pathkb/` package + 5 unit tests; `pgwriter.go` + `natspub.go` switched to `pathkb.BrokerRoot(site)`. |
| `b9175a0e` | M-1d | Laptop construction: `build_kb.sh` + `slice_bootstrap.sh` extend `LUA_PATH` with `runtime/dcs_host`; `slice_bootstrap.lua` (5 callsites) + `subsystems/cpu_bootstrap.lua` + 2 acceptance tests refactored. |
| `300c5abe` | plan | Inserted Layer **O** (observability tree-by-namespace) between M and F. |

Helper output verified byte-for-byte identical to the legacy format strings;
zero active production callsites still hardcode `"system.site.%s..."` outside
`ndc_paths.lua` / `pathkb.go`.

### Tonight's rebuild + soak

| Step | Result |
|---|---|
| 5 platform images rebuilt (luajit_base, openresty_base, observability, dcs_console, docker_host_broker) | green; ~75s total |
| Stop platform containers (so node_control reconcile pulls new images) | observability_01, dcs_console_01, docker-host-broker stopped |
| `build_kb.sh` (M-1d touched it) | defs=9, cpus=2, instances=9, subsystems=16 |
| `slice_bootstrap.sh` per-CPU bootstraps | cpu_01=2014 rows, cpu_02=581 rows (matches Phase A baseline) |
| `stage_deploy.sh --mode=dev` | runtime symlinks refreshed, env.sh preserved |
| `phase6_preflight.sh` | all 10 checks PASS |
| broker re-run with new image (canonical bootstrap) | pgwriter connected, http listening, probes finding test_app_01 |
| Boot cpu_01 (master), sleep 3, boot cpu_02 (slave) | exactly 1 dcs.lua + 1 watchdog per CPU |
| 5-min soak health check | sys_ready=1, both peers ACTIVE, ready_bits=3, heartbeats <3s old, broker docker_socket_ok=true, zero active SYS_EXCEPTIONs, zero panics in error.log |

Two transient ~500ms tick spikes (cpu_01 burst=313, cpu_02 burst=311 around
22:44:52) — single occurrence, no SYS_EXCEPTION fired, likely pg-cache
warm-up after the fresh build_kb. Watch overnight; not actionable yet.

App containers (test_app_01, robot_manager_01, ros_mission_planner_ii_01)
intentionally NOT rebuilt — they `FROM luajit_base` but have no M-1
callsites of their own. Their cached layers stay on pre-M-1 base; supervisor
emits identical paths to the new ndc_paths-based one.

### **First action next session — verify soak survived**

```bash
export NDC_BASE=/home/gedgar/knowledge_base_assembly/luajit_programs_and_containers/nano_data_center_base
export PGPASSWORD=$(docker exec pg-vector printenv POSTGRES_PASSWORD)
DEP=$NDC_BASE/commissioning_software/system_node_control/deployment

# 1. processes still 1 dcs.lua + 1 start.sh per CPU?
pgrep -af "dcs\.lua|start\.sh" | grep -v claude

# 2. system_ready / peer_state still ACTIVE?
docker exec pg-vector psql -U gedgar -d knowledge_base -tAc \
  "SELECT path::text, data FROM knowledge_base_status \
   WHERE path::text LIKE '%system_ready' OR path::text LIKE '%peer_state%' \
   ORDER BY path"

# 3. heartbeat freshness (age_s should be <15s)
docker exec pg-vector psql -U gedgar -d knowledge_base -tAc \
  "SELECT node_id, extract(epoch from now())-bit_mask/1e9 AS age_s \
   FROM bit_mask_table WHERE node_id LIKE '%heartbeat%' ORDER BY node_id"

# 4. any SYS_EXCEPTIONs raised overnight?
docker exec pg-vector psql -U gedgar -d knowledge_base -tAc \
  "SELECT path::text, data->>'last_error', data->>'acknowledged' \
   FROM knowledge_base_status \
   WHERE path::text ~ 'SYS_EXCEPTION' AND (data->>'status')::boolean = true"

# 5. error.log scan for new panics/errors
grep -iE "ERROR|PANIC|FATAL|stack trace" $DEP/cpu_01/error.log $DEP/cpu_02/error.log
```

If all-clean: proceed to Layer M-2 below. If any check regresses, rollback the
M-1 commits without losing the unrelated robot work that interleaved on master
(commits `907cd431` + `532c16ae` are L5/L6 work, untouched by M-1):

```bash
# Reverts each M-1 commit by creating a NEW commit that undoes it.
# Order: NEWEST first (so the diff applies cleanly).
cd $NDC_BASE
git revert --no-edit b9175a0e 232183c9 0d248831 bfeb2afb
# Then rebuild + reslice + restage + reboot per the recipe below.
```

`git revert` is non-destructive: it makes new commits, doesn't rewrite history,
and never touches files outside the M-1 scope. After the reverts, your robot
L5/L6 work is still on master, and the cluster goes back to pre-M-1 path
emission.

### **Layer M-2 — the actual rename (next session)**

Now small thanks to M-1 centralization. Single session.

1. **Topology change.** `commissioning_software/system_node_control/construction/catalogs/topology.lua`:
   ```lua
   -- before:
   site = "moonbase.alpha.dcs"
   -- after:
   system_name = "moon_base"
   site        = "moon_base_alpha"
   ```
   Search for any other place that consumes `topology.site` and surface them
   (`grep -rn "TOPOLOGY.site\|topology\.site"`).

2. **Path composer flip.** `runtime/dcs_host/ndc_paths.lua` and
   `commissioning_software/infrastructure/docker_host_broker/container/internal/pathkb/pathkb.go`:
   add module-private `cfg.system_name` + `Configure{system_name=...}`,
   change `SiteRoot(site)` to `"system." .. cfg.system_name .. ".site." .. site`.
   Update the `pathkb_test.go` expected strings.

3. **Configure call wiring.** Three places that bootstrap the cluster need to
   call `Configure(...)` once at startup:
   - `runtime/dcs_host/dcs.lua` — read `system_name` from
     `bootstrap.config.system_name` (added to bootstrap.config in step 4) and
     call `ndc_paths.configure{system_name = ...}`.
   - `construction/build_kb.lua` + `slice_bootstrap.lua` — read from `topology.system_name`
     and configure before any path emission.
   - Broker `cmd/broker/main.go` — accept `SYSTEM` env var (parallel to
     `SITE`) and call `pathkb.Configure(systemName)`.

4. **Bootstrap.config schema bump.** `subsystems/cpu_bootstrap.lua` writes
   `system_name` into the bootstrap row alongside `site`/`cpu_id`. Update
   `kb_root` to use the new shape (already does this via `ndc_paths.cpu_root`
   so just re-running build_kb after step 2 suffices).

5. **Broker run command update.** Where the broker is launched (continue.md
   in `infrastructure/docker_host_broker/`), add `-e SYSTEM=moon_base`
   alongside `-e SITE=moon_base_alpha`.

6. **Doc-comment sweep.** ~30 lines reference the old shape literally.
   Search: `grep -rn "system\.site\.<S>\|system\.site\.<site>"`. Replace
   shape only where comments document path layout.

7. **Smoke.** Stop cluster → `build_kb.sh` → `slice_bootstrap.sh` →
   `stage_deploy.sh` → `phase6_preflight.sh` → boot → 5-min check.
   Verify pg paths now read `system.moon_base.site.moon_base_alpha.*`:
   ```bash
   docker exec pg-vector psql -U gedgar -d knowledge_base -tAc \
     "SELECT count(*) FROM knowledge_base WHERE path::text LIKE 'system.moon_base.%'"
   # should be > 0; legacy 'system.site.moonbase.alpha.dcs.%' should be 0
   ```

8. **Commit + soak.** Two commits ideally: `M-2a topology + composer flip`,
   `M-2b doc-comment sweep`. Then short soak (1–2 hours) before opening Layer O.

### Layer ordering after M-2

```
M-2 (rename)  →  short soak  →  O (observability tree-by-namespace)  →  F + A + I  →  N  →  V
```

Layer O lands BEFORE F so app-container logs (`app_containers.<c>.runtime.*`,
`app_containers.<c>.KB_LOG.*`) are visible in the tree from day one of app
onboarding. See the layer table below for full Phase B scope.

---

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
| **O** — observability tree-by-namespace | Update the observability container (`platform_containers/observability/container/log_web/` + `exception_web/`) so the UI renders KB_LOG / SYS_EXCEPTION rows under a collapsible **tree control** keyed by the path namespace (`system.<sys>.site.<s>.cpu.<id>.container.<c>.KB_LOG.<sample>`). Each ltree segment becomes a tree node; leaves are the actual log/exception streams. Drives discoverability for app-container logs landing under `app_containers.<c>.runtime.*` once F lands. Server-side: extend `log_web/lua/helpers.lua` + `exception_web/lua/helpers.lua` with a tree-build query (`SELECT path, count(*) FROM knowledge_base_stream GROUP BY path` etc.) returning a node tree; client-side: htmx-driven expand/collapse view per node. | Operator can navigate full KB log/exception namespace as a tree; clicking a leaf opens the existing strip-chart / detail pane. |
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
- Layer O (observability tree control) lands AFTER M and BEFORE F. Rationale: with the namespace finalized, the tree control's path math stops drifting; landing it before app containers means new app-container logs (under `app_containers.<c>.runtime.*` and `app_containers.<c>.KB_LOG.*`) are visible in the tree from day one. Single session — backend + frontend live in the same container.
- Layers F + A + I can be a single session if the planner port goes smoothly; expect it to take 1–2 sessions.
- Layer N is its own session because of node_control refactor risk.
- Layer V is half a session (build + smoke).

Total: 4–6 sessions for Phase B as scoped above (was 3–5 before O insertion).

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
| 3.5 | Phase B Layer M-1 — path-composition centralization | ✅ DONE 2026-05-01 evening (`bfeb2afb`..`b9175a0e`) |
| 3.6 | Phase B Layer M-2 — namespace rename + system_name segment | ✅ DONE 2026-05-02 (`9c2daee1`) |
| 4 | KB-driven everything (file store, three-tier config, catalog hydration) | 🔲 (Layer O next, then F+A+I) |
| 5 | App-container build documentation | 🔲 (gated by Layer O after M-2) |
| 6 | Log-analysis web UI by KB namespace tree | 🔲 (now scoped as Phase B Layer O — lands BEFORE app port) |
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

## End of 2026-05-01 EVENING session

Phase B Layer M-1 (path-composition centralization) DONE across host runtime,
container processes, Go broker, and laptop construction pipeline. 5 commits
pushed to local master (`bfeb2afb`, `0d248831`, `232183c9`, `b9175a0e`,
`300c5abe`). Cluster rebuilt + booted on the M-1 code path; 5-min check
green; overnight soak underway.

**Open the next session at the top of this file** — the "State at end of
2026-05-01 EVENING session" block has the morning soak-check command and
the step-by-step Layer M-2 (the actual rename) recipe.

Rollback line if soak regresses: `git revert --no-edit b9175a0e 232183c9 0d248831 bfeb2afb`
(creates 4 new commits that undo M-1 in reverse order; preserves the unrelated
L5/L6 robot commits `907cd431` + `532c16ae` that landed on master tonight).

Earlier in the day (afternoon): Phase A complete, cluster ported to
`nano_data_center_base/`; `building_blocks/nanodatacenter_dcs/` retained
historical only.
