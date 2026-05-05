# Nanodatacenter DCS — Continuation Plan

## State at end of 2026-05-05 (latest) — B.2.A.4e DONE (planner-side board read path + hash threading)

A.4e landed as **one holding commit** wiring the planner-side read path
for file_store-backed boards:

- **`kb_doc_store.lua` vendored** into `planner/lib/` (same pattern as
  nats_*.lua / mqtt_*.lua vendoring from A.3.x).
- **`kb_query:get_active_board(name)`** + **`:get_active_board_sha(name)`**:
  reads `boards.<name>` fs_node pointer, parses JSON, caches by sha256.
  Cache hits skip the bytea decode + JSON parse cost; misses repopulate.
  Schema-on-read JSONB; new fields free.
- **`global_planner.new`** switched from the missing `q:get_board(...)`
  v2 API to `q:get_active_board(name)`; captures `self.board_sha256`;
  exposes `:get_board_sha256()` for downstream consumers.
- **`board_name` + `board_sha256` threaded** action_server → sequencer
  → mission → kb_runtime opts; kb_runtime auto-injects both into every
  push_event record so per-action history correlates to the exact board
  version that was active at mission start.
- **Sequencer drift-detection hook** `_check_board_drift()` invoked
  once per action transition (cheaper than per-tick). Under policy (1)
  drain-then-flip: logs once if drift observed, continues on captured
  hash. Wiring point for future (3) replan-in-place — the `if current
  ~= captured` branch becomes a replan trigger instead of a log line.

### A.4e smoke results (2026-05-05 evening)

| Check | Result |
|---|---|
| `kb_doc_store.lua` reachable from container's package.path | ✓ |
| `q:get_active_board("landing_zone_smoke")` returns `{graph_data, sha256_hex, cache_hit}` | ✓ 3 nodes, cache_hit=false |
| Second call: `cache_hit=true`, same sha256 | ✓ |
| `q:get_active_board_sha(name)` returns hex matching first fetch | ✓ |
| `kb_runtime:push_event` auto-injects `board_name` + `board_sha256` | ✓ verified in pg JSONB column |
| `mission_start` + `action_complete` records both carry the hash | ✓ |
| Cluster soak: peers ACTIVE / ACTIVE, 0 SYS_EXCEPTIONs, heartbeat fresh | ✓ |
| jq observer still subscribed; mock mission received cleanly post-rebuild | ✓ |

Smoke board cleaned up post-validation; class registration stays as
permanent schema. fs_blob/fs_node empty awaiting A.4f's real
construction-phase board.

### Drift detection — what's wired vs what's deferred

Wired now (policy 1 = drain-then-flip):
- `sequencer:_check_board_drift()` called once per action transition.
- `kb_query:get_active_board_sha(name)` is the cheap doc_stat-only probe.
- Compares to `sequencer.board_sha256` captured at mission start.
- On mismatch: log once via `io.stderr` with truncated hash prefixes
  for both sides. `self._drift_logged = true` flag prevents log spam.
- Mission continues to completion on the captured hash.

Deferred for policy 3 (replan-in-place) graduation:
- The "log once and continue" branch is the wiring point. To graduate:
  replace the io.stderr write with a call into global_planner to
  rebuild the graph against the new hash, then ask sequencer to splice
  a new route from the current pose to the original target.
- Mission state already captures the original target via the route +
  action_offset bookkeeping; no new state shape needed.

### Layer ordering

```
Phase B done: M-2 ✅ → O ✅ → F ✅ → A-pre ✅ → A ✅ → I ✅ → N ✅ → V ✅
Phase B.2 in progress:
  A.1 ✅ → A.2 ✅ → A.3.1-3.4 ✅ → A.3.5 ✅ → A.3.6 ✅ →
  A.4a ✅ → A.4b ✅ → A.4c ✅ → A.4d ✅ → A.4e ✅ →
  A.4f (author construction-phase landing_zone.json + upload + verify) →
  A.5 Phase 1 (jq observer dispatches; rejection path) →
  A.5 Phase 2 (mqtt_robot fixture; completion path)
Then queued:
  A.6 (map_api_tool — operator UI/CLI for board generation + validation)
  A.7 (robot_manager — fleet-wide robot registry + class catalog)
  N+1 (topology + slicer simplify), file-store loader generalization,
  three-tier config
```

### **First action next session — A.4f (construction-phase landing_zone)**

Author the representative board JSON:
- 12-20 nodes (start, multiple waypoints, 2-3 module-placement stops,
  2 charging stations, target).
- Mixed nav: line + B-spline edges. B-spline edges should carry control
  points compatible with the `building_blocks/ros_planner_*` route_builder.
- Edge weights vary by terrain difficulty.
- Operational stop nodes have a `params` blob carrying capability hints
  (`pickup_module_1` → `{operation: "pickup", module: "1"}`).

Steps:
1. Look at `building_blocks/ros_planner_ii/runtime/` and dsl_tests for
   any existing v2 board JSON to use as a structural reference.
2. Hand-author `boards/landing_zone.json` (probably under
   `nano_data_center_instance/boards/` or similar — pick a stable
   home now since A.6 map_api_tool will share it).
3. Upload via `upload_board.lua --name landing_zone`.
4. Verify via `kb_query:get_active_board("landing_zone")` in-container
   probe — graph node count, edge count, sha256 stable across re-fetch.

Estimated effort: **1 session** (most of it is JSON authoring; pipeline
is already smoke-tested at A.4d/A.4e).

### Quick start-of-session check (verifies A.4e still soaks)

```bash
# kb_doc_store reachable in planner container?
docker exec mission_planner_01 luajit -e "require('kb_doc_store'); print('OK')"

# kb_query API methods present?
docker exec mission_planner_01 luajit -e "
package.path = '/opt/apps/planner/lib/?.lua;' .. package.path
local q = require('kb_query')
print('get_active_board:', type(q.get_active_board) == 'function' and 'OK' or 'MISSING')
print('get_active_board_sha:', type(q.get_active_board_sha) == 'function' and 'OK' or 'MISSING')"

# Cluster soak invariants (unchanged through A.4e):
pgrep -af "dcs\.lua" | grep -v claude
docker exec pg-vector psql -U gedgar -d knowledge_base -tAc \
  "SELECT count(*) FROM knowledge_base_status WHERE path::text ~ 'SYS_EXCEPTION' AND (data->>'status')::boolean = true"
docker exec pg-vector psql -U gedgar -d knowledge_base -tAc \
  "SELECT count(*) FROM knowledge_base_doc_class WHERE namespace::text LIKE '%boards%'"
```

### Rollback recipe

A.4e revert: `git revert --no-edit <hash>` removes the kb_doc_store
vendor copy + kb_query board API + global_planner board_sha256
capture + sequencer drift hook + opts threading. Cluster falls back
to A.4d state where the file_store class registration exists but no
runtime code reads it. Pre-allocated kb_stream rows for mission_log
unaffected. Cluster operationally healthy at the rolled-back state
because A.5 dispatch isn't wired yet — kb_runtime would still error
on the new board_name/board_sha256 asserts post-revert; A.4b's earlier
shape (no board fields) would also need to be restored. Practical
revert path is reverting BOTH A.4e and the kb_runtime shape change
together. Easier: roll forward.

---

## State at end of 2026-05-05 (latest) — B.2.A.4d DONE (file_store boards class + upload tool)

A.4c was a pure smoke validation — file_store driver (`kb_doc_store.lua`)
+ schema (`knowledge_base_fs_blob`, `_fs_node`, `_doc_class`) all healthy
under live pg, framework facade `Construct_Data_Tables:add_doc_class`
already wired. **No commit for A.4c** (no code change needed).

A.4d landed as **one holding commit**:

- **`construction/subsystems/boards.lua`** (new): registers a
  `commissioning_only` doc-class at site root namespace
  `system.<sys>.site.<S>.boards`. Site-wide on purpose: the physical
  world is shared across every planner instance.
- **`construction/scripts/upload_board.lua`** (new): operator CLI. Reads
  a JSON file, validates parses, computes sha256 (via `sha256sum`),
  calls `doc_put` with `writer="commissioning"`, writes an audit row
  at `boards.<name>.KB_STATUS_FIELD.last_upload` with the hash + uploader
  + timestamp. All in one pg transaction.
- **`build_kb.lua`** SUBSYSTEMS list updated to include `"boards"` after
  `"site_scalars"` and before `"infrastructure_registry"`.

### A.4d smoke results (2026-05-05 evening)

| Check | Result |
|---|---|
| `boards` class registered (`commissioning_only`, `application/json`) | ✓ via build_kb |
| `upload_board.lua` accepts `--system / --site / --name / --file` | ✓ |
| 582-byte board JSON uploaded → `fs_blob` row + `fs_node` pointer + audit row | ✓ |
| Content readable as UTF8 → JSONB; structure preserved (3 nodes) | ✓ |
| Re-upload of same file is idempotent (sha256 dedup; still 1 blob row) | ✓ |
| Cluster soak post-rebuild: peers ACTIVE / ACTIVE, 0 SYS_EXCEPTIONs | ✓ |

**Smoke artifact cleaned up** — class registration stays (it's permanent
schema), but the test board's `fs_blob` + `fs_node` + audit rows were
deleted post-validation. pg fs_blob/fs_node now empty awaiting A.4f's
real construction-phase board.

### Architectural decisions locked this session

- **Storage = file_store-backed (option b)**. Boards live in pg as
  content-addressable blobs; sha256 keyed; site-wide namespace; not
  per-planner-instance. Robots aren't consumers — only mission planners
  are, and they may be on remote nodes, so pg distribution is the
  natural fit.
- **Mid-mission revision policy = (1) drain-then-flip for now, design
  for (3) replan-in-place later**. Mission state will capture the hash
  at start (lands in A.4e). Sequencer drift detection is a no-op hook
  under (1), wired during A.4e for future (3) graduation without a
  rewrite.
- **First board = construction-phase representative** (12-20 nodes,
  multi-stop with pickup_module_1 / drop_module_1 / charging_station,
  mixed nav line + B-spline, terrain-varying weights). Lands in A.4f
  to exercise storage at realistic scale.
- **map_api_tool + robot_manager = queued layers** (A.6 + A.7), NOT
  woven into A.5. They're real but separate; A.5's holding-commit
  discipline cleaner without them.

### Layer ordering

```
Phase B done: M-2 ✅ → O ✅ → F ✅ → A-pre ✅ → A ✅ → I ✅ → N ✅ → V ✅
Phase B.2 in progress:
  A.1 ✅ → A.2 ✅ → A.3.1-3.4 ✅ → A.3.5 ✅ → A.3.6 ✅ →
  A.4a ✅ → A.4b ✅ → A.4c ✅ (smoke, no commit) → A.4d ✅ →
  A.4e (kb_query:get_active_board API + hash capture in mission state — next session) →
  A.4f (author construction-phase landing_zone.json + upload + verify) →
  A.5 Phase 1 (jq observer dispatches; rejection path) →
  A.5 Phase 2 (mqtt_robot fixture; completion path)
Then queued:
  A.6 (map_api_tool — operator UI/CLI for board generation + validation)
  A.7 (robot_manager — fleet-wide robot registry + class catalog)
  N+1 (topology + slicer simplify), file-store loader generalization,
  three-tier config
```

### **First action next session — A.4e (kb_query:get_active_board)**

Adds the planner-side read path. Three sub-steps:

1. **`kb_query:get_active_board(name)`** — reads `boards.<name>` fs_node
   pointer (via `kb_doc_store.doc_get`), parses content as JSON, returns
   `{ graph_data, sha256_hex }`. Caches by sha256 in an in-memory map so
   subsequent calls for the same hash skip the fetch. Replaces the
   missing `list_boards`/`get_board` slot that A.5 Phase 1 needs.

2. **Mission state captures hash at start**. mission.lua's `:start()`
   pushes a `mission_start` event with `board_sha256 = <hex>` so every
   downstream `push_event` record is correlatable to the exact board
   version. Also stored on `self.board_sha256` for the (3) replan-in-place
   drift detector wiring.

3. **Sequencer drift-detection hook** (no-op under (1), wired for (3)).
   Add `_check_board_drift()` method called once per tick (cheap: read
   `fs_node.sha256` for the active board, compare to `self.board_sha256`).
   Under policy (1): just log if different. Under future (3): trigger
   replan. This way the (3) graduation is a single-method change, not
   a structural rewrite.

Pre-emptive watch-outs:
- `kb_doc_store.doc_get` requires the planner to have access to
  `kb_doc_store.lua` in its container. Currently lives in
  `building_blocks/knowledge_base/postgres/data_structures/`. May need
  to vendor into planner image (similar to how nats_*.lua + mqtt_*
  were vendored in A.3.x). ~1 file copy, not a heavy lift.
- `dkjson` is already available in container (used by kb_runtime).
- Cache invalidation: drift detector handles it. Cache key = sha256.

Estimated effort: **½ session**.

### Quick start-of-session check (verifies A.4d still soaks)

```bash
docker exec pg-vector psql -U gedgar -d knowledge_base -tAc \
  "SELECT namespace::text, writer FROM knowledge_base_doc_class WHERE namespace::text LIKE '%boards%';"
# expect: system.moon_base.site.moon_base_alpha.boards | commissioning_only

# upload tool exists + chmod +x:
ls -l /home/gedgar/knowledge_base_assembly/luajit_programs_and_containers/nano_data_center_base/commissioning_software/system_node_control/construction/scripts/upload_board.lua

# Cluster soak invariants (unchanged through A.4d):
pgrep -af "dcs\.lua" | grep -v claude
docker exec pg-vector psql -U gedgar -d knowledge_base -tAc \
  "SELECT count(*) FROM knowledge_base_status WHERE path::text ~ 'SYS_EXCEPTION' AND (data->>'status')::boolean = true"
```

### Rollback recipe

A.4d revert: `git revert --no-edit <hash>` removes the boards subsystem
and upload tool. The class registration row in pg's
`knowledge_base_doc_class` survives the revert (it was inserted at
build_kb time, not by source). Re-running build_kb after revert leaves
the class row in place but the boards subsystem is gone — operator can
manually `DELETE FROM knowledge_base_doc_class WHERE namespace LIKE
'%boards%'` if a clean state is desired. Cluster operationally
unaffected (no runtime code uses boards yet — A.4e/A.4f land that).

---

## State at end of 2026-05-05 (latest) — B.2.A.4b DONE (per-action persistence via kb_stream)

A.4b landed as **one holding commit** porting kb_runtime.lua's body from
sqlite to pg-backed kb_stream capped FIFO with JSONB action records.
Wires through the full chain:

- `mission_planner/kb_build.lua` → declares `mission_log/actions` sub-header + `add_stream_field("samples", 256, ...)` → pre-allocates 256 rows at `app_containers.mission_planner_01.mission_log.actions.KB_STREAM_FIELD.samples`
- `kb_runtime.lua` → DBI-direct push (mirrors dcs_host/kb_stream.lua's `M.push` pattern, no KBM/KB_Search dependency); single `push_event(record)` API; auto-injects `robot_id`, `mission_id`, `timestamp`
- `mission.lua` → 5 call sites (`merge_status` ×2 + `write_heartbeat` ×3) collapsed onto `push_event` with explicit `type` discriminators (`mission_start` / `action_start` / `action_complete` / `action_failed` / `mission_finish`)
- `action_server.lua` → tags `cmd.mission_id = job.id` from JobQueue claim before stashing; threads `mission_id` into both `sequencer_mod.new` call sites with synthetic-id fallback for direct-submit paths
- `sequencer.lua` → asserts `opts.mission_id`, threads to `mission_mod.new`

**Storage decision**: one ring per planner instance (NOT per robot).
`robot_id` lives in the JSON payload, not the ltree path — UI consumers
filter by `data->>'robot_id'`. Cap=256 = ~40-80 missions of history per
planner. JSONB schema-on-read (new fields free, no migration).

| Slice | Scope |
|---|---|
| `(this commit)` | **A.4b** — Per-action persistence: full kb_runtime body rewrite, mission.lua call-site collapse, kb_build stream-field declaration, mission_id threading through action_server → sequencer → mission. ~7 files. |

### A.4b smoke results (2026-05-05 evening, latest)

| Check | Result |
|---|---|
| build_kb pre-allocates 256 rows at `mission_log.actions.KB_STREAM_FIELD.samples` | ✓ verified via pg row count |
| `kb_runtime.new` opens DBI conn cleanly | ✓ |
| In-container probe: 4 push_event calls (mission_start / action_start / action_complete / mission_finish) all `ok=true` | ✓ |
| 4 valid + 252 pre-allocated invalid rows after probe | ✓ |
| JSON predicate query (`data->>'mission_id' = 'probe_mission_001'`) returns full lifecycle in order | ✓ |
| jq observer log-only handler still subscribed + receives missions | ✓ |
| heartbeat fresh, peers ACTIVE / ACTIVE, 0 SYS_EXCEPTIONs | ✓ |

### Sample record (UI-style fetch)

```sql
SELECT data::jsonb->>'type'         AS type,
       data::jsonb->>'robot_id'     AS robot_id,
       data::jsonb->>'mission_id'   AS mission_id,
       data::jsonb->>'action_index' AS action_index,
       data::jsonb->>'success'      AS success
  FROM knowledge_base_stream
 WHERE path::text = 'system.moon_base.site.moon_base_alpha.app_containers.mission_planner_01.mission_log.actions.KB_STREAM_FIELD.samples'
   AND valid = TRUE
   AND data::jsonb->>'mission_id' = '<job_id>'
 ORDER BY recorded_at;
```

returns:
```
mission_start  | rover_X | <mission_id> |   |
action_start   | rover_X | <mission_id> | 1 |
action_complete| rover_X | <mission_id> | 1 | true
...
mission_finish | rover_X | <mission_id> |   | true
```

### What's now unblocked

A.4b closes the third (and last) of the three blockers documented at end
of A.3.5:
- ✅ kb_query positional args (A.4a)
- ✅ sequencer:get_site dead path (A.4a)
- ✅ kb_runtime body sqlite-coded (A.4b)

**Mission DISPATCH path is now structurally sound.** A.5 V-heavy is the
next logical layer: switch the jq observer's log-only handler over to
`action_srv:_drain_nats_queue` (or call `action_srv:execute_mission`
directly), exercise the rejection path (no robot — `submitted` →
`rejected_no_robot`), then the completion path with the
`building_blocks/ros_planner_ii_mqtt_robot/` fixture.

### Layer ordering

```
Phase B done: M-2 ✅ → O ✅ → F ✅ → A-pre ✅ → A ✅ → I ✅ → N ✅ → V ✅
Phase B.2 in progress:
  A.1 ✅ → A.2 ✅ → A.3.1 ✅ → A.3.2 ✅ → A.3.3 ✅ → A.3.3b ✅ → A.3.4 ✅ →
  A.3.5 ✅ → A.3.6 ✅ → A.4a ✅ → A.4b ✅ →
  A.5 (= V-heavy: rejection path + completion path with mqtt_robot fixture)
Then queued: N+1 (topology + slicer simplify), file-store loader, three-tier config
```

### **First action next session — A.5 V-heavy**

Two phases:

**Phase 1 — rejection path (~½ session).** Wire jq observer to actually
dispatch (replace the log-only handler with a call into action_server).
Submit a mission for a robot class with no live robot:
```lua
{"robot_id":"rover_1","class_name":"drive_base","board":"landing_zone"}
```
Expected: planner sees no live robot for `rover_1` (link_manager has no
entry), publishes `state="rejected_no_robot"` to the NATS status key,
mission_finish event lands in pg with `success=false`. No mqtt_robot
fixture required.

**Phase 2 — completion path (~½ session).** Start the
`building_blocks/ros_planner_ii_mqtt_robot/` Linux container as a fake
robot. It announces via link protocol, takes the dispatched mission,
produces action results back through the planner pipeline. Expected:
full action lifecycle in pg (mission_start → action_start ×n →
action_complete ×n → mission_finish success=true), final pose updated.

**Pre-emptive watch-outs:**
- Mission DISPATCH may unmask further mismatches in the runtime chain
  (board lookup, link_manager wiring, mqtt transport handshake). The
  three named blockers are closed but A.5 may surface secondary ones.
- The jq observer wiring change is small but invasive: changing from
  log-only to dispatch means the observer now owns the mission lifecycle.
  Consider wrapping in pcall so a crash doesn't take down the heartbeat
  loop. See `feedback_no_soft_faults` though — fault paths are
  fail-stop; a crash here SHOULD halt until explicit reset, not be
  swallowed.
- `list_boards` method is still missing on kb_query (A.4a notes); board
  lookup will likely need that to be implemented first. Probably 1-2
  hours of method addition + tests.

### Quick start-of-session check (verifies A.4b still soaks)

```bash
docker logs mission_planner_01 2>&1 | grep "action_server instantiated" | tail -1
docker logs mission_planner_01 2>&1 | grep "jq observer subscribed" | tail -1

# Pre-allocated stream still 256?
docker exec pg-vector psql -U gedgar -d knowledge_base -tAc \
  "SELECT count(*) FROM knowledge_base_stream
    WHERE path::text ~ 'mission_planner_01.mission_log.actions.*samples'"

# kb_runtime probe (push 4 events to a synthetic mission_id):
docker exec mission_planner_01 luajit -e "
package.path = '/opt/apps/planner/lib/?.lua;/opt/apps/planner/?.lua;/opt/apps/planner/hub_dsl/?.lua;/opt/apps/planner/hub_dsl/kb_construct/?.lua;/usr/local/share/lua/5.1/chain_tree/lua_dsl/luajit_pipeline/?.lua;' .. package.path
local kb_runtime = require('kb_runtime')
local pg_conn = { host = os.getenv('PG_HOST'), port = tonumber(os.getenv('PG_PORT')),
                  dbname = os.getenv('PG_DB'), user = os.getenv('PG_USER'),
                  password = os.getenv('PG_PASSWORD') }
local rt = kb_runtime.new({
    pg_conn=pg_conn, site=os.getenv('APP_SITE'),
    system_name=os.getenv('APP_SYSTEM'), container_name=os.getenv('CONTAINER_NAME'),
    robot_id='rover_smoke', mission_id='smoke_'..os.time() })
local ok, err = rt:push_event({type='mission_start', route_length=2})
io.stdout:write('push: ok='..tostring(ok)..' err='..tostring(err)..'\n')
rt:close()"
# expect: push: ok=true err=nil

pgrep -af "dcs\.lua" | grep -v claude
docker exec pg-vector psql -U gedgar -d knowledge_base -tAc \
  "SELECT count(*) FROM knowledge_base_status WHERE path::text ~ 'SYS_EXCEPTION' AND (data->>'status')::boolean = true"
```

### Rollback recipe

A.4b revert: `git revert --no-edit <hash>` — single holding commit, ~7
files revert atomically. Cluster falls back to A.4a state where
constructors instantiate cleanly but kb_runtime body crashes on first
mission. mission_planner_01's mission_log stream stays declared in pg
(harmless — just unused empty rows). To fully roll back the stream
declaration, re-run build_kb at the reverted state and the orphan
kb_build mission_log block disappears.

---

## State at end of 2026-05-05 — B.2.A.4a DONE (kb_query positional args + opts threading)

A.4a landed as **one holding commit** unifying the v3 kb_query positional
arg shape across action_server / global_planner / sequencer / hub_runtime
/ mission, threading `system_name` + `own_instance_id` through every
constructor's opts, and deleting sequencer's dead `kb_q:get_site()`
fallback (the v3 kb_query has no `get_site` method; v2-only path that
was never reached because action_server always supplied opts.site).

This was the **mechanical half** of A.4. The remaining half (A.4b) is
the kb_runtime body port to `kb_stream.push_stream_data` — design locked
this session: per-action records as JSONB in a capped FIFO ring, one
stream-field path per robot, cap depth ~64 actions per robot. UI
consumers fold in later. 1-1.5 sessions of focused work to land.

| Slice | Scope |
|---|---|
| `(this commit)` | **A.4a** — Renamed v2-shape `kb_query.new(db_file, "knowledge_base", ltree_path, site)` → v3 `kb_query.new(pg_conn, system_name, site, own_instance_id)` at all 5 live runtime call sites: action_server.lua (3× lines 83/260/884), global_planner.lua:94, hub_runtime.lua:93. Threaded `system_name` + `own_instance_id` through every constructor's opts: action_server.new, global_planner.new, sequencer.new, hub_runtime.new, mission.new. main.lua passes `APP_SYSTEM` and `CONTAINER_NAME` into action_server opts. Deleted sequencer.lua's `if not opts.site then kb_q:get_site() else opts.site end` block (v2-only fallback; v3 kb_query has no get_site method, action_server always supplies opts.site so the branch was dead). 6 files / ~30 edit points (similar pattern to A.3.5 db_file→pg_conn refactor). |

### A.4a smoke results (2026-05-05 evening, latest)

| Check | Result |
|---|---|
| `planner libs loaded: ... action_server=ok` | ✓ |
| `action_server instantiated: nats_server=nats://nats-js-ram:4222` | ✓ |
| In-container kb_query.new probe: `system_name=moon_base site=moon_base_alpha own=mission_planner_01` | ✓ — v3 signature actually invoked |
| `jq observer subscribed: bucket=... queue=...` | ✓ |
| 4th mock mission received cleanly | ✓ id=30310d8d... |
| heartbeat fresh | ✓ |
| peers cpu_01 / cpu_02 | ACTIVE / ACTIVE |
| active SYS_EXCEPTIONs | 0 |

### Pre-existing artifacts (NOT broken by A.4a, NOT in scope)

- **`list_boards` method missing on kb_query**: action_server's constructor pcall calls `q:list_boards()` to discover the initial board node. v3 kb_query doesn't define `list_boards`. The pcall swallows the error silently — this is the *intended* shape for best-effort init. boards table is also empty (0 rows in pg) so even if the method existed, the lookup wouldn't find anything. Both ends will resolve when board-loader / board-registry work lands; not part of A.4 / A.5.
- **Build-time scripts not migrated**: `hub_dsl/kb_construct/test_kb.lua:34` and `kb_exporter.lua:46` still call v2-shape `kb_query.new(db_file, "knowledge_base", ltree_path)`. These run separately from the container's main loop (they're build-time helpers); migrating them when their code path next executes.

### Three deferred V-heavy blockers — status update

A.4a closed **2 of 3** blockers documented at end of A.3.5:

- ✅ kb_query positional args (this commit)
- ✅ sequencer:get_site dead path (deleted this commit)
- ⏳ **kb_runtime body still sqlite-coded** — A.4b's job. Constructor signature is pg-correct (asserts pg_conn table); body uses `self.db = self.kb.db` and `sqlite3_helpers`. First mission DISPATCH still crashes when `kb_rt:merge_status` / `:write_heartbeat` fires.

### **First action next session — A.4b kb_runtime body port to kb_stream**

Per the design lock this session: **per-action persistence requirement
is real** (will be folded into UI later); use `kb_stream.lua` capped-FIFO
ring with JSONB payload. The driver lives at
`building_blocks/knowledge_base/postgres/data_structures/kb_stream.lua`
(`push_stream_data(path, data)` writes JSON-encoded Lua table into a
pre-allocated capped ring — cap depth declared at build_kb time via
`add_stream_field(stream_key, stream_length, description)`).

**Step-by-step:**

1. **Decide entity_key shape.** Two shapes evaluated this session:
   - (1) per-robot: `...mission_log.<robot_id>.actions.KB_STREAM_FIELD.samples`, cap=64. Survives across missions for that robot. Recommended.
   - (2) global: `...mission_log.actions.KB_STREAM_FIELD.samples`, cap=large. UI filters by robot_id+mission_id JSON predicate. Loses per-robot retention.
   Going with (1) unless you say otherwise.

2. **Pick robot list source.** Robots aren't in topology (they register dynamically via link protocol). Two options for build-time stream-field declaration:
   - Pre-allocate a fixed pool of stream fields by *robot slot* (`rover_01`, `rover_02`, ...) — works if robots have stable IDs.
   - Lazy-create stream fields at first-mission time (no build_kb declaration; runtime `add_stream_field`). Requires kb_stream to support live add. Probably it does (the framework already calls add_stream_field at commission for static paths; runtime call may work).
   Investigate at start of A.4b which is supported.

3. **Define JSON payload shape.** Single shape used at all 5 calls in mission.lua:
   ```lua
   {
       mission_id      = job_id,
       action_index    = i,
       action_total    = n,
       capability      = "drive_to" | ...,
       kb_name         = node_name,
       phase           = "started" | "complete" | "failed",
       started_at_ms   = ...,
       completed_at_ms = ...,         -- nil for "started"
       elapsed_ms      = ...,
       energy_used     = ...,
       fault           = { reason, detail },  -- "failed" only
       partial_state   = { ... },     -- "failed" only
   }
   ```
   Schema-on-read JSONB; new fields can be added later without migration.

4. **Rewrite kb_runtime.lua body.** Replace `self.db = self.kb.db` and `sqlite3_helpers` references with KBM ltree writes against `knowledge_base_stream` (or whatever pg path the kb_stream driver uses internally). The 5 call sites in mission.lua (`:merge_status` ×2 + `:write_heartbeat` ×3) all push the same shape; kb_runtime can collapse to a single `kb_rt:push_action(record)` API with the shape above, and mission.lua callers compose the record.

5. **Rebuild + smoke.** Submit a mock mission via the existing smoke helper, then read the stream rows back from pg to confirm the action records landed:
   ```sql
   SELECT path::text, data
     FROM knowledge_base_stream
    WHERE path::text ~ 'mission_log.rover_1.actions.*samples'
    ORDER BY recorded_at DESC LIMIT 10;
   ```

**Pre-emptive watch-outs:**
- The kb_stream `push_stream_data` REQUIRES pre-allocated rows (errors on `No records found for path='...'. Records must be pre-allocated.`). So step 2 (robot list source for build-time declaration) is critical.
- If lazy add_stream_field at runtime is supported, that's cleanest. If not, a fixed pool of robot slots is the fallback.
- mission.lua has 5 kb_rt calls; the rewrite either preserves their existing shape (`:merge_status` + `:write_heartbeat`) or collapses to one API. Lower-touch is preserve-shape, but the rewrite's already touching kb_runtime guts so collapsing is fair game.
- Schema is JSONB at the kb_stream level (per `push_stream_data`'s `dkjson.encode(data)`), so new fields are free.

### Layer ordering

```
Phase B done: M-2 ✅ → O ✅ → F ✅ → A-pre ✅ → A ✅ → I ✅ → N ✅ → V ✅
Phase B.2 in progress:
  A.1 ✅ → A.2 ✅ → A.3.1 ✅ → A.3.2 ✅ → A.3.3 ✅ → A.3.3b ✅ → A.3.4 ✅ →
  A.3.5 ✅ → A.3.6 ✅ → A.4a ✅ →
  A.4b (kb_runtime body port to kb_stream — next session) →
  A.5 (= V-heavy: rejection path + completion path with mqtt_robot fixture)
Then queued: N+1 (topology + slicer simplify), file-store loader, three-tier config
```

### Quick start-of-session check (verifies A.4a still soaks)

```bash
docker logs mission_planner_01 2>&1 | grep "action_server instantiated" | tail -1
docker logs mission_planner_01 2>&1 | grep "jq observer subscribed" | tail -1

# Constructor opts threaded correctly?
docker exec mission_planner_01 luajit -e "
package.path = '/opt/apps/planner/lib/?.lua;/opt/apps/planner/?.lua;/usr/local/share/lua/5.1/chain_tree/lua_dsl/luajit_pipeline/?.lua;' .. package.path
local kb_query = require('kb_query')
local pg_conn = { host = os.getenv('PG_HOST'), port = tonumber(os.getenv('PG_PORT')),
                  dbname = os.getenv('PG_DB'), user = os.getenv('PG_USER'),
                  password = os.getenv('PG_PASSWORD') }
local q = kb_query.new(pg_conn, os.getenv('APP_SYSTEM'),
                       os.getenv('APP_SITE'), os.getenv('CONTAINER_NAME'))
io.stdout:write('OK system='..q.system_name..' site='..q.site..' own='..q.own_instance_id..'\n')
q:close()"
# expect: OK system=moon_base site=moon_base_alpha own=mission_planner_01

pgrep -af "dcs\.lua" | grep -v claude
docker exec pg-vector psql -U gedgar -d knowledge_base -tAc \
  "SELECT count(*) FROM knowledge_base_status WHERE path::text ~ 'SYS_EXCEPTION' AND (data->>'status')::boolean = true"
```

### Rollback recipe

A.4a revert: `git revert --no-edit <hash>` — single holding commit, all
6 lua files + continue.md revert atomically. Cluster falls back to A.3.6
state (action_server instantiates but constructor pcall masks the
positional-arg type mismatch; dispatch still blocked on kb_runtime).

---

## State at end of 2026-05-05 (latest) — B.2.A.3.6 DONE (NATS JobQueue observer green)

A.3.6 landed as **one holding commit** covering the JobQueue log-only
observer in main.lua + the planner-net plumbing through the catalog →
container_definitions subsystem → spec_adapter → docker.lua chain.

End-to-end verified: a `submit_test_mission.lua` invocation inside the
container produces the log line
`mission received #1 id=<sha> payload={"robot_id":"rover_1",...}` within
seconds. Heartbeat keeps ticking, peers ACTIVE, 0 SYS_EXCEPTIONs.

Continue.md's pre-session bullet count was "~½ session, log-only handler,
mock-publish via `nats pub`". Two surprises (and the second is bigger
than the first):

1. **JobQueue is KV-backed, not subject pub/sub.** continue.md guessed
   `subject = APP_SITE..".action_server.missions"` and `nats pub <subject>
   <json>` for mock-publish — wrong. Real shape: `KeyStore.new({server,
   bucket = "<site_bucket>_action_server", create_bucket=true})` then
   `JobQueue.new(ks:handle(), worker_id)`. Submission goes through
   `jq:submit(payload, queue, priority, retries, timeout)` writing
   structured Job records into JetStream KV. Standalone `nats pub` would
   not be picked up. **A submit_test_mission.lua helper is now committed
   at `planner/scripts/`** for repeatable smoke.

2. **Default-bridge has no DNS for container names.** mission_planner_01
   was on `bridge` but `nats-js-ram` is on `planner-net`; even though
   both were nominally on `bridge` too, Docker's default-bridge does NOT
   service-discover. JQ observer's first connect attempt died with
   `KeyStore error: connection error`. The infra_discovery KB row says
   `host=nats-js-ram` but DNS only works on user-defined networks.
   **Required a four-file plumbing change to land `--network planner-net`
   for app containers.**

| Slice | Scope |
|---|---|
| `(this commit)` | **A.3.6** — main.lua: `nats_ks.KeyStore.new` + `nats_jq.JobQueue.new` log-only observer (worker_id `planner_log_observer`); per-tick `drain_observer()` claims up to 5 jobs, logs payload, completes with `"logged_only"` status. New `planner/scripts/submit_test_mission.lua` for end-to-end smoke. Plumbing: `catalogs/definitions.lua` adds `networks = { "planner-net" }` to mission_planner def; `subsystems/container_definitions.lua` passes `networks` through to build.spec; `spec_adapter.lua` reads `spec.networks[1]` (with `spec.network` legacy fallback) into RunSpec.network; `docker.lua` adds `--network` + `docker network connect` for additional networks (parallel to broker path; both paths now consistent on input shape). Container respawned at 172.18.0.7 (planner-net), DNS resolves `nats-js-ram` → 172.18.0.4. |

### A.3.6 smoke results (2026-05-05 evening, later)

| Check | Result |
|---|---|
| `planner libs loaded: ... action_server=ok` | ✓ |
| `action_server instantiated: nats_server=nats://nats-js-ram:4222` | ✓ |
| mission_planner_01 on `planner-net` | ✓ 172.18.0.7 |
| `getent hosts nats-js-ram` from inside | ✓ 172.18.0.4 |
| `jq observer subscribed: bucket=moon_base_alpha_action_server queue=moon_base_alpha.action_server.missions` | ✓ |
| `submit_test_mission.lua` returns job_id | ✓ |
| `mission received #1 id=<sha> payload={...}` log line | ✓ within ~5s of submit |
| heartbeat ticks during JQ drain | ✓ |
| peers cpu_01 / cpu_02 | ACTIVE / ACTIVE |
| active SYS_EXCEPTIONs | 0 |

### Architectural note: which `docker run` path is in force

There are now TWO container-launch code paths in this tree:

- `docker.lua:run_from_spec` — original direct shell-out path. Receives
  `spec.networks = { ... }` from build.spec and emits `--network <first>`
  + `docker network connect` for additional. Used for legacy/internal
  container ops (e.g. `system_control` infra start).
- `broker_client.run` (Go broker) — newer path used by node_control's
  `START_ASSIGNED_CONTAINERS` to launch app containers. Receives
  `network` (singular string) on the wire, set by spec_adapter.lua from
  `spec.networks[1]`. Broker's `buildCreateConfigs` honors EndpointsConfig
  + NetworkMode for user-defined nets (was already implemented; this
  refactor only touched the wire-encoder).

Both paths now consistent on input shape (`networks` plural array on
catalog/build.spec); they just differ on output (broker takes a single
string, docker.lua handles multi-net via post-run `network connect`).

### Three V-heavy blockers from A.3.5 still open (unchanged)

A.3.6 was a green-field add and didn't touch dispatch path, so the three
deferred blockers from A.3.5 remain (kb_runtime sqlite body, kb_query
positional args, sequencer:get_site dead path). Documented in detail in
the prior section.

### Layer ordering

```
Phase B done: M-2 ✅ → O ✅ → F ✅ → A-pre ✅ → A ✅ → I ✅ → N ✅ → V ✅
Phase B.2 in progress:
  A.1 ✅ → A.2 ✅ → A.3.1 ✅ → A.3.2 ✅ → A.3.3 ✅ → A.3.3b ✅ → A.3.4 ✅ →
  A.3.5 ✅ → A.3.6 ✅ →
  A.4 (kb_runtime body port + kb_query positional args fix — next session) →
  A.5 (= V-heavy: rejection path + completion path with mqtt_robot fixture)
Then queued: N+1 (topology + slicer simplify), file-store loader, three-tier config
```

### **First action next session — A.4 (kb_runtime body port + kb_query positional args)**

Unblocks dispatch. Two coupled concerns:

1. **kb_runtime body port (sqlite → pg).** kb_runtime.lua's constructor
   signature is pg-correct after A.3.5 but the body still uses
   `self.db = self.kb.db` and `sqlite3_helpers`. Pick:
   - (a) **NATS-only telemetry**: drop kb_rt entirely from mission.lua;
     all live telemetry through NATS KV (action_server already does the
     `_publish_status` pattern). Cheaper, but loses the durable side.
   - (b) **Full pg port**: rewrite kb_runtime body to KBM ltree writes
     against `knowledge_base_status` / `knowledge_base_stream`. Larger
     surface but preserves the three-tier model.

   Recommendation: **(a) NATS-only first** — gets dispatch working,
   doesn't churn the durable-telemetry design. Promote to (b) only when
   real telemetry needs durable storage.

2. **kb_query positional args at 5 sites.** v3 signature:
   `(pg_conn, system_name, site, own_instance_id)`. Current call sites
   pass v2 shape `(db_file, "knowledge_base", ltree_path, site)`:
   - action_server.lua:80 (constructor pcall — masks; OK)
   - action_server.lua:257, 878 (execute_mission — would crash)
   - global_planner.lua:93 (would crash)
   - sequencer.lua:77 (dead path; delete)
   - hub_runtime.lua:90 (only reached when opts.pg_conn truthy at
     execute_mission time)

   Fix: thread `system_name` + `own_instance_id` through every constructor
   opts, drop ltree_path arg, update each call site.

Estimated effort: **1 session** (mirrors A.3.5 — ~9 files, similar
blast radius pattern).

### Quick start-of-session check (verifies A.3.6 still soaks)

```bash
docker logs mission_planner_01 2>&1 | grep "jq observer subscribed" | tail -1
# expect: ... bucket=moon_base_alpha_action_server queue=moon_base_alpha.action_server.missions

docker inspect mission_planner_01 --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}={{$v.IPAddress}} {{end}}'
# expect: planner-net=172.18.0.X

# Submit a mock mission, then check it's logged:
docker exec mission_planner_01 luajit /opt/apps/planner/scripts/submit_test_mission.lua \
    '{"robot_id":"rover_1","class_name":"drive_base","board":"landing_zone"}'
sleep 7
docker logs mission_planner_01 2>&1 | grep "mission received" | tail -1

# Soak invariants:
pgrep -af "dcs\.lua" | grep -v claude
docker exec pg-vector psql -U gedgar -d knowledge_base -tAc \
  "SELECT (data::jsonb->'value'->>'tick')::int, extract(epoch from now())*1000 - (data::jsonb->'value'->>'at')::bigint
   FROM knowledge_base_status
   WHERE path::text ~ 'mission_planner_01.runtime.heartbeat.KB_STATUS_FIELD.snapshot'"
docker exec pg-vector psql -U gedgar -d knowledge_base -tAc \
  "SELECT count(*) FROM knowledge_base_status \
   WHERE path::text ~ 'SYS_EXCEPTION' AND (data->>'status')::boolean = true"
```

### Rollback recipe

A.3.6 revert: `git revert --no-edit <hash>` — single holding commit, all
6 files revert atomically. Cluster falls back to A.3.5 state where
action_server instantiates but no jobs are subscribed. mission_planner_01
would also revert from planner-net to default `bridge` — but since A.3.5
doesn't actually try to connect to NATS, that's harmless.

---

## State at end of 2026-05-05 — B.2.A.3.5 DONE (action_server instantiates against live pg)

A.3.5 landed as **one holding commit** covering the db_file→pg_conn API
unification across the planner runtime chain plus a main.lua wiring slice
that actually instantiates action_server post-infra-discovery. Smoke
green: container logs `action_server instantiated:
nats_server=nats://nats-js-ram:4222`, heartbeat fresh, peers ACTIVE, zero
SYS_EXCEPTIONs.

Continue.md's pre-session handoff anticipated "~7 call sites, 1 session"
for the refactor — actual scope was **9 files / ~25 edit points** because
the parameter flowed through 5 runtime constructors (action_server →
global_planner / sequencer → hub_runtime / mission → kb_runtime). Plus
kb_runtime's positional-arg signature change (string→table at slot 1).
Still one session of focused work; just bigger blast radius than
continue.md's bullet-count suggested.

| Slice | Scope |
|---|---|
| `(this commit)` | **A.3.5** — Renamed `db_file` (string, sqlite path) → `pg_conn` (table, pg conn params) at action_server, global_planner, sequencer, hub_runtime, mission opts; kb_runtime first positional arg same change. kb_query.new signature updated (both architectural twin + container-vendored copy). Latent bug fixed: kb_query was calling `KBM.new(...,nil,true)` with `true` at slot 4 — KBM's signature is 3-arg, slot 4 was silently ignored, meaning kb_query was creating tables on every connect (now `KBM.new(...,true)` at slot 3). main.lua builds pg_conn from PG_HOST/PORT/DB/USER/PASSWORD env, instantiates action_server with `{pg_conn, site, nats_server}` after NATS infra_discovery. Image rebuilt 193MB, respawned via `docker rm -f` + node_control reconcile (~5s). |

### A.3.5 smoke results (2026-05-05 evening)

| Check | Result |
|---|---|
| `planner libs loaded: ... action_server=ok` | ✓ (was already green from A.3.4) |
| `infra nats host=nats-js-ram port=4222 healthy=true age=4s` | ✓ |
| `infra mqtt host=mosquitto-ram-ws_main port=1883 healthy=true age=4s` | ✓ |
| **`action_server instantiated: nats_server=nats://nats-js-ram:4222`** | ✓ ← A.3.5 milestone |
| heartbeat tick=8 age=477ms | ✓ |
| peer states cpu_01/cpu_02 | ACTIVE / ACTIVE |
| active SYS_EXCEPTIONs | 0 |

### Three deferred V-heavy blockers discovered during A.3.5

A.3.5 made `action_server.new()` instantiate clean. **Mission DISPATCH
still hits multiple landmines** — none affect A.3.5's instantiation +
NATS-subscribe smoke, all gate B.2.A.5 V-heavy completion path.
Documented here so the next session doesn't re-derive them.

1. **kb_runtime.lua body still sqlite-coded.** Constructor signature is
   pg-correct after A.3.5 (assert table + 3-arg KBM call), but the body
   uses `self.db = self.kb.db` and `sqlite3_helpers` — pg KBM has neither.
   First mission dispatch crashes at `kb_rt:merge_status` /
   `kb_rt:write_heartbeat` (called from mission.lua's start/action_start/
   action_complete/action_failed/finish). Either rewrite to use KBM ltree
   writes against pg `knowledge_base_status` / `knowledge_base_stream`
   tables, or drop kb_rt entirely from mission.lua and go fully NATS-only
   for telemetry (the comment at top of mission.lua already declares
   "Live telemetry flows through NATS JetStream KV ... Durable records go
   to SQLite via kb_runtime" — the durable side is the only thing
   broken).

2. **kb_query positional args are v2-shaped at all 5 call sites.** v3
   kb_query.new signature is `(pg_conn, system_name, site, own_instance_id)`
   but every upstream caller passes the v2 shape `(db_file,
   "knowledge_base", ltree_path, site)`:
   - action_server.lua:80 (constructor pcall — silent fail OK)
   - action_server.lua:257, 878 (execute_mission — would crash)
   - global_planner.lua:93 (mission planning — would crash)
   - sequencer.lua:77 (only reached when opts.site is nil; dead in
     current chain because action_server always provides site)
   - hub_runtime.lua:90 (only reached when opts.pg_conn truthy +
     constructor invoked; dead at A.3.5 instantiation since
     hub_runtime is constructed by sequencer at execute_mission time)

   The constructor pcall at action_server.lua:80 masks this — it's why
   instantiation succeeded today. The two unwrapped calls in
   execute_mission would fail loudly. Fix: thread system_name +
   own_instance_id through every constructor opts, drop ltree_path
   (not needed by pg KBM), update each call site.

3. **sequencer.lua:77 calls `q:get_site()` which v3 kb_query doesn't
   define.** Dead path (action_server always passes opts.site so the
   `if not opts.site` branch never fires) but worth deleting on the next
   sweep through sequencer to avoid surprise during a future refactor.

### Layer ordering

```
Phase B done: M-2 ✅ → O ✅ → F ✅ → A-pre ✅ → A ✅ → I ✅ → N ✅ → V ✅
Phase B.2 in progress:
  A.1 ✅ → A.2 ✅ → A.3.1 ✅ → A.3.2 ✅ → A.3.3 ✅ → A.3.3b ✅ → A.3.4 ✅ →
  A.3.5 ✅ →
  A.3.6 (NATS subscribe + skeleton handler — next session) →
  A.4 → A.5 (= V-heavy, gated on kb_runtime body port + kb_query call-args fix)
Then queued: N+1 (topology + slicer simplify), file-store loader, three-tier config
```

### **First action next session — A.3.6 NATS subscribe + skeleton handler (~½ session)**

`action_srv` already exists in main.lua (post-A.3.5). Goal: subscribe to
`{site}.action_server.missions` via the JobQueue API and log the mission
JSON. **Do NOT actually dispatch** — that hits the kb_runtime landmine.

**Step-by-step:**

1. **Read action_server.lua's existing serve loop** to understand how it
   normally subscribes via JobQueue (lines around `M:serve` /
   `M:_consume_jobs` — needs to be located). Decide whether to:
   (a) call `action_srv:serve()` and let it do its own subscribe + dispatch
   (will crash at first mission), OR
   (b) subscribe via a minimal `nats_jq.JobQueue.new` directly in main.lua
   with a custom log-only handler. Cleaner; doesn't fight the action_server
   internals.

   Recommendation: **(b)** — main.lua owns the subscribe, log-only handler
   ack's the message, action_server stays uninstantiated for serve.
   Touches no library code; pure additive ~30 lines.

2. **Construct a minimal JobQueue:**
   ```lua
   local jq = nats_jq.JobQueue.new({
       server      = nats_url,
       stream      = "action_server_missions",  -- TBD; check upstream JobQueue defaults
       subject     = APP_SITE .. ".action_server.missions",
       client_name = "planner_jq_" .. CONTAINER_NAME,
   })
   jq:connect()
   ```

3. **Run a tick loop** that does a non-blocking `jq:claim(timeout_ms)`
   (or whatever the API is — read `nats_job_queue.lua` first). On
   message: `logf("mission received: %s", payload_json)`, then ack
   (`jq:complete(job)` or similar — TBD).

4. **Mock-publish from host:**
   ```bash
   docker exec nats-js-ram nats pub \
       moon_base_alpha.action_server.missions \
       '{"robot_id":"rover_1","class_name":"drive_base","board":"landing_zone"}'
   ```

5. **Acceptance:** container log shows `mission received: {"robot_id":...}`
   within 1-2 seconds of publish; container doesn't crash; heartbeat keeps
   ticking.

**Pre-emptive watch-outs:**
- JobQueue API surface unread — read `lib/lib/nats_job_queue.lua` before
  writing the wrapper code. Don't guess method names.
- JetStream stream may need to be created before subscribe (JobQueue
  might do this on `connect()`; might not). Check the upstream `KeyStore`
  pattern in nats_key_store.lua for the `create_bucket=true` analogue.
- Co-existence with the heartbeat loop: jq:claim with a short timeout
  inside the existing `while true` loop, OR a coroutine. Probably simplest
  to interleave with a 100-200ms claim timeout per heartbeat tick.
- per `feedback_luajit_signal_safety`: JobQueue uses libnats sync sub
  (poll-based, not async callback) per the upstream design that A.3.1
  vendored — should be safe. Verify.

### Quick start-of-session check (verifies A.3.5 still soaks)

```bash
docker logs mission_planner_01 2>&1 | grep "action_server instantiated" | tail -1
# expect: ... nats_server=nats://nats-js-ram:4222

pgrep -af "dcs\.lua" | grep -v claude
docker ps --format '{{.Names}}\t{{.Status}}' | grep mission_planner_01

docker exec pg-vector psql -U gedgar -d knowledge_base -tAc \
  "SELECT (data::jsonb->'value'->>'tick')::int AS tick,
          extract(epoch from now())*1000 - (data::jsonb->'value'->>'at')::bigint AS age_ms
   FROM knowledge_base_status
   WHERE path::text ~ 'mission_planner_01.runtime.heartbeat.KB_STATUS_FIELD.snapshot'"

docker exec pg-vector psql -U gedgar -d knowledge_base -tAc \
  "SELECT count(*) FROM knowledge_base_status \
   WHERE path::text ~ 'SYS_EXCEPTION' AND (data->>'status')::boolean = true"
```

### Rollback recipe

A.3.5 revert: `git revert --no-edit <hash>` — single holding commit, all
9 files revert atomically. Cluster falls back to A.3.4 state where
action_server chunk-loads but doesn't instantiate. Cluster soak survives
the revert because the heartbeat tick loop is independent of action_server
instantiation.

---

## State at end of 2026-05-05 — B.2.A.3.4 DONE (full planner library tree imports clean)

Most of B.2.A.3 landed today (4 sub-commits) plus A.1, A.2 earlier. The
planner library tree is now structurally reachable inside
mission_planner_01: 50+ files, NATS + MQTT FFI vendored, action_server
chunk loads with `action_server=ok` in main.lua's smoke log. Stopping
short of A.3.5 because integration revealed an API mismatch worth
designing properly rather than papering over.

| Commit | Slice | Scope |
|---|---|---|
| `54664307` | **A.3.1** | Vendor NATS .so files (libnats + libnats_key_store + libnats_job_queue) into mission_planner image. Decision: per-app `prebuilt_libs/` for now; promote to luajit-base when 2nd NATS app lands. Apt: libcjson1. |
| `75c5c671` | **A.3.2** | Vendor NATS lua wrappers under `planner/lib/lib/` to preserve upstream `lib.nats_*` require namespace. Skip nats.lua entrypoint (action_server uses key_store + job_queue directly). |
| `f9cb40cf` | **A.3.3** | Copy 7 remaining runtime/ files; ct_loader_pure + ks_blackboard smoke-load. Extend package.path to reach json_util in image. |
| `c57a616c` | **A.3.3b** | Vendor MQTT FFI (libmqtt_pubsub + liblua_cbor) + lua wrappers. Apt: libmosquitto1. mqtt_transport smoke-loads. |
| `1a30c6a0` | **A.3.4** | Bulk import: 49 files. local_planner/lib (3) + global_planner/lib (3) + action_server/lib (2) + hub_dsl/* (30) + KBM (1) + kb_query (1, from package-root lua/). main.lua package.path now spans 8 entries. action_server chunk loads clean. |

### B.2.A so far

| Slice | Status | Commit |
|---|---|---|
| B.2.A.1 (skeleton + heartbeat) | ✅ | `e65efba5` |
| B.2.A.2 (lib/ + first 2 files) | ✅ | `0c69a6fa` |
| B.2.A.3.1 (.so vendoring) | ✅ | `54664307` |
| B.2.A.3.2 (NATS lua wrappers) | ✅ | `75c5c671` |
| B.2.A.3.3 (remaining runtime/) | ✅ | `f9cb40cf` |
| B.2.A.3.3b (MQTT FFI vendoring) | ✅ | `c57a616c` |
| B.2.A.3.4 (hub_dsl + action_server) | ✅ | `1a30c6a0` |
| B.2.A.3.5 (wire NATS subscribe) | BLOCKED on API design | -- |
| B.2.A.4 (V-heavy Phase 1) | TBD | -- |
| B.2.A.5 (V-heavy Phase 2) | TBD | -- |

### Blocker discovered in A.3.5 (handoff for next session)

**action_server's constructor expects `db_file = "surface_ops.db"` (a
sqlite file path) but v3 kb_query's KBM-backed query layer expects a
`connection_params` table (host/port/dbname/user/password) for pg.**
The two haven't been reconciled — Layer A.2's kb_query refactor changed
the data layer but action_server's caller-facing API is still v2-shaped.

**Specifics:**
- `action_server.new(opts)` requires `opts.db_file` and stores it as
  `self.db_file`. It then uses `self.db_file` as the FIRST argument to
  `kb_query_mod.new(self.db_file, "knowledge_base", self.ltree_path, self.site)`
  (action_server.lua line 80, 257, 338, 878, etc.).
- v3 kb_query.new(db_file, system_name, site, own_instance_id) — itself
  recently refactored — passes `db_file` to KBM.new("knowledge_base",
  db_file, nil, true). But KBM in v3 is `commissioning_software/kb/postgres/construct_kb/knowledge_base_manager.lua`
  which expects `connection_params` (a TABLE), NOT a string path.
- So kb_query.new() with a path string would already explode at
  `KBM.new("knowledge_base", db_file_string, nil, true)` because KBM's
  assertion is `assert(type(connection_params) == "table", ...)`.

**This isn't a Phase B.2.A.5 wiring problem; it's a Phase B.2.A
mid-layer design call.** Three options:

1. **Refactor action_server to take connection_params instead of
   db_file**, propagate the rename through every kb_query.new call site.
   Largest blast radius (action_server is 1330 lines; ~7 call sites).
   Cleanest end-state.
2. **Refactor v3 kb_query.new to accept a string AND look up
   connection_params from somewhere**. Smaller blast radius. Hides
   coupling.
3. **Add a "db_file → connection_params" translation layer** (e.g., a
   per-container config file that maps a logical name to pg conn
   params, exposed via a new `kb_query.connect_by_name("knowledge_base")`
   helper). Most flexible; biggest design surface.

Recommendation: **(1)** plus a tiny pg-config helper so `db_file` arg
becomes `pg_conn` (a table), constructed once in main.lua from
infra_discovery.lookup("postgres") + the env vars planner already has.

Estimated effort: 1 session of focused refactor + smoke retest.

### **First action next session — A.3.5 design call**

1. Decide between options 1 / 2 / 3 above.
2. Implement; smoke = `action_server=ok` AND
   `srv = action_server.new({pg_conn = ..., site = ..., nats_server = ...})`
   succeeds (instantiation, not just chunk-load).
3. Then continue A.3.5 proper: NATS connect + subscribe to
   `{site}.action_server.missions` + skeleton handler logs the mission
   JSON. Mock-publish via `nats pub` from host or `docker exec
   nats-js-ram nats pub ...`.

### Quick start-of-session check (verifies the soak)

```bash
docker logs mission_planner_01 2>&1 | grep "planner libs loaded" | tail -1
# expect: ... action_server=ok

pgrep -af "dcs\.lua" | grep -v claude
docker ps --format '{{.Names}}\t{{.Status}}' | grep mission_planner_01

docker exec pg-vector psql -U gedgar -d knowledge_base -tAc \
  "SELECT (data::jsonb->'value'->>'tick')::int AS tick,
          extract(epoch from now())*1000 - (data::jsonb->'value'->>'at')::bigint AS age_ms
   FROM knowledge_base_status
   WHERE path::text ~ 'mission_planner_01.runtime.heartbeat.KB_STATUS_FIELD.snapshot'"

docker exec pg-vector psql -U gedgar -d knowledge_base -tAc \
  "SELECT count(*) FROM knowledge_base_status \
   WHERE path::text ~ 'SYS_EXCEPTION' AND (data->>'status')::boolean = true"
```

### Layer ordering

```
Phase B done: M-2 ✅ → O ✅ → F ✅ → A-pre ✅ → A ✅ → I ✅ → N ✅ → V ✅
Phase B.2 in progress:
  A.1 ✅ → A.2 ✅ → A.3.1 ✅ → A.3.2 ✅ → A.3.3 ✅ → A.3.3b ✅ → A.3.4 ✅ →
  A.3.5 (blocked on db_file vs pg_conn API design call) → A.4 → A.5 (= V-heavy)
Then queued: N+1 (topology + slicer simplify), file-store loader, three-tier config
```

---

## State at end of 2026-05-05 (later) — B.2.A.2 DONE (planner lib/ wired)

| Commit | Slice | Scope |
|---|---|---|
| `0c69a6fa` | **B.2.A.2** | `/opt/apps/planner/lib/` namespace via `package.path`; first 2 no-dep runtime libs (`fn_registry.lua`, `kv_writer.lua`) imported from `building_blocks/ros_planner_ii/runtime/`. main.lua logs `planner libs loaded: fn_registry=ok kv_writer=ok` at startup. Heartbeat snapshot still updating each tick. |

Validated the LUA_PATH plumbing for the planner package. Other runtime
files (`link_*`, `mqtt_*`, `ks_blackboard`, `ct_loader_pure`,
`queue_monitor`) need the FFI dep chain (libmqtt_pubsub.so,
libnats_key_store.so, json_util resolution) and land together in A.3.

### **Next session — B.2.A.3: action_server + hub_dsl + NATS .so vendoring**

This is the **biggest** B.2 slice, genuinely 1.5-2 hours focused work.
Hard architectural decision in step 1; defer-no-further. Landing it as
ONE commit (per `feedback_holding_commits` "one layer = one commit")
because the parts are coupled (action_server ↔ NATS ↔ hub_dsl).

**Step-by-step:**

1. **Decide where NATS .so files live.** Current state: `building_blocks/knowledge_base/nats/` has `libnats_pubsub.so`, `libnats_kb_store.so`, etc. Mission-planner needs `libnats.so*`, `libnats_key_store.so`, `libnats_job_queue.so` (action_server uses KV + JobQueue, NOT pubsub per `project_phase6_transport` memory's note about pubsub being unsafe). Three placement options:

   | Option | Pros | Cons |
   |---|---|---|
   | (a) `luajit_base/prebuilt_libs/` | Reusable across all apps | +10MB to base image; affects everyone |
   | (b) `mission_planner/container/lib/` | Targeted; only planner ships NATS | Duplicate when next NATS app lands |
   | (c) `openresty_base/prebuilt_libs/` | Half-way; only OpenResty-derived apps | Awkward layering; openresty-base shouldn't know about NATS |

   **Recommendation: (b) for now**, promote to (a) when a second NATS-using app lands. mission_planner/container/Dockerfile gets a COPY for the 7 .so files.

2. **Copy remaining runtime/ libs** (7 files: `link_client`, `link_manager`, `mqtt_transport`, `mqtt_hub_transport`, `ks_blackboard`, `queue_monitor`, `ct_loader_pure`) into `planner/lib/`. Resolve `json_util` require — either symlink/copy or extend package.path to `/usr/local/share/lua/5.1/chain_tree/lua_dsl/luajit_pipeline/?.lua` (already in image).

3. **Vendor NATS lua wrappers**: `nats_key_store.lua` + `nats_job_queue.lua` + `nats.lua` (entrypoint) from `nano_data_center_base/commissioning_software/kb/nats/lib/` into `planner/lib/`. Skip pubsub/rpc/stream — action_server doesn't use them.

4. **Copy hub_dsl/* (30 files)** into `planner/`. The hub_dsl directory has its own structure (kb_construct/, hub_functions/, kb/, protocol/). Decide whether to vendor verbatim or restructure. Default: vendor verbatim under `planner/hub_dsl/` to keep it disposable. Build script `hub_dsl/build.sh` produces `hub.json` (chain-tree IR); commit the IR alongside the source so app boot doesn't need to compile.

5. **Copy action_server/lib/{action_server,mission_builder}.lua** into `planner/lib/`. action_server is 38KB — read it carefully for require()s before wiring.

6. **Wire main.lua**: after pg + infra_discovery, instantiate action_server with `nats_server = "nats://" .. infra.nats.host .. ":" .. infra.nats.port`, call `:start()` (or whatever its main loop entry is), then enter the heartbeat tick loop concurrently.

7. **Smoke**: planner connects to NATS jetstream + sets up KV bucket; mock-publish a mission via `nats pub` CLI; observe logs show the mission JSON received.

**Pre-emptive watch-outs:**
- `feedback_luajit_signal_safety`: nats_pubsub.lua's ffi.cast async callback PANICs. Action_server uses key_store + job_queue (not pubsub) — likely safe but verify before committing.
- `project_phase6_transport`: NATS deferred at v3 6.4 due to that signal-safety issue. Re-evaluate whether KV+JQ are clean.
- Multi-arch: `prebuilt_libs/` files might need both linux/amd64 and linux/arm64 builds depending on Docker Desktop platform (WSL2 = amd64; Pi target = arm64).

### Quick start-of-session check (verifies B.2.A.2 still soaks)

```bash
export NDC_BASE=/home/gedgar/knowledge_base_assembly/luajit_programs_and_containers/nano_data_center_base
DEP=$NDC_BASE/commissioning_software/system_node_control/deployment

pgrep -af "dcs\.lua" | grep -v claude
docker ps --format '{{.Names}}\t{{.Status}}' | grep mission_planner_01

# planner libs loaded log line on each container start
docker logs mission_planner_01 2>&1 | grep "planner libs loaded" | tail -1

# heartbeat freshness
docker exec pg-vector psql -U gedgar -d knowledge_base -tAc \
  "SELECT (data::jsonb->'value'->>'tick')::int AS tick,
          extract(epoch from now())*1000 - (data::jsonb->'value'->>'at')::bigint AS age_ms
   FROM knowledge_base_status
   WHERE path::text ~ 'mission_planner_01.runtime.heartbeat.KB_STATUS_FIELD.snapshot'"

# peers + zero exceptions
docker exec pg-vector psql -U gedgar -d knowledge_base -tAc \
  "SELECT path::text, data->>'state' FROM knowledge_base_status \
   WHERE path::text LIKE '%peer_state%' ORDER BY path"
```

### Layer ordering

```
Phase B done: M-2 ✅ → O ✅ → F ✅ → A-pre ✅ → A ✅ → I ✅ → N ✅ → V ✅
Phase B.2 in progress:
  B.2.A.1 ✅ -> B.2.A.2 ✅ -> B.2.A.3 (next, BIG) -> B.2.A.4 -> B.2.A.5 (= V-heavy)
Then queued: N+1 (topology + slicer simplify), file-store loader, three-tier config
```

### B.2.A so far

| Slice | Status | Commit |
|---|---|---|
| B.2.A.1 (skeleton + heartbeat) | ✅ | `e65efba5` |
| B.2.A.2 (lib/ + first 2 files) | ✅ | `0c69a6fa` |
| B.2.A.3 (action_server + hub_dsl + NATS) | NEXT | -- |
| B.2.A.4 (V-heavy Phase 1) | TBD | -- |
| B.2.A.5 (V-heavy Phase 2) | TBD | -- |

---

## State at end of 2026-05-05 (later) — B.2.A.1 DONE (planner runtime skeleton)

After Phase B closed out (Layer V lightweight green earlier today), Phase
B.2 started with **B.2.A.1: planner runtime skeleton + framework
runtime.heartbeat pre-allocation** (`e65efba5`). Smoke green:
mission_planner_01 now writes a `runtime.heartbeat.KB_STATUS_FIELD.snapshot`
row every 5s carrying `{at, host, cpu, ui_port, tick}`. V-heavy pre-2
("heartbeat_at <10s old") finally satisfied — was the lockout for
running the heavy NATS mission test. NATS + MQTT addressing both
discovered healthy via `infra_discovery.lookup`, ready for B.2.A.2.

| Commit | Slice | Scope |
|---|---|---|
| `e65efba5` | **B.2.A.1** | apps_builder_framework's driver.lua now emits `runtime.heartbeat.snapshot` (universal liveness field every app gets); `ndc_paths.app_runtime_heartbeat_path(site, container)` helper; planner main.lua replaces the v2 heartbeat shell with pg-connect + infra_discovery + 5s heartbeat write loop. 25/25 framework driver + 23/23 validator tests green. |

### B.2.A.1 smoke results (2026-05-05 17:20Z)

| Check | Result |
|---|---|
| Framework pre-allocation row in pg | ✓ default `{at:0,host:"",cpu:"",ui_port:0}` |
| Planner pg connect | ✓ first attempt |
| Planner infra_discovery for nats | ✓ host=nats-js-ram, port=4222, healthy |
| Planner infra_discovery for mqtt | ✓ host=mosquitto-ram-ws_main, port=1883, healthy |
| `runtime.heartbeat.snapshot` updates | ✓ tick increasing, age 1.7s |
| Cluster health post-rebuild | ✓ peers ACTIVE; 0 SYS_EXCEPTIONs; all apps respawned cleanly |

Operational gotcha discovered + fixed during the smoke: **openresty-base
+ luajit-base images had stale staged `ndc_paths.lua` and were missing
`infra_discovery.lua`** (built before today's helper additions). Image
rebuild order when staged libs change: `luajit-base` → `openresty-base`
→ each app image (mission-planner here). Add to runbook.

### **Next session — B.2.A.2: import runtime/ libraries + NATS client**

The planner shell can now write KB rows but has no protocol clients.
B.2.A.2 brings in the link layer (MQTT-based robot transport) + NATS
client for receiving missions:

1. **Copy** `building_blocks/ros_planner_ii/runtime/*.lua` (9 files:
   `link_client`, `link_manager`, `mqtt_transport`, `mqtt_hub_transport`,
   `kv_writer`, `queue_monitor`, `ks_blackboard`, `ct_loader_pure`,
   `fn_registry`) into
   `nano_data_center_instance/app_containers/mission_planner/container/planner/lib/`.
2. **Add NATS client lib**. Options: (a) pure-lua NATS client (small;
   ~15KB if available), (b) ffi to `libnats.so` (heavier; needs system
   package). Decide first; v2 cluster used MQTT-only for telemetry per
   `feedback_telemetry_routing` memory but NATS for action_server
   per the manifest's `nats_protocol` declaration. Likely already in
   building_blocks somewhere — `grep -rln "nats" building_blocks/ros_planner_ii/`.
3. **Bundler**: docker_build.sh uses `bundle_controller`; verify the
   chain-tree IR and per-process manifests still compile after files
   land in lib/.
4. **Wire** NATS connect (in main.lua's startup, after infra_discovery)
   + skeleton subscription to `{site}.action_server.missions` with a
   no-op handler that just logs the mission JSON. Validates connect
   path; mission state machine waits for B.2.A.3.

Estimated time: 1–2 hours of focused work, depending on whether NATS
client lib is already in building_blocks or needs vendoring.

### Remaining B.2.A slices (after B.2.A.2)

- **B.2.A.3**: import `action_server/lib/{action_server,mission_builder}.lua`
  + `hub_dsl/*` (30 files; chain-tree IR for mission state machine
  compiles to `hub.json`). Wire action_server's NATS handler. ~1 session.
- **B.2.A.4**: V-heavy Phase 1 (rejection). Test client publishes mock
  mission for class drive_base; observe `submitted` → `rejected_no_robot`.
  ~½ session.
- **B.2.A.5**: V-heavy Phase 2 (completion). Start mqtt_robot fixture
  from building_blocks; observe full pipeline. ~½ session.

Total Phase B.2.A budget: 3–4 sessions remaining after B.2.A.1.

### Quick start-of-session check (verifies B.2.A.1 still soaks)

```bash
export NDC_BASE=/home/gedgar/knowledge_base_assembly/luajit_programs_and_containers/nano_data_center_base
DEP=$NDC_BASE/commissioning_software/system_node_control/deployment

pgrep -af "dcs\.lua" | grep -v claude
docker ps --format '{{.Names}}\t{{.Status}}' | grep mission_planner_01

# planner heartbeat freshness (should be <10s)
docker exec pg-vector psql -U gedgar -d knowledge_base -tAc \
  "SELECT (data::jsonb->'value'->>'tick')::int AS tick,
          extract(epoch from now())*1000 - (data::jsonb->'value'->>'at')::bigint AS age_ms
   FROM knowledge_base_status
   WHERE path::text ~ 'mission_planner_01.runtime.heartbeat.KB_STATUS_FIELD.snapshot'"

# peers ACTIVE + zero exceptions
docker exec pg-vector psql -U gedgar -d knowledge_base -tAc \
  "SELECT path::text, data->>'state' FROM knowledge_base_status \
   WHERE path::text LIKE '%peer_state%' ORDER BY path"
docker exec pg-vector psql -U gedgar -d knowledge_base -tAc \
  "SELECT count(*) FROM knowledge_base_status \
   WHERE path::text ~ 'SYS_EXCEPTION' AND (data->>'status')::boolean = true"
```

### Layer ordering

```
Phase B done: M-2 ✅ → O ✅ → F ✅ → A-pre ✅ → A ✅ → I ✅ → N ✅ → V ✅
Phase B.2 in progress:
  B.2.A.1 ✅ -> B.2.A.2 (next) -> B.2.A.3 -> B.2.A.4 -> B.2.A.5 (= V-heavy)
Then queued: N+1 (topology + slicer), file-store loader, three-tier config
```

---

## State at end of 2026-05-05 session — Phase B CODE-COMPLETE (Layer V lightweight green)

Layer V landed in lightweight form per the layer table at line 859
("Boot from scratch; verify anchor populated; verify UI proxied; verify
cross-discovery query works"). The heavy Q3-locked mission lifecycle
test (NATS rejection + completion paths) requires the real planner
runtime; mission_planner_01's `container/planner/main.lua` is still the
v2 heartbeat shell, so that test is **deferred to Phase B.2** with the
"Real planner library import" work-item. Phase B layer ordering now
reads M-2 ✅ → O ✅ → F ✅ → A-pre ✅ → A ✅ → I ✅ → N ✅ → V ✅.

### Layer V smoke results (2026-05-05 17:55Z)

| Check | Result |
|---|---|
| **Pre-1** spec.manifest.version=1.0 + class=mission_planner in pg | ✓ |
| **Pre-2** runtime.heartbeat_at < 10s old | ✗ (planner shell does not write runtime.* — feature owned by real planner; Phase B.2) |
| **Pre-3** GET http://localhost:19005/ direct | HTTP 200 |
| **V.1** anchor populated (manifest + placement rows in pg) | ✓ verified by N-smoke; 11 manifest rows + 2 placement rows |
| **V.2** UI proxied via dcs_console gateway | ✓ landing lists `mission_planner_01/ui`; `GET /ui/mission_planner_01/ui/` → HTTP 200, 0 redirects. Routes via CONTAINER_REGISTRY (written by node_control's REGISTER), cleanly decoupled from the runtime.* gap. |
| **V.3** cross-discovery KB query | ✓ `data::jsonb @> '["drive_base"]'::jsonb` containment query finds mission_planner_01; wider sibling-app join (manifest class + capabilities + nats port + placement.cpu) returns one row with all four fields. |

### What "Phase B code-complete" means

Phase B's structural work is done:
- Namespace migration (M-1, M-2) ✅
- Apps-builder framework (F) ✅
- KB-driven infra discovery (A-pre) ✅
- Mission planner ported under v3 anchor (A) ✅
- Apps-builder pipeline live (I) ✅
- Placement-driven node_control (N) ✅
- Anchor-populated + UI-proxied + cross-discovery verified (V) ✅

What's pending is **Phase B.2** content:
- Real planner library import (action_server + hub_dsl + local_planner +
  global_planner + runtime) into `container/planner/`. Current shell
  ticks but doesn't subscribe to NATS or write runtime.* rows.
- Heavy NATS mission test (Q3 lock at lines 353-385): rejection path
  with no robot, then completion path via the
  `building_blocks/ros_planner_ii_mqtt_robot/` fixture. Re-runnable as
  the lockedQ3 acceptance once the real planner is in place.
- File-store loader (own design surface; sha256 + ltree + class registry)
- Three-tier config loader
- README/runbook references still mentioning `ros_mission_planner_ii_01`
  (cosmetic; rolling cleanup)

### Deferred N+1 sub-layer (still queued)

continue.md's original Layer N spec also called for **topology
restructure** (drop application instances from `cpus[*].instances`) and
**bootstrap.db slicer simplify** (drop per-CPU container list for
app-kind). Held off because they reshape build_kb output and risk
breaking the chain-tree's initial KB activation. Land as a follow-up
"N+1" sub-layer once placement-driven boot has soaked.

### **First action next session — pick from Phase B.2**

Top three candidates (rough effort estimate in parens):

1. **Real planner library import** (1.5–2 sessions) — biggest unlock; gates
   the Q3-locked NATS mission test. Touches `container/planner/main.lua`
   bring-up, NATS subscribe wiring, runtime.* row writes, action_server
   integration. Once green, queue Q3 mission test as V-heavy.
2. **N+1 topology restructure + bootstrap slicer simplify** (½ session) —
   smaller; closes the placement-rewire loop architecturally. Drops app
   instances from `cpus[*].instances`, simplifies bootstrap.db. Low risk
   if soak across overnight stays clean.
3. **File-store loader** (1 session) — independent design surface;
   sha256 blobs + ltree paths + class registry. Schema exists in pg per
   `project_file_store` memory; runtime loader pending.

Recommend **(1)** first — biggest application-side payoff and unblocks
the heavy V test that was originally scoped for this session.

### Quick start-of-session check

```bash
export NDC_BASE=/home/gedgar/knowledge_base_assembly/luajit_programs_and_containers/nano_data_center_base
DEP=$NDC_BASE/commissioning_software/system_node_control/deployment

# 1. processes still 1 dcs.lua per CPU?
pgrep -af "dcs\.lua" | grep -v claude

# 2. peer states ACTIVE + system_ready=1?
docker exec pg-vector psql -U gedgar -d knowledge_base -tAc \
  "SELECT path::text, data FROM knowledge_base_status \
   WHERE path::text LIKE '%peer_state%' OR path::text LIKE '%system_ready' \
   ORDER BY path"

# 3. mission_planner_01 still up + reachable through gateway?
docker ps --format '{{.Names}}\t{{.Status}}' | grep mission_planner_01
curl -s -o /dev/null -w "gateway HTTP %{http_code}\n" --max-time 5 \
  http://localhost:19003/ui/mission_planner_01/ui/

# 4. zero active SYS_EXCEPTIONs
docker exec pg-vector psql -U gedgar -d knowledge_base -tAc \
  "SELECT count(*) FROM knowledge_base_status \
   WHERE path::text ~ 'SYS_EXCEPTION' AND (data->>'status')::boolean = true"
```

### Layer ordering

```
Phase B: M-2 ✅ → O ✅ → F ✅ → A-pre ✅ → A ✅ → I ✅ → N ✅ → V ✅
Phase B.2: real planner port (1.5–2s) → V-heavy NATS test (½s)
            file-store loader (1s) → three-tier config (½s)
            N+1 topology + slicer simplify (½s)
v3 step 4 onward (gated on Phase B.2 completion)
```

### Layer V rollback (cosmetic only)

Layer V landed no source changes — verification only. No rollback needed.
The Layer N commits (`ead9fb83`, `f4b85304`) cover the actual code that
makes V pass; revert those if cluster regresses.

---

## State at end of 2026-05-05 session — Layer N DONE (placement-driven node_control live)

Layer N landed as two independently-revertible holding commits + a
continue.md done-state commit (this section). Cluster cold-booted on the
new placement query path; cpu_02's node_control now reads
`app_containers.<i>.placement.current.KB_STATUS_FIELD.cpu` from pg
instead of scanning `cpu.<my>.container.*`. mission_planner_01 picked up
correctly; legacy `ros_mission_planner_ii_01` removed.

| Commit | Slice | Scope |
|---|---|---|
| `ead9fb83` | **N.1** | apps_builder_framework's driver.lua emits `placement.current.{cpu, role}` after each kb_build. ndc_paths gains `app_containers_root()` + `app_placement_status_path()`. 23/23 driver + 23/23 validator tests green; row-count bumped 4 → 10 (5 per app x 2). |
| `f4b85304` | **N.2** | `kb_assignments.list_node_managed` flipped to placement-driven two-step: query placement.current.cpu = my_cpu, resolve service from `cpu.<my>.container.<i>.service.main` (untouched). Decouples placement from row locality. |

### Smoke results (2026-05-05 17:55Z)

| Check | Result |
|---|---|
| `[apps_builder] 1 apps committed` in build_kb | ✓ (mission_planner_01) |
| Placement rows in pg | `app_containers.mission_planner_01.placement.current.KB_STATUS_FIELD.cpu = "cpu_02"` + `.role = "active"` |
| slice_bootstrap | cpu_01=2014 rows, cpu_02=581 rows (Phase A baseline preserved) |
| cpu_02 NODE_READ_OWN_CONFIG | `1 assignment(s) [mission_planner_01]` (was previously empty + crashed-restart loop) |
| `mission_planner_01` launch | broker run -> d9f906ef5c0a, Up 33s |
| `ros_mission_planner_ii_01` | absent from `docker ps` (cleanup as part of cutover) |
| Both peers | ACTIVE; system_ready=1 |
| Heartbeats | cpu_01=2.8s, cpu_02=4.8s (well under 15s threshold) |
| Active SYS_EXCEPTIONs | 0 |
| Errors/panics in error.log | 0 |

### Deferred from continue.md's Layer N spec

continue.md's Layer N also called for **topology restructure** (drop
application instances from `cpus[*].instances`) and **bootstrap.db
slicer simplify** (drop per-CPU container list for app-kind). These
reshape build_kb output and risk breaking the chain-tree's initial KB
activation; landing them as a follow-up sub-layer (call it **N+1**)
keeps blast radius per commit small per `feedback_holding_commits`.

Current state: `cpu.<id>.container.<inst>.service.main` is still the
service-spec source of truth; placement just decides which CPU consults
which row. Topology still has apps in `cpus[*].instances` so build_kb
emits the per-CPU container subtrees. Bootstrap.db still includes them.
node_control reads placement first, then resolves service from the
per-CPU rows that still exist.

### **First action next session — Layer V (acceptance smoke; ½ session)**

Per Q3 lock (continue.md lines 353-385): tests BOTH rejection and
completion paths via the existing `building_blocks/ros_planner_ii_mqtt_robot/`
fixture. Pre-conditions:

1. pg row at `app_containers.mission_planner_01.spec.manifest.KB_STATUS_FIELD.version = "1.0"` ✓ (Layer A verified)
2. `app_containers.mission_planner_01.runtime.heartbeat_at` <10s old. **TBD: planner port's main.lua is still v2 heartbeat shell — needs runtime.heartbeat_at written under the v3 anchor before Phase 2 below.**
3. `GET http://localhost:19005/` returns 200 (planner_ui supervisor alive). Verify before Phase 1.

Phase 1 (rejection): publish mock mission for class `drive_base` to
`{site}.action_server.missions`; observe status KV `submitted` →
`rejected_no_robot`.

Phase 2 (completion): start `ros_planner_ii_mqtt_robot` from
`building_blocks/` with `site=moon_base_alpha` + mqtt addressing from
`infra_discovery.lookup("mqtt_broker")`; submit mission; observe
`submitted` → `dispatched` → `in_progress` → `completed`.

Optional cheap signal: rejection-on-unknown-class with robot online.

Out of scope (defer Phase B.2): manual web-UI testing, HTTP
`POST /api/missions`, hardware robot, energy edges.

### Layer ordering

```
M-2 ✅ → soak ✅ → O ✅ → F ✅ → A-pre ✅ → A ✅ → I ✅ → N ✅ → V
                                                              ↑ next
```

Phase B remaining: ½ session for V, plus the deferred N+1 topology /
slicer simplify (follow-up).

### Rollback recipe

Layer N revert (full): `git revert --no-edit f4b85304 ead9fb83` (newest
first). N.2 alone reverts node_control to scanning `cpu.<my>.container.*`
(legacy path; still works because rows still exist). N.1 alone removes
placement-row emission (harmless extras until N.2's read path needs them).

Note: today's `build_kb.sh` wrote placement rows to live pg. A revert +
fresh build_kb run cleans them up via construct_kb's reconciliation.

---

## State at end of 2026-05-04 session — Layer A + Layer I DONE (apps-builder pipeline live)

Layer A (4 commits) plus Layer I (1 commit) plus a continue.md done-state
commit landed today. End-to-end pipeline is now green: `build_kb.sh` →
apps_builder subsystem auto-discovers `mission_planner` → driver mirrors
`manifest.lua` to pg → 11 rows at the locked
`app_containers.mission_planner_01.spec.manifest.*` shape with correct
data in `knowledge_base.data`. Framework unit tests still 42/42 green.

| Commit | Layer | Scope |
|---|---|---|
| `092d67d7` | A.1 | git mv container shell to v3 home |
| `448d8e2b` | A.2 | kb_query.lua refactor + ndc_paths app_container helpers |
| `27e96866` | A.3 | manifest + container_spec + kb_build (sub-namespace decision) |
| `8d7872c2` | A.4 | catalog rename + image green at `nanodatacenter/mission-planner:latest` |
| `d8f352c6` | A done | continue.md done-state for Layer A |
| `a74014ab` | **I** | apps_builder subsystem (in-process, not container) |

### Important: live cluster is FROZEN on pre-A topology

Today's `build_kb.sh` runs wrote new `app_containers.*` rows to the LIVE
pg-vector. But `slice_bootstrap.sh` was NOT run, so cpu_02's `bootstrap.db`
still has the OLD topology entry `ros_mission_planner_ii_01 → ros_mission_planner_ii`.
node_control reads from bootstrap.db, so the cluster keeps the old
shell container running:

```
ros_mission_planner_ii_01    nanodatacenter/ros-mission-planner-ii:latest    Up 2 days
robot_manager_01             nanodatacenter/robot-manager:latest             Up 2 days
test_app_01, observability_01, dcs_console_01, ...                          Up 29 hours
```

**This is intentional.** Layer N is what converges the transition
(node_control reads placement from KB instead of bootstrap.db topology
list). The cluster will boot the new `mission_planner_01` container
once Layer N lands and `slice_bootstrap.sh + stage_deploy.sh` re-runs.

### What's in pg right now (verified by `SELECT path FROM knowledge_base WHERE path LIKE 'system.moon_base.site.moon_base_alpha.app_containers.mission_planner_01.%'`)

```
.../app_containers.mission_planner_01                                        ← anchor
.../app_containers.mission_planner_01.spec.manifest                          ← spec/manifest header
.../app_containers.mission_planner_01.spec.manifest.KB_STATUS_FIELD.class    = "mission_planner"
.../app_containers.mission_planner_01.spec.manifest.KB_STATUS_FIELD.version  = "1.0"
.../app_containers.mission_planner_01.spec.manifest.KB_JSONB_FIELD.capabilities      ← 4 entries
.../app_containers.mission_planner_01.spec.manifest.KB_JSONB_FIELD.virtual_nodes     ← 6 entries
.../app_containers.mission_planner_01.spec.manifest.KB_JSONB_FIELD.wire_formats      ← 2 entries
.../app_containers.mission_planner_01.spec.manifest.KB_JSONB_FIELD.ui_protocol       ← 6 endpoints
.../app_containers.mission_planner_01.spec.manifest.KB_JSONB_FIELD.nats_protocol     ← 6 subjects
.../app_containers.mission_planner_01.spec.manifest.KB_JSONB_FIELD.mqtt_protocol     ← 7 topics
.../app_containers.mission_planner_01.spec.manifest.KB_JSONB_FIELD.streams           ← []
```

Note: the `knowledge_base_status` satellite shows empty `{}` for these
rows — that's by design (satellites are runtime-mutation caches; spec
data lives in `knowledge_base.data`). See memory
`feedback_kb_status_satellite_empty.md`.

### **First action next session — Layer N (node_control reads placement from KB)**

Per Phase B layer table (continue.md ~line 558):
> **N — node_control reads placement from KB**: RECONCILE compares
> `placement.cpu` against `runtime.cpu`; stop+start on mismatch.
> Removes per-CPU container list from topology.lua; bootstrap.db slicer
> simplifies. Cluster boots; node_control on cpu_02 reads placement,
> finds planner assigned, starts it.

**N's pieces (in implementation order):**

1. **Add `placement.<name>` rows under app_containers** — apps_builder
   subsystem (or a new `placement` subsystem; TBD) writes
   `app_containers.<i>.placement.cpu = "cpu_02"` etc. Sourced from
   topology for now; rewritten by load balancer later.

2. **Audit bootstrap.db per-CPU container list** — what does
   slice_bootstrap.sh embed beyond container names? If just container
   names, we can drop the topology.cpus[*].instances list once N
   reads from KB. If anything else (port allocations, env vars), it
   needs migrating to `placement.*` KB rows too. Per locked design
   choices in continue.md: "Audit bootstrap.db per-CPU container list
   during N1".

3. **node_control's RECONCILE rewires** — current loop reads from a
   placement table built at supervisor startup; switch to reading
   `app_containers.<i>.runtime.<state_name>.KB_STATUS_FIELD.cpu` and
   comparing to `placement.<name>.KB_STATUS_FIELD.cpu`. Stop+start on
   mismatch.

4. **bootstrap.db slicer simplifies** — remove the per-CPU container
   list from the slice; per-CPU bootstraps now only need site +
   system_name + cpu_id + the schema (no container assignments).

5. **Smoke** — boot from scratch (stop cluster → build_kb →
   slice → stage → start → wait converge); verify on cpu_02 that
   node_control read placement from KB, found `mission_planner_01`
   assigned to cpu_02, started the new image, AND stopped the legacy
   `ros_mission_planner_ii_01` (which is no longer in the placement).
   Should converge within ~30s.

Compatible-mode discipline (locked Phase B choice): A first (planner
ports while node_control still reads bootstrap.db), N second. We're at
the transition. Layer N is the cutover commit. Smaller blast radius
per checkpoint.

Estimated 1 session for Layer N.

### Then Layer V (acceptance smoke; ½ session)

Per Q3 lock (continue.md lines 132+): rejection + completion paths via
the existing `building_blocks/ros_planner_ii_mqtt_robot/` fixture.
Pre-conditions: spec in pg, runtime heartbeat fresh, UI :8090 returns
200. Phase 1 (rejection no robot) → Phase 2 (start mqtt_robot from
building_blocks/ with site + mqtt config tweak; observe submitted →
completed).

### After Phase B (Phase B.2)

Carry-forward items deferred during Layer A:
- File-store loader (own design surface; sha256 + ltree + class registry)
- Real planner library import (current container is heartbeat shell;
  bring in action_server + hub_dsl + local_planner + global_planner from
  `building_blocks/ros_planner_ii/`)
- Three-tier config loader
- README/runbook references still mentioning `ros_mission_planner_ii_01`
  (cosmetic; rolling cleanup)

### Layer ordering after I

```
M-2 ✅ → soak ✅ → O ✅ → F ✅ → A-pre ✅ → A ✅ → I ✅ → N → V
                                                       ↑ next
```

### Rollback recipe

Layer I revert: `git revert --no-edit a74014ab`. apps_builder subsystem
disappears from build_kb.lua's SUBSYSTEMS list; ndc_paths app_container
helpers stay (introduced by A.2); kb_build.lua keeps reading manifest
from ctx.manifest (broken until reverted; revert A.3 too if needed).

Layer A revert (full): `git revert --no-edit 8d7872c2 27e96866 448d8e2b 092d67d7`
in newest-first order. Leaves a clean A-pre state.

Note: today's `build_kb.sh` runs wrote rows to live pg. A revert + fresh
build_kb run cleans them up via construct_kb's reconciliation (deletes
rows no longer specified).

---

## State at end of 2026-05-04 session — Layer A DONE (mission_planner ported)

Layer A landed as four independently revertible holding commits per
`feedback_holding_commits`. Mission planner now lives at the v3 path
`nano_data_center_instance/app_containers/mission_planner/` with locked
manifest/spec/kb_build files; image builds green at
`nanodatacenter/mission-planner:latest`. Apps-builder framework
end-to-end smoke green: driver → kb_build emits 11 rows at the locked
`app_containers.mission_planner_01.spec.manifest.*` shape. Live cluster
NOT touched yet (Layer I integrates).

| Commit | Slice | Scope |
|---|---|---|
| `092d67d7` | **A.1** | git mv container shell from `building_blocks/ros_mission_planner_ii/container/` to `nano_data_center_instance/app_containers/mission_planner/container/`. 6 files, blame preserved, zero edits. |
| `448d8e2b` | **A.2** | Refactor `kb_query.lua` per Q2.1: deleted v2-only `get_infrastructure`/`get_container_config`/`get_domain`/`get_board*`; simplified `get_site` into ctor args; added `get_app_spec_jsonb`/`get_app_spec_status`; added Q2.2 `FALLBACK_ROBOT_CLASSES` (drive_base entry; engages when `robot_manager_01` spec absent; single-place deletion path). Added 5 app-container path helpers to `ndc_paths.lua` (canonical + 2 gitignored staged copies synced). |
| `27e96866` | **A.3** | Authored `manifest.lua` (locked status scalars + 7 JSONB blobs) + `container_spec.lua` (validated by Layer F validator) + `kb_build.lua` (uses `add_info_node` for bare-Construct_KB + facade compatibility). Mid-layer design call: kb_build pushes `with_header("spec","manifest",...)` so paths land at `app_containers.<i>.spec.manifest.KB_*FIELD.*`. ndc_paths helpers renamed `app_spec_*` → `app_manifest_*` accordingly. |
| `8d7872c2` | **A.4** | Renamed `ros_mission_planner_ii` → `mission_planner` in `definitions.lua` + `topology.lua`; image tag → `nanodatacenter/mission-planner:latest`; nginx.conf listen 8080 → 8090 (matches manifest); cosmetic title strings. **Image builds green**, bundler registered planner + planner_ui processes. Standalone container start parses identity, loads chain-tree IR, exits cleanly when PG unreachable (expected for image-level smoke; runtime end-to-end is Layer I/V scope). |

### Mid-layer design decision (recorded in `27e96866` commit body)

The framework as shipped (Layer F) opens scope at `app_containers.<i>` and stops there. The locked manifest design called for sub-paths `spec.*`, `runtime.*`, `placement.*`. Three options considered:

1. Drop `.spec.` from literal paths (abandon design).
2. **CHOSEN: kb_build pushes its own `with_header("spec","manifest",...)`** — two-segment shape forced by `add_header_node`'s link+name pair contract; "spec" is the namespace marker, "manifest" is the catalog name. Future `spec.tunables` / `spec.placement_hints` coexist.
3. Extend the framework to push 3 sub-anchors (rejected: framework already shipped green; runtime/placement not written by apps-builder anyway).

`ndc_paths` exposes 3 helpers reflecting this:
- `app_manifest_status_path(site, container, name)` → `app_containers.<c>.spec.manifest.KB_STATUS_FIELD.<name>`
- `app_manifest_jsonb_path(site, container, key)`  → `app_containers.<c>.spec.manifest.KB_JSONB_FIELD.<key>`
- `app_runtime_status_path(site, container, state_name, field)` → `app_containers.<c>.runtime.<state_name>.KB_STATUS_FIELD.<field>`

### What's NOT done in Layer A (deferred to Layer I or later)

- **Apps-builder image + invocation pipeline** — Layer I builds the one-shot apps-builder container, composes framework + per-app kb_build functions at image build, wires it into `build_kb.sh` / `rebuild_and_start.sh`.
- **Real planner library import** — current `container/planner/main.lua` is still the v2 heartbeat shell. Importing the real `building_blocks/ros_planner_ii/` library (action_server + hub_dsl + local_planner + global_planner + runtime) into `/opt/apps/planner/` is Phase B.2 work; Layer A's checkpoint was "image builds green; not yet started".
- **node_control reads placement from KB** — Layer N work; current topology.lua-based bootstrap still drives node_control.
- **Layer V acceptance test** — needs Layer I + N first; mqtt_robot fixture standalone-runs from `building_blocks/` per Q3 lock.
- **README/runbook references** — `nano_data_center_base/README.md`, `support_procedures/runbooks/commissioning.md`, `platform_containers/observability/continue.md`, `system_node_control/PHASE6_DESIGN.md` still mention `ros_mission_planner_ii_01`. Cosmetic; rolling cleanup in Phase B.2.

### **First action next session — Layer I (apps-builder image + plumbing)**

Layer I assembles the apps-builder one-shot container that:
1. Composes `apps_builder_framework/` + every app's `kb_build.lua` + `manifest.lua` at image-build time.
2. Reads the placement table (currently `topology.lua`'s instances; future-N: from KB), iterates each `app_containers.<c>` placement, calls `driver.drive(kb, kb, placements)`.
3. Exits with non-zero on any kb_build failure (atomic-fail).

Wire it into `rebuild_and_start.sh` (opt-in via `--apps-builder` flag per Phase B locked design — apps-builder rebuild is rare enough to not run on every `--full`). Verify pg row at `app_containers.mission_planner_01.spec.manifest.KB_STATUS_FIELD.version` after a fresh apps-builder run.

```bash
# Verify after Layer I implementation:
docker exec pg-vector psql -U gedgar -d knowledge_base -tAc \
  "SELECT path::text, data
   FROM knowledge_base
   WHERE path::text LIKE 'system.moon_base.site.moon_base_alpha.app_containers.mission_planner_01.spec.manifest.%'
   ORDER BY path"
# expect: 1 anchor row + 1 spec.manifest header + 9 leaf rows (2 status + 7 jsonb)
```

Layer ordering after A:

```
M-2 ✅ → soak ✅ → O ✅ → F ✅ → A-pre ✅ → A ✅ → I → N → V
                                            ↑ next
```

Phase B remaining: 2.5 sessions (I, N, V).

### Rollback recipe (any of A.1..A.4)

```bash
git revert --no-edit <hash>   # each is independently revertible.
```

Reverting A.4 alone leaves the manifest/kb_build/refactored kb_query landed but restores `ros_mission_planner_ii` as the active container name. Reverting A.2 in isolation is messier (path helpers used by A.3 would orphan); revert A.4→A.3→A.2→A.1 in reverse order if a full rollback is needed.

---

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
