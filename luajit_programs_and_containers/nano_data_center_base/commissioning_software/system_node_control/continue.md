# Nanodatacenter DCS — Continuation Plan

## State at end of 2026-05-10 — PHASE 5b COMPLETE (6/6).

Two commits this session on top of yesterday evening's 5. **27 commits
ahead of origin/master**, **626 host-side tests green**.

This session's commits:
- `15769083` **5b C5** — mission launcher: planner_ui submits missions
  to NATS JobQueue via direct FFI to libnats_job_queue (option 4 of
  the design question — same container as worker, ldconfig'd .so).
  3 new files + 6 modified. +50 new submit tests + 37 added to
  renderer (was 75 → 112).
- (this-session) **5b C6** — mission status overlay: polls
  `/api/missions` every 2s (paused while tab hidden), renders cards in
  `#status-region`, click a card for per-robot detail
  (`/api/mission/<robot>`). Same option-4 pattern but for *reads*:
  direct FFI to libnats_key_store. 4 new files + 4 modified. +44 new
  status tests + 25 added to renderer (was 112 → 137).

### Design — Phase 5b option-4 pattern (locked across C5 + C6)

planner_ui's nginx FFI-loads the planner worker's NATS wrappers
directly. Same container, same `/usr/local/lib/`, same `ldconfig`
graph. Reasons:
- option 1 (resty NATS over cosocket): not vendored — multi-day add
- option 2 (HTTP-to-worker): main.lua's 5s blocking nanosleep loop
  has no poll structure today
- option 3 (kv-bridge MQTT): mqtt_pubsub absent from openresty_base
  lualib (verified `ls` 2026-05-10)

Cost: each FFI call briefly blocks the nginx worker (~1-5ms on
localhost NATS). For a single-operator admin UI on a 2s polling
cadence this is fine. Each module owns its own KS singleton (lazy
init on first call) so connection cost is paid once per worker.

### Open architectural concern (not 5b-blocking)

`main.lua` (planner worker) ships a 5s nanosleep tick that **never
calls `action_server.serve()`**. So the launcher's submitted missions
sit in NATS-KV with no consumer, and C6's status panel will show
"no missions in flight" forever in production. The hookup is
straightforward — call `action_server:serve({drain_nats=true})` from
main.lua's tick loop and replace `nanosleep` with the action_server
scheduler's own tick (2-50ms). Tracked in remaining-work table below
as "Worker hookup".

### Architectural references

1. `~/.claude/projects/-home-gedgar-knowledge-base-assembly/memory/project_planner_active_node_contract.md` — the architecture
2. `~/.claude/projects/-home-gedgar-knowledge-base-assembly/memory/project_planner_implementation_plan.md` — the phased plan (now Phase 5b 6/6 done)
3. `~/.claude/projects/-home-gedgar-knowledge-base-assembly/memory/project_planner_team_scope.md` — scope boundary
4. `~/.claude/projects/-home-gedgar-knowledge-base-assembly/memory/project_v2_board_dsl_design.md` — DSL skeleton

### All recent commits (yesterday + today)

| Commit | Phase | What |
|---|---|---|
| `2aa36b4d` | 3a C1 | drive-packet round-trip via simulator |
| `c2823ab4` | 5 C5 prep | mission+simulator integration test |
| `a4108970` | 5 C5 prep | `mission_builder.rebuild` forwards `use_drive_v2` |
| `538c6fec` | 5 C5 main | drive_v2 default + `PLANNER_LEGACY_NAV=1` escape hatch |
| `4bcef7d5` | 5b C1 | planner_ui chassis (nginx + Lua handler skeleton) |
| `bc8026f1` | 5b C2 | read API (/api/boards, /api/board/<name>, /api/active_nodes) |
| `a959f616` | 5b C3 | SVG L1 topology renderer (vanilla JS) |
| `b2cc4051` | 5b C4 | SVG L2 drill-down + node popup + Esc handler |
| `15769083` | 5b C5 | mission launcher (FFI direct enqueue) |
| (this-session) | 5b C6 | mission status overlay + 2s polling |

### Aggregate test counts (host-side)

| Phase | Tests passing |
|---|---|
| Phase 1 catalog | 20/20 |
| Phase 2 (drive packet round-trip) | 39/39 |
| Phase 4 (board DSL C1-C4) | 36 + 42 + 16 + 31 = 125/125 |
| Phase 5 C1 route_builder | 29/29 |
| Phase 5 C2 + C3a hub_runtime ACK | 54/54 |
| Phase 5 C3b drive_v2 dispatch | 34/34 |
| Phase 5 C4 namespace + C5 default flip | 20/20 |
| Phase 3a C1 simulator round-trip | 34/34 |
| Phase 5 C5 prep mission+simulator integration | 28/28 |
| Phase 5b C1 planner_ui chassis | 19/19 |
| Phase 5b C2 db (boards / board / active_nodes) | 32/32 |
| Phase 5b C3+C4+C5+C6 renderer (SVG L1+L2 + popup + Esc + launcher + status) | 137/137 |
| Phase 5b C5 submit_mission | 50/50 |
| **Phase 5b C6 status (this session)** | **44/44** |
| **Total** | **626/626 host-side** |

### Cluster smoke — STILL queued (now covers C2+C3+C4+C5+C6)

Per `feedback_user_driven_testing.md`, you run; I analyze.

```bash
# 1. Build planner container with today's code.
cd ~/knowledge_base_assembly/luajit_programs_and_containers/nano_data_center_instance/app_containers/mission_planner
./build.sh   # whatever your staging-copy build script is here

# 2. Bring up planner with default config.
docker compose up mission_planner_01

# 3. Subscribe to the rpc topic to observe drive_v2 wire format.
mosquitto_sub -h <mqtt_host> -t 'moonbase/+/+/robots/+/rpc' -v &

# 4. Open planner_ui in a browser at port 8090 (or whatever
#    node_control exposes). Verify each:
#    a. board picker populates
#    b. select a board -> SVG topology renders (L1)
#    c. click a node -> properties popup
#    d. press Esc -> popup closes
#    e. click an edge -> L2 drill-down with colored leaves
#    f. press Esc -> back to L1
#    g. type robot id, click "Pick source & target", click two nodes,
#       click "Submit mission" -> green toast "queued: <job_id>"
#    h. mission status panel polls every 2s; shows "no missions in
#       flight" until the worker hookup lands; shows the launched
#       mission cards once the hookup IS in place
#    i. switch to another tab; come back -> polling resumes

# 5. API smoke (also exercises the new endpoints):
curl http://<host>:8090/                                          # shell
curl http://<host>:8090/health                                    # JSON
curl http://<host>:8090/api/boards                                # list
curl http://<host>:8090/api/board/landing_zone | head             # one
curl http://<host>:8090/api/active_nodes                          # list
curl -X POST http://<host>:8090/api/submit_mission \
  -H "Content-Type: application/json" \
  -d '{"robot_id":"rover_1","board":"landing_zone",
       "source":"lander_pad","target":"habitat_site"}'
curl http://<host>:8090/api/missions                              # NEW
curl http://<host>:8090/api/mission/rover_1                       # NEW

# 6. NATS-KV inspection (confirm launcher's job + status flow):
docker exec mission_planner_01 nats kv ls
docker exec mission_planner_01 nats kv watch <site>_action_server '$KV.<bucket>.>'

# 7. Drive-v2 wire-format check: outbound RPC payloads should be CBOR
#    (binary), not JSON. ONE publish per polyline edge, not N-1.

# 8. Rollback test: stop, set PLANNER_LEGACY_NAV=1, restart, resubmit.
#    Expect JSON cmd_path_*_t payloads on the wire.
```

### Remaining planner work

| Phase | Status | Sub-commits | Dependencies |
|---|---|---|---|
| **Worker hookup** (call action_server.serve from main.lua) | **NEXT** | 1-2 | none — pure planner runtime change. Without this the launcher enqueues + C6 status polling are wired to a queue/buckets nobody writes to in production. |
| 5 C5 follow-up (delete legacy nav code) | gated on cluster smoke | ~2-3 | needs cluster smoke green |
| 7 (multi-planner) | not started | ~3-4 | depends on KB schema decision (deferred) |

**Suggested order:** Worker hookup next. It's the smallest unblocking
change that turns the now-complete UI into a useful operator tool —
without it, every screen in 5b is showing fake/empty data. After that,
cluster smoke + 5 C5 follow-up legacy delete.

### Worker hookup (next session) — design notes

`main.lua`'s current shape (lines 282-305):
```lua
while running do
  kb_status:set_status_data(...)        -- heartbeat
  drain_observer()                       -- claim up to 5 jobs/tick
  C.nanosleep(...)                       -- 5s
end
```

`drain_observer` calls `JobQueue:claim_job` directly but does NOT
hand jobs to action_server's coroutine scheduler. Result: jobs are
claimed and discarded.

The right shape is to instantiate action_server at startup and call
`action_server:serve({drain_nats=true, ...})` instead of the manual
loop. action_server's internal scheduler (lib/action_server.lua lines
834-889) already implements:
- `_drain_nats_queue()` per cycle (claim + start mission coroutine)
- 2-50ms tick (`tick_usleep` or idle 50ms)
- coroutine resume per active mission
- status publish via `_publish_status` per state transition

So `main.lua` becomes:
```lua
local action_server = require("lib.action_server").new({...})
local kb_handle, ks_handle, ... = wire_dependencies()
running = action_server:serve({
  drain_nats = true,
  on_tick    = function() kb_status:set_status_data(...) end,
  -- existing heartbeat + signal handling preserved as on_tick hooks
})
```

Risk: medium. Need to thread the kb_status / signal handling that
main.lua owns into action_server's scheduler. action_server already
has hooks for this (it's the production design); main.lua just
hasn't been migrated to use them.

Test approach: extend `test_mission_simulator_integration.lua` to
exercise the full pipeline end-to-end (submit → drain → coroutine
resume → packet emit → ack → done → publish_status → list_missions
returns the result).

### Quick start tomorrow

```bash
cd ~/knowledge_base_assembly/luajit_programs_and_containers/nano_data_center_base/commissioning_software/system_node_control

# Verify state
git log --oneline -15
git status

# Re-run host-side smoke (~25s total)
luajit construction/tests/test_planner_phase1_catalog.lua            # 20/20
luajit construction/tests/board_dsl/test_board_dsl_c1.lua            # 36/36
luajit construction/tests/board_dsl/test_board_dsl_c2.lua            # 42/42
luajit construction/tests/board_dsl/test_board_dsl_c3.lua            # 16/16
luajit construction/tests/board_dsl/test_board_dsl_c4.lua            # 31/31
luajit construction/tests/test_route_builder_drive_packets.lua       # 29/29
luajit construction/tests/test_drive_v2_dispatch.lua                 # 34/34
luajit construction/tests/test_planner_namespace_threading.lua       # 20/20
luajit construction/tests/test_planner_ui_chassis.lua                # 19/19
luajit construction/tests/test_planner_ui_db.lua                     # 32/32
luajit construction/tests/test_planner_ui_renderer.lua               # 137/137
luajit construction/tests/test_planner_ui_submit_mission.lua         # 50/50
luajit construction/tests/test_planner_ui_status.lua                 # 44/44

PLANNER=../../../nano_data_center_instance/app_containers/mission_planner
LD_LIBRARY_PATH="$(realpath $PLANNER/container/prebuilt_libs)" \
  luajit construction/tests/test_hub_runtime_send_drive.lua          # 54/54
LD_LIBRARY_PATH="$(realpath $PLANNER/container/prebuilt_libs)" \
  luajit construction/tests/test_phase3a_simulator_round_trip.lua    # 34/34
LD_LIBRARY_PATH="$(realpath $PLANNER/container/prebuilt_libs)" \
  luajit construction/tests/test_mission_simulator_integration.lua   # 28/28

cd $PLANNER/container/planner
LD_LIBRARY_PATH="$(realpath ../prebuilt_libs)" \
  luajit hub_dsl/protocol/test_drive_packet.lua                      # 39/39
```

### Operating-mode reminders

- **Commit style**: title like "B.2 Planner Phase X: ...", body with
  bulleted file-level changes, test counts, design rationale. Trailer:
  `Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>`.
- **One layer = one commit** per `feedback_holding_commits.md`.
- **No-soft-faults** rule: every fault halts until explicit reset.
  (UI submit/status errors are NOT control-system faults — return
  4xx/5xx and let the operator retry.)
- **User runs cluster tests**; assistant analyzes pasted logs (`feedback_user_driven_testing.md`).
- **Test isolation**: don't run anything that drops/recreates pg tables against the live cluster (`feedback_test_db_isolation.md`).
- **planner_ui handlers use pgmoon for pg, direct FFI for NATS** (option 4). Two parallel worlds.
- **planner_ui module env-check pattern**: check APP_SITE BEFORE
  `require("cjson.safe")` so a host shell without cjson doesn't crash
  on early-exit paths during testing. (Burned this in submit.lua's
  initial test cycle and the C6 status module.)

### Rollback recipes

```bash
# Undo this session's last commit (5b C6)
git reset --hard HEAD~1

# Undo BOTH of this session's commits (5b C5 + C6)
git reset --hard b2cc4051   # back to "5b C4 SVG L2 drill"

# Undo all of yesterday + today's commits (back to morning baseline)
git reset --hard 41e1e367

# Inspect commits
git show HEAD --stat        # this session: 5b C6 status overlay
git show 15769083 --stat    # 5b C5 launcher
git show b2cc4051 --stat    # 5b C4
git show 4bcef7d5 --stat    # 5b C1 chassis
```

### Known issues / parking lot

- Phase 4 C5 visualizer Python smoke needs matplotlib on the host — not currently installed in the WSL venv.
- Pre-existing: `mission_builder.rebuild`'s 4th arg `current_heading` is silently dropped. Out of scope.
- Per-tenant query SCOPING in kb_query / link_manager (which boards a planner sees) is not yet wired — only the field is threaded. Lands with Phase 7 KB schema decision.
- **`main.lua` does NOT call `action_server.serve()`** — see "Worker hookup" above. Highest-priority remaining work.
- planner_ui's lazy NATS singletons (one for KS reads in status.lua, one for KS+JQ writes in submit.lua) don't reconnect if the NATS server restarts mid-session. Add reconnect handling if observed in cluster smoke. Today's behavior is whatever libnats does on a stale connection.
- C6 polling is unauthenticated — anyone with HTTP access to port 8090 can poll the dashboard. Acceptable today (all access is gateway-mediated). Auth lands with Phase 7 multi-tenant scoping.

---

*continue.md rewritten 2026-05-10 (this-session, after 5b C6 ship).
Earlier 2026-05-09 / 2026-05-10 morning content available via
`git log -p -- continue.md` if needed.*
