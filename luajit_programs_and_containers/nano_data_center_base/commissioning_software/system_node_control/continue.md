# Nanodatacenter DCS — Continuation Plan

## State at end of 2026-05-10 — 5b C5 SHIPPED. Phase 5b is 5/6 done.

One commit this session on top of yesterday evening's 5 (afternoon+evening
of 2026-05-09). **26 commits ahead of origin/master**, **557 host-side
tests green**.

This session's commit:
- (this-session) **5b C5** — mission launcher: planner_ui submits
  missions to NATS JobQueue via direct FFI to libnats_job_queue
  (option 4 of the design question — see "Design decision" below).
  Touches `planner_ui/lua/submit.lua`, `lua/api_submit_mission.lua`,
  `conf/nginx.conf`, `lua/shell_page.lua`, `assets/map_render.js`,
  `assets/planner_ui.css`. **+50 new submit-handler tests, +37 added
  to renderer test (was 75 → 112).**

### Design decision — 5b C5 enqueue path = option 4 (FFI direct)

The 4 options from yesterday's continue.md plus a 4th surfaced today:

| # | Option | Verdict |
|---|---|---|
| 1 | pure-Lua resty NATS over cosocket | Rejected — not vendored. Multi-day add to openresty_base. |
| 2 | HTTP-to-worker via unix socket | Rejected — main.lua's 5s blocking nanosleep loop has no poll structure today. |
| 3 | kv-bridge MQTT pattern | Rejected — `mqtt_pubsub` not in openresty_base lualib (verified `ls` 2026-05-10). |
| 4 | **planner_ui FFI-loads libnats_job_queue.so directly** | **Chosen.** Same container as planner worker; .so already in /usr/local/lib/ + ldconfig'd; lib/nats_*.lua wrappers reachable via extended `lua_package_path`. |

Cost of option 4: `jq:submit()` is a synchronous FFI call, briefly
blocks the nginx worker (~1-5ms on localhost NATS). Acceptable for a
single-operator admin UI. Lazy KS+JQ singleton means only the FIRST
submit pays the connect cost (~10-50ms); subsequent submits are
publish-only.

**Separate concern raised by today's survey, not blocking C5:**
`main.lua` (planner worker) ships a 5s tick that **never calls
`action_server.serve()`** — meaning the mission-execution drain isn't
wired in production yet. Whatever the launcher enqueues sits in
NATS-KV until 3b runtime hookup happens. That's a Phase 3b/runtime
follow-up, NOT a C5 blocker.

### Architectural references

1. `~/.claude/projects/-home-gedgar-knowledge-base-assembly/memory/project_planner_active_node_contract.md` — the architecture
2. `~/.claude/projects/-home-gedgar-knowledge-base-assembly/memory/project_planner_implementation_plan.md` — the phased plan
3. `~/.claude/projects/-home-gedgar-knowledge-base-assembly/memory/project_planner_team_scope.md` — scope boundary (planner team vs separate programs)
4. `~/.claude/projects/-home-gedgar-knowledge-base-assembly/memory/project_v2_board_dsl_design.md` — DSL skeleton

### Yesterday's commits (afternoon + evening of 2026-05-09)

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

(Yesterday morning landed 6 commits: `75c6fa74` `efa69339` `c5c495c0`
`9814f78f` `426daccf` `41e1e367` covering Phase 5 C1-C4 + flaky-test
fix.)

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
| Phase 5b C3+C4+C5 renderer (SVG L1+L2 + popup + Esc + launcher) | 112/112 |
| **Phase 5b C5 submit_mission (this session)** | **50/50** |
| **Total** | **557/557 host-side** |

### Cluster smoke — STILL queued (now covers C2+C3+C4+C5)

Per `feedback_user_driven_testing.md`, you run; I analyze.

```bash
# 1. Build planner container with today's code.
cd ~/knowledge_base_assembly/luajit_programs_and_containers/nano_data_center_instance/app_containers/mission_planner
./build.sh   # whatever your staging-copy build script is here

# 2. Bring up planner with default config.
docker compose up mission_planner_01

# 3. Subscribe to the rpc topic to observe drive_v2 wire format.
mosquitto_sub -h <mqtt_host> -t 'moonbase/+/+/robots/+/rpc' -v &

# 4. Submit a mission via the *launcher* (NEW C5 path) -- open the
#    planner_ui in a browser at port 8090 (or wherever node_control
#    exposes it):
#    - pick a board from the header dropdown
#    - type a robot id in the "robot:" field
#    - click "Pick source & target", then click two nodes
#    - click "Submit mission"
#    - expect a green toast "queued: <job_id>"
#
#    Behind the scenes: POST /api/submit_mission -> direct FFI to
#    libnats_job_queue -> NATS KV bucket <site>_action_server.
#
#    OR via the existing CLI for comparison:
docker exec mission_planner_01 luajit /opt/apps/planner/scripts/submit_test_mission.lua \
  --robot rover_1 --board landing_zone --start lander_pad --stop habitat_site

# 5. Subscribe to the NATS KV bucket to confirm the launcher's job
#    landed:
docker exec mission_planner_01 nats kv ls
docker exec mission_planner_01 nats kv watch <site>_action_server '$KV.<bucket>.>'

# 6. Hit planner_ui chassis at port 8090:
curl http://<host>:8090/ | head            # shell loads
curl http://<host>:8090/api/boards         # boards list
curl http://<host>:8090/api/board/landing_zone | head   # one board
curl http://<host>:8090/api/active_nodes   # active nodes
curl -X POST http://<host>:8090/api/submit_mission \
  -H "Content-Type: application/json" \
  -d '{"robot_id":"rover_1","board":"landing_zone",
       "source":"lander_pad","target":"habitat_site"}'

# 7. Drive-v2 wire-format check: outbound RPC payloads should be CBOR
#    (binary), not JSON. ONE publish per polyline edge, not N-1.

# 8. Rollback test: stop, set PLANNER_LEGACY_NAV=1, restart, resubmit.
#    Expect JSON cmd_path_*_t payloads on the wire.
```

### Remaining planner work

| Phase | Status | Sub-commits | Dependencies |
|---|---|---|---|
| 5b C6 (mission status overlay) | not started; ~1 sub-commit | 1 | depends on action_server publishing status keys; htmx polling pattern |
| 5 C5 follow-up (delete legacy nav code) | gated on cluster smoke | ~2-3 | needs cluster smoke green |
| 7 (multi-planner) | not started | ~3-4 | depends on KB schema decision (deferred) |
| Worker hookup (call action_server.serve from main.lua) | not yet | 1 | needed before the launcher's enqueued jobs actually drive a robot. Phase 3b/runtime concern, not 5b. |

**Suggested order:** 5b C6 status overlay next (extends today's chassis;
independent of cluster smoke). After cluster smoke green, 5 C5 follow-up
can land in parallel.

### 5b C6 design notes (next session)

Status-overlay panel inside `#status-region`. Polls
`action_server`'s NATS-KV status bucket and renders per-mission cards.

**Driver:** same as C5 — direct FFI to `lib/nats_key_store` (NOT
job_queue) for KV reads. KeyStore.get is a synchronous call;
acceptable on a 2-3s polling cadence for a single-operator UI. Same
"option 4" rationale.

**Endpoints (proposed):**
- `GET /api/missions` → array of `{robot_id, state, current_packet,
  last_heartbeat_ts, current_action_node?}`. Reads
  `<site>_action_server` KV bucket; filters keys matching
  `mission.<robot_id>.status`.
- `GET /api/mission/<robot_id>` → full mission state (timeline of
  packets, ACK status per packet, action results).

**Polling cadence:** start with 2s; htmx `hx-trigger="every 2s"`
swap into `#status-region`. Move to SSE if cadence becomes a
bottleneck.

**Test approach:** stub the KS via the same `package.preload` /
opts-injection pattern used in `test_planner_ui_submit_mission.lua`.
Verify the SQL-like KV-key prefix, parse + envelope, error paths.

### Quick start tomorrow

```bash
cd ~/knowledge_base_assembly/luajit_programs_and_containers/nano_data_center_base/commissioning_software/system_node_control

# Verify state
git log --oneline -15             # this session's 1 + yesterday's 5 + prior
git status                        # only continue.md modified (after this session's commit)

# Re-run host-side smoke (~20s total)
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
luajit construction/tests/test_planner_ui_renderer.lua               # 112/112
luajit construction/tests/test_planner_ui_submit_mission.lua         # 50/50

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

- **Commit style**: title like "B.2 Planner Phase 5b C6: ...", body
  with bulleted file-level changes, test counts, design rationale.
  Trailer: `Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>`.
- **One layer = one commit** per `feedback_holding_commits.md`.
- **No-soft-faults** rule: every fault halts until explicit reset.
  (UI submit errors are NOT control-system faults — return 4xx/5xx and
  let the operator retry; this is the right behavior.)
- **User runs cluster tests**; assistant analyzes pasted logs (`feedback_user_driven_testing.md`).
- **Test isolation**: don't run anything that drops/recreates pg tables against the live cluster (`feedback_test_db_isolation.md`).
- **planner_ui handlers use pgmoon** (cosocket) for pg, **direct FFI** for NATS (option 4). Two parallel worlds.

### Rollback recipes

```bash
# Undo this session's last commit (5b C5 launcher)
git reset --hard HEAD~1

# Undo all 6 of yesterday + today's commits (back to morning baseline)
git reset --hard 41e1e367   # back to "C4 planner_namespace threading"

# Undo all 12 of yesterday morning + afternoon + evening + today's commits
git reset --hard 365d0727

# Inspect commits
git show HEAD --stat        # this session: 5b C5 launcher
git show b2cc4051 --stat    # 5b C4
git show a959f616 --stat    # 5b C3
git show bc8026f1 --stat    # 5b C2
git show 4bcef7d5 --stat    # 5b C1 chassis
git show 538c6fec --stat    # 5 C5 main default flip
```

### Known issues / parking lot

- Phase 4 C5 visualizer Python smoke needs matplotlib on the host — not currently installed in the WSL venv.
- Pre-existing: `mission_builder.rebuild`'s 4th arg `current_heading` is silently dropped (`build()` has no start_heading parameter). Documented in mission_builder.lua. Out of scope.
- Per-tenant query SCOPING in kb_query / link_manager (which boards a planner sees) is not yet wired — only the field is threaded. Lands with Phase 7 KB schema decision.
- **`main.lua` does NOT call `action_server.serve()`** — the planner worker drains nothing today. Launcher enqueues land in NATS-KV but no consumer. Hookup is Phase 3b/runtime; tracked above as "Worker hookup".
- The planner_ui submit handler creates a NATS KeyStore on first use and reuses it for the worker's lifetime (lazy singleton). If the NATS server is restarted while planner_ui is up, the cached connection may go stale; today's behavior is whatever libnats does on a stale connection. Add reconnect handling in C6 if observed in cluster smoke.

---

*continue.md rewritten 2026-05-10 (this-session). Earlier 2026-05-09
content available via `git log -p -- continue.md` if needed.*
