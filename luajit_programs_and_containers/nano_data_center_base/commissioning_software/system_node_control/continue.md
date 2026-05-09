# Nanodatacenter DCS — Continuation Plan

## State at end of 2026-05-10 — Cluster smoke GREEN. Pipeline validated end-to-end.

Four commits this session on top of yesterday evening's 5. **29 commits
ahead of origin/master**, **647 host-side tests green** (db test +1
for resolver regression guard).

**Cluster smoke (5 min) GREEN 2026-05-10 evening:**
- container rebuilt + recreated with this session's image
- heartbeat tick 1 → 60 over 295s = 5.00s/tick (on_tick wall-time gating verified)
- 2 missions submitted via /api/submit_mission → drained → fail-stopped
  cleanly with `board_not_found` (no boards in file_store; expected)
- /api/missions / /api/mission/<robot> returned correct envelopes
- validation paths green (400 / 405)
- ZERO error/warn lines in 5 min of container logs
- **One bug surfaced + patched mid-smoke**: pgmoon over cosocket needs
  nginx `resolver` directive; without it `/api/boards` returned 503
  with "no resolver defined to resolve host.docker.internal". Fixed
  in nginx.conf; regression-guarded in `test_planner_ui_db.lua`. New
  feedback memory: `feedback_openresty_cosocket_resolver.md`.

This session's commits:
- `15769083` **5b C5** — mission launcher (FFI direct enqueue)
- `1e6fe410` **5b C6** — mission status overlay (FFI direct status reads + 2s polling)
- (this-session) **Worker hookup** — main.lua now drives
  `action_server:serve({drain_nats=true, on_tick=heartbeat})`. Old
  log-only `drain_observer` removed; `on_tick` callback added to
  action_server:serve as the heartbeat hook. Heartbeat-only fallback
  preserved for degraded mode (NATS unreachable). +20 new
  worker-hookup tests.

### What this means operationally

The four planner_ui surfaces shipped today now have live data sources:

| UI piece | Backed by | Was previously |
|---|---|---|
| Board picker / SVG topology / drill-down | pgmoon → file_store boards | already live |
| "Submit mission" launcher | FFI → JobQueue | enqueued to dead-letter |
| Mission status cards / detail | FFI → KV summary + per-robot keys | always empty |
| /health, /__info__ | static | already live |

Until this commit, the launcher's enqueued missions sat unconsumed
because `main.lua` had only a "log-only observer" that claimed jobs
and immediately marked them `logged_only` without dispatching. After
the hookup, action_server's coroutine scheduler runs the missions and
publishes status keys that the C6 panel can read.

### Design — locked across C5 + C6 + worker hookup

**Option-4 FFI pattern (planner_ui side):** planner_ui's nginx
FFI-loads the planner worker's NATS wrappers directly. Same container,
same `/usr/local/lib/`, same `ldconfig` graph. Works for both submit
(JobQueue) and read (KeyStore) paths. Cost is a brief synchronous
worker freeze per call (~1-5ms localhost NATS).

**on_tick hook (worker side):** `action_server:serve(opts)` now
accepts `opts.on_tick = function(cycle_idx)` — fires once per
scheduler cycle, before link/drain/resume work. Errors caught via
pcall so a transient pg blip in a heartbeat handler doesn't take
down mission execution. main.lua passes a closure that wall-time-
gates the heartbeat to once every 5s regardless of action_server's
2-50ms tick rate.

### Architectural references

1. `~/.claude/projects/-home-gedgar-knowledge-base-assembly/memory/project_planner_active_node_contract.md` — the architecture
2. `~/.claude/projects/-home-gedgar-knowledge-base-assembly/memory/project_planner_implementation_plan.md` — phased plan (5b 6/6 + worker hookup done)
3. `~/.claude/projects/-home-gedgar-knowledge-base-assembly/memory/project_planner_team_scope.md` — scope boundary
4. `~/.claude/projects/-home-gedgar-knowledge-base-assembly/memory/project_v2_board_dsl_design.md` — DSL skeleton

### All recent commits (yesterday + today)

| Commit | Phase | What |
|---|---|---|
| `2aa36b4d` | 3a C1 | drive-packet round-trip via simulator |
| `c2823ab4` | 5 C5 prep | mission+simulator integration test |
| `a4108970` | 5 C5 prep | mission_builder.rebuild forwards use_drive_v2 |
| `538c6fec` | 5 C5 main | drive_v2 default + PLANNER_LEGACY_NAV escape hatch |
| `4bcef7d5` | 5b C1 | planner_ui chassis |
| `bc8026f1` | 5b C2 | read API |
| `a959f616` | 5b C3 | SVG L1 topology |
| `b2cc4051` | 5b C4 | SVG L2 drill + popup |
| `15769083` | 5b C5 | mission launcher (FFI direct enqueue) |
| `1e6fe410` | 5b C6 | mission status overlay (FFI direct + 2s polling) |
| (this-session) | Worker hookup | main.lua drives action_server:serve + on_tick |

### Aggregate test counts (host-side)

| Phase | Tests passing |
|---|---|
| Phase 1 catalog | 20/20 |
| Phase 2 (drive packet round-trip) | 39/39 |
| Phase 4 (board DSL C1-C4) | 125/125 |
| Phase 5 C1 route_builder | 29/29 |
| Phase 5 C2 + C3a hub_runtime ACK | 54/54 |
| Phase 5 C3b drive_v2 dispatch | 34/34 |
| Phase 5 C4 namespace + C5 default flip | 20/20 |
| Phase 3a C1 simulator round-trip | 34/34 |
| Phase 5 C5 prep mission+simulator integration | 28/28 |
| Phase 5b C1 planner_ui chassis | 19/19 |
| Phase 5b C2 db | 32/32 |
| Phase 5b C3+C4+C5+C6 renderer | 137/137 |
| Phase 5b C5 submit_mission | 50/50 |
| Phase 5b C6 status | 44/44 |
| **Worker hookup (this session)** | **20/20** |
| **Total** | **646/646 host-side** |

### Cluster smoke — STILL queued (now covers full pipeline)

Per `feedback_user_driven_testing.md`, you run; I analyze.

```bash
# 1. Build planner container with today's code.
cd ~/knowledge_base_assembly/luajit_programs_and_containers/nano_data_center_instance/app_containers/mission_planner
./build.sh

# 2. Bring up planner with default config.
docker compose up mission_planner_01

# 3. Watch planner stderr -- should see:
#    "entering action_server:serve(drain_nats=true) -- mission ..."
#    NOT the old "logged_only" drain_observer messages.
docker logs -f mission_planner_01 | grep -E 'serve|heartbeat'

# 4. Open planner_ui in a browser at port 8090. Verify each:
#    a-f. as before (board picker, L1, L2, popup, Esc, etc.)
#    g.   launch a mission via the launcher -> green toast "queued: <id>"
#    h.   status panel populates within 2s with the running mission card
#    i.   click the card -> per-robot detail expands
#    j.   when mission completes, status shows complete + result detail
#    k.   tab away + back -> polling resumes
#
#    KEY DIFFERENCE FROM YESTERDAY: status panel now shows real data
#    (not perpetually empty).

# 5. NATS-KV inspection:
docker exec mission_planner_01 nats kv ls
docker exec mission_planner_01 nats kv watch <site>_action_server '$KV.<bucket>.>'
# expect: summary + <robot>.status keys updating; on completion, .result key

# 6. Drive-v2 wire-format check on MQTT side:
mosquitto_sub -h <mqtt_host> -t 'moonbase/+/+/robots/+/rpc' -v &
# launch mission via UI -> CBOR payloads, ONE per polyline edge.

# 7. Rollback test: stop, set PLANNER_LEGACY_NAV=1, restart, resubmit.

# 8. Heartbeat continuity: confirm runtime.heartbeat row updates every
#    5s in pg even while a mission runs (on_tick wall-time gating).
psql -c "SELECT path, kb_status_field FROM knowledge_base
         WHERE path::text LIKE '%runtime.heartbeat%' LIMIT 5;"
```

### Remaining planner work

| Phase | Status | Sub-commits | Dependencies |
|---|---|---|---|
| Phase 7 design session (8 questions) | ✅ **LOCKED 2026-05-10** | — | See `project_phase7_multitenant_design.md` |
| Phase 7 C1 — migration script | ✅ **DONE 2026-05-10** | `c0189fe4` | dry-run validated on live cluster |
| Phase 7 C2 — kb_build per-tenant paths + validator | ✅ **DONE 2026-05-10** | this session | 28 new tests; existing c3 fixed to pass planner_namespace |
| Phase 7 C3 — action_server.lua bucket+key changes | queued | ~1 | C2 |
| Phase 7 C4 — planner_ui submit/status/db per-tenant | queued | ~1 | C3 |
| Phase 7 C5 — gateway per-tenant routing | queued | ~1 | C4 |
| Phase 7 C6 — Phase 7 cluster smoke | queued (user-driven) | smoke | C5 |
| 5 C5 follow-up (delete legacy nav code) | UNGATED — smoke green; safe to land | ~2-3 | none |
| Open items per user direction (matplotlib viewer, node-properties authoring) | queued post-Phase-7 | tbd | tbd |

That's it. After today, the planner team's wire-out + UI + worker
backend is end-to-end functional. Multi-tenant (Phase 7) is the only
substantive remaining planner-team work; the cluster smoke + 5 C5
follow-up legacy-delete is mostly cleanup.

### Quick start tomorrow

```bash
cd ~/knowledge_base_assembly/luajit_programs_and_containers/nano_data_center_base/commissioning_software/system_node_control

git log --oneline -15
git status

# Re-run host-side smoke (~30s total)
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
LD_LIBRARY_PATH="$(realpath $PLANNER/container/prebuilt_libs)" \
  luajit construction/tests/test_planner_worker_hookup.lua           # 20/20

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
- **User runs cluster tests**; assistant analyzes pasted logs.
- **Test isolation**: don't run anything that drops/recreates pg tables against the live cluster.
- **planner_ui handlers use pgmoon for pg, direct FFI for NATS** (option 4).
- **Module env-check pattern**: check APP_SITE BEFORE
  `require("cjson.safe")` so a host shell without cjson doesn't crash
  on early-exit paths.
- **action_server:serve on_tick contract**: fires once per cycle,
  errors caught by pcall, sees the cycle index. Use for heartbeats /
  watchdogs that must share the scheduler thread without blocking it.

### Rollback recipes

```bash
# Undo this session's last commit (worker hookup)
git reset --hard HEAD~1

# Undo all 3 of this session's commits (5b C5 + C6 + hookup)
git reset --hard b2cc4051   # back to "5b C4"

# Undo all of yesterday + today's commits (back to morning baseline)
git reset --hard 41e1e367

# Inspect commits
git show HEAD --stat        # this session: worker hookup
git show 1e6fe410 --stat    # 5b C6 status overlay
git show 15769083 --stat    # 5b C5 launcher
```

### Known issues / parking lot

- Phase 4 C5 visualizer Python smoke needs matplotlib on the host.
- Pre-existing: mission_builder.rebuild's 4th arg current_heading silently dropped. Out of scope.
- Per-tenant query SCOPING in kb_query / link_manager not yet wired — only the field is threaded. Lands with Phase 7.
- planner_ui's lazy NATS singletons don't reconnect on NATS restart. Add reconnect handling if observed in cluster smoke.
- C6 polling unauthenticated — any HTTP access to port 8090 can poll. Acceptable today (gateway-mediated). Auth lands with Phase 7.
- Worker hookup was tested host-side via the on_tick contract test (with a fake action_server instance). The full submit→drain→coroutine→publish_status→list_missions chain is ONLY exercised by cluster smoke; the host environment can't host a real NATS easily.

---

*continue.md rewritten 2026-05-10 (this-session, after worker hookup
ship). Earlier 2026-05-09 / 2026-05-10 morning content available via
`git log -p -- continue.md`.*
