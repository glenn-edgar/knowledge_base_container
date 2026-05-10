# Nanodatacenter DCS — Continuation Plan

## State at end of 2026-05-10 EOD #4 — Parking lot drained (5/6 items shipped)

**53 commits ahead of origin/master** after this session's five commits.
Parking lot from EOD #3 (7 items → 6 after dropping #5 per active-hub
boundary correction) is now at **1 remaining**: a documentation TODO
for `mission_builder.rebuild`'s silently-dropped `current_heading`
arg, which needs a contract decision before the fix lands.

## This session's commits (5 implementation)

| # | Commit | Item |
|---|---|---|
| 1 | `33ecc833` | **P3 #5** — gate `test_mission_simulator_integration` on `liblua_cbor.so`. Probes the bare-name `ffi.load` up front; on bare hosts (no `LD_LIBRARY_PATH`) prints `SKIPPED` and exits 0. Sweep stays green; with `LD_LIBRARY_PATH` set, still passes 26/26. |
| 2 | `d89314ac` | **P1 #3** — fix `/api/missions active_missions` overcount. `mission_count` was incremented on the synchronous-fail path in `_drain_nats_queue` (mission created in `state="done"`) without a matching decrement. Now the immediate-done branch doesn't inc; `/api/missions` returns to 0 after every sync-fail and after every active→done transition. |
| 3 | `71bc37dd` | **P1 #4** — delete legacy nav code (5 C5 follow-up). Removed `route_builder.M.build`, `global_planner.M:plan` + `M:replan`, `mission_builder` `use_drive_v2` conditionals, `mission_builder.rebuild`'s 5th arg, `action_server.use_drive_v2` + `PLANNER_LEGACY_NAV` env hatch, and all forwarding. **-316 lines.** Tests updated; dual-tenant cluster smoke success=true. |
| 4 | `37f2a749` | **P1 #1** — MQTT auto-reconnect + robot_sim init-state retry. `mqtt_hub_transport`: initial connect failures no longer throw; `_ensure_connected` retries with 1s→30s exponential backoff and re-subscribes on success; `_safe_publish` pcall-wraps every send. `robot_sim/main.lua`: re-publishes `link_announce` every 5s while `state=init` so a robot doesn't sit stuck if the first announce got no ack. Verified by bouncing the broker — both planners reconnected, fresh missions completed. |
| 5 | `10b32840` | **P1 #2** — `planner_ui` infra_discovery for `NATS_URL`. New `infra_lookup.lua` (pgmoon flavor of `app_lib/infra_discovery.lua`). `submit.lua` + `status.lua` now resolve `NATS_URL` via opts → pg-registry → env → hardcoded default. Verified via openresty error log: `infra_lookup: NATS via registry -> nats://nats-js-ram:4222`. |

## End-state of the parking lot

### Closed this session (5)

- ~~P1 #1: MQTT broker reconnect + robot_sim stuck-in-init~~ → `37f2a749`
- ~~P1 #2: planner_ui NATS_URL hardcoding~~ → `10b32840`
- ~~P1 #3: `active_missions` overcount~~ → `d89314ac`
- ~~P1 #4: 5 C5 legacy nav deletion~~ → `71bc37dd`
- ~~P3 #5: `test_mission_simulator_integration` FFI failure~~ → `33ecc833`

### Closed prior sessions

- ~~Phase 4 C5 matplotlib visualizer dependency~~ (Step D, 2026-05-10 EOD#3)

### Still open

- **P3 #6** — `mission_builder.rebuild`'s 4th arg `current_heading` is currently NOT consumed (build() has no `start_heading` parameter). Wiring it through would change the replan's first-leg geometry. **Needs a contract decision** before the fix lands (does a replanned leg start from the robot's current heading or from `initial_heading=0`?). Doc TODO already in code; tracking here for visibility.

### Cross-program notes (not on planner's plate)

- The **active-hub program** owns the DSL that declares each hub's location + board-icon type + comments. The planner only sees opaque `kb_ref` strings and dereferences at map-UI render time. Filed in `project_planner_active_node_contract.md` — separate team / program.

## End-state validation

### Tests

| Suite | Count | Status |
|---|---|---|
| `test_planner_ui_submit_mission` | 51 | green |
| `test_planner_ui_status` | 45 | green |
| `test_action_server_phase7` | 27 | green |
| `test_robot_sim_package` | 87 | green |
| `test_mock_mqtt_robot` | 38 | green |
| `test_drive_v2_dispatch` | 30 | green (was 34; -4 legacy-energy assertions retired with the code) |
| `test_link_manager_tenant_filter` | 14 | green |
| `test_planner_ui_renderer` | 157 | green |
| **Total host** | **449** | green |
| `test_mission_simulator_integration` | 26 + 1 SKIP path | green with `LD_LIBRARY_PATH`; clean skip without |

### Cluster

- Dual-tenant Phase 7 cluster: both `mission_planner_01` (ns=`mission_planner_01`) and `mission_planner_02` (ns=`tunnel_ops`) submit + complete missions in parallel with `success=true replans=0`.
- Stress test (broker bounce): planners reconnect with 1→2→4→8s backoff, fresh missions succeed.
- Active-mission counter accurate (=0 after both sync-fail and normal completion).
- `infra_lookup` confirmed preferred over env via openresty access log.

### Operational

- 53 commits ahead of `origin/master`.
- Image rebuilds: `nanodatacenter/mission-planner:latest` + `nanodatacenter/robot-sim:latest` both at this session's SHAs.
- Cluster state at handoff:
  - `mission_planner_01` (UI 19005, ns=`mission_planner_01`) ↔ `robot_sim_rover_1`
  - `mission_planner_02` (UI 19009, ns=`tunnel_ops`)         ↔ `robot_sim_rover_2`
  - All infra containers (`pg-vector`, `nats-js-ram`, `mosquitto-ram-ws_main`, `kv-bridge`, `docker-host-broker`) up.

## Recommended next session

The Phase 7 surface is essentially done; the major code path (planner
worker + ui + robot_sim) is well-tested and clean. Reasonable next
moves, roughly in order of value:

**Option A — Phase 8 / multi-planner integration.** Per
`project_planner_implementation_plan.md` row 48: "not started; ~3-4
sub-commits, uses 3a simulator as fixture." Now that the multi-tenant
isolation works, the next architectural slice is making *multiple
planners coordinate* (e.g., one planner handing a robot off to another
tenant's planner). Design first, then implement.

**Option B — Active-hub program kickoff.** Per the
cross-program note above, the active-hub team will need a DSL that
exposes hub identity through to the planner's L2 map renderer. If
that team has bandwidth this would unblock real-board onboarding.

**Option C — Operational cleanup.** P3 #6 (`current_heading` contract
decision + fix) is the only thing left in the parking lot. Decide
the contract, write a regression test, ship. ~1 commit.

## Quick start tomorrow

```bash
cd ~/knowledge_base_assembly/luajit_programs_and_containers/nano_data_center_base/commissioning_software/system_node_control

# Verify session commits
git log --oneline -10

# Cluster state
ps aux | grep dcs.lua | grep -v grep
docker ps | grep -E 'mission_planner|robot_sim'

# Dual-tenant smoke
curl -sS -X POST -H 'Content-Type: application/json' \
    -d '{"robot_id":"rover_1","board":"landing_zone","source":"lander_pad","target":"habitat_site"}' \
    http://localhost:19005/api/submit_mission &
curl -sS -X POST -H 'Content-Type: application/json' \
    -d '{"robot_id":"rover_2","board":"landing_zone","source":"lander_pad","target":"habitat_site"}' \
    http://localhost:19009/api/submit_mission &
wait
sleep 8
curl -s http://localhost:19005/api/mission/rover_1 | jq
curl -s http://localhost:19009/api/mission/rover_2 | jq
# Expect: both state=completed, success=true, replans=0

# Host regression sweep (449 tests)
for t in test_planner_ui_submit_mission test_planner_ui_status \
         test_action_server_phase7 test_robot_sim_package test_mock_mqtt_robot \
         test_drive_v2_dispatch test_link_manager_tenant_filter \
         test_planner_ui_renderer; do
    luajit construction/tests/$t.lua | tail -2
done
# Optional with LD_LIBRARY_PATH for the FFI-gated test (26 more):
LD_LIBRARY_PATH=$(pwd)/../kb/mqtt \
    luajit construction/tests/test_mission_simulator_integration.lua | tail -2
```

## Architectural references

1. `~/.claude/projects/-home-gedgar-knowledge-base-assembly/memory/project_planner_active_node_contract.md`
2. `~/.claude/projects/-home-gedgar-knowledge-base-assembly/memory/project_planner_implementation_plan.md`
3. `~/.claude/projects/-home-gedgar-knowledge-base-assembly/memory/project_phase7_multitenant_design.md`
4. `~/.claude/projects/-home-gedgar-knowledge-base-assembly/memory/feedback_robot_wire_dispatch.md`
5. `~/.claude/projects/-home-gedgar-knowledge-base-assembly/memory/feedback_luajit_stdio_buffering.md`

## Operating-mode reminders

- **Phase 7 + ROBSIM signed off**. Assistant-runs-cluster-smoke scope from EOD #3 effectively continued through this session for the parking-lot cleanup, but the next session should treat live-cluster testing as user-driven again unless explicitly re-locked.
- **No application auth** per `feedback_no_application_auth.md`.
- **One layer = one commit** per `feedback_holding_commits.md`.
- **No-soft-faults** rule for control-system fault paths.
- **Bare "yes" means accept the recommendation** per `feedback_yes_means_accept.md`.
- **`io.stderr:write+flush` not `print()`** for diagnostics in containerized LuaJIT processes.
- **`deployment/<cpu>/stop.sh` not `pkill -f`** for dcs.lua processes.
- **`build_kb.sh` wipes file_store** — re-upload boards after every rebuild before any mission submit.
- **`stage_deploy.sh` after `slice_bootstrap.sh`** — slicer wipes per-CPU dirs; stager re-symlinks runtime/ + seeds env.sh.

## Rollback recipes

```bash
# Undo this session (back to 41759edf EOD #3 wrap)
git reset --hard 41759edf

# Undo only P1 #2 (infra_discovery)
git reset --hard 37f2a749

# Undo only P1 #1 (MQTT reconnect)
git reset --hard 71bc37dd

# Undo only P1 #4 (legacy nav deletion)
git reset --hard d89314ac

# Undo only P1 #3 (active_missions fix)
git reset --hard 33ecc833

# Undo only P3 #5 (FFI gate)
git reset --hard 41759edf

# Inspect this session's commits
git log --oneline 41759edf..HEAD
```

---

*continue.md rewritten 2026-05-10 EOD #4 (after parking-lot drain: 5 of 6 items shipped; 1 remaining P3 needs contract decision). Phase 7 A→F complete + post-cleanup. 53 commits ahead of origin/master.*
