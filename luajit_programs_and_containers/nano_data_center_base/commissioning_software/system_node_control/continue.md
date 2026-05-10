# Nanodatacenter DCS — Continuation Plan

## State at end of 2026-05-10 EOD #2 — Phase 7 Step B GREEN; multi-tenant next

**45 commits ahead of origin/master** after this session's three commits.
**Golden-path smoke is GREEN end-to-end** for the first time: mission state
`completed`, success=true, replans=0, drive packet decoded from CBOR. Gap 7
audited + confirmed safe (one-time provisioning miss, not a recurring wipe).
Next session = **Step C: multi-tenant validation**.

## This session's commits (3 implementation)

| # | Commit | What |
|---|---|---|
| 1 | `1b5b1148` | gap-9 fix: align robot/planner heartbeat periods; widen stale window (HB_PERIOD_S 10→3, HEARTBEAT_INTERVAL 2→3, MISS_LIMIT 3→5 = stale@15s) |
| 2 | `d7810a8a` | gap-10 bundle: planner-loss detection (robot_sim re-announces after planner restart) + submit.lua stops shape fix + mission_builder mission_id type guard + action_server diagnostic stderr |
| 3 | `ad463ce7` | gap-11 fix: robot_sim CBOR support (vendor lua_cbor) + dispatch on packet_type=30 for drive_ack/drive_done vs ack/kb_done |

Plus continue.md wrap (this commit).

## Today's gap progression — final

| Gap | Status | Note |
|---|---|---|
| 9 — HB period mismatch | ✅ Fixed (`1b5b1148`) | Robot 10s vs planner stale@6s caused link to bounce live→stale→offline→registering→…. Symmetric fix: 3s/15s window. |
| 10 — robot_sim has no planner-loss detection | ✅ Fixed (`d7810a8a`) | After planner restart, robot stayed `live` forever in its own view → planner had no record of robot → energy check fell back to KB class energy_max=0 → "insufficient_energy". Robot now tracks last_planner_msg, re-announces after 9s silent window. |
| 10b — `submit.lua` stops shape | ✅ Fixed (`d7810a8a`) | Built `{ target_str }`; mission_builder requires `{ {node=str} }`. |
| 10c — `mission_builder` leaked string mission_id into cmd_drive | ✅ Fixed (`d7810a8a`) | action_server's mission_id is JobQueue job.id (string); cmd_drive's optional mission_id is uint32 wire-format. Type-guarded. |
| 10d — `action_server` silent on coroutine throw | ✅ Fixed (`d7810a8a`) | Added `io.stderr:write` on `not resume_ok` + synchronous_fail branches. Made gap-11 diagnosis 5 min instead of an hour. |
| 11 — robot_sim is JSON-only, can't decode CBOR drive packets | ✅ Fixed (`ad463ce7`) | Vendored liblua_cbor.so + lua_cbor.lua into robot_sim image. main.lua tries JSON first, falls back to CBOR-decode-to-JSON. Dispatches on packet_type=30 → drive_ack+drive_done (keyed by packet_id) vs legacy ack+kb_done (keyed by seq). |
| 7 — `knowledge_base_fs_node` empty | ✅ Confirmed-safe (audit, no code change) | Only `migrate_phase7.sh` deletes from fs_node, and only at deprecated `<site>.boards` path (not Phase 7 `<site>.planner.<ns>.boards.*`). Tests use uniquely-named test tables. upload_board.lua UPSERTs only. Two back-to-back smokes confirm row survives normal ops. Original wipe was almost certainly a one-time provisioning miss, not a recurring bug. |

## End-state of pipeline validation — Phase 7

| Stage | Validated |
|---|---|
| Launcher → per-tenant queue | ✅ |
| Worker drains per-tenant bucket | ✅ |
| Per-tenant board upload + read | ✅ |
| Worker board lookup via per-tenant path | ✅ |
| Mission ingest + planning starts | ✅ |
| Orchestrator launches both planner + robot_sim with proper env | ✅ |
| Planner mqtt_hub instantiated + wired to action_server | ✅ |
| planner_ui /api/submit_mission via NATS JobQueue | ✅ |
| Planner ↔ robot link handshake (`LINK: rover_1 → live`) | ✅ |
| Link stable past first heartbeat window | ✅ (gap-9 fix) |
| Robot survives planner restart (re-announce + handshake) | ✅ (gap-10 fix) |
| Mission state advances `planning → executing → completed` | ✅ |
| Drive packets (CBOR) → robot RPC decode → drive_ack/drive_done | ✅ (gap-11 fix) |
| **Full mission completes success=true** | ✅ (today) |
| Multi-tenant isolation (tenant-2 doesn't see tenant-1's missions/boards) | ❌ **Step C** |

## Tomorrow's sequence

| Step | What | Outcome |
|---|---|---|
| C | **Multi-tenant validation.** Add `mission_planner_02` (planner_namespace="tunnel_ops") + `robot_sim_rover_2` to topology. Rebuild master KB, slice_bootstrap, let orchestrator reconcile spawn the new instances. Upload `tunnel_ops` board (can be a copy of landing_zone or new). Submit missions to both tenants in parallel. Verify: tenant-2's mission doesn't appear in tenant-1's `/api/missions`; tenant-2's board doesn't shadow tenant-1's; per-tenant NATS bucket separation visible. | Phase 7 multi-tenant fully validated. |
| D | **Drop matplotlib viewer** (per locked decision 2026-05-10):<br>- Delete `construction/scripts/board_dsl/visualizer.py`<br>- Delete `construction/tests/board_dsl/test_visualizer_smoke.py`<br>- Update `project_planner_implementation_plan.md` Phase 4 status<br>- Remove from continue.md known-issues + parking lot | Cleaner repo. |
| E | **Map UI tweak: per-L2-segment click-popup.** Add click handler per leaf in `map_render.js renderSegment` that pops up properties panel (mirrors `showNodePopup` from 5b C4). Per-leaf fields:<br>- straight_line / spline → start_pos, end_pos, end_heading<br>- rotate → end_heading + tangent indicator<br>- wall_follow / line_follow → base kind + offset<br>- activate → action_id, kb_ref, params<br>Tests: renderer-test grep pattern, asserts `showSegmentPopup` exists, click handler attached, leaf-specific fields rendered. | Operator can inspect any segment in L2. |
| F | After E green: re-prioritize parking lot. | Honest re-prioritization. |

## Step C detailed plan

Files to edit:
1. `/home/gedgar/knowledge_base_assembly/luajit_programs_and_containers/nano_data_center_base/commissioning_software/system_node_control/construction/catalogs/topology.lua` lines 130-160 (cpu_02 instances). Add:
   ```lua
   { name = "mission_planner_02", def = "mission_planner",
     ports = { ui = 19009 },
     params = { planner_namespace = "tunnel_ops" } },
   { name = "robot_sim_rover_2", def = "robot_sim",
     ports = {},
     params = {
       robot_id          = "rover_2",
       planner_namespace = "tunnel_ops",
       capabilities      = { "recharge", "dock_in", "dock_out" },
     } },
   ```
2. Create a board for tunnel_ops (or just copy landing_zone): the upload happens via `upload_board.lua --planner-namespace tunnel_ops`.

Steps:
```bash
cd ~/knowledge_base_assembly/luajit_programs_and_containers/nano_data_center_base/commissioning_software/system_node_control

# 1. Edit topology
$EDITOR construction/catalogs/topology.lua

# 2. Rebuild master KB
bash construction/build_kb.sh

# 3. Re-slice bootstrap (regenerates deployment/<cpu>/bootstrap.db)
bash construction/slice_bootstrap.sh

# 4. The dcs.lua processes read bootstrap.db at startup. Restart cpu_02
#    (where mission_planner_02 will land). cpu_01 stays up. Use the
#    deployment's stop.sh + start.sh — DON'T pkill -f the path (see
#    feedback_pkill_pid_match.md).
bash deployment/cpu_02/stop.sh
bash deployment/cpu_02/start.sh

# 5. Orchestrator reconciles → spawns mission_planner_02 + robot_sim_rover_2
#    Watch:
docker ps | grep -E 'mission_planner|robot_sim'
docker logs mission_planner_02 --since 30s 2>&1 | grep -E 'LINK:|action_server'

# 6. Upload board for tunnel_ops
luajit construction/scripts/upload_board.lua \
    --board landing_zone \
    --planner-namespace tunnel_ops

# 7. Submit missions to both tenants
curl -sS -X POST -H 'Content-Type: application/json' \
    -d '{"robot_id":"rover_1","board":"landing_zone","source":"lander_pad","target":"habitat_site"}' \
    http://localhost:19005/api/submit_mission

curl -sS -X POST -H 'Content-Type: application/json' \
    -d '{"robot_id":"rover_2","board":"landing_zone","source":"lander_pad","target":"habitat_site"}' \
    http://localhost:19009/api/submit_mission

# 8. Verify isolation
curl -s http://localhost:19005/api/missions | jq '.missions'   # only rover_1
curl -s http://localhost:19009/api/missions | jq '.missions'   # only rover_2
docker exec pg-vector psql -U gedgar knowledge_base -c \
    "SELECT path::text FROM knowledge_base_fs_node;"
# Expect: <ns_01>.boards.landing_zone AND <ns_02>.boards.landing_zone, no shadow
```

Risk: rebuilding KB + restarting cpu_02 can disturb the currently-green
cluster. Cheap revert: `git checkout topology.lua` + rebuild + restart.

## Operational state of the cluster (end of session)

- Orchestrator alive (cpu_01 + cpu_02 dcs.lua processes).
- mission_planner_01 + robot_sim_rover_1 alive on planner-net.
- mission_planner image: built at the gap-9/10/11 SHAs:
  `nanodatacenter/mission-planner:latest` (sha256 `cc28e1765c11`).
- robot_sim image: built with CBOR + drive_ack dispatch (current latest).
- Per-tenant pg rows present. landing_zone board at
  `system.moon_base.site.moon_base_alpha.planner.mission_planner_01.boards.landing_zone`
  with sha256 `f4fb864500366097…`.
- Two missions ran today, both `state=completed, success=true, replans=0`.
- One cosmetic anomaly: `/api/missions` `active_missions` field overcounts
  (mission_count is monotonic; never decremented on done/error). After
  planner restart it resets to 0.

## Quick start tomorrow

```bash
cd ~/knowledge_base_assembly/luajit_programs_and_containers/nano_data_center_base/commissioning_software/system_node_control

# Verify recent commits + nothing got lost
git log --oneline -8

# Cluster state
ps aux | grep dcs.lua | grep -v grep
docker ps | grep -E 'mission_planner|robot_sim'

# Quick smoke: prior gap-fixes still hold
curl -sS -X POST -H 'Content-Type: application/json' \
    -d '{"robot_id":"rover_1","board":"landing_zone","source":"lander_pad","target":"habitat_site"}' \
    http://localhost:19005/api/submit_mission
sleep 10
curl -s http://localhost:19005/api/mission/rover_1 | jq
# Expect: state="completed", success=true, replans=0

# Re-run host-side tests as sanity check
luajit construction/tests/test_planner_ui_submit_mission.lua   # 51/51
luajit construction/tests/test_planner_ui_status.lua           # 45/45
luajit construction/tests/test_action_server_phase7.lua        # 27/27
luajit construction/tests/test_robot_sim_package.lua           # 84/84
luajit construction/tests/test_mock_mqtt_robot.lua             # 38/38
luajit construction/tests/test_drive_v2_dispatch.lua           # 34/34

# THEN start Step C per "Step C detailed plan" section above.
```

## Architectural references

1. `~/.claude/projects/-home-gedgar-knowledge-base-assembly/memory/project_planner_active_node_contract.md`
2. `~/.claude/projects/-home-gedgar-knowledge-base-assembly/memory/project_planner_implementation_plan.md`
3. `~/.claude/projects/-home-gedgar-knowledge-base-assembly/memory/project_phase7_multitenant_design.md`
4. `~/.claude/projects/-home-gedgar-knowledge-base-assembly/memory/feedback_robot_wire_dispatch.md` (NEW today)
5. `~/.claude/projects/-home-gedgar-knowledge-base-assembly/memory/feedback_luajit_stdio_buffering.md` (NEW today)

## Operating-mode reminders

- **Testing delegation for THIS phase** (locked 2026-05-10): assistant runs cluster smoke (overrides feedback_user_driven_testing.md). Scope ends after Phase 7 + ROBSIM signed off.
- **No application auth** per `feedback_no_application_auth.md`.
- **One layer = one commit** per `feedback_holding_commits.md`.
- **No-soft-faults** rule for control-system fault paths (UI 4xx/5xx errors are fine).
- **Bare "yes" means accept the recommendation** per `feedback_yes_means_accept.md`.
- **For diagnostic logs in containerized LuaJIT processes**: use `io.stderr:write+flush`, NOT `print()` — stdout is buffered (see `feedback_luajit_stdio_buffering.md`).
- **pkill on paths** silently no-ops for `bash ./start.sh` patterns — use `deployment/<cpu>/stop.sh` (see `feedback_pkill_pid_match.md`).

## Rollback recipes

```bash
# Undo this session (back to "lock tomorrow's sequence" commit)
git reset --hard a548fffd

# Undo only gap-11 (keep gap-9 + gap-10)
git reset --hard d7810a8a

# Undo only gap-10 (keep gap-9)
git reset --hard 1b5b1148

# Undo all 3 session commits
git reset --hard 56ffcb66

# Inspect this session's commits
git log --oneline 56ffcb66..HEAD
git show ad463ce7 --stat
git show d7810a8a --stat
git show 1b5b1148 --stat
```

## Known issues / parking lot (not in tomorrow's sequence)

- `active_missions` field on `/api/missions` overcounts (mission_count is monotonic; never decremented on done/error). Cosmetic for dashboard. Fix when revisiting action_server's mission lifecycle.
- planner_ui's lazy NATS singletons don't reconnect on NATS restart.
- planner_ui's NATS_URL/MQTT_HOST hardcoded to cluster defaults. **Right architectural fix**: use `infra_discovery` from pg (parallel to planner worker). Parking lot — would land alongside lifting `MQTT_HOST` for robot_sim too.
- Phase 4 C5 visualizer Python smoke needs matplotlib in WSL venv. → resolved by Step D.
- Node-properties authoring (per `project_v2_board_dsl_design.md`). Parking lot.
- pre-existing: `mission_builder.rebuild`'s 4th arg `current_heading` silently dropped. Out of scope.
- 5 C5 follow-up (delete legacy nav code) — UNGATED but deprioritized.
- pre-existing test failure: `test_mission_simulator_integration.lua` errors on host loading `encoder.lua_cbor` FFI. Not a regression; deferred.

---

*continue.md rewritten 2026-05-10 EOD #2 (after gap-9 + gap-10 bundle + gap-11 ship, golden-path smoke GREEN end-to-end, gap-7 audit closed). 3 fixed + 1 audited. Phase 7 ~75% complete on A→F sequence; remaining: C (multi-tenant) → D (drop matplotlib) → E (L2 popup) → F (re-prioritize).*
