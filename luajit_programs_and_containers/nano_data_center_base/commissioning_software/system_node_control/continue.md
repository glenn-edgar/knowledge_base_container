# Nanodatacenter DCS — Continuation Plan

## State at end of 2026-05-10 evening (revised) — Phase 7 gap-6 + gap-8 fixed; Step B blocked on gap-9

**~42 commits ahead of origin/master** after this session's two fixes. Two gaps fixed and committed (`a3bb0bfa`, `f567a1df`); one gap mitigated tactically (gap 7 — board re-uploaded, root cause unknown); one new gap surfaced and diagnosed but not fixed (gap 9 — heartbeat-period mismatch). Step B golden-path smoke still blocked.

## This session's commits (2 implementation + this wrap)

| # | Commit | What |
|---|---|---|
| 1 | `a3bb0bfa` | gap-6 fix: planner_ui empty-string NATS_URL fallback trap (3 sites in submit.lua + status.lua) |
| 2 | `f567a1df` | gap-8 fix: action_server always polls MQTT in idle path (drop mission_count gate) |
| 3 | (this wrap) | docs(continue.md): revised after gap 6+8 ship; gap 7+9 documented |

## Today's gap progression

| Gap | Status | Note |
|---|---|---|
| 6 — openresty NATS_URL empty-string trap | ✅ Fixed | Lua `""` is truthy; `or "..."` chain stopped at empty. `nonempty()` helper. Planner worker untouched (uses infra_discovery from pg). 51+45 host-side tests still green. |
| 7 — `knowledge_base_fs_node` empty | ⚠️ Mitigated | Re-uploaded landing_zone via `upload_board.lua --planner-namespace mission_planner_01` (sha256=f4fb8645…). Root cause of original wipe NOT investigated — possible test isolation issue (cf. `feedback_test_db_isolation.md`) or build_kb without follow-up upload. |
| 8 — action_server idle MQTT poll dead | ✅ Fixed | `action_server.lua:930` gated `mqtt_hub:poll_and_route` on `mission_count == 0`, but mission_count is monotonic (only incremented). After 1st failed claim, link traffic never delivered. Fix: poll unconditionally. |
| 9 — heartbeat period mismatch | ❌ Diagnosed | Robot `HB_PERIOD_S=10`; planner stale at `HEARTBEAT_INTERVAL=2 × HEARTBEAT_MISS_LIMIT=3 = 6s` (logs say 7s, presumably +1s tick slack). Link bounces live → stale → offline → registering → live → … indefinitely. Mission stuck in `state="planning"`. |

## End-state of pipeline validation

| Stage | Validated |
|---|---|
| Launcher → per-tenant queue | ✅ |
| Worker drains per-tenant bucket | ✅ |
| Per-tenant board upload + read | ✅ |
| Worker board lookup via per-tenant path | ✅ |
| Mission ingest + planning starts | ✅ (gap 7 mitigation) |
| Orchestrator launches both planner + robot_sim with proper env | ✅ |
| Planner mqtt_hub instantiated + wired to action_server | ✅ |
| planner_ui /api/submit_mission via NATS KeyStore from openresty | ✅ (gap 6 fix) |
| action_server kv_writer ticks without crashing | ✅ |
| Planner ↔ robot link handshake (`LINK: rover_1 → live`) | ✅ (gap 8 fix) |
| Link stability past first heartbeat window | ❌ **gap 9** |
| Mission state advances `planning → executing` | ❌ blocked by gap 9 |
| Drive packets → robot RPC echo → mission_log update | ❌ blocked by gap 9 |

## Tomorrow's revised sequence

| Step | What | Outcome |
|---|---|---|
| A | **Gap 9 fix**. Constants live in `nano_data_center_instance/app_containers/mission_planner/container/planner/lib/link_manager.lua` lines 49-53 (`HEARTBEAT_INTERVAL=2`, `HEARTBEAT_MISS_LIMIT=3`, `STALE_TO_OFFLINE_TIMEOUT=15`, `REGISTRATION_TIMEOUT=10`, `PLANNER_HB_INTERVAL=3`) and robot's `HB_PERIOD_S=10` env-defaulted in `nano_data_center_instance/app_containers/robot_sim/container/robot_sim/main.lua:79`. **Recommended**: lower robot HB to 3s (matches `PLANNER_HB_INTERVAL`) AND raise miss limit to 5 (so 6×2=12s tolerates a docker-desktop hiccup). Or expose HB_PERIOD_S via env injection from supervisor's app_env — this is a robot-side spawn env, currently using main.lua's `tonumber(env_or("HB_PERIOD_S", "10"))`. **Trace before changing**: was 10s ever validated to work, or did the original smoke just happen to fit inside one heartbeat-miss window? Check git log on `link_manager.lua` for tuning history. | Stable link past first 30s; mission can dispatch. |
| B | Once A green: full pipeline smoke via `/api/submit_mission`. Verify `planning → executing → completed`, drive packets → robot RPC echo → kb_done arrives → mission_log updates. | Phase 7 + ROBSIM end-to-end validated. |
| B.1 | **Gap 7 root cause** before going multi-tenant. Run smoke twice in a row, verify fs_node survives. Inspect every script that touches `knowledge_base_fs_node` (TRUNCATE / DELETE / DROP). Check `migrate_phase7.sh`, `build_kb.lua`, every `test_*.lua` that uses pg. The risk: gap 7 re-fires when we add tenant 2, wiping tenant 1's boards mid-test. | Either confirmed-safe OR test isolation patch shipped. |
| C | Validate the multi-tenant path. Add `mission_planner_02` to topology with `planner_namespace="tunnel_ops"`. Two-tenant cluster smoke. Per-tenant bucket isolation: tenant-2 missions don't see tenant-1's NATS keys; tenant-2 board upload doesn't shadow tenant-1's. | Phase 7 multi-tenant fully validated. |
| D | **Drop matplotlib viewer** (per locked decision 2026-05-10):<br>- Delete `construction/scripts/board_dsl/visualizer.py`<br>- Delete `construction/tests/board_dsl/test_visualizer_smoke.py`<br>- Update `project_planner_implementation_plan.md` Phase 4 status<br>- Remove from continue.md known-issues + parking lot | Cleaner repo. |
| E | **Map UI tweak: per-L2-segment click-popup (option b).** Add click handler per leaf in `map_render.js renderSegment` that pops up properties panel (mirrors `showNodePopup` from 5b C4). Per-leaf fields:<br>- straight_line / spline → start_pos, end_pos, end_heading<br>- rotate → end_heading + tangent indicator<br>- wall_follow / line_follow → base kind + offset<br>- activate → action_id, kb_ref, params<br>Tests: renderer-test grep pattern, asserts `showSegmentPopup` exists, click handler attached, leaf-specific fields rendered. | Operator can inspect any segment in L2. |
| F | After E green: pause; pick parking-lot items per priority. | Honest re-prioritization. |

## Operational state of the cluster

- Orchestrator IS running (both cpu_01 + cpu_02 dcs.lua processes alive since this morning).
- mission_planner_01 + robot_sim_rover_1 alive on planner-net (orchestrator-managed).
- mission_planner image is the gap-8-fixed build; SHA `b6c45d51…`.
- robot_sim image unchanged.
- Per-tenant pg rows present. landing_zone board uploaded to per-tenant path (sha256 f4fb8645…); `/api/board/landing_zone` returns it. `/api/missions` works. `/api/submit_mission` accepts and returns job_ids.
- Planner heartbeat ticks. Action_server claims missions. Link handshake completes but bounces (gap 9).
- mission_count is currently inflated (failed claims accumulate). After tomorrow morning's planner restart, it'll be 0 again.

## Quick start tomorrow

```bash
cd ~/knowledge_base_assembly/luajit_programs_and_containers/nano_data_center_base/commissioning_software/system_node_control

git log --oneline -5
git status

# Verify orchestrator still alive
ps aux | grep dcs.lua | grep -v grep

# Verify containers + their image SHAs
docker ps | grep -E 'mission_planner|robot_sim'
docker inspect mission_planner_01 --format '{{.Image}}'   # should match latest build SHA

# Verify gap-6 + gap-8 fixes in code
grep -n nonempty ~/knowledge_base_assembly/luajit_programs_and_containers/nano_data_center_instance/app_containers/mission_planner/container/planner_ui/lua/submit.lua
grep -n "Always poll MQTT" ~/knowledge_base_assembly/luajit_programs_and_containers/nano_data_center_instance/app_containers/mission_planner/container/planner/lib/action_server.lua

# Confirm gap 9 still bites (link bounces)
docker logs mission_planner_01 --since 60s | grep "LINK:" | tail -10

# Re-run host-side tests
luajit construction/tests/test_planner_ui_submit_mission.lua   # 51/51
luajit construction/tests/test_planner_ui_status.lua           # 45/45
luajit construction/tests/test_action_server_phase7.lua        # 27/27
```

## Architectural references

1. `~/.claude/projects/-home-gedgar-knowledge-base-assembly/memory/project_planner_active_node_contract.md`
2. `~/.claude/projects/-home-gedgar-knowledge-base-assembly/memory/project_planner_implementation_plan.md`
3. `~/.claude/projects/-home-gedgar-knowledge-base-assembly/memory/project_planner_team_scope.md`
4. `~/.claude/projects/-home-gedgar-knowledge-base-assembly/memory/project_phase7_multitenant_design.md`

## Operating-mode reminders

- **Testing delegation for THIS phase** (locked 2026-05-10): assistant runs cluster smoke (overrides feedback_user_driven_testing.md). Scope ends after Phase 7 + ROBSIM signed off.
- **No application auth** per `feedback_no_application_auth.md`.
- **One layer = one commit** per `feedback_holding_commits.md`.
- **No-soft-faults** rule for control-system fault paths (UI 4xx/5xx errors are fine).
- **Bare "yes" means accept the recommendation** per `feedback_yes_means_accept.md`.

## Rollback recipes

```bash
# Undo this session (back to "lock tomorrow's sequence" commit)
git reset --hard a548fffd

# Undo only gap-8 (keep gap-6)
git reset --hard a3bb0bfa

# Undo only gap-6 (return to "gaps 1+5+kv_writer")
git reset --hard fffd10b8

# Inspect this session's commits
git log --oneline a548fffd..HEAD
git show f567a1df --stat
git show a3bb0bfa --stat
```

## Known issues / parking lot (not in tomorrow's sequence)

- Phase 4 C5 visualizer Python smoke needs matplotlib in WSL venv. → resolved by Step D.
- Node-properties authoring (per `project_v2_board_dsl_design.md`). Parking lot.
- pre-existing: `mission_builder.rebuild`'s 4th arg `current_heading` silently dropped. Out of scope.
- 5 C5 follow-up (delete legacy nav code) — UNGATED but deprioritized.
- planner_ui's lazy NATS singletons don't reconnect on NATS restart.
- planner_ui's NATS_URL default is hardcoded to "nats://nats-js-ram:4222" (matches MQTT_HOST default in robot_sim/main.lua). **Right architectural fix**: use `infra_discovery` from pg (parallel to planner worker's main.lua:150). Parking lot — would land alongside lifting `MQTT_HOST` for robot_sim too.
- Gap 7 root cause still open. Mitigation works for now (re-upload), but a wipe-on-restart bug somewhere in the test or build path will fire again. → Step B.1 in tomorrow's sequence.
- mission_count is monotonic. Done/error missions never decrement it. Not a correctness issue post-gap-8, but `/api/missions` `active_missions` field overcounts. Cosmetic for the dashboard. Add a fix when revisiting action_server's mission lifecycle.

---

*continue.md rewritten 2026-05-10 EOD (after gap-6 + gap-8 ship + gap-7 mitigation + gap-9 diagnosed). 2 fixed, 1 mitigated, 1 outstanding. Pipeline working through link handshake; link stability is the holdout.*
