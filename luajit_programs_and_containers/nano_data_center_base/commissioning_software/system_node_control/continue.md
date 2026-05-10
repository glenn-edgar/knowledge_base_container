# Nanodatacenter DCS — Continuation Plan

## State at end of 2026-05-10 evening — Phase 7 + ROBSIM substantially live; gap 6 blocks full smoke

**~38-40 commits ahead of origin/master** (commit count grows with this session's wrap commit), comprehensive cluster validation, **5 of 6 surfaced gaps fixed**, full robot link bridge + params injection chain working, **planner_ui's openresty NATS connect remains as gap 6 for next session**.

## Today's commits (5 implementation + 2 doc + 1 wrap = 8 commits)

| # | Commit | What |
|---|---|---|
| 1 | `017b9a39` | ROBSIM C1 — robots.lua subsystem + ctx.ROBOTS |
| 2 | `4020bd46` | docs: previous EOD wrap |
| 3 | `953bcfa5` | ROBSIM C2 — robot_sim container package |
| 4 | `33fe03c9` | ROBSIM C3 cluster smoke + per-tenant fixes (upload_board + kb_query paths) |
| 5 | `a0855393` | gap-4 fix — kb_query find_by_pattern → raw SQL via KBM.conn |
| 6 | (this session) | gap-1+5+kv_writer+NATS_URL + smoke writeup commit |

## End-state of pipeline validation

| Stage | Validated |
|---|---|
| Launcher → per-tenant queue | ✅ |
| Worker drains per-tenant bucket | ✅ |
| Per-tenant board upload + read | ✅ (fixed mid-smoke commit 4) |
| Worker board lookup via per-tenant path | ✅ (gap 3 fix commit 4) |
| Mission ingest + planning starts | ✅ (gap 4 fix commit 5) |
| Orchestrator launches both planner + robot_sim with proper env | ✅ (gap 1 fix this session) |
| Planner mqtt_hub instantiated + wired to action_server | ✅ (gap 5 fix this session) |
| Planner → robot link_bridge_ack flow (`LINK: rover_1 → registering → live (caps=6)`) | ✅ |
| action_server kv_writer ticks without crashing | ✅ (kv_writer late-bind this session) |
| planner_ui /api/submit_mission via NATS KeyStore from openresty | ❌ **gap 6** |
| Robot RPC echo end-to-end | ⏸ blocked by gap 6 in the test path |

## Outstanding gap for next session

### **Gap 6: planner_ui openresty handler can't open NATS KeyStore**

**Symptom**: `/api/submit_mission` and `/api/missions` both return `500 ks connect: nats_key_store.lua:172: KeyStore error: connection error`.

**What we know rules things OUT:**
- ✅ NATS server (nats-js-ram) is running and reachable from inside mp_01 container
- ✅ libnats FFI library works — direct test from mp_01's `/usr/local/openresty/luajit/bin/luajit -e "..."` returned `connected!`
- ✅ Same library, same server, same network all working from the planner WORKER process (`LINK: rover_1 → live` happens, which requires _ensure_nats to have succeeded)
- ✅ DNS resolves (`getent hosts nats-js-ram` → 172.18.0.6)
- ✅ Image is fresh (verified Image SHA + grep'd new code IS present)

**What's specific to the failure path**: openresty nginx worker → planner_ui's submit.lua `ensure_jq` → ks_lib.KeyStore.new → k:connect() → fails. STANDALONE luajit doing the EXACT same call from the same container succeeds.

**Hypotheses to investigate**:
- openresty's cosocket/FFI sandbox has some restriction on libnats's blocking C client
- per-request package.loaded reload causes the FFI library state to re-init and fail subsequent connects (would require module init to not be idempotent)
- libnats's threading interacts badly with openresty's worker model
- A required env (NATS_URL? libnats config dir?) is in the planner worker's env but not propagated to openresty workers (openresty `env` directive in nginx.conf strips by default — but I declared `env NATS_URL;` already)

**Next-session recipe to investigate gap 6**:
1. Check what `env NATS_URL;` actually exposes to nginx workers (run `docker exec mission_planner_01 cat /proc/$(pgrep -f openresty)/environ | tr '\\0' '\\n' | grep NATS`).
2. Try connecting from openresty handler context via raw bash test that exec's a luajit -e — if THAT fails, it's the openresty process env vs subprocess env distinction.
3. If it's an FFI module-state issue: try preloading lib.nats_key_store at nginx init_by_lua_block (forces single-process-lifetime FFI init, not per-request).
4. As a workaround for full pipeline validation: bypass planner_ui's submit; build a CLI submit_test_mission.lua that runs in the worker's process context (where NATS works) and submits via JobQueue directly.

**Why this is gap 6 and not "Phase 7 done"**: Phase 7's whole architecture is "planner_ui submits via FFI direct to NATS JobQueue" (option 4 from yesterday's design). If that path doesn't work in production openresty, Phase 7's option-4 design is broken. Either fix gap 6, or revisit the option-4 vs option-2 (HTTP-to-worker) decision.

## Operational state of the cluster

- Orchestrator IS running (was started during this session). `ps aux | grep dcs.lua` shows 4 processes (2 watchdog + 2 dcs.lua per-CPU).
- mission_planner_01 + robot_sim_rover_1 are both alive on planner-net (orchestrator-managed, auto-spawned by RECONCILE).
- Per-tenant pg rows present: `app_containers.<inst>.spec.params`, `planner.mission_planner_01.robots.catalog.robot.rover_1`, etc.
- landing_zone board uploaded to per-tenant path; `/api/board/landing_zone` returns it correctly.
- robot link is `live` per planner logs.
- mp_01's planner worker is in serve loop; can drain NATS; ticks heartbeat every 5s.

## Tests at end of session

All 22 host-side tests still green (no test additions for gap-1/5 fixes — they're cluster-validated only):
- 13 standalone tests (sum: 545)
- 5 board_dsl tests (sum: 153)
- 4 FFI-dependent tests (sum: 136)

**Total: 864 host-side tests** (no change from morning since these fixes are cluster-only).

## Quick start tomorrow

```bash
cd ~/knowledge_base_assembly/luajit_programs_and_containers/nano_data_center_base/commissioning_software/system_node_control

git log --oneline -10
git status

# Verify orchestrator still alive
ps aux | grep dcs.lua | grep -v grep

# Verify containers still alive
docker ps | grep -E 'mission_planner|robot_sim'

# Check link bridge
docker logs mission_planner_01 2>&1 | grep "LINK:" | tail
# Expected: "LINK: rover_1 → live (wire=json, caps=6)"

# Re-run host-side smoke
luajit construction/tests/test_planner_phase1_catalog.lua            # 20/20
luajit construction/tests/test_planner_ui_submit_mission.lua         # 51/51
luajit construction/tests/test_planner_ui_status.lua                 # 45/45
luajit construction/tests/test_action_server_phase7.lua              # 27/27
luajit construction/tests/test_robot_sim_package.lua                 # 84/84
luajit construction/tests/test_robots_subsystem.lua                  # 37/37
# (and the others — see earlier continue.md for full list)
```

## Tomorrow's recommended sequence

| Step | What | Outcome |
|---|---|---|
| A | Investigate gap 6 (openresty + libnats FFI). Start with the env-vs-subprocess distinction (recipe above), then init_by_lua_block preload, then the rest of the hypotheses. | Either gap 6 fixed OR architectural pivot back to HTTP-to-worker (option 2 from Phase 7 Q3 design). |
| B | If gap 6 fixed: full pipeline smoke via /api/submit_mission. Verify drive packets flow + robot RPC echo + kb_done arrives + mission_log updates. | Phase 7 + ROBSIM end-to-end validated. |
| C | If gap 6 fixed: also verify the C5/C6 path (add mission_planner_02 to topology with planner_namespace="tunnel_ops" — gap 1 chain handles env injection now). Two-tenant cluster smoke. | Phase 7 multi-tenant fully validated. |
| D | After C green: matplotlib viewer, node-properties authoring, or pause. | Honest re-prioritization. |

## Architectural references

1. `~/.claude/projects/-home-gedgar-knowledge-base-assembly/memory/project_planner_active_node_contract.md`
2. `~/.claude/projects/-home-gedgar-knowledge-base-assembly/memory/project_planner_implementation_plan.md`
3. `~/.claude/projects/-home-gedgar-knowledge-base-assembly/memory/project_planner_team_scope.md`
4. `~/.claude/projects/-home-gedgar-knowledge-base-assembly/memory/project_phase7_multitenant_design.md` (read full Outstanding gaps section for gap 6 context)

## Operating-mode reminders

- **Testing delegation for THIS phase** (locked 2026-05-10): assistant runs cluster smoke (overrides feedback_user_driven_testing.md). Scope ends after Phase 7 + ROBSIM signed off.
- **No application auth** per `feedback_no_application_auth.md`.
- **One layer = one commit** per `feedback_holding_commits.md`.
- **No-soft-faults** rule for control-system fault paths (UI 4xx/5xx errors are fine).
- **Bare "yes" means accept the recommendation** per `feedback_yes_means_accept.md`.

## Rollback recipes

```bash
# Undo all of today's morning + afternoon work (back to yesterday's EOD)
git reset --hard 4020bd46

# Inspect today's commits
git log --oneline 4020bd46..HEAD
git show HEAD --stat
```

## Known issues / parking lot (other than gap 6)

- Phase 4 C5 visualizer Python smoke needs matplotlib in WSL venv. Parking lot.
- Node-properties authoring (per `project_v2_board_dsl_design.md`). Parking lot.
- pre-existing: mission_builder.rebuild's 4th arg current_heading silently dropped. Out of scope.
- 5 C5 follow-up (delete legacy nav code) — UNGATED but deprioritized.
- planner_ui's lazy NATS singletons don't reconnect on NATS restart. Gap 6 makes this moot for now.
- Planner_ui's NATS_URL default is hardcoded to "nats://nats-js-ram:4222" (matches MQTT_HOST default in robot_sim/main.lua). Real fix: use infra_discovery from pg. Parking lot.

---

*continue.md rewritten 2026-05-10 evening (after gap-1+5+kv_writer fixes ship + gap-6 surface). 5 fixed gaps + 1 outstanding. Pipeline working end-to-end on the worker side; planner_ui NATS access is the holdout.*
