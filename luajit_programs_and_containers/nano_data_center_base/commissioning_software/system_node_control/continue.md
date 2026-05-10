# Nanodatacenter DCS — Continuation Plan

## State at end of 2026-05-10 EOD #3 — Phase 7 multi-tenant GREEN end-to-end

**48 commits ahead of origin/master** after this session's three commits.
**Phase 7 A→F sequence complete.** Multi-tenant isolation validated in a
live cluster: two planners (`mission_planner_01` namespace, `tunnel_ops`
namespace) each own one robot, submit missions in parallel, both
complete with `success=true, replans=0` — no cross-tenant leak.
Matplotlib viewer dropped. Per-L2-segment click-popup shipped.

## This session's commits (3 implementation)

| # | Commit | What |
|---|---|---|
| 1 | `26687181` | gap-12 Step C: 4 multi-tenant bugs fixed — build_kb PLANNERS namespace, link_manager allowed_robots filter, mqtt_hub per-tenant client_id, global_planner threading. 296 host tests green; cluster smoke parallel two-tenant green. |
| 2 | `571dc605` | Step D: drop matplotlib visualizer.py + test_visualizer_smoke.py + stale comment in renderer test. 137/137 renderer tests still green. |
| 3 | `b31833af` | Step E: per-L2-segment click-popup. `<g class="leaf-group">` wrapper + `showSegmentPopup()` dispatch by leaf kind. Renderer tests +20 → 157/157. Total host regression: 453 green. |

## Gap progression — final

| Gap | Status | Note |
|---|---|---|
| 12a — build_kb reads inst.planner_namespace (always nil) | ✅ Fixed (`26687181`) | Should be `inst.params.planner_namespace`. Worked accidentally for `mission_planner_01` because namespace == inst.name; second tenant exposed it. |
| 12b — link_manager has no tenant filter | ✅ Fixed (`26687181`) | mqtt_hub subscribes site-wide; every planner saw every robot. Added kb_query.list_tenant_robots + allowed_robots set threaded through action_server. Foreign robots dropped silently. 14 new unit tests. |
| 12c — MQTT client_id collision | ✅ Fixed (`26687181`) | Both planners used `planner_<site>` → second connection kicked out the first (rc=7). Now `planner_<site>_<ns>`. |
| 12d — global_planner threading | ✅ Fixed (`26687181`) | `kb_query.new(...)` was called without `planner_namespace`, fell back to `own_instance_id`, looked at the wrong boards subtree → "board not found" for any tenant where ns != container name. |
| Audit — action_server pcall mask | ✅ Closed (Step C) | Original `q:list_boards()` call has been silently failing forever (kb_query has no `list_boards` method). _init_node has been nil the whole time; link_manager.on_link_change just skips write_position. Diagnosed via pcall-error logging, then the boards probe was split into its own pcall so the new robots query stays independent. |
| Step D | ✅ Done (`571dc605`) | Visualizer removed. |
| Step E | ✅ Done (`b31833af`) | Click any L2 leaf → properties popup. |

## End-state of Phase 7 validation

| Stage | Validated |
|---|---|
| Two tenants (mission_planner_01 + tunnel_ops) co-exist on cpu_02 | ✅ |
| Each planner enumerates only its own robots (KB query) | ✅ |
| Each planner registers only its own robot via MQTT (link filter) | ✅ |
| Foreign robot announces silently dropped (one-line log + no state alloc) | ✅ |
| Per-tenant NATS buckets: `KV_<site>_planner_<ns>_action_server` + `_mission_log` | ✅ |
| Per-tenant board rows in `fs_node` under `planner.<ns>.boards.*` | ✅ |
| Per-tenant `/api/missions` only lists own missions/robots | ✅ |
| Per-tenant `/api/submit_mission` queues under per-tenant subject | ✅ |
| Per-tenant MQTT client_id prevents broker kick-out | ✅ |
| Parallel mission completion: rover_1 + rover_2 both `success=true replans=0` | ✅ |
| Per-leaf properties popup in L2 view | ✅ (Step E) |
| Drop matplotlib viewer | ✅ (Step D) |

## Re-prioritized parking lot (Step F)

Priority groupings: **P1 = next session candidates** · **P2 = real-robot
gating** · **P3 = polish, no current blocker**.

### P1 — visible operational gaps

1. **MQTT broker reconnect / robot stuck-in-init.** Two related stability gaps surfaced today:
   - `mqtt_hub_transport` doesn't auto-reconnect on rc=7 disconnect (we hit this when the two planners collided; fixed the collision, but a genuine broker hiccup would still permanently disconnect a planner).
   - `robot_sim` doesn't retry `link_announce` if the first one gets no ack within a window; robot stays in `state=init` until container restart. Hit during the Step C debug cycle.
   Both should add a bounded-retry + reconnect timer. Estimate: 1 commit, mostly mqtt_pubsub plumbing.

2. **planner_ui NATS_URL / MQTT_HOST hardcoding.** Should use `infra_discovery` (parallel to planner worker) instead of env-injected defaults. Right architectural fix per the Phase 6 design. Lifts MQTT_HOST for robot_sim at the same time. Estimate: 1 commit per service that hardcodes.

3. **`active_missions` field on `/api/missions` overcounts.** `mission_count` is monotonic; never decremented on done/error. Cosmetic only (state column on each card is correct); the count is wrong on the dashboard until planner restart. Estimate: 1 small commit in action_server's mission lifecycle.

### P2 — gated on real robot

4. **5 C5 follow-up: delete legacy nav code.** Default-flip was done 2026-05-09; the legacy code path is unreachable in production once cluster validation is signed off on real hardware. Not blocking anything; just code hygiene.

5. **Node-properties authoring** (per `project_v2_board_dsl_design.md`). DSL skeleton landed; per-node property surface is the next slice. Real boards will need it before real-robot field trials.

### P3 — diagnostic / cleanup

6. **`test_mission_simulator_integration.lua` host-side FFI failure.** Pre-existing — errors on `encoder.lua_cbor` FFI load on host. Not a regression. Could be fixed by gating the test on `FFI_AVAILABLE`. Estimate: ~10 lines.

7. **`mission_builder.rebuild`'s 4th arg `current_heading` silently dropped.** Pre-existing. Out of scope for Phase 7. Document the contract before fixing so we don't paper over a real intent.

### Closed today (do not re-open)

- ~~Phase 4 C5 visualizer Python smoke needs matplotlib in WSL venv~~ — resolved by Step D (file deleted).

## Recommended next session

The cluster is healthy, two tenants are running, all four parking-lot
P1 items would land in 1–2 commits each. Two reasonable starting points:

**Option A (stability)** — P1 #1 + #3. Address the rc=7 and
stuck-in-init gaps we discovered today, plus fix the cosmetic
`active_missions` overcount. ~2 commits. Doesn't unlock new
capability but burns down the recent debugging-tax footprint.

**Option B (architecture)** — P1 #2. Migrate planner_ui +
robot_sim from hardcoded env to `infra_discovery`. ~2 commits.
Pays off later when we move off `host.docker.internal` defaults
(e.g. multi-host real cluster).

**Option C (Phase 8 boundary)** — Start the next phase of the
locked plan in `project_planner_implementation_plan.md`:
8 (multi-planner integration), or pivot to a new application
phase (per `project_v3_platform_roadmap.md` step list).

## Operational state of the cluster (end of session)

- Orchestrator alive (cpu_01 + cpu_02 dcs.lua).
- **TWO planners** on cpu_02 with distinct namespaces:
  - `mission_planner_01` (UI port 19005, namespace="mission_planner_01")
  - `mission_planner_02` (UI port 19009, namespace="tunnel_ops")
- **TWO robots** on cpu_02:
  - `robot_sim_rover_1` (owned by mission_planner_01)
  - `robot_sim_rover_2` (owned by tunnel_ops)
- mission_planner image: built this session at the Step C SHA.
- Per-tenant pg + NATS rows verified populated.
- Two final mission round-trips this session, both `state=completed, success=true, replans=0`.

## Quick start tomorrow

```bash
cd ~/knowledge_base_assembly/luajit_programs_and_containers/nano_data_center_base/commissioning_software/system_node_control

# Verify session commits + nothing got lost
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
sleep 10
curl -s http://localhost:19005/api/mission/rover_1 | jq
curl -s http://localhost:19009/api/mission/rover_2 | jq
# Expect: both state=completed, success=true, replans=0

# Per-tenant isolation snapshot
curl -s http://localhost:19005/api/missions | jq '{robots: .registered_robots, missions: [.missions[].robot_id]}'
curl -s http://localhost:19009/api/missions | jq '{robots: .registered_robots, missions: [.missions[].robot_id]}'

# Host-side regression sweep
for t in test_planner_ui_submit_mission test_planner_ui_status \
         test_action_server_phase7 test_robot_sim_package test_mock_mqtt_robot \
         test_drive_v2_dispatch test_link_manager_tenant_filter \
         test_planner_ui_renderer; do
    luajit construction/tests/$t.lua | tail -2
done
# Expect: 51 + 45 + 27 + 87 + 38 + 34 + 14 + 157 = 453 green

# THEN pick from "Recommended next session" above.
```

## Architectural references

1. `~/.claude/projects/-home-gedgar-knowledge-base-assembly/memory/project_planner_active_node_contract.md`
2. `~/.claude/projects/-home-gedgar-knowledge-base-assembly/memory/project_planner_implementation_plan.md`
3. `~/.claude/projects/-home-gedgar-knowledge-base-assembly/memory/project_phase7_multitenant_design.md`
4. `~/.claude/projects/-home-gedgar-knowledge-base-assembly/memory/feedback_robot_wire_dispatch.md`
5. `~/.claude/projects/-home-gedgar-knowledge-base-assembly/memory/feedback_luajit_stdio_buffering.md`

## Operating-mode reminders

- **Testing delegation for THIS phase** (locked 2026-05-10): assistant runs cluster smoke; scope ends after Phase 7 + ROBSIM signed off. As of this session, **Phase 7 is signed off** — future stability/feature work reverts to the default (user-driven) testing mode unless explicitly re-locked.
- **No application auth** per `feedback_no_application_auth.md`.
- **One layer = one commit** per `feedback_holding_commits.md`.
- **No-soft-faults** rule for control-system fault paths.
- **Bare "yes" means accept the recommendation** per `feedback_yes_means_accept.md`.
- **`io.stderr:write+flush` not `print()`** for diagnostics in containerized LuaJIT processes (`feedback_luajit_stdio_buffering.md`).
- **`deployment/<cpu>/stop.sh` not `pkill -f`** (`feedback_pkill_pid_match.md`).
- **`build_kb.sh` wipes file_store** — after every rebuild, re-upload boards before any mission submit.
- **`stage_deploy.sh` after `slice_bootstrap.sh`** — the slicer wipes per-CPU dirs; the stager re-symlinks runtime/ + seeds env.sh.

## Rollback recipes

```bash
# Undo this session (back to 632ea8a3 EOD #2 wrap)
git reset --hard 632ea8a3

# Undo only Step E (keep C + D)
git reset --hard 571dc605

# Undo only Step D (keep C)
git reset --hard 26687181

# Undo only Step C
git reset --hard 632ea8a3

# Inspect this session's commits
git log --oneline 632ea8a3..HEAD
git show b31833af --stat   # Step E
git show 571dc605 --stat   # Step D
git show 26687181 --stat   # Step C
```

## Known issues / parking lot — see "Re-prioritized parking lot (Step F)" above.

---

*continue.md rewritten 2026-05-10 EOD #3 (after Step C multi-tenant + Step D viewer drop + Step E click-popup ship). Phase 7 A→F sequence COMPLETE. Two-tenant cluster validated end-to-end. 48 commits ahead of origin/master.*
