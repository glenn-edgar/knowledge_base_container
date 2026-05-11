# Nanodatacenter DCS — Continuation Plan

## State at end of 2026-05-10 EOD #5 — Mission planner feature-complete

**56 commits ahead of origin/master** (pushed at session end). Parking
lot fully drained. Next session is the **manual-drive sign-off smoke**;
if green, the mission-planner team's work is **complete** and remaining
work belongs to the active-hub program (separate team).

## EOD #4 + #5 commits (in order)

| Commit | What |
|---|---|
| `33ecc833` | P3 #5 — gate `test_mission_simulator_integration` on liblua_cbor.so |
| `d89314ac` | P1 #3 — fix `/api/missions active_missions` overcount |
| `71bc37dd` | P1 #4 — delete legacy nav code (−316 lines) |
| `37f2a749` | P1 #1 — MQTT auto-reconnect + robot_sim init-state retry |
| `10b32840` | P1 #2 — planner_ui infra_discovery for NATS_URL |
| `8a473842` | continue.md EOD #4 wrap |
| `c1f03481` | P3 #6 — drop unused `current_heading` arg (robot owns heading) |

Plus this commit (continue.md + final push).

## Architectural call locked this session

**Tenant-to-tenant handoff = active hub.** No planner-to-planner RPC,
no shared cross-tenant state. The border active hub owns the seam.
That collapses what was tracked as Phase 8 (multi-planner integration)
INTO the active-hub program. See
`~/.claude/projects/-home-gedgar-knowledge-base-assembly/memory/project_active_hub_handoff.md`.

The planner-side seam for future dynamic membership is ~30 lines (NATS
subscriber that adds to / removes from `allowed_robots` on hub-claim /
hub-release events). Gated on the hub program defining the subject +
payload shape — not on the planner team.

## Manual sign-off smoke (TOMORROW — 2026-05-11)

User-driven testing reverts to the default mode this session per
`feedback_user_driven_testing.md`. Walk this checklist; if every step
passes, the mission planner team is signed off.

```bash
cd ~/knowledge_base_assembly/luajit_programs_and_containers/nano_data_center_base/commissioning_software/system_node_control
git log --oneline -10        # verify EOD #5 + the heading-cleanup landed
docker ps | grep -E 'mission_planner|robot_sim|pg-vector|nats|mosquitto'
```

### 1. L1 + L2 map renders, both tenants

Browser:
- `http://localhost:19005/` (mission_planner_01)
- `http://localhost:19009/` (tunnel_ops)

Expect: pick a board (landing_zone), see the SVG topology render. Click an L1 edge → L2 detail view. Click an L2 leaf → properties popup with kind-specific fields (straight_line/spline → start_pos+end_pos+end_heading; rotate → end_heading; wall_follow/line_follow → base.kind+offset; activate → action_id+kb_ref+params).

### 2. Mission launcher (UI path, not curl)

In planner_01's UI: click **Pick source & target**, click two nodes on the map, type `rover_1`, submit. Watch the status overlay — should show queued → planning → executing → completed within ~10s. Repeat in planner_02 with `rover_2`.

### 3. `/api/missions` shape stays clean

```bash
curl -s http://localhost:19005/api/missions | jq '{robots: .registered_robots, active_missions, missions: [.missions[] | {robot_id, state}]}'
curl -s http://localhost:19009/api/missions | jq '{robots: .registered_robots, active_missions, missions: [.missions[] | {robot_id, state}]}'
```

Expect: each planner sees only its own robot + missions. `active_missions = 0` after any settled mission.

### 4. Broker bounce → auto-reconnect

```bash
docker restart mosquitto-ram-ws_main
sleep 12
docker logs mission_planner_01 2>&1 | grep -E 'mqtt_hub_transport:' | tail -8
# Expect: reconnect failed 1s → 2s → 4s → 8s → connected
```

Then submit fresh missions in both tenants — should complete `success=true replans=0`. (In-flight missions during the bounce are expected to fail with `max replans exceeded`; that's not store-and-forward.)

### 5. Sync-fail doesn't inflate active_missions

```bash
curl -sS -X POST -H 'Content-Type: application/json' \
    -d '{"robot_id":"rover_1","board":"DOES_NOT_EXIST","source":"a","target":"b"}' \
    http://localhost:19005/api/submit_mission
sleep 4
curl -s http://localhost:19005/api/missions | jq '.active_missions'
# Expect: 0
```

### 6. infra_discovery preferred over env

```bash
docker exec mission_planner_01 cat /opt/apps/planner_ui/logs/error.log | grep infra_lookup
# Expect: at least one "infra_lookup: NATS via registry -> nats://nats-js-ram:..." line
```

### Bonus: host regression sweep

```bash
for t in test_planner_ui_submit_mission test_planner_ui_status \
         test_action_server_phase7 test_robot_sim_package test_mock_mqtt_robot \
         test_drive_v2_dispatch test_link_manager_tenant_filter \
         test_planner_ui_renderer; do
    luajit construction/tests/$t.lua | tail -2
done
# Expect: 51 + 45 + 27 + 87 + 38 + 30 + 14 + 157 = 449 green

LD_LIBRARY_PATH=$(pwd)/../kb/mqtt \
    luajit construction/tests/test_mission_simulator_integration.lua | tail -2
# Expect: 26 passed
```

## After sign-off

Mission-planner-team work is **complete**. Subsequent work:

- **Active-hub program** (separate team) owns:
  - Tenant-to-tenant handoff (the formerly-Phase-8 work)
  - Active-hub DSL: identity + location + board-icon-type + comments
  - Dynamic membership subject the planner will eventually subscribe to
- **Robot integration program** (separate team) — already scoped per
  `project_planner_team_scope.md`.

Planner-side accumulated technical decisions (memory):
- `project_planner_active_node_contract.md` — the contract surface
- `project_planner_implementation_plan.md` — what shipped
- `project_phase7_multitenant_design.md` — multi-tenant lock
- `project_active_hub_handoff.md` — handoff-as-hub call
- `feedback_robot_owns_heading.md` — heading-ownership contract
- `feedback_robot_wire_dispatch.md` — drive_ack/done CBOR dispatch
- `feedback_luajit_stdio_buffering.md` — diagnostics in containers

## Operating-mode reminders

- **Testing reverts to user-driven** (default mode per
  `feedback_user_driven_testing.md`). Phase 7 + cleanup scope is closed.
- **No application auth** (`feedback_no_application_auth.md`).
- **One layer = one commit** (`feedback_holding_commits.md`).
- **No-soft-faults** for control-system fault paths.
- **`io.stderr:write+flush` not `print()`** for diagnostics in
  containerized LuaJIT processes.
- **`deployment/<cpu>/stop.sh` not `pkill -f`** for dcs.lua processes.
- **`build_kb.sh` wipes file_store** — re-upload boards before any
  mission submit after a rebuild.

## Rollback recipes

```bash
# Undo EOD #5 (back to EOD #4 wrap)
git reset --hard 8a473842

# Undo all of EOD #4 + #5 (back to EOD #3 wrap)
git reset --hard 41759edf

# Inspect the parking-lot session
git log --oneline 41759edf..HEAD
```

---

*continue.md rewritten 2026-05-10 EOD #5 — mission planner feature-complete pending tomorrow's manual sign-off smoke. 56 commits ahead at session start; pushed to origin/master at session end.*
