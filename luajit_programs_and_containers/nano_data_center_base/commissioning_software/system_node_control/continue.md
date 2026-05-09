# Nanodatacenter DCS — Continuation Plan

## State at end of 2026-05-10 EOD

**33 commits ahead of origin/master**, **743 host-side tests green**.

Eight commits this session:

| # | Commit | Phase | What |
|---|---|---|---|
| 1 | `15769083` | 5b C5 | mission launcher (FFI direct enqueue) |
| 2 | `1e6fe410` | 5b C6 | mission status overlay (FFI direct + 2s polling) |
| 3 | `0049c6bf` | Worker hookup | main.lua drives action_server:serve + on_tick heartbeat |
| 4 | `64dff82f` | Cluster fix | nginx resolver directive (surfaced by 5-min smoke) |
| 5 | `c0189fe4` | 7 C1 | migration script (drop+rebuild prep) |
| 6 | `0425194e` | 7 C2 | kb_build per-tenant paths + validator REJECT catalog |
| 7 | `2d2c197f` | 7 C3 | action_server per-tenant NATS bucket + key/subject |
| 8 | `38897e8b` | 7 C4 | planner_ui per-tenant scoping (submit + status + db) |
| 9 | (this session) | ROBSIM C1 | robots.lua kb_build subsystem + ctx.ROBOTS enumeration; closes the robot-model gap from a constructive direction (data layer first; container + cluster smoke land in C2/C3) |

Plus the locked 8-question Phase 7 design (`project_phase7_multitenant_design.md`).

## Honest assessment for tomorrow (push-back, not rubber-stamp)

Three things I want flagged before tomorrow's plan locks:

### 1. The running cluster is 4 commits stale.

`mission_planner_01` was rebuilt + restarted today for the 5b C5/C6/hookup smoke
(commit `64dff82f` resolver fix included). Phase 7 C1-C4 (commits `c0189fe4`,
`0425194e`, `2d2c197f`, `38897e8b`) **are not running on the cluster**. We
have great host-side test coverage, but per the 5b smoke pattern (host green
→ cluster surfaced resolver gap), validation in the cluster matters
disproportionately.

**Recommendation**: Step A tomorrow = rebuild + restart + 5-min smoke against
C1-C4, BEFORE landing C5. If anything surfaces, fix in place; only then
proceed to C5. Compounding more code on top of unvalidated code is the kind
of band-aid pattern `feedback_no_band_aid_over_architecture.md` warns
against.

### 2. C5 (gateway routing) in isolation is theatrical.

The design memo's C5 says "gateway per-tenant routing" — but the cluster
has ONE planner instance (`mission_planner_01`). Routing by tenant when
there's one tenant is config that does nothing observable. The Phase 7
test scenarios in the plan memo all assume planner_A vs planner_B —
two tenants.

**Recommendation**: bundle C5 + "add `mission_planner_02` to topology with
`planner_namespace = 'tunnel_ops'`" into one commit. Then the gateway
routing has something real to route to, and the C6 cluster smoke can
exercise the six scenarios from the plan memo (cross-tenant rejection,
both planners using the same shared dock, isolation, etc).

### 3. The robot model gap is bigger than my C3 TODO suggests.

Phase 7 schema places robots at `system.<sys>.site.<S>.planner.<ns>.robots.<id>`.
Today: zero robot rows in the KB, no `robots.lua` subsystem in
`construction/subsystems/`, no robot publisher, no robot UI consumer. The
launcher accepts any `robot_id` string; the worker fails on
`board_not_found` before robot validation fires (5b smoke proved this).

So Phase 7 ships the *capacity* for tenant-scoped robot ownership but
doesn't *test* it. That's fine as a v1, but it means the migration script
wipes nothing on the robot side (because nothing exists), the validator's
REJECT catalog has no real cross-tenant robot ref to reject, and C6 smoke
will only cover "two planners with empty robot fleets."

**Recommendation**: defer a `robots.lua` subsystem + a minimal robot
publisher to a sibling phase (call it Phase 7.5 or "robot model"). It's
real work, not just an adjustment to existing code. Don't sneak it into
C5 just to have something to test.

## Tomorrow's recommended sequence

| Step | What | Outcome |
|---|---|---|
| A | Rebuild `nanodatacenter/mission-planner:latest` + restart `mission_planner_01` (per recipe below). Run a 5-min smoke validating C1-C4 doesn't break the existing single-tenant flow. Migration script: don't apply yet — current cluster has only smoke artifacts in the deprecated bucket; new container will create the new per-tenant bucket on first publish; old bucket can age out manually. | Validates C1-C4 in cluster. Catches any cluster-only gaps. |
| B | Bundled C5: add `mission_planner_02` to topology (`planner_namespace = "tunnel_ops"`, ports `ui = 19009`), gateway adds `/planner/<ns>/*` routing to both planner_uis. Includes its tests. | Two-tenant cluster ready. |
| C | C6 cluster smoke: launch missions in both planners; verify isolation (planner_A's mission only visible in planner_A's UI); verify cross-tenant kb_ref rejection at compile time using a fixture board. | Phase 7 e2e validation. |
| D | After C5+C6 green: rewrite continue.md again, decide between (1) Phase 7+ robot model, (2) matplotlib viewer, (3) node-properties authoring, or (4) pause. | Honest re-prioritization. |

If you want to deviate from this sequence:
- **Skip A** (rebuild+smoke) → faster but riskier; if Phase 7 introduces a cluster bug, it surfaces later mixed with whatever C5 adds. Not recommended.
- **Skip B's bundling** (do C5 alone) → C5 ships as config nobody exercises; C6 needs a second planner anyway and has to add it then. Pointless detour.
- **Tackle robot model first** → real work; 1+ session. Defers Phase 7 close-out. Worth doing if "tenant ownership of robots" is the actual operator value, not "tenant ownership of boards."

## Architectural references

1. `~/.claude/projects/-home-gedgar-knowledge-base-assembly/memory/project_planner_active_node_contract.md` — the architecture
2. `~/.claude/projects/-home-gedgar-knowledge-base-assembly/memory/project_planner_implementation_plan.md` — phased plan (Phase 7 C1-C4 done; C5+C6 next)
3. `~/.claude/projects/-home-gedgar-knowledge-base-assembly/memory/project_planner_team_scope.md` — scope boundary
4. `~/.claude/projects/-home-gedgar-knowledge-base-assembly/memory/project_phase7_multitenant_design.md` — full 8-Q Phase 7 spec + C1-C4 status

## Aggregate test counts (host-side)

| Phase | Tests passing |
|---|---|
| Phase 1 catalog | 20/20 |
| Phase 2 (drive packet round-trip) | 39/39 |
| Phase 4 (board DSL C1-C4) | 125/125 |
| Phase 5 C1 route_builder | 29/29 |
| Phase 5 C2+C3a hub_runtime ACK | 54/54 |
| Phase 5 C3b drive_v2 dispatch | 34/34 |
| Phase 5 C4 namespace + C5 default flip | 20/20 |
| Phase 3a C1 simulator round-trip | 34/34 |
| Phase 5 C5 prep mission+simulator integration | 28/28 |
| Phase 5b C1 chassis | 19/19 |
| Phase 5b C2 db (now per-tenant) | 34/34 |
| Phase 5b C3+C4+C5+C6 renderer | 137/137 |
| Phase 5b C5 submit_mission (now per-tenant) | 51/51 |
| Phase 5b C6 status (now per-tenant) | 45/45 |
| Worker hookup | 20/20 |
| Phase 7 C1 migration script | 38/38 |
| Phase 7 C2 board_dsl validator + boards.lua | 28/28 |
| Phase 7 C3 action_server | 27/27 |
| **Total** | **743/743 host-side** |

## Quick start tomorrow

```bash
cd ~/knowledge_base_assembly/luajit_programs_and_containers/nano_data_center_base/commissioning_software/system_node_control

git log --oneline -10
git status

# Re-run host-side smoke (~30s total, all should be green)
luajit construction/tests/test_planner_phase1_catalog.lua            # 20/20
luajit construction/tests/board_dsl/test_board_dsl_c1.lua            # 36/36
luajit construction/tests/board_dsl/test_board_dsl_c2.lua            # 42/42
luajit construction/tests/board_dsl/test_board_dsl_c3.lua            # 16/16
luajit construction/tests/board_dsl/test_board_dsl_c4.lua            # 31/31
luajit construction/tests/board_dsl/test_board_dsl_c5_phase7.lua     # 28/28
luajit construction/tests/test_route_builder_drive_packets.lua       # 29/29
luajit construction/tests/test_drive_v2_dispatch.lua                 # 34/34
luajit construction/tests/test_planner_namespace_threading.lua       # 20/20
luajit construction/tests/test_planner_ui_chassis.lua                # 19/19
luajit construction/tests/test_planner_ui_db.lua                     # 34/34
luajit construction/tests/test_planner_ui_renderer.lua               # 137/137
luajit construction/tests/test_planner_ui_submit_mission.lua         # 51/51
luajit construction/tests/test_planner_ui_status.lua                 # 45/45
luajit construction/tests/test_migrate_phase7_script.lua             # 38/38
luajit construction/tests/test_action_server_phase7.lua              # 27/27

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

## Step A — cluster rebuild + smoke recipe

```bash
# 1. Rebuild image with all 8 of today's commits
cd ~/knowledge_base_assembly/luajit_programs_and_containers/nano_data_center_instance/app_containers/mission_planner/container
bash ./docker_build.sh

# 2. Restart mission_planner_01. Note: orchestrator (system_control / node_control)
#    is NOT running today; recreating manually with full env shape (matches
#    what node_control would inject, captured from earlier today's smoke).
docker rm -f mission_planner_01
docker run -d --name mission_planner_01 \
  --network planner-net --restart unless-stopped \
  -p 19005:8090 \
  -e APP_SYSTEM=moon_base \
  -e APP_SITE=moon_base_alpha \
  -e APP_CPU_ID=cpu_02 \
  -e CONTAINER_NAME=mission_planner_01 \
  -e PG_HOST=host.docker.internal -e PG_PORT=5432 \
  -e PG_DB=knowledge_base -e PG_USER=gedgar -e PG_PASSWORD=ready2go \
  -e NATS_URL=nats://nats-js-ram:4222 \
  -l nanodatacenter=true \
  nanodatacenter/mission-planner:latest

# 3. Watch boot logs for the new "entering action_server:serve" line
sleep 5
docker logs --tail 30 mission_planner_01

# 4. Hit endpoints. /api/boards SHOULD return empty (zero boards in pg);
#    /api/missions SHOULD return empty envelope (no per-tenant bucket
#    exists yet); /api/active_nodes SHOULD work (infrastructure shared).
curl -s http://localhost:19005/health
curl -s http://localhost:19005/api/boards         # expect: {count:0, boards:{}}
curl -s http://localhost:19005/api/missions       # expect: empty envelope
curl -s http://localhost:19005/api/active_nodes

# 5. Submit a test mission. SHOULD create the per-tenant bucket
#    moon_base_alpha_planner_mission_planner_01_action_server
#    (with mission_planner_01 default namespace).
curl -s -X POST http://localhost:19005/api/submit_mission \
  -H "Content-Type: application/json" \
  -d '{"robot_id":"rover_1","board":"landing_zone",
       "source":"a","target":"b"}'
# expect: 200 with job_id; queue field shows
#   "moon_base_alpha.planner.mission_planner_01.action_server.missions"

# 6. Verify the new per-tenant bucket exists in NATS
docker run --rm --network planner-net natsio/nats-box:latest \
  nats --server nats://nats-js-ram:4222 kv ls
# Expect to see: moon_base_alpha_planner_mission_planner_01_action_server
# Old buckets (moon_base_alpha_action_server, _mission_log) may still exist
# from yesterday's smoke -- they're orphaned, can be left or manually deleted.

# 7. Re-poll missions; should now show rover_1 with state=failed +
#    board_not_found (no boards uploaded; expected).
curl -s http://localhost:19005/api/missions
curl -s http://localhost:19005/api/mission/rover_1

# 8. Heartbeat continuity check: confirm runtime.heartbeat row updates
PGPASSWORD=ready2go psql -h localhost -U gedgar -d knowledge_base -c \
  "SELECT data FROM knowledge_base_status WHERE path::text LIKE '%mission_planner_01%heartbeat%snapshot%';"
```

If anything in steps 4-8 surprises, paste logs back and I'll analyze.

## Step B — design notes for the bundled C5

```
construction/catalogs/topology.lua  (modify)
  cpu_02.instances += {
    name = "mission_planner_02",
    def  = "mission_planner",
    planner_namespace = "tunnel_ops",   -- NEW topology field, optional;
                                        -- defaults to instance name
    ports = { ui = 19009 },             -- distinct from mp_01's 19005
  }
  Note: build_kb.lua already enumerates planners from topology
  (ctx.PLANNERS), so this single topology change automatically extends
  boards.lua's iteration to register a doc_class for tunnel_ops too.

dcs_console/admin's gateway / planner_ui routing  (modify wherever the
gateway hosts planner_ui today; survey first)
  - Per-tenant route: /planner/<ns>/* -> planner_ui_<ns> upstream
  - Mapping table: namespace -> upstream container/port
  - No auth check (per Q6)

Tests:
  - test_topology_planners.lua: assert ctx.PLANNERS has 2 entries with
    expected names + namespaces
  - test_gateway_phase7.lua: structural check on the new route block

Cluster smoke (C6) immediately follows:
  - Bring up mission_planner_02 (manual docker run mirroring mp_01)
  - Hit /planner/mission_planner_01/api/missions and
    /planner/tunnel_ops/api/missions
  - Submit mission to mp_01 -> verify ONLY appears in mp_01's UI
  - Submit mission to mp_02 with kb_ref into mp_01's namespace ->
    expect compile-time REJECT case 1
```

## Operating-mode reminders

- **Commit style**: title like "B.2 Planner Phase X: ...", body with bulleted file-level changes, test counts, design rationale. Trailer: `Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>`.
- **One layer = one commit** per `feedback_holding_commits.md`. C5 = gateway routing + topology change is ONE layer (they belong together).
- **No-soft-faults** rule: every fault halts until explicit reset. UI submit/status errors return 4xx/5xx and let the operator retry — this is the right behavior.
- **No application auth** per `feedback_no_application_auth.md`. Gateway routing is just a route map, not an auth boundary.
- **planner_ui module env-check pattern**: APP_SITE / PLANNER_NAMESPACE checked BEFORE `require("cjson.safe")` so host shells without cjson don't crash on early-exit paths.
- **Cluster smoke is user-driven** per `feedback_user_driven_testing.md` — but you authorized me to run it once today for the 5b layer. Default back to user-driven for tomorrow unless you say otherwise.
- **Test isolation**: don't run anything that drops/recreates pg tables against the live cluster (`feedback_test_db_isolation.md`).

## Rollback recipes

```bash
# Undo Phase 7 C4 only
git reset --hard HEAD~1

# Undo all 4 Phase 7 commits (back to "cluster smoke fix" baseline)
git reset --hard 64dff82f

# Undo all 8 of this session's commits
git reset --hard b2cc4051   # back to 5b C4

# Inspect commits
git show HEAD --stat        # this session: Phase 7 C4
git show 38897e8b --stat    # 7 C4
git show 2d2c197f --stat    # 7 C3
git show 0425194e --stat    # 7 C2
git show c0189fe4 --stat    # 7 C1
git show 64dff82f --stat    # nginx resolver fix
git show 0049c6bf --stat    # worker hookup
git show 1e6fe410 --stat    # 5b C6
git show 15769083 --stat    # 5b C5
```

## Known issues / parking lot

- **Cluster running stale code** (pre-Phase-7 + with the resolver fix from C5/C6/hookup smoke). Step A tomorrow is the rebuild.
- **No robot publisher / robot model.** Schema places robots at `planner.<ns>.robots.<id>` but no producer writes them. Phase 7 doesn't actually exercise tenant-scoped robot ownership. Sibling phase ("robot model") should land robots.lua subsystem + minimal publisher.
- **`_read_robot_energy` in action_server.lua still uses site-level bucket** (TODO marker). No-op today (no robot publisher); blocks future robot-energy dashboard.
- **No second planner instance in topology yet.** C5 should change that to give Phase 7 real exercise.
- **Phase 4 C5 visualizer Python smoke** needs matplotlib in WSL venv. Parking-lot.
- **Node-properties authoring** flagged in `project_v2_board_dsl_design.md` — DSL author convenience; not in any current plan.
- **Pre-existing**: `mission_builder.rebuild`'s 4th arg `current_heading` silently dropped; out of scope.
- **5 C5 follow-up**: delete legacy nav code paths. UNGATED (cluster smoke green for the 5b stack 2026-05-09 evening). Can land any time; ~2-3 small commits.
- **planner_ui's lazy NATS singletons** don't reconnect on NATS restart. Add reconnect handling if observed in cluster smoke.
- **Old NATS-KV buckets from yesterday** (`moon_base_alpha_action_server`, `_mission_log`) are orphaned. After cluster rebuild they're not used; can be manually deleted via `nats kv del` or left to age out.

---

*continue.md rewritten 2026-05-10 EOD. Eight commits this session + Phase 7
design locked + Phase 7 C1-C4 shipped. Earlier 2026-05-09/10 morning content
available via `git log -p -- continue.md` if needed.*
