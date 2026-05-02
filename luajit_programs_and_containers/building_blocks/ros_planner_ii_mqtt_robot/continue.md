# continue.md — 2026-05-02 end-of-session handoff (3-layer + L6 green)

**Open here next session.** Three pieces of state to be aware of:

1. **L5 paths_only milestone** — Linux mission planner drives the
   four-thread dongle architecture end-to-end (closed 2026-05-01).

2. **3-layer container stack BUILT + e2e green 2026-05-02.** Three
   images live under `containers/`:
   - `mycorp/robot_base:1.0` — supervisor + ENV gate + render-config
     + spawn helpers + Phase-2 upward_peer stub. (~165MB)
   - `mycorp/dongle_base:1.0` — USB protocol + internal bus + virtual-
     robot framework. Static archives (libcomm.a, libphysics.a) +
     headers + master-side Lua + libmqtt_pubsub + test harness.
     (~167MB cumulative; ~2MB delta over robot_base.)
   - `mycorp/lunar_rover-class:1.0` — class-specific only. Final-links
     robot_sim from class C against dongle_base's static libs. Adds
     tunables.bin + manifest + remote.json + configs. (**~400KB
     unique** on top of dongle_base.)
   See `project_robot_container_design.md` memory for full details.

3. **paths_only e2e validated against the refactored stack.**
   `Summary: cmds=6 ack=6 hb=168 done=6 (6 ok / 0 fail)`. Boot
   fail-stop on missing ROBOT_ID and on ROBOT_CLASS mismatch both
   fire. SIGTERM teardown clean.

5. **L6 drive_base tool catalogue shipped + e2e green.** 7 new wire
   opcodes (BEGIN_GRIP/RELEASE/DOCK/CHARGE/TOOL_MOVE + GET_TOOL_STATUS
   + GET_STATION). Slave-side dispatch in `libcomm/drive_base_robot.c`
   calls `phys_begin_*`. Master-side `dongle_hal.lua` replaces the tool
   stubs with real `comm_submit` + poll-for-response. Mission workers
   needed **zero changes** — `WKR_DELIVER_PART / PAINT_SAMPLE /
   LOAD_SHIPPING / RECHARGE` already called the right hal:* methods,
   they just stopped erroring out on the dongle path. Mixed-mode e2e:
   `cmds=10 ack=10 done=10 (10 ok / 0 fail)`.

4. **Static-link policy.** Everything we build is statically linked
   into the rover binary (libcomm.a + libphysics.a → robot_sim). Lua
   FFI consumers still load .so via ldconfig (libcomm.so, libphysics.so,
   libmqtt_pubsub.so all in /usr/local/lib).

## Verify on resume

Native source-tree harness (no containers):
```bash
cd /home/gedgar/knowledge_base_assembly/luajit_programs_and_containers/building_blocks/ros_planner_ii_mqtt_robot
make
bash run_tests.sh --skip-e2e
# → 306 unit checks across 14 groups, all green
```

Containerized rover smoke:
```bash
# Pre-req: nanodatacenter/luajit-base:latest must exist locally.
# Build base image first (only needed if you nuked it):
#   cd ../../nano_data_center_base/luajit/luajit_base/container && ./docker_build.sh

# Build rover images (in order):
bash containers/robot_base/docker_build.sh
bash containers/dongle_base/docker_build.sh
bash containers/lunar_rover-class/docker_build.sh

# Bring up mock_planner on the host MQTT broker:
LD_LIBRARY_PATH="../knowledge_base/mqtt:$LD_LIBRARY_PATH" \
LUA_PATH="../chain_tree_luajit/lua_dsl/luajit_pipeline/?.lua;\
../chain_tree_luajit/runtime_dict/?.lua;\
../knowledge_base/mqtt/?.lua;../knowledge_base/mqtt/lib/?.lua;\
../ros_planner_ii/runtime/?.lua;\
../ros_planner_ii/hub_dsl/protocol/?.lua;./?.lua;;" \
luajit test_mock_planner.lua --host localhost --port 1883 --duration 600 &

# Run rover container talking to host broker:
docker run -d --add-host=host.docker.internal:host-gateway \
    -e ROBOT_ID=rover_1 -e ROBOT_CLASS=lunar_rover \
    -e DONGLE_INSTANCE=1 -e MQTT_HOST=host.docker.internal \
    -e SPEED_FACTOR=10.0 --name rover_smoke \
    mycorp/lunar_rover-class:1.0

# Drive paths_only against it:
LD_LIBRARY_PATH=... LUA_PATH=... \
luajit test_random_paths.lua --robot rover_1 --host localhost --port 1883 \
    --seed 11 --count 5 --mode paths_only --workspace 2.0 --wait 60
# Expect: ack=6  done=5 (5 ok / 0 fail)

# Cleanup:
docker rm -f rover_smoke; pkill -f test_mock_planner.lua
```

## Locked roadmap (greenlit 2026-05-02)

### 1. L6 drive_base tool catalogue ✅ DONE 2026-05-02 evening

7 wire opcodes added (BEGIN_GRIP/RELEASE/DOCK/CHARGE/TOOL_MOVE +
GET_TOOL_STATUS + GET_STATION). Mixed e2e 10/10 green. Mission-side
needed zero changes.

### 2. Test harness consolidation (NEXT)

Extract `planner_test_peer.lua` from test_mock_planner.lua +
test_random_paths.lua boilerplate. Generic mission-side handshake +
heartbeat + RPC peer with `:send/:wait_for/:expect_telemetry/:expect_done`
methods. Each new mixed_* test becomes ~30 lines on top. Also remove
the requirement that test_mock_planner be a separate process — the
peer can serve both roles for self-contained tests.

Also write `robot_controller_test_peer.lua` + `robot_controller_contract.md`
**before** Phase 2 implementation. Harness defines the contract; service
implementation is graded against the green harness.

### 3. mkdocs site

Consolidate existing docs/ + document the mission. This rover is the
template for actual hardware, so docs must support the retargeting
story (porting/, components/, containers/, testing/). Theme: match
existing repo mkdocs config. ~2 sessions.

### 4. Phase 2 robot_controller

Real upward_peer.lua client. Service spec defined by harness from #2.
Rebuild robot_base:1.1 with the real client. Multi-session.

### 5. Track B Pico hardware port

Architecture proven. Hardware: Pico (~$5) + Debug Probe (~$12).
Multi-session.

## Recent commit stack (origin/master)

```
be54aa4e  continue.md: robot container design locked, code next session
16729d4a  continue.md: correct rollback line — use git revert, preserve L5/L6 robot work
79003844  continue.md: end of 2026-05-01 evening — M-1 done, soak underway, M-2 recipe drafted
300c5abe  plan: insert Layer O (observability tree-by-namespace) between M and F
907cd431  continue.md: end-of-session 2026-05-01 — L5 closed, L6 next
```

(This session's container work is in working tree, not yet committed.)

## Memory pointers

- `project_robot_container_design.md` — built + green status, image
  hierarchy, runtime ENV contract, supervisor SM, asm_wait gotcha.
- `project_dongle_l1_l5_progress.md` — Linux waypoint achieved.
- `project_dongle_track_c.md` — Track C contract locks.
- `project_dongle_architecture.md` — locked dongle architecture.
- `project_luajit_base_design.md` — base image conventions.
- `feedback_container_build.md` — staging-copy build pattern.
- `feedback_no_soft_faults.md` — fail-stop child supervision.
- `feedback_chaintree_runtime_fixes.md` — chain-tree gotchas.
- `feedback_chaintree_verify_event_filter.md` — verify event filter.

## Working tree state (post-L6, post-3-layer)

3-layer container stack landed in `cb1f5bc9` and pushed to origin/master.

L6 modifications (uncommitted at end of session):
- `libcomm/drive_base_robot.h` — 7 new opcode #defines + tool_status decoder
- `libcomm/drive_base_robot.c` — 5 BEGIN_*, GET_TOOL_STATUS,
  GET_STATION cases in `on_msg_fn` + `drive_base_decode_tool_status`
- `drive_base_ffi.lua` — new CMD/TOOL_F/TOOL_KIND/STATION_KIND
  constants + 4 builders + 2 decoders
- `dongle_hal.lua` — real implementations of `begin_grip / begin_release
  / begin_dock / begin_charge / begin_tool_move / read_tool_status /
  station_at_pose`, slot name resolution from physics_config.tools
- `robot_hal.lua` — pass `dir / physics_config / sim_map` opts through
  to dongle_hal so the master can resolve slot names

Nothing else changed in the rover dir. Suggested commit message:
`L6: drive_base tool catalogue (7 wire opcodes), mixed e2e 10/10 green`
