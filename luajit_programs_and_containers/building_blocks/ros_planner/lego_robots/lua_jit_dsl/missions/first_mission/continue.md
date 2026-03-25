# Continue: Lego SPIKE Prime DSL Helpers

## What Was Done

Built two LuaJIT DSL helper files that map the full Pybricks MicroPython API (motors, sensors, drivebase, IMU) into leaf and composite nodes for both the ChainTree and S-Expression engines. Added safety guard infrastructure including bump detection via IMU accelerometer.

### Files Created

| File | Purpose |
|------|---------|
| `se_lego_spike.lua` | S-Expression engine DSL helper (all SPIKE commands) |
| `ct_lego_spike.lua` | ChainTree DSL helper (all SPIKE commands) |
| `README.md` | Teaching guide for user's son/grandsons |
| `README_micro_python_commands.md` | Pybricks API reference (pre-existing) |

### Copies also placed at (NOT wired in):
- `c_programs_and_containers/build_blocks/chain_tree_c/s_expression/lua_dsl/se_helpers_dir/se_lego_spike.lua`
- `c_programs_and_containers/build_blocks/chain_tree_c/lua_dsl/lua_support/lego_spike.lua`

## What the Helpers Contain

### Blackboard (60+ fields)
- 6 motors (A-F): angle, speed, load, stalled, done
- Color sensor: hue, sat, val, id, reflection
- Ultrasonic: distance
- Force: value, pressed, touched
- IMU: heading, pitch, roll, ready, accel_x/y/z
- DriveBase: distance, angle, speed, done, stalled
- Safety: battery_voltage, comm_last_tick, bump_detected, sensor_a-f_connected

### Leaf Functions
- 11 sensor predicates (motor stalled/done, force, color, distance, IMU, drivebase)
- 7 fault predicates (battery low, overcurrent, runaway, tilt, bump, comm timeout, sensor disconnect)
- 18 oneshots (motor run/stop/brake/hold/dc/reset, sensor lights, drivebase drive/stop/brake/reset, IMU reset, read sensors, motor PID/limits)
- 4 recovery oneshots (emergency stop, safe shutdown, log fault, clear bump)
- 11 blocking main functions (motor run angle/target/time/stall, drivebase straight/turn/curve, wait color/distance/force/heading)
- 1 bump detector main (SPIKE_BUMP_DETECT)

### Composites
- S-Expression: spike_guarded_action, spike_multi_guard, 5 pre-built guards (battery/stall/tilt/bump/comm), spike_full_safety_guard, spike_safe_straight/turn/motor_angle, plus convenience helpers (do_motor_angle, do_straight, do_turn, drive_until_close, find_endpoint, sensor_poll_loop)
- ChainTree: define_spike_battery/stall/tilt/bump/comm_guard (exception_catch pattern), asm_spike_find_endpoint, asm_spike_drive_square

## What Needs to Be Done Next

### 1. C Leaf Function Implementations
None of the DSL functions have C implementations yet. Each `register_user()` name in the DSL needs a matching C function. The function signatures are:
- **Predicates**: `bool fn(s_expr_tree_instance_t*, const s_expr_param_t*, uint16_t, s_expr_event_type_t, uint16_t, void*)`
- **Oneshots**: `void fn(...)` (same params)
- **Main**: `s_expr_result_t fn(...)` (same params)
- **ChainTree boolean**: `bool fn(void* handle, unsigned node_index, unsigned event_type, unsigned event_id, void* event_data)`
- **ChainTree oneshot**: `void fn(void* handle, uint16_t node_index)`
- **ChainTree main**: `unsigned fn(void* handle, unsigned bool_fn_idx, unsigned node_idx, unsigned event_type, unsigned event_id, void* event_data)`

Priority C implementations:
1. `SPIKE_READ_SENSORS` — polls all hardware, updates blackboard
2. `SPIKE_EMERGENCY_STOP` / `SPIKE_SAFE_SHUTDOWN` — safety critical
3. `SPIKE_BUMP_DETECT` — accelerometer magnitude tracking with persistent state
4. Motor commands (run/stop/brake/hold/run_angle/run_target/run_time/run_until_stalled)
5. DriveBase commands (straight/turn/curve/drive/stop)
6. All predicates (mostly just blackboard field reads with threshold comparisons)

### 2. Communication Bridge
The C leaf functions need a way to talk to the actual Pybricks MicroPython runtime on the SPIKE hub. Options not yet decided:
- Serial UART (most likely for SPIKE Prime)
- Shared memory
- USB CDC
- Bluetooth

This bridge is the critical missing piece. The C runtime runs on one side, sends commands and receives sensor data from the Pybricks side.

### 3. Wiring Into Engines (if desired)
The helpers are NOT wired into the chain_tree_c source tree. To wire them in:
- S-Expression: add `dofile("se_helpers_dir/se_lego_spike.lua")` to `s_engine_helpers.lua`
- ChainTree: add `local LegoSpike = require("lua_support.lego_spike")` and `mixin(ChainTreeMaster, LegoSpike)` to `chain_tree_master.lua`

### 4. Composite C Builtins (future)
These were identified as candidates for new C builtins (need per-tick internal state):
- `SPIKE_LINE_FOLLOW` — PID line follower using color sensor reflection
- `SPIKE_WALL_FOLLOW` — PID wall follower using ultrasonic distance
- `SPIKE_FORCE_GRASP` — force-controlled grasp with hold maintenance
- `SPIKE_CALIBRATE_MOTOR` — two-direction stall calibration
- `SPIKE_SYNC_MOTORS` — synchronized dual-motor motion
- `SPIKE_MOTOR_RAMP` — smooth acceleration ramp
- `SPIKE_OBSTACLE_AVOID` — reactive drive + avoid pattern

### 5. Test DSL Scripts
No test .lua scripts exist yet. Would be good to write example programs:
- Simple drive square
- Obstacle avoidance with ultrasonic
- Line following
- Pick and place with gripper
- Full safety-guarded mission

## Key Design Decisions Made
- No new C composite builtins needed for safety guards — existing `se_verify` and `define_exception_catch` suffice
- Bump detection requires a new C main function (`SPIKE_BUMP_DETECT`) for per-tick accelerometer magnitude tracking with persistent state
- All hardware communication goes through blackboard fields — decouples DSL from transport layer
- Fault predicates return TRUE on fault; guard composites invert them for `se_verify` (which fires on FALSE)
- ChainTree guards use 3-column exception_catch: main (normal) -> recovery (stop/shutdown) -> finalize (log/cleanup)
