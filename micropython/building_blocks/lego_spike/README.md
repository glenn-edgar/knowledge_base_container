# Lego SPIKE Prime Robot Programming with Behavior Trees

The full ChainTree and S-Expression engine documentation (with all design guides, test walkthroughs, and API references) is published at: **https://glenn-edgar.github.io/knowledge_base_container/**

## Why Behavior Trees Beat State Machines

When you program a robot with a state machine, every state needs to know about every other state it might transition to. A simple robot that drives, avoids obstacles, and picks up objects might have three states, but each state needs arrows pointing to the others. Add a fourth behavior and you suddenly have to update all the existing states with new transitions. The complexity grows explosively. If something goes wrong partway through an action, state machines make it hard to cleanly stop what you are doing and recover. Behavior trees solve this by organizing robot behavior as a tree. The top of the tree decides what the robot should be doing right now, and the branches below carry out the details. Each branch only cares about its own job. If you want to add obstacle avoidance, you add a new branch; the existing branches do not change. If something goes wrong, the tree naturally stops the current branch and can switch to a recovery branch. This structure maps directly to how you think about robot behavior: "drive forward, but if you see something close, stop and turn." The tree handles the "but if" part automatically.

## ChainTree: The Full Control Flow Engine

ChainTree is a C framework that takes behavior trees further. It combines behavior trees, state machines, and sequential control flows into one unified engine. It runs on everything from tiny 32KB microcontrollers to full servers. Instead of writing the tree structure by hand in C, you write it in a LuaJIT DSL (Domain Specific Language) that reads almost like plain English. The DSL then generates all the C code or binary images automatically. You describe what the robot should do in short, readable Lua scripts, and the build pipeline turns it into efficient runtime code.

The ChainTree DSL organizes behavior into **columns** (sequential flows of actions), **state machines**, **exception handlers** (for fault recovery), and **gate nodes** (conditional activation). Writing a robot program looks like this:

```lua
local ct = ChainTreeMaster.new("my_robot.json")
ct:start_test("my_robot")
ct:define_spike_blackboard()

local drive_col = ct:define_column("drive", nil, nil, nil, nil, nil, true)
    ct:asm_spike_drivebase_straight(500, SPIKE_STOP_HOLD)
    ct:asm_spike_drivebase_turn(90, SPIKE_STOP_HOLD)
    ct:asm_spike_drivebase_straight(500, SPIKE_STOP_HOLD)
    ct:asm_terminate()
ct:end_column(drive_col)

ct:end_test()
```

For deeper reading on the ChainTree framework and its DSL:

- [ChainTree Design Overview](../../c_programs_and_containers/build_blocks/chain_tree_c/docs/README_chaintree_design.md)
- [ChainTree DSL Guide](../../c_programs_and_containers/build_blocks/chain_tree_c/docs/README_chaintree_dsl.md)
- [Build Pipeline](../../c_programs_and_containers/build_blocks/chain_tree_c/docs/README_build_pipeline.md)
- [Exception Handlers](../../c_programs_and_containers/build_blocks/chain_tree_c/docs/README_exception_handler.md)
- [Blackboard (Shared Data)](../../c_programs_and_containers/build_blocks/chain_tree_c/docs/README_blackboard.md)
- [Controlled Nodes](../../c_programs_and_containers/build_blocks/chain_tree_c/docs/README_controlled_nodes.md)
- [Runtime API](../../c_programs_and_containers/build_blocks/chain_tree_c/docs/README_runtime_api.md)
- [Cross-Compilation (ARM targets)](../../c_programs_and_containers/build_blocks/chain_tree_c/docs/README_cross_compilation.md)

## S-Expression Engine: The Lightweight Interpreter

The S-Expression engine is a second, lighter-weight execution engine that sits alongside ChainTree. Traditional s-expressions (like Lisp) are powerful but drown you in parentheses. Nobody wants to read `(sequence (if (and (not (stalled)) (ready)) (drive 500) (stop)))` and count matching parens. The S-Expression engine solves this with a LuaJIT DSL that uses normal function calls and closures. You write readable Lua and the compiler flattens it into compact parameter arrays stored in ROM. The result runs on the same tiny microcontrollers as ChainTree, using as little as 4 bytes of RAM per node.

The S-Expression DSL looks like this:

```lua
start_tree("obstacle_avoider")
    se_function_interface(function()
        se_fork(
            function() spike_sensor_poll_loop() end,
            function() spike_bump_detect(18000) end,
            function()
                se_sequence(
                    function() spike_drivebase_straight(500, SPIKE_STOP_HOLD) end,
                    function() spike_drivebase_turn(90, SPIKE_STOP_HOLD) end,
                    function() se_return_reset() end
                )
            end
        )
    end)
end_tree()
```

No parenthesis counting. No nesting confusion. The DSL handles all the structural encoding.

For deeper reading on the S-Expression engine:

- [S-Expression Top-Level Design](../../c_programs_and_containers/build_blocks/chain_tree_c/docs/s_expression/README_top_design.md)
- [S-Expression DSL Guide](../../c_programs_and_containers/build_blocks/chain_tree_c/docs/s_expression/README_DSL.md)
- [DSL Closure Guide](../../c_programs_and_containers/build_blocks/chain_tree_c/docs/s_expression/README_DSL_closure_guide.md)
- [Composite Functions Overview](../../c_programs_and_containers/build_blocks/chain_tree_c/docs/s_expression/composite_functions/README_composite_functions.md)
- [Predicate System](../../c_programs_and_containers/build_blocks/chain_tree_c/docs/s_expression/README_predicate_system.md)
- [Return Codes](../../c_programs_and_containers/build_blocks/chain_tree_c/docs/s_expression/README_return_codes.md)
- [User-Defined Functions](../../c_programs_and_containers/build_blocks/chain_tree_c/docs/s_expression/README_user_defined_functions.md)
- [Blackboard System](../../c_programs_and_containers/build_blocks/chain_tree_c/docs/s_expression/tests/black_board/README_MAIN.md)

## Leaf Nodes and Composite Nodes

Every behavior tree is built from two kinds of building blocks:

**Leaf nodes** are the actions and checks that actually touch the real world. A leaf node might spin a motor, read a sensor, or check if the battery is low. Leaf nodes do not contain other nodes. They are the "hands and eyes" of the robot.

**Composite nodes** control the flow. They contain other nodes (which can be leaves or other composites) and decide how and when those children execute. A **sequence** runs its children one after another, stopping if any child fails. A **fork** runs all its children at the same time in parallel. A **state machine** picks which child to run based on the current state. An **if-then-else** checks a condition and picks one of two branches.

Think of it like a recipe. The composite nodes are the instructions ("first do this, then do that, but if the oven is too hot, do this instead"). The leaf nodes are the actual cooking actions ("set oven to 350", "check temperature", "remove from oven").

For the Lego SPIKE, the leaf nodes map directly to the Pybricks MicroPython commands for motors, sensors, the drivebase, and the hub IMU. The composite nodes are the control flow patterns from ChainTree and the S-Expression engine. You snap them together like Lego bricks to build complex robot behaviors.

---

## Pybricks Command Reference

For the complete Pybricks MicroPython API that these DSL commands map to, see [README_micro_python_commands.md](README_micro_python_commands.md).

---

## DSL Command Reference

Both DSL helper files (`se_lego_spike.lua` for the S-Expression engine, `ct_lego_spike.lua` for ChainTree) provide the same set of robot operations. The sections below describe each command, what it does, and when you would use it.

### Constants

These constants are shared by both DSL files.

| Constant | Value | Meaning |
|----------|-------|---------|
| `SPIKE_PORT_A` through `SPIKE_PORT_F` | 0-5 | Which port a motor or sensor is plugged into |
| `SPIKE_STOP_COAST` | 0 | Let the motor spin freely after the command finishes |
| `SPIKE_STOP_BRAKE` | 1 | Passively resist motion (shorts motor leads) |
| `SPIKE_STOP_HOLD` | 2 | Actively hold position using PID control (the default for most commands) |
| `SPIKE_STOP_NONE` | 3 | Do not slow down at the end; keep going at speed |

### Blackboard (Shared Data)

Before using any commands, call the blackboard setup function once. This creates all the shared data fields that sensors write to and commands read from.

**S-Expression:** `spike_define_blackboard()`
**ChainTree:** `ct:define_spike_blackboard()`

The blackboard contains fields for every motor (angle, speed, load, stalled, done), every sensor (color, ultrasonic, force), the IMU (heading, pitch, roll, accelerometer), the drivebase (distance, angle, speed, done, stalled), and safety monitoring (battery voltage, communication status, bump detection, sensor connection flags).

---

### Motor Commands

#### spike_motor_run(port, speed)

Start a motor spinning at a constant speed. The motor keeps running until you give it another command. This is **non-blocking** -- execution continues immediately.

- **port**: Which port the motor is on (`SPIKE_PORT_A` through `SPIKE_PORT_F`)
- **speed**: Rotation speed in degrees per second. Positive = forward, negative = backward.

**When to use:** Continuous spinning, conveyor belts, fans, anything that should run until told to stop.

```lua
-- S-Expression: spin motor A forward at 300 deg/s
spike_motor_run(SPIKE_PORT_A, 300)

-- ChainTree:
ct:asm_spike_motor_run(SPIKE_PORT_A, 300)
```

#### spike_motor_stop(port)

Coast-stop a motor. The motor is free to spin; it will slow down by friction.

**When to use:** Gentle stops where you do not care about the exact stopping position.

#### spike_motor_brake(port)

Passive brake. The motor resists being turned but does not actively hold a position. Uses less power than hold.

**When to use:** Stopping a mechanism that should resist being pushed but does not need precision.

#### spike_motor_hold(port)

Active PID hold. The motor actively holds its current angle. If something pushes it, the motor pushes back.

**When to use:** Holding a gripper closed, keeping an arm in position against gravity.

#### spike_motor_dc(port, duty)

Raw duty cycle control. Sets the motor voltage directly as a percentage from -100 to 100. No PID, no speed control.

- **duty**: -100 (full reverse) to 100 (full forward)

**When to use:** Low-level control, testing, or situations where PID control gets in the way.

#### spike_motor_reset_angle(port, angle)

Reset the motor's encoder to a specific angle value. Defaults to 0.

**When to use:** After finding a mechanical endpoint (stall calibration), to establish a known reference point.

#### spike_motor_run_angle(port, speed, angle, stop_type)

Run the motor a specific number of degrees relative to its current position, then stop. This command **blocks** (returns HALT each tick) until the motor reaches its target.

- **speed**: How fast to turn (deg/s)
- **angle**: How far to turn (degrees). Positive = forward.
- **stop_type**: What to do when done (default: `SPIKE_STOP_HOLD`)

**When to use:** Moving an arm a specific amount, opening a gripper a set distance, rotating a turntable.

```lua
-- S-Expression: rotate motor B 360 degrees at 500 deg/s, then hold
spike_motor_run_angle(SPIKE_PORT_B, 500, 360, SPIKE_STOP_HOLD)

-- ChainTree:
ct:asm_spike_motor_run_angle(SPIKE_PORT_B, 500, 360, SPIKE_STOP_HOLD)
```

#### spike_motor_run_target(port, speed, target, stop_type)

Run the motor to an absolute angle position. Unlike `run_angle` (which is relative), this goes to a specific number on the encoder.

- **target**: The absolute angle to go to (degrees)

**When to use:** Returning to a known position ("go back to 0"), precision positioning.

#### spike_motor_run_time(port, speed, time_ms, stop_type)

Run the motor for a specific duration in milliseconds. Blocks until the time is up.

**When to use:** When you care about how long something runs rather than how far it turns.

#### spike_motor_run_until_stalled(port, speed, duty_limit)

Run the motor until it physically cannot turn anymore (stall detection). Returns after stall. Writes the stall angle into the blackboard.

- **duty_limit**: Cap the motor power as a percentage (default 100). Lower values mean gentler stall detection.

**When to use:** Finding mechanical endpoints (how far a door opens, where a gripper closes), calibration.

```lua
-- S-Expression: close gripper gently until it grabs something
spike_motor_run_until_stalled(SPIKE_PORT_C, 200, 40)
-- Now the stall angle is in the blackboard

-- ChainTree:
ct:asm_spike_motor_run_until_stalled(SPIKE_PORT_C, 200, 40)
```

---

### Motor Configuration

#### spike_set_motor_pid(port, kp, ki, kd)

Change the PID control gains for a motor. PID controls how aggressively the motor corrects errors.

- **kp**: Proportional gain (stiffness). Higher = snappier response but more oscillation.
- **ki**: Integral gain (steady-state correction). Eliminates persistent errors.
- **kd**: Derivative gain (damping). Reduces oscillation and overshoot.

**When to use:** Heavy mechanisms that oscillate, compliant grippers, precision positioning.

#### spike_set_motor_limits(port, max_speed, acceleration, torque)

Set maximum speed, acceleration, and torque for a motor.

- **max_speed**: Speed cap in deg/s
- **acceleration**: Ramp rate in deg/s^2
- **torque**: Maximum feedback torque in mNm

**When to use:** Limiting speed for safety, smoothing acceleration for heavy loads.

---

### DriveBase Commands

The drivebase controls two motors together as a wheeled robot base, with built-in odometry (distance and heading tracking).

#### spike_drivebase_drive(speed, turn_rate)

Start driving continuously. The robot keeps driving until you give another command. Non-blocking.

- **speed**: Forward speed in mm/s. Negative = backward.
- **turn_rate**: Turning speed in deg/s. Positive = turn right.

**When to use:** Free driving, line following, manual control, anything where you update speed each tick.

#### spike_drivebase_straight(distance_mm, stop_type)

Drive forward or backward a specific distance. Blocks until done.

- **distance_mm**: How far to drive. Positive = forward, negative = backward.

**When to use:** Moving precise distances between waypoints.

```lua
-- S-Expression: drive forward 500mm, then hold
spike_drivebase_straight(500, SPIKE_STOP_HOLD)

-- ChainTree:
ct:asm_spike_drivebase_straight(500, SPIKE_STOP_HOLD)
```

#### spike_drivebase_turn(angle_deg, stop_type)

Turn in place. Blocks until done.

- **angle_deg**: Degrees to turn. Positive = right (clockwise from above).

**When to use:** Turning at waypoints, pointing at targets.

#### spike_drivebase_curve(radius, angle, stop_type)

Drive an arc. Blocks until done.

- **radius**: Curve radius in mm
- **angle**: Arc angle in degrees

**When to use:** Smooth curved paths, following a wall at a distance.

#### spike_drivebase_stop() / spike_drivebase_brake()

Coast-stop or brake the drivebase.

#### spike_drivebase_reset()

Zero the odometry counters (distance and heading go back to 0).

---

### Sensor Commands

#### spike_read_sensors()

Trigger a full sensor poll. This reads all connected sensors and updates the blackboard fields (color, ultrasonic, force, IMU, motor encoders, battery voltage, accelerometer). Call this in a loop to keep data fresh.

**When to use:** At the start of every tick cycle, or in a dedicated polling fork.

```lua
-- S-Expression: continuous sensor polling loop (runs alongside other behavior)
spike_sensor_poll_loop()

-- This is a built-in composite that does:
--   read sensors -> wait 1 tick -> reset (loop forever)
```

#### spike_color_lights_on(port, brightness) / spike_color_lights_off(port)

Turn the color sensor's built-in LEDs on or off.

- **brightness**: 0-100 percent

#### spike_ultrasonic_lights_on(port, brightness) / spike_ultrasonic_lights_off(port)

Turn the ultrasonic sensor's 4 built-in LEDs on or off.

#### spike_imu_reset_heading(angle)

Reset the IMU heading reference to a specific angle (default 0).

---

### Sensor Predicates (Checks)

Predicates are yes/no questions about the world. They return true or false and are used inside composites like `se_if_then_else`, `se_while`, `se_wait`, and `se_verify`.

#### spike_motor_stalled(stalled_field) / spike_motor_done(done_field)

Check if a motor is stalled or has finished its last command.

- **stalled_field**: The blackboard field name, e.g. `"motor_a_stalled"`
- **done_field**: The blackboard field name, e.g. `"motor_a_done"`

#### spike_force_pressed() / spike_force_touched()

Check if the force sensor is being pressed (above 3N threshold) or lightly touched.

#### spike_color_is(target_color)

Check if the color sensor sees a specific color.

- **target_color**: Integer color ID from the Pybricks Color enum

#### spike_distance_lt(threshold_mm) / spike_distance_gt(threshold_mm)

Check if the ultrasonic distance is less than or greater than a threshold.

```lua
-- S-Expression: wait until something is closer than 100mm
se_wait(spike_distance_lt(100))
```

#### spike_imu_ready()

Check if the IMU is calibrated and ready to use.

#### spike_drivebase_done() / spike_drivebase_stalled()

Check if the drivebase has finished its last command or is stuck.

#### spike_field_in_range(field_name, lo, hi)

Generic check: is a blackboard field value between lo and hi?

---

### Wait Commands

These block execution until a condition is met.

#### spike_wait_color(target_color)

Wait until the color sensor detects a specific color.

#### spike_wait_distance_lt(threshold_mm)

Wait until the ultrasonic sensor reads less than threshold.

#### spike_wait_force(threshold_n)

Wait until the force sensor reads above threshold (in Newtons).

#### spike_wait_heading(target_deg, tolerance)

Wait until the IMU heading is within tolerance of the target angle.

#### spike_wait_imu_ready()

Wait until the IMU is calibrated.

---

### Safety Guards (Baby Exception Handlers)

These are the most important commands for real robots. Physical robots can hit things, run low on battery, tip over, or lose communication. Safety guards wrap your normal behavior and automatically abort and run recovery actions when something goes wrong.

#### How Guards Work

A guard monitors a fault condition every single tick while your main action runs. If the fault triggers, the guard immediately:
1. Stops the main action
2. Runs a recovery action (like emergency stop)
3. Logs what happened

This is like a safety referee watching your robot at all times.

#### spike_bump_detect(threshold)

Runs continuously and monitors the IMU accelerometer. When it detects a sudden acceleration spike (a collision or bump), it sets the `bump_detected` flag in the blackboard. Place this inside a `se_fork` alongside your main logic.

- **threshold**: Acceleration threshold in mm/s^2. Gravity is about 9810 mm/s^2. A typical bump threshold is 15000-20000.

#### spike_emergency_stop()

Immediately coast-stop ALL motors on ALL ports. The fastest way to make the robot safe.

#### spike_safe_shutdown()

Brake all motors, turn off all sensor LEDs, and log the shutdown. More controlled than emergency stop.

#### spike_log_fault(fault_name, fault_field)

Log a fault event with a name and the blackboard value that triggered it. Useful for debugging after an incident.

#### spike_clear_bump()

Clear the bump_detected flag after you have handled the collision.

#### Fault Predicates

| Predicate | What It Checks |
|-----------|----------------|
| `spike_battery_low(threshold_v)` | Battery voltage below threshold (Volts) |
| `spike_motor_overcurrent(load_field, threshold_mNm)` | Motor torque exceeds safe limit |
| `spike_motor_runaway(speed_field, max_speed)` | Motor spinning faster than safe maximum |
| `spike_tilt_exceeded(max_degrees)` | Robot pitch or roll beyond safe angle |
| `spike_bump_detected()` | Collision detected by accelerometer |
| `spike_comm_timeout(max_ticks)` | Communication bridge stopped responding |
| `spike_sensor_disconnected(connected_field)` | A sensor has been unplugged |

#### Pre-Built Guard Patterns (S-Expression)

##### spike_guarded_action(fault_pred, recovery_fn, action_fn)

The core pattern. Wraps any action with a single fault monitor.

```lua
-- Drive forward, but abort if the robot tips over
spike_guarded_action(
    function() spike_tilt_exceeded(45) end,       -- fault: tipped > 45 degrees
    function() spike_emergency_stop() end,         -- recovery: stop everything
    function() spike_drivebase_straight(1000) end  -- action: drive 1 meter
)
```

##### spike_multi_guard(guards, action_fn)

Stack multiple guards on one action. Any guard can trigger independently.

##### spike_battery_guard(threshold_v, action_fn)

Abort and safe-shutdown if battery drops below threshold.

##### spike_stall_guard(load_field, threshold_mNm, action_fn)

Abort and emergency-stop if motor draws too much current.

##### spike_tilt_guard(max_degrees, action_fn)

Abort and emergency-stop if robot tips.

##### spike_bump_guard(action_fn)

Abort and emergency-stop on collision.

##### spike_comm_guard(max_ticks, action_fn)

Abort and safe-shutdown if communication is lost.

##### spike_full_safety_guard(action_fn, opts)

The all-in-one guard. Monitors battery, tilt, bumps, and communication simultaneously. Also runs the bump detector in a parallel fork.

```lua
-- Drive with all safety monitors active
spike_full_safety_guard(function()
    se_sequence(
        function() spike_drivebase_straight(500) end,
        function() spike_drivebase_turn(90) end,
        function() spike_drivebase_straight(500) end,
        function() se_return_continue() end
    )
end, {
    battery_v = 6.5,     -- minimum battery voltage
    max_tilt = 45,       -- max tilt angle before abort
    bump_thresh = 18000, -- accelerometer bump threshold (mm/s^2)
    comm_ticks = 100,    -- max ticks without communication
})
```

##### spike_safe_straight(distance_mm, stop_type, opts) / spike_safe_turn(angle_deg, stop_type, opts)

Convenience wrappers that combine drivebase commands with the full safety suite.

##### spike_safe_motor_angle(port, speed, angle, load_field, max_load, stop_type)

Run a motor with overcurrent and bump protection.

#### Pre-Built Guard Patterns (ChainTree)

ChainTree guards use the **exception catch** pattern, which creates a three-column structure: main (normal behavior), recovery (what to do on fault), and finalize (cleanup).

```lua
-- Battery-guarded drive column
ct:define_spike_battery_guard("safe_drive", 6.5, function(self)
    self:asm_spike_drivebase_straight(500, SPIKE_STOP_HOLD)
    self:asm_spike_drivebase_turn(90, SPIKE_STOP_HOLD)
end)
```

| Guard Method | Fault Detected | Recovery Action |
|-------------|----------------|-----------------|
| `define_spike_battery_guard(name, threshold_v, fn)` | Low battery | Safe shutdown |
| `define_spike_stall_guard(name, aux_data, fn)` | Motor overcurrent | Emergency stop |
| `define_spike_tilt_guard(name, max_degrees, fn)` | Robot tipped | Emergency stop |
| `define_spike_bump_guard(name, fn)` | Collision detected | Emergency stop + clear bump |
| `define_spike_comm_guard(name, max_ticks, fn)` | Communication lost | Safe shutdown |

---

### Composite Helpers

These combine multiple leaf commands into common patterns.

| Helper | What It Does |
|--------|-------------|
| `spike_do_motor_angle(port, speed, angle, stop_type)` | Run motor angle, then continue |
| `spike_do_straight(distance_mm, stop_type)` | Drive straight, then continue |
| `spike_do_turn(angle_deg, stop_type)` | Turn, then continue |
| `spike_drive_until_close(speed, turn_rate, threshold_mm)` | Drive until ultrasonic detects object, then brake |
| `spike_find_endpoint(port, speed, duty_limit)` | Run until stalled, reset angle to 0 |
| `spike_sensor_poll_loop()` | Continuous sensor reading loop (for use in forks) |
| `spike_wait_imu_ready()` | Block until IMU calibrated |

ChainTree equivalents:

| Helper | What It Does |
|--------|-------------|
| `asm_spike_find_endpoint(port, speed, duty_limit)` | Stall + reset to 0 |
| `asm_spike_drive_square(side_mm, turn_angle)` | Drive a square pattern |

---

## Files in This Directory

| File | Purpose |
|------|---------|
| `README.md` | This guide |
| `README_micro_python_commands.md` | Complete Pybricks MicroPython API reference |
| `se_lego_spike.lua` | S-Expression engine DSL helper -- all SPIKE commands |
| `ct_lego_spike.lua` | ChainTree DSL helper -- all SPIKE commands |
