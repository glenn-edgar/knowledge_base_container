# First Mission: FLL Competition Robot

## Overview

This mission defines a complete FIRST LEGO League competition strategy for a SPIKE Prime robot. Three DSL files describe the robot hardware, the competition field, and the match plan. A compiler transforms them into runtime artifacts that deploy to the SPIKE Prime hub as Pybricks MicroPython data structures.

## Architecture

```
 equipment_dsl.lua   map_dsl.lua   mission_dsl.lua   spline.lua
       |                 |               |               |
       +--------+--------+---------------+---------------+
                |
        compile_mission.lua
                |
    +-----------+-----------+-------------+
    |           |           |             |
plan_expanded  mission_    robot_kb     bindings
   .txt        tree.lua    .sqlite       .json
                |
        s-expression compiler (stage6)
                |
        MicroPython data structures
                |
        SPIKE Prime hub (Pybricks)
```

The three DSL files are the only things a human edits. The compiler produces all runtime artifacts. The s-expression tree compiles down to MicroPython data that runs on the SPIKE Prime hub.

## Execution Model

A single `se_state_machine` reads the `run_select` blackboard field. The kid presses the hub button to select a menu_id (1, 2, or 3). The state machine dispatches to the corresponding run. After the run completes, `run_select` resets to 0 (idle) and the kid swaps the attachment jig for the next run.

```
Menu:
  0 = idle (waiting for selection)
  1 = run_1_pusher    (pusher jig,      75 pts)
  2 = run_2_gripper   (gripper arm jig, 25 pts)
  3 = run_3_cargo     (cargo tray jig,  20 pts)
```

## DSL Files

### equipment_dsl.lua -- What the robot is

Defines the physical robot: hub type, port assignments, drivebase geometry, sensor configuration, guard thresholds, and virtual function slots.

**Chassis (fixed across all runs):**
- Ports A/B: large drive motors (56mm wheels, 128mm axle track)
- Port E: color sensor for line following (reflection threshold 30)
- Port F: ultrasonic for obstacle detection (close threshold 50mm)

**Attachment jigs (swapped between runs):**
- **Pusher** (menu_id=1): passive flat blade, no motors. Rams levers and spins wheels.
- **Gripper arm** (menu_id=2): port C arm lift (0-160 deg) + port D gripper (0-90 deg). Picks up and carries objects.
- **Cargo tray** (menu_id=3): port C tray lift (0-120 deg). Carries and dumps pre-loaded cargo.

**Base virtual function slots** (available to all jigs):
- 13 predicates: sensor reads, fault detection
- 6 oneshots: stop, brake, reset, emergency
- 10 main functions: drive, turn, curve, line follow, wall ride, spline follow, wait, align

**Jig-specific slots** are only available when that jig is mounted. The compiler validates that each run only uses slots from its jig + the base set.

**Spline configuration:** Catmull-Rom tau=0.5, base speed 150mm/s, heading Kp=2.0 for IMU feedback control.

### map_dsl.lua -- Where the robot operates

Defines the FLL competition field: 2362x1143mm table with 89mm walls.

Coordinate system: origin at bottom-left, X = long side (east), Y = short side (north).

**Navigation structure:**
- **24 waypoints** with position, heading, and navigation method
- **24 edges** connecting waypoints (line_follow, wall_ride, or direct)
- **5 zones** for rule enforcement (launch area, no-touch zones)
- **6 mission models** with interaction types and point values
- **5 navigation lines** printed on the mat (black lines the color sensor follows)
- **5 spline routes** for smooth curved paths between waypoints

The main navigation spine runs east-west at y=571mm. Two perpendicular branches (north at x=700, south at x=1500) and an east loop provide access to all mission models.

**Spline routes** define smooth multi-waypoint curves:
- `launch_to_spine`: home -> launch_exit -> spine_west (419mm smooth curve)
- `spine_to_south`: spine_west -> branch_n_base -> spine_center -> branch_s_base (1200mm)
- `spine_to_home`: spine_west -> launch_exit -> home (419mm)
- `west_sweep` and `east_loop` available for future optimization

### mission_dsl.lua -- What the robot does

Defines the match strategy: 3 runs in 150 seconds, 10-second reserve. Each run specifies its jig and menu_id.

**Safety guards** (applied to every run, outermost = highest priority):
1. Battery critical (< 6200mV) -- emergency stop, abort match
2. Comm timeout (> 500ms silence) -- brake all, abort match
3. Tilt (> 45 degrees) -- stop all, pause
4. Bump (> 2500 milli-g) -- brake, wait 0.5s, continue
5. Battery low (< 6800mV) -- log only

**Navigation helpers:**
- `nav.goto(from, to)` -- compiler resolves shortest path, emits drive/turn/line-follow
- `nav.spline_route("name")` -- compiler fits Catmull-Rom spline, emits SPLINE_FOLLOW with cubic Bezier control points
- `nav.align_wall()` -- square against wall for position correction
- `nav.find_line()` -- drive until color sensor finds a black line

**Action templates** (tagged with required jig):
- `push_lever` -- any jig, drive into lever, reverse out
- `drive_over` -- any jig, straight drive across structure
- `spin_wheel` -- any jig, push contact, wait, reverse
- `grip_and_carry` -- gripper_arm only, lower arm, close gripper, lift
- `deliver_payload` -- gripper_arm only, open gripper to drop piece
- `tray_deliver` -- cargo_tray only, tilt tray to dump contents

**Action overrides:** m3_cargo_deliver uses `deliver_payload` with gripper_arm but `tray_deliver` with cargo_tray. The compiler selects the right template per run's jig.

## Spline Paths

Advanced FLL teams use spline paths instead of stop-turn-drive sequences. A spline is a smooth mathematical curve the robot follows using IMU heading feedback and differential wheel speeds.

**How it works:**
1. The Map DSL defines named spline routes as ordered waypoint lists
2. The compiler fits a Catmull-Rom spline through the (x,y) coordinates
3. The spline is converted to cubic Bezier segments (4 control points each)
4. Control points are flattened to integer mm and emitted as SPLINE_FOLLOW parameters
5. The hub evaluates the Bezier curves per-tick, computing v_left and v_right from the tangent and axle track

**Benefits:**
- Robot maintains momentum through turns (no stopping)
- Saves seconds in a 2.5-minute match
- Smoother motion reduces risk of attachments falling off
- Consistent trajectories from run to run

**Current spline usage:** launch exit (419mm curve replaces 2 stop-and-turn hops), spine traverse (1200mm curve replaces 3 hops), and return to launch.

## Match Strategy

### Run 1: Pusher Jig (budget 50s, expected 75 pts)

```
home ~~ spline ~~ spine_west
  -> m6_approach: flip switch (10 pts)    [push_lever]
  -> m4_approach: spin wheel (15 pts)     [spin_wheel]
  -> m1_approach: push solar panel (20 pts) [push_lever]
  -> m2_approach: drive over bridge (30 pts) [drive_over]
spine_west ~~ spline ~~ home
```

No motors needed -- all missions use passive chassis contact. Highest points-per-second run. Does 4 missions in one sweep.

### Run 2: Gripper Arm Jig (budget 50s, expected 25 pts)

```
home ~~ spline ~~ spine_west ~~ spline ~~ branch_s_base
  -> loop_e_entry -> loop_e_far
  -> m5_approach: collect water sample (25 pts) [grip_and_carry]
  return via spine
spine_west ~~ spline ~~ home
```

Long traverse to east loop. Requires gripper arm to pick up the water sample. Carries it back to launch for bonus.

### Run 3: Cargo Tray Jig (budget 40s, expected 20 pts)

```
home ~~ spline ~~ spine_west ~~ spline ~~ branch_s_base
  -> branch_s_mid -> m3_approach
  -> deliver energy unit (20 pts)         [tray_deliver]
  return via spine
spine_west ~~ spline ~~ home
```

Pre-loads energy_unit on cargo tray at launch. Tips it out at delivery zone. First run to drop if time is short (lowest points/time).

## SPIKE Prime Hub Memory Footprint

The compiled mission tree with spline support fits within the SPIKE Prime's Pybricks resource limits.

### Flash (frozen into firmware bytecode)

| Component | Bytecode |
|-----------|----------|
| Module data (~310 nodes, 3 runs + guards + splines) | ~62K |
| se_runtime_spike.py (slim engine) | ~10K |
| SPIKE user functions (~15 functions) | ~5K |
| **Total** | **~77K of 100K** |
| **Headroom** | **23K** |

### Heap (runtime)

| Component | Heap |
|-----------|------|
| new_module() function tables | ~4K |
| new_instance() -- 310 node_states | ~25K |
| Blackboard (~20 fields) | ~2K |
| Instance overhead | ~1K |
| **Total** | **~32K of 256K** |
| Per tick allocation | 0 |

The biggest cost is node_states -- 310 dicts at ~80 bytes each. If that becomes a concern, those can be replaced with a flat array of 3-element lists ([flags, state, user_data]) to cut it roughly in half.

Zero per-tick heap allocation means no garbage collection pauses during execution.

## Compiler Output

### plan_expanded.txt

Human-readable step-by-step expansion of all three runs. Shows every slot call, parameters, timeouts, jig assignments, and spline route details. Use this to review the plan before deploying.

82 total steps across 3 runs. 15,374mm total travel distance. 120 expected points.

### mission_tree.lua

Single `define_tree("match_dispatcher")` containing an `se_state_machine` on the `run_select` field. Each `se_case` corresponds to a run/jig pair, wrapped in `spike_multi_guard` with 5 fault monitors. Body is `se_sequence` of concrete slot calls including `spline_follow()` with cubic Bezier control points.

This file feeds into the s-expression compiler (stage6) which produces the MicroPython data structures for the hub.

### robot_kb.sqlite

SQLite knowledge base. All data stored as ltree-style dot-separated paths in a single `kb` table:

| Path prefix | Content |
|-------------|---------|
| `robot.*` | Hub identity, firmware, season |
| `equipment.port.*` | Chassis port-to-role-to-device mapping |
| `equipment.jig.*` | Per-jig ports, motors, limits, slots |
| `equipment.drivebase.*` | Wheel diameter, axle track, speeds |
| `equipment.guard.*` | Threshold values for fault detection |
| `equipment.slot.*` | Base virtual function slot definitions |
| `equipment.menu.*` | Menu field name and default |
| `map.waypoint.*` | x, y, heading, nav_method per waypoint |
| `map.edge.*` | Distance, method per edge |
| `map.zone.*` | Rectangular regions with rule lists |
| `map.mission.*` | Position, interact type, points per model |
| `plan.run.*` | Compiled steps with slot, params, timeout, jig |
| `plan.precond.*` | Preconditions and effects per mission |
| `plan.jig_compat.*` | Which missions each jig can handle |

The planner queries this database at runtime to track world state and (future) search for applicable actions.

### bindings.json

Maps each virtual function slot to its RPC command format. Includes chassis ports, drivebase geometry, and per-jig port configurations. Predicates are host-side blackboard reads (no RPC). Oneshots and main functions send JSON commands over BLE UART.

## How to Compile

```
luajit compile_mission.lua --all
```

Flags: `--plan`, `--tree`, `--kb`, `--bindings`, or `--all` (default).

Requires LuaJIT and sqlite3 on the path. If sqlite3 is missing, `robot_kb.sql` is still generated and can be loaded manually.

## How to Modify

**Change robot hardware:** Edit `equipment_dsl.lua`. Change port assignments, wheel diameter, guard thresholds, add new jig configurations. Recompile.

**Change field layout:** Edit `map_dsl.lua`. Move waypoints, add/remove missions, adjust edge distances, define new spline routes. Recompile.

**Change strategy:** Edit `mission_dsl.lua`. Reorder missions within runs, move missions between runs, adjust time budgets, swap `nav.goto` for `nav.spline_route` or vice versa. Recompile.

**Add a new jig:** Add entry to `Equipment.jigs` with menu_id, ports, motors, and slots. Add action templates that use the jig's slots. Create a run that references the new jig.

**Add a new mission model:** Add entry to `Map.missions` with position, approach waypoint, interact type, and points. Add approach waypoint and edge. If the interact type is new, add an action template in `Mission.actions`. If the mission can be done by multiple jigs, add entries to `Mission.action_override` and `Mission.jig_compat`.

**Add a spline route:** Add entry to `Map.spline_routes` listing waypoint names in order. Use `Mission.nav.spline_route("name")` in run steps. The compiler fits the curve and emits control points automatically.

## What Is Not Built Yet

- **se_runtime_spike.py** -- the slim MicroPython s-expression engine for the hub (~10K bytecode)
- **SPIKE user functions** -- MicroPython implementations of the 15 leaf function slots (~5K bytecode)
- **BLE communication bridge** -- transport layer between Pi Zero 2 W host and SPIKE hub
- **Runtime planner loop** -- walks KB steps, dispatches to ChainTree, tracks match time
- **Auto-planning from preconditions** -- precondition/effect data is in the KB but no solver queries it yet
- **LINE_FOLLOW and WALL_RIDE** -- currently compile to `spike_drivebase_straight()` (same as direct drive); real implementations need PID control using the color/ultrasonic sensor
- **Hub menu UI** -- button press handler to set `run_select` field with confirmation display
