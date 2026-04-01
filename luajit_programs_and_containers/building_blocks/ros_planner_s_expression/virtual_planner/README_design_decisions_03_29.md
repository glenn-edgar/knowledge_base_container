# Design Decisions and Integration Steps — 2026-03-29

## Session Summary

This session evolved the planner from a conceptual architecture with ad-hoc
simulation into a real ChainTree DSL-based system with proper directory
structure, build scripts, and test framework.

## Key Design Decisions

### 1. Virtual Nodes Are the Atomic Unit

The global planner doesn't know what happens inside a virtual node. It only
knows the graph of virtual nodes and routes through them. Each virtual node
maps to one ChainTree KB (test) on the hub.

We started with generic types (path, mission) and refined to specific nodes
matching the actual competition scenario:
- 3 path types: spline, line, wall
- 5 mission types: deliver_part, paint_sample, load_shipping, pass_gate, inspection_scan
- 2 infrastructure types: init_check, path_rotate

### 2. Blackboard Initialization via JSON Strings

The ChainTree blackboard requires declared fields. Virtual action parameters
are variable-length nested structures (segments with coordinates, waypoints).
We solved this by JSON-encoding the init data into per-KB string fields:

```
handle.blackboard.path_spline_init = '{"from":{"x":0,"y":0},"segments":[...]}'
```

The KB's init one-shot decodes the JSON. One string field per KB in the
blackboard declaration.

### 3. KB Chaining via next_test

Instead of the sequencer explicitly activating each KB, the KBs chain
themselves. The finalize column calls PLANNER_START_NEXT_TEST which reads
next_test (int) from the blackboard and activates the next KB.

The sequencer only needs to:
- Stage the init JSON for current and next (double-buffered)
- Set next_test to the upcoming KB index
- Activate the first KB

This mirrors how car navigation works — the system always knows the next
instruction before the current one completes.

### 4. Coordinates Not Names in Path Segments

The robot doesn't know node names. Path segments carry actual {x,y}
coordinates. The global planner resolves names to coordinates when building
path actions. Names kept as from_name/to_name for human debugging.

### 5. Heading Tracking and Rotation Insertion

The global planner tracks heading through the entire plan:
- Calculates heading from segment geometry (atan2 of dx,dy)
- Spline paths handle heading changes through curve geometry
- Non-spline paths (wall_ride) need explicit rotation before them
- Missions need rotation to approach_heading

path_rotate actions are automatically inserted where heading mismatches.

### 6. Scan Tree Bitmaps as uint64

Rather than individual boolean fields for each sensor state, the blackboard
carries two uint64 bitmap fields:
- bitmap_robot_good_conditions — scan tree "all OK" output
- bitmap_robot_faults — scan tree "something wrong" output

Plus odometry: local_distance_x/y, current_heading, current_speed.

The scan tree on the robot evaluates all sensors, packs results into bitmaps,
streams to hub. The hub reads two words instead of N individual fields.

### 7. nav_data for Edge-Specific Parameters

Different nav methods need different parameters (wall_standoff for wall_ride,
junction info for line_follow). The edge format carries an optional nav_data
table that gets merged into the segment output. Extensible without changing
the edge format.

### 8. Hub DSL Follows Existing ChainTree Test Pattern

The hub_dsl.lua follows the exact same pattern as incremental_build.lua:
- add_header() creates ChainTreeMaster and defines blackboard
- One function per KB (test)
- test_list and test_dict for dispatch
- Main generates JSON + debug YAML

This means the hub trees are real ChainTree structures compiled through
the standard pipeline. Same JSON IR format, same ct_loader, same ct_runtime.

### 9. Custom Functions Prefixed PLANNER_

Hub-specific user functions go in hub_functions/ directory and are prefixed
PLANNER_ to avoid collision with CFL_ builtins. First function:
PLANNER_START_NEXT_TEST reads next_test from blackboard and activates the
corresponding KB.

### 10. Original Lego Robot Project Preserved

The original s-expression lego robot project (lego_robots/, lua_dsl/) is
untouched. The original drive_program DSLs are preserved in drive_program_v1/.
The new work lives entirely in virtual_planner/.

## Integration Steps Completed

1. **Directory reorganization:** drive_program/ + inner_planner/ → virtual_planner/
   with lib/, hub_trees/, remote_trees/, boards/, dsl_tests/, hub_functions/

2. **Shell scripts:** build_trees.sh (compile DSLs), run_test.sh (single test),
   run_all_tests.sh (all tests), run_planner.sh (global plan only)

3. **Hub DSL:** 10 empty KB stubs compiled to hub.json (75 nodes) via
   ChainTreeMaster pipeline

4. **Remote DSL:** 1 KB compiled to remote.json (16 nodes)

5. **ct_loader_pure:** Drop-in replacement for ct_loader that uses json_util
   instead of cjson (avoids Lua 5.4/LuaJIT incompatibility)

6. **Global planner updates:** init_check action, heading tracking, path_rotate
   insertion, coordinates in segments, nav_data for wall_standoff

7. **CT full simulation test:** dsl_tests/ct_full_simulation/ loads real JSON IR,
   sequences 22 virtual actions through ChainTree runtime. All pass (as stubs).

## What's Next

- Build init_check KB tree (hub + remote) — first real end-to-end
- Build path_spline KB tree — first real path execution
- Wire RPC channel into the CT test (currently 0 RPC packets)
- Each KB built one at a time, tested incrementally

## Files Changed/Created This Session

```
virtual_planner/
  lib/global_planner.lua        — heading tracking, init_check, path_rotate, coords
  lib/vn_dsl.lua                — nav_data parameter for edges
  lib/ct_loader_pure.lua        — pure Lua JSON loader (new)
  hub_trees/hub_dsl.lua         — 10 KBs with proper blackboard
  hub_trees/hub.json            — compiled (75 nodes)
  hub_trees/hub_debug.yaml      — debug output
  remote_trees/remote_dsl.lua   — unchanged
  remote_trees/remote.json      — compiled (16 nodes)
  hub_functions/planner_start_next_test.lua — PLANNER_START_NEXT_TEST (new)
  boards/workshop_floor/board.lua — wall_standoff nav_data
  boards/workshop_floor/global_plan.json — regenerated with new format
  boards/workshop_floor/global_plan.yaml — regenerated
  dsl_tests/ct_full_simulation/ — new test (config.lua + main.lua)
  build_trees.sh                — updated for single-file DSL pattern
  run_test.sh                   — hub_functions/ in LUA_PATH
  README_design_decisions_03_29.md — this file
```
