# Hub DSL

Universal hub — knows ALL virtual node types and AVRC packet formats.
Robot-independent. Instantiated by the local planner, one per robot.

## Boundary
- **Accepts:** current_test_json / next_test_json on blackboard
- **Knows about:** Every virtual node KB, every AVRC packet format
- **Does NOT know about:** Which robot, which board, planning

## Interface
- Reads action JSON from blackboard
- Generates AVRC binary command packets (FFI structs)
- Writes packet pointer to blackboard
- Sends packets to remote via transport pipe
- Receives streaming data from remote via transport pipe

## Function Levels
- Level 2 (hub↔robot shared): `protocol/` — packet definitions both sides speak
- Level 4 (per-KB): `kb/` — behavior tree + one-shot per virtual node type

## Subdirectories
- `protocol/` — command_packets.lua, stream_packets.lua, blackboard_schema.lua
- `kb/` — one file per virtual node KB (init_check.lua, path_spline.lua, ...)
- `hub_functions/` — chaining logic (planner_start_next_test)
- `hub_dsl.lua` — master DSL assembling all KBs + blackboard
