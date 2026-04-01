# Robots

One directory per robot kind. Each declares capabilities and provides
the remote-side implementation.

## Boundary
- **Accepts:** AVRC command packets from hub via transport
- **Knows about:** Its own hardware, its own capabilities
- **Does NOT know about:** Other robots, planning, board graph

## Per-robot directory contents
- `capabilities.lua` — declares supported virtual node types
- `remote_dsl.lua` — remote behavior tree (ChainTree DSL)
- `common/` — Level 3: functions shared across all tests for this robot
- `remote_user_functions.lua` — hardware-specific function implementations
