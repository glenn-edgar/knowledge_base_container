# Local Planner

Sequences virtual actions from the global plan. Manages hub lifecycle per robot.

## Boundary
- **Accepts:** Ordered virtual actions (route from global planner)
- **Knows about:** Action sequences, pipe protocol, hub lifecycle
- **Does NOT know about:** Packet internals, hardware, board graph

## Interface
- Receives route from global planner
- Instantiates hub (one per robot) via runtime
- Stages current_test_json / next_test_json through pipe
- Monitors action completion events from hub
- Reports overall mission status to action server

## Subdirectories
- `lib/` — sequencer, KB chaining logic, pipe protocol
