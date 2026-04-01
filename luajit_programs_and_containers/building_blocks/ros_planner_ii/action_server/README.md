# Action Server

Entry point for the planner system. Accepts commands on a virtual input channel.

## Boundary
- **Accepts:** Commands ("move robot X to virtual node Y")
- **Knows about:** Robot names, command types
- **Does NOT know about:** Routes, packets, hardware, behavior trees

## Interface
- Virtual input channel for commands
- Dispatches to global planner with robot name + target node
- Returns status/completion to caller
