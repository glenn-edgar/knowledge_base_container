# Global Planner

Dijkstra route planning over the virtual node graph.

## Boundary
- **Accepts:** Start node, target node, robot capabilities
- **Knows about:** Board graph (nodes, edges, weights), robot capability masks
- **Does NOT know about:** Hub, packets, runtime, hardware

## Interface
- Input: board definition + strategy + robot capabilities
- Output: ordered list of virtual actions (route)
- Constrains routes to actions the robot supports

## Subdirectories
- `lib/` — dijkstra.lua, vn_dsl.lua, global_planner.lua, plan_yaml.lua
- `boards/` — board definitions (one per physical environment)
