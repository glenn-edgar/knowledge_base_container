# Knowledge Base

## Two SQLite Schemas

### Extracted KB (from master_build.lua)

Per-domain SQLite files at `/home/gedgar/Sqlite_Data/`. Two KB namespaces:

- **system** — hardware topology: site → CPU → containers → services
- **subsystems** — domain definition: robots, classes, boards, virtual nodes

Used by the container bootstrap for infrastructure discovery.

### Constructed KB (from construct_surface_ops.lua)

Runtime KB built at startup. Single namespace: `moonbase.alpha.surface_ops`

Contains:
- Board graphs (nodes, edges with nav type and weight)
- Robot class definitions (capabilities, energy, hardware config)
- Robot instances (connection info, status, energy)
- Virtual node definitions (packet_type_id, json_schema, bitmask, pose_fields)
- Planner state

Used by action_server, sequencer, hub_runtime, global_planner.

## KB Query API

```lua
local kb_query = require("kb_query")
local q = kb_query.new("surface_ops.db")

-- Infrastructure (extracted KB)
q:get_infrastructure()     -- { nats={host,port}, mqtt={host,port}, postgres={...} }
q:get_domain()             -- { site="moonbase.alpha.surface_ops", container="..." }
q:get_container_config(n)  -- { image, sqlite_db, params }

-- Boards
q:get_board("landing_zone")  -- { nodes=[...], edges=[...] }
q:list_boards()

-- Robots
q:list_robots()
q:get_robot_config(id)     -- { instance, class, capabilities, hardware, connection, energy }
q:get_capabilities(id)     -- ["init_check", "path_spline", ...]
q:get_energy_max(id)
q:get_hardware(id)

-- Virtual nodes
q:get_virtual_node("path_spline")  -- { packet_type_id, json_schema, bitmask, pose_fields }
q:get_all_virtual_nodes()
q:list_virtual_nodes()

-- Site
q:get_site()               -- "moonbase.alpha.surface_ops"
q:get_site_config()        -- full site overview

q:close()
```

## Unified Namespace

All KB paths, NATS topics, and MQTT topics follow the same lowercase dot-separated convention:

```
moonbase.alpha.surface_ops                          -- site root
moonbase.alpha.surface_ops.robots.rover_1.status.state
moonbase.alpha.surface_ops.robots.rover_1.status.energy
moonbase.alpha.surface_ops.robots.rover_1.status.link
moonbase.alpha.surface_ops.boards.landing_zone
moonbase.alpha.surface_ops.robot_class.lunar_rover.infra.shared
moonbase.alpha.surface_ops.virtual_nodes.definitions.vn_type.path_spline
moonbase.alpha.surface_ops.action_server.rover_1.status
moonbase.alpha.surface_ops.action_server.summary
moonbase.alpha.surface_ops.action_server.mission_log
```

MQTT topics use `/` instead of `.`:

```
moonbase/alpha/surface_ops/robots/rover_1/rpc
moonbase/alpha/surface_ops/robots/rover_1/stream_bus
moonbase/alpha/surface_ops/robots/rover_1/link
moonbase/alpha/surface_ops/robots/rover_1/planner/ack
```
