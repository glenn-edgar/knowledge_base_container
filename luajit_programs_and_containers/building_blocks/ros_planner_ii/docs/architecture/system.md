# System Architecture

## Components

| Component | Location | Role |
|-----------|----------|------|
| Planner Container | `third_party_containers/ros_planner/` | Plans and executes missions |
| MQTT Robot | `ros_planner_ii_mqtt_robot/` | Executes virtual nodes on hardware |
| KV Bridge | `third_party_containers/kv_bridge/` | MQTT → NATS KV async writer |
| NATS | Infrastructure | Mission queue, KV store, JetStream telemetry |
| MQTT | Infrastructure | Robot command/stream transport |
| SQLite KB | `/data/*.db` | Knowledge base (bind-mounted) |

## Data Flow

```
SQLite KB (extracted)
    |
    +-- Infrastructure discovery (NATS host:port, MQTT host:port, site name)
    |
    +-- Virtual node definitions (packet_type_id, json_schema, bitmask, pose_fields)
    |
    +-- Board graph (nodes, edges, nav types, weights)
    |
    +-- Robot class config (capabilities, energy, hardware)

NATS (external consumers)
    |
    +-- Mission submission (JobQueue)
    +-- Mission status/result (KeyStore)
    +-- Fleet summary (KeyStore)
    +-- Mission log (KeyStore, history=50)
    +-- Telemetry stream (JetStream)

MQTT (robot transport)
    |
    +-- RPC commands (planner → robot)
    +-- Stream responses (robot → planner: ACK, heartbeat, KB_DONE)
    +-- Link protocol (announce, confirm, heartbeats, disconnect)
    +-- Robot status (retained: state, energy, bitmask)
```

## Infrastructure Discovery

The planner reads infrastructure connection details from the extracted SQLite KB at startup. No hardcoded defaults.

```lua
local q = kb_query.new(db_file)
local infra = q:get_infrastructure()
-- infra.nats = { host = "0.0.0.0", port = 4222, ws_port = 9222 }
-- infra.mqtt = { host = "0.0.0.0", port = 1883 }
local domain = q:get_domain()
-- domain.site = "moonbase.alpha.surface_ops"
```

The DSL (`kb_dsl/scripts/site_config.lua`) defines infrastructure containers. The master build extracts them into per-domain SQLite files.

## Container Architecture

Single generic image: `nanodatacenter/ros-planner:latest`

```bash
docker run --network host \
  -v /home/gedgar/Sqlite_Data:/data:ro \
  -e SQLITE_DB=/data/surface_ops.db \
  nanodatacenter/ros-planner:latest
```

Startup flow (`bootstrap_container.lua`):

1. Query extracted SQLite for NATS/MQTT/site
2. Build runtime KB (`construct_surface_ops.lua`)
3. Build robot remote.json (ChainTree DSL)
4. Export KB to NATS KV
5. Start planner_server.lua (in-process via dofile)
