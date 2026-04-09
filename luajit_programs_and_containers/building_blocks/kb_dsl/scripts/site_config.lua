--[[
  site_config.lua — Declarative site definition

  All CPUs, containers, services, domains, and robots in one place.
  Add a new mission planner / robot controller by adding entries here.
  The builder scripts loop over this data — no builder code changes needed.

  Physical model:
    CPU → containers → services
    CPU has a master flag; master holds Postgres.
    CPU controllers read their own subtree to know what to instantiate.

  Container lifecycle:
    depends_on      — names of containers that must be running before this one starts
    docker_name     — actual Docker container name on the host
    Startup order:  topological sort of depends_on (roots first)
    Shutdown order:  reverse topological sort (leaves first)

  Run spec (complete deployment manifest):
    image           — Docker image
    ports           — { "host:container", ... }
    volumes         — { "host_path:container_path[:ro]", ... }
    tmpfs           — { "/path:opts", ... }
    env             — { KEY = "value", ... }  container-own config ONLY
    network         — Docker network name (nil = default bridge)
    restart         — restart policy (nil = no, "unless-stopped", "always")
    links           — { "container:alias", ... }
    cmd             — override CMD (nil = image default)

  Service discovery (injected by orchestrator at runtime):
    The orchestrator resolves service endpoints from depends_on containers.
    For each dependency's service, it injects env vars using the dependency's
    docker_name and service data (protocol, port). No hardcoded endpoints
    in env — Postgres KB is the single source of truth.

  Architecture (2026-04-08):
    - MQTT for all robot communication (planner owns client directly)
    - NATS for job queues, telemetry streams, KB export
    - NATS KV for external consumers (dashboard, monitoring)
    - kv_bridge container: MQTT → NATS KV async writes
    - No NATS↔MQTT bridge (eliminated — planner talks MQTT directly)
    - Robots support JSON or CBOR wire format
    - One-shot orchestrator containers for startup/shutdown/KB rebuild
]]

-- Host paths (resolved at build time, stored in KB for orchestrator)
local SQLITE_DATA   = os.getenv("SQLITE_DATA")   or "/home/gedgar/Sqlite_Data"
local POSTGRES_DATA = os.getenv("POSTGRES_DATA")  or "/home/gedgar/Postgres_Data/vector"
local SHELL_DIR     = os.getenv("SHELL_DIR")      or "/home/gedgar/knowledge_base_assembly/luajit_programs_and_containers/building_blocks/system_api/shell"

return {

  ---------------------------------------------------------------------------
  -- Physical tree: CPUs → containers → services
  ---------------------------------------------------------------------------
  site_name = "main",

  cpus = {
    { name        = "cpu_01",
      type        = "system_controller",
      description = "System controller node",
      master      = true,

      containers = {
        { name        = "nats_server",
          type        = "infrastructure",
          description = "NATS JetStream message bus (RAM-backed)",
          image       = "nanodatacenter/nats-js-ram:latest",
          docker_name = "nats-js-ram",
          depends_on  = {},
          ports       = { "4222:4222", "8222:8222", "9222:9222" },
          tmpfs       = { "/data:rw,size=50m" },
          services = {
            { name = "nats", protocol = "nats",
              data = { host = "127.0.0.1", port = 4222, ws_port = 9222 } },
            { name = "monitoring", protocol = "http",
              data = { host = "127.0.0.1", port = 8222 } },
          },
        },

        { name        = "mqtt_broker",
          type        = "infrastructure",
          description = "Mosquitto MQTT broker for robot communication",
          image       = "nanodatacenter/mosquitto-ram-ws:latest",
          docker_name = "mosquitto-ram-ws_main",
          depends_on  = {},
          ports       = { "1883:1883", "9001:9001" },
          services = {
            { name = "mqtt", protocol = "mqtt",
              data = { host = "127.0.0.1", port = 1883 } },
          },
        },

        { name        = "postgres",
          type        = "infrastructure",
          description = "Postgres 17 with pgvector — system KB store",
          image       = "pgvector/pgvector:pg17",
          docker_name = "pg-vector",
          depends_on  = {},
          ports       = { "5432:5432" },
          volumes     = { POSTGRES_DATA .. ":/var/lib/postgresql/data" },
          env         = {
            POSTGRES_USER = "gedgar",
            POSTGRES_PASSWORD = "$POSTGRES_PASSWORD",
            POSTGRES_DB = "knowledge_base",
          },
          services = {
            { name = "postgres", protocol = "postgres",
              data = { host = "127.0.0.1", port = 5432, dbname = "knowledge_base" } },
          },
        },

        { name        = "web_gateway",
          type        = "infrastructure",
          description = "OpenResty HTTP gateway",
          image       = "nanodatacenter/openresty-gateway:latest",
          docker_name = "openresty-gateway",
          depends_on  = { "nats_server", "postgres" },
          ports       = { "8080:8080" },
          volumes     = { SHELL_DIR .. ":/srv/shell:ro" },
          env         = {
            POSTGRES_PASSWORD = "$POSTGRES_PASSWORD",
          },
          links       = { "pg-vector:pg-vector" },
          services = {
            { name = "http", protocol = "http",
              data = { host = "127.0.0.1", port = 8080 } },
          },
        },

        { name        = "kv_bridge",
          type        = "service",
          description = "MQTT to NATS KV async writer (Go container)",
          image       = "nanodatacenter/kv-bridge:latest",
          docker_name = "kv-bridge",
          depends_on  = { "nats_server", "mqtt_broker" },
          network     = "planner-net",
          restart     = "unless-stopped",
          env         = {
            MQTT_TOPIC = "kv_bridge/write",
            LOG_LEVEL  = "info",
          },
          env_names   = { nats = "NATS_URL" },
          services = {
            { name = "mqtt_client", protocol = "mqtt",
              data = { role = "subscriber", topic = "kv_bridge/write" } },
            { name = "nats_client", protocol = "nats",
              data = { role = "client" } },
          },
        },

        { name        = "telemetry_collector",
          type        = "action_server",
          description = "Telemetry collection and history",
          image       = "nanodatacenter/telemetry:latest",
          docker_name = "telemetry-collector",
          depends_on  = { "nats_server" },
          network     = "planner-net",
          restart     = "unless-stopped",
          volumes     = { SQLITE_DATA .. ":/data" },
          env         = {
            SQLITE_DB = "/data/telemetry.db",
          },
          sqlite_db   = "/data/telemetry.db",
          params      = {},
          services = {
            { name = "nats_client", protocol = "nats",
              data = { role = "client" } },
          },
        },

        { name        = "surface_ops_planner",
          type        = "action_server",
          description = "Surface operations mission planner",
          image       = "nanodatacenter/ros-planner:latest",
          docker_name = "surface_ops-planner",
          depends_on  = { "nats_server", "mqtt_broker" },
          network     = "planner-net",
          restart     = "unless-stopped",
          volumes     = { SQLITE_DATA .. ":/data" },
          env         = {
            SQLITE_DB = "/data/surface_ops.db",
          },
          sqlite_db   = "/data/surface_ops.db",
          params      = { max_concurrent_missions = 4,
                          heartbeat_interval_s     = 10 },
          services = {
            { name = "nats_client", protocol = "nats",
              data = { role = "client" } },
            { name = "mqtt_client", protocol = "mqtt",
              data = { role = "client" } },
          },
        },

      },
    },
  },

  ---------------------------------------------------------------------------
  -- Domains: mission planner / robot controller logical groupings
  -- References containers by cpu_name/container_name.
  -- All robots use MQTT transport. Wire format per robot (json or cbor).
  ---------------------------------------------------------------------------
  domains = {
    { name        = "surface_ops",
      description = "Surface operations",
      site        = "moonbase.alpha.surface_ops",
      cpu         = "cpu_01",
      container   = "surface_ops_planner",
      planner_data = "surface_ops_planner_data",
      robots = {
        { name = "rover_1", transport = "mqtt", wire_format = "json",
          robot_class = "lunar_rover",
          capabilities = {
            "init_check", "path_spline", "path_line", "path_wall",
            "path_rotate", "deliver_part", "paint_sample", "load_shipping",
            "pass_gate", "inspection_scan", "recharge", "idle",
          },
        },
        { name = "rover_2", transport = "mqtt", wire_format = "cbor",
          robot_class = "lunar_rover",
          capabilities = {
            "init_check", "path_spline", "path_line", "path_wall",
            "path_rotate", "deliver_part", "paint_sample", "load_shipping",
            "pass_gate", "inspection_scan", "recharge", "idle",
          },
        },
      },
    },

    { name        = "telemetry",
      description = "Telemetry collection and history",
      site        = "moonbase.alpha.telemetry",
      cpu         = "cpu_01",
      container   = "telemetry_collector",
      robots = {},
    },

  },
}
