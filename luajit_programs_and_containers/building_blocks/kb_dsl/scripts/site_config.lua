--[[
  site_config.lua — Declarative site definition

  All CPUs, containers, services, domains, and robots in one place.
  Add a new mission planner / robot controller by adding entries here.
  The builder scripts loop over this data — no builder code changes needed.

  Physical model:
    CPU → containers → services
    CPU has a master flag; master holds Postgres.
    CPU controllers read their own subtree to know what to instantiate.
]]

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
          description = "NATS JetStream message bus",
          image       = "nanodatacenter/nats-js-ram:latest",
          services = {
            { name = "nats", protocol = "nats",
              data = { host = "0.0.0.0", port = 4222, ws_port = 9222 } },
            { name = "monitoring", protocol = "http",
              data = { host = "0.0.0.0", port = 8222 } },
          },
        },

        { name        = "mqtt_broker",
          type        = "infrastructure",
          description = "Mosquitto MQTT broker for edge robots",
          image       = "nanodatacenter/mosquitto-ram-ws:latest",
          services = {
            { name = "mqtt", protocol = "mqtt",
              data = { host = "0.0.0.0", port = 1883 } },
          },
        },

        { name        = "postgres",
          type        = "infrastructure",
          description = "Postgres 17 with pgvector — system KB store",
          image       = "pgvector/pgvector:pg17",
          services = {
            { name = "postgres", protocol = "postgres",
              data = { host = "0.0.0.0", port = 5432, dbname = "knowledge_base" } },
          },
        },

        { name        = "web_gateway",
          type        = "infrastructure",
          description = "OpenResty HTTP gateway",
          image       = "openresty/openresty:alpine",
          services = {
            { name = "http", protocol = "http",
              data = { host = "0.0.0.0", port = 8080 } },
          },
        },

        { name        = "kb_sidecar",
          type        = "service",
          description = "KB query sidecar — Postgres access over NATS",
          image       = "nanodatacenter/kb_sidecar:latest",
          services = {
            { name = "nats_client", protocol = "nats",
              data = { role = "client", subjects = "system_api.kb.query.*" } },
            { name = "pg_client", protocol = "postgres",
              data = { role = "client", dbname = "knowledge_base" } },
          },
        },

        { name        = "mqtt_bridge",
          type        = "service",
          description = "NATS-MQTT bridge for edge robots",
          image       = "nanodatacenter/mqtt_bridge:latest",
          services = {
            { name = "nats_client", protocol = "nats",
              data = { role = "client" } },
            { name = "mqtt_client", protocol = "mqtt",
              data = { role = "client" } },
          },
        },

        { name        = "fleet_manager",
          type        = "action_server",
          description = "Fleet management and scheduling",
          image       = "nanodatacenter/fleet:latest",
          sqlite_db   = "/data/fleet.db",
          params      = {},
          services = {
            { name = "nats_client", protocol = "nats",
              data = { role = "client" } },
          },
        },

        { name        = "telemetry_collector",
          type        = "action_server",
          description = "Telemetry collection and history",
          image       = "nanodatacenter/telemetry:latest",
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
          image       = "nanodatacenter/surface_ops:latest",
          sqlite_db   = "/data/surface_ops.db",
          params      = { max_concurrent_missions = 4,
                          heartbeat_interval_s     = 10 },
          services = {
            { name = "nats_client", protocol = "nats",
              data = { role = "client" } },
          },
        },

        { name        = "warehouse_ops_planner",
          type        = "action_server",
          description = "Warehouse operations mission planner",
          image       = "nanodatacenter/warehouse_ops:latest",
          sqlite_db   = "/data/warehouse_ops.db",
          params      = { max_concurrent_missions = 6,
                          heartbeat_interval_s     = 10 },
          services = {
            { name = "nats_client", protocol = "nats",
              data = { role = "client" } },
          },
        },
      },
    },
  },

  ---------------------------------------------------------------------------
  -- Domains: mission planner / robot controller logical groupings
  -- References containers by cpu_name/container_name.
  ---------------------------------------------------------------------------
  domains = {
    { name        = "surface_ops",
      description = "Surface operations",
      nats_prefix = "moonbase.alpha.services.robot_control.surface_ops",
      cpu         = "cpu_01",
      container   = "surface_ops_planner",
      robots = {
        { name = "rover_01", transport = "nats",  wire_format = "json",
          capabilities = { "navigate", "sample", "photograph" } },
        { name = "rover_02", transport = "mqtt",  wire_format = "cbor",
          capabilities = { "navigate", "drill" } },
      },
    },

    { name        = "fleet",
      description = "Fleet management and scheduling",
      nats_prefix = "moonbase.alpha.services.robot_control.fleet",
      cpu         = "cpu_01",
      container   = "fleet_manager",
      robots = {},
    },

    { name        = "telemetry",
      description = "Telemetry collection and history",
      nats_prefix = "moonbase.alpha.services.robot_control.telemetry",
      cpu         = "cpu_01",
      container   = "telemetry_collector",
      robots = {},
    },

    { name        = "warehouse_ops",
      description = "Warehouse logistics operations",
      nats_prefix = "moonbase.alpha.services.robot_control.warehouse_ops",
      cpu         = "cpu_01",
      container   = "warehouse_ops_planner",
      robots = {
        { name = "forklift_01", transport = "nats",  wire_format = "json",
          capabilities = { "navigate", "lift", "stack" } },
        { name = "forklift_02", transport = "nats",  wire_format = "json",
          capabilities = { "navigate", "lift", "stack" } },
        { name = "sorter_01",   transport = "mqtt",  wire_format = "cbor",
          capabilities = { "sort", "scan", "label" } },
      },
    },
  },
}
