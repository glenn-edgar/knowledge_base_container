-- =============================================================================
-- definitions.lua -- Container definition catalog (class blueprints).
--
-- One entry per container *type*. Instances reference these by name from
-- topology.lua. Code-versioned; edit means code review.
--
-- Schema per entry:
--   kind          : "infrastructure" | "application" | "host_process"
--   runtime       : container backend ("docker" today; "virtual" planned
--                   for pi-zero software-virtual containers)
--   image         : pre-built image name (infrastructure: never rebuilt)
--   build_ctx     : build-context dir (control/application: rebuilt)
--   entrypoint    : { argv... } the controller will exec
--   env_defaults  : { KEY = "value", ... } non-secret env
--   env_required  : { "POSTGRES_PASSWORD", ... } names resolved from operator
--                   secrets.env at docker run time (NEVER baked into image)
--   default_cfg   : opaque table written into the instance's service info_node
--   ports         : LEGACY host/cont list used by infrastructure defs.
--                   { { host = X, cont = Y }, ... } or { Y, ... } (same).
--                   Mutually exclusive with port_spec.
--   port_spec     : NEW -- per-image named-slot declaration for applications.
--                   Each slot describes an INTERNAL port the image binds on;
--                   topology.lua's instance-level `ports = { <slot> = <ext> }`
--                   supplies the external port numbers author-managed (no
--                   auto-assignment). Slot names are the stable identifier
--                   used by the gateway for routing and by the registry.
--                   Shape:
--                     port_spec = {
--                       <slot_name> = {
--                         internal    = 8080,          -- required, int
--                         protocol    = "tcp",         -- default "tcp"
--                         purpose     = "ui",          -- "ui" | "service" | ...
--                         description = "human text",  -- optional
--                       },
--                       ...
--                     }
--                   Construct-time: def-level uniqueness of `internal` across
--                   slots within a port_spec is enforced; per-CPU uniqueness of
--                   EXTERNAL ports across all instances (port_spec + legacy) is
--                   enforced; every slot must have a matching entry in the
--                   instance's topology-level `ports` table.
--   volumes       : { { host = "~/Postgres_Data", cont = "/var/lib/..." }, ... }
--                   host-side paths resolved against $HOME at run time
--   labels        : { key = value, ... } -- "nanodatacenter=true" added
--                   implicitly for docker stop-labelled semantics
--   restart_policy: docker restart policy ("no" default, "always" for pg)
--   cli_databases : { "name1", "name2", ... } per-app-CLI db files
--
-- Infrastructure entries match the existing planner-net containers.
-- =============================================================================

return {

  ----------------------------------------------------------------------
  -- Infrastructure (pre-built images; system_control starts/stops)
  ----------------------------------------------------------------------

  postgres = {
    kind          = "infrastructure",
    runtime       = "docker",
    image         = "pgvector/pgvector:pg17",
    ports         = { { host = 5432, cont = 5432 } },
    env_defaults  = {
      POSTGRES_USER = "gedgar",
      POSTGRES_DB   = "knowledge_base",
    },
    env_required  = { "POSTGRES_PASSWORD" },
    volumes       = {
      { host = "~/Postgres_Data/vector", cont = "/var/lib/postgresql/data" },
    },
    restart_policy = "always",
    default_cfg    = { db_name = "knowledge_base", user = "gedgar" },
  },

  nats = {
    kind    = "infrastructure",
    runtime = "docker",
    image   = "nanodatacenter/nats-js-ram:latest",
    ports   = {
      { host = 4222, cont = 4222 },
      { host = 9222, cont = 9222 },
    },
    restart_policy = "always",
  },

  mosquitto = {
    kind    = "infrastructure",
    runtime = "docker",
    image   = "nanodatacenter/mosquitto-ram-ws:latest",
    -- 1883 = MQTT, 9001 = MQTT-over-WebSocket (install script binds both).
    -- Declared here so construct-time per-CPU conflict detection catches
    -- collisions with application ports.
    ports   = { { host = 1883, cont = 1883 },
                { host = 9001, cont = 9001 } },
    restart_policy = "always",
  },

  kv_bridge = {
    kind    = "infrastructure",
    runtime = "docker",
    image   = "nanodatacenter/kv-bridge:latest",
    ports   = { { host = 8080, cont = 8080 } },
    restart_policy = "always",
  },

  -- NOTE: system_control + node_control are NOT containers. They run as
  -- one DCS host process (host_processes/dcs.lua) managed by
  -- build_output/<cpu>/start.sh. They don't appear in this catalog.

  ----------------------------------------------------------------------
  -- Applications
  ----------------------------------------------------------------------

  -- test_app: 4-process shell container used to exercise the full
  -- registration/deregistration path through node_control + gateway.
  -- Image is built out-of-tree in building_blocks/test_app/. Two
  -- supervised web processes bind internal ports 8080 + 8081; two
  -- supervised lua worker processes have no ports.
  test_app = {
    kind          = "application",
    runtime       = "docker",
    image         = "nanodatacenter/test-app:latest",
    restart_policy = "unless-stopped",
    port_spec = {
      exceptions_ui = {
        internal    = 8080,
        protocol    = "tcp",
        purpose     = "ui",
        description = "Exception aggregation viewer (shell)",
      },
      logs_ui = {
        internal    = 8081,
        protocol    = "tcp",
        purpose     = "ui",
        description = "Log aggregation viewer (shell)",
      },
    },
  },

  -- dcs_console: two-web-server pod. gateway slot will become the
  -- site-wide reverse proxy; admin slot will become the real operator
  -- UI (pg reads + htmx). Both start as shells; slot names are final so
  -- filling them in doesn't churn CONTAINER_REGISTRY rows.
  dcs_console = {
    kind          = "application",
    runtime       = "docker",
    image         = "nanodatacenter/dcs-console:latest",
    restart_policy = "unless-stopped",
    port_spec = {
      gateway = {
        internal    = 8080,
        protocol    = "tcp",
        purpose     = "ui",
        description = "Site-wide reverse proxy (shell)",
      },
      admin = {
        internal    = 8081,
        protocol    = "tcp",
        purpose     = "ui",
        description = "DCS operator admin UI (shell)",
      },
    },
  },

  -- robot_planner = {
  --   kind          = "application",
  --   runtime       = "docker",
  --   build_ctx     = "containers/robot_planner",
  --   entrypoint    = { "luajit", "planner.lua" },
  --   env_defaults  = { LOG_LEVEL = "info" },
  --   default_cfg   = { tick_rate_ms = 100 },
  --   cli_databases = { "planner" },
  --   restart_policy = "unless-stopped",
  -- },

}
