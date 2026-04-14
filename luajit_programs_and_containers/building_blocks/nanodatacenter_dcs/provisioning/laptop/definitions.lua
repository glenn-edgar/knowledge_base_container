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
--   ports         : { { host = X, cont = Y }, ... } or { Y, ... } (same)
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
    ports   = { { host = 1883, cont = 1883 } },
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
  -- Application (none in v1; example shape preserved)
  ----------------------------------------------------------------------

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
