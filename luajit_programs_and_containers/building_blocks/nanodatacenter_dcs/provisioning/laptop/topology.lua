-- =============================================================================
-- topology.lua -- Per-site placement of container instances onto CPUs.
--
-- Operator-edited. Holds:
--   site         : enterprise.<...>.dcs path used as the site KB name and
--                  baked into system.site.<SITE>.cpu.<id>.container.<inst>
--   master       : id of the CPU that runs system_control + kv_bridge
--   pg_connect   : where the construct script connects to load the master KB.
--                  Bootstrap-only; the runtime KB mirrors this under
--                  system.site.<SITE>.cpu.<master>.container.postgres.service.pg
--   cpus[<id>]   : per-CPU placement.
--                  properties: free-form metadata (hostname, role, ...)
--                  instances : { { name = "<unique>", def = "<def_name>",
--                                  params = {...} }, ... }
--                              name is globally unique; params are
--                              instance-specific overrides (zone, robot_id, ...)
--
-- v1 single-CPU: one CPU "cpu_01" hosts every instance; it is master.
-- =============================================================================

return {

  site   = "moonbase.alpha.dcs",
  master = "cpu_01",

  pg_connect = {
    host    = "localhost",
    port    = "5432",
    dbname  = "knowledge_base",
    user    = "gedgar",
  },

  cpus = {

    cpu_01 = {
      bit_index  = 0,    -- which bit in site-level ready_bits this CPU owns
      properties = { hostname = "localhost", role = "master" },
      instances = {
        -- infrastructure (pre-placed by laptop install scripts; DCS only
        -- starts/stops, never creates or removes). Names match the
        -- proven scripts under ~/knowledge_base_assembly/third_party_containers/.
        { name = "pg-vector",             def = "postgres" },
        { name = "nats-js-ram",           def = "nats" },
        { name = "mosquitto-ram-ws_main", def = "mosquitto" },
        { name = "kv-bridge",             def = "kv_bridge" },

        -- NOTE: system_control + node_control are NOT containers. They run
        -- as a single DCS host process (host_processes/dcs.lua) managed by
        -- build_output/<cpu>/start.sh. Not placed here.

        -- applications (none in v1)
      },
    },

  },

}
