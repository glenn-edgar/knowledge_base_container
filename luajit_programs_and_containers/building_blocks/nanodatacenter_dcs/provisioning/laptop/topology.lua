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

  -- Agent exception catalog. Construct script iterates per CPU and creates
  -- SYS_EXCEPTION rows (label=SYS_EXCEPTION, properties={type, instance,
  -- description}, status row default {status=true, ts=0, last_error="",
  -- trace_b64="", acknowledged=false}).
  --
  -- system_control exceptions are master-only (only the master CPU gets
  -- the rows). local_system_monitor and node_control exceptions apply to
  -- every CPU (each CPU gets its own copy of each exception path).
  --
  -- `instance` is per-occurrence context (broker name, container name, etc.);
  -- for agent-level faults we use the agent name itself.
  agent_exceptions = {
    system_control = {
      { name = "aggregator_timeout",
        type = "aggregator", instance = "system_control",
        description = "Timeout aggregating slave heartbeats / ready_bits" },
      { name = "slave_unreachable",
        type = "slave",      instance = "system_control",
        description = "A slave CPU has stopped heartbeating" },
    },
    local_system_monitor = {
      { name = "host_cpu_saturated",
        type = "host_resource", instance = "local_system_monitor",
        description = "Host CPU usage above threshold" },
      { name = "host_mem_saturated",
        type = "host_resource", instance = "local_system_monitor",
        description = "Host memory usage above threshold" },
      { name = "heartbeat_stuck",
        type = "self_check",    instance = "local_system_monitor",
        description = "local_system_monitor failed to advance its own heartbeat" },
    },
    node_control = {
      { name = "docker_socket_gone",
        type = "docker", instance = "node_control",
        description = "docker socket unreachable" },
      { name = "container_start_failed",
        type = "docker", instance = "node_control",
        description = "Failed to start an assigned container" },
      { name = "container_died",
        type = "docker", instance = "node_control",
        description = "An assigned container exited unexpectedly" },
    },
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
