-- =============================================================================
-- subsystems/container_runtime.lua
--
-- Per-container runtime-state schema. Dispatches by def.kind and
-- specific def names:
--
--   def.kind == 'infrastructure'     -- broker-style: health + events
--   inst.def == 'system_control'     -- master flags, ops RPC, exception ring
--   inst.def == 'node_control'       -- per-CPU control: operational, RPC,
--                                       welford state, assignments
--   def.kind == 'application'        -- app: health/started/restart + events
--
-- The service info-node is emitted for ALL kinds: it holds resolved port
-- records and is the handle node_control reads to issue `docker run`.
-- =============================================================================

return {

  install_container = function(ctx, cpu_id, cpu_cfg, inst, def)
    local kb = ctx.kb

    -- service endpoint (common to all kinds).
    kb:add_info_node("service",
      (def.kind == "infrastructure") and inst.name or "main",
      { type = def.kind },
      {
        host  = def.image and inst.name,
        ports = ctx.resolve_instance_ports(def, inst),
        cfg   = def.default_cfg or {},
      },
      "Service endpoint")

    -- Per-kind runtime-state fields.
    if def.kind == "infrastructure" then
      kb:add_status_field("health", {}, "broker health", { value = 0 })
      kb:add_stream_field("events", 64, "lifecycle events")

    elseif inst.def == "system_control" then
      kb:add_status_field("system_ready", {},
        "site-wide ready gate", { value = 0 })
      kb:add_stream_field("system_ready_transitions", 64, "ready flips")
      kb:add_status_field("master_heartbeat_ts", {},
        "last heartbeat (ms)", { value = 0 })
      kb:add_status_field("master_heartbeat_count", {},
        "heartbeat counter", { value = 0 })
      kb:add_rpc_server_field("ops_rpc", 16,
        "ops commands (teardown/reload/query)")
      kb:add_stream_field("resource_samples", 100, "resource sampler ring")
      kb:add_jsonb_field("welford_state", "welford",
        "running mean/variance for resources",
        { mean = 0, m2 = 0, n = 0 })
      kb:add_stream_field("exceptions", 200,
        "system_control exception ring")

    elseif inst.def == "node_control" then
      kb:add_status_field("operational",      {},
        "node up & apps healthy", { value = 0 })
      kb:add_status_field("heartbeat_ts",     {},
        "last node heartbeat (ms)", { value = 0 })
      kb:add_status_field("teardown_request", {},
        "teardown asked", { value = 0 })
      kb:add_status_field("stopped",          {},
        "all apps stopped", { value = 0 })
      kb:add_jsonb_field("assignments", "node_assignments",
        "list of app containers assigned to this CPU",
        { app_containers = {} })
      kb:add_rpc_server_field("ctrl_rpc", 8,
        "system_control -> node_control commands")
      kb:add_stream_field("resource_samples", 100,
        "container resources sampler ring")
      kb:add_jsonb_field("welford_state", "welford",
        "running mean/var for app resources",
        { mean = 0, m2 = 0, n = 0 })
      kb:add_stream_field("exceptions", 200,
        "node_control exception ring")

    elseif def.kind == "application" then
      kb:add_status_field("health",        {}, "app health",
        { value = 0 })
      kb:add_status_field("started_ts",    {}, "last start (ms)",
        { value = 0 })
      kb:add_status_field("restart_count", {}, "restart count",
        { value = 0 })
      kb:add_status_field("maintenance_until", {},
        "operator maintenance lease (epoch seconds; 0 = not in maintenance)",
        { value = 0 })
      kb:add_stream_field("events", 32, "app lifecycle events")
    end
  end,

}
