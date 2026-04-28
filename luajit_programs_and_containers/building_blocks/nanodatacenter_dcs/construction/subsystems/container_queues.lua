-- =============================================================================
-- subsystems/container_queues.lua
--
-- Phase 6.4 container-layer RPC infrastructure (Option A, pg-backed).
--
-- Mirrors Phase 6.1's sync_queues with the container layer's verb set:
--   master -> container : PAUSE, RESUME, DRAIN, RESET_HINT, HEARTBEAT_ACK
--   container -> master : CONTAINER_READY, HEARTBEAT
--
-- Topology:
--   container_<name>_q          (master writes commands; container reads)
--   container_inbox_<cpu_id>_q  (containers on this CPU write status/HBs;
--                                master reads, dispatches by payload.name)
--
-- Only "node_control"-managed app containers participate. Infra containers
-- (pg-vector, nats-js-ram, mosquitto, kv-bridge) and control containers
-- (the dcs.lua processes themselves) stay broker-probed per design §7.1.
--
-- Also declares container_state_<name> KB rows for observability, mirroring
-- peer_state_<cpu_id> from Phase 6.1.
--
-- Memory: project_phase6_transport (Option A locked 2026-04-28),
-- feedback_phase6_handler_budget (cadence + budget rules carry over).
-- =============================================================================

return {

  install_site = function(ctx)
    local kb = ctx.kb

    -- Queues live under a CLASS header (matches sync_queues pattern).
    kb:add_header_node("CLASS", "container_queues_class", {}, {},
      "Phase 6.4 container-layer RPC transport queues")

    -- One inbox per CPU.
    for cpu_id, _ in pairs(ctx.TOPOLOGY.cpus) do
      kb:add_sync_queue({
        queue_name  = "container_inbox_" .. cpu_id .. "_q",
        description = "CPU " .. cpu_id ..
                      " container inbox (containers -> master): " ..
                      "CONTAINER_READY, HEARTBEAT.",
      })
    end

    -- Per-container outbox queue (master writes; container reads).
    -- Iterate topology here -- install_site is the only hook with the
    -- right header context for declaring queues that live at the
    -- queues_class scope.
    for cpu_id, cpu_cfg in pairs(ctx.TOPOLOGY.cpus) do
      for _, inst in ipairs(cpu_cfg.instances or {}) do
        local def = ctx.DEFINITIONS[inst.def]
        local managed = (def.kind == "infrastructure") and "manual"
                         or (inst.def == "kv_bridge" and "system_control"
                             or (def.kind == "control" and "self" or "node_control"))
        if managed == "node_control" then
          kb:add_sync_queue({
            queue_name  = "container_" .. inst.name .. "_q",
            description = "Container " .. inst.name ..
                          " inbox (master -> container): PAUSE, RESUME, " ..
                          "DRAIN, RESET_HINT, HEARTBEAT_ACK.",
          })
        end
      end
    end

    kb:leave_header_node("CLASS", "container_queues_class")

    -- Observability rows declared OUTSIDE the queues CLASS so they
    -- live flat under site (mirrors peer_state_<cpu_id>). This keeps
    -- the writeback path simple in container_rpc:_write_kb_state ->
    -- system.site.<SITE>.KB_STATUS_FIELD.container_state_<name>.
    for cpu_id, cpu_cfg in pairs(ctx.TOPOLOGY.cpus) do
      for _, inst in ipairs(cpu_cfg.instances or {}) do
        local def = ctx.DEFINITIONS[inst.def]
        local managed = (def.kind == "infrastructure") and "manual"
                         or (inst.def == "kv_bridge" and "system_control"
                             or (def.kind == "control" and "self" or "node_control"))
        if managed == "node_control" then
          kb:add_status_field(
            "container_state_" .. inst.name,
            {},
            "Phase 6.4 master-side observability for container " ..
            inst.name .. " (UNKNOWN | JOINING | ACTIVE | PAUSED | " ..
            "DRAINING | LOST). Updated by container_rpc scheduler tick.",
            {
              state             = "UNKNOWN",
              epoch             = 0,
              last_heartbeat_at = 0,
              last_verb_seen    = "",
              missed_acks       = 0,
              in_place_resets   = 0,
              drained           = 0,
              outbound          = 0,
              updated_at        = 0,
            })
        end
      end
    end

    kb:add_status_field(
      "container_rpc_budget_summary",
      {},
      "Phase 6.4 60s rolling container-layer RPC handler budget telemetry.",
      {
        max_ms        = 0,
        p95_ms        = 0,
        violations    = 0,
        warnings      = 0,
        drained_total = 0,
        sample_count  = 0,
        window_start  = 0,
      })
  end,

}
