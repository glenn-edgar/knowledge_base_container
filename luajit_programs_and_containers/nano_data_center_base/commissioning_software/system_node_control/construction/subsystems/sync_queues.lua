-- =============================================================================
-- subsystems/sync_queues.lua
--
-- Phase 6.1 sync-layer infrastructure:
--   1. One pg-backed sync queue per CPU:
--        master_q       (master's inbox; slaves write here)
--        cpu_<id>_q     (slave's inbox; master writes here)
--      Backed by kb_sync_queue (UNLOGGED <db>_sync_msg__<queue_name>).
--      Verbs: JOIN_REQ, JOIN_ACK, JOIN_CONFIRM, HEARTBEAT, HEARTBEAT_ACK,
--      RESET_HINT, DRAIN.
--
--   2. peer_state.<cpu_id> status fields under
--        system.site.<SITE>.KB_STATUS_FIELD.peer_state_<cpu_id>
--      Master writes these in its rpc_scheduler tick for observability;
--      slaves never read them. Correctness lives in master's RAM map.
--
-- Memory: project_kb_sync_queue, feedback_phase6_handler_budget,
-- project_phase6_transport.
-- =============================================================================

return {

  install_site = function(ctx)
    local kb = ctx.kb

    -- 1. Declare one sync queue per CPU. master_q + cpu_<id>_q for each
    -- non-master CPU. Queue names must match [a-z][a-z0-9_]*.
    kb:add_header_node("CLASS", "sync_queues_class", {}, {},
      "Phase 6.1 inter-CPU sync transport queues")

    kb:add_sync_queue({
      queue_name  = "master_q",
      description = "Master inbox (slaves -> master): JOIN_REQ, JOIN_CONFIRM, HEARTBEAT, DRAIN.",
    })

    for cpu_id, _ in pairs(ctx.TOPOLOGY.cpus) do
      if cpu_id ~= ctx.MASTER_CPU then
        kb:add_sync_queue({
          queue_name  = string.lower(cpu_id) .. "_q",
          description = "Slave " .. cpu_id .. " inbox (master -> slave): JOIN_ACK, HEARTBEAT_ACK, RESET_HINT.",
        })
      end
    end

    kb:leave_header_node("CLASS", "sync_queues_class")

    -- 2. peer_state observability rows. One per CPU (master too, for
    -- symmetry; master writes its own row via the scheduler heartbeat
    -- column so observability shows the master-side state at a glance).
    -- Initial value mirrors the in-RAM shape:
    --   { state = "UNKNOWN", epoch = 0, last_heartbeat_at = 0,
    --     last_verb_seen = "", drained = 0, outbound = 0 }
    for cpu_id, _ in pairs(ctx.TOPOLOGY.cpus) do
      kb:add_status_field(
        "peer_state_" .. cpu_id,
        {},
        "Phase 6.1 master-side observability for peer " .. cpu_id ..
        " (UNKNOWN | JOINING_SAW_REQ | ACTIVE | DRAINING). " ..
        "Updated by rpc_scheduler tick on master; correctness lives in RAM.",
        {
          state             = "UNKNOWN",
          epoch             = 0,
          last_heartbeat_at = 0,
          last_verb_seen    = "",
          drained           = 0,
          outbound          = 0,
          updated_at        = 0,
        })
    end

    -- 3. RPC scheduler budget summary (one row, written by master + slave).
    -- Reflects 60s rolling stats from the rpc_scheduler tick column.
    kb:add_status_field(
      "rpc_budget_summary",
      {},
      "Phase 6.1 60s rolling RPC handler budget telemetry. " ..
      "Updated by rpc_scheduler tick on every CPU.",
      {
        max_ms        = 0,
        p95_ms        = 0,
        violations    = 0,    -- handlers > 50ms in last 60s
        warnings      = 0,    -- handlers > 30ms in last 60s
        drained_total = 0,    -- verbs drained in last 60s
        sample_count  = 0,
        window_start  = 0,
      })
  end,

}
