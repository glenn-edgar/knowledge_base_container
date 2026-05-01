-- =============================================================================
-- subsystems/readiness_sync.lua
--
-- One site-level bit_mask entry:
--   ready_bits -- CPU i sets bit i once synced+operational;
--                  master compares whole mask to (1<<N)-1 for system_ready.
--
-- Phase 6.1: cluster_sync_bits removed. Inter-CPU sync handshake is now
-- RPC-queue based (kb_sync_queue + sync_rpc.lua). ready_bits remains
-- because it encodes a different invariant (operational-phase setup
-- complete) that's orthogonal to sync.
-- =============================================================================

return {

  install_site = function(ctx)
    local kb = ctx.kb

    kb:clear_bit_mask_flags()
    for cpu_id, cpu in pairs(ctx.TOPOLOGY.cpus) do
      kb:add_bit_mask_flag("CPU_" .. cpu_id .. "_READY",
        cpu.bit_index,
        "CPU " .. cpu_id .. " synced+operational")
    end
    kb:create_bit_mask_entry("system", "ready_bits",
      ctx.CPU_COUNT, 0,
      "site-wide CPU readiness mask")
  end,

}
