-- =============================================================================
-- subsystems/readiness_sync.lua
--
-- Two site-level bit_mask entries:
--   ready_bits        -- CPU i sets bit i once synced+operational;
--                       master compares whole mask to (1<<N)-1 for system_ready.
--   cluster_sync_bits -- CPU i sets bit i once it has verified all 4 infra
--                       services reachable during the pre-op sync phase.
--                       Master flips cluster_go to 1 when mask is complete,
--                       else fires slave_never_joined on timeout.
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

    kb:clear_bit_mask_flags()
    for cpu_id, cpu in pairs(ctx.TOPOLOGY.cpus) do
      kb:add_bit_mask_flag("CPU_" .. cpu_id .. "_SYNCED",
        cpu.bit_index,
        "CPU " .. cpu_id .. " verified all 4 infra services reachable")
    end
    kb:create_bit_mask_entry("system", "cluster_sync_bits",
      ctx.CPU_COUNT, 0,
      "sync-phase mask: per-CPU infra-verified bits")
  end,

}
