-- =============================================================================
-- subsystems/cpu_heartbeat.lua
--
-- Per-CPU heartbeat: bit_mask_table row where the 64-bit bit_mask column
-- holds a Unix-nanosecond timestamp (repurposed, not a bit flag set).
-- Each CPU's local agent writes its own; master reads to detect dead CPUs.
-- size=1 because the whole 64-bit value is the timestamp, not bits.
-- =============================================================================

return {

  install_cpu = function(ctx, cpu_id, cpu_cfg)
    local kb = ctx.kb
    kb:clear_bit_mask_flags()
    kb:add_bit_mask_flag("LSB", 0, "raw timestamp byte (unused as flag)")
    kb:create_bit_mask_entry("system", "heartbeat", 1, 0,
      "CPU " .. cpu_id .. " heartbeat (64-bit ns timestamp)")
  end,

}
