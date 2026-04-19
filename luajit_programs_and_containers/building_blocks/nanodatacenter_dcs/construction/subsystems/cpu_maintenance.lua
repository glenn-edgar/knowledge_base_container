-- =============================================================================
-- subsystems/cpu_maintenance.lua
--
-- Per-CPU maintenance lease. Operator can pause an entire CPU's workload
-- in one click; node_control's transition handler treats every assignment
-- as if individually in maintenance while this value is > now(). Cleared
-- (value=0) on resume.
-- =============================================================================

return {

  install_cpu = function(ctx, cpu_id, cpu_cfg)
    ctx.kb:add_status_field("cpu_maintenance_until", {},
      "operator CPU-wide maintenance lease (epoch seconds; 0 = live)",
      { value = 0 })
  end,

}
