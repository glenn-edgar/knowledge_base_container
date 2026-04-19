-- =============================================================================
-- subsystems/site_scalars.lua
--
-- Site-wide operator-tunable status fields that sit directly under
-- system.site.<SITE>.KB_STATUS_FIELD.<name>. Read/written by DCS
-- supervisors, the gateway, and the admin UI.
-- =============================================================================

return {

  install_site = function(ctx)
    local kb = ctx.kb
    kb:add_status_field("system_ready", {},
      "site-wide ready gate (1 = all CPUs synced+operational)",
      { value = 0 })
    kb:add_status_field("unmonitor_lease_default_s", {},
      "default unmonitor lease length in seconds (operator policy)",
      { value = 900 })
    kb:add_status_field("gateway_poll_interval_sec", {},
      "dcs_console gateway poll cadence in seconds (operator-tunable)",
      { value = 15 })
    kb:add_status_field("cluster_go", {},
      "sync-phase gate: 1 = all CPUs synced, handoff to operational",
      { value = 0 })
  end,

}
