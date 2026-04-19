-- =============================================================================
-- subsystems/site_logs.lua
--
-- Site-wide KB_LOGs with seed rules. These sit directly under
-- system.site.<SITE>.KB_LOG.<name>. Useful for cross-CPU signals:
-- cluster-wide sync latency, readiness fraction.
--
-- Each log's auto_health emits a sibling SYS_EXCEPTION.<name>_unhealthy
-- (pri 3) that fires when the log stops reporting samples.
--
-- Manual rules fire higher-priority exceptions when operational
-- thresholds are crossed. Exceptions targeted by rules are either
-- existing catalog entries (from topology.agent_exceptions) or
-- declared inline below.
--
-- Design-memory: project_dcs_task4_design.md.
-- =============================================================================

return {

  install_site = function(ctx)
    local kb = ctx.kb

    -- Inline exception catalog for site-level operational alarms.
    kb:add_exception("cluster_sync_slow", {
      type        = "sync",
      instance    = "system_control",
      description = "Cluster sync handshake exceeded latency threshold",
      priority    = 2,
    })
    kb:add_exception("cluster_not_ready", {
      type        = "sync",
      instance    = "system_control",
      description = "ready_bits fraction < 1.0 (one or more CPUs not ready)",
      priority    = 1,
    })

    -- Cluster sync latency: time from master's cluster_go set until all
    -- slaves acknowledge. Expected < 5s; > 5s = struggling.
    kb:add_log("cluster_sync_latency_ms", {
      kind             = "operational",
      description      = "Cluster sync handshake round-trip latency",
      unit             = "ms",
      sample_cap       = 256,
      expected_hz      = 1.0,
      ma_short_s       = 60,
      ma_long_s        = 900,
      default_window_s = 300,
    }, function()
      kb:add_log_rule("slow_sync", {
        kind             = "threshold",
        op               = ">=", value = 5000,
        target_exception = "cluster_sync_slow",
        cooldown_s       = 60,
        description      = "sync latency exceeded 5s",
      })
    end)

    -- Readiness bits completeness: fraction 0..1 of CPUs in ready state.
    kb:add_log("ready_bits_completeness", {
      kind             = "operational",
      description      = "Fraction of CPUs in ready state (1.0 = all up)",
      unit             = "fraction",
      sample_cap       = 256,
      expected_hz      = 1.0,
      ma_short_s       = 60,
      ma_long_s        = 900,
      default_window_s = 300,
    }, function()
      kb:add_log_rule("incomplete", {
        kind             = "threshold",
        op               = "<",  value = 1.0,
        target_exception = "cluster_not_ready",
        cooldown_s       = 30,
        description      = "one or more CPUs missing from ready mask",
      })
    end)
  end,

}
