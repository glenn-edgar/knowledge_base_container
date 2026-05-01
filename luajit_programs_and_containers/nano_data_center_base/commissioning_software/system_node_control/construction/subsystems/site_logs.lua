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

    -- Observability-self-watch: analyzer heartbeats. If observability_01
    -- (or either analyzer process inside it) silently dies -- the failure
    -- mode that cost us a 2-day overnight run on 2026-04-22 -- the
    -- sample_gap rule here raises a priority-1 alarm. Without this, the
    -- thing that would raise the alarm is the thing that's dead, so the
    -- cluster appears healthy while data silently stops flowing.
    kb:add_exception("log_analyzer_stalled", {
      type        = "observability",
      instance    = "log_analyzer",
      description = "log_analyzer heartbeat has stopped (process dead or blocked)",
      priority    = 1,
    })
    kb:add_exception("exception_analyzer_stalled", {
      type        = "observability",
      instance    = "exception_analyzer",
      description = "exception_analyzer heartbeat has stopped (process dead or blocked)",
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

    -- Analyzer heartbeat signals. Each analyzer pushes wall-clock epoch
    -- once per tick (~1 Hz). sample_gap=60 means 60 s without a sample
    -- fires *_stalled. Value carries the most recent tick epoch so
    -- viewers can see "analyzer last checked in X seconds ago" directly.
    kb:add_log("log_analyzer_heartbeat", {
      kind             = "diagnostic", unit = "epoch_s",
      description      = "log_analyzer wall-clock heartbeat (written per tick)",
      sample_cap       = 120, expected_hz = 1.0,
      ma_short_s       = 60,  ma_long_s   = 300,
      default_window_s = 300,
      auto_health      = false,
    }, function()
      kb:add_log_rule("stalled", {
        kind             = "sample_gap", gap_s = 60,
        target_exception = "log_analyzer_stalled",
        cooldown_s       = 120,
        description      = "no log_analyzer heartbeat in 60 s",
      })
    end)

    kb:add_log("exception_analyzer_heartbeat", {
      kind             = "diagnostic", unit = "epoch_s",
      description      = "exception_analyzer wall-clock heartbeat (written per tick)",
      sample_cap       = 120, expected_hz = 1.0,
      ma_short_s       = 60,  ma_long_s   = 300,
      default_window_s = 300,
      auto_health      = false,
    }, function()
      kb:add_log_rule("stalled", {
        kind             = "sample_gap", gap_s = 60,
        target_exception = "exception_analyzer_stalled",
        cooldown_s       = 120,
        description      = "no exception_analyzer heartbeat in 60 s",
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
