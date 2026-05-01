-- =============================================================================
-- subsystems/cpu_logs.lua
--
-- Per-CPU KB_LOGs covering host resources + DCS-host-process internals.
-- Writers:
--   host-resource logs     -- fed by host_sampler (existing sampler reads)
--   DCS-internal logs      -- fed by the DCS host process itself (dcs.lua
--                              tick loop writes tick_duration_ms /
--                              ticks_per_burst / pg_roundtrip_ms)
--
-- Paths land at system.site.<SITE>.cpu.<cpu_id>.KB_LOG.<name>.
--
-- Seed rules target either existing catalog exceptions
-- (host_cpu_saturated, host_mem_saturated from cpu_exceptions) or new
-- exceptions declared inline here.
-- =============================================================================

return {

  install_cpu = function(ctx, cpu_id, cpu_cfg)
    local kb = ctx.kb

    --------------------------------------------------------------------
    -- Inline exception catalog for CPU-level operational alarms not
    -- already in topology.agent_exceptions.
    --------------------------------------------------------------------
    kb:add_exception("host_cpu_climbing", {
      type = "host_resource", instance = "host_sampler",
      description = "host CPU usage has a sustained upward trend",
      priority = 3,
    })
    kb:add_exception("host_mem_leak", {
      type = "host_resource", instance = "host_sampler",
      description = "host memory usage drifting upward (probable leak)",
      priority = 3,
    })
    kb:add_exception("host_mem_low", {
      type = "host_resource", instance = "host_sampler",
      description = "host free memory below critical threshold",
      priority = 1,
    })
    kb:add_exception("disk_high", {
      type = "host_resource", instance = "host_sampler",
      description = "disk utilization elevated (warning)",
      priority = 3,
    })
    kb:add_exception("disk_critical", {
      type = "host_resource", instance = "host_sampler",
      description = "disk utilization critical (<5% free)",
      priority = 1,
    })
    kb:add_exception("net_sampler_stalled", {
      type = "host_resource", instance = "host_sampler",
      description = "network counters not updating (sampler stuck)",
      priority = 4,
    })
    kb:add_exception("tick_overrun", {
      type = "dcs_internal", instance = "dcs_host",
      description = "chain-tree tick took longer than expected",
      priority = 3,
    })
    kb:add_exception("tick_degrading", {
      type = "dcs_internal", instance = "dcs_host",
      description = "chain-tree tick duration drifting upward",
      priority = 3,
    })
    kb:add_exception("dcs_tick_stalled", {
      type = "dcs_internal", instance = "dcs_host",
      description = "DCS host process no longer reporting ticks",
      priority = 1,
    })
    kb:add_exception("pg_slow", {
      type = "dcs_internal", instance = "dcs_host",
      description = "Postgres round-trip latency elevated",
      priority = 2,
    })
    kb:add_exception("pg_degrading", {
      type = "dcs_internal", instance = "dcs_host",
      description = "Postgres round-trip latency drifting upward",
      priority = 3,
    })

    --------------------------------------------------------------------
    -- Host-resource logs (6)
    --------------------------------------------------------------------

    -- Host-resource log rates: host_sampler writes at 60s cadence, so
    -- rings declare the matching 1/60 Hz. cap=60 covers 1 hour of raw
    -- history; longer windows hit tier-1 rollups.
    kb:add_log("host_cpu_pct", {
      kind = "operational", unit = "%",
      description = "host CPU percent usage",
      sample_cap = 60, expected_hz = 1/60,
      ma_short_s = 300, ma_long_s = 3600,
      default_window_s = 1800,
    }, function()
      kb:add_log_rule("saturated", {
        kind = "threshold", op = ">=", value = 95,
        target_exception = "host_cpu_saturated",
        cooldown_s = 60, description = "CPU at or above 95%",
      })
      kb:add_log_rule("climbing", {
        kind = "slope_trend",
        ma_source = "ma_long", slope_threshold = 0.05, consecutive_k = 5,
        target_exception = "host_cpu_climbing",
        cooldown_s = 300,
        description = "CPU drifting upward",
      })
    end)

    kb:add_log("host_mem_used_mb", {
      kind = "operational", unit = "mb",
      description = "host memory RSS used in MB",
      sample_cap = 120, expected_hz = 1/60,
      ma_short_s = 300, ma_long_s = 3600,
      default_window_s = 1800,
    }, function()
      kb:add_log_rule("high", {
        kind = "threshold", op = ">=", value = 6000,  -- tune per host
        target_exception = "host_mem_saturated",
        cooldown_s = 60, description = "memory usage exceeded hard ceiling",
      })
      kb:add_log_rule("leak", {
        kind = "slope_trend",
        ma_source = "ma_long", slope_threshold = 0.5, consecutive_k = 10,
        target_exception = "host_mem_leak",
        cooldown_s = 600,
        description = "memory drifting upward (probable leak)",
      })
    end)

    kb:add_log("host_mem_free_mb", {
      kind = "operational", unit = "mb",
      description = "host free memory in MB",
      sample_cap = 60, expected_hz = 1/60,
      ma_short_s = 300, ma_long_s = 3600,
      default_window_s = 1800,
    }, function()
      kb:add_log_rule("critical", {
        kind = "threshold", op = "<=", value = 128,
        target_exception = "host_mem_low",
        cooldown_s = 30, description = "free memory below 128 MB",
      })
    end)

    kb:add_log("net_rx_kbps", {
      kind = "operational", unit = "kbps",
      description = "host inbound network rate",
      sample_cap = 60, expected_hz = 1/60,
      ma_short_s = 300, ma_long_s = 3600,
      default_window_s = 1800,
    }, function()
      kb:add_log_rule("stalled", {
        kind = "sample_gap", gap_s = 180,  -- 3 missed samples at 60s
        target_exception = "net_sampler_stalled",
        cooldown_s = 120, description = "no network samples for >180s",
      })
    end)

    kb:add_log("net_tx_kbps", {
      kind = "operational", unit = "kbps",
      description = "host outbound network rate",
      sample_cap = 60, expected_hz = 1/60,
      ma_short_s = 300, ma_long_s = 3600,
      default_window_s = 1800,
    })  -- no custom rules; auto_health covers stalled sampler

    kb:add_log("disk_used_pct", {
      kind = "operational", unit = "%",
      description = "root filesystem utilization percent",
      sample_cap = 60, expected_hz = 1/60,
      ma_short_s = 300, ma_long_s = 3600,
      default_window_s = 1800,
    }, function()
      kb:add_log_rule("high", {
        kind = "threshold", op = ">=", value = 85,
        target_exception = "disk_high",
        cooldown_s = 300, description = "disk >= 85% full",
      })
      kb:add_log_rule("critical", {
        kind = "threshold", op = ">=", value = 95,
        target_exception = "disk_critical",
        cooldown_s = 60, description = "disk >= 95% full (critical)",
      })
    end)

    --------------------------------------------------------------------
    -- DCS-host-process internal logs (3)
    -- Paths: cpu.<id>.KB_LOG.{tick_duration_ms, ticks_per_burst, pg_roundtrip_ms}
    -- Writers: dcs.lua (per tick) and pg_connector.lua (per roundtrip)
    --------------------------------------------------------------------

    kb:add_log("tick_duration_ms", {
      kind = "operational", unit = "ms",
      description = "chain-tree burst runtime (dcs.lua run_ms)",
      sample_cap = 600, expected_hz = 1.0,
      ma_short_s = 60, ma_long_s = 900,
      default_window_s = 300,
    }, function()
      kb:add_log_rule("overrun", {
        kind = "threshold", op = ">=", value = 1500,
        target_exception = "tick_overrun",
        cooldown_s = 60,
        description = "tick exceeded 1500 ms (was 500 until 2026-04-24; " ..
                      "master CPU baseline with SAMPLE_CONTAINERS fires " ..
                      "600-900 ms per tick, so 500 was under the noise floor)",
      })
      kb:add_log_rule("degrading", {
        kind = "slope_trend",
        ma_source = "ma_long", slope_threshold = 0.5, consecutive_k = 5,
        target_exception = "tick_degrading",
        cooldown_s = 300,
        description = "tick duration drifting upward",
      })
    end)

    kb:add_log("ticks_per_burst", {
      kind = "operational", unit = "count",
      description = "chain-tree ticks per burst (dcs.lua tick_count delta)",
      -- Throttled to every 10th burst (~1/10 Hz). Value is near-constant
      -- (= 1 under normal ops), so fast sampling wastes writes.
      sample_cap = 360, expected_hz = 0.1,
      ma_short_s = 300, ma_long_s = 3600,
      default_window_s = 1800,
    }, function()
      kb:add_log_rule("stalled", {
        kind = "sample_gap", gap_s = 60,  -- ~6 missed samples
        target_exception = "dcs_tick_stalled",
        cooldown_s = 30, description = "DCS host process stopped reporting ticks",
      })
    end)

    --------------------------------------------------------------------
    -- node_control supervision timings (2)
    -- Writers: user_functions.lua RECONCILE_ASSIGNED_CONTAINERS (~0.1 Hz)
    --          + WATCHDOG_CHECK_ASSIGNED_CONTAINERS (~0.067 Hz).
    -- Track how long the supervision passes take, not just whether they
    -- happened. Growing reconcile/watchdog durations usually precede
    -- operational issues (docker socket slow, pg contention, flaky net).
    --------------------------------------------------------------------
    kb:add_exception("reconcile_slow", {
      type = "dcs_internal", instance = "node_control",
      description = "container reconcile pass taking longer than expected",
      priority = 3,
    })
    kb:add_exception("watchdog_slow", {
      type = "dcs_internal", instance = "node_control",
      description = "watchdog probe batch taking longer than expected",
      priority = 3,
    })

    kb:add_log("reconcile_check_ms", {
      kind = "operational", unit = "ms",
      description = "duration of one RECONCILE_ASSIGNED_CONTAINERS pass",
      sample_cap = 360, expected_hz = 0.1,
      ma_short_s = 300, ma_long_s = 3600,
      default_window_s = 1800,
    }, function()
      kb:add_log_rule("slow", {
        kind = "threshold", op = ">=", value = 500,
        target_exception = "reconcile_slow",
        cooldown_s = 300,
        description = "reconcile pass >= 500 ms",
      })
    end)

    kb:add_log("watchdog_probe_ms", {
      kind = "operational", unit = "ms",
      description = "duration of one WATCHDOG_CHECK_ASSIGNED_CONTAINERS pass",
      sample_cap = 240, expected_hz = 1/15,
      ma_short_s = 300, ma_long_s = 3600,
      default_window_s = 1800,
    }, function()
      kb:add_log_rule("slow", {
        kind = "threshold", op = ">=", value = 2000,
        target_exception = "watchdog_slow",
        cooldown_s = 300,
        description = "watchdog probe batch >= 2 s",
      })
    end)

    kb:add_log("pg_roundtrip_ms", {
      kind = "operational", unit = "ms",
      description = "sampled Postgres query roundtrip latency",
      -- Throttled to every 5th burst (~0.2 Hz). Still fast enough to
      -- detect pg stress within seconds; 5x less write pressure.
      sample_cap = 360, expected_hz = 0.2,
      ma_short_s = 60, ma_long_s = 900,
      default_window_s = 1800,
    }, function()
      kb:add_log_rule("slow", {
        kind = "threshold", op = ">=", value = 200,
        target_exception = "pg_slow",
        cooldown_s = 60, description = "pg roundtrip >= 200 ms",
      })
      kb:add_log_rule("degrading", {
        kind = "slope_trend",
        ma_source = "ma_long", slope_threshold = 0.5, consecutive_k = 10,
        target_exception = "pg_degrading",
        cooldown_s = 600,
        description = "pg latency drifting upward",
      })
    end)
  end,

}
