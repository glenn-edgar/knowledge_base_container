-- =============================================================================
-- subsystems/container_logs.lua
--
-- Per-container KB_LOGs. Declared for every container instance
-- (application + infrastructure + control). Writer is log_manager
-- (translates monitor.samples into per-signal rings).
--
-- Paths: system.site.<SITE>.cpu.<cpu>.container.<inst>.KB_LOG.<name>
--
-- Seed rules target new exceptions declared inline below (per-container
-- resource alarms are distinct from host-level alarms).
-- =============================================================================

return {

  install_container = function(ctx, cpu_id, cpu_cfg, inst, def)
    local kb = ctx.kb

    --------------------------------------------------------------------
    -- Inline exception catalog for container-level operational alarms.
    --------------------------------------------------------------------
    kb:add_exception("container_cpu_hog", {
      type = "container_resource", instance = inst.name,
      description = "container CPU usage persistently high",
      priority = 3,
    })
    kb:add_exception("container_mem_leak", {
      type = "container_resource", instance = inst.name,
      description = "container memory drifting upward (probable leak)",
      priority = 2,
    })
    kb:add_exception("container_mem_ceiling", {
      type = "container_resource", instance = inst.name,
      description = "container memory usage at hard ceiling",
      priority = 2,
    })
    kb:add_exception("container_disk_spike", {
      type = "container_resource", instance = inst.name,
      description = "container disk I/O spike detected",
      priority = 4,
    })
    kb:add_exception("container_restarted", {
      type = "container_lifecycle", instance = inst.name,
      description = "container restarted at least once",
      priority = 3,
    })

    --------------------------------------------------------------------
    -- Log declarations (5 per container)
    --------------------------------------------------------------------

    kb:add_log("container_cpu_pct", {
      kind = "operational", unit = "%",
      description = "container CPU percent",
      sample_cap = 512, expected_hz = 1.0,
      ma_short_s = 60, ma_long_s = 900,
      default_window_s = 300,
    }, function()
      kb:add_log_rule("hog", {
        kind = "threshold", op = ">=", value = 90,
        target_exception = "container_cpu_hog",
        cooldown_s = 300,
        description = "container CPU >= 90%",
      })
    end)

    kb:add_log("container_mem_rss_mb", {
      kind = "operational", unit = "mb",
      description = "container RSS memory in MB",
      sample_cap = 512, expected_hz = 1.0,
      ma_short_s = 60, ma_long_s = 900,
      default_window_s = 300,
    }, function()
      kb:add_log_rule("leak", {
        kind = "slope_trend",
        ma_source = "ma_long", slope_threshold = 0.2, consecutive_k = 10,
        target_exception = "container_mem_leak",
        cooldown_s = 600,
        description = "container memory drifting upward",
      })
      kb:add_log_rule("ceiling", {
        kind = "threshold", op = ">=", value = 2048,  -- tune per def later
        target_exception = "container_mem_ceiling",
        cooldown_s = 120,
        description = "container memory at hard ceiling",
      })
    end)

    kb:add_log("container_disk_read_kbps", {
      kind = "operational", unit = "kbps",
      description = "container block-device read rate",
      sample_cap = 512, expected_hz = 1.0,
      ma_short_s = 60, ma_long_s = 900,
      default_window_s = 300,
    })  -- no custom rules; auto_health only

    kb:add_log("container_disk_write_kbps", {
      kind = "operational", unit = "kbps",
      description = "container block-device write rate",
      sample_cap = 512, expected_hz = 1.0,
      ma_short_s = 60, ma_long_s = 900,
      default_window_s = 300,
    }, function()
      kb:add_log_rule("spike", {
        kind = "rate_of_change",
        abs_threshold = 50000,
        target_exception = "container_disk_spike",
        cooldown_s = 60,
        description = "sudden disk write spike (>50 MB/s change)",
      })
    end)

    kb:add_log("container_restart_count", {
      kind = "operational", unit = "count",
      description = "cumulative container restart count",
      sample_cap = 256, expected_hz = 0.1,  -- slow-moving
      ma_short_s = 300, ma_long_s = 3600,
      default_window_s = 3600,
    }, function()
      kb:add_log_rule("restarted", {
        kind = "threshold", op = ">=", value = 1,
        target_exception = "container_restarted",
        cooldown_s = 60,
        description = "container has restarted at least once",
      })
    end)
  end,

}
