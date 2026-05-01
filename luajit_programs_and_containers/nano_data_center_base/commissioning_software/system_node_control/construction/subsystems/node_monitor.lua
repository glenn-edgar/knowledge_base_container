-- =============================================================================
-- subsystems/node_monitor.lua
--
-- Per-CPU resource-sampler namespace: cpu.<id>.monitor.samples.{...}.
-- The chain-tree pushes one stream row per sample (kind = host/process/
-- container/trend_snapshot) as JSONB. trend_state holds last-computed
-- slopes; sampler updates it from COMPUTE_TRENDS.
--
-- 1440 samples = 24h at 60s cadence. Sized for ~1 day of raw history;
-- a downsampler can roll older data later.
-- =============================================================================

return {

  install_cpu = function(ctx, cpu_id, cpu_cfg)
    local kb = ctx.kb
    kb:with_header("monitor", "samples", { kind = "monitor" }, {},
      "Resource monitor namespace for CPU " .. cpu_id,
    function()
      kb:add_stream_field("samples", 1440,
        "resource sample ring (host/process/container/trend)")
      kb:add_jsonb_field("trend_state", "trend",
        "last-computed slopes per metric",
        {})
      kb:add_status_field("sampler_pid", {},
        "host process pid running the sampler",
        { value = 0 })
      kb:add_status_field("last_sample_ts", {},
        "wall-clock seconds of last successful sample",
        { value = 0 })
      kb:add_status_field("samples_dropped", {},
        "count of failed sample-write attempts",
        { value = 0 })
    end)
  end,

}
