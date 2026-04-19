-- stats.lua -- update the live_stats table for one new sample.
--
-- live_stats jsonb shape (maintained by log_analyzer in-place):
--   { welford:      { n, mean, m2 },     whole-history running stats
--     ma_short:     EWMA over short window,
--     ma_long:      EWMA over long window,
--     ewma:         default-alpha EWMA,
--     stddev_1min:  rolling stddev (approximated from welford for v1),
--     envelope:     { max, min, midpoint } peak-detect with slow decay,
--     slope:        { value, r2, window_s } approximated as ma_short−ma_long,
--     cusum:        { pos, neg } cumulative deviation from welford mean,
--     dv_dt:        derivative since last sample,
--     last_ts:      epoch s of last sample (for dv_dt),
--     last_value:   last sample value (for dv_dt),
--     last_update_ts: epoch s of last stats update }
--
-- Designed to be cheap: O(1) per sample, no buffers, no allocations.

local M = {}

---------------------------------------------------------------------------
-- Primitives
---------------------------------------------------------------------------

local function welford_update(w, x)
  w.n    = (w.n or 0) + 1
  local delta  = x - (w.mean or 0)
  w.mean = (w.mean or 0) + delta / w.n
  local delta2 = x - w.mean
  w.m2   = (w.m2 or 0) + delta * delta2
end

local function welford_stddev(w)
  if (w.n or 0) < 2 then return 0 end
  return math.sqrt((w.m2 or 0) / (w.n - 1))
end

--- EWMA: alpha selected to approximate a simple-moving-average of window_n.
--- alpha = 2 / (window_n + 1) is the common conversion.
local function ewma(prev, x, window_n)
  if window_n <= 0 then return x end
  local a = 2.0 / (window_n + 1)
  if prev == nil or prev == 0 then return x end
  return a * x + (1 - a) * prev
end

---------------------------------------------------------------------------
-- Update: fold one new sample into live_stats
---------------------------------------------------------------------------

function M.update(live, ts, value, props)
  live.welford  = live.welford  or { n = 0, mean = 0, m2 = 0 }
  live.envelope = live.envelope or { max = value, min = value, midpoint = value }
  live.cusum    = live.cusum    or { pos = 0, neg = 0 }

  -- Whole-history stats
  welford_update(live.welford, value)

  -- EWMA short + long. `expected_hz * window_s` = approximate #samples per window.
  local hz        = tonumber(props.expected_hz) or 1.0
  local short_n   = math.max(1, math.floor((tonumber(props.ma_short_s) or 60)  * hz))
  local long_n    = math.max(1, math.floor((tonumber(props.ma_long_s)  or 900) * hz))
  live.ma_short = ewma(live.ma_short, value, short_n)
  live.ma_long  = ewma(live.ma_long,  value, long_n)
  live.ewma     = ewma(live.ewma,     value, 10)   -- default alpha ~0.18

  -- Envelope: peak-hold with slow decay. Max sags toward value when below;
  -- min rises toward value when above. Decay rate arbitrary but small.
  local env = live.envelope
  if value > env.max then env.max = value
  else env.max = env.max - 0.001 * (env.max - value) end
  if value < env.min then env.min = value
  else env.min = env.min + 0.001 * (value - env.min) end
  env.midpoint = (env.max + env.min) / 2

  -- dV/dt
  if live.last_ts and live.last_ts ~= 0 then
    local dt = ts - live.last_ts
    if dt > 0 then
      live.dv_dt = (value - (live.last_value or value)) / dt
    end
  end
  live.last_ts    = ts
  live.last_value = value

  -- Slope approximation: (ma_short − ma_long) normalized by time gap.
  -- Proper rolling linear regression is deferred — this captures the sign
  -- and rough magnitude well enough for drift-under-oscillation detection.
  local win_gap = math.max(1, (tonumber(props.ma_long_s) or 900)
                            - (tonumber(props.ma_short_s) or 60))
  live.slope = live.slope or { value = 0, r2 = 0, window_s = win_gap }
  live.slope.value    = ((live.ma_short or 0) - (live.ma_long or 0)) / win_gap
  live.slope.window_s = win_gap

  -- CUSUM against welford mean (symmetric deviation accumulator)
  local dev = value - (live.welford.mean or 0)
  live.cusum.pos = math.max(0, (live.cusum.pos or 0) + dev)
  live.cusum.neg = math.min(0, (live.cusum.neg or 0) + dev)

  -- stddev_1min: approximated from welford for v1. Proper 1-min rolling
  -- stddev requires a time-windowed buffer; acceptable trade-off here.
  live.stddev_1min  = welford_stddev(live.welford)
  live.stddev_10min = live.stddev_1min   -- same approximation

  live.last_update_ts = ts
end

return M
