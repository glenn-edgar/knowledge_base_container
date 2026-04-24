-- rules.lua -- evaluate KB_RULE children against live_stats + sample value.
--
-- One evaluator per rule kind. On trip: record_fire on the rule, then
-- raise the target SYS_EXCEPTION. Cooldown + enabled + suppressed checks
-- are handled uniformly before the kind-specific evaluator runs.
--
-- target_exception in rule props is either:
--   * a bare name ("foo_unhealthy")       -> resolved as sibling of the log
--                                           at <log_scope>.SYS_EXCEPTION.<name>
--   * an absolute ltree path              -> used verbatim (already has dots)

local kb_rule  = require("kb_rule")
local kb_exc   = require("kb_exception")

local M = {}

---------------------------------------------------------------------------
-- target_exception path resolution
---------------------------------------------------------------------------

--- If target starts with a dot-component, treat as absolute ltree.
--- Otherwise, treat as bare name relative to the log's parent scope.
--- e.g. log_path = "system.site.X.cpu.cpu_01.KB_LOG.host_cpu_pct"
---      target_exception = "host_cpu_saturated"
---      resolved = "system.site.X.cpu.cpu_01.SYS_EXCEPTION.host_cpu_saturated"
local function resolve_target(log_path, target)
  if target:find("%.") then return target end   -- already absolute
  local scope = log_path:match("^(.-)%.KB_LOG%.[^%.]+$") or log_path
  return scope .. ".SYS_EXCEPTION." .. target
end

---------------------------------------------------------------------------
-- Per-kind evaluators
-- each returns (tripped_bool, observed_for_log, extra_details_tbl)
---------------------------------------------------------------------------

local evaluators = {}

evaluators.threshold = function(props, live, value)
  local op  = props.op
  local thr = tonumber(props.value)
  if not thr then return false end
  local tripped =
    (op == ">="  and value >= thr) or
    (op == "<="  and value <= thr) or
    (op == ">"   and value >  thr) or
    (op == "<"   and value <  thr) or
    (op == "=="  and value == thr) or
    (op == "!="  and value ~= thr)
  return tripped and true or false,
         value,
         { op = op, limit = thr, observed = value }
end

evaluators.z_score = function(props, live, value)
  local n_req = tonumber(props.min_samples) or 50
  local w     = live.welford or {}
  if (w.n or 0) < n_req then return false end
  local sd = math.sqrt((w.m2 or 0) / math.max(1, (w.n or 1) - 1))
  if sd <= 0 then return false end
  local z   = math.abs(value - (w.mean or 0)) / sd
  local k   = tonumber(props.z_threshold) or 3.0
  return z >= k,
         value,
         { z = z, mean = w.mean, stddev = sd, k = k }
end

evaluators.rate_of_change = function(props, live, value)
  local thr = tonumber(props.abs_threshold) or 0
  local d   = math.abs(live.dv_dt or 0)
  return d >= thr, value, { dv_dt = live.dv_dt, threshold = thr }
end

evaluators.slope_trend = function(props, live, value)
  local thr = tonumber(props.slope_threshold) or 0
  local slope = (live.slope or {}).value or 0
  return math.abs(slope) >= thr,
         value,
         { slope = slope, threshold = thr,
           ma_short = live.ma_short, ma_long = live.ma_long }
end

evaluators.envelope_drift = function(props, live, value)
  local thr = tonumber(props.drift_threshold) or 0
  local env = live.envelope or {}
  local mid = env.midpoint or 0
  local ref = (live.welford or {}).mean or 0
  local drift = math.abs(mid - ref)
  return drift >= thr,
         value,
         { midpoint = mid, reference_mean = ref, drift = drift, threshold = thr }
end

evaluators.cusum = function(props, live, value)
  local thr = tonumber(props.cusum_threshold) or 0
  local c = live.cusum or {}
  local mag = math.max(math.abs(c.pos or 0), math.abs(c.neg or 0))
  return mag >= thr,
         value,
         { cusum_pos = c.pos, cusum_neg = c.neg, threshold = thr }
end

-- sample_gap is evaluated by a separate pass (check_sample_gaps); here we
-- short-circuit to avoid firing on regular samples (which, by definition,
-- prove the source ISN'T silent).
evaluators.sample_gap = function(props, live, value)
  return false
end

---------------------------------------------------------------------------
-- Sample-driven evaluation (called per new sample per log)
---------------------------------------------------------------------------

-- Warmup gate: don't let rules fire until Welford has integrated
-- enough samples to produce a stable mean. Early samples are often
-- zeros or placeholder reads that skew mean + stddev dramatically
-- (observed: tick_duration_ms stddev 673 vs mean 212 for the first
-- few hundred samples because warmup hit zero-valued rows). Any rule
-- kind that reads `live.welford.mean` / `live.ma_*` / `live.stddev_*`
-- -- basically all statistical rules -- gets nonsense signals during
-- this window. Threshold rules that only compare the raw value to a
-- constant are safe; we exempt them.
local WELFORD_WARMUP_N = 10
local STATELESS_KINDS  = { threshold = true, sample_gap = true }

function M.evaluate(conn, log_path, rule_rows, live, value, ts, logger)
  for _, rule in ipairs(rule_rows) do
    local rule_path = rule.path
    local props     = rule.properties or {}
    local kind      = props.kind

    local evaluator = evaluators[kind]
    if not evaluator then
      -- unknown kind; skip silently (would have failed at construct time)
      goto continue
    end

    -- Warmup filter: skip statistical-rule kinds until welford.n > N.
    if not STATELESS_KINDS[kind] then
      local n = live and live.welford and tonumber(live.welford.n) or 0
      if n < WELFORD_WARMUP_N then goto continue end
    end

    local state    = kb_rule.read_state(conn, rule_path) or {}
    if not kb_rule.is_actionable(state) then goto continue end

    local tripped, observed, details = evaluator(props, live, value)
    local cooldown = tonumber(props.cooldown_s) or 60
    local target   = resolve_target(log_path, props.target_exception or "")

    if tripped then
      -- CONDITION TRIPPED: raise (respecting cooldown to avoid spam).
      if kb_rule.is_in_cooldown(state, cooldown, ts) then goto continue end
      kb_rule.record_fire(conn, rule_path, observed, details, ts)
      local err_msg = string.format(
        "rule %s (%s) tripped on %s: %s",
        (rule_path:match("KB_RULE%.(.+)$") or rule_path),
        kind, log_path,
        (details and details.op and (tostring(details.observed) .. " " .. details.op ..
                                      " " .. tostring(details.limit)))
          or string.format("observed=%s", tostring(observed)))
      kb_exc.raise(conn, target, {
        error         = err_msg,
        trigger_value = tostring(observed),
        limit_value   = tostring(details and (details.limit or details.threshold
                                              or details.k or "") or ""),
        source_path   = log_path,
      })
      if logger then
        logger(string.format("FIRED %s -> %s", rule_path, target))
      end
    else
      -- CONDITION CLEARED: auto-clear the target exception. Mirrors the
      -- sample_gap auto-clear (kb_exc.clear is idempotent: no-op on
      -- NORMAL, transitions UNACK_ACTIVE → RTN_UNACK etc). Without this
      -- every threshold/slope/rate_of_change/z_score/etc. alarm stays
      -- stuck UNACK_ACTIVE forever once raised, even after the metric
      -- returns to healthy -- the noise source behind container_hung
      -- and watchdog_slow lingering after boot transients.
      if target and target ~= "" then
        kb_exc.clear(conn, target)
      end
    end
    ::continue::
  end
end

---------------------------------------------------------------------------
-- Sample-gap evaluation (time-driven, called periodically per log)
---------------------------------------------------------------------------

function M.check_sample_gaps(conn, log_path, rule_rows, live, now, logger)
  local last_ts = tonumber(live.last_ts) or 0
  if last_ts == 0 then return end   -- no samples yet; nothing to be late

  for _, rule in ipairs(rule_rows) do
    local props = rule.properties or {}
    if props.kind ~= "sample_gap" then goto continue end

    local state    = kb_rule.read_state(conn, rule.path) or {}
    if not kb_rule.is_actionable(state) then goto continue end

    local gap_s = tonumber(props.gap_s) or 60
    local age   = now - last_ts
    local cooldown = tonumber(props.cooldown_s) or 60

    if age >= gap_s then
      -- GAP OPEN: fire (respecting cooldown to avoid re-raise spam).
      if kb_rule.is_in_cooldown(state, cooldown, now) then goto continue end
      kb_rule.record_fire(conn, rule.path, age, {
        last_ts = last_ts, now = now, age_s = age, gap_s = gap_s,
      }, now)
      local target = resolve_target(log_path, props.target_exception or "")
      kb_exc.raise(conn, target, {
        error = string.format("sample_gap: %d s since last sample (threshold %d s)",
                              age, gap_s),
        trigger_value = tostring(age),
        limit_value   = tostring(gap_s),
        source_path   = log_path,
      })
      if logger then
        logger(string.format("GAP %s -> %s (age=%ds)", rule.path, target, age))
      end
    else
      -- GAP CLOSED: auto-clear. kb_exc.clear is idempotent and transitions
      -- UNACK_ACTIVE → RTN_UNACK, ACK_ACTIVE → NORMAL, SHELVED → NORMAL,
      -- NORMAL → NORMAL (no-op). SCADA-correct semantics: once samples
      -- resume, the alarm reflects "condition cleared." Without this,
      -- transient outages (reboots, pg blips) leave stuck UNACK_ACTIVE
      -- alarms forever until operator acks + clears manually.
      local target = resolve_target(log_path, props.target_exception or "")
      kb_exc.clear(conn, target)
    end
    ::continue::
  end
end

return M
