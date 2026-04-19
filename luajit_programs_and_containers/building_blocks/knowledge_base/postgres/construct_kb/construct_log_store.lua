--[[
  construct_log_store.lua

  SCADA-style observability: KB_LOG + KB_RULE nested-header emitters,
  plus the rollups table for tier-0 → tier-1 → tier-2 → tier-3 aggregates.

  One kb:add_log(name, opts) call emits:
    KB_LOG.<name>                              (header)
      props: kind, description, unit, sample_cap, expected_hz,
             ma_short_s, ma_long_s, default_window_s, auto_health
      children: KB_STREAM_FIELD.samples
                KB_STATUS_FIELD.last_sample_ts / last_value /
                                sample_count_total
                KB_JSONB_FIELD.live_stats

  Plus, for `kind='operational'` + `auto_health=true` (defaults):
    SYS_EXCEPTION.<name>_unhealthy            (sibling of the KB_LOG)
    KB_LOG.<name>.KB_RULE.__health_gap        (nested rule)

  One kb:add_log_rule(rule_id, opts) call, positioned inside a KB_LOG
  header, emits:
    KB_RULE.<rule_id>                          (header)
      props: kind, kind-specific params, target_exception, cooldown_s
      children: KB_STATUS_FIELD.enabled / suppressed / suppressed_until /
                                fire_count / last_fired_ts /
                                last_fired_value / last_fired_details

  This module also creates the `<database>_rollups` table at construction
  time (one per KB database, mirrors the _status / _stream pattern).

  See project_dcs_task4_design.md for the locked design.
]]

local C = require("construct_driver_common")

local Construct_Log_Store = {}
Construct_Log_Store.__index = Construct_Log_Store

local VALID_KINDS = { operational = true, archival = true, diagnostic = true }
local VALID_RULE_KINDS = {
  z_score = true, threshold = true, rate_of_change = true,
  slope_trend = true, envelope_drift = true, cusum = true, sample_gap = true,
}

---------------------------------------------------------------------------
-- Constructor
---------------------------------------------------------------------------

function Construct_Log_Store.new(conn, cdt, database, upload_flag)
  local self = setmetatable({}, Construct_Log_Store)
  self.conn        = conn
  self.cdt         = cdt
  self.database    = database
  self.upload_flag = upload_flag or false
  self.t_rollups   = database .. "_rollups"

  if not self.upload_flag then
    self:_setup_schema()
  end

  return self
end

---------------------------------------------------------------------------
-- Schema setup (rollups table — one per KB database)
---------------------------------------------------------------------------

function Construct_Log_Store:_setup_schema()
  C.exec(self.conn, "CREATE EXTENSION IF NOT EXISTS ltree;")
  C.exec(self.conn, string.format("DROP TABLE IF EXISTS %s CASCADE;",
    C.quote_ident(self.t_rollups)))

  C.exec(self.conn, string.format([[
    CREATE TABLE %s (
      tier          TEXT NOT NULL CHECK (tier IN ('1min','1hour','1day')),
      source_path   LTREE NOT NULL,
      metric_name   TEXT NOT NULL,
      bucket_start  TIMESTAMPTZ NOT NULL,
      count         BIGINT NOT NULL,
      sum           DOUBLE PRECISION NOT NULL,
      sumsq         DOUBLE PRECISION NOT NULL,
      min_val       DOUBLE PRECISION NOT NULL,
      max_val       DOUBLE PRECISION NOT NULL,
      PRIMARY KEY (tier, source_path, metric_name, bucket_start)
    )
  ]], C.quote_ident(self.t_rollups)))

  C.exec(self.conn, string.format(
    "CREATE INDEX %s ON %s (source_path, metric_name, bucket_start)",
    C.quote_ident("idx_" .. self.t_rollups .. "_path_metric_ts"),
    C.quote_ident(self.t_rollups)))

  C.exec(self.conn, string.format(
    "CREATE INDEX %s ON %s USING GIST(source_path)",
    C.quote_ident("idx_" .. self.t_rollups .. "_path_gist"),
    C.quote_ident(self.t_rollups)))

  -- Per-table autovacuum: rollups are insert-heavy for tier='1min'
  -- and delete-heavy for retention trims. Tune tighter than defaults.
  C.exec(self.conn, string.format([[
    ALTER TABLE %s SET (
      autovacuum_vacuum_scale_factor  = 0.05,
      autovacuum_analyze_scale_factor = 0.05
    )
  ]], C.quote_ident(self.t_rollups)))
end

---------------------------------------------------------------------------
-- kb:add_log helper
---------------------------------------------------------------------------

--- Emit a KB_LOG nested-header + children, and (for operational logs with
--- auto_health) the companion SYS_EXCEPTION + __health_gap rule.
---
--- @param name string    log identifier (becomes header name)
--- @param opts table with fields:
---   kind                string   'operational' (default) | 'archival' | 'diagnostic'
---   description         string   human-readable
---   unit                string   'ms' | '%' | 'mb' | ...
---   sample_cap          int      ring size (default 512)
---   expected_hz         double   nominal sample rate; drives auto-health gap
---   ma_short_s          int      short-MA window (default 60)
---   ma_long_s           int      long-MA window (default 900)
---   default_window_s    int      UI initial time range (kind-dependent default)
---   auto_health         bool     default true for operational; else false
--- @param body function    optional; runs inside the KB_LOG header so the
---                         caller can add operator rules via add_log_rule.
---                         Fires AFTER standard children are emitted and
---                         BEFORE the auto_health rule.
function Construct_Log_Store:add_log(name, opts, body)
  assert(type(name) == "string" and #name > 0, "log name required")
  opts = opts or {}

  local kind = opts.kind or "operational"
  assert(VALID_KINDS[kind],
    "kind must be one of: operational, archival, diagnostic")

  local sample_cap       = opts.sample_cap or 512
  local expected_hz      = opts.expected_hz or 1.0
  local ma_short_s       = opts.ma_short_s or 60
  local ma_long_s        = opts.ma_long_s  or 900
  local default_window_s = opts.default_window_s
                          or (kind == "archival"   and 30 * 86400
                              or kind == "diagnostic" and 300
                              or 300)
  local auto_health = opts.auto_health
  if auto_health == nil then auto_health = (kind == "operational") end

  local description = opts.description or ""

  local header_props = {
    kind             = kind,
    unit             = opts.unit or "",
    sample_cap       = sample_cap,
    expected_hz      = expected_hz,
    ma_short_s       = ma_short_s,
    ma_long_s        = ma_long_s,
    default_window_s = default_window_s,
    auto_health      = auto_health,
  }

  local cdt = self.cdt

  cdt:with_header("KB_LOG", name, header_props, {}, description, function()
    -- Raw sample ring (size = sample_cap). Payload is jsonb { ts, value }.
    cdt:add_stream_field("samples", sample_cap,
      "raw sample ring for " .. name)

    -- Per-log state
    cdt:add_status_field("last_sample_ts", {},
      "epoch s of last sample write",
      { value = 0 })
    cdt:add_status_field("last_value", {},
      "numeric value of last sample",
      { value = 0 })
    cdt:add_status_field("sample_count_total", {},
      "lifetime samples pushed",
      { value = 0 })

    -- Analyzer-maintained stats (welford, moving averages, envelope,
    -- slope, cusum). Dumped as a single jsonb blob for cheap read/write.
    cdt:add_jsonb_field("live_stats", "log_stats",
      "analyzer-maintained running stats (welford, MA, envelope, slope, cusum)",
      {
        welford        = { n = 0, mean = 0, m2 = 0 },
        ma_short       = 0,
        ma_long        = 0,
        ewma           = 0,
        stddev_1min    = 0,
        stddev_10min   = 0,
        envelope       = { max = 0, min = 0, midpoint = 0 },
        slope          = { value = 0, r2 = 0, window_s = ma_long_s },
        cusum          = { pos = 0, neg = 0 },
        dv_dt          = 0,
        last_update_ts = 0,
      })

    -- Caller-supplied rules (run while KB_LOG header is still open so
    -- add_log_rule calls land at the correct nested path).
    if body then body() end

    -- auto_health: seed a sample_gap rule targeting a sibling
    -- SYS_EXCEPTION. target_exception is a BARE NAME; the analyzer
    -- resolves it to the sibling SYS_EXCEPTION at the log's scope.
    if auto_health then
      local gap_s = math.max(1, math.floor(3.0 / expected_hz))
      self:_add_rule_internal(cdt, "__health_gap", {
        kind             = "sample_gap",
        gap_s            = gap_s,
        target_exception = name .. "_unhealthy",
        cooldown_s       = 60,
        description      = "auto: log source silent > " .. gap_s .. "s",
      })
    end
  end)

  -- Emit the companion SYS_EXCEPTION as a SIBLING of the KB_LOG header
  -- (after the with_header closure pops). This keeps it queryable
  -- alongside operational alarms.
  if auto_health then
    cdt:add_exception(name .. "_unhealthy", {
      type        = "log_health",
      instance    = name,
      description = "Log '" .. name .. "' stopped reporting samples " ..
                    "(no writes for > 3 / expected_hz seconds)",
      priority    = 3,
      response_procedure = "",
    })
  end

  return { added = name, auto_health = auto_health }
end

---------------------------------------------------------------------------
-- kb:add_log_rule helper
---------------------------------------------------------------------------

--- Add a rule to the KB_LOG that is currently the open header.
--- Must be called from within a kb:with_header("KB_LOG", ...) body, OR
--- the caller must have manually positioned the path stack on a KB_LOG.
---
--- @param rule_id string   unique-within-this-KB_LOG identifier
--- @param opts    table    per-kind params + target_exception + cooldown_s
function Construct_Log_Store:add_log_rule(rule_id, opts)
  return self:_add_rule_internal(self.cdt, rule_id, opts)
end

function Construct_Log_Store:_add_rule_internal(cdt, rule_id, opts)
  assert(type(rule_id) == "string" and #rule_id > 0, "rule_id required")
  opts = opts or {}

  local kind = opts.kind or error("rule kind required")
  assert(VALID_RULE_KINDS[kind],
    "unknown rule kind: " .. kind ..
    " (valid: z_score, threshold, rate_of_change, slope_trend, " ..
    "envelope_drift, cusum, sample_gap)")

  assert(type(opts.target_exception) == "string" and #opts.target_exception > 0,
    "rule target_exception required")

  local props = {
    kind              = kind,
    target_exception  = opts.target_exception,
    cooldown_s        = opts.cooldown_s or 60,
    description       = opts.description or "",
    -- Kind-specific (only the relevant ones populate per rule; we copy
    -- all possible keys to the jsonb props for uniform read).
    pattern           = opts.pattern,        -- text_pattern would be here
                                              -- (deferred from v1 rule set)
    metric            = opts.metric,         -- (when a log emits multiple)
    z_threshold       = opts.z_threshold,
    min_samples       = opts.min_samples,
    op                = opts.op,
    value             = opts.value,
    rate_count        = opts.rate_count,
    rate_window_s     = opts.rate_window_s,
    severity_filter   = opts.severity_filter,
    gap_s             = opts.gap_s,          -- sample_gap
    ma_source         = opts.ma_source,      -- slope_trend: 'ma_short' | 'ma_long'
    slope_threshold   = opts.slope_threshold,
    consecutive_k     = opts.consecutive_k,
    abs_threshold     = opts.abs_threshold,  -- rate_of_change
    target_value      = opts.target_value,   -- cusum target
    cusum_threshold   = opts.cusum_threshold,
    drift_threshold   = opts.drift_threshold,-- envelope_drift
  }

  cdt:with_header("KB_RULE", rule_id, props, {},
                  props.description, function()
    cdt:add_status_field("enabled", {},
      "operator toggle: false disables rule without rebuild",
      { value = true })
    cdt:add_status_field("suppressed", {},
      "rule-level shelve (operator rationalization mute)",
      { value = false })
    cdt:add_status_field("suppressed_until", {},
      "epoch s when suppression auto-clears; 0 = permanent",
      { value = 0 })
    cdt:add_status_field("fire_count", {},
      "lifetime trips",
      { value = 0 })
    cdt:add_status_field("last_fired_ts", {},
      "epoch s of most recent trip",
      { value = 0 })
    cdt:add_status_field("last_fired_value", {},
      "observation that triggered last trip",
      { value = "" })
    cdt:add_status_field("last_fired_details", {},
      "jsonb-safe drill payload for UI",
      { value = "" })
  end)

  return { added = rule_id, kind = kind }
end

---------------------------------------------------------------------------
-- check_installation — orphan-sweep rollup rows for deleted KB_LOGs
---------------------------------------------------------------------------

function Construct_Log_Store:check_installation()
  -- Drop rollup rows whose source_path is no longer a KB_LOG in the KB.
  -- Cheap on install; rare in practice (only when a log is removed from
  -- the DSL between builds).
  C.exec(self.conn, string.format([[
    DELETE FROM %s r
    WHERE NOT EXISTS (
      SELECT 1 FROM %s k
       WHERE k.label = 'KB_LOG' AND k.path = r.source_path
    )
  ]], C.quote_ident(self.t_rollups), C.quote_ident(self.database)))
end

return Construct_Log_Store
