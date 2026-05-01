-- rollups.lua -- tier-0 (raw ring) → tier-1 (1min) → tier-2 (1hour) →
-- tier-3 (1day) compaction for KB_LOGs. Uses PG-side aggregation via
-- INSERT...SELECT...GROUP BY. One query per log per tier boundary.
--
-- Design (Task 4):
--   operational logs -> all three tiers (1min emitted every minute,
--                       1hour aggregated hourly, 1day aggregated daily)
--   archival logs    -> tier-3 only (skip tier-1/tier-2)
--   diagnostic logs  -> no rollups (ring is the whole record)
--
-- Table: <database>_rollups. Schema per design memory.

local M = {}

local function escape_sql(s) return tostring(s):gsub("'", "''") end

local function exec(conn, sql)
  local sth, err = conn:prepare(sql)
  if not sth then return nil, "prepare: " .. tostring(err) end
  local ok, eerr = sth:execute()
  if not ok then sth:close(); return nil, "execute: " .. tostring(eerr) end
  sth:close()
  return true
end

---------------------------------------------------------------------------
-- tier-1: aggregate per-minute from raw samples ring
---------------------------------------------------------------------------

--- Emit/refresh tier-1 rows for the last ~2 minutes of raw samples.
--- ON CONFLICT DO UPDATE makes this idempotent — running it every tick
--- refreshes the current bucket as new samples arrive. At the turn of
--- the minute the previous bucket becomes final.
function M.compact_tier1(conn, rollups_table, log_path)
  local stream_path = log_path .. ".KB_STREAM_FIELD.samples"
  local sql = string.format([[
    INSERT INTO %s (tier, source_path, metric_name, bucket_start,
                    count, sum, sumsq, min_val, max_val)
    SELECT '1min',
           '%s'::ltree,
           'value',
           date_trunc('minute', recorded_at),
           COUNT(*)::bigint,
           COALESCE(SUM((data->>'value')::float), 0),
           COALESCE(SUM(POWER((data->>'value')::float, 2)), 0),
           COALESCE(MIN((data->>'value')::float), 0),
           COALESCE(MAX((data->>'value')::float), 0)
      FROM knowledge_base_stream
     WHERE path = '%s'::ltree
       AND valid = TRUE
       AND recorded_at >= NOW() - INTERVAL '2 minutes'
     GROUP BY date_trunc('minute', recorded_at)
    ON CONFLICT (tier, source_path, metric_name, bucket_start) DO UPDATE SET
      count   = EXCLUDED.count,
      sum     = EXCLUDED.sum,
      sumsq   = EXCLUDED.sumsq,
      min_val = EXCLUDED.min_val,
      max_val = EXCLUDED.max_val
  ]], rollups_table, escape_sql(log_path), escape_sql(stream_path))
  return exec(conn, sql)
end

---------------------------------------------------------------------------
-- tier-2 / tier-3: cascade from lower tier (merging min/max + summing)
---------------------------------------------------------------------------

local function cascade(conn, rollups_table, source_path, from_tier, to_tier, trunc_unit, window)
  local sql = string.format([[
    INSERT INTO %s (tier, source_path, metric_name, bucket_start,
                    count, sum, sumsq, min_val, max_val)
    SELECT '%s',
           source_path,
           metric_name,
           date_trunc('%s', bucket_start),
           SUM(count),
           SUM(sum),
           SUM(sumsq),
           MIN(min_val),
           MAX(max_val)
      FROM %s
     WHERE tier = '%s'
       AND source_path = '%s'::ltree
       AND bucket_start >= NOW() - INTERVAL '%s'
     GROUP BY source_path, metric_name, date_trunc('%s', bucket_start)
    ON CONFLICT (tier, source_path, metric_name, bucket_start) DO UPDATE SET
      count   = EXCLUDED.count,
      sum     = EXCLUDED.sum,
      sumsq   = EXCLUDED.sumsq,
      min_val = EXCLUDED.min_val,
      max_val = EXCLUDED.max_val
  ]],
    rollups_table, to_tier, trunc_unit,
    rollups_table, from_tier, escape_sql(source_path), window, trunc_unit)
  return exec(conn, sql)
end

function M.compact_tier2(conn, rollups_table, log_path)
  return cascade(conn, rollups_table, log_path, "1min", "1hour",
                 "hour", "2 hours")
end

function M.compact_tier3(conn, rollups_table, log_path)
  -- For archival logs: aggregate from tier-0 raw samples directly (they
  -- never had tier-1/tier-2 to cascade from).
  return cascade(conn, rollups_table, log_path, "1hour", "1day",
                 "day", "2 days")
end

--- Special path for archival logs: tier-3 straight from raw samples ring.
function M.compact_tier3_from_raw(conn, rollups_table, log_path)
  local stream_path = log_path .. ".KB_STREAM_FIELD.samples"
  local sql = string.format([[
    INSERT INTO %s (tier, source_path, metric_name, bucket_start,
                    count, sum, sumsq, min_val, max_val)
    SELECT '1day',
           '%s'::ltree,
           'value',
           date_trunc('day', recorded_at),
           COUNT(*)::bigint,
           COALESCE(SUM((data->>'value')::float), 0),
           COALESCE(SUM(POWER((data->>'value')::float, 2)), 0),
           COALESCE(MIN((data->>'value')::float), 0),
           COALESCE(MAX((data->>'value')::float), 0)
      FROM knowledge_base_stream
     WHERE path = '%s'::ltree
       AND valid = TRUE
       AND recorded_at >= NOW() - INTERVAL '2 days'
     GROUP BY date_trunc('day', recorded_at)
    ON CONFLICT (tier, source_path, metric_name, bucket_start) DO UPDATE SET
      count   = EXCLUDED.count,
      sum     = EXCLUDED.sum,
      sumsq   = EXCLUDED.sumsq,
      min_val = EXCLUDED.min_val,
      max_val = EXCLUDED.max_val
  ]], rollups_table, escape_sql(log_path), escape_sql(stream_path))
  return exec(conn, sql)
end

---------------------------------------------------------------------------
-- Retention trim (bounded row count per tier per (path, metric))
---------------------------------------------------------------------------

function M.trim(conn, rollups_table)
  -- Per-tier hard age cutoff. Cheap: tier/bucket_start is indexed.
  local retention = {
    ["1min"]  = "1 day",
    ["1hour"] = "7 days",
    ["1day"]  = "365 days",
  }
  for tier, age in pairs(retention) do
    local sql = string.format(
      "DELETE FROM %s WHERE tier = '%s' AND bucket_start < NOW() - INTERVAL '%s'",
      rollups_table, tier, age)
    local ok, err = exec(conn, sql)
    if not ok then return nil, "trim " .. tier .. ": " .. tostring(err) end
  end
  return true
end

return M
