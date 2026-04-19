-- =============================================================================
-- kb_log.lua -- Runtime read/write for KB_LOG nodes (Task 4).
--
-- KB_LOG is a nested header with children emitted by construct_log_store:
--   KB_LOG.<name>                                (header; props: kind, unit,
--                                                 sample_cap, expected_hz, ...)
--     KB_STREAM_FIELD.samples                    pre-allocated ring,
--                                                 payload: { ts, value }
--     KB_STATUS_FIELD.last_sample_ts             epoch s of last push
--     KB_STATUS_FIELD.last_value                 numeric value of last push
--     KB_STATUS_FIELD.sample_count_total         lifetime counter
--     KB_JSONB_FIELD.live_stats                  analyzer-maintained state
--     KB_RULE.<rule_id>                          nested per-rule headers
--
-- This module is used by:
--   - Writers (DCS host / controllers / apps): push_sample
--   - Analyzer (observability container): list_all, read_props,
--       read_samples_since, read/write_live_stats
--
-- DBI-based; matches kb_exception.lua style. For openresty+pgmoon
-- consumers, a pgmoon adapter can be added in a sibling module.
-- =============================================================================

local dkjson = require("dkjson")
local ptime  = require("posix_time")

local M = {}

---------------------------------------------------------------------------
-- SQL helpers (duplicated from kb_exception.lua to keep modules standalone)
---------------------------------------------------------------------------

local function escape_sql(s) return tostring(s):gsub("'", "''") end

local function exec(conn, sql)
  local sth, err = conn:prepare(sql)
  if not sth then return nil, "prepare: " .. tostring(err) end
  local ok, eerr = sth:execute()
  if not ok then sth:close(); return nil, "execute: " .. tostring(eerr) end
  sth:close()
  return true
end

local function fetch_all(conn, sql)
  local sth, err = conn:prepare(sql)
  if not sth then return nil, "prepare: " .. tostring(err) end
  local ok, eerr = sth:execute()
  if not ok then sth:close(); return nil, "execute: " .. tostring(eerr) end
  local rows = {}
  while true do
    local r = sth:fetch(true)
    if not r then break end
    local copy = {}; for k, v in pairs(r) do copy[k] = v end
    rows[#rows + 1] = copy
  end
  sth:close()
  return rows
end

local function fetch_one(conn, sql)
  local rows, err = fetch_all(conn, sql)
  if err then return nil, err end
  return rows and rows[1]
end

local function now_s() return math.floor(ptime.now_sec()) end

local function decode_jsonb(v)
  if not v or v == "" then return {} end
  if type(v) == "table" then return v end
  local t = dkjson.decode(tostring(v))
  return t or {}
end

local function set_status_child(conn, log_path, field, value)
  local path = log_path .. ".KB_STATUS_FIELD." .. field
  return exec(conn, string.format(
    "INSERT INTO knowledge_base_status (path, data) " ..
    "VALUES ('%s'::ltree, '%s'::json) " ..
    "ON CONFLICT (path) DO UPDATE SET data = EXCLUDED.data",
    escape_sql(path), escape_sql(dkjson.encode({ value = value }))))
end

local function read_status_child(conn, log_path, field)
  local path = log_path .. ".KB_STATUS_FIELD." .. field
  local row, err = fetch_one(conn, string.format(
    "SELECT data FROM knowledge_base_status WHERE path = '%s'::ltree",
    escape_sql(path)))
  if err then return nil, err end
  if not row then return nil end
  local d = decode_jsonb(row.data)
  return d.value
end

---------------------------------------------------------------------------
-- Discovery
---------------------------------------------------------------------------

--- List every KB_LOG in the KB.
--- Returns list of { path, properties } rows; caller filters by scope/kind.
function M.list_all(conn)
  local rows, err = fetch_all(conn,
    "SELECT path::text AS path, properties " ..
    "FROM knowledge_base WHERE label = 'KB_LOG'")
  if not rows then return nil, err end
  for _, r in ipairs(rows) do r.properties = decode_jsonb(r.properties) end
  return rows
end

--- List KB_LOGs under a subtree.
function M.list_under(conn, prefix_path)
  local rows, err = fetch_all(conn, string.format(
    "SELECT path::text AS path, properties " ..
    "FROM knowledge_base WHERE label = 'KB_LOG' AND path <@ '%s'::ltree",
    escape_sql(prefix_path)))
  if not rows then return nil, err end
  for _, r in ipairs(rows) do r.properties = decode_jsonb(r.properties) end
  return rows
end

---------------------------------------------------------------------------
-- Header props (build-time static config: kind, unit, sample_cap, etc.)
---------------------------------------------------------------------------

function M.read_props(conn, log_path)
  local row, err = fetch_one(conn, string.format(
    "SELECT properties FROM knowledge_base " ..
    "WHERE label = 'KB_LOG' AND path = '%s'::ltree",
    escape_sql(log_path)))
  if err then return nil, err end
  if not row then return nil end
  return decode_jsonb(row.properties)
end

---------------------------------------------------------------------------
-- Sample push (writer API)
---------------------------------------------------------------------------

--- Push one sample. Circular-buffer semantics: overwrites the oldest slot
--- (lowest recorded_at) at KB_STREAM_FIELD.samples with { ts, value }.
--- Also updates last_sample_ts, last_value, sample_count_total.
---
--- @param conn DBI connection
--- @param log_path ltree of the KB_LOG header (without the .KB_STREAM_FIELD.samples suffix)
--- @param value  number
--- @param ts     optional epoch s; defaults to now
--- @param extra  optional extra key/value pairs merged into the payload
function M.push_sample(conn, log_path, value, ts, extra)
  ts = ts or now_s()
  local stream_path = log_path .. ".KB_STREAM_FIELD.samples"

  local oldest, err = fetch_one(conn, string.format(
    "SELECT id FROM knowledge_base_stream " ..
    "WHERE path = '%s'::ltree " ..
    "ORDER BY recorded_at ASC LIMIT 1",
    escape_sql(stream_path)))
  if err then return nil, err end
  if not oldest then
    return nil, "no preallocated slots at " .. stream_path ..
                " (run slice_bootstrap after build_kb)"
  end

  local payload = { ts = ts, value = value }
  if extra then for k, v in pairs(extra) do payload[k] = v end end

  local ok, oerr = exec(conn, string.format(
    "UPDATE knowledge_base_stream SET data = '%s'::jsonb, " ..
    "recorded_at = NOW(), valid = TRUE WHERE id = %d",
    escape_sql(dkjson.encode(payload)), tonumber(oldest.id)))
  if not ok then return nil, oerr end

  -- Update status children. Failures here are non-fatal for the sample
  -- itself; they just leave last_*_ts stale. Return first error if any.
  set_status_child(conn, log_path, "last_sample_ts", ts)
  set_status_child(conn, log_path, "last_value",     value)

  local cur = read_status_child(conn, log_path, "sample_count_total") or 0
  set_status_child(conn, log_path, "sample_count_total", (tonumber(cur) or 0) + 1)

  return true
end

---------------------------------------------------------------------------
-- Sample reads (analyzer + dashboard)
---------------------------------------------------------------------------

--- Read samples with recorded_at > cursor_ts, oldest-first.
--- Analyzer calls this each tick with its remembered cursor.
--- @return rows sorted ascending by recorded_at
function M.read_samples_since(conn, log_path, cursor_ts, max_rows)
  max_rows = max_rows or 500
  local stream_path = log_path .. ".KB_STREAM_FIELD.samples"
  local sql
  if cursor_ts and cursor_ts ~= "" then
    sql = string.format(
      "SELECT recorded_at, data FROM knowledge_base_stream " ..
      "WHERE path = '%s'::ltree AND recorded_at > '%s' AND valid = TRUE " ..
      "ORDER BY recorded_at ASC LIMIT %d",
      escape_sql(stream_path), escape_sql(cursor_ts), max_rows)
  else
    sql = string.format(
      "SELECT recorded_at, data FROM knowledge_base_stream " ..
      "WHERE path = '%s'::ltree AND valid = TRUE " ..
      "ORDER BY recorded_at ASC LIMIT %d",
      escape_sql(stream_path), max_rows)
  end
  local rows, err = fetch_all(conn, sql)
  if not rows then return nil, err end
  for _, r in ipairs(rows) do r.data = decode_jsonb(r.data) end
  return rows
end

--- Read the newest N samples, newest-first. Useful for dashboard strip
--- charts rendering the tail of the ring.
function M.read_samples_tail(conn, log_path, n)
  n = n or 100
  local stream_path = log_path .. ".KB_STREAM_FIELD.samples"
  local rows, err = fetch_all(conn, string.format(
    "SELECT recorded_at, data FROM knowledge_base_stream " ..
    "WHERE path = '%s'::ltree AND valid = TRUE " ..
    "ORDER BY recorded_at DESC LIMIT %d",
    escape_sql(stream_path), n))
  if not rows then return nil, err end
  for _, r in ipairs(rows) do r.data = decode_jsonb(r.data) end
  return rows
end

---------------------------------------------------------------------------
-- live_stats (analyzer-maintained jsonb blob)
---------------------------------------------------------------------------

function M.read_live_stats(conn, log_path)
  local path = log_path .. ".KB_JSONB_FIELD.live_stats"
  local row, err = fetch_one(conn, string.format(
    "SELECT data FROM knowledge_base_status WHERE path = '%s'::ltree",
    escape_sql(path)))
  if err then return nil, err end
  if not row then return {} end
  local d = decode_jsonb(row.data)
  -- stored as {"value": {...}} via add_jsonb_field convention
  return d.value or d
end

function M.write_live_stats(conn, log_path, stats)
  local path = log_path .. ".KB_JSONB_FIELD.live_stats"
  return exec(conn, string.format(
    "INSERT INTO knowledge_base_status (path, data) " ..
    "VALUES ('%s'::ltree, '%s'::json) " ..
    "ON CONFLICT (path) DO UPDATE SET data = EXCLUDED.data",
    escape_sql(path),
    escape_sql(dkjson.encode({ value = stats or {} }))))
end

---------------------------------------------------------------------------
-- Status accessors (exposed for analyzer's health checks)
---------------------------------------------------------------------------

function M.read_last_sample_ts(conn, log_path)
  return read_status_child(conn, log_path, "last_sample_ts")
end

function M.read_last_value(conn, log_path)
  return read_status_child(conn, log_path, "last_value")
end

return M
