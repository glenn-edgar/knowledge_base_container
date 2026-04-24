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

-- Wall-clock seconds. MUST be wall-clock (os.time), NOT monotonic
-- (ptime.now_sec returns CLOCK_MONOTONIC seconds since boot). The chart
-- viewer + alarm journal filter samples by `(os.time() - ts) <= window`;
-- a monotonic ts evicts everything.
local function now_s() return os.time() end

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

  local payload = { ts = ts, value = value }
  if extra then for k, v in pairs(extra) do payload[k] = v end end
  local payload_json = dkjson.encode(payload)

  -- Query 1 (was 2): UPDATE the oldest ring row in one round-trip using
  -- a subquery for the target id. Eliminates the separate SELECT id ...
  -- ORDER BY ASC step. Previously: 2 ops, now: 1.
  local ok, err = exec(conn, string.format(
    "UPDATE knowledge_base_stream " ..
    "   SET data = '%s'::jsonb, recorded_at = NOW(), valid = TRUE " ..
    " WHERE id = (SELECT id FROM knowledge_base_stream " ..
    "              WHERE path = '%s'::ltree " ..
    "              ORDER BY recorded_at ASC LIMIT 1)",
    escape_sql(payload_json), escape_sql(stream_path)))
  if not ok then return nil, err end

  -- Query 2 (was 2): upsert last_sample_ts + last_value in one
  -- multi-row INSERT ... ON CONFLICT. Values are constants so the DO
  -- UPDATE just takes EXCLUDED.data for both rows.
  local p_ts  = log_path .. ".KB_STATUS_FIELD.last_sample_ts"
  local p_val = log_path .. ".KB_STATUS_FIELD.last_value"
  local j_ts  = dkjson.encode({ value = ts })
  local j_val = dkjson.encode({ value = value })
  ok, err = exec(conn, string.format(
    "INSERT INTO knowledge_base_status (path, data) VALUES " ..
    "('%s'::ltree, '%s'::jsonb), " ..
    "('%s'::ltree, '%s'::jsonb) " ..
    "ON CONFLICT (path) DO UPDATE SET data = EXCLUDED.data",
    escape_sql(p_ts),  escape_sql(j_ts),
    escape_sql(p_val), escape_sql(j_val)))
  if not ok then return nil, err end

  -- Query 3 (was 2): atomic increment of sample_count_total. Previously
  -- a SELECT+UPDATE pair; now a single INSERT ... ON CONFLICT where the
  -- DO UPDATE body reads the existing row's counter and adds one.
  local p_cnt = log_path .. ".KB_STATUS_FIELD.sample_count_total"
  ok, err = exec(conn, string.format(
    "INSERT INTO knowledge_base_status (path, data) " ..
    "VALUES ('%s'::ltree, '{\"value\":1}'::jsonb) " ..
    "ON CONFLICT (path) DO UPDATE SET " ..
    "  data = jsonb_build_object('value', " ..
    "    COALESCE((knowledge_base_status.data->>'value')::bigint, 0) + 1)",
    escape_sql(p_cnt)))
  if not ok then return nil, err end

  -- Total: 3 pg ops (was 6). See 2026-04-24 work log for the Pi 4
  -- performance driver -- reduces observability pg load by ~50%.
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

-- live_stats is a KB_JSONB_FIELD child of each KB_LOG; KB_JSONB_FIELD
-- data lives in knowledge_base_document (key column: ltree), NOT in
-- knowledge_base_status (key column: path). Using the wrong table silently
-- succeeds (round-trip is internally consistent) but the log_web UI reads
-- from knowledge_base_document per the KB's field-type conventions, so
-- the Welford/MA/envelope panel stays empty.
function M.read_live_stats(conn, log_path)
  local path = log_path .. ".KB_JSONB_FIELD.live_stats"
  local row, err = fetch_one(conn, string.format(
    "SELECT data FROM knowledge_base_document WHERE ltree = '%s'::ltree",
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
    "INSERT INTO knowledge_base_document (ltree, data) " ..
    "VALUES ('%s'::ltree, '%s'::jsonb) " ..
    "ON CONFLICT (ltree) DO UPDATE SET data = EXCLUDED.data",
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
