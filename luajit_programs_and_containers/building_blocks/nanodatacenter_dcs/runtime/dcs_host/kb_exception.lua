-- =============================================================================
-- kb_exception.lua -- Exception runtime library.
--
-- Per the data model (see project_dcs_data_model.md):
--   * Schema row in `knowledge_base` main table at construct time:
--       label = 'SYS_EXCEPTION'
--       properties = { type, instance, description }
--   * Runtime row in `knowledge_base_status` at the same path:
--       data = { status, ts, last_error, trace_b64, acknowledged }
--
-- This library is read/write of the runtime status row only. The schema
-- row is created by the construct script (add_exception in topology
-- iteration) and is not touched at runtime.
--
-- Operations:
--   log_exception(conn, path, msg, trace_b64)
--   log_exception_status(conn, path, status_bool, msg, trace_b64)
--   ack_exception(conn, path)
--   clear_exception(conn, path)
--   mute_existing_on_boot(conn)
--
-- All operations take a DBI conn (autocommit on); all return (ok, err).
-- Path is the full ltree string the schema row was declared at.
-- =============================================================================

local dkjson = require("dkjson")
local ptime  = require("posix_time")

local M = {}

---------------------------------------------------------------------------
-- helpers
---------------------------------------------------------------------------

local function escape_sql(s)
  return tostring(s):gsub("'", "''")
end

local function exec(conn, sql)
  local sth, err = conn:prepare(sql)
  if not sth then return nil, "prepare: " .. tostring(err) end
  local ok, eerr = sth:execute()
  if not ok then return nil, "execute: " .. tostring(eerr) end
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
    local row = sth:fetch(true)
    if not row then break end
    local copy = {}
    for k, v in pairs(row) do copy[k] = v end
    rows[#rows + 1] = copy
  end
  sth:close()
  return rows
end

local function fetch_one(conn, sql)
  local rows, err = fetch_all(conn, sql)
  if not rows then return nil, err end
  return rows[1]
end

local function now_ns() return math.floor(ptime.now_sec() * 1e9) end

local function decode_data(data_field)
  if not data_field then return {} end
  if type(data_field) == "string" then
    local t, err = dkjson.decode(data_field)
    if not t then return nil, "decode: " .. tostring(err) end
    return t
  end
  return data_field
end

---------------------------------------------------------------------------
-- Status rows for exceptions are dynamic: appear on first raise (INSERT),
-- updated by re-raise/ack (UPDATE), removed by clear (DELETE). Schema row
-- (knowledge_base main table) is permanent.
---------------------------------------------------------------------------

-- returns (data_table, exists_bool) | nil, err
local function read_data(conn, path)
  local row, err = fetch_one(conn, string.format(
    "SELECT data FROM knowledge_base_status WHERE path = '%s'::ltree",
    escape_sql(path)))
  if err then return nil, err end
  if not row then return {}, false end
  return decode_data(row.data) or {}, true
end

-- UPSERT (insert or update) the status row for path with the given table.
local function write_data(conn, path, tbl)
  local json = dkjson.encode(tbl)
  return exec(conn, string.format(
    "INSERT INTO knowledge_base_status (path, data) " ..
    "VALUES ('%s'::ltree, '%s'::json) " ..
    "ON CONFLICT (path) DO UPDATE SET data = EXCLUDED.data",
    escape_sql(path), escape_sql(json)))
end

local function delete_row(conn, path)
  return exec(conn, string.format(
    "DELETE FROM knowledge_base_status WHERE path = '%s'::ltree",
    escape_sql(path)))
end

---------------------------------------------------------------------------
-- log_exception: raise/refresh a fault.
--
-- Default `status_bool = false` (faulted). If the row was previously
-- acknowledged but a new occurrence comes in, un-ack so the dashboard
-- re-notifies (per data-model rule).
---------------------------------------------------------------------------

function M.log_exception_status(conn, path, status_bool, msg, trace_b64)
  local existing, _ = read_data(conn, path)
  if not existing then return nil, "read failed" end

  local row = {
    status       = status_bool,
    ts           = now_ns(),
    last_error   = msg or "",
    trace_b64    = trace_b64 or "",
    -- new fault un-acks (re-notify); status=true keeps existing ack state
    acknowledged = (status_bool == false) and false
                                          or  (existing.acknowledged or false),
  }
  return write_data(conn, path, row)
end

function M.log_exception(conn, path, msg, trace_b64)
  return M.log_exception_status(conn, path, false, msg, trace_b64)
end

---------------------------------------------------------------------------
-- ack_exception: user has seen the fault; mute dashboard nag.
-- No-op if the row doesn't exist (no fault to ack).
---------------------------------------------------------------------------

function M.ack_exception(conn, path)
  local existing, exists = read_data(conn, path)
  if not existing then return nil, "read failed" end
  if not exists then return true end       -- nothing to ack
  existing.acknowledged = true
  return write_data(conn, path, existing)
end

---------------------------------------------------------------------------
-- clear_exception: DELETE the status row entirely. Schema row in
-- knowledge_base main table persists; next raise re-INSERTs status fresh.
---------------------------------------------------------------------------

function M.clear_exception(conn, path)
  return delete_row(conn, path)
end

---------------------------------------------------------------------------
-- mute_existing_on_boot: master's startup helper. Auto-acks all
-- pre-existing faults so a reboot doesn't re-alert on stale problems;
-- subsequent log_exception calls will un-ack as new occurrences come in.
---------------------------------------------------------------------------

function M.mute_existing_on_boot(conn)
  -- Find all exception status rows (only those with a row exist + are
  -- in fault state) and auto-ack. Schema rows without a status row mean
  -- "exception declared but never raised in this epoch" -- nothing to mute.
  local rows, err = fetch_all(conn,
    "SELECT k.path FROM knowledge_base k " ..
    "JOIN knowledge_base_status s ON s.path = k.path " ..
    "WHERE k.label = 'SYS_EXCEPTION'")
  if not rows then return nil, err end

  local muted = 0
  for _, r in ipairs(rows) do
    local path = tostring(r.path)
    local data, exists = read_data(conn, path)
    if data and exists
       and data.status == false
       and not data.acknowledged then
      data.acknowledged = true
      local ok, werr = write_data(conn, path, data)
      if not ok then return nil, "mute write failed at " .. path .. ": " .. tostring(werr) end
      muted = muted + 1
    end
  end
  return muted
end

return M
