-- =============================================================================
-- kb_exception.lua -- SCADA-style exception runtime library.
--
-- Each SYS_EXCEPTION is a nested header in the KB with 15 KB_STATUS_FIELD
-- children + 1 KB_JSONB_FIELD (signatures). See project_dcs_task4_design.md
-- for the full shape.
--
-- New SCADA API:
--   raise(conn, path, opts)        -- NORMAL/RTN_UNACK -> UNACK_ACTIVE
--   ack(conn, path, operator_id, comment?)
--   clear(conn, path)              -- -> NORMAL (RTN_UNACK if was UNACK)
--   shelve(conn, path, duration_s, operator_id, reason)
--   suppress(conn, path, operator_id, reason)
--   unshelve(conn, path, operator_id)
--
-- Legacy compat API (maps onto new):
--   log_exception(conn, path, msg, trace_b64)       -> raise(...)
--   log_exception_status(conn, path, s_bool, ...)   -> raise or clear
--   ack_exception(conn, path)                       -> ack(...)
--   clear_exception(conn, path)                     -> clear(...)
--   mute_existing_on_boot(conn)                     -> ack all UNACK_ACTIVE
--
-- All ops take a DBI conn (autocommit on); all return (ok, err).
-- `path` is the full ltree path of the SYS_EXCEPTION header (not a child).
-- =============================================================================

local dkjson = require("dkjson")
local ptime  = require("posix_time")

local M = {}

---------------------------------------------------------------------------
-- SQL helpers
---------------------------------------------------------------------------

local function escape_sql(s) return tostring(s):gsub("'", "''") end

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
  if not rows then return nil, err end
  return rows[1]
end

-- Wall-clock seconds (not monotonic -- the alarm journal sorts on
-- last_raised_ts and the UI computes "N minutes ago" against os.time()).
local function now_s() return os.time() end

local function decode_jsonb(v)
  if not v or v == "" then return {} end
  if type(v) == "table" then return v end
  local t, err = dkjson.decode(tostring(v))
  if not t then return {}, err end
  return t
end

---------------------------------------------------------------------------
-- Per-child read/write helpers (target knowledge_base_status)
---------------------------------------------------------------------------

local function read_child(conn, child_path)
  local row, err = fetch_one(conn, string.format(
    "SELECT data FROM knowledge_base_status WHERE path = '%s'::ltree",
    escape_sql(child_path)))
  if err then return nil, err end
  if not row then return {} end
  return decode_jsonb(row.data)
end

local function write_child(conn, child_path, data_tbl)
  local json = dkjson.encode(data_tbl)
  return exec(conn, string.format(
    "INSERT INTO knowledge_base_status (path, data) " ..
    "VALUES ('%s'::ltree, '%s'::json) " ..
    "ON CONFLICT (path) DO UPDATE SET data = EXCLUDED.data",
    escape_sql(child_path), escape_sql(json)))
end

-- Convenience: get/set a single named status field of a SYS_EXCEPTION.
local function get_status(conn, exc_path, field)
  local data = read_child(conn, exc_path .. ".KB_STATUS_FIELD." .. field)
  return data and data.value
end

local function set_status(conn, exc_path, field, value)
  return write_child(conn, exc_path .. ".KB_STATUS_FIELD." .. field,
                     { value = value })
end

local function get_jsonb(conn, exc_path, field)
  return read_child(conn, exc_path .. ".KB_JSONB_FIELD." .. field)
end

local function set_jsonb(conn, exc_path, field, tbl)
  return write_child(conn, exc_path .. ".KB_JSONB_FIELD." .. field, tbl)
end

---------------------------------------------------------------------------
-- Signature dedup (SCADA alarm-summary pattern)
---------------------------------------------------------------------------

-- Cheap stable hash of (error_text, source_path). Not cryptographic —
-- dedup collisions only merge entries that should stay distinct, which
-- is a display quirk rather than a correctness bug.
local function signature_of(error_text, source_path)
  local s = tostring(error_text or "") .. "|" .. tostring(source_path or "")
  local sum = 0
  for i = 1, #s do sum = (sum + s:byte(i) * i) % 0x7fffffff end
  return string.format("%x_%x_%x", #s, sum, s:byte(1) or 0)
end

local SIGNATURE_CAP = 64

local function update_signatures(conn, exc_path, now, opts)
  local data = get_jsonb(conn, exc_path, "signatures") or {}
  local sigs = data.value
  if type(sigs) ~= "table" then sigs = {} end  -- first time

  local sig = signature_of(opts.error, opts.source_path)
  local found = false
  for _, entry in ipairs(sigs) do
    if entry.signature == sig then
      entry.last_occurrence_ts = now
      entry.occurrence_count   = (entry.occurrence_count or 0) + 1
      found = true
      break
    end
  end

  if not found then
    local new_entry = {
      signature            = sig,
      error                = tostring(opts.error or ""),
      trigger_value        = tostring(opts.trigger_value or ""),
      limit_value          = tostring(opts.limit_value or ""),
      source_path          = tostring(opts.source_path or ""),
      first_occurrence_ts  = now,
      last_occurrence_ts   = now,
      occurrence_count     = 1,
    }
    table.insert(sigs, 1, new_entry)
    -- Evict oldest-last_occurrence beyond cap
    while #sigs > SIGNATURE_CAP do
      -- find index of min last_occurrence_ts
      local min_idx, min_ts = 1, sigs[1].last_occurrence_ts or 0
      for i = 2, #sigs do
        local ts = sigs[i].last_occurrence_ts or 0
        if ts < min_ts then min_idx, min_ts = i, ts end
      end
      table.remove(sigs, min_idx)
    end
  end

  return set_jsonb(conn, exc_path, "signatures", { value = sigs })
end

---------------------------------------------------------------------------
-- raise: NORMAL|RTN_UNACK|ACK_ACTIVE|UNACK_ACTIVE -> UNACK_ACTIVE (or
-- stays SHELVED with sig update only).
---------------------------------------------------------------------------

function M.raise(conn, exc_path, opts)
  opts = opts or {}
  local now = now_s()

  local state = get_status(conn, exc_path, "state") or "NORMAL"

  -- Shelved alarms still accumulate signatures + hit_count but don't
  -- transition state until the shelve expires (janitor will unshelve).
  local new_state = (state == "SHELVED") and "SHELVED" or "UNACK_ACTIVE"

  set_status(conn, exc_path, "state",              new_state)
  set_status(conn, exc_path, "last_raised_ts",     now)
  set_status(conn, exc_path, "last_error",         tostring(opts.error or ""))
  set_status(conn, exc_path, "last_trigger_value", tostring(opts.trigger_value or ""))
  set_status(conn, exc_path, "last_limit_value",   tostring(opts.limit_value or ""))
  set_status(conn, exc_path, "last_source_path",   tostring(opts.source_path or ""))

  local hc = get_status(conn, exc_path, "hit_count") or 0
  set_status(conn, exc_path, "hit_count", (tonumber(hc) or 0) + 1)

  return update_signatures(conn, exc_path, now, opts)
end

---------------------------------------------------------------------------
-- ack: UNACK_ACTIVE -> ACK_ACTIVE; RTN_UNACK -> NORMAL. Others: no-op.
---------------------------------------------------------------------------

function M.ack(conn, exc_path, operator_id, comment)
  local state = get_status(conn, exc_path, "state") or "NORMAL"
  local now = now_s()

  local new_state = state
  if state == "UNACK_ACTIVE"   then new_state = "ACK_ACTIVE" end
  if state == "RTN_UNACK"      then new_state = "NORMAL"     end

  if new_state ~= state then
    set_status(conn, exc_path, "state",        new_state)
  end
  set_status(conn, exc_path, "last_ack_ts", now)
  set_status(conn, exc_path, "last_ack_by", tostring(operator_id or ""))
  if comment then
    set_status(conn, exc_path, "last_comment", tostring(comment))
  end
  return true
end

---------------------------------------------------------------------------
-- clear: active alarm transitions to NORMAL or RTN_UNACK based on prior state.
--   UNACK_ACTIVE  -> RTN_UNACK   (problem gone before operator saw it)
--   ACK_ACTIVE    -> NORMAL      (operator knew + problem gone)
--   SHELVED       -> NORMAL      (shelve expired or operator unshelved)
--   RTN_UNACK     -> RTN_UNACK   (idempotent)
--   NORMAL        -> NORMAL      (idempotent)
---------------------------------------------------------------------------

function M.clear(conn, exc_path)
  local state = get_status(conn, exc_path, "state") or "NORMAL"
  local now = now_s()

  local new_state = state
  if state == "UNACK_ACTIVE" then new_state = "RTN_UNACK" end
  if state == "ACK_ACTIVE"   then new_state = "NORMAL"    end
  if state == "SHELVED"      then new_state = "NORMAL"    end

  if new_state ~= state then
    set_status(conn, exc_path, "state", new_state)
    set_status(conn, exc_path, "last_rtn_ts", now)
  end
  return true
end

---------------------------------------------------------------------------
-- shelve: any state -> SHELVED with shelve_until = now + duration_s.
-- duration_s = 0 means suppress (manual clear only).
---------------------------------------------------------------------------

function M.shelve(conn, exc_path, duration_s, operator_id, reason)
  local now = now_s()
  local until_ = (tonumber(duration_s) or 0) > 0
                 and (now + tonumber(duration_s)) or 0
  set_status(conn, exc_path, "state",           "SHELVED")
  set_status(conn, exc_path, "last_shelve_ts",  now)
  set_status(conn, exc_path, "last_shelve_by",  tostring(operator_id or ""))
  set_status(conn, exc_path, "shelve_until",    until_)
  if reason then
    set_status(conn, exc_path, "last_comment", tostring(reason))
  end
  return true
end

function M.suppress(conn, exc_path, operator_id, reason)
  return M.shelve(conn, exc_path, 0, operator_id, reason)
end

---------------------------------------------------------------------------
-- unshelve: SHELVED -> NORMAL (janitor or operator).
---------------------------------------------------------------------------

function M.unshelve(conn, exc_path, operator_id)
  local now = now_s()
  local state = get_status(conn, exc_path, "state") or "NORMAL"
  if state ~= "SHELVED" then return true end
  set_status(conn, exc_path, "state",        "NORMAL")
  set_status(conn, exc_path, "shelve_until", 0)
  if operator_id then
    set_status(conn, exc_path, "last_ack_by", tostring(operator_id))
  end
  return true
end

---------------------------------------------------------------------------
-- Legacy compatibility wrappers (keep old callers working)
---------------------------------------------------------------------------

function M.log_exception(conn, exc_path, msg, trace_b64)
  return M.raise(conn, exc_path, {
    error = msg,
    trigger_value = trace_b64,  -- legacy shape, reinterpreted
  })
end

function M.log_exception_status(conn, exc_path, status_bool, msg, trace_b64)
  if status_bool == true then
    -- legacy "not faulted" → clear
    return M.clear(conn, exc_path)
  else
    -- legacy "faulted" → raise
    return M.raise(conn, exc_path, { error = msg, trigger_value = trace_b64 })
  end
end

function M.ack_exception(conn, exc_path)
  return M.ack(conn, exc_path, "legacy")
end

function M.clear_exception(conn, exc_path)
  return M.clear(conn, exc_path)
end

---------------------------------------------------------------------------
-- mute_existing_on_boot: auto-ack any UNACK_ACTIVE alarms so a reboot
-- doesn't re-alert on stale problems. New raises after boot will
-- transition UNACK_ACTIVE again and be seen.
---------------------------------------------------------------------------

function M.mute_existing_on_boot(conn)
  local rows, err = fetch_all(conn,
    "SELECT path::text AS path FROM knowledge_base WHERE label = 'SYS_EXCEPTION'")
  if not rows then return nil, err end

  local muted = 0
  for _, r in ipairs(rows) do
    local state = get_status(conn, r.path, "state")
    if state == "UNACK_ACTIVE" then
      local ok, werr = M.ack(conn, r.path, "boot_mute")
      if not ok then
        return nil, "boot-mute failed at " .. r.path .. ": " .. tostring(werr)
      end
      muted = muted + 1
    end
  end
  return muted
end

return M
