-- =============================================================================
-- kb_rule.lua -- Runtime read/write for KB_RULE nodes (Task 4).
--
-- KB_RULE is a nested header always found under a parent KB_LOG:
--   KB_LOG.<log>.KB_RULE.<rule_id>           (header; props: kind, metric,
--                                             threshold, target_exception,
--                                             cooldown_s, ...)
--     KB_STATUS_FIELD.enabled                operator toggle
--     KB_STATUS_FIELD.suppressed             rule-level shelve
--     KB_STATUS_FIELD.suppressed_until       epoch s
--     KB_STATUS_FIELD.fire_count             lifetime trips
--     KB_STATUS_FIELD.last_fired_ts          epoch s
--     KB_STATUS_FIELD.last_fired_value       observation that tripped
--     KB_STATUS_FIELD.last_fired_details     jsonb-safe drill payload
--
-- Used primarily by log_analyzer to discover + evaluate rules, and by
-- operator UI to flip enabled/suppressed at runtime.
-- =============================================================================

local dkjson = require("dkjson")
local ptime  = require("posix_time")

local M = {}

---------------------------------------------------------------------------
-- SQL helpers (same pattern as kb_log.lua / kb_exception.lua)
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

local function decode_jsonb(v)
  if not v or v == "" then return {} end
  if type(v) == "table" then return v end
  local t = dkjson.decode(tostring(v))
  return t or {}
end

local function now_s() return math.floor(ptime.now_sec()) end

local function set_status_child(conn, rule_path, field, value)
  local path = rule_path .. ".KB_STATUS_FIELD." .. field
  return exec(conn, string.format(
    "INSERT INTO knowledge_base_status (path, data) " ..
    "VALUES ('%s'::ltree, '%s'::json) " ..
    "ON CONFLICT (path) DO UPDATE SET data = EXCLUDED.data",
    escape_sql(path), escape_sql(dkjson.encode({ value = value }))))
end

---------------------------------------------------------------------------
-- Discovery
---------------------------------------------------------------------------

--- List every KB_RULE in the KB (flat, cross-log).
function M.list_all(conn)
  local rows, err = fetch_all(conn,
    "SELECT path::text AS path, properties " ..
    "FROM knowledge_base WHERE label = 'KB_RULE'")
  if not rows then return nil, err end
  for _, r in ipairs(rows) do r.properties = decode_jsonb(r.properties) end
  return rows
end

--- List the KB_RULE children of a specific KB_LOG (subtree query).
function M.list_for_log(conn, log_path)
  local rows, err = fetch_all(conn, string.format(
    "SELECT path::text AS path, properties " ..
    "FROM knowledge_base WHERE label = 'KB_RULE' AND path <@ '%s'::ltree",
    escape_sql(log_path)))
  if not rows then return nil, err end
  for _, r in ipairs(rows) do r.properties = decode_jsonb(r.properties) end
  return rows
end

---------------------------------------------------------------------------
-- Config (static, build-time)
---------------------------------------------------------------------------

function M.read_config(conn, rule_path)
  local row, err = fetch_one(conn, string.format(
    "SELECT properties FROM knowledge_base " ..
    "WHERE label = 'KB_RULE' AND path = '%s'::ltree",
    escape_sql(rule_path)))
  if err then return nil, err end
  if not row then return nil end
  return decode_jsonb(row.properties)
end

---------------------------------------------------------------------------
-- State (runtime children: enabled/suppressed/fire_count/...)
---------------------------------------------------------------------------

--- Read all state children in one subtree query.
--- Returns { enabled=bool, suppressed=bool, suppressed_until=int,
---           fire_count=int, last_fired_ts=int, last_fired_value=text,
---           last_fired_details=text }
function M.read_state(conn, rule_path)
  local rows, err = fetch_all(conn, string.format(
    "SELECT path::text AS path, data FROM knowledge_base_status " ..
    "WHERE path <@ '%s'::ltree",
    escape_sql(rule_path)))
  if not rows then return nil, err end
  local state = {}
  for _, r in ipairs(rows) do
    local field = r.path:match("%.KB_STATUS_FIELD%.([^%.]+)$")
    if field then
      local d = decode_jsonb(r.data)
      state[field] = d.value
    end
  end
  return state
end

---------------------------------------------------------------------------
-- Runtime actions
---------------------------------------------------------------------------

--- Record that the rule tripped. Increments fire_count; sets
--- last_fired_ts / last_fired_value / last_fired_details.
---
--- @param value   observation that triggered (any jsonb-safe scalar)
--- @param details optional table — encoded as JSON into last_fired_details
---                for the UI drill view.
function M.record_fire(conn, rule_path, value, details, ts)
  ts = ts or now_s()

  -- Increment fire_count (read-modify-write; fine at our rates).
  local state = M.read_state(conn, rule_path) or {}
  local fc = tonumber(state.fire_count) or 0
  set_status_child(conn, rule_path, "fire_count",   fc + 1)
  set_status_child(conn, rule_path, "last_fired_ts", ts)
  set_status_child(conn, rule_path, "last_fired_value", tostring(value or ""))
  if details then
    set_status_child(conn, rule_path, "last_fired_details",
      dkjson.encode(details))
  end
  return true
end

--- Operator enables/disables a rule without rebuilding the KB.
function M.set_enabled(conn, rule_path, bool)
  return set_status_child(conn, rule_path, "enabled", bool and true or false)
end

--- Shelve (suppress) a rule until `until_ts` (0 = manual-unshelve only).
function M.shelve(conn, rule_path, until_ts)
  set_status_child(conn, rule_path, "suppressed", true)
  set_status_child(conn, rule_path, "suppressed_until", tonumber(until_ts) or 0)
  return true
end

function M.unshelve(conn, rule_path)
  set_status_child(conn, rule_path, "suppressed", false)
  set_status_child(conn, rule_path, "suppressed_until", 0)
  return true
end

--- Auto-unshelve rules whose suppressed_until has passed. Returns count.
--- Called from log_analyzer's per-tick janitor.
function M.sweep_expired_suppressions(conn)
  local now = now_s()
  local rows, err = fetch_all(conn, string.format([[
    SELECT k.path::text AS rule_path
    FROM knowledge_base k
    WHERE k.label = 'KB_RULE'
      AND EXISTS (
        SELECT 1 FROM knowledge_base_status s
        WHERE s.path = (k.path::text || '.KB_STATUS_FIELD.suppressed')::ltree
          AND (s.data->>'value')::text = 'true'
      )
      AND EXISTS (
        SELECT 1 FROM knowledge_base_status s
        WHERE s.path = (k.path::text || '.KB_STATUS_FIELD.suppressed_until')::ltree
          AND (s.data->>'value')::int > 0
          AND (s.data->>'value')::int <= %d
      )
  ]], now))
  if not rows then return nil, err end

  for _, r in ipairs(rows) do
    M.unshelve(conn, r.rule_path)
  end
  return #rows
end

---------------------------------------------------------------------------
-- Rule evaluation helpers — cooldown check (caller of record_fire uses
-- this to decide whether to actually fire).
---------------------------------------------------------------------------

function M.is_in_cooldown(state, cooldown_s, now)
  local last = tonumber(state.last_fired_ts) or 0
  cooldown_s = tonumber(cooldown_s) or 0
  return (now - last) < cooldown_s
end

function M.is_actionable(state)
  -- rule is actionable iff enabled and not suppressed
  local enabled     = state.enabled
  local suppressed  = state.suppressed
  -- nil defaults to enabled=true, suppressed=false (matches construction defaults)
  if enabled == nil then enabled = true end
  if suppressed == nil then suppressed = false end
  return (enabled == true) and (suppressed == false)
end

return M
