--[[
  construct_driver_common.lua

  Shared helpers for the new class/instance PG drivers
  (construct_doc_store, construct_stream_store).

  Both drivers share:
    - identifier sanitization (ltree labels and derived table names)
    - quote helpers
    - the dynamic-SQL purge-dispatch pattern

  Does NOT own any tables of its own. Each driver owns its registry +
  data tables.
]]

local M = {}

---------------------------------------------------------------------------
-- Quoting
---------------------------------------------------------------------------

function M.quote_ident(name)
  return '"' .. tostring(name):gsub('"', '""') .. '"'
end

function M.quote_literal(val)
  if val == nil then return "NULL" end
  return "'" .. tostring(val):gsub("'", "''") .. "'"
end

---------------------------------------------------------------------------
-- ltree label sanitization
---------------------------------------------------------------------------

-- Convert an arbitrary string to a valid ltree label ([A-Za-z0-9_]+).
-- Lowercased; any other char becomes '_'. Empty input is rejected.
function M.sanitize_label(s)
  assert(type(s) == "string" and #s > 0, "label must be non-empty string")
  local out = s:lower():gsub("[^a-z0-9_]", "_")
  if out:match("^[0-9]") then out = "l_" .. out end
  return out
end

-- Validate that a caller-supplied string is already a valid ltree label.
-- Returns true or (false, reason). Used in runtime paths where we want to
-- fail loud rather than silently rewrite.
function M.is_valid_label(s)
  if type(s) ~= "string" or #s == 0 then
    return false, "label must be non-empty string"
  end
  if not s:match("^[A-Za-z][A-Za-z0-9_]*$") then
    return false, "label must match [A-Za-z][A-Za-z0-9_]*: " .. s
  end
  return true
end

-- Validate an ltree path. Each label must be valid; dots are separators.
function M.is_valid_ltree(path)
  if type(path) ~= "string" or #path == 0 then
    return false, "path must be non-empty string"
  end
  for label in path:gmatch("[^.]+") do
    local ok, err = M.is_valid_label(label)
    if not ok then return false, err end
  end
  return true
end

---------------------------------------------------------------------------
-- Table-name derivation (ltree namespace -> safe SQL identifier)
---------------------------------------------------------------------------

-- Derive a per-class table name from a namespace ltree.
-- Example: ("system", "stream_msg", "telemetry.robot_heartbeat")
--       -> "system_stream_msg__telemetry_robot_heartbeat"
-- The database prefix keeps tables scoped per-KB, matching the existing
-- naming convention (system_stream, system_document, etc.).
function M.derived_table_name(database, suffix, namespace)
  local ok, err = M.is_valid_ltree(namespace)
  assert(ok, err)
  local flat = namespace:gsub("%.", "_")
  return database .. "_" .. suffix .. "__" .. flat
end

---------------------------------------------------------------------------
-- Derived plpgsql function name (from namespace)
---------------------------------------------------------------------------

function M.derived_function_name(database, prefix, namespace)
  local ok, err = M.is_valid_ltree(namespace)
  assert(ok, err)
  local flat = namespace:gsub("%.", "_")
  return database .. "_" .. prefix .. "__" .. flat
end

---------------------------------------------------------------------------
-- SQL exec / query helpers (DBI convention used elsewhere in this repo)
---------------------------------------------------------------------------

function M.exec(conn, sql_str)
  local stmt, err = conn:prepare(sql_str)
  if not stmt then
    error("SQL prepare error: " .. tostring(err) .. "\nQuery: " .. sql_str)
  end
  local ok, exec_err = stmt:execute()
  if not ok then
    error("SQL execute error: " .. tostring(exec_err) .. "\nQuery: " .. sql_str)
  end
  stmt:close()
end

function M.query_all(conn, sql_str)
  local stmt, err = conn:prepare(sql_str)
  if not stmt then
    error("SQL prepare error: " .. tostring(err) .. "\nQuery: " .. sql_str)
  end
  local ok, exec_err = stmt:execute()
  if not ok then
    error("SQL execute error: " .. tostring(exec_err) .. "\nQuery: " .. sql_str)
  end
  local rows = {}
  local row = stmt:fetch(true)
  while row do
    rows[#rows + 1] = row
    row = stmt:fetch(true)
  end
  stmt:close()
  return rows
end

function M.query_one(conn, sql_str)
  local rows = M.query_all(conn, sql_str)
  return rows[1]
end

return M
