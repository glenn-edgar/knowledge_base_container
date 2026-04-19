-- =============================================================================
-- kb_status.lua -- Direct SQL helpers for `knowledge_base_status` table.
--
-- Schema: (id SERIAL, data JSON, path LTREE UNIQUE).
-- Reads/writes the `data` JSON column at a given ltree path.
--
-- Thin wrapper around the patterns in kb_status_data.lua, scoped to the
-- specific operations our user functions need (set/get whole row).
-- =============================================================================

local dkjson = require("dkjson")

local M = {}

local function escape(s) return tostring(s):gsub("'", "''") end

local function exec(conn, sql)
  local sth, err = conn:prepare(sql)
  if not sth then return nil, "prepare: " .. tostring(err) end
  local ok, eerr = sth:execute()
  if not ok then return nil, "execute: " .. tostring(eerr) end
  sth:close()
  return true
end

local function query_one(conn, sql)
  local sth, err = conn:prepare(sql)
  if not sth then return nil, "prepare: " .. tostring(err) end
  local ok, eerr = sth:execute()
  if not ok then sth:close(); return nil, "execute: " .. tostring(eerr) end
  local row = sth:fetch(true)
  sth:close()
  return row
end

-- Set the JSON data for a status row at `path`. Caller passes a Lua
-- table; encoded to JSON. Status row must have been pre-allocated by
-- the construct script (UPDATE only; no INSERT).
function M.set_status_data(conn, path, value_table)
  local json = dkjson.encode(value_table)
  return exec(conn, string.format(
    "UPDATE knowledge_base_status SET data = '%s' WHERE path = '%s'::ltree",
    escape(json), escape(path)))
end

function M.get_status_data(conn, path)
  local row, err = query_one(conn, string.format(
    "SELECT data FROM knowledge_base_status WHERE path = '%s'::ltree",
    escape(path)))
  if not row then return nil, err or "row not found" end
  if type(row.data) == "string" then
    return dkjson.decode(row.data)
  end
  return row.data
end

return M
