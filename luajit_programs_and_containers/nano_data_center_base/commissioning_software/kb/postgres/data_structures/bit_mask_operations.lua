--[[
  BitMaskOperations - Manages bit masks and flag registers for distributed
  node control systems.  LuaJIT port using luadbi-postgresql.

  Usage:
    local BitMaskOperations = require("bit_mask_operations")
    local bm = BitMaskOperations.new(dbh, "bit_mask_table")
    bm:create_table()
    bm:create_entry("node_1", 0)
    bm:set_bit_mask("node_1", 0xFF, -1)
]]

local bit = require("bit")   -- LuaJIT bit library

local BitMaskOperations = {}
BitMaskOperations.__index = BitMaskOperations

---------------------------------------------------------------------------
-- Helpers
---------------------------------------------------------------------------

local function esc(val)
  if val == nil then return "NULL" end
  local s = tostring(val)
  s = s:gsub("'", "''")
  return "'" .. s .. "'"
end

local function raw_query(dbh, sql_str)
  local sth, err = dbh:prepare(sql_str)
  if not sth then error("Prepare error: " .. tostring(err) .. "\nSQL: " .. sql_str) end
  local ok, exec_err = sth:execute()
  if not ok then error("Execute error: " .. tostring(exec_err) .. "\nSQL: " .. sql_str) end
  return sth
end

local function fetch_all(dbh, sql_str)
  local sth = raw_query(dbh, sql_str)
  local rows = {}
  while true do
    local row = sth:fetch(true)
    if not row then break end
    local copy = {}
    for k, v in pairs(row) do copy[k] = v end
    rows[#rows + 1] = copy
  end
  return rows
end

local function fetch_one(dbh, sql_str)
  local sth = raw_query(dbh, sql_str)
  local row = sth:fetch(true)
  if not row then return nil end
  local copy = {}
  for k, v in pairs(row) do copy[k] = v end
  return copy
end

local function exec_dml(dbh, sql_str)
  local sth = raw_query(dbh, sql_str)
  local affected = sth:affected()
  return affected or 0
end

---------------------------------------------------------------------------
-- Constructor
---------------------------------------------------------------------------

--- Create a new BitMaskOperations instance.
-- @param dbh  DBI connection handle
-- @param table_name  table name (default "bit_mask_table")
function BitMaskOperations.new(dbh, table_name)
  local self = setmetatable({}, BitMaskOperations)
  self.dbh        = dbh
  self.table_name = table_name or "bit_mask_table"
  return self
end

---------------------------------------------------------------------------
-- Table management
---------------------------------------------------------------------------

function BitMaskOperations:create_table()
  raw_query(self.dbh, string.format("DROP TABLE IF EXISTS %s", self.table_name))
  raw_query(self.dbh, string.format([[
    CREATE TABLE %s (
      node_id  VARCHAR(255) PRIMARY KEY,
      bit_mask BIGINT NOT NULL DEFAULT 0
    )
  ]], self.table_name))
  self.dbh:commit()
end

---------------------------------------------------------------------------
-- CRUD
---------------------------------------------------------------------------

--- Create a new entry.
-- @return true on success
function BitMaskOperations:create_entry(node_id, bit_mask_val)
  bit_mask_val = bit_mask_val or 0
  local sql_str = string.format(
    "INSERT INTO %s (node_id, bit_mask) VALUES (%s, %s)",
    self.table_name, esc(node_id), tostring(bit_mask_val))

  local ok, err = pcall(function()
    exec_dml(self.dbh, sql_str)
    self.dbh:commit()
  end)
  if not ok then
    pcall(function() self.dbh:rollback() end)
    error(string.format("Node ID '%s' may already exist: %s", node_id, tostring(err)))
  end
  return true
end

--- Retrieve bit_mask for a node.
-- @return integer or nil
function BitMaskOperations:get_bit_mask(node_id)
  local sql_str = string.format(
    "SELECT bit_mask FROM %s WHERE node_id = %s",
    self.table_name, esc(node_id))
  local row = fetch_one(self.dbh, sql_str)
  if not row then return nil end
  return tonumber(row.bit_mask)
end

--- Atomically update specific bits:
--   new_mask = (current & ~change_mask) | (new_bits & change_mask)
-- @param node_id   string
-- @param new_bits  integer (64-bit)
-- @param change_mask integer (default -1 = all bits)
-- @return true if row updated
function BitMaskOperations:set_bit_mask(node_id, new_bits, change_mask)
  change_mask = change_mask or -1

  local sql_str = string.format([[
    UPDATE %s
       SET bit_mask = (bit_mask & (~(%s)::bigint)) | ((%s)::bigint & (%s)::bigint)
     WHERE node_id = %s
  ]], self.table_name,
     tostring(change_mask), tostring(new_bits), tostring(change_mask),
     esc(node_id))

  local ok, err = pcall(function()
    local affected = exec_dml(self.dbh, sql_str)
    self.dbh:commit()
    return affected
  end)

  if not ok then
    pcall(function() self.dbh:rollback() end)
    error(tostring(err))
  end
  -- err here is actually the return value from the pcall success path
  return (err or 0) > 0
end

--- Retrieve full entry as dict.
function BitMaskOperations:get_entry(node_id)
  local sql_str = string.format(
    "SELECT node_id, bit_mask FROM %s WHERE node_id = %s",
    self.table_name, esc(node_id))
  local row = fetch_one(self.dbh, sql_str)
  if not row then return nil end
  row.bit_mask = tonumber(row.bit_mask)
  return row
end

--- Delete an entry.
-- @return true if deleted
function BitMaskOperations:delete_entry(node_id)
  local sql_str = string.format(
    "DELETE FROM %s WHERE node_id = %s",
    self.table_name, esc(node_id))
  local affected = exec_dml(self.dbh, sql_str)
  self.dbh:commit()
  return affected > 0
end

--- List all node IDs.
function BitMaskOperations:list_all_nodes()
  local sql_str = string.format(
    "SELECT node_id FROM %s ORDER BY node_id", self.table_name)
  local rows = fetch_all(self.dbh, sql_str)
  local rv = {}
  for _, r in ipairs(rows) do rv[#rv + 1] = r.node_id end
  return rv
end

return BitMaskOperations