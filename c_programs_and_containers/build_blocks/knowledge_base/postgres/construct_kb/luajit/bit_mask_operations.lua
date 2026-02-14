--[[
  bit_mask_operations.lua
  
  LuaJIT translation of bit_mask_operations.py
  PostgreSQL bit mask operations for distributed node control systems.
  
  Dependencies:
    - DBI (luadbi)
  
  Usage:
    local BitMaskOperations = require("bit_mask_operations")
    local bm = BitMaskOperations.new(conn, "bit_mask_table")
]]

local BitMaskOperations = {}
BitMaskOperations.__index = BitMaskOperations

local function quote_ident(name)
  return '"' .. name:gsub('"', '""') .. '"'
end

local function quote_literal(val)
  if val == nil then return "NULL" end
  return "'" .. tostring(val):gsub("'", "''") .. "'"
end

---------------------------------------------------------------------------
-- Constructor
---------------------------------------------------------------------------

--- Create a new BitMaskOperations instance.
-- @param conn  DBI connection object
-- @param bit_mask_table_name string (default "bit_mask_table")
function BitMaskOperations.new(conn, bit_mask_table_name)
  local self = setmetatable({}, BitMaskOperations)
  self.conn = conn
  self.table_name = bit_mask_table_name or "bit_mask_table"
  return self
end

---------------------------------------------------------------------------
-- Internal helpers
---------------------------------------------------------------------------

function BitMaskOperations:_exec(sql_str)
  local stmt, err = self.conn:prepare(sql_str)
  if not stmt then
    error("SQL prepare error: " .. tostring(err) .. "\nQuery: " .. sql_str)
  end
  local ok, exec_err = stmt:execute()
  if not ok then
    error("SQL execute error: " .. tostring(exec_err) .. "\nQuery: " .. sql_str)
  end
  stmt:close()
end

function BitMaskOperations:_query(sql_str)
  local stmt, err = self.conn:prepare(sql_str)
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

function BitMaskOperations:_query_one(sql_str)
  local rows = self:_query(sql_str)
  return rows[1]
end

---------------------------------------------------------------------------
-- Table management
---------------------------------------------------------------------------

--- Create a fresh bit mask table, dropping any existing one.
function BitMaskOperations:create_table()
  local tn = quote_ident(self.table_name)

  self:_exec(string.format("DROP TABLE IF EXISTS %s", tn))
  self:_exec(string.format([[
    CREATE TABLE %s (
      node_id VARCHAR(255) PRIMARY KEY,
      bit_mask BIGINT NOT NULL DEFAULT 0
    )
  ]], tn))
end

---------------------------------------------------------------------------
-- CRUD operations
---------------------------------------------------------------------------

--- Create a new entry.
-- @param node_id string
-- @param bit_mask number (default 0)
-- @return true on success
function BitMaskOperations:create_entry(node_id, bit_mask)
  bit_mask = bit_mask or 0
  local tn = quote_ident(self.table_name)

  self:_exec(string.format(
    "INSERT INTO %s (node_id, bit_mask) VALUES (%s, %d)",
    tn, quote_literal(node_id), bit_mask))

  return true
end

--- Get the bit mask value for a node.
-- @param node_id string
-- @return number|nil
function BitMaskOperations:get_bit_mask(node_id)
  local tn = quote_ident(self.table_name)

  local row = self:_query_one(string.format(
    "SELECT bit_mask FROM %s WHERE node_id = %s",
    tn, quote_literal(node_id)))

  if not row then return nil end
  return tonumber(row.bit_mask)
end

--- Atomically update specific bits in the bit_mask.
-- new_mask = (current_mask & ~change_mask) | (new_bits & change_mask)
-- @param node_id string
-- @param new_bits number
-- @param change_mask number (default -1 for full overwrite)
-- @return boolean  true if row updated
function BitMaskOperations:set_bit_mask(node_id, new_bits, change_mask)
  change_mask = change_mask or -1

  local SIGNED_64_MIN = -9223372036854775808LL
  local SIGNED_64_MAX = 9223372036854775807LL

  -- LuaJIT handles 64-bit integers natively via LL suffix,
  -- but for the SQL we just pass as numbers.

  local tn = quote_ident(self.table_name)

  -- Check node exists first (since luadbi doesn't expose rowcount easily)
  local check = self:_query_one(string.format(
    "SELECT node_id FROM %s WHERE node_id = %s",
    tn, quote_literal(node_id)))

  if not check then return false end

  self:_exec(string.format([[
    UPDATE %s
    SET bit_mask = (bit_mask & (~(%d))) | ((%d) & (%d))
    WHERE node_id = %s
  ]], tn, change_mask, new_bits, change_mask, quote_literal(node_id)))

  return true
end

--- Get complete entry for a node.
-- @param node_id string
-- @return table|nil  {node_id, bit_mask}
function BitMaskOperations:get_entry(node_id)
  local tn = quote_ident(self.table_name)

  local row = self:_query_one(string.format(
    "SELECT node_id, bit_mask FROM %s WHERE node_id = %s",
    tn, quote_literal(node_id)))

  if not row then return nil end

  return {
    node_id  = row.node_id,
    bit_mask = tonumber(row.bit_mask),
  }
end

--- Delete an entry.
-- @param node_id string
-- @return boolean  true if deleted
function BitMaskOperations:delete_entry(node_id)
  local tn = quote_ident(self.table_name)

  local check = self:_query_one(string.format(
    "SELECT node_id FROM %s WHERE node_id = %s",
    tn, quote_literal(node_id)))

  if not check then return false end

  self:_exec(string.format(
    "DELETE FROM %s WHERE node_id = %s",
    tn, quote_literal(node_id)))

  return true
end

--- List all node IDs.
-- @return table  array of node_id strings
function BitMaskOperations:list_all_nodes()
  local tn = quote_ident(self.table_name)

  local rows = self:_query(string.format(
    "SELECT node_id FROM %s ORDER BY node_id", tn))

  local result = {}
  for _, row in ipairs(rows) do
    result[#result + 1] = row.node_id
  end
  return result
end

return BitMaskOperations

