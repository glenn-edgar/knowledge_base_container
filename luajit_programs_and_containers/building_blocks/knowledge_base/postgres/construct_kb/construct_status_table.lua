--[[
  construct_status_table.lua
  
  LuaJIT translation of construct_status_table.py
  Status table construction and synchronization with knowledge base.
  
  Dependencies:
    - DBI (luadbi)
    - dkjson
  
  Usage:
    local ConstructStatusTable = require("construct_status_table")
    local st = ConstructStatusTable.new(conn, construct_kb, database)
]]

local json = require("dkjson")

local Construct_Status_Table = {}
Construct_Status_Table.__index = Construct_Status_Table

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

--- Create a new Construct_Status_Table.
-- @param conn        DBI connection object
-- @param construct_kb  Construct_KB instance
-- @param database    string  base table name
-- @param upload_flag boolean (default false)
function Construct_Status_Table.new(conn, construct_kb, database, upload_flag)
  local self = setmetatable({}, Construct_Status_Table)
  self.conn = conn
  self.construct_kb = construct_kb
  self.database = database
  self.table_name = database .. "_status"
  self.upload_flag = upload_flag or false

  print("database: " .. self.database)

  if not self.upload_flag then
    self:_setup_schema()
  end

  return self
end

---------------------------------------------------------------------------
-- Internal helpers
---------------------------------------------------------------------------

function Construct_Status_Table:_exec(sql_str)
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

function Construct_Status_Table:_query(sql_str)
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

---------------------------------------------------------------------------
-- Schema setup
---------------------------------------------------------------------------

function Construct_Status_Table:_setup_schema()
  local tn = self.table_name

  self:_exec("CREATE EXTENSION IF NOT EXISTS ltree;")
  self:_exec(string.format("DROP TABLE IF EXISTS %s CASCADE;", quote_ident(tn)))

  self:_exec(string.format([[
    CREATE TABLE %s (
      id SERIAL PRIMARY KEY,
      data JSON,
      path LTREE UNIQUE
    )
  ]], quote_ident(tn)))

  -- Indexes
  self:_exec(string.format("CREATE INDEX IF NOT EXISTS %s ON %s USING GIST (path)",
    quote_ident("idx_" .. tn .. "_path_gist"), quote_ident(tn)))
  self:_exec(string.format("CREATE INDEX IF NOT EXISTS %s ON %s (path)",
    quote_ident("idx_" .. tn .. "_path_btree"), quote_ident(tn)))

  print("Status table '" .. tn .. "' created with optimized indexes.")
end

---------------------------------------------------------------------------
-- Public API
---------------------------------------------------------------------------

--- Add a status field to the knowledge base.
-- @param status_key string
-- @param properties table
-- @param description string
-- @param initial_data table
-- @return table  result summary
function Construct_Status_Table:add_status_field(status_key, properties, description, initial_data)
  assert(type(status_key) == "string", "status_key must be a string")
  assert(type(description) == "string", "description must be a string")
  assert(type(initial_data) == "table", "initial_data must be a table")

  if properties == nil then
    properties = {}
  end
  assert(type(properties) == "table", "properties must be a table")

  print("Added status field '" .. status_key ..
        "' with properties: " .. json.encode(properties) ..
        " and data: " .. json.encode(initial_data))

  self.construct_kb:add_info_node("KB_STATUS_FIELD", status_key, properties, initial_data, description)

  return {
    status = "success",
    message = "Status field '" .. status_key .. "' added successfully",
    properties = properties,
    data = initial_data,
  }
end

--- Synchronize status table with knowledge base.
-- @return table  summary of changes
function Construct_Status_Table:check_installation()
  local tn = quote_ident(self.table_name)
  local db = quote_ident(self.database)

  -- Get all paths from status table
  local status_rows = self:_query(string.format("SELECT path FROM %s;", tn))
  local all_paths = {}
  local all_paths_set = {}
  for _, row in ipairs(status_rows) do
    local p = tostring(row.path)
    all_paths[#all_paths + 1] = p
    all_paths_set[p] = true
  end

  -- Get specified paths from knowledge base
  local kb_rows = self:_query(string.format([[
    SELECT path FROM %s WHERE label = 'KB_STATUS_FIELD'
  ]], db))

  local specified_paths = {}
  local specified_set = {}
  for _, row in ipairs(kb_rows) do
    local p = tostring(row.path)
    specified_paths[#specified_paths + 1] = p
    specified_set[p] = true
  end
  print("specified_paths: " .. table.concat(specified_paths, ", "))

  -- Find missing paths (in specified but not in status table)
  local missing_paths = {}
  for _, path in ipairs(specified_paths) do
    if not all_paths_set[path] then
      missing_paths[#missing_paths + 1] = path
    end
  end
  print("missing_paths: " .. table.concat(missing_paths, ", "))

  -- Find not-specified paths (in status table but not in knowledge base)
  local not_specified_paths = {}
  for _, path in ipairs(all_paths) do
    if not specified_set[path] then
      not_specified_paths[#not_specified_paths + 1] = path
    end
  end
  print("not_specified_paths: " .. table.concat(not_specified_paths, ", "))

  -- Delete not-specified entries
  for _, path in ipairs(not_specified_paths) do
    print("deleting path: " .. path)
    self:_exec(string.format("DELETE FROM %s WHERE path = %s;",
      tn, quote_literal(path)))
  end

  -- Insert missing entries
  for _, path in ipairs(missing_paths) do
    print("inserting path: " .. path)
    self:_exec(string.format([[
      INSERT INTO %s (data, path) VALUES ('{}', %s)
    ]], tn, quote_literal(path)))
  end

  return {
    missing_paths_added = #missing_paths,
    not_specified_paths_removed = #not_specified_paths,
  }
end

return Construct_Status_Table

