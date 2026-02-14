--[[
  construct_jsonb_table.lua
  
  LuaJIT translation of construct_jsonb_table.py
  PostgreSQL table manager using ltree for hierarchical paths and jsonb for document storage.
  
  Dependencies:
    - DBI (luadbi)
    - dkjson
  
  Usage:
    local ConstructJsonbTable = require("construct_jsonb_table")
    local jt = ConstructJsonbTable.new(conn, construct_kb, database)
]]

local json = require("dkjson")

local Construct_Jsonb_Table = {}
Construct_Jsonb_Table.__index = Construct_Jsonb_Table

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

--- Create a new Construct_Jsonb_Table.
-- @param conn          DBI connection object
-- @param construct_kb  Construct_KB instance
-- @param database      string  base table name (knowledge base table)
-- @param upload_flag   boolean (default false)
function Construct_Jsonb_Table.new(conn, construct_kb, database, upload_flag)
  local self = setmetatable({}, Construct_Jsonb_Table)
  self.conn = conn
  self.construct_kb = construct_kb
  self.database = database
  self.table_name = database .. "_document"
  self.upload_flag = upload_flag or false

  self:_enable_ltree_extension()
  if not self.upload_flag then
    self:_create_table()
  end

  return self
end

---------------------------------------------------------------------------
-- Internal helpers
---------------------------------------------------------------------------

function Construct_Jsonb_Table:_exec(sql_str)
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

function Construct_Jsonb_Table:_query(sql_str)
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

--- Execute a query and return the first row, or nil.
function Construct_Jsonb_Table:_query_one(sql_str)
  local rows = self:_query(sql_str)
  return rows[1]
end

---------------------------------------------------------------------------
-- Schema setup
---------------------------------------------------------------------------

function Construct_Jsonb_Table:_enable_ltree_extension()
  self:_exec("CREATE EXTENSION IF NOT EXISTS ltree;")
end

function Construct_Jsonb_Table:_create_table()
  local tn = self.table_name

  self:_exec("CREATE EXTENSION IF NOT EXISTS ltree;")
  self:_exec(string.format("DROP TABLE IF EXISTS %s CASCADE;", quote_ident(tn)))

  self:_exec(string.format([[
    CREATE TABLE IF NOT EXISTS %s (
      id SERIAL PRIMARY KEY,
      ltree LTREE NOT NULL UNIQUE,
      type TEXT,
      data JSONB DEFAULT '{}'::jsonb,
      locked_by TEXT,
      locked_at TIMESTAMP,
      lock_expires TIMESTAMP,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  ]], quote_ident(tn)))

  self:_exec(string.format("CREATE INDEX IF NOT EXISTS %s ON %s USING GIST (ltree)",
    quote_ident(tn .. "_ltree_idx"), quote_ident(tn)))
  self:_exec(string.format("CREATE INDEX IF NOT EXISTS %s ON %s USING GIN (data)",
    quote_ident(tn .. "_data_idx"), quote_ident(tn)))
  self:_exec(string.format("CREATE INDEX IF NOT EXISTS %s ON %s (type)",
    quote_ident(tn .. "_type_idx"), quote_ident(tn)))
end

---------------------------------------------------------------------------
-- Public API
---------------------------------------------------------------------------

--- Add a jsonb field to the knowledge base.
-- @param jsonb_key string
-- @param doc_type string
-- @param description string
-- @param data table (default {})
-- @return table  result summary
function Construct_Jsonb_Table:add_jsonb_field(jsonb_key, doc_type, description, data)
  data = data or {}
  assert(type(jsonb_key) == "string", "jsonb_key must be a string")
  assert(type(doc_type) == "string", "type must be a string")
  assert(type(description) == "string", "description must be a string")

  local properties = { type = doc_type }

  self.construct_kb:add_info_node("KB_JSONB_FIELD", jsonb_key, properties, data, description)

  return {
    jsonb = "success",
    message = "jsonb field '" .. jsonb_key .. "' added successfully",
    properties = properties,
    data = data,
  }
end

--- Add a new record to the table.
-- @param ltree_path string
-- @param doc_type string|nil
-- @param data table|nil
-- @return number  the id of the new record
function Construct_Jsonb_Table:add_record(ltree_path, doc_type, data)
  local data_json = json.encode(data or {})
  local tn = quote_ident(self.table_name)

  local type_val = doc_type and quote_literal(doc_type) or "NULL"

  -- Insert and return the id
  local rows = self:_query(string.format([[
    INSERT INTO %s (ltree, type, data)
    VALUES (%s::ltree, %s, %s::jsonb)
    RETURNING id
  ]], tn, quote_literal(ltree_path), type_val, quote_literal(data_json)))

  if not rows or #rows == 0 then
    error("Failed to add record: no id returned")
  end

  return tonumber(rows[1].id)
end

--- Delete a record by id.
-- @param record_id number
-- @return boolean  true if deleted
function Construct_Jsonb_Table:delete_record(record_id)
  local tn = quote_ident(self.table_name)

  -- Check existence first, then delete
  local check = self:_query(string.format(
    "SELECT id FROM %s WHERE id = %d", tn, record_id))

  if #check == 0 then
    return false
  end

  self:_exec(string.format("DELETE FROM %s WHERE id = %d;", tn, record_id))
  return true
end

--- List all ltree paths and their ids.
-- @return table  array of {id, ltree, type}
function Construct_Jsonb_Table:list_ltree_ids()
  local tn = quote_ident(self.table_name)

  return self:_query(string.format([[
    SELECT id, ltree::text AS ltree, type
    FROM %s
    ORDER BY ltree
  ]], tn))
end

--- Get a record by id.
-- @param record_id number
-- @return table|nil  record dict or nil
function Construct_Jsonb_Table:get_record(record_id)
  local tn = quote_ident(self.table_name)

  return self:_query_one(string.format([[
    SELECT id, ltree::text AS ltree, type, data,
           locked_by, locked_at, lock_expires,
           created_at, updated_at
    FROM %s
    WHERE id = %d
  ]], tn, record_id))
end

--- Query records using ltree lquery pattern matching.
-- @param ltree_pattern string  e.g. 'root.*'
-- @return table  array of matching records
function Construct_Jsonb_Table:query_by_ltree(ltree_pattern)
  local tn = quote_ident(self.table_name)

  return self:_query(string.format([[
    SELECT id, ltree::text AS ltree, type, data
    FROM %s
    WHERE ltree ~ %s::lquery
    ORDER BY ltree
  ]], tn, quote_literal(ltree_pattern)))
end

--- Synchronize table records with a target list of ltree paths.
-- Adds missing paths and deletes paths not in the target list.
-- @param target_paths table       array of path strings
-- @param default_type table|nil   path -> type mapping
-- @param default_data table|nil   default data for new records
-- @return table  summary with added/deleted info
function Construct_Jsonb_Table:sync_ltree_paths(target_paths, default_type, default_data)
  default_data = default_data or {}

  -- Get current records
  local current_records = self:list_ltree_ids()

  -- Build sets
  local current_path_set = {}
  local path_to_id = {}
  for _, rec in ipairs(current_records) do
    current_path_set[rec.ltree] = true
    path_to_id[rec.ltree] = rec.id
  end

  local target_path_set = {}
  for _, p in ipairs(target_paths) do
    target_path_set[p] = true
  end

  -- Determine adds and deletes
  local paths_to_add = {}
  for _, p in ipairs(target_paths) do
    if not current_path_set[p] then
      paths_to_add[#paths_to_add + 1] = p
    end
  end
  table.sort(paths_to_add)

  local paths_to_delete = {}
  for _, rec in ipairs(current_records) do
    if not target_path_set[rec.ltree] then
      paths_to_delete[#paths_to_delete + 1] = rec.ltree
    end
  end
  table.sort(paths_to_delete)

  local added_records = {}
  local deleted_records = {}

  -- Add missing paths
  for _, path in ipairs(paths_to_add) do
    local doc_type = default_type and default_type[path] or nil
    local record_id = self:add_record(path, doc_type, default_data)
    added_records[#added_records + 1] = { id = record_id, ltree = path }
  end

  -- Delete extra paths
  for _, path in ipairs(paths_to_delete) do
    local record_id = path_to_id[path]
    if record_id and self:delete_record(record_id) then
      deleted_records[#deleted_records + 1] = { id = record_id, ltree = path }
    end
  end

  return {
    added = added_records,
    deleted = deleted_records,
    summary = {
      added_count = #added_records,
      deleted_count = #deleted_records,
      total_records = #current_records - #deleted_records + #added_records,
    },
  }
end

--- Synchronize with knowledge base KB_JSONB_FIELD entries.
function Construct_Jsonb_Table:check_installation()
  local db = quote_ident(self.database)

  -- Get KB_JSONB_FIELD entries from knowledge base
  local kb_rows = self:_query(string.format([[
    SELECT path, properties FROM %s
    WHERE label = 'KB_JSONB_FIELD'
  ]], db))

  local paths = {}
  local types = {}

  for _, row in ipairs(kb_rows) do
    local path_str = tostring(row.path)
    paths[#paths + 1] = path_str

    local props = row.properties
    if type(props) == "string" then
      props = json.decode(props)
    end
    types[path_str] = props.type
  end

  self:sync_ltree_paths(paths, types)
end

return Construct_Jsonb_Table

