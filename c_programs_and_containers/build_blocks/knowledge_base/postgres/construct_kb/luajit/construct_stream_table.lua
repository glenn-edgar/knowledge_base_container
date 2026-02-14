--[[
  construct_stream_table.lua
  
  LuaJIT translation of construct_stream_table.py
  Stream table construction and synchronization with knowledge base.
  
  Dependencies:
    - DBI (luadbi)
    - dkjson
  
  Usage:
    local ConstructStreamTable = require("construct_stream_table")
    local st = ConstructStreamTable.new(conn, construct_kb, database)
]]

local json = require("dkjson")

local Construct_Stream_Table = {}
Construct_Stream_Table.__index = Construct_Stream_Table

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

--- Create a new Construct_Stream_Table.
-- @param conn        DBI connection object
-- @param construct_kb  Construct_KB instance (for add_info_node calls)
-- @param database    string  base table name (knowledge base table)
-- @param upload_flag boolean (default false)
function Construct_Stream_Table.new(conn, construct_kb, database, upload_flag)
  local self = setmetatable({}, Construct_Stream_Table)
  self.conn = conn
  self.construct_kb = construct_kb
  self.database = database
  self.table_name = database .. "_stream"
  self.upload_flag = upload_flag or false

  if not self.upload_flag then
    self:_setup_schema()
  end

  return self
end

---------------------------------------------------------------------------
-- Internal helpers
---------------------------------------------------------------------------

function Construct_Stream_Table:_exec(sql_str)
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

function Construct_Stream_Table:_query(sql_str)
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

function Construct_Stream_Table:_setup_schema()
  local tn = self.table_name

  self:_exec("CREATE EXTENSION IF NOT EXISTS ltree;")
  self:_exec(string.format("DROP TABLE IF EXISTS %s CASCADE;", quote_ident(tn)))

  self:_exec(string.format([[
    CREATE TABLE %s (
      id SERIAL PRIMARY KEY,
      path LTREE,
      recorded_at TIMESTAMPTZ DEFAULT NOW(),
      valid BOOLEAN DEFAULT FALSE,
      data JSONB
    )
  ]], quote_ident(tn)))

  -- Indexes
  local indexes = {
    string.format("CREATE INDEX IF NOT EXISTS %s ON %s USING GIST (path)",
      quote_ident("idx_" .. tn .. "_path_gist"), quote_ident(tn)),
    string.format("CREATE INDEX IF NOT EXISTS %s ON %s (path)",
      quote_ident("idx_" .. tn .. "_path_btree"), quote_ident(tn)),
    string.format("CREATE INDEX IF NOT EXISTS %s ON %s (recorded_at)",
      quote_ident("idx_" .. tn .. "_recorded_at"), quote_ident(tn)),
    string.format("CREATE INDEX IF NOT EXISTS %s ON %s (recorded_at DESC)",
      quote_ident("idx_" .. tn .. "_recorded_at_desc"), quote_ident(tn)),
    string.format("CREATE INDEX IF NOT EXISTS %s ON %s (path, recorded_at)",
      quote_ident("idx_" .. tn .. "_path_recorded_at"), quote_ident(tn)),
  }

  for _, idx_sql in ipairs(indexes) do
    self:_exec(idx_sql)
  end
end

---------------------------------------------------------------------------
-- Public API
---------------------------------------------------------------------------

--- Add a stream field to the knowledge base.
-- @param stream_key string
-- @param stream_length integer
-- @param description string
-- @return table  result summary
function Construct_Stream_Table:add_stream_field(stream_key, stream_length, description)
  assert(type(stream_key) == "string", "stream_key must be a string")
  assert(type(stream_length) == "number", "stream_length must be a number")

  local properties = { stream_length = stream_length }

  self.construct_kb:add_info_node("KB_STREAM_FIELD", stream_key, properties, {}, description)

  return {
    stream = "success",
    message = "stream field '" .. stream_key .. "' added successfully",
    properties = properties,
    data = description,
  }
end

--- Remove entries with invalid paths in chunks.
-- @param invalid_stream_paths table  array of path strings
-- @param chunk_size number (default 500)
function Construct_Stream_Table:_remove_invalid_stream_fields(invalid_stream_paths, chunk_size)
  chunk_size = chunk_size or 500

  if not invalid_stream_paths or #invalid_stream_paths == 0 then
    return
  end

  local tn = quote_ident(self.table_name)

  for i = 1, #invalid_stream_paths, chunk_size do
    local chunk_end = math.min(i + chunk_size - 1, #invalid_stream_paths)
    local literals = {}
    for j = i, chunk_end do
      literals[#literals + 1] = quote_literal(invalid_stream_paths[j])
    end

    self:_exec(string.format("DELETE FROM %s WHERE path IN (%s);",
      tn, table.concat(literals, ",")))
  end
end

--- Manage stream table record counts to match specified lengths.
-- @param specified_stream_paths table   array of path strings
-- @param specified_stream_length table  array of target counts
function Construct_Stream_Table:_manage_stream_table(specified_stream_paths, specified_stream_length)
  local tn = quote_ident(self.table_name)

  for i = 1, #specified_stream_paths do
    local path = specified_stream_paths[i]
    local target_length = specified_stream_length[i]

    -- Get current count
    local rows = self:_query(string.format(
      "SELECT COUNT(*) AS cnt FROM %s WHERE path = %s;",
      tn, quote_literal(path)))

    local current_count = tonumber(rows[1].cnt) or 0
    local diff = target_length - current_count

    if diff < 0 then
      -- Remove oldest records
      self:_exec(string.format([[
        DELETE FROM %s
        WHERE path = %s AND recorded_at IN (
          SELECT recorded_at FROM %s
          WHERE path = %s
          ORDER BY recorded_at ASC
          LIMIT %d
        )
      ]], tn, quote_literal(path),
          tn, quote_literal(path),
          math.abs(diff)))

    elseif diff > 0 then
      -- Add new empty records
      for _ = 1, diff do
        self:_exec(string.format([[
          INSERT INTO %s (path, recorded_at, data, valid)
          VALUES (%s, CURRENT_TIMESTAMP, '{}', FALSE)
        ]], tn, quote_literal(path)))
      end
    end
  end
end

--- Synchronize stream table with knowledge base.
function Construct_Stream_Table:check_installation()
  local tn = quote_ident(self.table_name)
  local db = quote_ident(self.database)

  -- Get all unique paths from stream table
  local stream_rows = self:_query(string.format(
    "SELECT DISTINCT path::text AS path FROM %s;", tn))

  local unique_stream_paths = {}
  local stream_path_set = {}
  for _, row in ipairs(stream_rows) do
    unique_stream_paths[#unique_stream_paths + 1] = row.path
    stream_path_set[row.path] = true
  end

  -- Get specified paths from knowledge base
  local kb_rows = self:_query(string.format([[
    SELECT path, label, name, properties FROM %s
    WHERE label = 'KB_STREAM_FIELD'
  ]], db))

  local specified_stream_paths = {}
  local specified_stream_length = {}
  local specified_set = {}

  for _, row in ipairs(kb_rows) do
    local path_str = tostring(row.path)
    specified_stream_paths[#specified_stream_paths + 1] = path_str
    specified_set[path_str] = true

    -- Parse properties JSON to get stream_length
    local props = row.properties
    if type(props) == "string" then
      props = json.decode(props)
    end
    specified_stream_length[#specified_stream_length + 1] = props.stream_length
  end

  print("specified_stream_paths: " .. table.concat(specified_stream_paths, ", "))
  print("specified_stream_length: " .. table.concat(specified_stream_length, ", "))

  -- Find invalid and missing paths
  local invalid_stream_paths = {}
  for _, path in ipairs(unique_stream_paths) do
    if not specified_set[path] then
      invalid_stream_paths[#invalid_stream_paths + 1] = path
    end
  end

  local missing_stream_paths = {}
  for _, path in ipairs(specified_stream_paths) do
    if not stream_path_set[path] then
      missing_stream_paths[#missing_stream_paths + 1] = path
    end
  end

  print("invalid_stream_paths: " .. table.concat(invalid_stream_paths, ", "))
  print("missing_stream_paths: " .. table.concat(missing_stream_paths, ", "))

  self:_remove_invalid_stream_fields(invalid_stream_paths)
  self:_manage_stream_table(specified_stream_paths, specified_stream_length)
end

return Construct_Stream_Table

