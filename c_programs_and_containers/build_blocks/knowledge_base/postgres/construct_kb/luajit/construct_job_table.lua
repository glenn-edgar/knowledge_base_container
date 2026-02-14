--[[
  construct_job_table.lua
  
  LuaJIT translation of construct_job_table.py
  Job table construction and synchronization with knowledge base.
  
  Dependencies:
    - DBI (luadbi)
    - dkjson
  
  Usage:
    local ConstructJobTable = require("construct_job_table")
    local jt = ConstructJobTable.new(conn, construct_kb, database)
]]

local json = require("dkjson")

local Construct_Job_Table = {}
Construct_Job_Table.__index = Construct_Job_Table

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

--- Create a new Construct_Job_Table.
-- @param conn        DBI connection object
-- @param construct_kb  Construct_KB instance
-- @param database    string  base table name
-- @param upload_flag boolean (default false)
function Construct_Job_Table.new(conn, construct_kb, database, upload_flag)
  local self = setmetatable({}, Construct_Job_Table)
  self.conn = conn
  self.construct_kb = construct_kb
  self.database = database
  self.table_name = database .. "_job"
  self.upload_flag = upload_flag or false

  if not self.upload_flag then
    self:_setup_schema()
  end

  return self
end

---------------------------------------------------------------------------
-- Internal helpers
---------------------------------------------------------------------------

function Construct_Job_Table:_exec(sql_str)
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

function Construct_Job_Table:_query(sql_str)
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

function Construct_Job_Table:_setup_schema()
  local tn = self.table_name

  self:_exec("CREATE EXTENSION IF NOT EXISTS ltree;")
  self:_exec(string.format("DROP TABLE IF EXISTS %s CASCADE;", quote_ident(tn)))

  self:_exec(string.format([[
    CREATE TABLE %s (
      id SERIAL PRIMARY KEY,
      path LTREE,
      schedule_at TIMESTAMPTZ DEFAULT NOW(),
      started_at TIMESTAMPTZ DEFAULT NOW(),
      completed_at TIMESTAMPTZ DEFAULT NOW(),
      is_active BOOLEAN DEFAULT FALSE,
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
    string.format("CREATE INDEX IF NOT EXISTS %s ON %s (schedule_at)",
      quote_ident("idx_" .. tn .. "_schedule_at"), quote_ident(tn)),
    string.format("CREATE INDEX IF NOT EXISTS %s ON %s (is_active)",
      quote_ident("idx_" .. tn .. "_is_active"), quote_ident(tn)),
    string.format("CREATE INDEX IF NOT EXISTS %s ON %s (valid)",
      quote_ident("idx_" .. tn .. "_valid"), quote_ident(tn)),
    string.format("CREATE INDEX IF NOT EXISTS %s ON %s (is_active, schedule_at)",
      quote_ident("idx_" .. tn .. "_active_schedule"), quote_ident(tn)),
    string.format("CREATE INDEX IF NOT EXISTS %s ON %s (started_at)",
      quote_ident("idx_" .. tn .. "_started_at"), quote_ident(tn)),
    string.format("CREATE INDEX IF NOT EXISTS %s ON %s (completed_at)",
      quote_ident("idx_" .. tn .. "_completed_at"), quote_ident(tn)),
  }

  for _, idx_sql in ipairs(indexes) do
    self:_exec(idx_sql)
  end

  print("Job table '" .. tn .. "' created with optimized indexes.")
end

---------------------------------------------------------------------------
-- Public API
---------------------------------------------------------------------------

--- Add a job field to the knowledge base.
-- @param job_key string
-- @param job_length number
-- @param description string
-- @return table  result summary
function Construct_Job_Table:add_job_field(job_key, job_length, description)
  assert(type(job_key) == "string", "job_key must be a string")
  assert(type(job_length) == "number", "job_length must be a number")

  local properties = { job_length = job_length }
  local data = {}

  self.construct_kb:add_info_node("KB_JOB_QUEUE", job_key, properties, data, description)

  print("Added job field '" .. job_key ..
        "' with properties: " .. json.encode(properties) ..
        " and data: " .. json.encode(data))

  return {
    job = "success",
    message = "job field '" .. job_key .. "' added successfully",
    properties = properties,
    data = data,
  }
end

--- Remove entries with invalid paths in chunks.
-- @param invalid_job_paths table  array of path strings
-- @param chunk_size number (default 500)
function Construct_Job_Table:_remove_invalid_job_fields(invalid_job_paths, chunk_size)
  chunk_size = chunk_size or 500

  if not invalid_job_paths or #invalid_job_paths == 0 then
    return
  end

  local tn = quote_ident(self.table_name)

  for i = 1, #invalid_job_paths, chunk_size do
    local chunk_end = math.min(i + chunk_size - 1, #invalid_job_paths)
    local literals = {}
    for j = i, chunk_end do
      literals[#literals + 1] = quote_literal(invalid_job_paths[j])
    end

    self:_exec(string.format("DELETE FROM %s WHERE path IN (%s);",
      tn, table.concat(literals, ",")))
  end
end

--- Manage job table record counts to match specified lengths.
-- @param specified_job_paths table   array of path strings
-- @param specified_job_length table  array of target counts
function Construct_Job_Table:_manage_job_table(specified_job_paths, specified_job_length)
  local tn = quote_ident(self.table_name)

  print("specified_job_paths: " .. table.concat(specified_job_paths, ", "))
  print("specified_job_length: " .. table.concat(specified_job_length, ", "))

  for i = 1, #specified_job_paths do
    local path = specified_job_paths[i]
    local target_length = specified_job_length[i]

    -- Get current count
    local rows = self:_query(string.format(
      "SELECT COUNT(*) AS cnt FROM %s WHERE path = %s;",
      tn, quote_literal(path)))

    local current_count = tonumber(rows[1].cnt) or 0
    print("current_count: " .. current_count)

    local diff = target_length - current_count

    if diff < 0 then
      -- Remove oldest records
      self:_exec(string.format([[
        DELETE FROM %s
        WHERE path = %s AND completed_at IN (
          SELECT completed_at FROM %s
          WHERE path = %s
          ORDER BY completed_at ASC
          LIMIT %d
        )
      ]], tn, quote_literal(path),
          tn, quote_literal(path),
          math.abs(diff)))

    elseif diff > 0 then
      -- Add new empty records
      for _ = 1, diff do
        self:_exec(string.format([[
          INSERT INTO %s (path, data) VALUES (%s, NULL)
        ]], tn, quote_literal(path)))
      end
    end
  end

  print("Job table management completed.")
end

--- Synchronize job table with knowledge base.
function Construct_Job_Table:check_installation()
  local tn = quote_ident(self.table_name)
  local db = quote_ident(self.database)

  -- Get all unique paths from job table
  local job_rows = self:_query(string.format(
    "SELECT DISTINCT path::text AS path FROM %s;", tn))

  local unique_job_paths = {}
  local job_path_set = {}
  for _, row in ipairs(job_rows) do
    unique_job_paths[#unique_job_paths + 1] = row.path
    job_path_set[row.path] = true
  end
  print("unique_job_paths: " .. table.concat(unique_job_paths, ", "))

  -- Get specified paths from knowledge base
  local kb_rows = self:_query(string.format([[
    SELECT path, label, name, properties FROM %s
    WHERE label = 'KB_JOB_QUEUE'
  ]], db))

  local specified_job_paths = {}
  local specified_job_length = {}
  local specified_set = {}

  for _, row in ipairs(kb_rows) do
    local path_str = tostring(row.path)
    specified_job_paths[#specified_job_paths + 1] = path_str
    specified_set[path_str] = true

    local props = row.properties
    if type(props) == "string" then
      props = json.decode(props)
    end
    specified_job_length[#specified_job_length + 1] = props.job_length
  end

  print("specified_job_paths: " .. table.concat(specified_job_paths, ", "))
  print("specified_job_length: " .. table.concat(specified_job_length, ", "))

  -- Find invalid paths
  local invalid_job_paths = {}
  for _, path in ipairs(unique_job_paths) do
    if not specified_set[path] then
      invalid_job_paths[#invalid_job_paths + 1] = path
    end
  end

  self:_remove_invalid_job_fields(invalid_job_paths)
  self:_manage_job_table(specified_job_paths, specified_job_length)
end

return Construct_Job_Table

