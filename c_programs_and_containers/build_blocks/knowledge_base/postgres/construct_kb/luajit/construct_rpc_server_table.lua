--[[
  construct_rpc_server_table.lua
  
  LuaJIT translation of construct_rpc_server_table.py
  RPC server table construction and synchronization with knowledge base.
  
  Dependencies:
    - DBI (luadbi)
    - dkjson
  
  Usage:
    local ConstructRPCServerTable = require("construct_rpc_server_table")
    local rst = ConstructRPCServerTable.new(conn, construct_kb, database)
]]

local json = require("dkjson")

local Construct_RPC_Server_Table = {}
Construct_RPC_Server_Table.__index = Construct_RPC_Server_Table

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

function Construct_RPC_Server_Table.new(conn, construct_kb, database, upload_flag)
  local self = setmetatable({}, Construct_RPC_Server_Table)
  self.conn = conn
  self.construct_kb = construct_kb
  self.database = database
  self.table_name = database .. "_rpc_server"
  self.upload_flag = upload_flag or false

  if not self.upload_flag then
    self:_setup_schema()
  end

  return self
end

---------------------------------------------------------------------------
-- Internal helpers
---------------------------------------------------------------------------

function Construct_RPC_Server_Table:_exec(sql_str)
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

function Construct_RPC_Server_Table:_query(sql_str)
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

function Construct_RPC_Server_Table:_query_one(sql_str)
  local rows = self:_query(sql_str)
  return rows[1]
end

---------------------------------------------------------------------------
-- Schema setup
---------------------------------------------------------------------------

function Construct_RPC_Server_Table:_setup_schema()
  local tn = self.table_name

  self:_exec(string.format("DROP TABLE IF EXISTS %s CASCADE;", quote_ident(tn)))

  self:_exec(string.format([[
    CREATE TABLE %s (
      id SERIAL PRIMARY KEY,
      server_path LTREE NOT NULL,

      -- Request information
      request_id UUID NOT NULL DEFAULT gen_random_uuid(),
      rpc_action TEXT NOT NULL DEFAULT 'none',
      request_payload JSONB NOT NULL,
      request_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),

      -- Tag to prevent duplicate transactions
      transaction_tag TEXT NOT NULL,

      -- Status tracking
      state TEXT NOT NULL DEFAULT 'empty'
        CHECK (state IN ('empty', 'new_job', 'processing')),

      -- Additional useful fields
      priority INTEGER NOT NULL DEFAULT 0,

      -- Processing fields
      processing_timestamp TIMESTAMPTZ DEFAULT NULL,
      completed_timestamp TIMESTAMPTZ DEFAULT NULL,
      rpc_client_queue LTREE
    )
  ]], quote_ident(tn)))

  print("rpc_server table created.")
end

---------------------------------------------------------------------------
-- Public API
---------------------------------------------------------------------------

--- Add an RPC server field to the knowledge base.
-- @param rpc_server_key string
-- @param queue_depth number
-- @param description string
-- @return table
function Construct_RPC_Server_Table:add_rpc_server_field(rpc_server_key, queue_depth, description)
  assert(type(rpc_server_key) == "string", "rpc_server_key must be a string")
  assert(type(queue_depth) == "number", "queue_depth must be a number")
  assert(type(description) == "string", "description must be a string")

  local properties = { queue_depth = queue_depth }
  local data = {}

  self.construct_kb:add_info_node("KB_RPC_SERVER_FIELD", rpc_server_key, properties, data, description)

  print("Added rpc_server field '" .. rpc_server_key ..
        "' with properties: " .. json.encode(properties) .. " and data: " .. json.encode(data))

  return {
    status = "success",
    message = "RPC server field '" .. rpc_server_key .. "' added successfully",
    properties = properties,
    data = description,
  }
end

--- Remove entries whose server_path is not in the specified list.
-- Uses a temp table for efficient batch processing.
-- @param specified_server_paths table  array of path strings
-- @return number  count of deleted records
function Construct_RPC_Server_Table:remove_unspecified_entries(specified_server_paths)
  if not specified_server_paths or #specified_server_paths == 0 then
    print("Warning: No server_paths specified. No entries will be removed.")
    return 0
  end

  -- Filter nil values
  local valid_paths = {}
  for _, path in ipairs(specified_server_paths) do
    if path ~= nil then
      valid_paths[#valid_paths + 1] = tostring(path)
    end
  end

  if #valid_paths == 0 then
    print("Warning: No valid server_paths found after filtering. No entries will be removed.")
    return 0
  end

  print("Processing " .. #valid_paths .. " valid server paths")

  local tn = quote_ident(self.table_name)

  -- Create and populate temp table
  self:_exec("CREATE TEMP TABLE IF NOT EXISTS valid_server_paths (path text)")
  self:_exec("DELETE FROM valid_server_paths")

  -- Insert in batches
  local batch_size = 1000
  for i = 1, #valid_paths, batch_size do
    local chunk_end = math.min(i + batch_size - 1, #valid_paths)
    for j = i, chunk_end do
      self:_exec(string.format(
        "INSERT INTO valid_server_paths VALUES (%s)",
        quote_literal(valid_paths[j])))
    end
  end

  -- Set state to empty for specified entries
  self:_exec(string.format([[
    UPDATE %s SET state = 'empty'
    WHERE server_path::text IN (SELECT path FROM valid_server_paths)
  ]], tn))

  -- Count before delete (since luadbi doesn't expose rowcount)
  local before = self:_query_one(string.format("SELECT COUNT(*) AS cnt FROM %s", tn))
  local before_count = tonumber(before.cnt) or 0

  -- Delete unspecified entries
  self:_exec(string.format([[
    DELETE FROM %s
    WHERE server_path::text NOT IN (SELECT path FROM valid_server_paths)
  ]], tn))

  local after = self:_query_one(string.format("SELECT COUNT(*) AS cnt FROM %s", tn))
  local after_count = tonumber(after.cnt) or 0
  local deleted_count = before_count - after_count

  -- Cleanup temp table
  pcall(function() self:_exec("DROP TABLE IF EXISTS valid_server_paths") end)

  print("Removed " .. deleted_count .. " unspecified entries from " .. self.table_name)
  return deleted_count
end

--- Adjust queue lengths for each server path.
-- @param specified_server_paths table   array of path strings
-- @param specified_queue_lengths table  array of target counts
-- @return table  results keyed by path
function Construct_RPC_Server_Table:adjust_queue_length(specified_server_paths, specified_queue_lengths)
  local results = {}

  if #specified_server_paths ~= #specified_queue_lengths then
    error("Mismatch between paths and lengths lists")
  end

  local tn = quote_ident(self.table_name)

  for i = 1, #specified_server_paths do
    local server_path = specified_server_paths[i]
    local target_length = tonumber(specified_queue_lengths[i])

    local ok, path_err = pcall(function()
      -- Get current count
      local row = self:_query_one(string.format(
        "SELECT COUNT(*) AS cnt FROM %s WHERE server_path::text = %s",
        tn, quote_literal(server_path)))
      local current_count = tonumber(row.cnt) or 0

      -- Set state to empty
      self:_exec(string.format(
        "UPDATE %s SET state = 'empty' WHERE server_path::text = %s",
        tn, quote_literal(server_path)))

      if current_count > target_length then
        -- Remove excess (oldest first)
        local excess = current_count - target_length
        self:_exec(string.format([[
          DELETE FROM %s
          WHERE id IN (
            SELECT id FROM %s
            WHERE server_path::text = %s
            ORDER BY request_timestamp ASC
            LIMIT %d
          )
        ]], tn, tn, quote_literal(server_path), excess))

        results[server_path] = {
          action = "removed",
          count = excess,
          new_total = target_length,
        }

      elseif current_count < target_length then
        -- Add placeholder records
        local to_add = target_length - current_count
        for _ = 1, to_add do
          self:_exec(string.format([[
            INSERT INTO %s (server_path, request_payload, transaction_tag, state)
            VALUES (%s, '{}', 'placeholder_' || gen_random_uuid()::text, 'empty')
          ]], tn, quote_literal(server_path)))
        end

        results[server_path] = {
          action = "added",
          count = to_add,
          new_total = target_length,
        }

      else
        results[server_path] = {
          action = "unchanged",
          count = 0,
          new_total = current_count,
        }
      end
    end)

    if not ok then
      print("Error adjusting queue for path " .. server_path .. ": " .. tostring(path_err))
      results[server_path] = { error = tostring(path_err) }
    end
  end

  return results
end

--- Restore default values for all fields except server_path.
-- @return number  count of updated records
function Construct_RPC_Server_Table:restore_default_values()
  local tn = quote_ident(self.table_name)

  local rows = self:_query(string.format([[
    UPDATE %s
    SET
      request_id = gen_random_uuid(),
      rpc_action = 'none',
      request_payload = '{}'::jsonb,
      request_timestamp = NOW(),
      transaction_tag = CONCAT('reset_', gen_random_uuid()::text),
      state = 'empty',
      priority = 0,
      processing_timestamp = NULL,
      completed_timestamp = NULL,
      rpc_client_queue = NULL
    RETURNING id
  ]], tn))

  local updated_count = #rows
  print("Restored default values for " .. updated_count .. " records")
  return updated_count
end

--- Synchronize with knowledge base KB_RPC_SERVER_FIELD entries.
function Construct_RPC_Server_Table:check_installation()
  local db = quote_ident(self.database)

  local kb_rows = self:_query(string.format([[
    SELECT path, properties FROM %s
    WHERE label = 'KB_RPC_SERVER_FIELD'
  ]], db))

  local paths = {}
  local lengths = {}

  for _, row in ipairs(kb_rows) do
    local path_str = tostring(row.path)
    paths[#paths + 1] = path_str

    local props = row.properties
    if type(props) == "string" then
      props = json.decode(props)
    end
    lengths[#lengths + 1] = props.queue_depth
  end

  print("paths: " .. table.concat(paths, ", ") ..
        " lengths: " .. table.concat(lengths, ", "))

  self:remove_unspecified_entries(paths)
  self:adjust_queue_length(paths, lengths)
  self:restore_default_values()
end

return Construct_RPC_Server_Table

