--[[
  construct_rpc_client_table.lua
  
  LuaJIT translation of construct_rpc_client_table.py
  RPC client table construction and synchronization with knowledge base.
  
  Dependencies:
    - DBI (luadbi)
    - dkjson
  
  Usage:
    local ConstructRPCClientTable = require("construct_rpc_client_table")
    local rct = ConstructRPCClientTable.new(conn, construct_kb, database)
]]

local json = require("dkjson")

local Construct_RPC_Client_Table = {}
Construct_RPC_Client_Table.__index = Construct_RPC_Client_Table

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

function Construct_RPC_Client_Table.new(conn, construct_kb, database, upload_flag)
  local self = setmetatable({}, Construct_RPC_Client_Table)
  self.conn = conn
  self.construct_kb = construct_kb
  self.database = database
  self.table_name = database .. "_rpc_client"
  self.upload_flag = upload_flag or false

  if not self.upload_flag then
    self:_setup_schema()
  end

  return self
end

---------------------------------------------------------------------------
-- Internal helpers
---------------------------------------------------------------------------

function Construct_RPC_Client_Table:_exec(sql_str)
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

function Construct_RPC_Client_Table:_query(sql_str)
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

function Construct_RPC_Client_Table:_query_one(sql_str)
  local rows = self:_query(sql_str)
  return rows[1]
end

---------------------------------------------------------------------------
-- Schema setup
---------------------------------------------------------------------------

function Construct_RPC_Client_Table:_setup_schema()
  local tn = self.table_name

  self:_exec("CREATE EXTENSION IF NOT EXISTS ltree;")
  self:_exec(string.format("DROP TABLE IF EXISTS %s CASCADE;", quote_ident(tn)))

  self:_exec(string.format([[
    CREATE TABLE %s (
      id SERIAL PRIMARY KEY,

      -- Reference to the request
      request_id UUID NOT NULL,

      -- Path to identify the RPC client queue for routing responses
      client_path LTREE NOT NULL,
      server_path LTREE NOT NULL,

      -- Response information
      transaction_tag TEXT NOT NULL DEFAULT 'none',
      rpc_action TEXT NOT NULL DEFAULT 'none',
      response_payload JSONB NOT NULL,
      response_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),

      -- Boolean to identify new/unprocessed results
      is_new_result BOOLEAN NOT NULL DEFAULT FALSE
    )
  ]], quote_ident(tn)))

  print("rpc_client table created.")
end

---------------------------------------------------------------------------
-- Public API
---------------------------------------------------------------------------

--- Add an RPC client field to the knowledge base.
-- @param rpc_client_key string
-- @param queue_depth number
-- @param description string
-- @return table
function Construct_RPC_Client_Table:add_rpc_client_field(rpc_client_key, queue_depth, description)
  assert(type(rpc_client_key) == "string", "rpc_client_key must be a string")
  assert(type(description) == "string", "description must be a string")
  assert(type(queue_depth) == "number", "queue_depth must be a number")

  local properties = { queue_depth = queue_depth }

  self.construct_kb:add_info_node("KB_RPC_CLIENT_FIELD", rpc_client_key, properties, {}, description)

  print("Added rpc_client field '" .. rpc_client_key ..
        "' with properties: " .. json.encode(properties))

  return {
    rpc_client = "success",
    message = "rpc_client field '" .. rpc_client_key .. "' added successfully",
    properties = properties,
    data = description,
  }
end

--- Remove entries whose client_path is not in the specified list.
-- @param specified_client_paths table  array of path strings
-- @return number  count of deleted records
function Construct_RPC_Client_Table:remove_unspecified_entries(specified_client_paths)
  if not specified_client_paths or #specified_client_paths == 0 then
    print("Warning: No client_paths specified. No entries will be removed.")
    return 0
  end

  -- Filter nil values
  local valid_paths = {}
  for _, path in ipairs(specified_client_paths) do
    if path ~= nil then
      valid_paths[#valid_paths + 1] = tostring(path)
    end
  end

  if #valid_paths == 0 then
    print("Warning: No valid client_paths found after filtering. No entries will be removed.")
    return 0
  end

  print("Processing " .. #valid_paths .. " valid client paths")

  local tn = quote_ident(self.table_name)

  -- Create and populate temp table
  self:_exec("CREATE TEMP TABLE IF NOT EXISTS valid_client_paths (path text)")
  self:_exec("DELETE FROM valid_client_paths")

  -- Insert in batches
  local batch_size = 1000
  for i = 1, #valid_paths, batch_size do
    local chunk_end = math.min(i + batch_size - 1, #valid_paths)
    for j = i, chunk_end do
      self:_exec(string.format(
        "INSERT INTO valid_client_paths VALUES (%s)",
        quote_literal(valid_paths[j])))
    end
  end

  -- Count before delete
  local before = self:_query_one(string.format("SELECT COUNT(*) AS cnt FROM %s", tn))
  local before_count = tonumber(before.cnt) or 0

  -- Delete unspecified entries
  self:_exec(string.format([[
    DELETE FROM %s
    WHERE client_path::text NOT IN (SELECT path FROM valid_client_paths)
  ]], tn))

  local after = self:_query_one(string.format("SELECT COUNT(*) AS cnt FROM %s", tn))
  local after_count = tonumber(after.cnt) or 0
  local deleted_count = before_count - after_count

  -- Cleanup temp table
  pcall(function() self:_exec("DROP TABLE IF EXISTS valid_client_paths") end)

  print("Removed " .. deleted_count .. " unspecified entries from " .. self.table_name)
  return deleted_count
end

--- Adjust queue lengths for each client path.
-- @param specified_client_paths table   array of path strings
-- @param specified_queue_lengths table  array of target counts
-- @return table  results keyed by path
function Construct_RPC_Client_Table:adjust_queue_length(specified_client_paths, specified_queue_lengths)
  if #specified_client_paths ~= #specified_queue_lengths then
    error("The specified_client_paths and specified_queue_lengths lists must be of equal length")
  end

  local results = {}
  local tn = quote_ident(self.table_name)

  for i = 1, #specified_client_paths do
    local client_path = specified_client_paths[i]
    local queue_length = tonumber(specified_queue_lengths[i])

    if queue_length < 0 then
      results[client_path] = { error = "Invalid queue length (negative)" }
    else
      -- Count current records
      local row = self:_query_one(string.format(
        "SELECT COUNT(*) AS cnt FROM %s WHERE client_path = %s::ltree",
        tn, quote_literal(client_path)))
      local current_count = tonumber(row.cnt) or 0

      local path_result = { added = 0, removed = 0 }

      if current_count > queue_length then
        -- Remove excess (oldest first)
        local to_remove = current_count - queue_length
        local deleted = self:_query(string.format([[
          DELETE FROM %s
          WHERE id IN (
            SELECT id FROM %s
            WHERE client_path = %s::ltree
            ORDER BY response_timestamp ASC
            LIMIT %d
          )
          RETURNING id
        ]], tn, tn, quote_literal(client_path), to_remove))
        path_result.removed = #deleted

      elseif current_count < queue_length then
        -- Add placeholder records
        local to_add = queue_length - current_count
        for _ = 1, to_add do
          self:_exec(string.format([[
            INSERT INTO %s (
              request_id, client_path, server_path,
              transaction_tag, rpc_action,
              response_payload, response_timestamp, is_new_result
            )
            VALUES (
              gen_random_uuid(), %s::ltree, %s::ltree,
              'none', 'none',
              '{}'::jsonb, NOW(), FALSE
            )
          ]], tn,
              quote_literal(client_path),
              quote_literal(client_path)))
          path_result.added = path_result.added + 1
        end
      end

      results[client_path] = path_result
    end
  end

  return results
end

--- Restore default values for all fields except client_path.
-- @return number  count of updated records
function Construct_RPC_Client_Table:restore_default_values()
  local tn = quote_ident(self.table_name)

  local rows = self:_query(string.format([[
    UPDATE %s
    SET
      request_id = (SELECT gen_random_uuid()),
      server_path = client_path,
      transaction_tag = 'none',
      rpc_action = 'none',
      response_payload = '{}'::jsonb,
      response_timestamp = NOW(),
      is_new_result = FALSE
    RETURNING id
  ]], tn))

  local updated_count = #rows
  return updated_count
end

--- Synchronize with knowledge base KB_RPC_CLIENT_FIELD entries.
function Construct_RPC_Client_Table:check_installation()
  local db = quote_ident(self.database)

  local kb_rows = self:_query(string.format([[
    SELECT path, properties FROM %s
    WHERE label = 'KB_RPC_CLIENT_FIELD'
  ]], db))

  local paths = {}
  local lengths = {}

  print("specified_paths_data count: " .. #kb_rows)

  for _, row in ipairs(kb_rows) do
    local path_str = tostring(row.path)
    paths[#paths + 1] = path_str

    local props = row.properties
    if type(props) == "string" then
      props = json.decode(props)
    end
    lengths[#lengths + 1] = props.queue_depth
  end

  self:remove_unspecified_entries(paths)
  self:adjust_queue_length(paths, lengths)
  self:restore_default_values()
end

return Construct_RPC_Client_Table

