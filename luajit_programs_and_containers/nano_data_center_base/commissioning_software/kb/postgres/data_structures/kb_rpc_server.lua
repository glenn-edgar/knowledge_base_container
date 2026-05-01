--[[
  KB_RPC_Server - RPC server request queue operations.
  LuaJIT port using luadbi-postgresql and dkjson.

  Usage:
    local KB_Search     = require("kb_search")
    local KB_RPC_Server = require("kb_rpc_server")
    local kb  = KB_Search.new({ ... })
    local rpc = KB_RPC_Server.new(kb, "my_database")
]]

local dkjson = require("dkjson")

local KB_RPC_Server = {}
KB_RPC_Server.__index = KB_RPC_Server

--- Custom error for no matching record.
local NoMatchingRecordError = {}
NoMatchingRecordError.__index = NoMatchingRecordError
function NoMatchingRecordError.new(msg)
  return setmetatable({ message = msg }, NoMatchingRecordError)
end
function NoMatchingRecordError:__tostring()
  return "NoMatchingRecordError: " .. self.message
end
KB_RPC_Server.NoMatchingRecordError = NoMatchingRecordError

function KB_RPC_Server.new(kb_search, database)
  local self = setmetatable({}, KB_RPC_Server)
  self.kb_search  = kb_search
  self.dbh        = kb_search:get_connection()
  self.base_table = database .. "_rpc_server"
  return self
end

---------------------------------------------------------------------------
-- Helpers
---------------------------------------------------------------------------

local function esc(val)
  if val == nil then return "NULL" end
  local s = tostring(val)
  s = s:gsub("'", "''")
  return "'" .. s .. "'"
end

local function sleep(sec)
  local ok, socket = pcall(require, "socket")
  if ok then socket.sleep(sec) else
    local t = os.clock() + sec
    while os.clock() < t do end
  end
end

--- Validate ltree path format.
local function is_valid_ltree(path)
  if type(path) ~= "string" or path == "" then return false end
  for part in path:gmatch("[^.]+") do
    if part == "" then return false end
    if not part:sub(1,1):match("[%a_]") then return false end
    if not part:match("^[%w_]+$") then return false end
  end
  return true
end

local function uuid4()
  local template = "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx"
  math.randomseed(os.clock() * 1e6 + os.time())
  return (template:gsub("[xy]", function(c)
    local v = (c == "x") and math.random(0, 0xf) or math.random(8, 0xb)
    return string.format("%x", v)
  end))
end

---------------------------------------------------------------------------
-- Node discovery
---------------------------------------------------------------------------

function KB_RPC_Server:find_rpc_server_id(kb, node_name, properties, node_path)
  local results = self:find_rpc_server_ids(kb, node_name, properties, node_path)
  if #results == 0 then
    error(string.format("No RPC server found: name=%s", tostring(node_name)))
  end
  if #results > 1 then
    error(string.format("Multiple RPC servers (%d) found: name=%s", #results, tostring(node_name)))
  end
  return results
end

function KB_RPC_Server:find_rpc_server_ids(kb, node_name, properties, node_path)
  self.kb_search:clear_filters()
  self.kb_search:search_label("KB_RPC_SERVER_FIELD")

  if kb        then self.kb_search:search_kb(kb) end
  if node_name then self.kb_search:search_name(node_name) end
  if properties and type(properties) == "table" then
    for k, v in pairs(properties) do
      self.kb_search:search_property_value(k, v)
    end
  end
  if node_path then self.kb_search:search_path(node_path) end

  local rows = self.kb_search:execute_query()
  if not rows or #rows == 0 then
    error(string.format("No RPC server found: name=%s", tostring(node_name)))
  end
  return rows
end

function KB_RPC_Server:find_rpc_server_table_keys(key_data)
  local rv = {}
  for _, row in ipairs(key_data) do
    local p = row.path
    if p ~= nil then p = tostring(p) end
    rv[#rv + 1] = p
  end
  return rv
end

---------------------------------------------------------------------------
-- Job counting
---------------------------------------------------------------------------

function KB_RPC_Server:count_jobs_job_types(server_path, state)
  if not is_valid_ltree(server_path) then
    error("server_path must be a valid ltree format")
  end
  local valid = { empty = true, new_job = true, processing = true, completed_job = true }
  if not valid[state] then
    error("state must be one of: empty, new_job, processing, completed_job")
  end

  local sql_str = string.format([[
    SELECT COUNT(*) AS job_count
      FROM %s
     WHERE server_path = %s::ltree AND state = %s
  ]], self.base_table, esc(server_path), esc(state))

  local row = self.kb_search:_raw_query_one(sql_str)
  return row and tonumber(row.job_count) or 0
end

function KB_RPC_Server:count_empty_jobs(server_path)
  return self:count_jobs_job_types(server_path, "empty")
end

function KB_RPC_Server:count_new_jobs(server_path)
  return self:count_jobs_job_types(server_path, "new_job")
end

function KB_RPC_Server:count_processing_jobs(server_path)
  return self:count_jobs_job_types(server_path, "processing")
end

function KB_RPC_Server:count_all_jobs(server_path)
  return {
    empty_jobs      = self:count_empty_jobs(server_path),
    new_jobs        = self:count_new_jobs(server_path),
    processing_jobs = self:count_processing_jobs(server_path),
  }
end

---------------------------------------------------------------------------
-- List jobs by state
---------------------------------------------------------------------------

function KB_RPC_Server:list_jobs_job_types(server_path, state)
  if not is_valid_ltree(server_path) then
    error("server_path must be a valid ltree format")
  end
  local valid = { empty = true, new_job = true, processing = true }
  if not valid[state] then
    error("state must be one of: empty, new_job, processing")
  end

  local sql_str = string.format([[
    SELECT * FROM %s
     WHERE server_path = %s::ltree AND state = %s
     ORDER BY priority DESC, request_timestamp ASC
  ]], self.base_table, esc(server_path), esc(state))

  return self.kb_search:_raw_query(sql_str)
end

---------------------------------------------------------------------------
-- Push to RPC queue
---------------------------------------------------------------------------

function KB_RPC_Server:push_rpc_queue(server_path, request_id, rpc_action,
                                       request_payload, transaction_tag,
                                       priority, rpc_client_queue,
                                       max_retries, wait_time)
  max_retries = max_retries or 5
  wait_time   = wait_time   or 0.5
  priority    = priority    or 0

  -- Validation
  if not is_valid_ltree(server_path) then
    error("server_path must be a valid ltree format")
  end
  if not request_id or request_id == "" then
    request_id = uuid4()
  end
  if not rpc_action or rpc_action == "" then
    error("rpc_action must be a non-empty string")
  end
  if request_payload == nil then
    error("request_payload cannot be nil")
  end
  if not transaction_tag or transaction_tag == "" then
    error("transaction_tag must be a non-empty string")
  end
  if rpc_client_queue and not is_valid_ltree(rpc_client_queue) then
    error("rpc_client_queue must be nil or a valid ltree format")
  end

  local json_payload = dkjson.encode(request_payload)
  local max_wait = 8

  for attempt = 1, max_retries do
    local ok, result = pcall(function()
      -- Serializable isolation + advisory lock
      self.kb_search:_raw_query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
      local lock_key = tostring(#server_path * 31 + #self.base_table)  -- simple hash
      self.kb_search:_raw_query(string.format("SELECT pg_advisory_xact_lock(%s)", lock_key))

      -- Find an empty slot
      local sel_sql = string.format([[
        SELECT id FROM %s
         WHERE state = 'empty'
         ORDER BY priority DESC, request_timestamp ASC
         LIMIT 1
         FOR UPDATE
      ]], self.base_table)
      local row = self.kb_search:_raw_query_one(sel_sql)

      if not row then
        self.kb_search:rollback()
        error(NoMatchingRecordError.new("No matching record found with state = 'empty'"))
      end

      local rpc_cq = rpc_client_queue and esc(rpc_client_queue) or "NULL"

      local upd_sql = string.format([[
        UPDATE %s
           SET server_path        = %s,
               request_id         = %s,
               rpc_action         = %s,
               request_payload    = %s::jsonb,
               transaction_tag    = %s,
               priority           = %d,
               rpc_client_queue   = %s,
               state              = 'new_job',
               request_timestamp  = NOW() AT TIME ZONE 'UTC',
               completed_timestamp = NULL
         WHERE id = %s
         RETURNING *
      ]], self.base_table,
         esc(server_path), esc(request_id), esc(rpc_action),
         esc(json_payload), esc(transaction_tag), priority,
         rpc_cq, esc(row.id))

      local res = self.kb_search:_raw_query(upd_sql)
      if not res or #res == 0 then
        self.kb_search:rollback()
        error("Failed to update record in RPC queue")
      end

      self.kb_search:commit()
      return res[1]
    end)

    if ok then return result end

    self.kb_search:rollback()

    -- Check if it's a NoMatchingRecordError (propagate immediately)
    if type(result) == "table" and getmetatable(result) == NoMatchingRecordError then
      error(tostring(result))
    end
    if type(result) == "string" and result:find("NoMatchingRecordError") then
      error(result)
    end

    if attempt < max_retries then
      local sleep_time = math.min(wait_time * (2 ^ attempt), max_wait)
      sleep(sleep_time)
    else
      error(string.format("Failed to push to RPC queue after %d retries: %s",
        max_retries, tostring(result)))
    end
  end
end

---------------------------------------------------------------------------
-- Peak server queue (claim next job)
---------------------------------------------------------------------------

function KB_RPC_Server:peak_server_queue(server_path, retries, wait_time)
  retries   = retries   or 5
  wait_time = wait_time or 1

  for attempt = 1, retries do
    local ok, result = pcall(function()
      self.kb_search:_raw_query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")

      local sel_sql = string.format([[
        SELECT * FROM %s
         WHERE server_path = %s
           AND state = 'new_job'
         ORDER BY priority DESC, request_timestamp ASC
         LIMIT 1
         FOR UPDATE SKIP LOCKED
      ]], self.base_table, esc(server_path))

      local rows = self.kb_search:_raw_query(sel_sql)
      if not rows or #rows == 0 then
        self.kb_search:rollback()
        return nil
      end

      local record = rows[1]

      local upd_sql = string.format([[
        UPDATE %s
           SET state = 'processing',
               processing_timestamp = NOW() AT TIME ZONE 'UTC'
         WHERE id = %s
         RETURNING id
      ]], self.base_table, esc(record.id))

      local upd = self.kb_search:_raw_query(upd_sql)
      if not upd or #upd == 0 then
        self.kb_search:rollback()
        error(string.format("Failed to update state to 'processing' for id: %s", tostring(record.id)))
      end

      self.kb_search:commit()
      return record
    end)

    if ok then return result end

    self.kb_search:rollback()

    if attempt < retries then
      sleep(wait_time * (2 ^ attempt))
    else
      error(string.format("Failed to peak server queue after %d attempts: %s",
        retries, tostring(result)))
    end
  end

  return nil
end

---------------------------------------------------------------------------
-- Mark job completion
---------------------------------------------------------------------------

function KB_RPC_Server:mark_job_completion(server_path, id, retries, wait_time)
  retries   = retries   or 5
  wait_time = wait_time or 1

  for attempt = 1, retries do
    local ok, result = pcall(function()
      self.kb_search:_raw_query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")

      local verify_sql = string.format([[
        SELECT id FROM %s
         WHERE id = %s AND server_path = %s AND state = 'processing'
         FOR UPDATE
      ]], self.base_table, esc(id), esc(server_path))

      local row = self.kb_search:_raw_query_one(verify_sql)
      if not row then
        self.kb_search:rollback()
        return false
      end

      local upd_sql = string.format([[
        UPDATE %s
           SET state = 'empty',
               completed_timestamp = NOW() AT TIME ZONE 'UTC'
         WHERE id = %s
         RETURNING id
      ]], self.base_table, esc(id))

      local upd = self.kb_search:_raw_query(upd_sql)
      self.kb_search:commit()
      return upd and #upd > 0
    end)

    if ok then return result end

    self.kb_search:rollback()

    if attempt < retries then
      sleep(wait_time * (2 ^ attempt))
    else
      error(string.format("Failed to mark job as completed after %d attempts: %s",
        retries, tostring(result)))
    end
  end

  return false
end

---------------------------------------------------------------------------
-- Clear server queue
---------------------------------------------------------------------------

function KB_RPC_Server:clear_server_queue(server_path, max_retries, retry_delay)
  max_retries = max_retries or 3
  retry_delay = retry_delay or 1

  for attempt = 1, max_retries do
    local ok, result = pcall(function()
      -- Lock all rows
      local lock_sql = string.format([[
        SELECT 1 FROM %s WHERE server_path = %s::ltree FOR UPDATE NOWAIT
      ]], self.base_table, esc(server_path))
      self.kb_search:_raw_query(lock_sql)

      -- Reset all rows
      local upd_sql = string.format([[
        UPDATE %s
           SET request_id          = gen_random_uuid(),
               request_payload     = '{}'::jsonb,
               completed_timestamp = CURRENT_TIMESTAMP AT TIME ZONE 'UTC',
               state               = 'empty',
               rpc_client_queue    = NULL
         WHERE server_path = %s::ltree
      ]], self.base_table, esc(server_path))

      -- We need rowcount; use a RETURNING trick
      local ret_sql = upd_sql:gsub("WHERE server_path", "RETURNING id)\nSELECT count(*) as cnt FROM updated")
      -- Simpler approach: do update then count
      self.kb_search:_raw_query(upd_sql)
      -- Count what was there
      local cnt_sql = string.format(
        "SELECT COUNT(*) as cnt FROM %s WHERE server_path = %s::ltree",
        self.base_table, esc(server_path))
      local cnt_row = self.kb_search:_raw_query_one(cnt_sql)
      self.kb_search:commit()
      return cnt_row and tonumber(cnt_row.cnt) or 0
    end)

    if ok then return result end

    self.kb_search:rollback()
    if attempt < max_retries then sleep(retry_delay) end
  end

  error(string.format("Failed to acquire lock after %d attempts for server path: %s",
    max_retries, server_path))
end

return KB_RPC_Server