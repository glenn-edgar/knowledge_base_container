--[[
  KB_RPC_Client - RPC client response queue operations.
  LuaJIT port using luadbi-postgresql and dkjson.

  Usage:
    local KB_Search     = require("kb_search")
    local KB_RPC_Client = require("kb_rpc_client")
    local kb  = KB_Search.new({ ... })
    local rpc = KB_RPC_Client.new(kb, "my_database")
]]

local dkjson = require("dkjson")

local KB_RPC_Client = {}
KB_RPC_Client.__index = KB_RPC_Client

function KB_RPC_Client.new(kb_search, database)
  local self = setmetatable({}, KB_RPC_Client)
  self.kb_search  = kb_search
  self.dbh        = kb_search:get_connection()
  self.base_table = database .. "_rpc_client"
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

--- Generate a UUID v4 string (pure Lua fallback).
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

function KB_RPC_Client:find_rpc_client_id(kb, node_name, properties, node_path)
  local results = self:find_rpc_client_ids(kb, node_name, properties, node_path)
  if #results == 0 then
    error(string.format("No RPC client found: name=%s", tostring(node_name)))
  end
  if #results > 1 then
    error(string.format("Multiple RPC clients (%d) found: name=%s", #results, tostring(node_name)))
  end
  return results
end

function KB_RPC_Client:find_rpc_client_ids(kb, node_name, properties, node_path)
  self.kb_search:clear_filters()
  self.kb_search:search_label("KB_RPC_CLIENT_FIELD")

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
    error(string.format("No RPC client found: name=%s", tostring(node_name)))
  end
  return rows
end

function KB_RPC_Client:find_rpc_client_keys(key_data)
  local rv = {}
  for _, row in ipairs(key_data) do
    local p = row.path
    if p ~= nil then p = tostring(p) end
    rv[#rv + 1] = p
  end
  return rv
end

---------------------------------------------------------------------------
-- Slot counts
---------------------------------------------------------------------------

function KB_RPC_Client:find_free_slots(client_path)
  local sql_str = string.format([[
    SELECT COUNT(*) as total_records,
           COUNT(*) FILTER (WHERE is_new_result = FALSE) as free_slots
      FROM %s WHERE client_path = %s
  ]], self.base_table, esc(client_path))

  local row = self.kb_search:_raw_query_one(sql_str)
  if not row or tonumber(row.total_records) == 0 then
    error(string.format("No records found for client_path: %s", client_path))
  end
  return tonumber(row.free_slots)
end

function KB_RPC_Client:find_queued_slots(client_path)
  local sql_str = string.format([[
    SELECT COUNT(*) as total_records,
           COUNT(*) FILTER (WHERE is_new_result = TRUE) as queued_slots
      FROM %s WHERE client_path = %s
  ]], self.base_table, esc(client_path))

  local row = self.kb_search:_raw_query_one(sql_str)
  if not row or tonumber(row.total_records) == 0 then
    error(string.format("No records found for client_path: %s", client_path))
  end
  return tonumber(row.queued_slots)
end

---------------------------------------------------------------------------
-- Peak and claim reply
---------------------------------------------------------------------------

function KB_RPC_Client:peak_and_claim_reply_data(client_path, max_retries, retry_delay)
  max_retries = max_retries or 3
  retry_delay = retry_delay or 1.0

  for attempt = 1, max_retries do
    local ok, result = pcall(function()
      local upd_sql = string.format([[
        UPDATE %s
           SET is_new_result = FALSE
         WHERE id = (
           SELECT id FROM %s
            WHERE client_path = %s
              AND is_new_result = TRUE
            ORDER BY response_timestamp ASC
            FOR UPDATE SKIP LOCKED
            LIMIT 1
         )
         RETURNING *
      ]], self.base_table, self.base_table, esc(client_path))

      local rows = self.kb_search:_raw_query(upd_sql)
      if rows and #rows > 0 then
        self.kb_search:commit()
        return rows[1]
      end

      -- Check if any unclaimed rows exist
      local chk_sql = string.format([[
        SELECT EXISTS (
          SELECT 1 FROM %s
           WHERE client_path = %s AND is_new_result = TRUE
        ) as ex
      ]], self.base_table, esc(client_path))
      local chk = self.kb_search:_raw_query_one(chk_sql)
      local exists = chk and (chk.ex == true or chk.ex == "t")

      if not exists then
        self.kb_search:rollback()
        return nil  -- no results at all
      end

      self.kb_search:rollback()
      return "retry"
    end)

    if ok then
      if result == nil then return nil end
      if result ~= "retry" then return result end
    else
      self.kb_search:rollback()
    end

    if attempt < max_retries then sleep(retry_delay) end
  end

  error(string.format("Could not lock a new-reply row after %d attempts", max_retries))
end

---------------------------------------------------------------------------
-- Clear reply queue
---------------------------------------------------------------------------

function KB_RPC_Client:clear_reply_queue(client_path, max_retries, retry_delay)
  max_retries = max_retries or 3
  retry_delay = retry_delay or 1.0

  for attempt = 1, max_retries do
    local ok, result = pcall(function()
      -- Lock all rows for this client
      local sel_sql = string.format([[
        SELECT id FROM %s WHERE client_path = %s FOR UPDATE NOWAIT
      ]], self.base_table, esc(client_path))
      local rows = self.kb_search:_raw_query(sel_sql)

      if not rows or #rows == 0 then
        self.kb_search:commit()
        return 0
      end

      local updated = 0
      for _, row in ipairs(rows) do
        local new_uuid = uuid4()
        local upd_sql = string.format([[
          UPDATE %s
             SET request_id         = %s,
                 server_path        = %s,
                 response_payload   = '{}'::jsonb,
                 response_timestamp = NOW(),
                 is_new_result      = FALSE
           WHERE id = %s
        ]], self.base_table, esc(new_uuid), esc(client_path), esc(row.id))
        self.kb_search:_raw_query(upd_sql)
        updated = updated + 1
      end

      self.kb_search:commit()
      return updated
    end)

    if ok then return result end

    self.kb_search:rollback()
    if attempt < max_retries then sleep(retry_delay) end
  end

  error(string.format("Could not acquire lock after %d retries", max_retries))
end

---------------------------------------------------------------------------
-- Push and claim reply data
---------------------------------------------------------------------------

function KB_RPC_Client:push_and_claim_reply_data(client_path, request_uuid, server_path,
                                                  rpc_action, transaction_tag, reply_data,
                                                  max_retries, retry_delay)
  max_retries = max_retries or 3
  retry_delay = retry_delay or 1

  local json_payload = dkjson.encode(reply_data or {})

  for attempt = 1, max_retries do
    local ok, err = pcall(function()
      local sql_str = string.format([[
        WITH candidate AS (
          SELECT id FROM %s
           WHERE client_path = %s
             AND is_new_result = FALSE
           ORDER BY response_timestamp ASC
           FOR UPDATE SKIP LOCKED
           LIMIT 1
        )
        UPDATE %s
           SET request_id         = %s,
               server_path        = %s,
               rpc_action         = %s,
               transaction_tag    = %s,
               response_payload   = %s::jsonb,
               is_new_result      = TRUE,
               response_timestamp = CURRENT_TIMESTAMP
          FROM candidate
         WHERE %s.id = candidate.id
         RETURNING %s.id
      ]], self.base_table, esc(client_path),
         self.base_table,
         esc(request_uuid), esc(server_path), esc(rpc_action),
         esc(transaction_tag), esc(json_payload),
         self.base_table, self.base_table)

      local rows = self.kb_search:_raw_query(sql_str)
      if not rows or #rows == 0 then
        self.kb_search:rollback()
        error("No available record with is_new_result=FALSE found")
      end
      self.kb_search:commit()
    end)

    if ok then return end  -- success

    self.kb_search:rollback()

    if type(err) == "string" and err:find("No available record") then
      error(err)
    end

    if attempt >= max_retries then
      error(string.format("Failed after %d retries: %s", max_retries, tostring(err)))
    end
    sleep(retry_delay)
  end
end

---------------------------------------------------------------------------
-- List waiting jobs
---------------------------------------------------------------------------

function KB_RPC_Client:list_waiting_jobs(client_path)
  local sql_str
  if client_path then
    sql_str = string.format([[
      SELECT id, request_id, client_path, server_path,
             response_payload, response_timestamp, is_new_result
        FROM %s
       WHERE is_new_result = TRUE AND client_path = %s
       ORDER BY response_timestamp ASC
    ]], self.base_table, esc(client_path))
  else
    sql_str = string.format([[
      SELECT id, request_id, client_path, server_path,
             response_payload, response_timestamp, is_new_result
        FROM %s
       WHERE is_new_result = TRUE
       ORDER BY response_timestamp ASC
    ]], self.base_table)
  end

  local rows = self.kb_search:_raw_query(sql_str)
  -- Stringify UUIDs and paths for consistency
  for _, row in ipairs(rows) do
    if row.request_id then row.request_id = tostring(row.request_id) end
    if row.client_path then row.client_path = tostring(row.client_path) end
    if row.server_path then row.server_path = tostring(row.server_path) end
    if row.response_timestamp then row.response_timestamp = tostring(row.response_timestamp) end
  end
  return rows
end

return KB_RPC_Client