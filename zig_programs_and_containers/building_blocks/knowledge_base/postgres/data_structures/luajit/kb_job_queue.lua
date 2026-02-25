--[[
  KB_Job_Queue - Job queue with pre-allocated slots, locking, and retry.
  LuaJIT port using luadbi-postgresql and dkjson.

  Usage:
    local KB_Search    = require("kb_search")
    local KB_Job_Queue = require("kb_job_queue")
    local kb   = KB_Search.new({ ... })
    local jobs = KB_Job_Queue.new(kb, "my_database")
]]

local dkjson = require("dkjson")

local KB_Job_Queue = {}
KB_Job_Queue.__index = KB_Job_Queue

function KB_Job_Queue.new(kb_search, database)
  local self = setmetatable({}, KB_Job_Queue)
  self.kb_search  = kb_search
  self.dbh        = kb_search:get_connection()
  self.base_table = database .. "_job"
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

---------------------------------------------------------------------------
-- Node discovery
---------------------------------------------------------------------------

function KB_Job_Queue:find_job_id(kb, node_name, properties, node_path)
  local results = self:find_job_ids(kb, node_name, properties, node_path)
  if #results == 0 then
    error(string.format("No job found: name=%s", tostring(node_name)))
  end
  if #results > 1 then
    error(string.format("Multiple jobs (%d) found: name=%s", #results, tostring(node_name)))
  end
  return results[1]
end

function KB_Job_Queue:find_job_ids(kb, node_name, properties, node_path)
  self.kb_search:clear_filters()
  self.kb_search:search_label("KB_JOB_QUEUE")

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
    error(string.format("No jobs found: name=%s", tostring(node_name)))
  end
  return rows
end

function KB_Job_Queue:find_job_paths(table_dict_rows)
  if not table_dict_rows then return {} end
  local rv = {}
  for _, row in ipairs(table_dict_rows) do
    if row.path then rv[#rv + 1] = row.path end
  end
  return rv
end

---------------------------------------------------------------------------
-- Queue operations
---------------------------------------------------------------------------

function KB_Job_Queue:get_queued_number(path)
  if not path or path == "" then error("Path cannot be empty or nil") end
  local sql_str = string.format(
    "SELECT COUNT(*) as count FROM %s WHERE path = %s AND valid = TRUE",
    self.base_table, esc(path))
  local row = self.kb_search:_raw_query_one(sql_str)
  return row and tonumber(row.count) or 0
end

function KB_Job_Queue:get_free_number(path)
  if not path or path == "" then error("Path cannot be empty or nil") end
  local sql_str = string.format(
    "SELECT COUNT(*) as count FROM %s WHERE path = %s AND valid = FALSE",
    self.base_table, esc(path))
  local row = self.kb_search:_raw_query_one(sql_str)
  return row and tonumber(row.count) or 0
end

function KB_Job_Queue:peak_job_data(path, max_retries, retry_delay)
  max_retries = max_retries or 3
  retry_delay = retry_delay or 1

  if not path or path == "" then error("Path cannot be empty or nil") end

  for attempt = 1, max_retries do
    local ok, result = pcall(function()
      local sel_sql = string.format([[
        SELECT id, data, schedule_at
          FROM %s
         WHERE path = %s
           AND valid = TRUE
           AND is_active = FALSE
           AND (schedule_at IS NULL OR schedule_at <= NOW())
         ORDER BY schedule_at ASC NULLS FIRST
         FOR UPDATE SKIP LOCKED
         LIMIT 1
      ]], self.base_table, esc(path))
      local row = self.kb_search:_raw_query_one(sel_sql)

      if not row then
        self.kb_search:rollback()
        return nil
      end

      local job_id = row.id

      local upd_sql = string.format([[
        UPDATE %s
           SET started_at = NOW(), is_active = TRUE
         WHERE id = %s
           AND is_active = FALSE
           AND valid = TRUE
         RETURNING id, started_at
      ]], self.base_table, esc(job_id))
      local updated = self.kb_search:_raw_query_one(upd_sql)

      if not updated then
        self.kb_search:rollback()
        return "retry"
      end

      self.kb_search:commit()
      return {
        id          = row.id,
        data        = row.data,
        schedule_at = row.schedule_at,
        started_at  = updated.started_at,
      }
    end)

    if ok then
      if result == nil then return nil end
      if result ~= "retry" then return result end
    else
      self.kb_search:rollback()
    end

    if attempt < max_retries then
      sleep(retry_delay * (1.5 ^ attempt))
    end
  end

  error(string.format("Could not lock a job for path='%s' after %d retries", path, max_retries))
end

function KB_Job_Queue:mark_job_completed(job_id, max_retries, retry_delay)
  max_retries = max_retries or 3
  retry_delay = retry_delay or 1.0

  if not job_id or type(job_id) ~= "number" then
    error("job_id must be a valid integer")
  end

  for attempt = 1, max_retries do
    local ok, result = pcall(function()
      local lock_sql = string.format(
        "SELECT id FROM %s WHERE id = %s FOR UPDATE NOWAIT",
        self.base_table, tostring(job_id))
      local row = self.kb_search:_raw_query_one(lock_sql)

      if not row then
        self.kb_search:rollback()
        error(string.format("No job found with id=%d", job_id))
      end

      local upd_sql = string.format([[
        UPDATE %s
           SET completed_at = NOW(), valid = FALSE, is_active = FALSE
         WHERE id = %s
         RETURNING id, completed_at
      ]], self.base_table, tostring(job_id))
      local updated = self.kb_search:_raw_query_one(upd_sql)

      if not updated then
        self.kb_search:rollback()
        error(string.format("Failed to mark job %d as completed", job_id))
      end

      self.kb_search:commit()
      return {
        success      = true,
        job_id       = updated.id,
        completed_at = updated.completed_at,
      }
    end)

    if ok then return result end

    self.kb_search:rollback()

    -- Propagate non-retryable errors immediately
    if type(result) == "string" and
       (result:find("No job found") or result:find("Failed to mark")) then
      error(result)
    end

    if attempt < max_retries then sleep(retry_delay) end
  end

  error(string.format("Could not lock job id=%d after %d attempts", job_id, max_retries))
end

function KB_Job_Queue:push_job_data(path, data, max_retries, retry_delay)
  max_retries = max_retries or 3
  retry_delay = retry_delay or 1

  if not path or path == "" then error("Path cannot be empty or nil") end
  if type(data) ~= "table" then error("Data must be a table") end

  local json_data = dkjson.encode(data)

  for attempt = 1, max_retries do
    local ok, result = pcall(function()
      local sel_sql = string.format([[
        SELECT id FROM %s
         WHERE path = %s AND valid = FALSE
         ORDER BY completed_at ASC
         LIMIT 1
         FOR UPDATE SKIP LOCKED
      ]], self.base_table, esc(path))
      local row = self.kb_search:_raw_query_one(sel_sql)

      if not row then
        self.kb_search:rollback()
        error(string.format("No available job slot for path '%s'", path))
      end

      local upd_sql = string.format([[
        UPDATE %s
           SET data         = %s::jsonb,
               schedule_at  = timezone('UTC', now()),
               started_at   = timezone('UTC', now()),
               completed_at = timezone('UTC', now()),
               valid        = TRUE,
               is_active    = FALSE
         WHERE id = %s
         RETURNING id, schedule_at, data
      ]], self.base_table, esc(json_data), esc(row.id))
      local updated = self.kb_search:_raw_query_one(upd_sql)

      if not updated then
        self.kb_search:rollback()
        error(string.format("Failed to update job slot for path '%s'", path))
      end

      self.kb_search:commit()
      return {
        job_id      = updated.id,
        schedule_at = updated.schedule_at,
        data        = updated.data,
      }
    end)

    if ok then return result end

    self.kb_search:rollback()

    if type(result) == "string" and result:find("No available job slot") then
      error(result)
    end

    if attempt < max_retries then sleep(retry_delay) end
  end

  error(string.format("Could not acquire lock for path '%s' after %d attempts", path, max_retries))
end

function KB_Job_Queue:list_pending_jobs(path, limit, offset)
  if not path or path == "" then error("Path cannot be empty or nil") end
  offset = offset or 0

  local parts = { string.format([[
    SELECT id, path, schedule_at, started_at, completed_at, is_active, valid, data
      FROM %s
     WHERE path = %s AND valid = TRUE AND is_active = FALSE
     ORDER BY schedule_at ASC
  ]], self.base_table, esc(path)) }

  if limit and limit > 0 then
    parts[#parts + 1] = " LIMIT " .. tostring(limit)
  end
  if offset > 0 then
    parts[#parts + 1] = " OFFSET " .. tostring(offset)
  end

  return self.kb_search:_raw_query(table.concat(parts))
end

function KB_Job_Queue:list_active_jobs(path, limit, offset)
  if not path or path == "" then error("Path cannot be empty or nil") end
  offset = offset or 0

  local parts = { string.format([[
    SELECT id, path, schedule_at, started_at, completed_at, is_active, valid, data
      FROM %s
     WHERE path = %s AND valid = TRUE AND is_active = TRUE
     ORDER BY started_at ASC
  ]], self.base_table, esc(path)) }

  if limit and limit > 0 then
    parts[#parts + 1] = " LIMIT " .. tostring(limit)
  end
  if offset > 0 then
    parts[#parts + 1] = " OFFSET " .. tostring(offset)
  end

  return self.kb_search:_raw_query(table.concat(parts))
end

function KB_Job_Queue:clear_job_queue(path)
  if not path or path == "" then error("Path cannot be empty or nil") end

  local ok, result = pcall(function()
    self.kb_search:_raw_query(string.format(
      "LOCK TABLE %s IN EXCLUSIVE MODE", self.base_table))

    local sql_str = string.format([[
      UPDATE %s
         SET schedule_at  = NOW(),
             started_at   = NOW(),
             completed_at = NOW(),
             is_active    = FALSE,
             valid        = FALSE,
             data         = '{}'::jsonb
       WHERE path = %s
       RETURNING id, completed_at
    ]], self.base_table, esc(path))
    local res = self.kb_search:_raw_query(sql_str)

    self.kb_search:commit()
    return {
      success       = true,
      cleared_count = #res,
      cleared_jobs  = res,
    }
  end)

  if not ok then
    self.kb_search:rollback()
    error(string.format("Error in clear_job_queue for path '%s': %s", path, tostring(result)))
  end
  return result
end

function KB_Job_Queue:get_job_statistics(path)
  if not path or path == "" then error("Path cannot be empty or nil") end

  local sql_str = string.format([[
    SELECT
      COUNT(*) as total_jobs,
      COUNT(CASE WHEN valid = TRUE AND is_active = FALSE THEN 1 END) as pending_jobs,
      COUNT(CASE WHEN valid = TRUE AND is_active = TRUE THEN 1 END) as active_jobs,
      COUNT(CASE WHEN valid = FALSE THEN 1 END) as completed_jobs,
      MIN(schedule_at) as earliest_scheduled,
      MAX(completed_at) as latest_completed,
      AVG(EXTRACT(EPOCH FROM (completed_at - started_at))) as avg_processing_time_seconds
    FROM %s WHERE path = %s
  ]], self.base_table, esc(path))

  local row = self.kb_search:_raw_query_one(sql_str)
  if not row then
    return {
      total_jobs = 0, pending_jobs = 0, active_jobs = 0, completed_jobs = 0,
      earliest_scheduled = nil, latest_completed = nil,
      avg_processing_time_seconds = nil,
    }
  end
  return row
end

function KB_Job_Queue:get_job_by_id(job_id)
  if not job_id or type(job_id) ~= "number" then
    error("job_id must be a valid integer")
  end
  local sql_str = string.format([[
    SELECT id, path, schedule_at, started_at, completed_at, is_active, valid, data
      FROM %s WHERE id = %s
  ]], self.base_table, tostring(job_id))
  return self.kb_search:_raw_query_one(sql_str)
end

return KB_Job_Queue


