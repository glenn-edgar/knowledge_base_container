--[[
  KB_Stream - Stream data with pre-allocated circular buffer pattern.
  LuaJIT port using luadbi-postgresql and dkjson.

  Usage:
    local KB_Search = require("kb_search")
    local KB_Stream = require("kb_stream")
    local kb     = KB_Search.new({ ... })
    local stream = KB_Stream.new(kb, "my_database")
]]

local dkjson = require("dkjson")

local KB_Stream = {}
KB_Stream.__index = KB_Stream

function KB_Stream.new(kb_search, database)
  local self = setmetatable({}, KB_Stream)
  self.kb_search  = kb_search
  self.dbh        = kb_search:get_connection()
  self.base_table = database .. "_stream"
  return self
end

---------------------------------------------------------------------------
-- Internal helpers (delegate to kb_search shared utilities)
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
-- Node discovery (delegates to kb_search)
---------------------------------------------------------------------------

function KB_Stream:find_stream_id(kb, node_name, properties, node_path)
  local results = self:find_stream_ids(kb, node_name, properties, node_path)
  if #results == 0 then
    error(string.format("No stream node found: name=%s", tostring(node_name)))
  end
  if #results > 1 then
    error(string.format("Multiple stream nodes (%d) found: name=%s", #results, tostring(node_name)))
  end
  return results[1]
end

function KB_Stream:find_stream_ids(kb, node_name, properties, node_path)
  self.kb_search:clear_filters()
  self.kb_search:search_label("KB_STREAM_FIELD")

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
    error(string.format("No stream nodes found: name=%s", tostring(node_name)))
  end
  return rows
end

function KB_Stream:find_stream_table_keys(key_data)
  if not key_data then return {} end
  local rv = {}
  for _, row in ipairs(key_data) do
    if row.path then rv[#rv + 1] = tostring(row.path) end
  end
  return rv
end

---------------------------------------------------------------------------
-- Stream operations
---------------------------------------------------------------------------

function KB_Stream:push_stream_data(path, data, max_retries, retry_delay)
  max_retries = max_retries or 3
  retry_delay = retry_delay or 1.0

  if not path or path == "" then error("Path cannot be empty or nil") end
  if type(data) ~= "table" then error("Data must be a table") end

  local json_data = dkjson.encode(data)

  for attempt = 1, max_retries do
    local ok, err = pcall(function()
      -- Check at least one record exists
      local cnt_sql = string.format(
        "SELECT COUNT(*) as count FROM %s WHERE path = %s",
        self.base_table, esc(path))
      local cnt = self.kb_search:_raw_query_one(cnt_sql)
      local total = cnt and tonumber(cnt.count) or 0
      if total == 0 then
        error(string.format("No records found for path='%s'. Records must be pre-allocated.", path))
      end

      -- Lock oldest record
      local sel_sql = string.format([[
        SELECT id, recorded_at, valid
          FROM %s
         WHERE path = %s
         ORDER BY recorded_at ASC
         FOR UPDATE SKIP LOCKED
         LIMIT 1
      ]], self.base_table, esc(path))
      local row = self.kb_search:_raw_query_one(sel_sql)

      if not row then
        self.kb_search:rollback()
        if attempt < max_retries then
          sleep(retry_delay)
          return "retry"
        else
          error(string.format("Could not lock any row for path='%s' after %d attempts", path, max_retries))
        end
      end

      local record_id       = row.id
      local old_recorded_at = row.recorded_at
      local was_valid       = row.valid

      -- Update
      local upd_sql = string.format([[
        UPDATE %s
           SET data        = %s::jsonb,
               recorded_at = NOW(),
               valid       = TRUE
         WHERE id = %s
         RETURNING id, path, recorded_at, data, valid
      ]], self.base_table, esc(json_data), esc(record_id))
      local updated = self.kb_search:_raw_query_one(upd_sql)

      if not updated then
        self.kb_search:rollback()
        error(string.format("Failed to update record id=%s", tostring(record_id)))
      end

      self.kb_search:commit()
      return {
        id                   = updated.id,
        path                 = updated.path,
        recorded_at          = updated.recorded_at,
        data                 = updated.data,
        valid                = updated.valid,
        previous_recorded_at = old_recorded_at,
        was_previously_valid = was_valid,
        operation            = "circular_buffer_replace",
      }
    end)

    if ok then
      if err == "retry" then
        -- continue loop
      else
        return err  -- the result table
      end
    else
      self.kb_search:rollback()
      if tostring(err):find("No records found") then error(err) end
      if attempt >= max_retries then
        error(string.format("Error pushing stream data for path '%s': %s", path, tostring(err)))
      end
      sleep(retry_delay)
    end
  end
  error("Unexpected error in push_stream_data")
end

function KB_Stream:get_latest_stream_data(path)
  if not path or path == "" then error("Path cannot be empty or nil") end
  local sql_str = string.format([[
    SELECT id, path, recorded_at, data, valid
      FROM %s
     WHERE path = %s AND valid = TRUE
     ORDER BY recorded_at DESC
     LIMIT 1
  ]], self.base_table, esc(path))
  return self.kb_search:_raw_query_one(sql_str)
end

function KB_Stream:get_stream_data_count(path, include_invalid)
  if not path or path == "" then error("Path cannot be empty or nil") end
  local valid_clause = include_invalid and "" or " AND valid = TRUE"
  local sql_str = string.format(
    "SELECT COUNT(*) as count FROM %s WHERE path = %s%s",
    self.base_table, esc(path), valid_clause)
  local row = self.kb_search:_raw_query_one(sql_str)
  return row and tonumber(row.count) or 0
end

function KB_Stream:clear_stream_data(path, older_than)
  if not path or path == "" then error("Path cannot be empty or nil") end

  local sql_str
  if older_than then
    sql_str = string.format([[
      UPDATE %s SET valid = FALSE
       WHERE path = %s AND recorded_at < %s AND valid = TRUE
       RETURNING id, recorded_at
    ]], self.base_table, esc(path), esc(older_than))
  else
    sql_str = string.format([[
      UPDATE %s SET valid = FALSE
       WHERE path = %s AND valid = TRUE
       RETURNING id, recorded_at
    ]], self.base_table, esc(path))
  end

  local ok, res = pcall(self.kb_search._raw_query, self.kb_search, sql_str)
  if not ok then
    self.kb_search:rollback()
    return { success = false, cleared_count = 0, error = tostring(res), path = path }
  end

  self.kb_search:commit()
  return {
    success         = true,
    cleared_count   = #res,
    cleared_records = res,
    path            = path,
  }
end

function KB_Stream:list_stream_data(path, limit, offset, recorded_after, recorded_before, order)
  if not path or path == "" then error("Path cannot be empty or nil") end
  order  = order  or "ASC"
  offset = offset or 0
  if order ~= "ASC" and order ~= "DESC" then error("Order must be 'ASC' or 'DESC'") end

  local parts = { string.format(
    "SELECT id, path, recorded_at, data, valid FROM %s WHERE path = %s AND valid = TRUE",
    self.base_table, esc(path)) }

  if recorded_after then
    parts[#parts + 1] = " AND recorded_at >= " .. esc(recorded_after)
  end
  if recorded_before then
    parts[#parts + 1] = " AND recorded_at <= " .. esc(recorded_before)
  end
  parts[#parts + 1] = " ORDER BY recorded_at " .. order
  if limit and limit > 0 then
    parts[#parts + 1] = " LIMIT " .. tostring(limit)
  end
  if offset > 0 then
    parts[#parts + 1] = " OFFSET " .. tostring(offset)
  end

  return self.kb_search:_raw_query(table.concat(parts))
end

function KB_Stream:get_stream_data_range(path, start_time, end_time)
  if not path or path == "" then error("Path cannot be empty or nil") end
  if not start_time or not end_time then error("Both start_time and end_time required") end

  local sql_str = string.format([[
    SELECT id, path, recorded_at, data, valid
      FROM %s
     WHERE path = %s AND recorded_at >= %s AND recorded_at <= %s AND valid = TRUE
     ORDER BY recorded_at ASC
  ]], self.base_table, esc(path), esc(start_time), esc(end_time))
  return self.kb_search:_raw_query(sql_str)
end

function KB_Stream:get_stream_statistics(path, include_invalid)
  if not path or path == "" then error("Path cannot be empty or nil") end

  local sql_str
  if include_invalid then
    sql_str = string.format([[
      SELECT
        COUNT(*) as total_records,
        COUNT(CASE WHEN valid = TRUE THEN 1 END) as valid_records,
        COUNT(CASE WHEN valid = FALSE THEN 1 END) as invalid_records,
        MIN(CASE WHEN valid = TRUE THEN recorded_at END) as earliest_valid_recorded,
        MAX(CASE WHEN valid = TRUE THEN recorded_at END) as latest_valid_recorded,
        MIN(recorded_at) as earliest_recorded_overall,
        MAX(recorded_at) as latest_recorded_overall
      FROM %s WHERE path = %s
    ]], self.base_table, esc(path))
  else
    sql_str = string.format([[
      SELECT
        COUNT(*) as valid_records,
        MIN(recorded_at) as earliest_recorded,
        MAX(recorded_at) as latest_recorded
      FROM %s WHERE path = %s AND valid = TRUE
    ]], self.base_table, esc(path))
  end
  return self.kb_search:_raw_query_one(sql_str)
end

function KB_Stream:get_stream_data_by_id(record_id)
  if not record_id or type(record_id) ~= "number" then
    error("record_id must be a valid integer")
  end
  local sql_str = string.format(
    "SELECT id, path, recorded_at, data FROM %s WHERE id = %s",
    self.base_table, tostring(record_id))
  return self.kb_search:_raw_query_one(sql_str)
end

return KB_Stream