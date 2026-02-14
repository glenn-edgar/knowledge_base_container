--[[
  KB_Status_Data - Status data read/write for the knowledge base.
  LuaJIT port using luadbi-postgresql and dkjson.

  Usage:
    local KB_Search      = require("kb_search")
    local KB_Status_Data = require("kb_status_data")
    local kb     = KB_Search.new({ ... })
    local status = KB_Status_Data.new(kb, "my_database")
]]

local dkjson = require("dkjson")

local KB_Status_Data = {}
KB_Status_Data.__index = KB_Status_Data

function KB_Status_Data.new(kb_search, database)
  local self = setmetatable({}, KB_Status_Data)
  self.kb_search  = kb_search
  self.dbh        = kb_search:get_connection()
  self.base_table = database .. "_status"
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

function KB_Status_Data:find_node_id(kb, node_name, properties, node_path)
  local results = self:find_node_ids(kb, node_name, properties, node_path)
  if #results == 0 then
    error(string.format("No node found: kb=%s, name=%s", tostring(kb), tostring(node_name)))
  end
  if #results > 1 then
    error(string.format("Multiple nodes (%d) found: kb=%s, name=%s",
      #results, tostring(kb), tostring(node_name)))
  end
  return results[1]
end

function KB_Status_Data:find_node_ids(kb, node_name, properties, node_path)
  self.kb_search:clear_filters()
  self.kb_search:search_label("KB_STATUS_FIELD")

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
    error(string.format("No nodes found: kb=%s, name=%s", tostring(kb), tostring(node_name)))
  end
  return rows
end

---------------------------------------------------------------------------
-- Status data CRUD
---------------------------------------------------------------------------

function KB_Status_Data:get_status_data(path)
  if not path or path == "" then error("Path cannot be empty or nil") end

  local sql_str = string.format(
    "SELECT data, path FROM %s WHERE path = %s LIMIT 1",
    self.base_table, esc(path))
  local row = self.kb_search:_raw_query_one(sql_str)

  if not row then
    error(string.format("No data found for path: %s", path))
  end

  local data = row.data
  if type(data) == "string" then
    local decoded, _, err = dkjson.decode(data)
    if not decoded then
      error(string.format("Failed to decode JSON data for path '%s': %s", path, tostring(err)))
    end
    data = decoded
  end

  return data, row.path
end

function KB_Status_Data:get_multiple_status_data(paths)
  if not paths or #paths == 0 then return {} end
  if type(paths) ~= "table" then paths = { paths } end

  local placeholders = {}
  for i, p in ipairs(paths) do
    placeholders[i] = esc(p)
  end

  local sql_str = string.format(
    "SELECT data, path FROM %s WHERE path IN (%s)",
    self.base_table, table.concat(placeholders, ","))

  local res = self.kb_search:_raw_query(sql_str)
  local rv = {}
  for _, row in ipairs(res) do
    local data = row.data
    if type(data) == "string" then
      data = dkjson.decode(data) or data
    end
    rv[row.path] = data
  end
  return rv
end

function KB_Status_Data:set_status_data(path, data, retry_count, retry_delay)
  retry_count = retry_count or 3
  retry_delay = retry_delay or 1.0

  if not path or path == "" then error("Path cannot be empty or nil") end
  if type(data) ~= "table" then error("Data must be a table") end

  local json_data = dkjson.encode(data)

  local upsert_sql = string.format([[
    INSERT INTO %s (path, data) VALUES (%s, %s::jsonb)
    ON CONFLICT (path)
    DO UPDATE SET data = EXCLUDED.data
    RETURNING path, (xmax = 0) AS was_inserted
  ]], self.base_table, esc(path), esc(json_data))

  local last_err
  for attempt = 0, retry_count do
    local ok, result = pcall(function()
      local row = self.kb_search:_raw_query_one(upsert_sql)
      self.kb_search:commit()
      if not row then
        self.kb_search:rollback()
        error("Database operation completed but no result was returned")
      end
      return row
    end)

    if ok then
      local row = result
      local was_ins = (row.was_inserted == true or row.was_inserted == "t")
      local op = was_ins and "inserted" or "updated"
      return true, string.format("Successfully %s data for path: %s", op, row.path)
    end

    last_err = result
    self.kb_search:rollback()

    if attempt < retry_count then sleep(retry_delay) end
  end

  error(string.format("Failed to set status data for path '%s' after %d attempts: %s",
    path, retry_count + 1, tostring(last_err)))
end

function KB_Status_Data:set_multiple_status_data(path_data_pairs, retry_count, retry_delay)
  retry_count = retry_count or 3
  retry_delay = retry_delay or 1.0

  if not path_data_pairs then error("path_data_pairs cannot be empty") end

  -- Normalise to array of {path, json_string}
  local pairs_list = {}
  if path_data_pairs[1] and type(path_data_pairs[1]) == "table" then
    -- array of {path, data}
    for _, pair in ipairs(path_data_pairs) do
      if not pair[1] or pair[1] == "" then error("Path cannot be empty") end
      if type(pair[2]) ~= "table" then error("Data must be a table") end
      pairs_list[#pairs_list + 1] = { pair[1], dkjson.encode(pair[2]) }
    end
  else
    -- dict-style
    for p, d in pairs(path_data_pairs) do
      if type(p) ~= "string" or p == "" then error("Path cannot be empty") end
      if type(d) ~= "table" then error("Data must be a table") end
      pairs_list[#pairs_list + 1] = { p, dkjson.encode(d) }
    end
  end

  local last_err
  for attempt = 0, retry_count do
    local ok, err_or_results = pcall(function()
      local results = {}
      for _, pair in ipairs(pairs_list) do
        local upsert_sql = string.format([[
          INSERT INTO %s (path, data) VALUES (%s, %s::jsonb)
          ON CONFLICT (path)
          DO UPDATE SET data = EXCLUDED.data
          RETURNING path, (xmax = 0) AS was_inserted
        ]], self.base_table, esc(pair[1]), esc(pair[2]))

        local row = self.kb_search:_raw_query_one(upsert_sql)
        if row then
          local was_ins = (row.was_inserted == true or row.was_inserted == "t")
          results[row.path] = was_ins and "inserted" or "updated"
        else
          results[pair[1]] = "failed"
        end
      end
      self.kb_search:commit()
      return results
    end)

    if ok then
      local results = err_or_results
      local success_count = 0
      for _, v in pairs(results) do
        if v ~= "failed" then success_count = success_count + 1 end
      end
      return true,
        string.format("Successfully processed %d/%d records", success_count, #pairs_list),
        results
    end

    last_err = err_or_results
    self.kb_search:rollback()
    if attempt < retry_count then sleep(retry_delay) end
  end

  error(string.format("Failed to set multiple status data after %d attempts: %s",
    retry_count + 1, tostring(last_err)))
end

return KB_Status_Data

