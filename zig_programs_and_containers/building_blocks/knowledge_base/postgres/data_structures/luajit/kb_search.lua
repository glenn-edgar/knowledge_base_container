--[[
  KB_Search - Knowledge Base Search with CTE-based progressive filtering
  LuaJIT port using luadbi-postgresql and dkjson.

  Usage:
    local KB_Search = require("kb_search")
    local kb = KB_Search.new({
      host     = "127.0.0.1",
      port     = 5432,
      dbname   = "mydb",
      user     = "postgres",
      password = "secret",
      database = "knowledge_base",
    })
    kb:search_kb("my_kb")
    kb:search_label("KB_STATUS_FIELD")
    local rows = kb:execute_query()
    kb:disconnect()
]]

local DBI    = require("DBI")
local dkjson = require("dkjson")

---------------------------------------------------------------------------
-- Helpers
---------------------------------------------------------------------------

--- Escape a string value for safe SQL interpolation (single-quote wrapping).
local function escape_literal(val)
  if val == nil then return "NULL" end
  local s = tostring(val)
  s = s:gsub("'", "''")
  return "'" .. s .. "'"
end

--- Fetch all rows from an executed statement as an array of dicts.
-- luadbi-postgresql: sth:fetch(true) returns a single table {col=val,...} or nil.
local function fetch_all_dicts(sth)
  local rows = {}
  while true do
    local row = sth:fetch(true)
    if not row then break end
    -- DBI fetch(true) returns the row as a table {colname=value, ...}
    -- We must copy it because DBI may reuse the table on next fetch.
    local copy = {}
    for k, v in pairs(row) do copy[k] = v end
    rows[#rows + 1] = copy
  end
  return rows
end

--- Fetch a single row as a dict, or nil.
local function fetch_one_dict(sth)
  local row = sth:fetch(true)
  if not row then return nil end
  local copy = {}
  for k, v in pairs(row) do copy[k] = v end
  return copy
end

---------------------------------------------------------------------------
-- Class
---------------------------------------------------------------------------

local KB_Search = {}
KB_Search.__index = KB_Search

function KB_Search.new(opts)
  assert(opts.host,     "host required")
  assert(opts.dbname,   "dbname required")
  assert(opts.user,     "user required")
  assert(opts.password, "password required")
  assert(opts.database, "database (table name) required")

  local self = setmetatable({}, KB_Search)
  self.host       = opts.host
  self.port       = opts.port or 5432
  self.dbname     = opts.dbname
  self.user       = opts.user
  self.password   = opts.password
  self.base_table       = opts.database
  self.link_table       = opts.database .. "_link"
  self.link_mount_table = opts.database .. "_link_mount"

  self.filters = {}
  self.results = nil
  self.path    = {}

  local dsn = string.format("dbname=%s host=%s port=%s",
    self.dbname, self.host, self.port)
  local dbh, err = DBI.Connect("PostgreSQL", dsn, self.user, self.password)
  if not dbh then
    error("Error connecting to database: " .. tostring(err))
  end
  dbh:autocommit(false)
  self.dbh = dbh

  return self
end

function KB_Search:disconnect()
  if self.dbh then
    self.dbh:close()
    self.dbh = nil
  end
end

function KB_Search:get_connection()
  if not self.dbh then
    error("Not connected to database. Call new() first.")
  end
  return self.dbh
end

function KB_Search:clear_filters()
  self.filters = {}
  self.results = nil
end

---------------------------------------------------------------------------
-- Filter builders
---------------------------------------------------------------------------

function KB_Search:search_kb(knowledge_base)
  self.filters[#self.filters + 1] = {
    condition = "knowledge_base = $knowledge_base",
    params    = { knowledge_base = knowledge_base },
  }
end

function KB_Search:search_label(label)
  self.filters[#self.filters + 1] = {
    condition = "label = $label",
    params    = { label = label },
  }
end

function KB_Search:search_name(name)
  self.filters[#self.filters + 1] = {
    condition = "name = $name",
    params    = { name = name },
  }
end

function KB_Search:search_property_key(key)
  -- Use jsonb_exists() function instead of ? operator (DBI treats ? as placeholder)
  self.filters[#self.filters + 1] = {
    condition = "jsonb_exists(properties::jsonb, $property_key)",
    params    = { property_key = key },
  }
end

function KB_Search:search_property_value(key, value)
  local json_obj = dkjson.encode({ [key] = value })
  self.filters[#self.filters + 1] = {
    condition = "properties::jsonb @> $json_object::jsonb",
    params    = { json_object = json_obj },
  }
end

function KB_Search:search_starting_path(starting_path)
  self.filters[#self.filters + 1] = {
    condition = "path <@ $starting_path",
    params    = { starting_path = starting_path },
  }
end

function KB_Search:search_path(path_expression)
  self.filters[#self.filters + 1] = {
    condition = "path ~ $path_expr",
    params    = { path_expr = path_expression },
  }
end

function KB_Search:search_has_link()
  self.filters[#self.filters + 1] = {
    condition = "has_link = TRUE",
    params    = {},
  }
end

function KB_Search:search_has_link_mount()
  self.filters[#self.filters + 1] = {
    condition = "has_link_mount = TRUE",
    params    = {},
  }
end

---------------------------------------------------------------------------
-- Query execution
---------------------------------------------------------------------------

local function interpolate(template, params)
  return (template:gsub("%$([%w_]+)", function(name)
    local val = params[name]
    if val == nil then
      error("Missing parameter: " .. name)
    end
    return escape_literal(val)
  end))
end

--- Execute raw SQL, return all rows as array of dicts.
-- Auto-rollbacks on error to prevent "current transaction aborted" cascades.
function KB_Search:_raw_query(sql_str)
  local sth, err = self.dbh:prepare(sql_str)
  if not sth then
    pcall(function() self.dbh:rollback() end)
    error("Prepare error: " .. tostring(err) .. "\nSQL: " .. sql_str)
  end
  local ok, exec_err = sth:execute()
  if not ok then
    pcall(function() self.dbh:rollback() end)
    error("Execute error: " .. tostring(exec_err) .. "\nSQL: " .. sql_str)
  end
  return fetch_all_dicts(sth)
end

--- Execute raw SQL, return single row dict or nil.
-- Auto-rollbacks on error to prevent "current transaction aborted" cascades.
function KB_Search:_raw_query_one(sql_str)
  local sth, err = self.dbh:prepare(sql_str)
  if not sth then
    pcall(function() self.dbh:rollback() end)
    error("Prepare error: " .. tostring(err) .. "\nSQL: " .. sql_str)
  end
  local ok, exec_err = sth:execute()
  if not ok then
    pcall(function() self.dbh:rollback() end)
    error("Execute error: " .. tostring(exec_err) .. "\nSQL: " .. sql_str)
  end
  return fetch_one_dict(sth)
end

--- Commit the current transaction.
function KB_Search:commit()
  self.dbh:commit()
end

--- Rollback the current transaction.
function KB_Search:rollback()
  pcall(function() self.dbh:rollback() end)
end

--- Execute the progressive CTE query with all accumulated filters.
function KB_Search:execute_query()
  if not self.dbh then
    error("Not connected to database. Call new() first.")
  end

  local column_str = "*"

  if #self.filters == 0 then
    local sql_str = string.format("SELECT %s FROM %s", column_str, self.base_table)
    self.results = self:_raw_query(sql_str)
    return self.results
  end

  -- Build CTE chain
  local cte_parts = {}
  cte_parts[1] = string.format("base_data AS (SELECT %s FROM %s)", column_str, self.base_table)

  for i, fi in ipairs(self.filters) do
    local cond   = fi.condition
    local params = fi.params or {}

    local prefixed_params = {}
    local prefixed_cond   = cond
    for pname, pval in pairs(params) do
      local pn = string.format("p%d_%s", i, pname)
      prefixed_params[pn] = pval
      prefixed_cond = prefixed_cond:gsub("%$" .. pname, "$" .. pn)
    end

    local resolved_cond = interpolate(prefixed_cond, prefixed_params)

    local prev_cte = (i == 1) and "base_data" or string.format("filter_%d", i - 1)
    local cte_name = string.format("filter_%d", i)

    if cond and cond ~= "" then
      cte_parts[#cte_parts + 1] = string.format(
        "%s AS (SELECT %s FROM %s WHERE %s)", cte_name, column_str, prev_cte, resolved_cond)
    else
      cte_parts[#cte_parts + 1] = string.format(
        "%s AS (SELECT %s FROM %s)", cte_name, column_str, prev_cte)
    end
  end

  local with_clause  = "WITH " .. table.concat(cte_parts, ",\n")
  local final_select = string.format("SELECT %s FROM filter_%d", column_str, #self.filters)
  local final_query  = with_clause .. "\n" .. final_select

  local ok, res = pcall(self._raw_query, self, final_query)
  if not ok then
    error("Error executing query: " .. tostring(res) .. "\nQuery: " .. final_query)
  end
  self.results = res
  return self.results
end

---------------------------------------------------------------------------
-- Result helpers
---------------------------------------------------------------------------

function KB_Search:get_results()
  return self.results or {}
end

function KB_Search:find_path_values(key_data)
  if not key_data then return {} end
  if key_data.path then key_data = { key_data } end
  local rv = {}
  for _, row in ipairs(key_data) do
    local p = row.path
    if p ~= nil then p = tostring(p) end
    rv[#rv + 1] = p
  end
  return rv
end

function KB_Search:find_description(key_data)
  if not key_data then return {} end
  if key_data.path then key_data = { key_data } end
  local rv = {}
  for _, row in ipairs(key_data) do
    local props = row.properties or {}
    if type(props) == "string" then
      props = dkjson.decode(props) or {}
    end
    local desc = props.description or ""
    local path_key = row.path
    if path_key ~= nil then path_key = tostring(path_key) else path_key = "" end
    rv[#rv + 1] = { [path_key] = desc }
  end
  return rv
end

function KB_Search:find_description_paths(path_array)
  if type(path_array) ~= "table" then
    path_array = { path_array }
  end
  if #path_array == 0 then return {} end

  local placeholders = {}
  for i, p in ipairs(path_array) do
    placeholders[i] = escape_literal(p)
  end

  local sql_str
  if #path_array == 1 then
    sql_str = string.format(
      "SELECT path, data FROM %s WHERE path = %s",
      self.base_table, placeholders[1])
  else
    sql_str = string.format(
      "SELECT path, data FROM %s WHERE path IN (%s)",
      self.base_table, table.concat(placeholders, ","))
  end

  local res = self:_raw_query(sql_str)
  local rv = {}
  local found = {}
  for _, row in ipairs(res) do
    local p = tostring(row.path)
    rv[p] = row.data
    found[p] = true
  end
  for _, p in ipairs(path_array) do
    local ps = tostring(p)
    if not found[ps] then rv[ps] = nil end
  end
  return rv
end

function KB_Search:decode_link_nodes(path)
  if path == nil then
    error("Path must be a non-empty string")
  end
  path = tostring(path)
  if path == "" then
    error("Path must be a non-empty string")
  end

  local parts = {}
  for seg in path:gmatch("[^.]+") do
    parts[#parts + 1] = seg
  end

  if #parts < 3 then
    error(string.format("Path must have at least 3 elements (kb.link.name), got %d", #parts))
  end

  local remaining = #parts - 1
  if remaining % 2 ~= 0 then
    error(string.format(
      "Bad path format: after kb identifier, must have even number of elements, got %d", remaining))
  end

  local kb = parts[1]
  local result = {}
  for i = 2, #parts, 2 do
    result[#result + 1] = { parts[i], parts[i + 1] }
  end
  return kb, result
end

---------------------------------------------------------------------------
-- Expose helpers for sibling modules
---------------------------------------------------------------------------

KB_Search.escape_literal  = escape_literal
KB_Search.fetch_all_dicts = fetch_all_dicts
KB_Search.fetch_one_dict  = fetch_one_dict

return KB_Search