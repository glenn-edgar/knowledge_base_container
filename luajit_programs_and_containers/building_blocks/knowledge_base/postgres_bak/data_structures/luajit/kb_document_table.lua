--[[
  KB_Document_Table - JSONB operations on ltree document database with queue support.
  LuaJIT port using luadbi-postgresql and dkjson.

  Provides:
  - Core JSONB operations (get, set, delete, contains, etc.)
  - Array operations (append, prepend, remove, etc.)
  - Queue/Stack abstractions (enqueue, dequeue, push, pop)
  - Path-based queries using standard PostgreSQL JSONB operators

  Usage:
    local KB_Search         = require("kb_search")
    local KB_Document_Table = require("kb_document_table")
    local kb  = KB_Search.new({ ... })
    local doc = KB_Document_Table.new(kb, "my_database")
]]

local dkjson = require("dkjson")

---------------------------------------------------------------------------
-- Error class
---------------------------------------------------------------------------

local QueueOperationError = {}
QueueOperationError.__index = QueueOperationError
function QueueOperationError.new(msg)
  return setmetatable({ message = msg }, QueueOperationError)
end
function QueueOperationError:__tostring()
  return "QueueOperationError: " .. self.message
end

---------------------------------------------------------------------------
-- Class
---------------------------------------------------------------------------

local KB_Document_Table = {}
KB_Document_Table.__index = KB_Document_Table

function KB_Document_Table.new(kb_search, database)
  local self = setmetatable({}, KB_Document_Table)
  self.kb_search  = kb_search
  self.dbh        = kb_search:get_connection()
  self.database   = database
  self.table_name = database .. "_document"
  return self
end

---------------------------------------------------------------------------
-- Internal helpers
---------------------------------------------------------------------------

local function esc(val)
  if val == nil then return "NULL" end
  local s = tostring(val)
  s = s:gsub("'", "''")
  return "'" .. s .. "'"
end

--- Convert a Lua array to a PostgreSQL text[] literal: ARRAY['a','b']
local function pg_text_array(arr)
  local parts = {}
  for i, v in ipairs(arr) do
    parts[i] = esc(v)
  end
  return "ARRAY[" .. table.concat(parts, ",") .. "]::text[]"
end

--- Encode a Lua value as a JSONB literal suitable for SQL.
local function pg_jsonb(val)
  return esc(dkjson.encode(val)) .. "::jsonb"
end

--- Split dot-separated path into array of parts.
local function split_path(json_path)
  local parts = {}
  for seg in json_path:gmatch("[^.]+") do
    parts[#parts + 1] = seg
  end
  return parts
end

---------------------------------------------------------------------------
-- Node discovery (delegates to kb_search)
---------------------------------------------------------------------------

function KB_Document_Table:find_document_id(kb, node_name, properties, node_path)
  local results = self:find_document_ids(kb, node_name, properties, node_path)
  if #results == 0 then
    error(string.format("No document found: name=%s", tostring(node_name)))
  end
  if #results > 1 then
    error(string.format("Multiple documents (%d) found: name=%s", #results, tostring(node_name)))
  end
  return results[1]
end

function KB_Document_Table:find_document_ids(kb, node_name, properties, node_path)
  self.kb_search:clear_filters()
  self.kb_search:search_label("KB_JSONB_FIELD")

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
    error(string.format("No documents found: name=%s", tostring(node_name)))
  end
  return rows
end

function KB_Document_Table:find_document_paths(table_dict_rows)
  if not table_dict_rows then return {} end
  local rv = {}
  for _, row in ipairs(table_dict_rows) do
    if row.path then rv[#rv + 1] = tostring(row.path) end
  end
  return rv
end

---------------------------------------------------------------------------
-- Type filter helper
---------------------------------------------------------------------------

local function build_type_filter(doc_type)
  if doc_type then return " AND type = " .. esc(doc_type) end
  return ""
end

---------------------------------------------------------------------------
-- Core JSONB Operations
---------------------------------------------------------------------------

function KB_Document_Table:jsonb_get(ltree_path, json_path, as_text_or_opts, doc_type)
  -- Accept either boolean or {as_text = bool} table for 3rd arg
  local as_text = false
  if type(as_text_or_opts) == "table" then
    as_text  = as_text_or_opts.as_text or false
    doc_type = as_text_or_opts.doc_type or doc_type
  elseif type(as_text_or_opts) == "boolean" then
    as_text = as_text_or_opts
  end
  local tf = build_type_filter(doc_type)
  local accessor

  if json_path == "" or json_path == "{}" then
    accessor = "data"
  else
    local parts = split_path(json_path)
    if #parts == 1 then
      local op = as_text and "->>" or "->"
      accessor = string.format("data %s %s", op, esc(parts[1]))
    else
      local op = as_text and "#>>" or "#>"
      accessor = string.format("data %s %s", op, pg_text_array(parts))
    end
  end

  local sql_str = string.format(
    "SELECT %s as value FROM %s WHERE ltree = %s::ltree %s",
    accessor, self.table_name, esc(ltree_path), tf)

  local row = self.kb_search:_raw_query_one(sql_str)
  if not row then return nil end
  local val = row.value
  -- Attempt JSON decode for non-text results
  if not as_text and type(val) == "string" then
    local decoded = dkjson.decode(val)
    if decoded ~= nil then return decoded end
  end
  return val
end

function KB_Document_Table:jsonb_set(ltree_path, json_path, value, doc_type, create_missing)
  if create_missing == nil then create_missing = true end
  local tf = build_type_filter(doc_type)

  local sql_str
  if json_path == "" or json_path == "{}" then
    sql_str = string.format([[
      UPDATE %s SET data = %s, updated_at = CURRENT_TIMESTAMP
       WHERE ltree = %s::ltree %s RETURNING id
    ]], self.table_name, pg_jsonb(value), esc(ltree_path), tf)
  else
    local parts = split_path(json_path)
    sql_str = string.format([[
      UPDATE %s SET data = jsonb_set(data, %s, %s, %s),
             updated_at = CURRENT_TIMESTAMP
       WHERE ltree = %s::ltree %s RETURNING id
    ]], self.table_name, pg_text_array(parts), pg_jsonb(value),
       create_missing and "true" or "false", esc(ltree_path), tf)
  end

  local ok, err = pcall(function()
    local row = self.kb_search:_raw_query_one(sql_str)
    self.kb_search:commit()
    return row ~= nil
  end)

  if not ok then
    self.kb_search:rollback()
    error("Failed to set JSONB value: " .. tostring(err))
  end
  return err
end

function KB_Document_Table:jsonb_delete_key(ltree_path, key, doc_type)
  local tf = build_type_filter(doc_type)
  local sql_str = string.format([[
    UPDATE %s SET data = data - %s, updated_at = CURRENT_TIMESTAMP
     WHERE ltree = %s::ltree %s RETURNING id
  ]], self.table_name, esc(key), esc(ltree_path), tf)

  local ok, err = pcall(function()
    local row = self.kb_search:_raw_query_one(sql_str)
    self.kb_search:commit()
    return row ~= nil
  end)
  if not ok then
    self.kb_search:rollback()
    error("Failed to delete JSONB key: " .. tostring(err))
  end
  return err
end

function KB_Document_Table:jsonb_delete_path(ltree_path, json_path, doc_type)
  local tf = build_type_filter(doc_type)
  local parts = split_path(json_path)
  local sql_str = string.format([[
    UPDATE %s SET data = data #- %s, updated_at = CURRENT_TIMESTAMP
     WHERE ltree = %s::ltree %s RETURNING id
  ]], self.table_name, pg_text_array(parts), esc(ltree_path), tf)

  local ok, err = pcall(function()
    local row = self.kb_search:_raw_query_one(sql_str)
    self.kb_search:commit()
    return row ~= nil
  end)
  if not ok then
    self.kb_search:rollback()
    error("Failed to delete JSONB path: " .. tostring(err))
  end
  return err
end

---------------------------------------------------------------------------
-- Existence & Search
---------------------------------------------------------------------------

function KB_Document_Table:jsonb_has_key(ltree_path, key, doc_type)
  local tf = build_type_filter(doc_type)
  -- Use jsonb_exists() function instead of ? operator (DBI treats ? as placeholder)
  local sql_str = string.format(
    "SELECT jsonb_exists(data, %s) as has_key FROM %s WHERE ltree = %s::ltree %s",
    esc(key), self.table_name, esc(ltree_path), tf)
  local row = self.kb_search:_raw_query_one(sql_str)
  return row and (row.has_key == true or row.has_key == "t") or false
end

function KB_Document_Table:jsonb_has_any_keys(ltree_path, keys, doc_type)
  local tf = build_type_filter(doc_type)
  -- Use jsonb_exists_any() function instead of ?| operator (DBI placeholder conflict)
  local sql_str = string.format(
    "SELECT jsonb_exists_any(data, %s) as has_any FROM %s WHERE ltree = %s::ltree %s",
    pg_text_array(keys), self.table_name, esc(ltree_path), tf)
  local row = self.kb_search:_raw_query_one(sql_str)
  return row and (row.has_any == true or row.has_any == "t") or false
end

function KB_Document_Table:jsonb_has_all_keys(ltree_path, keys, doc_type)
  local tf = build_type_filter(doc_type)
  -- Use jsonb_exists_all() function instead of ?& operator (DBI placeholder conflict)
  local sql_str = string.format(
    "SELECT jsonb_exists_all(data, %s) as has_all FROM %s WHERE ltree = %s::ltree %s",
    pg_text_array(keys), self.table_name, esc(ltree_path), tf)
  local row = self.kb_search:_raw_query_one(sql_str)
  return row and (row.has_all == true or row.has_all == "t") or false
end

function KB_Document_Table:jsonb_contains(ltree_path, contained, doc_type)
  local tf = build_type_filter(doc_type)
  local sql_str = string.format(
    "SELECT data @> %s as contains FROM %s WHERE ltree = %s::ltree %s",
    pg_jsonb(contained), self.table_name, esc(ltree_path), tf)
  local row = self.kb_search:_raw_query_one(sql_str)
  return row and (row.contains == true or row.contains == "t") or false
end

function KB_Document_Table:jsonb_contained_by(ltree_path, container, doc_type)
  local tf = build_type_filter(doc_type)
  local sql_str = string.format(
    "SELECT data <@ %s as contained_by FROM %s WHERE ltree = %s::ltree %s",
    pg_jsonb(container), self.table_name, esc(ltree_path), tf)
  local row = self.kb_search:_raw_query_one(sql_str)
  return row and (row.contained_by == true or row.contained_by == "t") or false
end

---------------------------------------------------------------------------
-- Path Query Operations
---------------------------------------------------------------------------

function KB_Document_Table:jsonb_path_exists(ltree_path, json_path_query, doc_type)
  local tf = build_type_filter(doc_type)
  local sql_str = string.format(
    "SELECT jsonb_path_exists(data, %s::jsonpath) as exists FROM %s WHERE ltree = %s::ltree %s",
    esc(json_path_query), self.table_name, esc(ltree_path), tf)
  local row = self.kb_search:_raw_query_one(sql_str)
  return row and (row.exists == true or row.exists == "t") or false
end

function KB_Document_Table:jsonb_path_query(ltree_path, json_path_query, doc_type)
  local tf = build_type_filter(doc_type)
  local sql_str = string.format(
    "SELECT jsonb_path_query_array(data, %s::jsonpath) as results FROM %s WHERE ltree = %s::ltree %s",
    esc(json_path_query), self.table_name, esc(ltree_path), tf)
  local row = self.kb_search:_raw_query_one(sql_str)
  if row and row.results then
    if type(row.results) == "string" then
      return dkjson.decode(row.results) or {}
    end
    return row.results
  end
  return {}
end

function KB_Document_Table:jsonb_query(ltree_path, jsonb_filter, doc_type)
  local tf = build_type_filter(doc_type)
  local sql_str = string.format([[
    SELECT id, ltree::text as ltree, type, data
      FROM %s
     WHERE ltree = %s::ltree AND data @> %s %s
  ]], self.table_name, esc(ltree_path), pg_jsonb(jsonb_filter), tf)
  return self.kb_search:_raw_query_one(sql_str)
end

---------------------------------------------------------------------------
-- Array Operations
---------------------------------------------------------------------------

function KB_Document_Table:jsonb_array_append(ltree_path, json_path, item, doc_type)
  local tf = build_type_filter(doc_type)
  local parts = split_path(json_path)
  local arr = pg_text_array(parts)

  local sql_str = string.format([[
    UPDATE %s SET data = jsonb_set(
      data, %s,
      COALESCE(data #> %s, '[]'::jsonb) || %s, true
    ), updated_at = CURRENT_TIMESTAMP
    WHERE ltree = %s::ltree %s RETURNING id
  ]], self.table_name, arr, arr, pg_jsonb(item), esc(ltree_path), tf)

  local ok, err = pcall(function()
    local row = self.kb_search:_raw_query_one(sql_str)
    self.kb_search:commit()
    return row ~= nil
  end)
  if not ok then
    self.kb_search:rollback()
    error("Failed to append to JSONB array: " .. tostring(err))
  end
  return err
end

function KB_Document_Table:jsonb_array_prepend(ltree_path, json_path, item, doc_type)
  local tf = build_type_filter(doc_type)
  local parts = split_path(json_path)
  local arr = pg_text_array(parts)

  local sql_str = string.format([[
    UPDATE %s SET data = jsonb_set(
      data, %s,
      %s || COALESCE(data #> %s, '[]'::jsonb), true
    ), updated_at = CURRENT_TIMESTAMP
    WHERE ltree = %s::ltree %s RETURNING id
  ]], self.table_name, arr, pg_jsonb(item), arr, esc(ltree_path), tf)

  local ok, err = pcall(function()
    local row = self.kb_search:_raw_query_one(sql_str)
    self.kb_search:commit()
    return row ~= nil
  end)
  if not ok then
    self.kb_search:rollback()
    error("Failed to prepend to JSONB array: " .. tostring(err))
  end
  return err
end

function KB_Document_Table:jsonb_array_remove_index(ltree_path, json_path, index, doc_type)
  local tf = build_type_filter(doc_type)
  local parts = split_path(json_path)
  local arr = pg_text_array(parts)

  -- Get the item first
  local sel_sql = string.format(
    "SELECT (data #> %s) -> %d as item FROM %s WHERE ltree = %s::ltree %s",
    arr, index, self.table_name, esc(ltree_path), tf)
  local row = self.kb_search:_raw_query_one(sel_sql)
  local removed = row and row.item or nil

  if removed ~= nil then
    local upd_sql = string.format([[
      UPDATE %s SET data = jsonb_set(
        data, %s, (data #> %s) - %d, true
      ), updated_at = CURRENT_TIMESTAMP
      WHERE ltree = %s::ltree %s
    ]], self.table_name, arr, arr, index, esc(ltree_path), tf)

    local ok, err = pcall(function()
      self.kb_search:_raw_query(upd_sql)
      self.kb_search:commit()
    end)
    if not ok then
      self.kb_search:rollback()
      error("Failed to remove from JSONB array: " .. tostring(err))
    end
  end

  return removed
end

function KB_Document_Table:jsonb_array_contains(ltree_path, json_path, item, doc_type)
  local tf = build_type_filter(doc_type)
  local parts = split_path(json_path)
  local sql_str = string.format(
    "SELECT (data #> %s) @> %s as contains FROM %s WHERE ltree = %s::ltree %s",
    pg_text_array(parts), pg_jsonb({ item }), self.table_name, esc(ltree_path), tf)
  local row = self.kb_search:_raw_query_one(sql_str)
  return row and (row.contains == true or row.contains == "t") or false
end

function KB_Document_Table:jsonb_array_elements(ltree_path, json_path, doc_type)
  local tf = build_type_filter(doc_type)
  local parts = split_path(json_path)
  local sql_str = string.format(
    "SELECT jsonb_array_elements(data #> %s) as element FROM %s WHERE ltree = %s::ltree %s",
    pg_text_array(parts), self.table_name, esc(ltree_path), tf)
  local res = self.kb_search:_raw_query(sql_str)
  local rv = {}
  for _, row in ipairs(res) do
    rv[#rv + 1] = row.element
  end
  return rv
end

---------------------------------------------------------------------------
-- Queue / Stack Abstractions
---------------------------------------------------------------------------

function KB_Document_Table:enqueue(ltree_path, item, queue_path, doc_type)
  queue_path = queue_path or "items"
  local ok = self:jsonb_array_append(ltree_path, queue_path, item, doc_type)
  if not ok then
    error(QueueOperationError.new("Document not found: " .. ltree_path))
  end
  return true
end

function KB_Document_Table:dequeue(ltree_path, queue_path, doc_type)
  queue_path = queue_path or "items"
  return self:jsonb_array_remove_index(ltree_path, queue_path, 0, doc_type)
end

function KB_Document_Table:peek(ltree_path, queue_path, doc_type, index)
  queue_path = queue_path or "items"
  index = index or 0
  local queue = self:jsonb_get(ltree_path, queue_path, false, doc_type)
  if type(queue) == "string" then queue = dkjson.decode(queue) end
  if queue and type(queue) == "table" and queue[index + 1] then
    return queue[index + 1]
  end
  return nil
end

function KB_Document_Table:size(ltree_path, queue_path, doc_type)
  queue_path = queue_path or "items"
  local queue = self:jsonb_get(ltree_path, queue_path, false, doc_type)
  if type(queue) == "string" then queue = dkjson.decode(queue) end
  if queue and type(queue) == "table" then return #queue end
  return 0
end

function KB_Document_Table:is_empty(ltree_path, queue_path, doc_type)
  return self:size(ltree_path, queue_path, doc_type) == 0
end

function KB_Document_Table:clear(ltree_path, queue_path, doc_type)
  queue_path = queue_path or "items"
  local ok = self:jsonb_set(ltree_path, queue_path, {}, doc_type, true)
  if not ok then
    error(QueueOperationError.new("Document not found: " .. ltree_path))
  end
  return true
end

function KB_Document_Table:get_all(ltree_path, queue_path, doc_type)
  queue_path = queue_path or "items"
  local queue = self:jsonb_get(ltree_path, queue_path, false, doc_type)
  if type(queue) == "string" then queue = dkjson.decode(queue) end
  if queue and type(queue) == "table" then return queue end
  return {}
end

function KB_Document_Table:push(ltree_path, item, queue_path, doc_type)
  queue_path = queue_path or "items"
  local ok = self:jsonb_array_prepend(ltree_path, queue_path, item, doc_type)
  if not ok then
    error(QueueOperationError.new("Document not found: " .. ltree_path))
  end
  return true
end

function KB_Document_Table:pop(ltree_path, queue_path, doc_type)
  queue_path = queue_path or "items"
  local sz = self:size(ltree_path, queue_path, doc_type)
  if sz == 0 then return nil end
  return self:jsonb_array_remove_index(ltree_path, queue_path, sz - 1, doc_type)
end

function KB_Document_Table:get_metadata(ltree_path, metadata_path, doc_type)
  metadata_path = metadata_path or "metadata"
  return self:jsonb_get(ltree_path, metadata_path, false, doc_type)
end

function KB_Document_Table:set_metadata(ltree_path, metadata, metadata_path, doc_type)
  metadata_path = metadata_path or "metadata"
  return self:jsonb_set(ltree_path, metadata_path, metadata, doc_type)
end

KB_Document_Table.QueueOperationError = QueueOperationError

return KB_Document_Table