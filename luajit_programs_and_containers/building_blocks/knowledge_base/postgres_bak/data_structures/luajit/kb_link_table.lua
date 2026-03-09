--[[
  KB_Link_Table - Query the knowledge_base_link table.
  LuaJIT port using luadbi-postgresql.

  Usage:
    local KB_Link_Table = require("kb_link_table")
    local lt = KB_Link_Table.new(kb_search, "knowledge_base")
    local recs = lt:find_records_by_link_name("my_link")
]]

local KB_Link_Table = {}
KB_Link_Table.__index = KB_Link_Table

---------------------------------------------------------------------------
-- Helpers
---------------------------------------------------------------------------

local function esc(val)
  if val == nil then return "NULL" end
  local s = tostring(val)
  s = s:gsub("'", "''")
  return "'" .. s .. "'"
end

---------------------------------------------------------------------------
-- Constructor
---------------------------------------------------------------------------

function KB_Link_Table.new(kb_search, database)
  local self = setmetatable({}, KB_Link_Table)
  self.kb_search  = kb_search
  self.dbh        = kb_search:get_connection()
  self.base_table = database .. "_link"
  return self
end

---------------------------------------------------------------------------
-- Queries
---------------------------------------------------------------------------

--- Find records by link_name, optionally filtered by parent_node_kb.
function KB_Link_Table:find_records_by_link_name(link_name, kb)
  local sql_str
  if kb then
    sql_str = string.format(
      "SELECT * FROM %s WHERE link_name = %s AND parent_node_kb = %s",
      self.base_table, esc(link_name), esc(kb))
  else
    sql_str = string.format(
      "SELECT * FROM %s WHERE link_name = %s",
      self.base_table, esc(link_name))
  end
  return self.kb_search:_raw_query(sql_str)
end

--- Find records by parent_path, optionally filtered by parent_node_kb.
function KB_Link_Table:find_records_by_node_path(node_path, kb)
  local sql_str
  if kb then
    sql_str = string.format(
      "SELECT * FROM %s WHERE parent_path = %s AND parent_node_kb = %s",
      self.base_table, esc(node_path), esc(kb))
  else
    sql_str = string.format(
      "SELECT * FROM %s WHERE parent_path = %s",
      self.base_table, esc(node_path))
  end
  return self.kb_search:_raw_query(sql_str)
end

--- Get all unique link names.
function KB_Link_Table:find_all_link_names()
  local sql_str = string.format(
    "SELECT DISTINCT link_name FROM %s ORDER BY link_name", self.base_table)
  local rows = self.kb_search:_raw_query(sql_str)
  local rv = {}
  for _, row in ipairs(rows) do rv[#rv + 1] = row.link_name end
  return rv
end

--- Get all unique parent paths.
function KB_Link_Table:find_all_node_names()
  local sql_str = string.format(
    "SELECT DISTINCT parent_path FROM %s ORDER BY parent_path", self.base_table)
  local rows = self.kb_search:_raw_query(sql_str)
  local rv = {}
  for _, row in ipairs(rows) do rv[#rv + 1] = row.parent_path end
  return rv
end

return KB_Link_Table

