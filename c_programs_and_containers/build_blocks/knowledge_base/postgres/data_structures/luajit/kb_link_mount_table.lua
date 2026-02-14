--[[
  KB_Link_Mount_Table - Query the knowledge_base_link_mount table.
  LuaJIT port using luadbi-postgresql.

  Usage:
    local KB_Link_Mount_Table = require("kb_link_mount_table")
    local lmt = KB_Link_Mount_Table.new(kb_search, "knowledge_base")
    local recs = lmt:find_records_by_link_name("my_link")
]]

local KB_Link_Mount_Table = {}
KB_Link_Mount_Table.__index = KB_Link_Mount_Table

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

function KB_Link_Mount_Table.new(kb_search, database)
  local self = setmetatable({}, KB_Link_Mount_Table)
  self.kb_search  = kb_search
  self.dbh        = kb_search:get_connection()
  self.base_table = database .. "_link_mount"
  return self
end

---------------------------------------------------------------------------
-- Queries
---------------------------------------------------------------------------

--- Find records by link_name, optionally filtered by knowledge_base.
function KB_Link_Mount_Table:find_records_by_link_name(link_name, kb)
  local sql_str
  if kb then
    sql_str = string.format(
      "SELECT * FROM %s WHERE link_name = %s AND knowledge_base = %s",
      self.base_table, esc(link_name), esc(kb))
  else
    sql_str = string.format(
      "SELECT * FROM %s WHERE link_name = %s",
      self.base_table, esc(link_name))
  end
  return self.kb_search:_raw_query(sql_str)
end

--- Find records by mount_path, optionally filtered by knowledge_base.
function KB_Link_Mount_Table:find_records_by_mount_path(mount_path, kb)
  local sql_str
  if kb then
    sql_str = string.format(
      "SELECT * FROM %s WHERE mount_path = %s AND knowledge_base = %s",
      self.base_table, esc(mount_path), esc(kb))
  else
    sql_str = string.format(
      "SELECT * FROM %s WHERE mount_path = %s",
      self.base_table, esc(mount_path))
  end
  return self.kb_search:_raw_query(sql_str)
end

--- Get all unique link names.
function KB_Link_Mount_Table:find_all_link_names()
  local sql_str = string.format(
    "SELECT DISTINCT link_name FROM %s ORDER BY link_name", self.base_table)
  local rows = self.kb_search:_raw_query(sql_str)
  local rv = {}
  for _, row in ipairs(rows) do rv[#rv + 1] = row.link_name end
  return rv
end

--- Get all unique mount paths.
function KB_Link_Mount_Table:find_all_mount_paths()
  local sql_str = string.format(
    "SELECT DISTINCT mount_path FROM %s ORDER BY mount_path", self.base_table)
  local rows = self.kb_search:_raw_query(sql_str)
  local rv = {}
  for _, row in ipairs(rows) do rv[#rv + 1] = row.mount_path end
  return rv
end

return KB_Link_Mount_Table

