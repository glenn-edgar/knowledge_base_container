--[[
  construct_kb.lua
  
  LuaJIT translation of construct_kb.py
  Stack-based knowledge base construction layer on top of KnowledgeBaseManager.
  
  Dependencies:
    - knowledge_base_manager (local module)
  
  Usage:
    local ConstructKB = require("construct_kb")
    local kb = ConstructKB.new("localhost", 5432, "knowledge_base", "user", "pass")
]]

local KnowledgeBaseManager = require("knowledge_base_manager")

local Construct_KB = {}
Construct_KB.__index = Construct_KB

--- Create a new Construct_KB instance.
-- @param host string        Database host
-- @param port number|string Database port
-- @param dbname string      Database name
-- @param user string        Database user
-- @param password string    Database password
-- @param table_name string  (default "knowledge_base")
-- @param upload_flag boolean (default false)
-- @return Construct_KB
function Construct_KB.new(host, port, dbname, user, password, table_name, upload_flag)
  table_name  = table_name or "knowledge_base"
  upload_flag = upload_flag or false

  local connection_params = {
    host     = host,
    port     = tonumber(port) or 5432,
    database = dbname,
    user     = user,
    password = password,
  }

  -- Create the base manager (connects + creates tables)
  local base = KnowledgeBaseManager.new(table_name, connection_params, upload_flag)

  -- Re-tag the metatable so we get Construct_KB methods
  local self = setmetatable(base, Construct_KB)

  -- Construct_KB-specific state
  self.path        = {}   -- kb_name -> list of path segments
  self.path_values = {}   -- kb_name -> set of full paths seen
  self.working_kb  = nil  -- currently selected kb

  return self
end

-- Inherit KnowledgeBaseManager methods
setmetatable(Construct_KB, { __index = KnowledgeBaseManager })

---------------------------------------------------------------------------
-- Public API
---------------------------------------------------------------------------

--- Return the pgmoon connection object.
-- @return pgmoon connection
function Construct_KB:get_db_objects()
  return self.pg
end

--- Add a new knowledge base.
-- @param kb_name string
-- @param description string (default "")
function Construct_KB:add_kb(kb_name, description)
  description = description or ""

  if self.path[kb_name] then
    error("Knowledge base " .. kb_name .. " already exists")
  end

  self.path[kb_name]        = { kb_name }
  self.path_values[kb_name] = {}

  KnowledgeBaseManager.add_kb(self, kb_name, description)
end

--- Select a knowledge base as the active working kb.
-- @param kb_name string
function Construct_KB:select_kb(kb_name)
  if not self.path[kb_name] then
    error("Knowledge base " .. kb_name .. " does not exist")
  end
  self.working_kb = kb_name
end

--- Join a path-segment list with dots.
-- @param segments table  array of strings
-- @return string
local function join_path(segments)
  return table.concat(segments, ".")
end

--- Add a header node (pushes onto path stack).
-- @param link string
-- @param node_name string
-- @param node_properties table
-- @param node_data table
-- @param description string (default "")
-- @return string  the full path of the new node
function Construct_KB:add_header_node(link, node_name, node_properties, node_data, description)
  description = description or ""
  assert(type(description) == "string", "description must be a string")
  assert(type(node_properties) == "table", "node_properties must be a table")

  if description ~= nil then
    node_properties["description"] = description
  end

  local segments = self.path[self.working_kb]
  segments[#segments + 1] = link
  segments[#segments + 1] = node_name

  local node_path = join_path(segments)

  if self.path_values[self.working_kb][node_path] then
    error("Path " .. node_path .. " already exists in knowledge base")
  end

  self.path_values[self.working_kb][node_path] = true

  local path = join_path(segments)
  if os.getenv("DCS_KB_VERBOSE") == "1" then print("path", path) end

  KnowledgeBaseManager.add_node(self, self.working_kb, link, node_name,
                                 node_properties, node_data, path)
  return path
end

--- Add an info (leaf) node — pushes then immediately pops.
-- @param link string
-- @param node_name string
-- @param node_properties table
-- @param node_data table
-- @param description string (default "")
function Construct_KB:add_info_node(link, node_name, node_properties, node_data, description)
  self:add_header_node(link, node_name, node_properties, node_data, description)

  local segments = self.path[self.working_kb]
  segments[#segments] = nil  -- pop node_name
  segments[#segments] = nil  -- pop link
end

--- Leave (pop) a header node, verifying label and name match.
-- @param label string  expected link
-- @param name string   expected node name
function Construct_KB:leave_header_node(label, name)
  local segments = self.path[self.working_kb]

  if not segments or #segments == 0 then
    error("Cannot leave a header node: path is empty")
  end

  local ref_name = segments[#segments]
  segments[#segments] = nil  -- pop

  if #segments == 0 then
    segments[#segments + 1] = ref_name  -- put it back
    error("Cannot leave a header node: not enough elements in path")
  end

  local ref_label = segments[#segments]
  segments[#segments] = nil  -- pop

  -- Verify
  local errors = {}
  if ref_name ~= name then
    errors[#errors + 1] = string.format("Expected name '%s', but got '%s'", name, ref_name)
  end
  if ref_label ~= label then
    errors[#errors + 1] = string.format("Expected label '%s', but got '%s'", label, ref_label)
  end

  if #errors > 0 then
    error(table.concat(errors, ", "))
  end
end

--- Run `body(self)` between paired add_header_node / leave_header_node
--- calls. Eliminates the add/leave pairing-hazard bug class. If `body`
--- raises, the header is still popped before the error propagates, so
--- the path stack ends in a consistent state.
--
-- @param link string
-- @param name string
-- @param properties table
-- @param data table
-- @param description string
-- @param body function(kb) -> void
function Construct_KB:with_header(link, name, properties, data, description, body)
  self:add_header_node(link, name, properties, data, description)
  local ok, err = pcall(body, self)
  self:leave_header_node(link, name)
  if not ok then error(err, 0) end
end

--- Run `body(self)` with a different working KB selected, restoring
--- the previous working_kb afterward. Survives errors the same way
--- as with_header.
--
-- @param kb_name string
-- @param body function(kb) -> void
function Construct_KB:with_kb(kb_name, body)
  local prior = self.working_kb
  self:select_kb(kb_name)
  local ok, err = pcall(body, self)
  self.working_kb = prior
  if not ok then error(err, 0) end
end

--- Add a link node at the current path.
-- @param link_name string
function Construct_KB:add_link_node(link_name)
  KnowledgeBaseManager.add_link(self, self.working_kb,
                                 join_path(self.path[self.working_kb]),
                                 link_name)
end

--- Add a link mount at the current path.
-- @param link_mount_name string
-- @param description string (default "")
function Construct_KB:add_link_mount(link_mount_name, description)
  description = description or ""
  KnowledgeBaseManager.add_link_mount(self, self.working_kb,
                                       join_path(self.path[self.working_kb]),
                                       link_mount_name, description)
end

--- Verify all knowledge bases have been fully unwound.
-- @return true on success
-- @raise error if any path stack is not clean
function Construct_KB:check_installation()
  for kb_name, segments in pairs(self.path) do
    if #segments ~= 1 then
      error(string.format(
        "Installation check failed: Path is not empty for knowledge base %s. Path: {%s}",
        kb_name, join_path(segments)))
    end
    if segments[1] ~= kb_name then
      error(string.format(
        "Installation check failed: Path root mismatch for knowledge base %s. Path: {%s}",
        kb_name, join_path(segments)))
    end
  end
  return true
end

return Construct_KB