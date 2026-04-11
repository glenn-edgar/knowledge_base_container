--[[
    kb_query.lua -- Knowledge Base query helper for the planner.

    Reads the surface ops KB using KnowledgeBaseManager's ltree queries.
    Provides structured data for planner startup and runtime.

    Robot instances are NOT in the KB. Robots register dynamically via
    the link protocol and announce their class_name. The KB only stores
    robot classes (capabilities, energy config, topic templates).

    All paths use lowercase namespace convention:
      {site}.boards.{name}
      {site}.robot_class.{name}.infra.shared
      {site}.virtual_nodes.definitions.vn_type.{name}
      {site}.planner.route_planner.status.planner_state

    Usage:
        local kb_query = require("kb_query")
        local q = kb_query.new("surface_ops.db")

        local board   = q:get_board("landing_zone")
        local caps    = q:get_class_capabilities("lunar_rover")
        local classes = q:list_robot_classes()

        q:close()
]]

local KBM = require("knowledge_base_manager")
local json = require("sqlite3_helpers").json

local M = {}
M.__index = M

function M.new(db_file, database, ltree_path, site)
    local self = setmetatable({}, M)
    database = database or "knowledge_base"
    -- KBM.new(table_name, db_path, ltree_extension_path, upload_flag)
    -- upload_flag=true skips table creation (read-only mode)
    self.kb = KBM.new(database, db_file, ltree_path, true)
    self.database = database
    self._site = site  -- optional: skip auto-detection in get_site()
    return self
end

function M:close()
    self.kb:disconnect()
end

---------------------------------------------------------------------------
-- Helper: parse JSON fields from a row
---------------------------------------------------------------------------

local function parse_row(row)
    local r = {
        path  = row.path,
        label = row.label,
        name  = row.name,
    }
    if row.properties and row.properties ~= "" then
        local ok, p = pcall(json.decode, row.properties)
        r.properties = ok and p or {}
    else
        r.properties = {}
    end
    if row.data and row.data ~= "" then
        local ok, d = pcall(json.decode, row.data)
        r.data = ok and d or {}
    else
        r.data = {}
    end
    return r
end

---------------------------------------------------------------------------
-- Find site root
---------------------------------------------------------------------------

function M:get_site()
    -- If site was passed to constructor, use it directly
    if self._site then return self._site end

    -- Prefer domain entry (subsystems KB) — authoritative site name
    local domain = self:get_domain()
    if domain and domain.site then return domain.site end

    -- Fallback: find the domain KB root (filter out system/subsystems)
    local rows = self.kb:find_by_depth(0)
    for _, row in ipairs(rows) do
        if row.knowledge_base ~= "system" and row.knowledge_base ~= "subsystems" then
            return row.path
        end
    end
    return nil
end

---------------------------------------------------------------------------
-- BOARD
---------------------------------------------------------------------------

function M:get_board(board_name)
    local site = self:get_site()
    if not site then return nil end
    local pattern = site .. ".boards." .. board_name
    local rows = self.kb:find_by_pattern(pattern)
    if #rows > 0 then
        return parse_row(rows[1]).data
    end
    return nil
end

function M:list_boards()
    local site = self:get_site()
    if not site then return {} end
    local rows = self.kb:find_by_pattern(site .. ".boards.*")
    local boards = {}
    for _, row in ipairs(rows) do
        if row.label == "boards" then
            boards[#boards + 1] = row.name
        end
    end
    return boards
end

---------------------------------------------------------------------------
-- ROBOT_CLASS
---------------------------------------------------------------------------

function M:get_robot_infra(class_name)
    local site = self:get_site()
    if not site then return nil end
    local pattern = site .. ".robot_class." .. class_name .. ".infra.shared"
    local rows = self.kb:find_by_pattern(pattern)
    if #rows > 0 then
        return parse_row(rows[1]).data
    end
    return nil
end

function M:list_robot_classes()
    local site = self:get_site()
    if not site then return {} end
    local rows = self.kb:find_by_pattern(site .. ".robot_class.*")
    local classes = {}
    local seen = {}
    for _, row in ipairs(rows) do
        if row.label == "robot_class" and not seen[row.name] then
            classes[#classes + 1] = row.name
            seen[row.name] = true
        end
    end
    return classes
end

---------------------------------------------------------------------------
-- CLASS-BASED CAPABILITIES (robots register dynamically via link protocol)
---------------------------------------------------------------------------

function M:get_class_capabilities(class_name)
    local infra = self:get_robot_infra(class_name)
    if not infra then return {} end
    return infra.virtual_nodes or {}
end

function M:get_class_operation_types(class_name)
    local infra = self:get_robot_infra(class_name)
    if not infra then return {} end
    return infra.operation_types or {}
end

function M:get_class_energy_max(class_name)
    local infra = self:get_robot_infra(class_name)
    if not infra then return nil end
    return infra.energy_max
end

function M:get_class_energy_infinite(class_name)
    local infra = self:get_robot_infra(class_name)
    if not infra then return false end
    return infra.energy_infinite == true
end

---------------------------------------------------------------------------
-- PLANNER
---------------------------------------------------------------------------

function M:get_planner_state()
    local site = self:get_site()
    if not site then return nil end
    local rows = self.kb:find_by_pattern(
        site .. ".planner.route_planner.status.planner_state")
    if #rows > 0 then
        return parse_row(rows[1]).data
    end
    return nil
end


---------------------------------------------------------------------------
-- INFRASTRUCTURE SERVICES (system KB)
---------------------------------------------------------------------------

--- Query infrastructure service connection details from the system KB.
-- Searches the "system" knowledge_base for infrastructure containers
-- and their service nodes (nats, mqtt, postgres, etc.).
-- @return table: { nats={host,port,ws_port}, mqtt={host,port}, postgres={host,port,dbname} }
function M:get_infrastructure()
    local infra = {}
    -- Infrastructure services live in "system" KB with paths like:
    --   system.site.main.cpu.cpu_01.container.nats_server.service.nats
    -- Find all service nodes under infrastructure containers
    local rows = self.kb:find_by_pattern("system.site.*.cpu.*.container.*.service.*", "system")
    for _, row in ipairs(rows) do
        local r = parse_row(row)
        if r.data and r.data.port then
            infra[r.name] = r.data
        end
    end
    return infra
end

--- Get this container's own configuration from the system KB.
-- @param container_name string: e.g. "surface_ops_planner"
-- @return table: { image, sqlite_db, params }
function M:get_container_config(container_name)
    local rows = self.kb:find_by_pattern(
        "system.site.*.cpu.*.container." .. container_name, "system")
    if #rows > 0 then
        return parse_row(rows[1]).data
    end
    return nil
end

--- Get the domain config from the subsystems KB (extracted SQLite only).
-- Returns { site, container } where site is the full namespace
-- (e.g. "moonbase.alpha.surface_ops").
-- @return table or nil
function M:get_domain()
    local rows = self.kb:find_by_pattern("subsystems.domain.*", "subsystems")
    if #rows > 0 then
        return parse_row(rows[1]).data
    end
    return nil
end

---------------------------------------------------------------------------
-- FULL CLASS CONFIG
---------------------------------------------------------------------------

function M:get_class_config(class_name)
    local infra = self:get_robot_infra(class_name)
    if not infra then return nil end
    return {
        class_name      = class_name,
        capabilities    = infra.virtual_nodes or {},
        energy_max      = infra.energy_max,
        energy_infinite = infra.energy_infinite == true,
        topics          = infra.topics,
        tick_rate_ms    = infra.tick_rate_ms,
        worker_kbs      = infra.worker_kbs,
    }
end

---------------------------------------------------------------------------
-- Virtual node definitions
---------------------------------------------------------------------------

--- Get a single virtual node type definition.
-- @param vn_name string: e.g. "path_spline"
-- @return table: { packet_type_id, description, json_schema, bitmask, pose_fields }
function M:get_virtual_node(vn_name)
    local site = self:get_site()
    local path = site .. ".virtual_nodes.definitions.vn_type." .. vn_name
    local rows = self.kb:find_by_pattern(path)
    if #rows > 0 then
        local r = parse_row(rows[1])
        local result = r.data or {}
        if r.properties and r.properties.packet_type_id then
            result.packet_type_id = r.properties.packet_type_id
        end
        result.name = vn_name
        return result
    end
    return nil
end

--- List all virtual node type names.
-- @return array of strings
function M:list_virtual_nodes()
    local site = self:get_site()
    local parent = site .. ".virtual_nodes.definitions"
    local rows = self.kb:find_descendants(parent)
    local names = {}
    for _, row in ipairs(rows) do
        local name = row.path:match("vn_type%.([^.]+)$")
        if name then
            names[#names + 1] = name
        end
    end
    return names
end

--- Get all virtual node definitions as a table keyed by name.
-- @return table: { name = { packet_type_id, json_schema, bitmask, pose_fields } }
function M:get_all_virtual_nodes()
    local names = self:list_virtual_nodes()
    local result = {}
    for _, name in ipairs(names) do
        result[name] = self:get_virtual_node(name)
    end
    return result
end

function M:get_site_config()
    local site = self:get_site()
    local boards = self:list_boards()
    local classes = self:list_robot_classes()

    local class_configs = {}
    for _, name in ipairs(classes) do
        class_configs[name] = self:get_class_config(name)
    end

    return {
        site           = site,
        boards         = boards,
        robot_classes  = class_configs,
        planner        = self:get_planner_state(),
        virtual_nodes  = self:get_all_virtual_nodes(),
    }
end

return M
