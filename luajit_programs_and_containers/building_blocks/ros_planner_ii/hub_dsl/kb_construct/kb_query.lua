--[[
    kb_query.lua -- Knowledge Base query helper for the planner.

    Reads the surface ops KB using KnowledgeBaseManager's ltree queries.
    Provides structured data for planner startup and runtime.

    All paths use lowercase namespace convention:
      {site}.robots.{name}.status.connection
      {site}.boards.{name}
      {site}.robot_class.{name}.infra.shared
      {site}.virtual_nodes.definitions.vn_type.{name}
      {site}.planner.route_planner.status.planner_state

    Usage:
        local kb_query = require("kb_query")
        local q = kb_query.new("surface_ops.db")

        local board  = q:get_board("landing_zone")
        local caps   = q:get_capabilities("rover_1")
        local conn   = q:get_connection("rover_1")
        local hw     = q:get_hardware("rover_1")
        local robots = q:list_robots()

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

    -- The site is the KB name — find it via find_by_pattern at depth 0
    -- (e.g., moonbase.alpha.surface_ops)
    local rows = self.kb:find_by_depth(0)
    if #rows > 0 then
        return rows[1].path
    end
    -- Fallback: try depth 3 for system.site.subsystem
    for depth = 1, 5 do
        rows = self.kb:find_by_depth(depth)
        if #rows > 0 then
            return rows[1].knowledge_base
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

function M:get_robot_hw(class_name, hw_name)
    local site = self:get_site()
    if not site then return nil end
    local pattern = site .. ".robot_class." .. class_name .. ".hw." .. hw_name
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
-- ROBOTS (instances)
---------------------------------------------------------------------------

function M:list_robots()
    local site = self:get_site()
    if not site then return {} end
    local rows = self.kb:find_by_pattern(site .. ".robots.*")
    local robots = {}
    local seen = {}
    for _, row in ipairs(rows) do
        if row.label == "robots" and not seen[row.name] then
            robots[#robots + 1] = row.name
            seen[row.name] = true
        end
    end
    return robots
end

function M:get_connection(instance_name)
    local site = self:get_site()
    if not site then return nil end
    local pattern = site .. ".robots." .. instance_name ..
        ".status.connection"
    local rows = self.kb:find_by_pattern(pattern)
    if #rows > 0 then
        return parse_row(rows[1]).data
    end
    return nil
end

function M:get_robot_state(instance_name)
    local site = self:get_site()
    if not site then return nil end
    local pattern = site .. ".robots." .. instance_name ..
        ".status.state"
    local rows = self.kb:find_by_pattern(pattern)
    if #rows > 0 then
        return parse_row(rows[1]).data
    end
    return nil
end

---------------------------------------------------------------------------
-- CAPABILITIES: instance → class → infra → virtual_nodes
---------------------------------------------------------------------------

function M:find_class_for_instance(instance_name)
    local site = self:get_site()
    if not site then return nil end
    -- Find hw.<instance_name> to determine class
    local rows = self.kb:find_by_pattern(site .. ".robot_class.*.hw." .. instance_name)
    if #rows > 0 then
        -- Extract class name from path
        local path = rows[1].path
        local parts = {}
        for part in path:gmatch("[^.]+") do
            parts[#parts + 1] = part
        end
        for i, p in ipairs(parts) do
            if p == "robot_class" and parts[i + 1] then
                return parts[i + 1]
            end
        end
    end
    return nil
end

function M:get_capabilities(instance_name)
    local class_name = self:find_class_for_instance(instance_name)
    if not class_name then return {} end
    local infra = self:get_robot_infra(class_name)
    if not infra then return {} end
    return infra.virtual_nodes or {}
end

---------------------------------------------------------------------------
-- ENERGY
---------------------------------------------------------------------------

function M:get_energy(instance_name)
    local site = self:get_site()
    if not site then return nil end
    local pattern = site .. ".robots." .. instance_name .. ".status.energy"
    local rows = self.kb:find_by_pattern(pattern)
    if #rows > 0 then
        return parse_row(rows[1]).data
    end
    return nil
end

function M:get_energy_max(instance_name)
    local class_name = self:find_class_for_instance(instance_name)
    if not class_name then return nil end
    local infra = self:get_robot_infra(class_name)
    if not infra then return nil end
    return infra.energy_max
end

function M:get_energy_infinite(instance_name)
    local class_name = self:find_class_for_instance(instance_name)
    if not class_name then return false end
    local infra = self:get_robot_infra(class_name)
    if not infra then return false end
    return infra.energy_infinite == true
end

---------------------------------------------------------------------------
-- HARDWARE
---------------------------------------------------------------------------

function M:get_hardware(instance_name)
    local class_name = self:find_class_for_instance(instance_name)
    if not class_name then return nil end
    return self:get_robot_hw(class_name, instance_name)
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
-- NATS TOPICS
---------------------------------------------------------------------------

function M:get_nats_topics(instance_name)
    local conn = self:get_connection(instance_name)
    if not conn then return nil end
    return {
        rpc_topic        = conn.rpc_topic,
        stream_bus_topic = conn.stream_bus_topic,
        nats_server      = conn.nats_server,
        robot_id         = conn.robot_id,
    }
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
-- FULL CONFIG
---------------------------------------------------------------------------

function M:get_robot_config(instance_name)
    return {
        instance     = instance_name,
        class        = self:find_class_for_instance(instance_name),
        capabilities = self:get_capabilities(instance_name),
        hardware     = self:get_hardware(instance_name),
        connection   = self:get_connection(instance_name),
        state        = self:get_robot_state(instance_name),
        energy          = self:get_energy(instance_name),
        energy_max      = self:get_energy_max(instance_name),
        energy_infinite = self:get_energy_infinite(instance_name),
        nats         = self:get_nats_topics(instance_name),
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
    local robots = self:list_robots()

    local robot_configs = {}
    for _, name in ipairs(robots) do
        robot_configs[name] = self:get_robot_config(name)
    end

    return {
        site          = site,
        boards        = boards,
        robots        = robot_configs,
        planner       = self:get_planner_state(),
        virtual_nodes = self:get_all_virtual_nodes(),
    }
end

return M
