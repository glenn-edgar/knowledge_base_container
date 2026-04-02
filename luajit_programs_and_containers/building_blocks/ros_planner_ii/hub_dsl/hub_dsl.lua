--[[
    hub_dsl.lua -- Universal hub DSL. Assembles KBs from plugin files.

    Scans kb/ directory for plugin definitions. Each plugin provides:
      - name, index, packet_ctype, packet_type_id
      - json_schema, mapping
      - define_tree(ct, kb_name, one_shot_name)

    Build: luajit hub_dsl.lua hub.json
    Output: hub.json + hub_debug.yaml
]]

local ChainTreeMaster = require("chain_tree_master")
local lfs_ok, lfs = pcall(require, "lfs")

-- =========================================================================
-- Discover KB plugins
-- =========================================================================

local script_dir = debug.getinfo(1, "S").source:match("^@(.*/)")  or "./"
local kb_dir = script_dir .. "kb/"

-- Load all .lua files from kb/ directory
local kb_plugins = {}

if lfs_ok then
    -- Use lfs if available
    for file in lfs.dir(kb_dir) do
        if file:match("%.lua$") then
            local plugin = dofile(kb_dir .. file)
            if plugin and plugin.name and plugin.index then
                kb_plugins[#kb_plugins + 1] = plugin
            end
        end
    end
else
    -- Fallback: explicit list (works without lfs)
    local kb_files = {
        "error_recovery",
        "init_check", "path_spline", "path_line", "path_wall",
        "path_rotate", "deliver_part", "paint_sample", "load_shipping",
        "pass_gate", "inspection_scan", "recharge", "idle",
    }
    for _, name in ipairs(kb_files) do
        local ok, plugin = pcall(dofile, kb_dir .. name .. ".lua")
        if ok and plugin and plugin.name then
            kb_plugins[#kb_plugins + 1] = plugin
        end
    end
end

-- Sort by index for deterministic output
table.sort(kb_plugins, function(a, b) return a.index < b.index end)

-- =========================================================================
-- Merge KB virtual node definitions into plugins
-- =========================================================================

local function merge_kb_data(plugins)
    -- Try to load VN definitions from KB (surface_ops.db)
    local ok, kb_query = pcall(require, "kb_query")
    if not ok then return end

    local kb_construct_dir = script_dir .. "kb_construct/"
    local db_file = kb_construct_dir .. "surface_ops.db"

    -- Check if DB exists
    local f = io.open(db_file, "r")
    if not f then return end
    f:close()

    local q_ok, q = pcall(kb_query.new, db_file, "knowledge_base", "/usr/local/lib/ltree")
    if not q_ok then return end

    local vn_defs = q:get_all_virtual_nodes()
    q:close()

    if not vn_defs or not next(vn_defs) then return end

    for _, plugin in ipairs(plugins) do
        local vn = vn_defs[plugin.name]
        if vn then
            -- Merge KB data into plugin (KB is source of truth)
            plugin.packet_type_id = vn.packet_type_id or plugin.packet_type_id
            plugin.json_schema    = vn.json_schema or plugin.json_schema or {}
            plugin.bitmask        = vn.bitmask or plugin.bitmask or {}
            plugin.pose_fields    = vn.pose_fields or plugin.pose_fields or {}
            plugin.mapping        = plugin.mapping or {}
        else
            -- Plugin not in KB (e.g. error_recovery with packet_type_id=7/idle)
            -- Keep plugin defaults
            plugin.json_schema = plugin.json_schema or {}
            plugin.bitmask     = plugin.bitmask or {}
            plugin.pose_fields = plugin.pose_fields or {}
            plugin.mapping     = plugin.mapping or {}
        end
    end
end

merge_kb_data(kb_plugins)

-- =========================================================================
-- Blackboard (auto-assembled)
-- =========================================================================

local function add_header(yaml_file)
    local ct = ChainTreeMaster.new(yaml_file)

    ct:define_blackboard("hub_state")
        -- Bitmap inputs (from streaming channel)
        ct:bb_field("bitmap_robot_good_conditions", "uint64", 0)
        ct:bb_field("bitmap_robot_faults",          "uint64", 0)
        ct:bb_field("bitmap_seg_complete",          "bool", false)
        ct:bb_field("bitmap_action_complete",       "bool", false)
        ct:bb_field("bitmap_obstacle",              "bool", false)
        ct:bb_field("bitmap_motor_fault",           "bool", false)
        ct:bb_field("bitmap_action_fault",          "bool", false)
        -- Odometry
        ct:bb_field("local_distance_x",             "float", 0)
        ct:bb_field("local_distance_y",             "float", 0)
        ct:bb_field("current_heading",              "float", 0)
        ct:bb_field("current_speed",                "float", 0)
        -- Command packet (FFI pointer, written by generic mapper)
        ct:bb_field("command_packet",               "pointer", 0)
        ct:bb_field("command_packet_size",           "int32", 0)
        ct:bb_field("command_pending",              "bool", false)
        -- Status
        ct:bb_field("action_status",                "int32", 0)
        ct:bb_field("recovery_count",               "int32", 0)
        -- Double-buffered action JSON
        ct:bb_field("current_test_json",            "string", "")
        ct:bb_field("next_test_json",               "string", "")
    ct:end_blackboard()

    return ct
end

-- =========================================================================
-- Build all KBs from plugins
-- =========================================================================

-- Detect if running as CLI (arg[0] contains this file) vs required as module
local is_cli = arg and arg[0] and arg[0]:match("hub_dsl%.lua$")

if is_cli then
    if #arg ~= 1 then
        print("Usage: luajit hub_dsl.lua <json_file>")
        os.exit(1)
    end

    local json_file = arg[1]
    local ct = add_header(json_file)

    -- Build each KB from its plugin
    local test_list = {}
    for _, plugin in ipairs(kb_plugins) do
        local one_shot_name = "GENERATE_" .. string.upper(plugin.name) .. "_AVRC_PACKET"
        plugin.define_tree(ct, plugin.name, one_shot_name, plugin)
        test_list[#test_list + 1] = plugin.name
    end

    ct:check_and_generate_yaml()
    ct:generate_debug_yaml()
    ct:display_chain_tree_function_mapping()

    -- Print summary
    print("KBs: " .. table.concat(test_list, ", "))
    print("Total nodes: " .. ct.ctb:get_total_node_count())
    print("")

    -- Print plugin index map
    print("KB index map:")
    for _, plugin in ipairs(kb_plugins) do
        print(string.format("  %2d = %s (packet: %s, type_id: %d)",
            plugin.index, plugin.name, plugin.packet_ctype, plugin.packet_type_id))
    end
end

-- Export for runtime use
return {
    plugins = kb_plugins,
    add_header = add_header,
}
