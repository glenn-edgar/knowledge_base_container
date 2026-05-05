--[[
    construct_surface_ops.lua -- Knowledge Base DSL for Moon Base Surface Ops.

    Builds the SQLite KB for moonbase.alpha.surface_ops.
    Compatible with Postgres structure (same paths, same hierarchy).

    Namespace convention: all lowercase, dot-separated.
      moonbase.alpha.surface_ops
      +-- boards.landing_zone
      +-- robot_class.lunar_rover
      |   +-- infra.shared
      +-- robot_class.construction_arm
      |   +-- infra.shared
      +-- planner.route_planner
      |   +-- status.planner_state
      +-- virtual_nodes.init_check
      +-- virtual_nodes.path_spline
      +-- ...
      +-- virtual_nodes.idle

    Usage:
      luajit construct_surface_ops.lua <db_file>
      luajit construct_surface_ops.lua surface_ops.db
]]

local CDT = require("construct_data_tables")
local board_builder = require("board_builder")

local DATABASE = "knowledge_base"
local SITE = "moonbase.alpha.surface_ops"

-- Parse args
if #arg < 1 then
    print("Usage: luajit construct_surface_ops.lua <db_file>")
    os.exit(1)
end
local db_file = arg[1]

local kb = CDT.new(db_file, DATABASE)

-- =====================================================================
-- Create site root
-- =====================================================================
kb:add_kb(SITE, "Moon Base Alpha — Surface Operations")
kb:select_kb(SITE)

-- =====================================================================
-- boards.landing_zone — virtual node graph
-- =====================================================================
local nodes = {
    -- Western zone
    { name = "lander_pad",       x = 0,    y = 0,    type = "base" },
    { name = "habitat_site",     x = 800,  y = 0,    type = "pass_gate" },
    { name = "junction_north",   x = 1200, y = 0,    type = "transit" },
    { name = "junction_central", x = 800,  y = 400,  type = "transit" },
    { name = "charging_station", x = 800,  y = 800,  type = "recharge" },
    { name = "survey_point_1",   x = 0,    y = 800,  type = "inspection_scan" },
    { name = "survey_point_2",   x = 0,    y = 1600, type = "inspection_scan" },
    -- Construction zone — transit approach lattice with curved docking paths
    { name = "transit_build_n",  x = 800,  y = 1400, type = "transit" },
    { name = "transit_build_w",  x = 600,  y = 1600, type = "transit" },
    { name = "transit_build_e",  x = 1000, y = 1600, type = "transit" },
    { name = "transit_build_s",  x = 800,  y = 1800, type = "transit" },
    { name = "construction_bay", x = 800,  y = 1600, type = "load_shipping",
      params = { arm_target = 30, payload_type = 2 } },
    { name = "paint_station",    x = 1000, y = 1800, type = "paint_sample",
      params = { arm_target = 15, hold_time = 500 } },
    -- Mining/inspection station — transit ring with operation spurs
    { name = "transit_mine_w",   x = 1500, y = 0,    type = "transit" },
    { name = "transit_mine_n",   x = 1600, y = -60,  type = "transit" },
    { name = "transit_mine_e",   x = 1700, y = 0,    type = "transit" },
    { name = "transit_mine_s",   x = 1600, y = 100,  type = "transit" },
    { name = "mining_zone_a",    x = 1550, y = -80,  type = "deliver_part",
      params = { arm_target = -45, payload_type = 1 } },
    { name = "inspection_scan",  x = 1650, y = -80,  type = "inspection_scan" },
    -- Eastern zone
    { name = "mining_zone_b",    x = 1600, y = 800,  type = "deliver_part" },
}

local edges = {
    -- Western spine
    { from = "lander_pad",       to = "habitat_site",     nav = "path_spline", speed = 150, weight = 800,  path = {} },
    { from = "lander_pad",       to = "survey_point_1",   nav = "path_spline", speed = 120, weight = 800,  path = {} },
    { from = "habitat_site",     to = "junction_north",   nav = "path_spline", speed = 150, weight = 400,  path = {} },
    { from = "habitat_site",     to = "junction_central", nav = "path_spline", speed = 140, weight = 400,  path = {} },
    { from = "junction_central", to = "charging_station", nav = "path_spline", speed = 130, weight = 400,  path = {} },
    { from = "charging_station", to = "transit_build_n",   nav = "path_spline", speed = 130, weight = 600,  path = {} },
    { from = "survey_point_1",   to = "charging_station", nav = "path_spline", speed = 120, weight = 1131, path = {} },
    { from = "survey_point_1",   to = "survey_point_2",   nav = "path_line",   speed = 100, weight = 800,  path = {} },
    { from = "survey_point_2",   to = "transit_build_w",   nav = "path_spline", speed = 120, weight = 600,  path = {} },
    -- Construction zone transit lattice
    { from = "transit_build_n",  to = "transit_build_w",   nav = "path_spline", speed = 120, weight = 280,  path = {} },
    { from = "transit_build_n",  to = "transit_build_e",   nav = "path_spline", speed = 120, weight = 280,  path = {} },
    { from = "transit_build_w",  to = "transit_build_s",   nav = "path_spline", speed = 120, weight = 280,  path = {} },
    { from = "transit_build_e",  to = "transit_build_s",   nav = "path_spline", speed = 120, weight = 280,  path = {} },
    -- Curved docking approach: north → construction_bay (arc from above)
    { from = "transit_build_n",  to = "construction_bay",  nav = "path_spline", speed = 80,  weight = 220,
      path = { {x=780, y=1450}, {x=760, y=1520}, {x=780, y=1570} } },
    -- Curved docking approach: west → construction_bay (sweep from left)
    { from = "transit_build_w",  to = "construction_bay",  nav = "path_spline", speed = 80,  weight = 220,
      path = { {x=650, y=1580}, {x=700, y=1560}, {x=750, y=1580} } },
    -- Paint station spur from south-east transit (curved approach)
    { from = "transit_build_s",  to = "paint_station",     nav = "path_spline", speed = 80,  weight = 220,
      path = { {x=850, y=1820}, {x=920, y=1840}, {x=970, y=1820} } },
    { from = "transit_build_e",  to = "paint_station",     nav = "path_spline", speed = 80,  weight = 280,
      path = { {x=1020, y=1650}, {x=1040, y=1720}, {x=1020, y=1780} } },
    -- Mining station transit ring (bypass loop)
    { from = "junction_north",   to = "transit_mine_w",   nav = "path_spline", speed = 150, weight = 300,  path = {} },
    { from = "transit_mine_w",   to = "transit_mine_n",   nav = "path_spline", speed = 140, weight = 120,  path = {} },
    { from = "transit_mine_n",   to = "transit_mine_e",   nav = "path_spline", speed = 140, weight = 120,  path = {} },
    { from = "transit_mine_e",   to = "transit_mine_s",   nav = "path_spline", speed = 140, weight = 130,  path = {} },
    { from = "transit_mine_w",   to = "transit_mine_s",   nav = "path_spline", speed = 140, weight = 130,  path = {} },
    -- Spurs into operation points (single line in)
    { from = "transit_mine_w",   to = "mining_zone_a",    nav = "path_spline", speed = 100, weight = 100,  path = {} },
    { from = "transit_mine_e",   to = "inspection_scan",  nav = "path_spline", speed = 100, weight = 100,  path = {} },
    -- Ring exits to eastern zone
    { from = "transit_mine_s",   to = "mining_zone_b",    nav = "path_spline", speed = 130, weight = 700,  path = {} },
    { from = "junction_central", to = "mining_zone_b",    nav = "path_spline", speed = 130, weight = 894,  path = {} },
    { from = "charging_station", to = "mining_zone_b",    nav = "path_spline", speed = 130, weight = 800,  path = {} },
}

kb:add_info_node("boards", "landing_zone",
    { board_type = "outdoor", dimensions = "2400x2400", bidirectional = true },
    board_builder.build(nodes, edges),
    "Landing zone virtual node graph")

-- =====================================================================
-- robot_class.lunar_rover
-- =====================================================================
kb:add_header_node("robot_class", "lunar_rover", {}, {},
    "Lunar rover — wheeled platform with sensors and sample arm")

    -- Infrastructure (shared across all lunar_rover instances)
    kb:add_info_node("infra", "shared",
        { comm_type = "nats" },
        {
            energy_max = 10000,
            energy_rate = 0.5,
            energy_infinite = false,  -- true = plugged in, no energy budget
            virtual_nodes = {
                "init_check", "path_spline", "path_line", "operation", "idle",
            },
            operation_types = {
                "base", "deliver_part", "paint_sample", "load_shipping",
                "pass_gate", "inspection_scan", "recharge",
            },
            topics = {
                rpc        = "{site}.robots.{instance}.rpc",
                stream_bus = "{site}.robots.{instance}.stream_bus",
            },
            tick_rate_ms       = 100,
            heartbeat_interval = 10,
            controller_kb      = "controller",
        },
        "Lunar rover infrastructure configuration")

kb:leave_header_node("robot_class", "lunar_rover")

-- =====================================================================
-- robot_class.construction_arm
-- =====================================================================
kb:add_header_node("robot_class", "construction_arm", {}, {},
    "Construction arm — stationary manipulator at construction bay")

    kb:add_info_node("infra", "shared",
        { comm_type = "nats" },
        {
            energy_max = 5000,
            energy_rate = 0.3,
            energy_infinite = true,  -- stationary arm, plugged in
            virtual_nodes = {
                "init_check", "operation", "idle",
            },
            operation_types = {
                "base", "deliver_part", "load_shipping", "inspection_scan", "recharge",
            },
            topics = {
                rpc        = "{site}.robots.{instance}.rpc",
                stream_bus = "{site}.robots.{instance}.stream_bus",
            },
            tick_rate_ms       = 100,
            heartbeat_interval = 10,
            controller_kb      = "controller",
        },
        "Construction arm infrastructure configuration")

kb:leave_header_node("robot_class", "construction_arm")

-- =====================================================================
-- planner.route_planner
-- =====================================================================
kb:add_header_node("planner", "route_planner", {}, {},
    "Surface operations route planner")

    kb:add_status_field("planner_state", {},
        "Planner runtime state",
        {
            state           = "idle",
            active_robot    = "",
            active_mission  = "",
            actions_total   = 0,
            actions_complete = 0,
        })

kb:leave_header_node("planner", "route_planner")

-- =====================================================================
-- virtual_nodes: master definitions for all virtual node types
-- Single source of truth for schema, packet type, bitmask, pose fields.
-- Hub DSL plugins and robot capabilities reference these.
-- Flattened: virtual_nodes.{name} instead of VIRTUAL_NODES.definitions.VN_TYPE.{name}
-- =====================================================================
kb:add_header_node("virtual_nodes", "definitions", {}, {},
    "Master virtual node type definitions")

    kb:add_info_node("vn_type", "init_check",
        { packet_type_id = 1 },
        {
            description = "Preflight self-test",
            energy_cost = 50,
            json_schema = {},
            bitmask = {
                { name = "battery_ok",  bit = 0 },
                { name = "motors_ok",   bit = 1 },
                { name = "sensors_ok",  bit = 2 },
                { name = "comms_ok",    bit = 3 },
            },
            pose_fields = {},
        },
        "Init check virtual node")

    kb:add_info_node("vn_type", "path_spline",
        { packet_type_id = 2 },
        {
            description = "Follow spline path",
            energy_factor = 1.0,
            json_schema = {
                { name = "speed", type = "float", default = 100 },
                { name = "path",  type = "array", default = {} },
            },
            bitmask = {
                { name = "seg_complete", bit = 0 },
                { name = "obstacle",     bit = 1 },
                { name = "motor_fault",  bit = 2 },
            },
            pose_fields = { "delta_x", "delta_y", "delta_heading" },
        },
        "Path spline virtual node")

    kb:add_info_node("vn_type", "path_line",
        { packet_type_id = 3 },
        {
            description = "Follow line",
            energy_factor = 1.2,
            json_schema = {
                { name = "speed", type = "float", default = 100 },
                { name = "path",  type = "array", default = {} },
            },
            bitmask = {
                { name = "seg_complete", bit = 0 },
                { name = "obstacle",     bit = 1 },
                { name = "motor_fault",  bit = 2 },
            },
            pose_fields = { "delta_x", "delta_y", "delta_heading" },
        },
        "Path line virtual node")

    kb:add_info_node("vn_type", "operation",
        { packet_type_id = 20 },
        {
            description = "Generic operation at a stop (deliver, inspect, recharge, etc.)",
            energy_cost = 100,
            json_schema = {
                { name = "operation_type", type = "string", required = true },
                { name = "data",           type = "object", default = {} },
            },
            bitmask = {
                { name = "action_complete", bit = 0 },
                { name = "action_fault",    bit = 1 },
            },
            pose_fields = {},
        },
        "Generic operation virtual node")

    kb:add_info_node("vn_type", "idle",
        { packet_type_id = 11 },
        {
            description = "Park robot",
            energy_cost = 10,
            json_schema = {},
            bitmask = {
                { name = "parked", bit = 0 },
            },
            pose_fields = {},
        },
        "Idle virtual node")

kb:leave_header_node("virtual_nodes", "definitions")

-- =====================================================================
-- Bit masks (per-robot-class, registered at KB level)
-- =====================================================================

-- Rover bitmasks
kb:clear_bit_mask_flags()
kb:add_bit_mask_flag("battery_ok",  0, "Battery check passed")
kb:add_bit_mask_flag("motors_ok",   1, "Motor check passed")
kb:add_bit_mask_flag("sensors_ok",  2, "Sensor check passed")
kb:add_bit_mask_flag("comms_ok",    3, "Comms check passed")
kb:create_bit_mask_entry("system", "rover_init_check", 4, 0,
    "Rover init check bitmask")

kb:clear_bit_mask_flags()
kb:add_bit_mask_flag("seg_complete", 0, "Segment completed")
kb:add_bit_mask_flag("obstacle",     1, "Obstacle detected")
kb:add_bit_mask_flag("motor_fault",  2, "Motor fault")
kb:create_bit_mask_entry("system", "rover_path_spline", 3, 0,
    "Rover path spline bitmask")

kb:clear_bit_mask_flags()
kb:add_bit_mask_flag("arm_at_target",   0, "Arm reached target")
kb:add_bit_mask_flag("payload_gripped",  1, "Payload gripped")
kb:add_bit_mask_flag("action_complete",  2, "Action completed")
kb:add_bit_mask_flag("arm_fault",        3, "Arm fault")
kb:create_bit_mask_entry("system", "arm_operation", 4, 0,
    "Arm operation bitmask")

-- =====================================================================
-- Verify and sync
-- =====================================================================
kb:check_installation()

-- =====================================================================
-- Post-install: copy initial data from KB nodes to status table rows
-- (check_installation inserts empty {} — we need the actual initial data)
-- =====================================================================
local h = require("sqlite3_helpers")
local db_handle = kb.kb:get_db_objects()

local status_rows = h.sql_query(db_handle,
    string.format("SELECT path, data FROM %s WHERE label = 'status'", DATABASE), {})

for _, row in ipairs(status_rows) do
    if row.data and row.data ~= "" and row.data ~= "{}" then
        h.sql_exec(db_handle, string.format(
            "UPDATE %s_status SET data = '%s' WHERE path = '%s'",
            DATABASE, row.data:gsub("'", "''"), row.path))
    end
end

-- =====================================================================
-- System infrastructure (NATS, MQTT endpoints — from site_config.lua)
-- These are queried by get_infrastructure() and get_domain() at startup.
-- Container hostnames use Docker DNS names for planner-net.
-- =====================================================================
local h = require("sqlite3_helpers")
local json = h.json
local db_handle = kb.kb:get_db_objects()

-- Insert infrastructure service rows into "system" KB namespace
local infra_services = {
    { path = "system.site.main.cpu.cpu_01.container.nats_server.service.nats",
      name = "nats", label = "service",
      properties = { type = "infrastructure" },
      data = { host = "nats-js-ram", port = 4222, ws_port = 9222 } },
    { path = "system.site.main.cpu.cpu_01.container.mqtt_broker.service.mqtt",
      name = "mqtt", label = "service",
      properties = { type = "infrastructure" },
      data = { host = "mosquitto-ram-ws_main", port = 1883 } },
}

for _, svc in ipairs(infra_services) do
    h.sql_exec(db_handle, string.format(
        "INSERT INTO %s (knowledge_base, path, label, name, properties, data) VALUES ('%s', '%s', '%s', '%s', '%s', '%s')",
        DATABASE, "system",
        svc.path, svc.label, svc.name,
        json.encode(svc.properties):gsub("'", "''"),
        json.encode(svc.data):gsub("'", "''")))
end

-- Insert domain entry into "subsystems" KB namespace
h.sql_exec(db_handle, string.format(
    "INSERT INTO %s (knowledge_base, path, label, name, properties, data) VALUES ('%s', '%s', '%s', '%s', '%s', '%s')",
    DATABASE, "subsystems",
    "subsystems.domain.surface_ops", "domain", "surface_ops",
    json.encode({}):gsub("'", "''"),
    json.encode({ site = SITE, container = "surface_ops_planner" }):gsub("'", "''")))

print("\n=== Surface Ops KB Built ===")
print(string.format("Site: %s", SITE))
print(string.format("Database: %s", db_file))

kb:disconnect()
