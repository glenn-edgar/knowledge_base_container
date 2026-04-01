--[[
    construct_surface_ops.lua -- Knowledge Base DSL for Moon Base Surface Ops.

    Builds the SQLite KB for moonbase.alpha.surface_ops.
    Compatible with Postgres structure (same paths, same hierarchy).

    Hierarchy:
      moonbase.alpha.surface_ops
      ├── BOARD.landing_zone
      ├── ROBOT_CLASS.lunar_rover
      │   ├── ROBOT_INFRA.shared
      │   └── ROBOT_HW.rover_1
      ├── ROBOT_CLASS.construction_arm
      │   ├── ROBOT_INFRA.shared
      │   └── ROBOT_HW.arm_1
      ├── ROBOT_INSTANCE.rover_1
      ├── ROBOT_INSTANCE.rover_2
      ├── ROBOT_INSTANCE.arm_1
      ├── PLANNER.route_planner
      └── VIRTUAL_NODES.definitions
          └── VN_TYPE.{init_check, path_spline, ..., idle}

    Usage:
      luajit construct_surface_ops.lua <db_file>
      luajit construct_surface_ops.lua surface_ops.db
]]

local CDT = require("construct_data_tables")

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
-- BOARD: landing_zone — virtual node graph
-- =====================================================================
kb:add_info_node("BOARD", "landing_zone",
    { board_type = "outdoor", dimensions = "2400x2400", bidirectional = true },
    {
        nodes = {
            { name = "lander_pad",       x = 0,    y = 0,    type = "base" },
            { name = "habitat_site",     x = 800,  y = 0,    type = "waypoint" },
            { name = "charging_station", x = 800,  y = 800,  type = "waypoint" },
            { name = "mining_zone_a",    x = 1600, y = 0,    type = "mission" },
            { name = "mining_zone_b",    x = 1600, y = 800,  type = "mission" },
            { name = "survey_point_1",   x = 0,    y = 800,  type = "mission" },
            { name = "survey_point_2",   x = 0,    y = 1600, type = "mission" },
            { name = "construction_bay", x = 800,  y = 1600, type = "mission" },
        },
        edges = {
            { from = "lander_pad",       to = "habitat_site",     nav = "spline_follow", speed = 150, weight = 800 },
            { from = "lander_pad",       to = "survey_point_1",   nav = "spline_follow", speed = 120, weight = 800 },
            { from = "habitat_site",     to = "mining_zone_a",    nav = "spline_follow", speed = 150, weight = 800 },
            { from = "habitat_site",     to = "charging_station", nav = "spline_follow", speed = 130, weight = 800 },
            { from = "charging_station", to = "mining_zone_b",    nav = "spline_follow", speed = 130, weight = 800 },
            { from = "charging_station", to = "construction_bay", nav = "spline_follow", speed = 130, weight = 800 },
            { from = "survey_point_1",   to = "charging_station", nav = "spline_follow", speed = 120, weight = 1131 },
            { from = "survey_point_1",   to = "survey_point_2",   nav = "line_follow",   speed = 100, weight = 800 },
            { from = "survey_point_2",   to = "construction_bay", nav = "spline_follow", speed = 120, weight = 800 },
        },
    },
    "Landing zone virtual node graph")

-- =====================================================================
-- ROBOT_CLASS: lunar_rover
-- =====================================================================
kb:add_header_node("ROBOT_CLASS", "lunar_rover", {}, {},
    "Lunar rover — wheeled platform with sensors and sample arm")

    -- Infrastructure (shared across all lunar_rover instances)
    kb:add_info_node("ROBOT_INFRA", "shared",
        { comm_type = "nats" },
        {
            virtual_nodes = {
                "init_check", "path_spline", "path_line", "path_wall",
                "path_rotate", "deliver_part", "paint_sample", "load_shipping",
                "pass_gate", "inspection_scan", "idle",
            },
            topics = {
                rpc    = "{site}.{instance}.rpc",
                stream = "{site}.{instance}.stream",
            },
            tick_rate_ms       = 100,
            heartbeat_interval = 10,
            controller_kb      = "controller",
            worker_kbs = {
                "worker_init_check", "worker_path_spline", "worker_path_line",
                "worker_path_wall", "worker_path_rotate", "worker_deliver_part",
                "worker_paint_sample", "worker_load_shipping", "worker_pass_gate",
                "worker_inspection_scan", "worker_idle",
            },
        },
        "Lunar rover infrastructure configuration")

    -- Hardware config: rover_1
    kb:add_info_node("ROBOT_HW", "rover_1",
        {},
        {
            bitmask_defs = {
                init_check = {
                    { name = "battery_ok",  bit = 0 },
                    { name = "motors_ok",   bit = 1 },
                    { name = "sensors_ok",  bit = 2 },
                    { name = "comms_ok",    bit = 3 },
                },
                path_spline = {
                    { name = "seg_complete", bit = 0 },
                    { name = "obstacle",     bit = 1 },
                    { name = "motor_fault",  bit = 2 },
                },
                path_rotate = {
                    { name = "rotate_complete", bit = 0 },
                    { name = "motor_fault",     bit = 1 },
                },
                sensor_read = {
                    { name = "reading_ready", bit = 0 },
                    { name = "sensor_fault",  bit = 1 },
                },
            },
            port_map = {
                motors  = { left = "A", right = "B" },
                sensors = { front_distance = 1, ground_color = 2, imu = "internal" },
            },
            calibration = {
                wheel_diameter_mm = 56,
                track_width_mm    = 120,
                gear_ratio        = 3.0,
                imu_heading_offset = 0,
            },
            pose_dofs = { "x", "y", "heading" },
        },
        "Rover unit 1 hardware configuration")

    -- Hardware config: rover_2
    kb:add_info_node("ROBOT_HW", "rover_2",
        {},
        {
            bitmask_defs = {
                init_check = {
                    { name = "battery_ok",  bit = 0 },
                    { name = "motors_ok",   bit = 1 },
                    { name = "sensors_ok",  bit = 2 },
                    { name = "comms_ok",    bit = 3 },
                },
                path_spline = {
                    { name = "seg_complete", bit = 0 },
                    { name = "obstacle",     bit = 1 },
                    { name = "motor_fault",  bit = 2 },
                },
                path_rotate = {
                    { name = "rotate_complete", bit = 0 },
                    { name = "motor_fault",     bit = 1 },
                },
                sensor_read = {
                    { name = "reading_ready", bit = 0 },
                    { name = "sensor_fault",  bit = 1 },
                },
            },
            port_map = {
                motors  = { left = "A", right = "B" },
                sensors = { front_distance = 1, ground_color = 3, imu = "internal" },
            },
            calibration = {
                wheel_diameter_mm = 56,
                track_width_mm    = 120,
                gear_ratio        = 3.0,
                imu_heading_offset = 1.5,  -- different calibration per unit
            },
            pose_dofs = { "x", "y", "heading" },
        },
        "Rover unit 2 hardware configuration")

kb:leave_header_node("ROBOT_CLASS", "lunar_rover")

-- =====================================================================
-- ROBOT_CLASS: construction_arm
-- =====================================================================
kb:add_header_node("ROBOT_CLASS", "construction_arm", {}, {},
    "Construction arm — stationary manipulator at construction bay")

    kb:add_info_node("ROBOT_INFRA", "shared",
        { comm_type = "nats" },
        {
            virtual_nodes = {
                "init_check", "deliver_part", "load_shipping",
                "inspection_scan", "idle",
            },
            topics = {
                rpc    = "{site}.{instance}.rpc",
                stream = "{site}.{instance}.stream",
            },
            tick_rate_ms       = 100,
            heartbeat_interval = 10,
            controller_kb      = "controller",
            worker_kbs = {
                "worker_init_check", "worker_deliver_part", "worker_load_shipping",
                "worker_inspection_scan", "worker_idle",
            },
        },
        "Construction arm infrastructure configuration")

    kb:add_info_node("ROBOT_HW", "arm_1",
        {},
        {
            bitmask_defs = {
                init_check = {
                    { name = "battery_ok",  bit = 0 },
                    { name = "motors_ok",   bit = 1 },
                    { name = "sensors_ok",  bit = 2 },
                    { name = "comms_ok",    bit = 3 },
                },
                arm = {
                    { name = "arm_at_target",   bit = 0 },
                    { name = "payload_gripped",  bit = 1 },
                    { name = "action_complete",  bit = 2 },
                    { name = "arm_fault",        bit = 3 },
                },
            },
            port_map = {
                motors  = { arm = "C", gripper = "D" },
                sensors = { force = 1, alignment = 2 },
            },
            calibration = {
                arm_zero_angle = -5,
                arm_max_angle  = 180,
                gear_ratio     = 5.0,
                gripper_force_limit = 50,
            },
            pose_dofs = { "arm_angle", "gripper" },
        },
        "Construction arm unit 1 hardware configuration")

kb:leave_header_node("ROBOT_CLASS", "construction_arm")

-- =====================================================================
-- ROBOT_INSTANCE: rover_1
-- =====================================================================
kb:add_header_node("ROBOT_INSTANCE", "rover_1", {}, {},
    "Lunar rover unit 1")

    kb:add_link_mount("rover_1_class_link", "Link to rover class definition")
    -- Note: add_link_node would reference ROBOT_CLASS.lunar_rover.ROBOT_HW.rover_1

    kb:add_status_field("state", {},
        "Runtime state",
        {
            active_kb      = "",
            active_worker  = "",
            global_x       = 0,
            global_y       = 0,
            global_heading = 0,
            connected      = false,
            robot_id       = "rover_1",
        })

    kb:add_status_field("connection", {},
        "Connection info",
        {
            comm_type    = "nats",
            robot_id     = "rover_1",
            nats_server  = "nats://127.0.0.1:4222",
            rpc_topic    = "moonbase.alpha.surface_ops.rover_1.rpc",
            stream_topic = "moonbase.alpha.surface_ops.rover_1.stream",
        })

    kb:add_stream_field("telemetry", 100,
        "Heartbeat and telemetry stream")

kb:leave_header_node("ROBOT_INSTANCE", "rover_1")

-- =====================================================================
-- ROBOT_INSTANCE: rover_2
-- =====================================================================
kb:add_header_node("ROBOT_INSTANCE", "rover_2", {}, {},
    "Lunar rover unit 2")

    kb:add_link_mount("rover_2_class_link", "Link to rover class definition")

    kb:add_status_field("state", {},
        "Runtime state",
        {
            active_kb      = "",
            active_worker  = "",
            global_x       = 0,
            global_y       = 0,
            global_heading = 0,
            connected      = false,
            robot_id       = "rover_2",
        })

    kb:add_status_field("connection", {},
        "Connection info",
        {
            comm_type    = "nats",
            robot_id     = "rover_2",
            nats_server  = "nats://127.0.0.1:4222",
            rpc_topic    = "moonbase.alpha.surface_ops.rover_2.rpc",
            stream_topic = "moonbase.alpha.surface_ops.rover_2.stream",
        })

    kb:add_stream_field("telemetry", 100,
        "Heartbeat and telemetry stream")

kb:leave_header_node("ROBOT_INSTANCE", "rover_2")

-- =====================================================================
-- ROBOT_INSTANCE: arm_1
-- =====================================================================
kb:add_header_node("ROBOT_INSTANCE", "arm_1", {}, {},
    "Construction arm unit 1")

    kb:add_link_mount("arm_1_class_link", "Link to arm class definition")

    kb:add_status_field("state", {},
        "Runtime state",
        {
            active_kb      = "",
            active_worker  = "",
            global_arm_angle = 0,
            connected      = false,
            robot_id       = "arm_1",
        })

    kb:add_status_field("connection", {},
        "Connection info",
        {
            comm_type    = "nats",
            robot_id     = "arm_1",
            nats_server  = "nats://127.0.0.1:4222",
            rpc_topic    = "moonbase.alpha.surface_ops.arm_1.rpc",
            stream_topic = "moonbase.alpha.surface_ops.arm_1.stream",
        })

    kb:add_stream_field("telemetry", 50,
        "Heartbeat and telemetry stream")

kb:leave_header_node("ROBOT_INSTANCE", "arm_1")

-- =====================================================================
-- PLANNER: route_planner
-- =====================================================================
kb:add_header_node("PLANNER", "route_planner", {}, {},
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

kb:leave_header_node("PLANNER", "route_planner")

-- =====================================================================
-- VIRTUAL_NODES: master definitions for all virtual node types
-- Single source of truth for schema, packet type, bitmask, pose fields.
-- Hub DSL plugins and robot capabilities reference these.
-- =====================================================================
kb:add_header_node("VIRTUAL_NODES", "definitions", {}, {},
    "Master virtual node type definitions")

    kb:add_info_node("VN_TYPE", "init_check",
        { packet_type_id = 1 },
        {
            description = "Preflight self-test",
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

    kb:add_info_node("VN_TYPE", "path_spline",
        { packet_type_id = 2 },
        {
            description = "Follow spline path",
            json_schema = {
                { name = "from_x",          type = "float", default = 0 },
                { name = "from_y",          type = "float", default = 0 },
                { name = "to_x",            type = "float", default = 0 },
                { name = "to_y",            type = "float", default = 0 },
                { name = "speed",           type = "float", default = 100 },
                { name = "distance",        type = "float", default = 0 },
                { name = "segment_index",   type = "uint32", default = 0 },
                { name = "total_segments",  type = "uint32", default = 1 },
            },
            bitmask = {
                { name = "seg_complete", bit = 0 },
                { name = "obstacle",     bit = 1 },
                { name = "motor_fault",  bit = 2 },
            },
            pose_fields = { "delta_x", "delta_y", "delta_heading" },
        },
        "Path spline virtual node")

    kb:add_info_node("VN_TYPE", "path_line",
        { packet_type_id = 3 },
        {
            description = "Follow line",
            json_schema = {
                { name = "from_x",   type = "float", default = 0 },
                { name = "from_y",   type = "float", default = 0 },
                { name = "to_x",     type = "float", default = 0 },
                { name = "to_y",     type = "float", default = 0 },
                { name = "speed",    type = "float", default = 100 },
                { name = "distance", type = "float", default = 0 },
            },
            bitmask = {
                { name = "seg_complete", bit = 0 },
                { name = "obstacle",     bit = 1 },
                { name = "motor_fault",  bit = 2 },
            },
            pose_fields = { "delta_x", "delta_y", "delta_heading" },
        },
        "Path line virtual node")

    kb:add_info_node("VN_TYPE", "path_wall",
        { packet_type_id = 4 },
        {
            description = "Wall follow",
            json_schema = {
                { name = "from_x",        type = "float", default = 0 },
                { name = "from_y",        type = "float", default = 0 },
                { name = "to_x",          type = "float", default = 0 },
                { name = "to_y",          type = "float", default = 0 },
                { name = "speed",         type = "float", default = 100 },
                { name = "distance",      type = "float", default = 0 },
                { name = "wall_standoff", type = "float", default = 50 },
            },
            bitmask = {
                { name = "seg_complete", bit = 0 },
                { name = "obstacle",     bit = 1 },
                { name = "motor_fault",  bit = 2 },
                { name = "wall_lost",    bit = 3 },
            },
            pose_fields = { "delta_x", "delta_y", "delta_heading" },
        },
        "Path wall virtual node")

    kb:add_info_node("VN_TYPE", "path_rotate",
        { packet_type_id = 5 },
        {
            description = "Rotate in place",
            json_schema = {
                { name = "from_heading", type = "float", required = true },
                { name = "to_heading",   type = "float", required = true },
            },
            bitmask = {
                { name = "rotate_complete", bit = 0 },
                { name = "motor_fault",     bit = 1 },
            },
            pose_fields = { "delta_heading" },
        },
        "Path rotate virtual node")

    kb:add_info_node("VN_TYPE", "deliver_part",
        { packet_type_id = 6 },
        {
            description = "Arm delivery at assembly station",
            json_schema = {
                { name = "arm_target",   type = "float", required = true },
                { name = "arm_speed",    type = "float", default = 80 },
                { name = "arm_return",   type = "float", default = 0 },
                { name = "payload_type", type = "uint8", default = 0, enum = { none = 0, part = 1, container = 2 } },
            },
            bitmask = {
                { name = "arm_at_target",   bit = 0 },
                { name = "payload_gripped", bit = 1 },
                { name = "action_complete", bit = 2 },
                { name = "arm_fault",       bit = 3 },
            },
            pose_fields = { "delta_arm_angle" },
        },
        "Deliver part virtual node")

    kb:add_info_node("VN_TYPE", "paint_sample",
        { packet_type_id = 7 },
        {
            description = "Paint operation",
            json_schema = {
                { name = "arm_target", type = "float", required = true },
                { name = "arm_speed",  type = "float", default = 60 },
                { name = "arm_return", type = "float", default = 0 },
                { name = "hold_time",  type = "float", default = 500 },
            },
            bitmask = {
                { name = "arm_at_target",   bit = 0 },
                { name = "action_complete", bit = 1 },
                { name = "arm_fault",       bit = 2 },
            },
            pose_fields = { "delta_arm_angle" },
        },
        "Paint sample virtual node")

    kb:add_info_node("VN_TYPE", "load_shipping",
        { packet_type_id = 8 },
        {
            description = "Load container at shipping station",
            json_schema = {
                { name = "arm_target",   type = "float", required = true },
                { name = "arm_speed",    type = "float", default = 80 },
                { name = "arm_return",   type = "float", default = 0 },
                { name = "payload_type", type = "uint8", default = 0, enum = { none = 0, part = 1, container = 2 } },
            },
            bitmask = {
                { name = "arm_at_target",   bit = 0 },
                { name = "payload_gripped", bit = 1 },
                { name = "action_complete", bit = 2 },
                { name = "arm_fault",       bit = 3 },
            },
            pose_fields = { "delta_arm_angle" },
        },
        "Load shipping virtual node")

    kb:add_info_node("VN_TYPE", "pass_gate",
        { packet_type_id = 9 },
        {
            description = "Open gate, drive through, close gate",
            json_schema = {
                { name = "rpc_open_hash",  type = "uint32", default = 0 },
                { name = "rpc_close_hash", type = "uint32", default = 0 },
                { name = "drive_through",  type = "float",  default = 200 },
            },
            bitmask = {
                { name = "gate_opened",     bit = 0 },
                { name = "drive_complete",  bit = 1 },
                { name = "gate_closed",     bit = 2 },
                { name = "action_complete", bit = 3 },
            },
            pose_fields = { "delta_x", "delta_y", "delta_heading" },
        },
        "Pass gate virtual node")

    kb:add_info_node("VN_TYPE", "inspection_scan",
        { packet_type_id = 10 },
        {
            description = "Sensor read at inspection point",
            json_schema = {
                { name = "sensor_port", type = "uint8", default = 0 },
                { name = "sensor_type", type = "uint8", default = 0, enum = { color = 0, distance = 1, force = 2 } },
            },
            bitmask = {
                { name = "reading_ready", bit = 0 },
                { name = "sensor_fault",  bit = 1 },
            },
            pose_fields = {},
        },
        "Inspection scan virtual node")

    kb:add_info_node("VN_TYPE", "idle",
        { packet_type_id = 11 },
        {
            description = "Park robot",
            json_schema = {},
            bitmask = {
                { name = "parked", bit = 0 },
            },
            pose_fields = {},
        },
        "Idle virtual node")

kb:leave_header_node("VIRTUAL_NODES", "definitions")

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
    string.format("SELECT path, data FROM %s WHERE label = 'KB_STATUS_FIELD'", DATABASE), {})

for _, row in ipairs(status_rows) do
    if row.data and row.data ~= "" and row.data ~= "{}" then
        h.sql_exec(db_handle, string.format(
            "UPDATE %s_status SET data = '%s' WHERE path = '%s'",
            DATABASE, row.data:gsub("'", "''"), row.path))
    end
end

print("\n=== Surface Ops KB Built ===")
print(string.format("Site: %s", SITE))
print(string.format("Database: %s", db_file))

kb:disconnect()
