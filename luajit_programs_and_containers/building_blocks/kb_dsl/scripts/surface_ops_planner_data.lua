--[[
  surface_ops_planner_data.lua — Planner data for Moon Base Surface Ops.

  Called by planner_tree.lua. Writes boards, robot classes, robots,
  virtual nodes, and bitmasks into the Postgres KB under the domain's
  site namespace (moonbase.alpha.surface_ops).

  This is the single source of truth for all planner data.
  The same data flows: Postgres → SQLite extract → container → planner.
]]

--- @param kb      Construct_KB  Postgres KB handle (already select_kb'd)
--- @param site    string        Site namespace (e.g., "moonbase.alpha.surface_ops")
--- @param domain  table         Domain config from site_config.lua
return function(kb, site, domain)

-- =====================================================================
-- boards.landing_zone — virtual node graph
-- =====================================================================
kb:add_info_node("boards", "landing_zone",
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
-- robot_class.lunar_rover
-- =====================================================================
kb:add_header_node("robot_class", "lunar_rover", {}, {},
    "Lunar rover — wheeled platform with sensors and sample arm")

    kb:add_info_node("infra", "shared",
        { comm_type = "nats" },
        {
            energy_max = 10000,
            energy_infinite = false,
            virtual_nodes = {
                "init_check", "path_spline", "path_line", "path_wall",
                "path_rotate", "deliver_part", "paint_sample", "load_shipping",
                "pass_gate", "inspection_scan", "recharge", "idle",
            },
            topics = {
                rpc        = "{site}.robots.{instance}.rpc",
                stream_bus = "{site}.robots.{instance}.stream_bus",
            },
            tick_rate_ms       = 100,
            heartbeat_interval = 10,
            worker_kbs = {
                "worker_init_check", "worker_path_spline", "worker_path_line",
                "worker_path_wall", "worker_path_rotate", "worker_deliver_part",
                "worker_paint_sample", "worker_load_shipping", "worker_pass_gate",
                "worker_inspection_scan", "worker_recharge", "worker_idle",
            },
        },
        "Lunar rover infrastructure configuration")

    kb:add_info_node("hw", "rover_1",
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

    kb:add_info_node("hw", "rover_2",
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
                imu_heading_offset = 1.5,
            },
            pose_dofs = { "x", "y", "heading" },
        },
        "Rover unit 2 hardware configuration")

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
            energy_infinite = true,
            virtual_nodes = {
                "init_check", "deliver_part", "load_shipping",
                "inspection_scan", "recharge", "idle",
            },
            topics = {
                rpc        = "{site}.robots.{instance}.rpc",
                stream_bus = "{site}.robots.{instance}.stream_bus",
            },
            tick_rate_ms       = 100,
            heartbeat_interval = 10,
            worker_kbs = {
                "worker_init_check", "worker_deliver_part", "worker_load_shipping",
                "worker_inspection_scan", "worker_recharge", "worker_idle",
            },
        },
        "Construction arm infrastructure configuration")

    kb:add_info_node("hw", "arm_1",
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

kb:leave_header_node("robot_class", "construction_arm")

-- =====================================================================
-- robots (per-instance status, connection, energy, telemetry)
-- =====================================================================
local robot_instances = {
    { name = "rover_1", class = "lunar_rover", energy_max = 10000,
      pose_dofs = { "x", "y", "heading" } },
    { name = "rover_2", class = "lunar_rover", energy_max = 10000,
      pose_dofs = { "x", "y", "heading" } },
    { name = "arm_1",   class = "construction_arm", energy_max = 5000,
      pose_dofs = { "arm_angle" } },
}

for _, robot in ipairs(robot_instances) do
    kb:add_header_node("robots", robot.name, {}, {},
        robot.class .. " unit " .. robot.name)

        kb:add_status_field("state", {},
            "Runtime state",
            {
                active_kb      = "",
                active_worker  = "",
                connected      = false,
                robot_id       = robot.name,
            })

        kb:add_status_field("connection", {},
            "Connection info",
            {
                comm_type        = "nats",
                robot_id         = robot.name,
                nats_server      = "nats://127.0.0.1:4222",
                rpc_topic        = site .. ".robots." .. robot.name .. ".rpc",
                stream_bus_topic = site .. ".robots." .. robot.name .. ".stream_bus",
            })

        kb:add_status_field("energy", {},
            "Energy budget",
            {
                energy_max       = robot.energy_max,
                energy_remaining = robot.energy_max,
            })

        kb:add_stream_field("telemetry", 100,
            "Heartbeat and telemetry stream")

    kb:leave_header_node("robots", robot.name)
end

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
-- =====================================================================
kb:add_header_node("virtual_nodes", "definitions", {}, {},
    "Master virtual node type definitions")

    kb:add_info_node("vn_type", "init_check",
        { packet_type_id = 1 },
        { description = "Preflight self-test", json_schema = {},
          bitmask = { { name = "battery_ok", bit = 0 }, { name = "motors_ok", bit = 1 },
                      { name = "sensors_ok", bit = 2 }, { name = "comms_ok", bit = 3 } },
          pose_fields = {} },
        "Init check virtual node")

    kb:add_info_node("vn_type", "path_spline",
        { packet_type_id = 2 },
        { description = "Follow spline path",
          json_schema = {
              { name = "from_x", type = "float", default = 0 }, { name = "from_y", type = "float", default = 0 },
              { name = "to_x", type = "float", default = 0 }, { name = "to_y", type = "float", default = 0 },
              { name = "speed", type = "float", default = 100 }, { name = "distance", type = "float", default = 0 },
              { name = "segment_index", type = "uint32", default = 0 }, { name = "total_segments", type = "uint32", default = 1 },
          },
          bitmask = { { name = "seg_complete", bit = 0 }, { name = "obstacle", bit = 1 }, { name = "motor_fault", bit = 2 } },
          pose_fields = { "delta_x", "delta_y", "delta_heading" } },
        "Path spline virtual node")

    kb:add_info_node("vn_type", "path_line",
        { packet_type_id = 3 },
        { description = "Follow line",
          json_schema = {
              { name = "from_x", type = "float", default = 0 }, { name = "from_y", type = "float", default = 0 },
              { name = "to_x", type = "float", default = 0 }, { name = "to_y", type = "float", default = 0 },
              { name = "speed", type = "float", default = 100 }, { name = "distance", type = "float", default = 0 },
          },
          bitmask = { { name = "seg_complete", bit = 0 }, { name = "obstacle", bit = 1 }, { name = "motor_fault", bit = 2 } },
          pose_fields = { "delta_x", "delta_y", "delta_heading" } },
        "Path line virtual node")

    kb:add_info_node("vn_type", "path_wall",
        { packet_type_id = 4 },
        { description = "Wall follow",
          json_schema = {
              { name = "from_x", type = "float", default = 0 }, { name = "from_y", type = "float", default = 0 },
              { name = "to_x", type = "float", default = 0 }, { name = "to_y", type = "float", default = 0 },
              { name = "speed", type = "float", default = 100 }, { name = "distance", type = "float", default = 0 },
              { name = "wall_standoff", type = "float", default = 50 },
          },
          bitmask = { { name = "seg_complete", bit = 0 }, { name = "obstacle", bit = 1 },
                      { name = "motor_fault", bit = 2 }, { name = "wall_lost", bit = 3 } },
          pose_fields = { "delta_x", "delta_y", "delta_heading" } },
        "Path wall virtual node")

    kb:add_info_node("vn_type", "path_rotate",
        { packet_type_id = 5 },
        { description = "Rotate in place",
          json_schema = {
              { name = "from_heading", type = "float", required = true },
              { name = "to_heading", type = "float", required = true },
          },
          bitmask = { { name = "rotate_complete", bit = 0 }, { name = "motor_fault", bit = 1 } },
          pose_fields = { "delta_heading" } },
        "Path rotate virtual node")

    kb:add_info_node("vn_type", "deliver_part",
        { packet_type_id = 6 },
        { description = "Arm delivery at assembly station",
          json_schema = {
              { name = "arm_target", type = "float", required = true },
              { name = "arm_speed", type = "float", default = 80 },
              { name = "arm_return", type = "float", default = 0 },
              { name = "payload_type", type = "uint8", default = 0, enum = { none = 0, part = 1, container = 2 } },
          },
          bitmask = { { name = "arm_at_target", bit = 0 }, { name = "payload_gripped", bit = 1 },
                      { name = "action_complete", bit = 2 }, { name = "arm_fault", bit = 3 } },
          pose_fields = { "delta_arm_angle" } },
        "Deliver part virtual node")

    kb:add_info_node("vn_type", "paint_sample",
        { packet_type_id = 7 },
        { description = "Paint operation",
          json_schema = {
              { name = "arm_target", type = "float", required = true },
              { name = "arm_speed", type = "float", default = 60 },
              { name = "arm_return", type = "float", default = 0 },
              { name = "hold_time", type = "float", default = 500 },
          },
          bitmask = { { name = "arm_at_target", bit = 0 }, { name = "action_complete", bit = 1 },
                      { name = "arm_fault", bit = 2 } },
          pose_fields = { "delta_arm_angle" } },
        "Paint sample virtual node")

    kb:add_info_node("vn_type", "load_shipping",
        { packet_type_id = 8 },
        { description = "Load container at shipping station",
          json_schema = {
              { name = "arm_target", type = "float", required = true },
              { name = "arm_speed", type = "float", default = 80 },
              { name = "arm_return", type = "float", default = 0 },
              { name = "payload_type", type = "uint8", default = 0, enum = { none = 0, part = 1, container = 2 } },
          },
          bitmask = { { name = "arm_at_target", bit = 0 }, { name = "payload_gripped", bit = 1 },
                      { name = "action_complete", bit = 2 }, { name = "arm_fault", bit = 3 } },
          pose_fields = { "delta_arm_angle" } },
        "Load shipping virtual node")

    kb:add_info_node("vn_type", "pass_gate",
        { packet_type_id = 9 },
        { description = "Open gate, drive through, close gate",
          json_schema = {
              { name = "rpc_open_hash", type = "uint32", default = 0 },
              { name = "rpc_close_hash", type = "uint32", default = 0 },
              { name = "drive_through", type = "float", default = 200 },
          },
          bitmask = { { name = "gate_opened", bit = 0 }, { name = "drive_complete", bit = 1 },
                      { name = "gate_closed", bit = 2 }, { name = "action_complete", bit = 3 } },
          pose_fields = { "delta_x", "delta_y", "delta_heading" } },
        "Pass gate virtual node")

    kb:add_info_node("vn_type", "inspection_scan",
        { packet_type_id = 10 },
        { description = "Sensor read at inspection point",
          json_schema = {
              { name = "sensor_port", type = "uint8", default = 0 },
              { name = "sensor_type", type = "uint8", default = 0, enum = { color = 0, distance = 1, force = 2 } },
          },
          bitmask = { { name = "reading_ready", bit = 0 }, { name = "sensor_fault", bit = 1 } },
          pose_fields = {} },
        "Inspection scan virtual node")

    kb:add_info_node("vn_type", "recharge",
        { packet_type_id = 12 },
        { description = "Recharge energy at charging station",
          json_schema = { { name = "target_energy", type = "float", default = 0 } },
          bitmask = { { name = "charging", bit = 0 }, { name = "charge_complete", bit = 1 },
                      { name = "charger_fault", bit = 2 } },
          pose_fields = {} },
        "Recharge virtual node")

    kb:add_info_node("vn_type", "idle",
        { packet_type_id = 11 },
        { description = "Park robot", json_schema = {},
          bitmask = { { name = "parked", bit = 0 } },
          pose_fields = {} },
        "Idle virtual node")

kb:leave_header_node("virtual_nodes", "definitions")

end
