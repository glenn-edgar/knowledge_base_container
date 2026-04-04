--[[
    json_packet_test.lua — ChainTree DSL test for JSON packet streaming

    Test 1: Single telemetry packet type
      - Generator column emits telemetry packets on a timer (0.2s)
      - Sink column receives and prints them

    Test 2: Three generators with different delays
      - 3 generators emit different sensor packets (device 1,2,3)
      - Each fires every 1.0s
      - Single sink receives all packets

    User boolean functions (in user_json_functions.c):
      JSON_TELEM_SINK     — extracts x/y/z from telemetry
]]

local ChainTreeMaster = require("chain_tree_master")

-- =========================================================================
-- Helper: packet generator column (emits one telemetry packet per cycle)
-- =========================================================================

local function insert_packet_generator(ct, event_column, event_name)
    local gen_col = ct:define_column("packet_generator", nil, nil, nil, nil, nil, true)
    ct:asm_wait_time(0.2)
    ct:asm_log_message("emitting telemetry packet")
    ct:asm_json_emit_oneshot(
        { type = "telemetry", seq = 1, topic = "sensors/accel",
          payload = { x = 1.5, y = -0.3, z = 9.81 } },
        event_column, event_name)
    ct:asm_reset()
    ct:end_column(gen_col)
    return gen_col
end

-- =========================================================================
-- Helper: delayed packet generator (emits with configurable delay)
-- =========================================================================

local function insert_packet_generator_delayed(ct, event_column, event_name,
                                                device_id, delay, packet_data)
    local column_name = "packet_gen_" .. tostring(device_id)
    local gen_col = ct:define_column(column_name, nil, nil, nil, nil, nil, true)
    ct:asm_wait_time(delay)
    ct:asm_log_message("emitter " .. tostring(device_id) .. ": sending packet")
    ct:asm_json_emit_oneshot(packet_data, event_column, event_name)
    ct:asm_reset()
    ct:end_column(gen_col)
    return gen_col
end

-- =========================================================================
-- Helper: packet sink column (receives and prints packets)
-- =========================================================================

local function insert_packet_sink(ct, event_name)
    local sink_col = ct:define_column("packet_sink", nil, nil, nil, nil, nil, true)
    ct:asm_log_message("sink ready")
    ct:asm_json_sink("JSON_TELEM_SINK",
        { log_prefix = "telemetry" },
        event_name)
    ct:asm_halt()
    ct:end_column(sink_col)
    return sink_col
end

-- =========================================================================
-- Test 1: single telemetry packet — emit → sink
-- =========================================================================

local function json_telemetry_test(ct, kb_name)
    ct:start_test(kb_name, 50)

    local launch = ct:define_column("launch", nil, nil, nil, nil, nil, true)
    ct:asm_log_message("launch column")

    insert_packet_generator(ct, launch, "JSON_PACKET_EVENT")

    insert_packet_sink(ct, "JSON_PACKET_EVENT")

    ct:asm_wait_time(5)
    ct:asm_log_message("test complete — terminating")
    ct:asm_terminate()
    ct:end_column(launch)

    ct:end_test()
end

-- =========================================================================
-- Test 2: three generators → single sink
-- =========================================================================

local function json_multi_generator_test(ct, kb_name)
    ct:start_test(kb_name, 50)

    local launch = ct:define_column("launch", nil, nil, nil, nil, nil, true)
    ct:asm_log_message("launch column: multi-generator test starting")

    -- 3 generators, each with 1.0s delay, different sensor data
    insert_packet_generator_delayed(ct, launch, "JSON_PACKET_EVENT", 1, 1.0,
        { type = "telemetry", seq = 0, topic = "sensors/accel_1",
          payload = { x = 1.0, y = 0.0, z = 9.81 } })

    insert_packet_generator_delayed(ct, launch, "JSON_PACKET_EVENT", 2, 1.0,
        { type = "telemetry", seq = 0, topic = "sensors/accel_2",
          payload = { x = 2.0, y = 0.1, z = 9.82 } })

    insert_packet_generator_delayed(ct, launch, "JSON_PACKET_EVENT", 3, 1.0,
        { type = "telemetry", seq = 0, topic = "sensors/accel_3",
          payload = { x = 3.0, y = 0.2, z = 9.83 } })

    ct:asm_log_message("packet generators created")

    -- Single sink receives from all 3 generators
    insert_packet_sink(ct, "JSON_PACKET_EVENT")

    ct:asm_wait_time(10)
    ct:asm_log_message("multi-generator test complete — terminating")
    ct:asm_terminate()
    ct:end_column(launch)

    ct:end_test()
end

-- =========================================================================
-- Helper: verify generator (cycles x through values, some pass some fail)
-- =========================================================================

local function insert_verify_generator(ct, event_column, event_name, device_id, delay, x_value)
    local column_name = "verify_gen_" .. tostring(device_id)
    local gen_col = ct:define_column(column_name, nil, nil, nil, nil, nil, true)
    ct:asm_wait_time(delay)
    ct:asm_log_message("verify emitter " .. tostring(device_id) .. ": x=" .. tostring(x_value))
    ct:asm_json_emit_oneshot(
        { type = "telemetry", seq = device_id, topic = "sensors/accel",
          payload = { x = x_value, y = 0.0, z = 9.81 } },
        event_column, event_name)
    ct:asm_reset()
    ct:end_column(gen_col)
    return gen_col
end

-- =========================================================================
-- Helper: verified sink column
-- =========================================================================

local function insert_verified_sink(ct, event_name)
    local sink_col = ct:define_column("verified_sink", nil, nil, nil, nil, nil, true)
    ct:asm_log_message("verified sink: ready")
    ct:asm_json_sink("JSON_VERIFIED_SINK",
        { log_prefix = "verified" },
        event_name)
    ct:asm_halt()
    ct:end_column(sink_col)
    return sink_col
end

-- =========================================================================
-- Test 3: verify packet — generators with mixed x values → verify → sink
--
-- 3 generators emit packets with different x values every 0.5s:
--   device 1: x=0.3 (PASS — in [0.0, 0.5])
--   device 2: x=0.8 (REJECT — > 0.5)
--   device 3: x=0.1 (PASS — in [0.0, 0.5])
--
-- Verify sink checks x range and prints PASS/REJECT.
-- Verified sink prints all packets that reach it.
-- =========================================================================

local function json_verify_test(ct, kb_name)
    ct:start_test(kb_name, 50)

    local launch = ct:define_column("launch", nil, nil, nil, nil, nil, true)
    ct:asm_log_message("launch column: verify packet test starting")

    -- 3 generators with different x values (some pass, some fail)
    insert_verify_generator(ct, launch, "JSON_SENSOR_EVENT", 1, 0.5, 0.3)
    insert_verify_generator(ct, launch, "JSON_SENSOR_EVENT", 2, 0.5, 0.8)
    insert_verify_generator(ct, launch, "JSON_SENSOR_EVENT", 3, 0.5, 0.1)
    ct:asm_log_message("verify generators created")

    -- Verify sink — checks x range, prints PASS/REJECT
    ct:asm_json_sink("JSON_VERIFY_X_RANGE",
        { min_x = 0.0, max_x = 0.5 },
        "JSON_SENSOR_EVENT")

    -- Verified sink — prints all received packets
    insert_verified_sink(ct, "JSON_SENSOR_EVENT")

    ct:asm_wait_time(10)
    ct:asm_log_message("verify test complete — terminating")
    ct:asm_terminate()
    ct:end_column(launch)

    ct:end_test()
end

-- =========================================================================
-- JsonDroneControl — DSL extension for JSON-based drone flight control
--
-- Same pattern as DroneControl but uses JSON controlled nodes.
-- No Avro schemas — event_id routing only.
-- =========================================================================

local JsonDroneControl = {}
JsonDroneControl.__index = JsonDroneControl

function JsonDroneControl.new(ct)
    local self = setmetatable({}, JsonDroneControl)
    self.ct = ct

    self.command_container = {}

    self.command_container["fly_straight"] = {
        request_port  = ct:make_json_control_port("json_fly_straight_request"),
        response_port = ct:make_json_control_port("json_fly_straight_response"),
        api_name      = "json_drone_fly_straight",
    }
    self.command_container["fly_arc"] = {
        request_port  = ct:make_json_control_port("json_fly_arc_request"),
        response_port = ct:make_json_control_port("json_fly_arc_response"),
        api_name      = "json_drone_fly_arc",
    }
    self.command_container["fly_up"] = {
        request_port  = ct:make_json_control_port("json_fly_up_request"),
        response_port = ct:make_json_control_port("json_fly_up_response"),
        api_name      = "json_drone_fly_up",
    }
    self.command_container["fly_down"] = {
        request_port  = ct:make_json_control_port("json_fly_down_request"),
        response_port = ct:make_json_control_port("json_fly_down_response"),
        api_name      = "json_drone_fly_down",
    }

    return self
end

-- ---- Servers (controlled nodes) ----

function JsonDroneControl:fly_straight_server(column_name, monitor_fn, monitor_data)
    monitor_data = monitor_data or {}
    local c = self.command_container["fly_straight"]
    return self.ct:json_controlled_node(c.api_name, column_name, monitor_fn, monitor_data,
                                         c.request_port, c.response_port)
end

function JsonDroneControl:fly_arc_server(column_name, monitor_fn, monitor_data)
    monitor_data = monitor_data or {}
    local c = self.command_container["fly_arc"]
    return self.ct:json_controlled_node(c.api_name, column_name, monitor_fn, monitor_data,
                                         c.request_port, c.response_port)
end

function JsonDroneControl:fly_up_server(column_name, monitor_fn, monitor_data)
    monitor_data = monitor_data or {}
    local c = self.command_container["fly_up"]
    return self.ct:json_controlled_node(c.api_name, column_name, monitor_fn, monitor_data,
                                         c.request_port, c.response_port)
end

function JsonDroneControl:fly_down_server(column_name, monitor_fn, monitor_data)
    monitor_data = monitor_data or {}
    local c = self.command_container["fly_down"]
    return self.ct:json_controlled_node(c.api_name, column_name, monitor_fn, monitor_data,
                                         c.request_port, c.response_port)
end

-- ---- Clients (initiator nodes) ----

function JsonDroneControl:fly_straight_client(distance, final_altitude, final_speed, heading,
                                               finalize_fn, finalize_data)
    finalize_data = finalize_data or {}
    local c = self.command_container["fly_straight"]
    local request_data = {
        command        = "fly_straight",
        distance       = distance,
        final_altitude = final_altitude,
        final_speed    = final_speed,
        heading        = heading,
    }
    return self.ct:json_client_controlled_node(c.api_name, finalize_fn, request_data,
                                                c.request_port, c.response_port)
end

function JsonDroneControl:fly_arc_client(distance, final_altitude, final_speed, heading,
                                          finalize_fn, finalize_data)
    finalize_data = finalize_data or {}
    local c = self.command_container["fly_arc"]
    local request_data = {
        command        = "fly_arc",
        distance       = distance,
        final_altitude = final_altitude,
        final_speed    = final_speed,
        heading        = heading,
    }
    return self.ct:json_client_controlled_node(c.api_name, finalize_fn, request_data,
                                                c.request_port, c.response_port)
end

function JsonDroneControl:fly_up_client(final_altitude, final_speed, finalize_fn, finalize_data)
    finalize_data = finalize_data or {}
    local c = self.command_container["fly_up"]
    local request_data = {
        command        = "fly_up",
        final_altitude = final_altitude,
        final_speed    = final_speed,
    }
    return self.ct:json_client_controlled_node(c.api_name, finalize_fn, request_data,
                                                c.request_port, c.response_port)
end

function JsonDroneControl:fly_down_client(final_altitude, final_speed, finalize_fn, finalize_data)
    finalize_data = finalize_data or {}
    local c = self.command_container["fly_down"]
    local request_data = {
        command        = "fly_down",
        final_altitude = final_altitude,
        final_speed    = final_speed,
    }
    return self.ct:json_client_controlled_node(c.api_name, finalize_fn, request_data,
                                                c.request_port, c.response_port)
end

-- =========================================================================
-- Shared column helpers for JSON drone tests
-- =========================================================================

local function insert_json_fly_straight_column(ct)
    local col = ct.json_drone:fly_straight_server("json_fly_straight", "JSON_FLY_STRAIGHT_MONITOR", {})
    ct:asm_log_message("json fly straight: ready")
    ct:asm_wait_time(2)
    ct:asm_one_shot_handler("JSON_FLY_STRAIGHT_FINAL", {})
    ct:asm_log_message("json fly straight: terminating")
    ct:asm_terminate()
    ct:end_column(col)
    return col
end

local function insert_json_fly_arc_column(ct)
    local col = ct.json_drone:fly_arc_server("json_fly_arc", "JSON_FLY_ARC_MONITOR", {})
    ct:asm_log_message("json fly arc: ready")
    ct:asm_wait_time(2)
    ct:asm_one_shot_handler("JSON_FLY_ARC_FINAL", {})
    ct:asm_log_message("json fly arc: terminating")
    ct:asm_terminate()
    ct:end_column(col)
    return col
end

local function insert_json_fly_up_column(ct)
    local col = ct.json_drone:fly_up_server("json_fly_up", "JSON_FLY_UP_MONITOR", {})
    ct:asm_log_message("json fly up: ready")
    ct:asm_wait_time(2)
    ct:asm_one_shot_handler("JSON_FLY_UP_FINAL", {})
    ct:asm_log_message("json fly up: terminating")
    ct:asm_terminate()
    ct:end_column(col)
    return col
end

local function insert_json_fly_down_column(ct)
    local col = ct.json_drone:fly_down_server("json_fly_down", "JSON_FLY_DOWN_MONITOR", {})
    ct:asm_log_message("json fly down: ready")
    ct:asm_wait_time(2)
    ct:asm_one_shot_handler("JSON_FLY_DOWN_FINAL", {})
    ct:asm_log_message("json fly down: terminating")
    ct:asm_terminate()
    ct:end_column(col)
    return col
end

-- =========================================================================
-- Test 4: Basic JSON drone control
--
-- Container with 4 flight mode servers (fly_straight, fly_arc, fly_up, fly_down).
-- Client column sequences through all 4 modes.
-- Each server: receives request JSON, waits 2s, sends response JSON, terminates.
-- =========================================================================

local function json_drone_control_test(ct, kb_name)
    ct.json_drone = JsonDroneControl.new(ct)

    ct:start_test(kb_name, 50)

    local container = ct:controlled_node_container("json_control_container")
    insert_json_fly_straight_column(ct)
    insert_json_fly_arc_column(ct)
    insert_json_fly_up_column(ct)
    insert_json_fly_down_column(ct)
    ct:end_column(container)

    local launch = ct:define_column("launch_column", nil, nil, nil, nil, nil, true)
    ct:asm_log_message("launch: starting JSON drone client")

    local client = ct:define_column("json_client_control", nil, nil, nil, nil, nil, true)

    -- Fly straight: 100m at 50m altitude, 10m/s, heading 90
    ct.json_drone:fly_straight_client(
        100.0, 50.0, 10.0, 90.0,
        "JSON_ON_FLY_STRAIGHT_COMPLETE", {}
    )
    ct:asm_log_message("fly straight command sent")
    ct:asm_wait_time(2)

    -- Fly arc: 50m at 60m altitude, 8m/s, heading 180
    ct.json_drone:fly_arc_client(
        50.0, 60.0, 8.0, 180.0,
        "JSON_ON_FLY_ARC_COMPLETE", {}
    )
    ct:asm_log_message("fly arc command sent")
    ct:asm_wait_time(2)

    -- Fly up: climb to 100m at 5m/s
    ct.json_drone:fly_up_client(
        100.0, 5.0,
        "JSON_ON_FLY_UP_COMPLETE", {}
    )
    ct:asm_log_message("fly up command sent")
    ct:asm_wait_time(2)

    -- Fly down: descend to 20m at 3m/s
    ct.json_drone:fly_down_client(
        20.0, 3.0,
        "JSON_ON_FLY_DOWN_COMPLETE", {}
    )
    ct:asm_log_message("fly down command sent")
    ct:asm_log_message("json client control: complete")
    ct:asm_terminate()
    ct:end_column(client)

    ct:define_join_link(client)
    ct:asm_log_message("launch: complete")
    ct:asm_terminate_system()
    ct:end_column(launch)

    ct:end_test()
end

-- =========================================================================
-- Test 5: JSON drone control with exception handling
--
-- Same as test 4 except fly_straight server raises an exception after 2s.
-- Client uses catch_all_exception to handle it.
-- =========================================================================

local function insert_json_fly_exception_straight_column(ct)
    local col = ct.json_drone:fly_straight_server("json_fly_straight", "JSON_FLY_STRAIGHT_MONITOR", {})
    ct:asm_log_message("json fly straight: ready")
    ct:asm_wait_time(2)
    ct:asm_raise_exception(1, { ["low battery"] = 12.0 })
    ct:asm_one_shot_handler("JSON_FLY_STRAIGHT_FINAL", {})
    ct:asm_log_message("json fly straight: terminating")
    ct:asm_terminate()
    ct:end_column(col)
    return col
end

local function json_drone_exception_test(ct, kb_name)
    ct.json_drone = JsonDroneControl.new(ct)

    ct:start_test(kb_name, 50)

    local container = ct:controlled_node_container("json_exc_control_container")
    insert_json_fly_exception_straight_column(ct)
    insert_json_fly_arc_column(ct)
    insert_json_fly_up_column(ct)
    insert_json_fly_down_column(ct)
    ct:end_column(container)

    local launch = ct:define_column("launch_column", nil, nil, nil, nil, nil, true)
    ct:asm_log_message("launch: starting JSON drone exception client")

    local client = ct:catch_all_exception(
        "json_exc_client_control",
        "JSON_DRONE_EXCEPTION_CATCH",
        { aux_data = {} }
    )

    -- Fly straight: will raise exception
    ct.json_drone:fly_straight_client(
        100.0, 50.0, 10.0, 90.0,
        "JSON_ON_FLY_STRAIGHT_COMPLETE", {}
    )
    ct:asm_log_message("fly straight command sent")
    ct:asm_wait_time(2)

    -- Fly arc: 50m at 60m altitude, 8m/s, heading 180
    ct.json_drone:fly_arc_client(
        50.0, 60.0, 8.0, 180.0,
        "JSON_ON_FLY_ARC_COMPLETE", {}
    )
    ct:asm_log_message("fly arc command sent")
    ct:asm_wait_time(2)

    -- Fly up
    ct.json_drone:fly_up_client(
        100.0, 5.0,
        "JSON_ON_FLY_UP_COMPLETE", {}
    )
    ct:asm_log_message("fly up command sent")
    ct:asm_wait_time(2)

    -- Fly down
    ct.json_drone:fly_down_client(
        20.0, 3.0,
        "JSON_ON_FLY_DOWN_COMPLETE", {}
    )
    ct:asm_log_message("fly down command sent")
    ct:asm_log_message("json exception client: complete")
    ct:asm_terminate()
    ct:end_column(client)

    ct:define_join_link(client)
    ct:asm_log_message("launch: complete")
    ct:asm_terminate_system()
    ct:end_column(launch)

    ct:end_test()
end

-- =========================================================================
-- Main
-- =========================================================================

local function add_header(json_file)
    local ct = ChainTreeMaster.new(json_file)

    ct:define_blackboard("json_test_state")
        ct:bb_field("packet_count",   "int32", 0)
        ct:bb_field("telem_count",    "int32", 0)
        ct:bb_field("sensor_x",      "float", 0.0)
        ct:bb_field("sensor_y",      "float", 0.0)
        ct:bb_field("sensor_z",      "float", 0.0)
    ct:end_blackboard()

    return ct
end

local test_list = {
    "json_telemetry_test",
    "json_multi_generator_test",
    "json_verify_test",
    "json_drone_control_test",
    "json_drone_exception_test",
}

local test_dict = {
    json_telemetry_test = json_telemetry_test,
    json_verify_test = json_verify_test,
    json_multi_generator_test = json_multi_generator_test,
    json_drone_control_test = json_drone_control_test,
    json_drone_exception_test = json_drone_exception_test,
}

if arg then
    if #arg ~= 1 then
        print("Usage: luajit json_packet_test.lua <json_file>")
        os.exit(1)
    end

    local json_file = arg[1]
    print(json_file)

    local ct = add_header(json_file)
    for _, test_name in ipairs(test_list) do
        test_dict[test_name](ct, test_name)
    end

    ct:check_and_generate_yaml()
    ct:generate_debug_yaml()
    ct:display_chain_tree_function_mapping()

    local kbs = ct:list_kbs()
    print(table.concat(kbs, ", "))
    print("total nodes", ct.ctb:get_total_node_count())
end
