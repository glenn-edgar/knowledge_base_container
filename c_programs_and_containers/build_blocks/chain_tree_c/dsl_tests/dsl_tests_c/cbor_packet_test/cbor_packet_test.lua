--[[
    cbor_packet_test.lua — ChainTree DSL test for CBOR packet streaming

    Mirrors json_packet_test.lua but uses CBOR on the wire.
    Packets defined as Lua tables, converted JSON→CBOR at emit,
    CBOR→JSON at sink. User booleans see identical cfl_json_packet_t.

    Test 1: Single telemetry packet — emit → sink (CBOR wire)
    Test 2: Three generators → single sink (CBOR wire)
    Test 3: Verify boolean filters by x range (CBOR wire)
    Test 4: CBOR controlled nodes, 4 flight modes
    Test 5: Exception in server → catch_all handler (CBOR wire)
]]

local ChainTreeMaster = require("chain_tree_master")

-- =========================================================================
-- Helper: CBOR packet generator column
-- =========================================================================

local function insert_cbor_packet_generator(ct, event_column, event_name)
    local gen_col = ct:define_column("cbor_packet_generator", nil, nil, nil, nil, nil, true)
    ct:asm_wait_time(0.2)
    ct:asm_log_message("emitting CBOR telemetry packet")
    ct:asm_cbor_emit_oneshot(
        { type = "telemetry", seq = 1, topic = "sensors/accel",
          payload = { x = 1.5, y = -0.3, z = 9.81 } },
        event_column, event_name)
    ct:asm_reset()
    ct:end_column(gen_col)
    return gen_col
end

-- =========================================================================
-- Helper: delayed CBOR packet generator
-- =========================================================================

local function insert_cbor_packet_generator_delayed(ct, event_column, event_name,
                                                     device_id, delay, packet_data)
    local column_name = "cbor_packet_gen_" .. tostring(device_id)
    local gen_col = ct:define_column(column_name, nil, nil, nil, nil, nil, true)
    ct:asm_wait_time(delay)
    ct:asm_log_message("cbor emitter " .. tostring(device_id) .. ": sending packet")
    ct:asm_cbor_emit_oneshot(packet_data, event_column, event_name)
    ct:asm_reset()
    ct:end_column(gen_col)
    return gen_col
end

-- =========================================================================
-- Helper: CBOR packet sink column
-- =========================================================================

local function insert_cbor_packet_sink(ct, event_name)
    local sink_col = ct:define_column("cbor_packet_sink", nil, nil, nil, nil, nil, true)
    ct:asm_log_message("cbor sink ready")
    ct:asm_cbor_sink("CBOR_TELEM_SINK",
        { log_prefix = "cbor_telemetry" },
        event_name)
    ct:asm_halt()
    ct:end_column(sink_col)
    return sink_col
end

-- =========================================================================
-- Test 1: single telemetry packet — emit → sink (CBOR wire)
-- =========================================================================

local function cbor_telemetry_test(ct, kb_name)
    ct:start_test(kb_name, 50)

    local launch = ct:define_column("launch", nil, nil, nil, nil, nil, true)
    ct:asm_log_message("launch column")

    insert_cbor_packet_generator(ct, launch, "CBOR_PACKET_EVENT")

    insert_cbor_packet_sink(ct, "CBOR_PACKET_EVENT")

    ct:asm_wait_time(5)
    ct:asm_log_message("test complete — terminating")
    ct:asm_terminate()
    ct:end_column(launch)

    ct:end_test()
end

-- =========================================================================
-- Test 2: three generators → single sink (CBOR wire)
-- =========================================================================

local function cbor_multi_generator_test(ct, kb_name)
    ct:start_test(kb_name, 50)

    local launch = ct:define_column("launch", nil, nil, nil, nil, nil, true)
    ct:asm_log_message("launch column: cbor multi-generator test starting")

    insert_cbor_packet_generator_delayed(ct, launch, "CBOR_PACKET_EVENT", 1, 1.0,
        { type = "telemetry", seq = 0, topic = "sensors/accel_1",
          payload = { x = 1.0, y = 0.0, z = 9.81 } })

    insert_cbor_packet_generator_delayed(ct, launch, "CBOR_PACKET_EVENT", 2, 1.0,
        { type = "telemetry", seq = 0, topic = "sensors/accel_2",
          payload = { x = 2.0, y = 0.1, z = 9.82 } })

    insert_cbor_packet_generator_delayed(ct, launch, "CBOR_PACKET_EVENT", 3, 1.0,
        { type = "telemetry", seq = 0, topic = "sensors/accel_3",
          payload = { x = 3.0, y = 0.2, z = 9.83 } })

    ct:asm_log_message("cbor packet generators created")

    insert_cbor_packet_sink(ct, "CBOR_PACKET_EVENT")

    ct:asm_wait_time(10)
    ct:asm_log_message("cbor multi-generator test complete — terminating")
    ct:asm_terminate()
    ct:end_column(launch)

    ct:end_test()
end

-- =========================================================================
-- Helper: CBOR verify generator
-- =========================================================================

local function insert_cbor_verify_generator(ct, event_column, event_name, device_id, delay, x_value)
    local column_name = "cbor_verify_gen_" .. tostring(device_id)
    local gen_col = ct:define_column(column_name, nil, nil, nil, nil, nil, true)
    ct:asm_wait_time(delay)
    ct:asm_log_message("cbor verify emitter " .. tostring(device_id) .. ": x=" .. tostring(x_value))
    ct:asm_cbor_emit_oneshot(
        { type = "telemetry", seq = device_id, topic = "sensors/accel",
          payload = { x = x_value, y = 0.0, z = 9.81 } },
        event_column, event_name)
    ct:asm_reset()
    ct:end_column(gen_col)
    return gen_col
end

-- =========================================================================
-- Helper: CBOR verified sink column
-- =========================================================================

local function insert_cbor_verified_sink(ct, event_name)
    local sink_col = ct:define_column("cbor_verified_sink", nil, nil, nil, nil, nil, true)
    ct:asm_log_message("cbor verified sink: ready")
    ct:asm_cbor_sink("CBOR_VERIFIED_SINK",
        { log_prefix = "cbor_verified" },
        event_name)
    ct:asm_halt()
    ct:end_column(sink_col)
    return sink_col
end

-- =========================================================================
-- Test 3: verify packet — generators with mixed x values → verify → sink
-- =========================================================================

local function cbor_verify_test(ct, kb_name)
    ct:start_test(kb_name, 50)

    local launch = ct:define_column("launch", nil, nil, nil, nil, nil, true)
    ct:asm_log_message("launch column: cbor verify packet test starting")

    insert_cbor_verify_generator(ct, launch, "CBOR_SENSOR_EVENT", 1, 0.5, 0.3)
    insert_cbor_verify_generator(ct, launch, "CBOR_SENSOR_EVENT", 2, 0.5, 0.8)
    insert_cbor_verify_generator(ct, launch, "CBOR_SENSOR_EVENT", 3, 0.5, 0.1)
    ct:asm_log_message("cbor verify generators created")

    ct:asm_cbor_sink("CBOR_VERIFY_X_RANGE",
        { min_x = 0.0, max_x = 0.5 },
        "CBOR_SENSOR_EVENT")

    insert_cbor_verified_sink(ct, "CBOR_SENSOR_EVENT")

    ct:asm_wait_time(10)
    ct:asm_log_message("cbor verify test complete — terminating")
    ct:asm_terminate()
    ct:end_column(launch)

    ct:end_test()
end

-- =========================================================================
-- CborDroneControl — DSL extension for CBOR-based drone flight control
-- =========================================================================

local CborDroneControl = {}
CborDroneControl.__index = CborDroneControl

function CborDroneControl.new(ct)
    local self = setmetatable({}, CborDroneControl)
    self.ct = ct

    self.command_container = {}

    self.command_container["fly_straight"] = {
        request_port  = ct:make_cbor_control_port("cbor_fly_straight_request"),
        response_port = ct:make_cbor_control_port("cbor_fly_straight_response"),
        api_name      = "cbor_drone_fly_straight",
    }
    self.command_container["fly_arc"] = {
        request_port  = ct:make_cbor_control_port("cbor_fly_arc_request"),
        response_port = ct:make_cbor_control_port("cbor_fly_arc_response"),
        api_name      = "cbor_drone_fly_arc",
    }
    self.command_container["fly_up"] = {
        request_port  = ct:make_cbor_control_port("cbor_fly_up_request"),
        response_port = ct:make_cbor_control_port("cbor_fly_up_response"),
        api_name      = "cbor_drone_fly_up",
    }
    self.command_container["fly_down"] = {
        request_port  = ct:make_cbor_control_port("cbor_fly_down_request"),
        response_port = ct:make_cbor_control_port("cbor_fly_down_response"),
        api_name      = "cbor_drone_fly_down",
    }

    return self
end

-- ---- Servers (controlled nodes) ----

function CborDroneControl:fly_straight_server(column_name, monitor_fn, monitor_data)
    monitor_data = monitor_data or {}
    local c = self.command_container["fly_straight"]
    return self.ct:cbor_controlled_node(c.api_name, column_name, monitor_fn, monitor_data,
                                         c.request_port, c.response_port)
end

function CborDroneControl:fly_arc_server(column_name, monitor_fn, monitor_data)
    monitor_data = monitor_data or {}
    local c = self.command_container["fly_arc"]
    return self.ct:cbor_controlled_node(c.api_name, column_name, monitor_fn, monitor_data,
                                         c.request_port, c.response_port)
end

function CborDroneControl:fly_up_server(column_name, monitor_fn, monitor_data)
    monitor_data = monitor_data or {}
    local c = self.command_container["fly_up"]
    return self.ct:cbor_controlled_node(c.api_name, column_name, monitor_fn, monitor_data,
                                         c.request_port, c.response_port)
end

function CborDroneControl:fly_down_server(column_name, monitor_fn, monitor_data)
    monitor_data = monitor_data or {}
    local c = self.command_container["fly_down"]
    return self.ct:cbor_controlled_node(c.api_name, column_name, monitor_fn, monitor_data,
                                         c.request_port, c.response_port)
end

-- ---- Clients (initiator nodes) ----

function CborDroneControl:fly_straight_client(distance, final_altitude, final_speed, heading,
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
    return self.ct:cbor_client_controlled_node(c.api_name, finalize_fn, request_data,
                                                c.request_port, c.response_port)
end

function CborDroneControl:fly_arc_client(distance, final_altitude, final_speed, heading,
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
    return self.ct:cbor_client_controlled_node(c.api_name, finalize_fn, request_data,
                                                c.request_port, c.response_port)
end

function CborDroneControl:fly_up_client(final_altitude, final_speed, finalize_fn, finalize_data)
    finalize_data = finalize_data or {}
    local c = self.command_container["fly_up"]
    local request_data = {
        command        = "fly_up",
        final_altitude = final_altitude,
        final_speed    = final_speed,
    }
    return self.ct:cbor_client_controlled_node(c.api_name, finalize_fn, request_data,
                                                c.request_port, c.response_port)
end

function CborDroneControl:fly_down_client(final_altitude, final_speed, finalize_fn, finalize_data)
    finalize_data = finalize_data or {}
    local c = self.command_container["fly_down"]
    local request_data = {
        command        = "fly_down",
        final_altitude = final_altitude,
        final_speed    = final_speed,
    }
    return self.ct:cbor_client_controlled_node(c.api_name, finalize_fn, request_data,
                                                c.request_port, c.response_port)
end

-- =========================================================================
-- Shared column helpers for CBOR drone tests
-- =========================================================================

local function insert_cbor_fly_straight_column(ct)
    local col = ct.cbor_drone:fly_straight_server("cbor_fly_straight", "CBOR_FLY_STRAIGHT_MONITOR", {})
    ct:asm_log_message("cbor fly straight: ready")
    ct:asm_wait_time(2)
    ct:asm_one_shot_handler("CBOR_FLY_STRAIGHT_FINAL", {})
    ct:asm_log_message("cbor fly straight: terminating")
    ct:asm_terminate()
    ct:end_column(col)
    return col
end

local function insert_cbor_fly_arc_column(ct)
    local col = ct.cbor_drone:fly_arc_server("cbor_fly_arc", "CBOR_FLY_ARC_MONITOR", {})
    ct:asm_log_message("cbor fly arc: ready")
    ct:asm_wait_time(2)
    ct:asm_one_shot_handler("CBOR_FLY_ARC_FINAL", {})
    ct:asm_log_message("cbor fly arc: terminating")
    ct:asm_terminate()
    ct:end_column(col)
    return col
end

local function insert_cbor_fly_up_column(ct)
    local col = ct.cbor_drone:fly_up_server("cbor_fly_up", "CBOR_FLY_UP_MONITOR", {})
    ct:asm_log_message("cbor fly up: ready")
    ct:asm_wait_time(2)
    ct:asm_one_shot_handler("CBOR_FLY_UP_FINAL", {})
    ct:asm_log_message("cbor fly up: terminating")
    ct:asm_terminate()
    ct:end_column(col)
    return col
end

local function insert_cbor_fly_down_column(ct)
    local col = ct.cbor_drone:fly_down_server("cbor_fly_down", "CBOR_FLY_DOWN_MONITOR", {})
    ct:asm_log_message("cbor fly down: ready")
    ct:asm_wait_time(2)
    ct:asm_one_shot_handler("CBOR_FLY_DOWN_FINAL", {})
    ct:asm_log_message("cbor fly down: terminating")
    ct:asm_terminate()
    ct:end_column(col)
    return col
end

-- =========================================================================
-- Test 4: Basic CBOR drone control
-- =========================================================================

local function cbor_drone_control_test(ct, kb_name)
    ct.cbor_drone = CborDroneControl.new(ct)

    ct:start_test(kb_name, 50)

    local container = ct:controlled_node_container("cbor_control_container")
    insert_cbor_fly_straight_column(ct)
    insert_cbor_fly_arc_column(ct)
    insert_cbor_fly_up_column(ct)
    insert_cbor_fly_down_column(ct)
    ct:end_column(container)

    local launch = ct:define_column("launch_column", nil, nil, nil, nil, nil, true)
    ct:asm_log_message("launch: starting CBOR drone client")

    local client = ct:define_column("cbor_client_control", nil, nil, nil, nil, nil, true)

    ct.cbor_drone:fly_straight_client(
        100.0, 50.0, 10.0, 90.0,
        "CBOR_ON_FLY_STRAIGHT_COMPLETE", {}
    )
    ct:asm_log_message("fly straight command sent")
    ct:asm_wait_time(2)

    ct.cbor_drone:fly_arc_client(
        50.0, 60.0, 8.0, 180.0,
        "CBOR_ON_FLY_ARC_COMPLETE", {}
    )
    ct:asm_log_message("fly arc command sent")
    ct:asm_wait_time(2)

    ct.cbor_drone:fly_up_client(
        100.0, 5.0,
        "CBOR_ON_FLY_UP_COMPLETE", {}
    )
    ct:asm_log_message("fly up command sent")
    ct:asm_wait_time(2)

    ct.cbor_drone:fly_down_client(
        20.0, 3.0,
        "CBOR_ON_FLY_DOWN_COMPLETE", {}
    )
    ct:asm_log_message("fly down command sent")
    ct:asm_log_message("cbor client control: complete")
    ct:asm_terminate()
    ct:end_column(client)

    ct:define_join_link(client)
    ct:asm_log_message("launch: complete")
    ct:asm_terminate_system()
    ct:end_column(launch)

    ct:end_test()
end

-- =========================================================================
-- Test 5: CBOR drone control with exception handling
-- =========================================================================

local function insert_cbor_fly_exception_straight_column(ct)
    local col = ct.cbor_drone:fly_straight_server("cbor_fly_straight", "CBOR_FLY_STRAIGHT_MONITOR", {})
    ct:asm_log_message("cbor fly straight: ready")
    ct:asm_wait_time(2)
    ct:asm_raise_exception(1, { ["low battery"] = 12.0 })
    ct:asm_one_shot_handler("CBOR_FLY_STRAIGHT_FINAL", {})
    ct:asm_log_message("cbor fly straight: terminating")
    ct:asm_terminate()
    ct:end_column(col)
    return col
end

local function cbor_drone_exception_test(ct, kb_name)
    ct.cbor_drone = CborDroneControl.new(ct)

    ct:start_test(kb_name, 50)

    local container = ct:controlled_node_container("cbor_exc_control_container")
    insert_cbor_fly_exception_straight_column(ct)
    insert_cbor_fly_arc_column(ct)
    insert_cbor_fly_up_column(ct)
    insert_cbor_fly_down_column(ct)
    ct:end_column(container)

    local launch = ct:define_column("launch_column", nil, nil, nil, nil, nil, true)
    ct:asm_log_message("launch: starting CBOR drone exception client")

    local client = ct:catch_all_exception(
        "cbor_exc_client_control",
        "CBOR_DRONE_EXCEPTION_CATCH",
        { aux_data = {} }
    )

    ct.cbor_drone:fly_straight_client(
        100.0, 50.0, 10.0, 90.0,
        "CBOR_ON_FLY_STRAIGHT_COMPLETE", {}
    )
    ct:asm_log_message("fly straight command sent")
    ct:asm_wait_time(2)

    ct.cbor_drone:fly_arc_client(
        50.0, 60.0, 8.0, 180.0,
        "CBOR_ON_FLY_ARC_COMPLETE", {}
    )
    ct:asm_log_message("fly arc command sent")
    ct:asm_wait_time(2)

    ct.cbor_drone:fly_up_client(
        100.0, 5.0,
        "CBOR_ON_FLY_UP_COMPLETE", {}
    )
    ct:asm_log_message("fly up command sent")
    ct:asm_wait_time(2)

    ct.cbor_drone:fly_down_client(
        20.0, 3.0,
        "CBOR_ON_FLY_DOWN_COMPLETE", {}
    )
    ct:asm_log_message("fly down command sent")
    ct:asm_log_message("cbor exception client: complete")
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

    ct:define_blackboard("cbor_test_state")
        ct:bb_field("packet_count",   "int32", 0)
        ct:bb_field("telem_count",    "int32", 0)
        ct:bb_field("sensor_x",      "float", 0.0)
        ct:bb_field("sensor_y",      "float", 0.0)
        ct:bb_field("sensor_z",      "float", 0.0)
    ct:end_blackboard()

    return ct
end

local test_list = {
    "cbor_telemetry_test",
    "cbor_multi_generator_test",
    "cbor_verify_test",
    "cbor_drone_control_test",
    "cbor_drone_exception_test",
}

local test_dict = {
    cbor_telemetry_test = cbor_telemetry_test,
    cbor_verify_test = cbor_verify_test,
    cbor_multi_generator_test = cbor_multi_generator_test,
    cbor_drone_control_test = cbor_drone_control_test,
    cbor_drone_exception_test = cbor_drone_exception_test,
}

if arg then
    if #arg ~= 1 then
        print("Usage: luajit cbor_packet_test.lua <json_file>")
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
