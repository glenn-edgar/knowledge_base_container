--[[
    robot_mqtt_dsl.lua — ChainTree DSL for C MQTT/CBOR robot

    Architecture:
      Main loop:  mqps_poll() -> CBOR payload -> cfl_send_streaming_data_event()
      ChainTree:  CBOR sink decodes -> user boolean dispatches by packet_type
      Outbound:   user functions write to global robot context -> main loop publishes

    KBs:
      controller  — always active, receives MQTT commands via CBOR sink
      worker_*    — one per action type, each a separate DSL function

    Build (from chain_tree_c directory):
      ./s_build_json.sh  ../ros_planner_ii_robot/dsl/robot_mqtt_dsl.lua ../ros_planner_ii_robot/dsl
      ./s_build_headers_binary.sh ../ros_planner_ii_robot/dsl/robot_mqtt_dsl.json ../ros_planner_ii_robot/dsl robot_handle
]]

local ChainTreeMaster = require("chain_tree_master")

-- =========================================================================
-- Blackboard
-- =========================================================================

local function add_header(json_file)
    local ct = ChainTreeMaster.new(json_file)

    ct:define_blackboard("remote_state")
        ct:bb_field("controller_active",     "uint16", 0)
        ct:bb_field("current_packet_type",   "int32",  0)
        ct:bb_field("current_test_id",       "int32",  0)
        ct:bb_field("current_seq",           "int32",  0)
        ct:bb_field("current_max_time",      "int32",  0)
        ct:bb_field("active_worker_idx",     "int32",  -1)
        ct:bb_field("watchdog_ticks",        "int32",  0)
        ct:bb_field("watchdog_expired",      "uint16", 0)
        ct:bb_field("heartbeat_counter",     "int32",  0)
        ct:bb_field("heartbeat_interval",    "int32",  10)
        ct:bb_field("exec_active",           "uint16", 0)
        ct:bb_field("exec_start",            "uint16", 0)
        ct:bb_field("ticks_remaining",       "int32",  0)
        ct:bb_field("delta_x",              "float",  0)
        ct:bb_field("delta_y",              "float",  0)
        ct:bb_field("delta_z",              "float",  0)
        ct:bb_field("delta_heading",        "float",  0)
        ct:bb_field("delta_arm_angle",      "float",  0)
        ct:bb_field("worker_done",          "uint16", 0)
        ct:bb_field("worker_success",       "uint16", 0)
        ct:bb_field("lookahead_pending",    "uint16", 0)
        ct:bb_field("lookahead_packet_type","int32",  0)
        ct:bb_field("lookahead_test_id",    "int32",  0)
        ct:bb_field("lookahead_seq",        "int32",  0)
        ct:bb_field("lookahead_max_time",   "int32",  0)
        ct:bb_field("shutdown_requested",   "uint16", 0)
        ct:bb_field("fault_code",           "int32",  0)
    ct:end_blackboard()

    return ct
end

-- =========================================================================
-- KB: controller (always active)
--   CBOR sink for RPC dispatch
--   Completion node for worker done handling
-- =========================================================================

-- =========================================================================
-- KB: robot_init (runs once at startup)
--   Publishes initial status (state, energy, bitmask) then terminates.
-- =========================================================================

local function robot_init(ct, kb_name)
    ct:start_test(kb_name, 50)
    local col = ct:define_column(kb_name .. "_exec", nil, nil, nil, nil, {}, true)
        ct:asm_one_shot_handler("ROB_PUBLISH_STATE", {})
        ct:asm_one_shot_handler("ROB_PUBLISH_ENERGY", {})
        ct:asm_one_shot_handler("ROB_PUBLISH_BITMASK", {})
        ct:asm_log_message(kb_name .. ": initial status published")
        ct:asm_terminate()
    ct:end_column(col)
    ct:end_test()
end

local function controller(ct, kb_name)
    ct:start_test(kb_name, 50)

    local main_col = ct:define_column("controller_main", nil, nil, nil, nil, {}, true)

        ct:asm_cbor_sink("CBOR_RPC_DISPATCH",
            { log_prefix = "rpc" },
            "MQTT_RPC_EVENT")

        ct:define_column_link("CTRL_COMPLETION_MAIN", "CTRL_COMPLETION_INIT",
            "CFL_NULL", "CFL_NULL", {}, "COMPLETION")

    ct:end_column(main_col)
    ct:end_test()
end

-- =========================================================================
-- Individual worker KBs — each is its own function for future complexity
--
-- Tomorrow: worker_recharge gets DSL push for energy restore,
-- path workers get spline/line computation, etc.
-- DSL push function will translate to MQTT or Thread depending on hardware.
-- =========================================================================

local function worker_init_check(ct, kb_name)
    ct:start_test(kb_name, 50)
    local col = ct:define_column(kb_name .. "_exec", nil, nil, nil, nil, {}, true)
        ct:asm_one_shot_handler("ROB_SEND_ACK", {})
        ct:asm_log_message(kb_name .. ": started")
        ct:asm_one_shot_handler("WKR_INIT_CHECK_INIT", {})
        ct:asm_wait_time(0.15)
        ct:asm_log_message(kb_name .. ": completed")
        ct:asm_one_shot_handler("WORKER_TERM", {})
        ct:asm_terminate()
    ct:end_column(col)
    ct:end_test()
end

local function worker_path_spline(ct, kb_name)
    ct:start_test(kb_name, 50)
    local col = ct:define_column(kb_name .. "_exec", nil, nil, nil, nil, {}, true)
        ct:asm_one_shot_handler("ROB_SEND_ACK", {})
        ct:asm_log_message(kb_name .. ": started")
        ct:asm_one_shot_handler("WKR_PATH_SPLINE_INIT", {})
        ct:asm_wait_time(0.25)
        ct:asm_log_message(kb_name .. ": completed")
        ct:asm_one_shot_handler("WORKER_TERM", {})
        ct:asm_terminate()
    ct:end_column(col)
    ct:end_test()
end

local function worker_path_line(ct, kb_name)
    ct:start_test(kb_name, 50)
    local col = ct:define_column(kb_name .. "_exec", nil, nil, nil, nil, {}, true)
        ct:asm_one_shot_handler("ROB_SEND_ACK", {})
        ct:asm_log_message(kb_name .. ": started")
        ct:asm_one_shot_handler("WKR_PATH_LINE_INIT", {})
        ct:asm_wait_time(0.25)
        ct:asm_log_message(kb_name .. ": completed")
        ct:asm_one_shot_handler("WORKER_TERM", {})
        ct:asm_terminate()
    ct:end_column(col)
    ct:end_test()
end

local function worker_path_wall(ct, kb_name)
    ct:start_test(kb_name, 50)
    local col = ct:define_column(kb_name .. "_exec", nil, nil, nil, nil, {}, true)
        ct:asm_one_shot_handler("ROB_SEND_ACK", {})
        ct:asm_log_message(kb_name .. ": started")
        ct:asm_one_shot_handler("WKR_PATH_WALL_INIT", {})
        ct:asm_wait_time(0.25)
        ct:asm_log_message(kb_name .. ": completed")
        ct:asm_one_shot_handler("WORKER_TERM", {})
        ct:asm_terminate()
    ct:end_column(col)
    ct:end_test()
end

local function worker_path_rotate(ct, kb_name)
    ct:start_test(kb_name, 50)
    local col = ct:define_column(kb_name .. "_exec", nil, nil, nil, nil, {}, true)
        ct:asm_one_shot_handler("ROB_SEND_ACK", {})
        ct:asm_log_message(kb_name .. ": started")
        ct:asm_one_shot_handler("WKR_PATH_ROTATE_INIT", {})
        ct:asm_wait_time(0.15)
        ct:asm_log_message(kb_name .. ": completed")
        ct:asm_one_shot_handler("WORKER_TERM", {})
        ct:asm_terminate()
    ct:end_column(col)
    ct:end_test()
end

local function worker_deliver_part(ct, kb_name)
    ct:start_test(kb_name, 50)
    local col = ct:define_column(kb_name .. "_exec", nil, nil, nil, nil, {}, true)
        ct:asm_one_shot_handler("ROB_SEND_ACK", {})
        ct:asm_log_message(kb_name .. ": started")
        ct:asm_one_shot_handler("WKR_DELIVER_PART_INIT", {})
        ct:asm_wait_time(0.20)
        ct:asm_log_message(kb_name .. ": completed")
        ct:asm_one_shot_handler("WORKER_TERM", {})
        ct:asm_terminate()
    ct:end_column(col)
    ct:end_test()
end

local function worker_paint_sample(ct, kb_name)
    ct:start_test(kb_name, 50)
    local col = ct:define_column(kb_name .. "_exec", nil, nil, nil, nil, {}, true)
        ct:asm_one_shot_handler("ROB_SEND_ACK", {})
        ct:asm_log_message(kb_name .. ": started")
        ct:asm_one_shot_handler("WKR_PAINT_SAMPLE_INIT", {})
        ct:asm_wait_time(0.20)
        ct:asm_log_message(kb_name .. ": completed")
        ct:asm_one_shot_handler("WORKER_TERM", {})
        ct:asm_terminate()
    ct:end_column(col)
    ct:end_test()
end

local function worker_load_shipping(ct, kb_name)
    ct:start_test(kb_name, 50)
    local col = ct:define_column(kb_name .. "_exec", nil, nil, nil, nil, {}, true)
        ct:asm_one_shot_handler("ROB_SEND_ACK", {})
        ct:asm_log_message(kb_name .. ": started")
        ct:asm_one_shot_handler("WKR_LOAD_SHIPPING_INIT", {})
        ct:asm_wait_time(0.20)
        ct:asm_log_message(kb_name .. ": completed")
        ct:asm_one_shot_handler("WORKER_TERM", {})
        ct:asm_terminate()
    ct:end_column(col)
    ct:end_test()
end

local function worker_pass_gate(ct, kb_name)
    ct:start_test(kb_name, 50)
    local col = ct:define_column(kb_name .. "_exec", nil, nil, nil, nil, {}, true)
        ct:asm_one_shot_handler("ROB_SEND_ACK", {})
        ct:asm_log_message(kb_name .. ": started")
        ct:asm_one_shot_handler("WKR_PASS_GATE_INIT", {})
        ct:asm_wait_time(0.15)
        ct:asm_log_message(kb_name .. ": completed")
        ct:asm_one_shot_handler("WORKER_TERM", {})
        ct:asm_terminate()
    ct:end_column(col)
    ct:end_test()
end

local function worker_inspection_scan(ct, kb_name)
    ct:start_test(kb_name, 50)
    local col = ct:define_column(kb_name .. "_exec", nil, nil, nil, nil, {}, true)
        ct:asm_one_shot_handler("ROB_SEND_ACK", {})
        ct:asm_log_message(kb_name .. ": started")
        ct:asm_one_shot_handler("WKR_INSPECTION_SCAN_INIT", {})
        ct:asm_wait_time(0.12)
        ct:asm_log_message(kb_name .. ": completed")
        ct:asm_one_shot_handler("WORKER_TERM", {})
        ct:asm_terminate()
    ct:end_column(col)
    ct:end_test()
end

local function worker_idle(ct, kb_name)
    ct:start_test(kb_name, 50)
    local col = ct:define_column(kb_name .. "_exec", nil, nil, nil, nil, {}, true)
        ct:asm_one_shot_handler("ROB_SEND_ACK", {})
        ct:asm_log_message(kb_name .. ": started")
        ct:asm_one_shot_handler("WKR_IDLE_INIT", {})
        ct:asm_wait_time(0.05)
        ct:asm_log_message(kb_name .. ": completed")
        ct:asm_one_shot_handler("WORKER_TERM", {})
        ct:asm_terminate()
    ct:end_column(col)
    ct:end_test()
end

local function worker_recharge(ct, kb_name)
    ct:start_test(kb_name, 50)
    local col = ct:define_column(kb_name .. "_exec", nil, nil, nil, nil, {}, true)
        ct:asm_one_shot_handler("ROB_SEND_ACK", {})
        ct:asm_log_message(kb_name .. ": started")
        ct:asm_one_shot_handler("WKR_RECHARGE_INIT", {})
        ct:asm_wait_time(0.30)
        ct:asm_log_message(kb_name .. ": completed")
        ct:asm_one_shot_handler("WORKER_TERM", {})
        ct:asm_terminate()
    ct:end_column(col)
    ct:end_test()
end

-- =========================================================================
-- KB list
-- =========================================================================

local test_list = {
    "robot_init",
    "controller",
    "worker_init_check",
    "worker_path_spline",
    "worker_path_line",
    "worker_path_wall",
    "worker_path_rotate",
    "worker_deliver_part",
    "worker_paint_sample",
    "worker_load_shipping",
    "worker_pass_gate",
    "worker_inspection_scan",
    "worker_idle",
    "worker_recharge",
}

local test_dict = {
    robot_init             = robot_init,
    controller             = controller,
    worker_init_check      = worker_init_check,
    worker_path_spline     = worker_path_spline,
    worker_path_line       = worker_path_line,
    worker_path_wall       = worker_path_wall,
    worker_path_rotate     = worker_path_rotate,
    worker_deliver_part    = worker_deliver_part,
    worker_paint_sample    = worker_paint_sample,
    worker_load_shipping   = worker_load_shipping,
    worker_pass_gate       = worker_pass_gate,
    worker_inspection_scan = worker_inspection_scan,
    worker_idle            = worker_idle,
    worker_recharge        = worker_recharge,
}

-- =========================================================================
-- Main
-- =========================================================================

if arg then
    if #arg ~= 1 then
        print("Usage: luajit robot_mqtt_dsl.lua <json_file>")
        os.exit(1)
    end

    local ct = add_header(arg[1])
    for _, test_name in ipairs(test_list) do
        test_dict[test_name](ct, test_name)
    end

    ct:check_and_generate_yaml()
    ct:generate_debug_yaml()
    ct:display_chain_tree_function_mapping()

    local kbs = ct:list_kbs()
    print("KBs: " .. table.concat(kbs, ", "))
    print("Total nodes: " .. ct.ctb:get_total_node_count())
end
