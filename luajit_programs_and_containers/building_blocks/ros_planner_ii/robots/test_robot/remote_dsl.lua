--[[
    remote_dsl.lua — ChainTree DSL for test_robot remote side.

    One worker KB per virtual node. Robot-independent command mapping.

    Build: luajit remote_dsl.lua remote.json
]]

local ChainTreeMaster = require("chain_tree_master")

-- =========================================================================
-- Blackboard
-- =========================================================================

local function add_header(yaml_file)
    local ct = ChainTreeMaster.new(yaml_file)

    ct:define_blackboard("remote_state")
        ct:bb_field("controller_active",     "bool",   false)
        ct:bb_field("current_packet_type",   "int32",  0)
        ct:bb_field("current_test_id",       "int32",  0)
        ct:bb_field("current_seq",           "int32",  0)
        ct:bb_field("current_max_time",      "int32",  0)
        ct:bb_field("active_worker",         "string", "")
        ct:bb_field("watchdog_ticks",        "int32",  0)
        ct:bb_field("watchdog_expired",      "bool",   false)
        ct:bb_field("heartbeat_counter",     "int32",  0)
        ct:bb_field("heartbeat_interval",    "int32",  10)
        ct:bb_field("exec_active",           "bool",   false)
        ct:bb_field("exec_start",            "bool",   false)
        ct:bb_field("ticks_remaining",       "int32",  0)
        ct:bb_field("delta_x",              "float",  0)
        ct:bb_field("delta_y",              "float",  0)
        ct:bb_field("delta_z",              "float",  0)
        ct:bb_field("delta_heading",        "float",  0)
        ct:bb_field("delta_arm_angle",      "float",  0)
        ct:bb_field("worker_done",          "bool",   false)
        ct:bb_field("worker_success",       "bool",   false)
        ct:bb_field("command_json",         "string", "")
        ct:bb_field("lookahead_pending",    "bool",   false)
        ct:bb_field("lookahead_json",       "string", "")
        ct:bb_field("lookahead_packet_type","int32",  0)
        ct:bb_field("lookahead_test_id",    "int32",  0)
        ct:bb_field("lookahead_seq",        "int32",  0)
        ct:bb_field("lookahead_max_time",   "int32",  0)
        ct:bb_field("shutdown_requested",   "bool",   false)
        ct:bb_field("fault_reason",         "string", "")
    ct:end_blackboard()

    return ct
end

-- =========================================================================
-- KB: controller (always active)
-- =========================================================================

local function controller(ct, kb_name)
    ct:start_test(kb_name)
    local main_col = ct:define_column("controller_main", nil, nil, nil, nil, {}, true)
        ct:define_column_link("CTRL_DISPATCH_MAIN", "CTRL_DISPATCH_INIT",
            "CFL_NULL", "CFL_NULL", {}, "CMD_DISPATCH")
        ct:define_column_link("CTRL_WATCHDOG_MAIN", "CTRL_WATCHDOG_INIT",
            "CFL_NULL", "CFL_NULL", {}, "WATCHDOG")
        ct:define_column_link("CTRL_HEARTBEAT_MAIN", "CTRL_HEARTBEAT_INIT",
            "CFL_NULL", "CFL_NULL", {}, "HEARTBEAT")
        ct:define_column_link("CTRL_COMPLETION_MAIN", "CTRL_COMPLETION_INIT",
            "CFL_NULL", "CFL_NULL", {}, "COMPLETION")
    ct:end_column(main_col)
    ct:end_test()
end

-- =========================================================================
-- Worker KB builder: each virtual node gets its own worker
-- =========================================================================

local function make_worker(worker_name, main_fn, init_fn)
    return function(ct, kb_name)
        ct:start_test(kb_name)
        local col = ct:define_column(kb_name .. "_exec", nil, nil, nil, nil, {}, true)
            ct:asm_one_shot_handler(init_fn, {})
            ct:define_column_link(main_fn, "CFL_NULL",
                "CFL_NULL", "WORKER_TERM", {}, "EXEC")
        ct:end_column(col)
        ct:end_test()
    end
end

-- =========================================================================
-- Test list: controller + one worker per virtual node
-- =========================================================================

local test_list = {
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
}

local test_dict = {
    controller             = controller,
    worker_init_check      = make_worker("worker_init_check",      "WKR_INIT_CHECK_MAIN",      "WKR_INIT_CHECK_INIT"),
    worker_path_spline     = make_worker("worker_path_spline",     "WKR_PATH_SPLINE_MAIN",     "WKR_PATH_SPLINE_INIT"),
    worker_path_line       = make_worker("worker_path_line",       "WKR_PATH_LINE_MAIN",        "WKR_PATH_LINE_INIT"),
    worker_path_wall       = make_worker("worker_path_wall",       "WKR_PATH_WALL_MAIN",        "WKR_PATH_WALL_INIT"),
    worker_path_rotate     = make_worker("worker_path_rotate",     "WKR_PATH_ROTATE_MAIN",     "WKR_PATH_ROTATE_INIT"),
    worker_deliver_part    = make_worker("worker_deliver_part",    "WKR_DELIVER_PART_MAIN",    "WKR_DELIVER_PART_INIT"),
    worker_paint_sample    = make_worker("worker_paint_sample",    "WKR_PAINT_SAMPLE_MAIN",    "WKR_PAINT_SAMPLE_INIT"),
    worker_load_shipping   = make_worker("worker_load_shipping",   "WKR_LOAD_SHIPPING_MAIN",   "WKR_LOAD_SHIPPING_INIT"),
    worker_pass_gate       = make_worker("worker_pass_gate",       "WKR_PASS_GATE_MAIN",       "WKR_PASS_GATE_INIT"),
    worker_inspection_scan = make_worker("worker_inspection_scan", "WKR_INSPECTION_SCAN_MAIN", "WKR_INSPECTION_SCAN_INIT"),
    worker_idle            = make_worker("worker_idle",            "WKR_IDLE_MAIN",             "WKR_IDLE_INIT"),
}

-- =========================================================================
-- Main (CLI mode)
-- =========================================================================

local is_cli = arg and arg[0] and arg[0]:match("remote_dsl%.lua$")

if is_cli then
    if #arg ~= 1 then
        print("Usage: luajit remote_dsl.lua <json_file>")
        os.exit(1)
    end

    local json_file = arg[1]
    local ct = add_header(json_file)
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
