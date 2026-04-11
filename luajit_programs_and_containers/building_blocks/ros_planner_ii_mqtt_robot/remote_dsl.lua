--[[
    remote_dsl.lua — ChainTree DSL for MQTT robot remote side.

    Individual worker KB per virtual node — matches KB definitions.
    No controller KB (controller is robot_controller.lua).

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
        ct:bb_field("active_worker",         "string", "")
        ct:bb_field("watchdog_ticks",        "int32",  0)
        ct:bb_field("watchdog_silence",      "int32",  0)
        ct:bb_field("worker_alive",          "bool",   false)
        ct:bb_field("heartbeat_counter",     "int32",  0)
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
        ct:bb_field("shutdown_requested",   "bool",   false)
        ct:bb_field("fault_reason",         "string", "")
    ct:end_blackboard()

    return ct
end

-- =========================================================================
-- Individual worker KB definitions — one per virtual node
-- Each matches the KB virtual node definition in construct_surface_ops.lua
-- =========================================================================

-- init_check: preflight self-test
-- packet_type=1, no params, bitmask: battery_ok|motors_ok|sensors_ok|comms_ok
local function worker_init_check(ct, kb_name)
    ct:start_test(kb_name)
    local col = ct:define_column("init_check_exec", nil, nil, nil, nil, {}, true)
        ct:asm_one_shot_handler("WKR_INIT_CHECK_INIT", {})
        ct:define_column_link("WKR_INIT_CHECK_MAIN", "CFL_NULL",
            "CFL_NULL", "WORKER_TERM", {}, "EXEC")
    ct:end_column(col)
    ct:end_test()
end

-- path_spline: follow spline path between nodes
-- packet_type=2, params: from_x,from_y,to_x,to_y,speed,distance,segment_index,total_segments
-- bitmask: seg_complete|obstacle|motor_fault, pose: delta_x,delta_y,delta_heading
local function worker_path_spline(ct, kb_name)
    ct:start_test(kb_name)
    local col = ct:define_column("path_spline_exec", nil, nil, nil, nil, {}, true)
        ct:asm_one_shot_handler("WKR_PATH_SPLINE_INIT", {})
        ct:define_column_link("WKR_PATH_SPLINE_MAIN", "CFL_NULL",
            "CFL_NULL", "WORKER_TERM", {}, "EXEC")
    ct:end_column(col)
    ct:end_test()
end

-- path_line: line follow between nodes
-- packet_type=3, params: from_x,from_y,to_x,to_y,speed,distance
-- bitmask: seg_complete|obstacle|motor_fault, pose: delta_x,delta_y,delta_heading
local function worker_path_line(ct, kb_name)
    ct:start_test(kb_name)
    local col = ct:define_column("path_line_exec", nil, nil, nil, nil, {}, true)
        ct:asm_one_shot_handler("WKR_PATH_LINE_INIT", {})
        ct:define_column_link("WKR_PATH_LINE_MAIN", "CFL_NULL",
            "CFL_NULL", "WORKER_TERM", {}, "EXEC")
    ct:end_column(col)
    ct:end_test()
end

-- path_wall: wall follow between nodes
-- packet_type=4, params: from_x,from_y,to_x,to_y,speed,distance,wall_standoff
-- bitmask: seg_complete|obstacle|motor_fault|wall_lost, pose: delta_x,delta_y,delta_heading
local function worker_path_wall(ct, kb_name)
    ct:start_test(kb_name)
    local col = ct:define_column("path_wall_exec", nil, nil, nil, nil, {}, true)
        ct:asm_one_shot_handler("WKR_PATH_WALL_INIT", {})
        ct:define_column_link("WKR_PATH_WALL_MAIN", "CFL_NULL",
            "CFL_NULL", "WORKER_TERM", {}, "EXEC")
    ct:end_column(col)
    ct:end_test()
end

-- path_rotate: rotate in place to heading
-- packet_type=5, params: from_heading,to_heading
-- bitmask: rotate_complete|motor_fault, pose: delta_heading
local function worker_path_rotate(ct, kb_name)
    ct:start_test(kb_name)
    local col = ct:define_column("path_rotate_exec", nil, nil, nil, nil, {}, true)
        ct:asm_one_shot_handler("WKR_PATH_ROTATE_INIT", {})
        ct:define_column_link("WKR_PATH_ROTATE_MAIN", "CFL_NULL",
            "CFL_NULL", "WORKER_TERM", {}, "EXEC")
    ct:end_column(col)
    ct:end_test()
end

-- deliver_part: arm delivery at assembly station
-- packet_type=6, params: arm_target,arm_speed,arm_return,payload_type
-- bitmask: arm_at_target|payload_gripped|action_complete|arm_fault, pose: delta_arm_angle
local function worker_deliver_part(ct, kb_name)
    ct:start_test(kb_name)
    local col = ct:define_column("deliver_part_exec", nil, nil, nil, nil, {}, true)
        ct:asm_one_shot_handler("WKR_DELIVER_PART_INIT", {})
        ct:define_column_link("WKR_DELIVER_PART_MAIN", "CFL_NULL",
            "CFL_NULL", "WORKER_TERM", {}, "EXEC")
    ct:end_column(col)
    ct:end_test()
end

-- paint_sample: paint operation at painting station
-- packet_type=7, params: arm_target,arm_speed,arm_return,hold_time
-- bitmask: arm_at_target|action_complete|arm_fault, pose: delta_arm_angle
local function worker_paint_sample(ct, kb_name)
    ct:start_test(kb_name)
    local col = ct:define_column("paint_sample_exec", nil, nil, nil, nil, {}, true)
        ct:asm_one_shot_handler("WKR_PAINT_SAMPLE_INIT", {})
        ct:define_column_link("WKR_PAINT_SAMPLE_MAIN", "CFL_NULL",
            "CFL_NULL", "WORKER_TERM", {}, "EXEC")
    ct:end_column(col)
    ct:end_test()
end

-- load_shipping: load container at shipping station
-- packet_type=8, params: arm_target,arm_speed,arm_return,payload_type
-- bitmask: arm_at_target|payload_gripped|action_complete|arm_fault, pose: delta_arm_angle
local function worker_load_shipping(ct, kb_name)
    ct:start_test(kb_name)
    local col = ct:define_column("load_shipping_exec", nil, nil, nil, nil, {}, true)
        ct:asm_one_shot_handler("WKR_LOAD_SHIPPING_INIT", {})
        ct:define_column_link("WKR_LOAD_SHIPPING_MAIN", "CFL_NULL",
            "CFL_NULL", "WORKER_TERM", {}, "EXEC")
    ct:end_column(col)
    ct:end_test()
end

-- pass_gate: open gate, drive through, close gate
-- packet_type=9, params: rpc_open_hash,rpc_close_hash,drive_through
-- bitmask: gate_opened|drive_complete|gate_closed|action_complete, pose: delta_x,delta_y,delta_heading
local function worker_pass_gate(ct, kb_name)
    ct:start_test(kb_name)
    local col = ct:define_column("pass_gate_exec", nil, nil, nil, nil, {}, true)
        ct:asm_one_shot_handler("WKR_PASS_GATE_INIT", {})
        ct:define_column_link("WKR_PASS_GATE_MAIN", "CFL_NULL",
            "CFL_NULL", "WORKER_TERM", {}, "EXEC")
    ct:end_column(col)
    ct:end_test()
end

-- inspection_scan: sensor read at inspection point
-- packet_type=10, params: sensor_port,sensor_type
-- bitmask: reading_ready|sensor_fault, pose: (none)
local function worker_inspection_scan(ct, kb_name)
    ct:start_test(kb_name)
    local col = ct:define_column("inspection_scan_exec", nil, nil, nil, nil, {}, true)
        ct:asm_one_shot_handler("WKR_INSPECTION_SCAN_INIT", {})
        ct:define_column_link("WKR_INSPECTION_SCAN_MAIN", "CFL_NULL",
            "CFL_NULL", "WORKER_TERM", {}, "EXEC")
    ct:end_column(col)
    ct:end_test()
end

-- recharge: recharge energy at charging station
-- packet_type=12, params: target_energy
-- bitmask: charging|charge_complete|charger_fault, pose: (none)
local function worker_recharge(ct, kb_name)
    ct:start_test(kb_name)
    local col = ct:define_column("recharge_exec", nil, nil, nil, nil, {}, true)
        ct:asm_one_shot_handler("WKR_RECHARGE_INIT", {})
        ct:define_column_link("WKR_RECHARGE_MAIN", "CFL_NULL",
            "CFL_NULL", "WORKER_TERM", {}, "EXEC")
    ct:end_column(col)
    ct:end_test()
end

-- operation: generic operation at a stop
-- packet_type=20, params: operation_type, data
-- bitmask: action_complete|action_fault, pose: (none)
local function worker_operation(ct, kb_name)
    ct:start_test(kb_name)
    local col = ct:define_column("operation_exec", nil, nil, nil, nil, {}, true)
        ct:asm_one_shot_handler("WKR_OPERATION_INIT", {})
        ct:define_column_link("WKR_OPERATION_MAIN", "CFL_NULL",
            "CFL_NULL", "WORKER_TERM", {}, "EXEC")
    ct:end_column(col)
    ct:end_test()
end

-- idle: park robot
-- packet_type=11, no params, bitmask: parked, pose: (none)
local function worker_idle(ct, kb_name)
    ct:start_test(kb_name)
    local col = ct:define_column("idle_exec", nil, nil, nil, nil, {}, true)
        ct:asm_one_shot_handler("WKR_IDLE_INIT", {})
        ct:define_column_link("WKR_IDLE_MAIN", "CFL_NULL",
            "CFL_NULL", "WORKER_TERM", {}, "EXEC")
    ct:end_column(col)
    ct:end_test()
end

-- =========================================================================
-- Test list: one worker per virtual node (matches KB packet_type order)
-- =========================================================================

-- Test list: index = packet_type. Gaps are nil (unused packet_type IDs).
local test_list = {}
test_list[1]  = "worker_init_check"       -- packet_type=1
test_list[2]  = "worker_path_spline"      -- packet_type=2
test_list[3]  = "worker_path_line"        -- packet_type=3
test_list[4]  = "worker_path_wall"        -- packet_type=4
test_list[5]  = "worker_path_rotate"      -- packet_type=5
test_list[6]  = "worker_deliver_part"     -- packet_type=6
test_list[7]  = "worker_paint_sample"     -- packet_type=7
test_list[8]  = "worker_load_shipping"    -- packet_type=8
test_list[9]  = "worker_pass_gate"        -- packet_type=9
test_list[10] = "worker_inspection_scan"  -- packet_type=10
test_list[11] = "worker_idle"             -- packet_type=11
test_list[12] = "worker_recharge"         -- packet_type=12
-- 13-19 reserved
test_list[20] = "worker_operation"        -- packet_type=20

local test_dict = {
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
    worker_operation       = worker_operation,
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
    -- Sparse list — find max index, skip nil gaps
    local max_idx = 0
    for k in pairs(test_list) do if k > max_idx then max_idx = k end end
    for i = 1, max_idx do
        local test_name = test_list[i]
        if test_name then
            test_dict[test_name](ct, test_name)
        end
    end

    ct:check_and_generate_yaml()
    ct:generate_debug_yaml()
    ct:display_chain_tree_function_mapping()

    local kbs = ct:list_kbs()
    print("KBs: " .. table.concat(kbs, ", "))
    print("Total nodes: " .. ct.ctb:get_total_node_count())
end
