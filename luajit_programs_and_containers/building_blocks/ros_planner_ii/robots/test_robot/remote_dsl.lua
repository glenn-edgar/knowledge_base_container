--[[
    remote_dsl.lua — ChainTree DSL for the test_robot remote side.

    Single KB (remote_unit) with three leaf nodes under a column:
      receiver  — reads AVRC command packets from RPC socket
      executor  — simulates physical action (tick countdown)
      streamer  — sends ack + bitmap responses on stream socket

    In a real robot, the user functions would be C functions driving
    hardware. The tree structure is identical — only the implementations change.

    Build: luajit remote_dsl.lua remote.json
]]

local ChainTreeMaster = require("chain_tree_master")

-- =========================================================================
-- Blackboard
-- =========================================================================

local function add_header(yaml_file)
    local ct = ChainTreeMaster.new(yaml_file)

    ct:define_blackboard("remote_state")
        -- Command input (written by receiver from RPC socket)
        ct:bb_field("command_pending",    "bool",   false)
        ct:bb_field("command_type",       "int32",  0)
        ct:bb_field("command_seq",        "int32",  0)
        ct:bb_field("command_test_id",    "int32",  0)
        -- Execution state
        ct:bb_field("exec_active",        "bool",   false)
        ct:bb_field("exec_start",         "bool",   false)
        ct:bb_field("ticks_remaining",    "int32",  0)
        -- Stream output (written by executor, sent by streamer)
        ct:bb_field("stream_seg_complete",    "bool", false)
        ct:bb_field("stream_action_complete", "bool", false)
        ct:bb_field("stream_obstacle",        "bool", false)
        ct:bb_field("stream_motor_fault",     "bool", false)
        ct:bb_field("stream_action_fault",    "bool", false)
        -- Ack output
        ct:bb_field("ack_pending",        "bool",   false)
        ct:bb_field("ack_seq",            "int32",  0)
        ct:bb_field("ack_status",         "int32",  0)
        -- Shutdown flag
        ct:bb_field("shutdown_requested", "bool",   false)
    ct:end_blackboard()

    return ct
end

-- =========================================================================
-- KB: remote_unit
-- =========================================================================

local function remote_unit(ct, kb_name)
    ct:start_test(kb_name)

    local main_col = ct:define_column("remote_main", nil, nil, nil, nil, {}, true)

        -- Receiver: reads AVRC packets from RPC socket, writes to blackboard
        ct:define_column_link(
            "REMOTE_RECV_MAIN",
            "REMOTE_RECV_INIT",
            "CFL_NULL",
            "CFL_NULL",
            {}, "CMD_RECV")

        -- Executor: simulates action (tick countdown), sets completion flags
        ct:define_column_link(
            "REMOTE_EXEC_MAIN",
            "REMOTE_EXEC_INIT",
            "CFL_NULL",
            "REMOTE_EXEC_TERM",
            {}, "ACT_EXEC")

        -- Streamer: reads completion flags, sends ack + bitmap on stream socket
        ct:define_column_link(
            "REMOTE_STREAM_MAIN",
            "REMOTE_STREAM_INIT",
            "CFL_NULL",
            "CFL_NULL",
            {}, "BMP_STREAM")

    ct:end_column(main_col)

    ct:end_test()
end

-- =========================================================================
-- Test list
-- =========================================================================

local test_list = { "remote_unit" }
local test_dict = { remote_unit = remote_unit }

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
