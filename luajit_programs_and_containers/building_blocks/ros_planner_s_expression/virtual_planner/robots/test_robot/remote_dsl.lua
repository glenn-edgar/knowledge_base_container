--[[
    Remote Unit Tree — ChainTree DSL for the robot side.

    Single KB. Receives commands via RPC channel, executes actions,
    streams bitmap state back to the hub.

    In a real robot, the user functions are C functions driving hardware.
    The tree structure is identical — only the function implementations change.
]]

local ChainTreeMaster = require("chain_tree_master")

-- =========================================================================
-- Blackboard
-- =========================================================================

local function add_header(yaml_file)
    local ct = ChainTreeMaster.new(yaml_file)

    ct:define_blackboard("remote_state")
        -- Command input (from RPC channel)
        ct:bb_field("command_pending",  "bool",   false)
        ct:bb_field("command_type",     "string", "")
        -- Execution state
        ct:bb_field("exec_active",      "bool",   false)
        ct:bb_field("ticks_remaining",  "int32",  0)
        -- Bitmap output (to streaming channel)
        ct:bb_field("stream_seg_complete",    "bool", false)
        ct:bb_field("stream_action_complete", "bool", false)
        ct:bb_field("stream_obstacle",        "bool", false)
        ct:bb_field("stream_motor_fault",     "bool", false)
        ct:bb_field("stream_action_fault",    "bool", false)
        -- Sim injection (test harness sets these)
        ct:bb_field("sim_obstacle",     "bool", false)
        ct:bb_field("sim_motor_fault",  "bool", false)
    ct:end_blackboard()

    return ct
end

-- =========================================================================
-- KB: remote_unit
-- =========================================================================
-- Dummy remote unit. Three leaf nodes under a column:
--   receiver  — picks up RPC command packets
--   executor  — simulates physical action
--   streamer  — writes bitmap state for streaming channel

local function remote_unit(ct, kb_name)
    ct:start_test(kb_name)

    local main_col = ct:define_column("remote_main", nil, nil, nil, nil, {}, true)

        ct:define_column_link(
            "REMOTE_RECV_MAIN",
            "REMOTE_RECV_INIT",
            "CFL_NULL",
            "CFL_NULL",
            {}, "CMD_RECV")

        ct:define_column_link(
            "REMOTE_EXEC_MAIN",
            "REMOTE_EXEC_INIT",
            "REMOTE_EXEC_CHECK",
            "REMOTE_EXEC_TERM",
            {}, "ACT_EXEC")

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
-- KB: remote_idle
-- =========================================================================
-- Terminal state. Robot parks, stops streaming.

local function remote_idle(ct, kb_name)
    ct:start_test(kb_name)
    -- TODO: build tree -- stop motors, disable streaming
    ct:end_test()
end

-- =========================================================================
-- Test list
-- =========================================================================

local test_list = {
    "remote_unit",
    "remote_idle",
}

local test_dict = {
    remote_unit = remote_unit,
    remote_idle = remote_idle,
}

-- =========================================================================
-- Main
-- =========================================================================

if arg then
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
