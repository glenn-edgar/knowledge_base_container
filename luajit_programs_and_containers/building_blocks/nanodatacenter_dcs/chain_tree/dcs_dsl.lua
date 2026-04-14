-- =============================================================================
-- dcs_dsl.lua -- Single-CPU DCS ChainTree DSL (Build 2a).
--
-- KB 0: system_control  (infra lifecycle)
-- KB 1: node_control    (app supervision; gated by system_control)
--
-- Both KBs run in one ChainTree VM. node_control is added to the runtime
-- by system_control's ENABLE_NODE_CONTROL_KB oneshot (cfl_rt.add_test);
-- removed by DISABLE_NODE_CONTROL_KB (cfl_rt.delete_test).
--
-- State lives in Lua dicts held by host_processes/dcs.lua (connectors,
-- process_globals, *_globals). Blackboard is a single placeholder scalar
-- (the builder requires non-empty).
-- =============================================================================

local ChainTreeMaster = require("chain_tree_master")

-- =============================================================================
-- KB 0: system_control
-- =============================================================================

local function system_control(ct, kb_name)
    ct:start_test(kb_name)

    local launch = ct:define_column("launch", nil, nil, nil, nil, nil, true)

        ct:asm_one_shot_handler("READ_ENVIRONS",             {})
        ct:asm_one_shot_handler("READ_BOOTSTRAP_CONFIG",     {})
        ct:asm_one_shot_handler("KILL_NON_INFRA_CONTAINERS", {})
        ct:asm_one_shot_handler("SET_SYSTEM_STATE",          {})

        local sys_sm = ct:define_state_machine(
            "sys_sm_col", "sys_sm",
            { "sync", "setup", "monitor", "teardown" },
            "sync", true)

            local sync_st = ct:define_state("sync", nil)
                ct:asm_one_shot_handler("START_PG_CONTAINER", {})
                ct:asm_verify_timeout(30.0, true, "ERR_INFRA_FAIL", {})
                ct:asm_verify("VERIFY_PG", {}, false, "ERR_INFRA_FAIL", {})

                ct:asm_one_shot_handler("START_NATS_CONTAINER", {})
                ct:asm_verify_timeout(15.0, true, "ERR_INFRA_FAIL", {})
                ct:asm_verify("VERIFY_NATS", {}, false, "ERR_INFRA_FAIL", {})

                ct:asm_one_shot_handler("START_MQTT_CONTAINER", {})
                ct:asm_verify_timeout(15.0, true, "ERR_INFRA_FAIL", {})
                ct:asm_verify("VERIFY_MQTT", {}, false, "ERR_INFRA_FAIL", {})

                ct:asm_one_shot_handler("START_KV_BRIDGE_CONTAINER", {})
                ct:asm_verify_timeout(15.0, true, "ERR_INFRA_FAIL", {})
                ct:asm_verify("VERIFY_KV_BRIDGE", {}, false,
                              "ERR_INFRA_FAIL", {})

                ct:change_state(sys_sm, "setup")
                ct:asm_halt()
            ct:end_column(sync_st)

            local setup_st = ct:define_state("setup", nil)
                ct:asm_verify("VERIFY_PG",        {}, false, "ERR_INFRA_FAIL", {})
                ct:asm_verify("VERIFY_NATS",      {}, false, "ERR_INFRA_FAIL", {})
                ct:asm_verify("VERIFY_MQTT",      {}, false, "ERR_INFRA_FAIL", {})
                ct:asm_verify("VERIFY_KV_BRIDGE", {}, false, "ERR_INFRA_FAIL", {})

                ct:asm_one_shot_handler("CREATE_SHARED_KV_KEYS", {})

                ct:asm_one_shot_handler("ENABLE_NODE_CONTROL_KB", {})
                -- Halt setup for a couple of ticks so node_control, which
                -- was just activated, has real wall time to run its own
                -- sync -> setup and set process_globals.node_control_operational
                -- before the verify fires.
                ct:asm_wait_time(2.0)
                ct:asm_verify_timeout(30.0, true,
                                      "ERR_NODE_CTRL_START_FAIL", {})
                ct:asm_verify("VERIFY_NODE_CTRL_OPERATIONAL", {}, false,
                              "ERR_NODE_CTRL_START_FAIL", {})

                -- Build 2c: write our own ready_bit; then master aggregates
                -- across all CPUs' bits before flipping system_ready.
                ct:asm_one_shot_handler("SET_OWN_READY_BIT", {})
                ct:asm_verify_timeout(30.0, true,
                                      "ERR_NODE_CTRL_START_FAIL", {})
                ct:asm_verify("VERIFY_ALL_CPUS_READY", {}, false,
                              "ERR_NODE_CTRL_START_FAIL", {})

                ct:asm_one_shot_handler("WRITE_SYSTEM_READY_TRUE", {})

                ct:change_state(sys_sm, "monitor")
                ct:asm_halt()
            ct:end_column(setup_st)

            local monitor_st = ct:define_state("monitor", nil)
                -- DCS_ALWAYS_TRUE as while-aux -> loop forever; without it
                -- the while exits after one body iteration and the state
                -- (and KB) terminates.
                local loop = ct:define_while_column(
                    "sys_monitor_loop", nil, nil, nil, "DCS_ALWAYS_TRUE")
                    local body = ct:define_column("sys_monitor_body")
                        ct:asm_one_shot_handler("PUBLISH_SYSTEM_HEARTBEAT", {})
                        ct:asm_verify("VERIFY_NODE_CTRL_HEARTBEAT_FRESH", {}, false,
                                      "ERR_MONITOR_TRIP", {})
                        ct:asm_verify("VERIFY_KV_BRIDGE_HEALTHY", {}, false,
                                      "ERR_MONITOR_TRIP", {})
                        ct:asm_wait_time(1.0)
                    ct:end_column(body)
                ct:end_column(loop)
            ct:end_column(monitor_st)

            local teardown_st = ct:define_state("teardown", nil)
                ct:asm_one_shot_handler("WRITE_SYSTEM_READY_FALSE", {})
                ct:asm_one_shot_handler("CLEAR_OWN_READY_BIT", {})
                ct:asm_wait_time(5.0)

                ct:asm_one_shot_handler("COMMAND_NODE_CONTROL_TEARDOWN", {})
                ct:asm_verify_timeout(15.0, true, "ERR_TEARDOWN_FORCE", {})
                ct:asm_verify("VERIFY_NODE_CTRL_STOPPED", {}, false,
                              "ERR_TEARDOWN_FORCE", {})

                ct:asm_one_shot_handler("DISABLE_NODE_CONTROL_KB",   {})
                ct:asm_one_shot_handler("STOP_KV_BRIDGE_CONTAINER",  {})
                ct:asm_one_shot_handler("STOP_MQTT_CONTAINER",       {})
                ct:asm_one_shot_handler("STOP_NATS_CONTAINER",       {})
                ct:asm_one_shot_handler("STOP_PG_CONTAINER",         {})

                ct:change_state(sys_sm, "sync")
                ct:asm_halt()
            ct:end_column(teardown_st)

        ct:end_state_machine(sys_sm, "sys_sm")

    ct:end_column(launch)
    ct:end_test()
end

-- =============================================================================
-- KB 1: node_control
-- =============================================================================

local function node_control(ct, kb_name)
    ct:start_test(kb_name)

    local launch = ct:define_column("launch", nil, nil, nil, nil, nil, true)

        ct:asm_one_shot_handler("NODE_READ_OWN_CONFIG", {})

        local node_sm = ct:define_state_machine(
            "node_sm_col", "node_sm",
            { "sync", "setup", "monitor", "teardown" },
            "sync", true)

            local sync_st = ct:define_state("sync", nil)
                ct:asm_verify("NODE_VERIFY_BROKERS_REACHABLE", {}, false,
                              "ERR_INFRA_FAIL", {})
                ct:change_state(node_sm, "setup")
                ct:asm_halt()
            ct:end_column(sync_st)

            local setup_st = ct:define_state("setup", nil)
                ct:asm_one_shot_handler("START_ASSIGNED_CONTAINERS", {})
                ct:asm_verify_timeout(60.0, true,
                                      "ERR_CONTAINERS_START_FAIL", {})
                ct:asm_verify("VERIFY_ALL_ASSIGNED_CONTAINERS_HEALTHY", {}, false,
                              "ERR_CONTAINERS_START_FAIL", {})

                ct:asm_one_shot_handler(
                    "WRITE_PROCESS_GLOBALS_NODE_OPERATIONAL_TRUE", {})

                ct:change_state(node_sm, "monitor")
                ct:asm_halt()
            ct:end_column(setup_st)

            local monitor_st = ct:define_state("monitor", nil)
                local loop = ct:define_while_column(
                    "node_monitor_loop", nil, nil, nil, "DCS_ALWAYS_TRUE")
                    local body = ct:define_column("node_monitor_body")
                        ct:asm_verify("VERIFY_ALL_ASSIGNED_CONTAINERS_HEALTHY",
                                      {}, false, "ERR_CONTAINER_DIED", {})
                        ct:asm_one_shot_handler(
                            "WRITE_PROCESS_GLOBALS_NODE_HEARTBEAT", {})
                        ct:asm_verify("VERIFY_NO_TEARDOWN_REQUEST", {}, false,
                                      "ERR_TEARDOWN_REQUESTED", {})
                        ct:asm_one_shot_handler("LOG_SYSTEM_READY_TRANSITIONS", {})
                        ct:asm_wait_time(1.0)
                    ct:end_column(body)
                ct:end_column(loop)
            ct:end_column(monitor_st)

            local teardown_st = ct:define_state("teardown", nil)
                ct:asm_one_shot_handler("STOP_ASSIGNED_CONTAINERS", {})
                ct:asm_verify_timeout(30.0, true, "ERR_TEARDOWN_FORCE", {})
                ct:asm_verify("VERIFY_ALL_ASSIGNED_CONTAINERS_STOPPED",
                              {}, false, "ERR_TEARDOWN_FORCE", {})
                ct:asm_one_shot_handler(
                    "WRITE_PROCESS_GLOBALS_NODE_STOPPED_TRUE", {})
                ct:change_state(node_sm, "sync")
                ct:asm_halt()
            ct:end_column(teardown_st)

        ct:end_state_machine(node_sm, "node_sm")

    ct:end_column(launch)
    ct:end_test()
end

-- =============================================================================
-- Header (blackboard)
-- =============================================================================

local function add_header(output_file)
    local ct = ChainTreeMaster.new(output_file)
    ct:define_blackboard("dcs_bb")
        ct:bb_field("placeholder", "uint16", 0)
    ct:end_blackboard()
    return ct
end

-- =============================================================================
-- Main
-- =============================================================================

local test_list = { "system_control", "node_control" }
local test_dict = {
    system_control = system_control,
    node_control   = node_control,
}

if arg then
    if #arg ~= 1 then
        print("Usage: luajit dcs_dsl.lua <json_file>")
        os.exit(1)
    end

    local json_file = arg[1]
    local ct = add_header(json_file)

    for _, name in ipairs(test_list) do
        test_dict[name](ct, name)
    end

    ct:check_and_generate_yaml()
    ct:generate_debug_yaml()
    ct:display_chain_tree_function_mapping()

    print(table.concat(ct:list_kbs(), ", "))
    print("total nodes", ct.ctb:get_total_node_count())
end
