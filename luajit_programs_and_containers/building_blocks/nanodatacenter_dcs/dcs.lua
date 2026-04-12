-- =============================================================================
-- dcs.lua — Nanodatacenter DCS ChainTree DSL
--
-- Single DSL file containing all KBs for the distributed control system.
-- Test order determines KB index:
--     KB 0: system_control
--     KB 1: node_control
--
-- Both KBs run in the same LuaJIT process, coordinated via:
--     process_globals   — cross-KB, same-process Lua dict
--     system_control_globals / node_control_globals — per-KB config/state
--
-- Design decisions (see continue.md / memory):
--   * No retry on verify failure: terminate_system, docker restart takes over.
--   * Teardown -> sync is the only voluntary recovery loop.
--   * system_control on every CPU; outer state machine dispatches master/slave
--     statically by MASTER_FLAG env. No runtime master failover.
--   * node_control supervises containers assigned to this CPU by
--     provisioning.json (same code on master and slaves).
--   * Apps subscribe directly to NATS KV system.ready (not relayed).
--   * system_control copies system.ready to process_globals.system_ready_current
--     for node_control's visibility; container lifecycle ties to teardown only.
-- =============================================================================

local ChainTreeMaster = require("chain_tree_master")

-- =============================================================================
-- KB 0: system_control
-- =============================================================================

local function system_control(ct, kb_name)
    ct:start_test(kb_name)

    local launch = ct:define_column("launch", nil, nil, nil, nil, nil, true)

        -- -----------------------------------------------------------
        -- Launch oneshots (every CPU, master or slave)
        -- -----------------------------------------------------------
        ct:asm_one_shot_handler("READ_ENVIRONS", {})
        ct:asm_one_shot_handler("READ_PROVISIONING_JSON", {})
        ct:asm_one_shot_handler("KILL_ALL_CONTAINERS", {})
        ct:asm_one_shot_handler("SET_SYSTEM_STATE", {})

        -- -----------------------------------------------------------
        -- Outer state machine: static master-vs-slave dispatch.
        -- -----------------------------------------------------------
        ct:define_state_machine("sys_sm", nil, nil)

            -- ==========================================================
            -- MASTER
            -- ==========================================================
            ct:define_state("master", false)
                ct:define_state_machine("master_sm", nil, nil)

                    ct:define_state("sync", true)
                        ct:asm_one_shot_handler("START_PG_CONTAINER", {})
                        ct:asm_verify_timeout(30.0, true,
                                              "ERR_INFRA_FAIL", {})
                        ct:asm_verify("VERIFY_PG", {}, false,
                                      "ERR_INFRA_FAIL", {})

                        ct:asm_one_shot_handler("START_NATS_CONTAINER", {})
                        ct:asm_verify_timeout(15.0, true,
                                              "ERR_INFRA_FAIL", {})
                        ct:asm_verify("VERIFY_NATS", {}, false,
                                      "ERR_INFRA_FAIL", {})

                        ct:asm_one_shot_handler("START_MQTT_CONTAINER", {})
                        ct:asm_verify_timeout(15.0, true,
                                              "ERR_INFRA_FAIL", {})
                        ct:asm_verify("VERIFY_MQTT", {}, false,
                                      "ERR_INFRA_FAIL", {})

                        ct:change_state("master_sm", "setup")
                        ct:asm_halt()
                    ct:end_state()

                    ct:define_state("setup", false)
                        ct:asm_verify("VERIFY_PG",   {}, false,
                                      "ERR_INFRA_FAIL", {})
                        ct:asm_verify("VERIFY_NATS", {}, false,
                                      "ERR_INFRA_FAIL", {})
                        ct:asm_verify("VERIFY_MQTT", {}, false,
                                      "ERR_INFRA_FAIL", {})

                        ct:asm_one_shot_handler("CREATE_SHARED_KV_KEYS", {})

                        ct:asm_one_shot_handler("ENABLE_NODE_CONTROL_KB", {})
                        ct:asm_verify_timeout(30.0, true,
                                              "ERR_NODE_CTRL_START_FAIL", {})
                        ct:asm_verify("VERIFY_NODE_CTRL_OPERATIONAL", {}, false,
                                      "ERR_NODE_CTRL_START_FAIL", {})

                        ct:asm_one_shot_handler(
                            "WRITE_NODE_CONTAINERS_OPERATIONAL_TRUE", {})

                        ct:asm_verify_timeout(120.0, true,
                                              "ERR_AGGREGATOR_TIMEOUT", {})
                        ct:asm_verify("VERIFY_ALL_NODES_OPERATIONAL", {}, false,
                                      "ERR_AGGREGATOR_TIMEOUT", {})

                        ct:asm_one_shot_handler("WRITE_SYSTEM_READY_TRUE", {})

                        ct:change_state("master_sm", "monitor")
                        ct:asm_halt()
                    ct:end_state()

                    ct:define_state("monitor", false)
                        local loop = ct:define_while_column(
                            "master_monitor_loop", "CFL_NEVER_TRUE")

                            ct:asm_one_shot_handler(
                                "PUBLISH_SYSTEM_HEARTBEAT", {})

                            ct:asm_verify("VERIFY_NODE_CTRL_HEARTBEAT_FRESH",
                                          {}, false, "ERR_MONITOR_TRIP", {})

                            ct:asm_verify("VERIFY_ALL_SLAVE_HEARTBEATS_FRESH",
                                          {}, false, "ERR_MONITOR_TRIP", {})

                            ct:asm_verify("VERIFY_ALL_NODES_STILL_OPERATIONAL",
                                          {}, false, "ERR_MONITOR_TRIP", {})

                            ct:asm_one_shot_handler(
                                "DISPATCH_SLAVE_RPC_QUEUE", {})

                            ct:asm_wait_time(1.0)
                        ct:end_column(loop)
                    ct:end_state()

                    ct:define_state("teardown", false)
                        ct:asm_one_shot_handler("WRITE_SYSTEM_READY_FALSE", {})
                        ct:asm_wait_time(5.0)

                        ct:asm_one_shot_handler("RPC_ALL_SLAVES_TEARDOWN", {})
                        ct:asm_verify_timeout(60.0, true,
                                              "ERR_TEARDOWN_FORCE", {})
                        ct:asm_verify("VERIFY_ALL_SLAVES_STOPPED", {}, false,
                                      "ERR_TEARDOWN_FORCE", {})

                        ct:asm_one_shot_handler(
                            "COMMAND_NODE_CONTROL_TEARDOWN", {})
                        ct:asm_verify_timeout(15.0, true,
                                              "ERR_TEARDOWN_FORCE", {})
                        ct:asm_verify("VERIFY_NODE_CTRL_STOPPED", {}, false,
                                      "ERR_TEARDOWN_FORCE", {})

                        ct:asm_one_shot_handler("STOP_MQTT_CONTAINER", {})
                        ct:asm_one_shot_handler("STOP_NATS_CONTAINER", {})
                        ct:asm_one_shot_handler("STOP_PG_CONTAINER",   {})

                        ct:change_state("master_sm", "sync")
                        ct:asm_halt()
                    ct:end_state()

                ct:end_state_machine()
            ct:end_state()

            -- ==========================================================
            -- SLAVE
            -- ==========================================================
            ct:define_state("slave", false)
                ct:define_state_machine("slave_sm", nil, nil)

                    ct:define_state("sync", true)
                        ct:asm_wait_for_event("PG_CONNECTION_UP", 1, true,
                                              0, nil, nil, nil)
                        ct:asm_wait_time(3.0)
                        ct:asm_verify("VERIFY_PG",   {}, false,
                                      "ERR_INFRA_FAIL", {})
                        ct:asm_verify("VERIFY_NATS", {}, false,
                                      "ERR_INFRA_FAIL", {})
                        ct:asm_verify("VERIFY_MQTT", {}, false,
                                      "ERR_INFRA_FAIL", {})

                        ct:change_state("slave_sm", "setup")
                        ct:asm_halt()
                    ct:end_state()

                    ct:define_state("setup", false)
                        ct:asm_verify("VERIFY_PG",   {}, false,
                                      "ERR_INFRA_FAIL", {})
                        ct:asm_verify("VERIFY_NATS", {}, false,
                                      "ERR_INFRA_FAIL", {})
                        ct:asm_verify("VERIFY_MQTT", {}, false,
                                      "ERR_INFRA_FAIL", {})

                        ct:asm_verify("VERIFY_SHARED_KV_KEYS", {}, false,
                                      "ERR_INFRA_FAIL", {})

                        ct:asm_one_shot_handler("CREATE_OWN_KV_KEYS", {})

                        ct:asm_one_shot_handler("ENABLE_NODE_CONTROL_KB", {})
                        ct:asm_verify_timeout(30.0, true,
                                              "ERR_NODE_CTRL_START_FAIL", {})
                        ct:asm_verify("VERIFY_NODE_CTRL_OPERATIONAL", {}, false,
                                      "ERR_NODE_CTRL_START_FAIL", {})

                        ct:asm_one_shot_handler(
                            "WRITE_NODE_CONTAINERS_OPERATIONAL_TRUE", {})

                        ct:change_state("slave_sm", "monitor")
                        ct:asm_halt()
                    ct:end_state()

                    ct:define_state("monitor", false)
                        local loop = ct:define_while_column(
                            "slave_monitor_loop", "CFL_NEVER_TRUE")

                            ct:asm_verify("VERIFY_MASTER_HEARTBEAT_FRESH",
                                          {}, false, "ERR_MONITOR_TRIP", {})

                            ct:asm_verify("VERIFY_NODE_CTRL_HEARTBEAT_FRESH",
                                          {}, false, "ERR_MONITOR_TRIP", {})

                            ct:asm_one_shot_handler(
                                "OBSERVE_SYSTEM_READY_TO_PROCESS_GLOBALS", {})

                            ct:asm_one_shot_handler(
                                "POLL_RPC_DISPATCH_ONE", {})

                            ct:asm_one_shot_handler(
                                "PUBLISH_NODE_HEARTBEAT", {})

                            ct:asm_wait_time(1.0)
                        ct:end_column(loop)
                    ct:end_state()

                    ct:define_state("teardown", false)
                        ct:asm_one_shot_handler(
                            "WRITE_NODE_CONTAINERS_OPERATIONAL_FALSE", {})

                        ct:asm_one_shot_handler(
                            "COMMAND_NODE_CONTROL_TEARDOWN", {})
                        ct:asm_verify_timeout(15.0, true,
                                              "ERR_TEARDOWN_FORCE", {})
                        ct:asm_verify("VERIFY_NODE_CTRL_STOPPED", {}, false,
                                      "ERR_TEARDOWN_FORCE", {})

                        ct:asm_one_shot_handler("KILL_NON_SYSTEM_CONTAINERS",
                                                {})

                        ct:change_state("slave_sm", "sync")
                        ct:asm_halt()
                    ct:end_state()

                ct:end_state_machine()
            ct:end_state()

        ct:end_state_machine()

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

        ct:define_state_machine("node_sm", nil, nil)

            ct:define_state("sync", true)
                ct:asm_verify("NODE_VERIFY_BROKERS_REACHABLE", {}, false,
                              "ERR_INFRA_FAIL", {})

                ct:change_state("node_sm", "setup")
                ct:asm_halt()
            ct:end_state()

            ct:define_state("setup", false)
                ct:asm_one_shot_handler("START_ASSIGNED_CONTAINERS", {})

                ct:asm_verify_timeout(60.0, true,
                                      "ERR_CONTAINERS_START_FAIL", {})
                ct:asm_verify("VERIFY_ALL_ASSIGNED_CONTAINERS_HEALTHY",
                              {}, false, "ERR_CONTAINERS_START_FAIL", {})

                ct:asm_one_shot_handler(
                    "WRITE_PROCESS_GLOBALS_NODE_OPERATIONAL_TRUE", {})

                ct:change_state("node_sm", "monitor")
                ct:asm_halt()
            ct:end_state()

            ct:define_state("monitor", false)
                local loop = ct:define_while_column(
                    "node_monitor_loop", "CFL_NEVER_TRUE")

                    ct:asm_verify("VERIFY_ALL_ASSIGNED_CONTAINERS_HEALTHY",
                                  {}, false, "ERR_CONTAINER_DIED", {})

                    ct:asm_one_shot_handler(
                        "WRITE_PROCESS_GLOBALS_NODE_HEARTBEAT", {})

                    ct:asm_verify("VERIFY_NO_TEARDOWN_REQUEST", {}, false,
                                  "ERR_TEARDOWN_REQUESTED", {})

                    ct:asm_one_shot_handler("LOG_SYSTEM_READY_TRANSITIONS", {})

                    ct:asm_wait_time(1.0)
                ct:end_column(loop)
            ct:end_state()

            ct:define_state("teardown", false)
                ct:asm_one_shot_handler("STOP_ASSIGNED_CONTAINERS", {})

                ct:asm_verify_timeout(30.0, true,
                                      "ERR_TEARDOWN_FORCE", {})
                ct:asm_verify("VERIFY_ALL_ASSIGNED_CONTAINERS_STOPPED",
                              {}, false, "ERR_TEARDOWN_FORCE", {})

                ct:asm_one_shot_handler(
                    "WRITE_PROCESS_GLOBALS_NODE_STOPPED_TRUE", {})

                ct:change_state("node_sm", "sync")
                ct:asm_halt()
            ct:end_state()

        ct:end_state_machine()

    ct:end_column(launch)
    ct:end_test()
end

-- =============================================================================
-- Blackboard + header
-- Shared across all KBs. Intentionally minimal: all config/strings/handles
-- live in per-KB global Lua dicts (system_control_globals,
-- node_control_globals) and in cross-KB process_globals.
-- =============================================================================

local function add_header(yaml_file)
    local ct = ChainTreeMaster.new(yaml_file)

    ct:define_blackboard("dcs_bb")
        -- empty; add only if a composite node needs a scalar to dispatch on
    ct:end_blackboard()

    return ct
end

-- =============================================================================
-- Test list — ORDER DETERMINES KB INDEX
-- =============================================================================

local test_list = {
    "system_control",   -- KB 0
    "node_control",     -- KB 1
}

local test_dict = {
    system_control = system_control,
    node_control   = node_control,
}

-- =============================================================================
-- Main
-- =============================================================================

if arg then
    if #arg ~= 1 then
        print("Usage: luajit dcs.lua <json_file>")
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
    print(table.concat(kbs, ", "))
    print("total nodes", ct.ctb:get_total_node_count())
end
