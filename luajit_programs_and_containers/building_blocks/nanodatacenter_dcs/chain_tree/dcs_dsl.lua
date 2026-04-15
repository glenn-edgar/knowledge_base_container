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

        -- ct:asm_log_message("sys launch: entering")
        ct:asm_one_shot_handler("READ_ENVIRONS",             {})
        ct:asm_one_shot_handler("READ_BOOTSTRAP_CONFIG",     {})
        ct:asm_one_shot_handler("KILL_NON_INFRA_CONTAINERS", {})
        ct:asm_one_shot_handler("SET_SYSTEM_STATE",          {})

        -- auto_start=false: state machine's INIT selectively enables only
        -- the initial state child. Setting true would trip AUTO_START_BIT
        -- in the engine (cfl_engine.lua execute_node) and enable ALL state
        -- children at once, making every state run simultaneously.
        local sys_sm = ct:define_state_machine(
            "sys_sm_col", "sys_sm",
            { "sync", "setup", "monitor", "request_shutdown", "teardown" },
            "sync", false)

            local sync_st = ct:define_state("sync", nil)
                ct:asm_log_message("sys state: sync (entering)")
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

                ct:asm_log_message("sys state: sync -> setup (change_state)")
                ct:change_state(sys_sm, "setup")
                ct:asm_halt()
            ct:end_column(sync_st)

            local setup_st = ct:define_state("setup", nil)
                ct:asm_log_message("sys state: setup (entering)")
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

                ct:asm_log_message("sys state: setup -> monitor (change_state)")
                ct:change_state(sys_sm, "monitor")
                ct:asm_halt()
            ct:end_column(setup_st)

            local monitor_st = ct:define_state("monitor", nil)
                ct:asm_log_message("sys state: monitor (entering)")
                -- Parallel columns under monitor_st:
                --   hb_col     — periodic heartbeat publisher (reset-loop,
                --                5s cadence)
                --   verify_col — verifies run every tick after a 5s settle;
                --                asm_halt pins the walker so siblings don't
                --                re-fire.
                -- Fresh window (ctx.settings.heartbeat_fresh_s) is 10s —
                -- twice the 5s publish cadence so a normal tick can't race.
                local hb_col = ct:define_column("sys_monitor_hb")
                    ct:asm_one_shot_handler("PUBLISH_SYSTEM_HEARTBEAT", {})
                    ct:asm_wait_time(5.0)
                    ct:asm_reset()
                ct:end_column(hb_col)

                local verify_col = ct:define_column("sys_monitor_verify")
                    ct:asm_wait_time(5.0)   -- settle before first verify
                    ct:asm_verify("VERIFY_NODE_CTRL_HEARTBEAT_FRESH", {}, false,
                                  "ERR_MONITOR_TRIP", {})
                    ct:asm_verify("VERIFY_SYSTEM_CONTAINERS_HEALTHY", {}, false,
                                  "ERR_MONITOR_TRIP", {})
                    ct:asm_halt()
                ct:end_column(verify_col)
            ct:end_column(monitor_st)

            -- request_shutdown: cooperative pause between monitor and
            -- teardown. Posts the shutdown request to node_control (and
            -- to remote CPUs in future) and waits for them to confirm
            -- they've stopped their app containers. Both success and
            -- timeout converge on teardown:
            --   wait passes -> change_state(teardown)
            --   wait times out -> ERR_FORCE_TEARDOWN -> change_state(teardown)
            -- Salient chain-tree property: all events generated within a
            -- tick are drained before the next TIMER_EVENT, so the
            -- change_state event from ERR_FORCE_TEARDOWN always wins
            -- over SM_MAIN seeing an orphaned active_child.
            local request_shutdown_st = ct:define_state("request_shutdown", nil)
                ct:asm_log_message("sys state: request_shutdown (entering)")
                ct:asm_one_shot_handler("POST_SHUTDOWN_REQUEST", {})
                -- timeout in TIMER_EVENT counts (~ ticks). At the default
                -- 1s outer interval, 30 ticks ≈ 30s grace.
                ct:asm_wait("VERIFY_ALL_NODES_SHUTDOWN", {}, false,
                            30, "CFL_TIMER_EVENT",
                            "ERR_FORCE_TEARDOWN", {})
                ct:asm_log_message("sys state: request_shutdown -> teardown (acked)")
                ct:change_state(sys_sm, "teardown")
                ct:asm_halt()
            ct:end_column(request_shutdown_st)

            local teardown_st = ct:define_state("teardown", nil)
                ct:asm_log_message("sys state: teardown (entering)")
                -- Kill local node_control KB first; remote CPUs already
                -- acked (or timed out) in request_shutdown.
                ct:asm_one_shot_handler("DISABLE_NODE_CONTROL_KB",   {})
                ct:asm_one_shot_handler("WRITE_SYSTEM_READY_FALSE", {})
                ct:asm_one_shot_handler("CLEAR_OWN_READY_BIT", {})

                ct:asm_one_shot_handler("STOP_KV_BRIDGE_CONTAINER",  {})
                ct:asm_one_shot_handler("STOP_MQTT_CONTAINER",       {})
                ct:asm_one_shot_handler("STOP_NATS_CONTAINER",       {})
                ct:asm_one_shot_handler("STOP_PG_CONTAINER",         {})

                ct:asm_log_message("sys state: teardown (exiting)")
                ct:asm_terminate_system()
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

        -- ct:asm_log_message("node launch: entering")
        ct:asm_one_shot_handler("NODE_READ_OWN_CONFIG", {})

        local node_sm = ct:define_state_machine(
            "node_sm_col", "node_sm",
            { "sync", "setup", "monitor", "teardown" },
            "sync", false)    -- see sys_sm comment above

            local sync_st = ct:define_state("sync", nil)
                ct:asm_log_message("node state: sync (entering)")
                ct:asm_verify("NODE_VERIFY_BROKERS_REACHABLE", {}, false,
                              "ERR_INFRA_FAIL", {})
                ct:asm_log_message("node state: sync -> setup (change_state)")
                ct:change_state(node_sm, "setup")
                ct:asm_halt()
            ct:end_column(sync_st)

            local setup_st = ct:define_state("setup", nil)
                ct:asm_log_message("node state: setup (entering)")
                ct:asm_one_shot_handler("START_ASSIGNED_CONTAINERS", {})
                ct:asm_verify_timeout(60.0, true,
                                      "ERR_CONTAINERS_START_FAIL", {})
                ct:asm_verify("VERIFY_ALL_ASSIGNED_CONTAINERS_HEALTHY", {}, false,
                              "ERR_CONTAINERS_START_FAIL", {})

                ct:asm_one_shot_handler(
                    "WRITE_PROCESS_GLOBALS_NODE_OPERATIONAL_TRUE", {})

                -- Activate node_monitor KB AFTER apps are healthy. This
                -- way the resource sampler captures the operational
                -- baseline. Deactivation is the LAST step in teardown_st
                -- so the sampler covers the teardown sequence too.
                ct:asm_one_shot_handler("ENABLE_NODE_MONITOR_KB", {})

                ct:asm_log_message("node state: setup -> monitor (change_state)")
                ct:change_state(node_sm, "monitor")
                ct:asm_halt()
            ct:end_column(setup_st)

            local monitor_st = ct:define_state("monitor", nil)
                ct:asm_log_message("node state: monitor (entering)")
                -- Parallel columns under monitor_st:
                --   hb_col     — heartbeat publisher (5s reset-loop)
                --   verify_col — settle 5s, then verifies + asm_halt
                -- LOG_SYSTEM_READY_TRANSITIONS intentionally omitted until
                -- it has a real implementation (stub today).
                local hb_col = ct:define_column("node_monitor_hb")
                    ct:asm_one_shot_handler(
                        "WRITE_PROCESS_GLOBALS_NODE_HEARTBEAT", {})
                    ct:asm_wait_time(5.0)
                    ct:asm_reset()
                ct:end_column(hb_col)

                local verify_col = ct:define_column("node_monitor_verify")
                    ct:asm_wait_time(5.0)   -- settle before first verify
                    ct:asm_verify("VERIFY_ALL_ASSIGNED_CONTAINERS_HEALTHY",
                                  {}, false, "ERR_CONTAINER_DIED", {})
                    ct:asm_verify("VERIFY_NO_TEARDOWN_REQUEST", {}, false,
                                  "ERR_TEARDOWN_REQUESTED", {})
                    ct:asm_halt()
                ct:end_column(verify_col)
            ct:end_column(monitor_st)

            local teardown_st = ct:define_state("teardown", nil)
                ct:asm_log_message("node state: teardown (entering)")
                ct:asm_one_shot_handler("STOP_ASSIGNED_CONTAINERS", {})
                ct:asm_verify_timeout(30.0, true, "ERR_TEARDOWN_FORCE", {})
                ct:asm_verify("VERIFY_ALL_ASSIGNED_CONTAINERS_STOPPED",
                              {}, false, "ERR_TEARDOWN_FORCE", {})
                ct:asm_one_shot_handler(
                    "WRITE_PROCESS_GLOBALS_NODE_STOPPED_TRUE", {})
                -- Stop the sampler LAST so it covers the teardown.
                ct:asm_one_shot_handler("DISABLE_NODE_MONITOR_KB", {})
                -- Exit cleanly; watchdog restarts from sync.
                ct:asm_terminate_system()
            ct:end_column(teardown_st)

        ct:end_state_machine(node_sm, "node_sm")

    ct:end_column(launch)
    ct:end_test()
end

-- =============================================================================
-- KB 2: node_monitor
--
-- Resource sampler. Activated by node_control's setup_st AFTER apps are
-- healthy; deactivated as the LAST step of node_control's teardown_st so
-- the sampler's history covers the teardown sequence.
--
-- Failure isolation: monitor verifies do NOT advance any state machine.
-- A flaky sampler logs and continues. Ground rule: monitor KBs are
-- best-effort; supervision KBs are cooperative.
--
-- Tick discipline: each sampler is a single-fire one-shot doing a
-- handful of /proc + cgroup reads (microseconds) and one pg insert.
-- Total well under the chain-tree tick budget. If container count
-- grows large enough that one container_sample tick costs too much,
-- split SAMPLE_CONTAINERS into per-container one-shots stepped through
-- across ticks.
-- =============================================================================

local function node_monitor(ct, kb_name)
    ct:start_test(kb_name)

    local launch = ct:define_column("launch", nil, nil, nil, nil, nil, true)

        local mon_sm = ct:define_state_machine(
            "mon_sm_col", "mon_sm",
            { "sync", "monitor" },
            "sync", false)

            local sync_st = ct:define_state("sync", nil)
                ct:asm_log_message("monitor state: sync (entering)")
                ct:asm_one_shot_handler("MONITOR_DISCOVER_CGROUPS", {})
                ct:asm_one_shot_handler("MONITOR_INIT_STATE",       {})
                ct:asm_log_message("monitor state: sync -> monitor (change_state)")
                ct:change_state(mon_sm, "monitor")
                ct:asm_halt()
            ct:end_column(sync_st)

            local monitor_st = ct:define_state("monitor", nil)
                ct:asm_log_message("monitor state: monitor (entering)")

                -- Each parallel column is a reset-loop with its own cadence.
                -- Default 60s for samples; 5 min for trend recompute.
                -- Cadences pulled from ctx.settings inside the user fns
                -- would be cleaner; left as wait_time literals for now.

                local host_col = ct:define_column("mon_host_col")
                    ct:asm_one_shot_handler("SAMPLE_HOST", {})
                    ct:asm_wait_time(60.0)
                    ct:asm_reset()
                ct:end_column(host_col)

                local proc_col = ct:define_column("mon_proc_col")
                    ct:asm_one_shot_handler(
                        "SAMPLE_SYSTEM_CONTROL_PROCESS", {})
                    ct:asm_wait_time(60.0)
                    ct:asm_reset()
                ct:end_column(proc_col)

                local cont_col = ct:define_column("mon_cont_col")
                    ct:asm_one_shot_handler("SAMPLE_CONTAINERS", {})
                    ct:asm_wait_time(60.0)
                    ct:asm_reset()
                ct:end_column(cont_col)

                local trend_col = ct:define_column("mon_trend_col")
                    ct:asm_one_shot_handler("COMPUTE_TRENDS", {})
                    ct:asm_wait_time(300.0)   -- 5 min
                    ct:asm_reset()
                ct:end_column(trend_col)
            ct:end_column(monitor_st)

        ct:end_state_machine(mon_sm, "mon_sm")

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

local test_list = { "system_control", "node_control", "node_monitor" }
local test_dict = {
    system_control = system_control,
    node_control   = node_control,
    node_monitor   = node_monitor,
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
