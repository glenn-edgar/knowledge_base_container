-- =============================================================================
-- dsl.lua -- luajit-base controller chain-tree DSL.
--
-- Single KB, single state machine, five states (same shape as DCS sys_sm):
--   sync       open pg, load command_map from controller sqlite
--   setup      spawn all apps in start_order; verify each pid is alive
--   monitor    parallel columns: hb_col (strobe) + liveness_col (reap+respawn)
--                + teardown_watch_col (SIGTERM flag -> ERR_TEARDOWN_REQUESTED)
--   request_shutdown   SIGTERM all apps; wait for exits or timeout
--   teardown   SIGKILL stragglers, close connections, terminate_system
--
-- Compile: `luajit dsl.lua controller.json`. The generated IR is loaded
-- at container boot by entrypoint.lua.
-- =============================================================================

local ChainTreeMaster = require("chain_tree_master")

local function controller(ct, kb_name)
    ct:start_test(kb_name)

    local launch = ct:define_column("launch", nil, nil, nil, nil, nil, true)
        ct:asm_one_shot_handler("READ_ENVIRONS",              {})
        ct:asm_one_shot_handler("LOAD_CONTROLLER_KB",         {})

        local sm = ct:define_state_machine(
            "ctrl_sm_col", "ctrl_sm",
            { "sync", "setup", "monitor", "request_shutdown", "teardown" },
            "sync", false)

            -- sync: pg up, command_map loaded
            local sync_st = ct:define_state("sync", nil)
                ct:asm_log_message("ctrl state: sync (entering)")
                ct:asm_verify_timeout(60.0, true, "ERR_PG_UNREACHABLE", {})
                ct:asm_verify("VERIFY_PG", {}, false, "ERR_PG_UNREACHABLE", {})
                ct:asm_one_shot_handler("LOAD_COMMAND_MAP", {})
                ct:asm_log_message("ctrl state: sync -> setup")
                ct:change_state(sm, "setup")
                ct:asm_halt()
            ct:end_column(sync_st)

            -- setup: spawn apps and confirm alive.
            local setup_st = ct:define_state("setup", nil)
                ct:asm_log_message("ctrl state: setup (entering)")
                ct:asm_one_shot_handler("SPAWN_ALL_APPS", {})
                ct:asm_wait_time(2.0)
                ct:asm_verify_timeout(30.0, true,
                                      "ERR_APPS_START_FAIL", {})
                ct:asm_verify("VERIFY_ALL_APPS_ALIVE", {}, false,
                              "ERR_APPS_START_FAIL", {})
                ct:asm_one_shot_handler("WRITE_CONTAINER_HEALTH_TRUE", {})
                ct:asm_log_message("ctrl state: setup -> monitor")
                ct:change_state(sm, "monitor")
                ct:asm_halt()
            ct:end_column(setup_st)

            -- monitor: two parallel reset-loop columns.
            -- (A heartbeat column will reappear once bit_mask_table grows
            --  a per-container heartbeat bit; pruned for now since the
            --  previous STROBE_HEARTBEAT only updated an in-process var.)
            local monitor_st = ct:define_state("monitor", nil)
                ct:asm_log_message("ctrl state: monitor (entering)")

                local liveness_col = ct:define_column("ctrl_liveness_col")
                    ct:asm_one_shot_handler("REAP_AND_RESPAWN", {})
                    ct:asm_wait_time(2.0)
                    ct:asm_reset()
                ct:end_column(liveness_col)

                -- Signal watcher: if SIGTERM/SIGINT was caught, advance to
                -- request_shutdown via ERR_TEARDOWN_REQUESTED.
                local shutdown_col = ct:define_column("ctrl_shutdown_watch")
                    ct:asm_wait_time(1.0)
                    ct:asm_verify("VERIFY_NO_SHUTDOWN_SIGNAL", {}, false,
                                  "ERR_TEARDOWN_REQUESTED", {})
                    ct:asm_reset()
                ct:end_column(shutdown_col)
            ct:end_column(monitor_st)

            -- request_shutdown: cooperative stop of apps.
            local request_shutdown_st = ct:define_state("request_shutdown", nil)
                ct:asm_log_message("ctrl state: request_shutdown (entering)")
                ct:asm_one_shot_handler("SIGTERM_ALL_APPS", {})
                ct:asm_wait("VERIFY_ALL_APPS_EXITED", {}, false,
                            20, "CFL_TIMER_EVENT",
                            "ERR_FORCE_TEARDOWN", {})
                ct:asm_log_message("ctrl state: request_shutdown -> teardown")
                ct:change_state(sm, "teardown")
                ct:asm_halt()
            ct:end_column(request_shutdown_st)

            -- teardown: SIGKILL stragglers, close conns, exit.
            local teardown_st = ct:define_state("teardown", nil)
                ct:asm_log_message("ctrl state: teardown (entering)")
                ct:asm_one_shot_handler("SIGKILL_ALL_APPS",         {})
                ct:asm_one_shot_handler("WRITE_CONTAINER_HEALTH_FALSE", {})
                ct:asm_one_shot_handler("CLOSE_CONNECTIONS",        {})
                ct:asm_terminate_system()
            ct:end_column(teardown_st)

        ct:end_state_machine(sm, "ctrl_sm")
    ct:end_column(launch)

    ct:end_test()
end

---------------------------------------------------------------------------
-- Blackboard header + main
---------------------------------------------------------------------------

local function add_header(output_file)
    local ct = ChainTreeMaster.new(output_file)
    ct:define_blackboard("ctrl_bb")
        ct:bb_field("placeholder", "uint16", 0)
    ct:end_blackboard()
    return ct
end

if arg then
    if #arg ~= 1 then
        print("Usage: luajit dsl.lua <json_file>")
        os.exit(1)
    end
    local json_file = arg[1]
    local ct = add_header(json_file)
    controller(ct, "controller")
    ct:check_and_generate_yaml()
    ct:generate_debug_yaml()
    ct:display_chain_tree_function_mapping()
    print(table.concat(ct:list_kbs(), ", "))
    print("total nodes", ct.ctb:get_total_node_count())
end
