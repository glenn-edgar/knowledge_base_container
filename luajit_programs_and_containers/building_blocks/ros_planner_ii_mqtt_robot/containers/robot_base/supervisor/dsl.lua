-- =============================================================================
-- dsl.lua -- robot_base controller chain-tree DSL.
--
-- Single KB, single state machine "robot_sm":
--   sync               VALIDATE_ENV  (fail-stop on missing)
--   render_config      RENDER_CONFIG (template -> /run/robot/config.json)
--   spawn_sim          SPAWN_ROBOT_SIM (capture stdout pipe)
--   wait_for_ready     parse PTY=/dev/pts/N then READY from sim stdout
--   spawn_mqtt_robot   SPAWN_MQTT_ROBOT with ROBOT_SIM_PTY env
--   register_peer      REGISTER_WITH_PEER (Phase 1 = no-op)
--   monitor            parallel reset-loop columns:
--                        liveness_col      reap children, fail-stop
--                        shutdown_watch    SIGTERM/SIGINT poll
--                        peer_tick         upward_peer:tick (Phase 2 hook)
--   request_shutdown   SIGTERM all children, wait
--   teardown           SIGKILL stragglers, terminate_system
--
-- Compile: `luajit dsl.lua robot_supervisor.json`
-- =============================================================================

local ChainTreeMaster = require("chain_tree_master")

local function controller(ct, kb_name)
    ct:start_test(kb_name)

    local launch = ct:define_column("launch", nil, nil, nil, nil, nil, true)
        local sm = ct:define_state_machine(
            "robot_sm_col", "robot_sm",
            { "sync", "render_config", "spawn_sim", "wait_for_ready",
              "spawn_mqtt_robot", "register_peer", "monitor",
              "request_shutdown", "teardown" },
            "sync", false)

            local sync_st = ct:define_state("sync", nil)
                ct:asm_log_message("robot state: sync (validate env)")
                ct:asm_one_shot_handler("VALIDATE_ENV", {})
                ct:change_state(sm, "render_config")
                ct:asm_halt()
            ct:end_column(sync_st)

            local rc_st = ct:define_state("render_config", nil)
                ct:asm_log_message("robot state: render_config")
                ct:asm_one_shot_handler("RENDER_CONFIG", {})
                ct:change_state(sm, "spawn_sim")
                ct:asm_halt()
            ct:end_column(rc_st)

            local ss_st = ct:define_state("spawn_sim", nil)
                ct:asm_log_message("robot state: spawn_sim")
                ct:asm_one_shot_handler("SPAWN_ROBOT_SIM", {})
                ct:change_state(sm, "wait_for_ready")
                ct:asm_halt()
            ct:end_column(ss_st)

            -- wait_for_ready: poll the sim stdout pipe each tick until we
            -- have parsed both PTY=… and READY. asm_wait halts while
            -- VERIFY_SIM_READY is false, advances when true, fires
            -- ERR_SIM_NOT_READY after 30 timer events (~15s @ 0.5s/tick).
            -- (asm_verify advances on bool=true even at INIT; we want
            -- halting behavior, so use asm_wait.)
            local wfr_st = ct:define_state("wait_for_ready", nil)
                ct:asm_log_message("robot state: wait_for_ready")
                ct:asm_wait("VERIFY_SIM_READY", {}, false,
                            30, "CFL_TIMER_EVENT",
                            "ERR_SIM_NOT_READY", {})
                ct:change_state(sm, "spawn_mqtt_robot")
                ct:asm_halt()
            ct:end_column(wfr_st)

            local smr_st = ct:define_state("spawn_mqtt_robot", nil)
                ct:asm_log_message("robot state: spawn_mqtt_robot")
                ct:asm_one_shot_handler("SPAWN_MQTT_ROBOT", {})
                ct:change_state(sm, "register_peer")
                ct:asm_halt()
            ct:end_column(smr_st)

            local rp_st = ct:define_state("register_peer", nil)
                ct:asm_log_message("robot state: register_peer (Phase 1 stub)")
                ct:asm_one_shot_handler("REGISTER_WITH_PEER", {})
                ct:change_state(sm, "monitor")
                ct:asm_halt()
            ct:end_column(rp_st)

            local mon_st = ct:define_state("monitor", nil)
                ct:asm_log_message("robot state: monitor (entering)")

                -- Liveness: any child death is fatal (feedback_no_soft_faults).
                local liveness_col = ct:define_column("robot_liveness_col")
                    ct:asm_one_shot_handler("DRAIN_SIM_STDOUT", {})
                    ct:asm_verify("VERIFY_CHILDREN_ALIVE", {}, false,
                                  "ERR_CHILD_DIED", {})
                    ct:asm_wait_time(1.0)
                    ct:asm_reset()
                ct:end_column(liveness_col)

                -- Signal watcher.
                local shutdown_col = ct:define_column("robot_shutdown_watch")
                    ct:asm_wait_time(0.5)
                    ct:asm_verify("VERIFY_NO_SHUTDOWN_SIGNAL", {}, false,
                                  "ERR_TEARDOWN_REQUESTED", {})
                    ct:asm_reset()
                ct:end_column(shutdown_col)

                -- Phase 2 hook (no-op today).
                local peer_col = ct:define_column("robot_peer_col")
                    ct:asm_one_shot_handler("UPWARD_PEER_TICK", {})
                    ct:asm_wait_time(2.0)
                    ct:asm_reset()
                ct:end_column(peer_col)
            ct:end_column(mon_st)

            local rsd_st = ct:define_state("request_shutdown", nil)
                ct:asm_log_message("robot state: request_shutdown (entering)")
                ct:asm_one_shot_handler("SIGTERM_ALL_CHILDREN", {})
                ct:asm_wait("VERIFY_ALL_CHILDREN_EXITED", {}, false,
                            10, "CFL_TIMER_EVENT",
                            "ERR_FORCE_TEARDOWN", {})
                ct:change_state(sm, "teardown")
                ct:asm_halt()
            ct:end_column(rsd_st)

            local td_st = ct:define_state("teardown", nil)
                ct:asm_log_message("robot state: teardown (entering)")
                ct:asm_one_shot_handler("SIGKILL_ALL_CHILDREN", {})
                ct:asm_one_shot_handler("UPWARD_PEER_SHUTDOWN", {})
                ct:asm_terminate_system()
            ct:end_column(td_st)

        ct:end_state_machine(sm, "robot_sm")
    ct:end_column(launch)

    ct:end_test()
end

local function add_header(output_file)
    local ct = ChainTreeMaster.new(output_file)
    ct:define_blackboard("robot_bb")
        ct:bb_field("placeholder", "uint16", 0)
    ct:end_blackboard()
    return ct
end

if arg then
    if #arg ~= 1 then
        print("Usage: luajit dsl.lua <json_file>")
        os.exit(1)
    end
    local ct = add_header(arg[1])
    controller(ct, "robot_supervisor")
    ct:check_and_generate_yaml()
    ct:generate_debug_yaml()
    ct:display_chain_tree_function_mapping()
    print(table.concat(ct:list_kbs(), ", "))
    print("total nodes", ct.ctb:get_total_node_count())
end
