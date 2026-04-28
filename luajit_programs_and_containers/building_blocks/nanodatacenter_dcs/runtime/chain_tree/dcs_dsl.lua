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
-- KB 0: sync_control_master  (Phase 6.1 -- RPC-queue handshake)
--
-- Runs ONLY on the master CPU at boot. Brings up the 4 infra containers,
-- runs MASTER_SYNC_INIT (resets per-peer state map + 2s grace), then
-- enters await_active. The rpc_scheduler column drains master_q one peer
-- per tick and processes JOIN_REQ/JOIN_CONFIRM/HEARTBEAT/DRAIN verbs;
-- VERIFY_ALL_PEERS_ACTIVE returns true when every peer has reached
-- ACTIVE. Hands off to system_control + node_control and disables self.
--
-- Per feedback_one_reset_path: this is the ONE reset path. Any failure
-- in operational KBs -> watchdog restart -> re-enters this KB cleanly.
-- =============================================================================

local function sync_control_master(ct, kb_name)
    ct:start_test(kb_name)

    local launch = ct:define_column("launch", nil, nil, nil, nil, nil, true)

        ct:asm_one_shot_handler("READ_ENVIRONS",             {})
        ct:asm_one_shot_handler("READ_BOOTSTRAP_CONFIG",     {})
        ct:asm_one_shot_handler("KILL_NON_INFRA_CONTAINERS", {})

        local sync_sm = ct:define_state_machine(
            "sync_master_sm_col", "sync_master_sm",
            { "bring_up_infra", "await_active", "handoff" },
            "bring_up_infra", false)

            local bring_up_st = ct:define_state("bring_up_infra", nil)
                ct:asm_log_message("sync_master: bring_up_infra (entering)")
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

                -- Phase 6.1: open pg conn so the rpc_scheduler can
                -- start servicing master_q. Reset per-peer state map
                -- and start the 2s grace clock.
                ct:asm_one_shot_handler("OPEN_PG_CONNECTION", {})
                ct:asm_one_shot_handler("MASTER_SYNC_INIT", {})
                ct:change_state(sync_sm, "await_active")
                ct:asm_halt()
            ct:end_column(bring_up_st)

            local active_st = ct:define_state("await_active", nil)
                ct:asm_log_message("sync_master: await_active (entering)")
                -- Parallel columns: scheduler ticks alongside the wait.
                local sched_col = ct:define_column("sync_master_sched")
                    ct:asm_one_shot_handler("RPC_SCHEDULER_TICK", {})
                    ct:asm_one_shot_handler("RPC_KB_WRITEBACK_TICK", {})
                    ct:asm_wait_time(0.2)   -- 5 Hz drain
                    ct:asm_reset()
                ct:end_column(sched_col)

                local verify_col = ct:define_column("sync_master_verify")
                    -- Wait until every peer has reached ACTIVE. No
                    -- timeout: master is patient (matches old "patient-
                    -- forever" semantics). Slaves that never join
                    -- log slave_never_joined upstream; admin can use
                    -- peer_state KB rows to see who's stuck.
                    ct:asm_wait("VERIFY_ALL_PEERS_ACTIVE", {}, false,
                                0, "CFL_TIMER_EVENT",
                                "CFL_NULL", {})
                    ct:change_state(sync_sm, "handoff")
                    ct:asm_halt()
                ct:end_column(verify_col)
            ct:end_column(active_st)

            local handoff_st = ct:define_state("handoff", nil)
                ct:asm_log_message("sync_master: handoff (entering)")
                ct:asm_one_shot_handler("ENABLE_SYSTEM_CONTROL_KB", {})
                ct:asm_one_shot_handler("ENABLE_NODE_CONTROL_KB",   {})
                -- Steady-state keepalive: master drains master_q forever
                -- so live slaves see HEARTBEAT_ACK round-trips. Loses kb-
                -- self-disable; sync_control_master_kb stays enabled for
                -- the lifetime of the dcs.lua process.
                local sched_col = ct:define_column("sync_master_sched_keep")
                    ct:asm_one_shot_handler("RPC_SCHEDULER_TICK", {})
                    ct:asm_one_shot_handler("RPC_KB_WRITEBACK_TICK", {})
                    ct:asm_wait_time(0.2)
                    ct:asm_reset()
                ct:end_column(sched_col)
            ct:end_column(handoff_st)

        ct:end_state_machine(sync_sm, "sync_master_sm")

    ct:end_column(launch)
    ct:end_test()
end

-- =============================================================================
-- KB 1: sync_control_slave  (Phase 6.1 -- RPC-queue handshake)
--
-- Runs on every non-master CPU. Wait for infra reachable (master brings
-- it up); SLAVE_SEND_JOIN pushes JOIN_REQ to master_q; rpc_scheduler
-- column drains own inbox and processes JOIN_ACK / HEARTBEAT_ACK /
-- RESET_HINT; VERIFY_OWN_ACTIVE returns true once own state == ACTIVE.
--
-- The slave_heartbeat_tick column also runs in parallel and:
--   - sends HEARTBEAT every 5s ±10% jitter once we've passed JOINING
--   - increments missed_acks count when no HEARTBEAT_ACK seen
--   - calls os.exit(0) at 3 missed (watchdog respawns into wait_infra)
--
-- This is the bidirectional master-loss detection (Phase 6.3) -- a free
-- fallout of the heartbeat round-trip.
-- =============================================================================

local function sync_control_slave(ct, kb_name)
    ct:start_test(kb_name)

    local launch = ct:define_column("launch", nil, nil, nil, nil, nil, true)

        ct:asm_one_shot_handler("READ_ENVIRONS",         {})
        ct:asm_one_shot_handler("READ_BOOTSTRAP_CONFIG", {})

        local sync_sm = ct:define_state_machine(
            "sync_slave_sm_col", "sync_slave_sm",
            { "wait_infra", "join", "handoff" },
            "wait_infra", false)

            local wait_infra_st = ct:define_state("wait_infra", nil)
                ct:asm_log_message("sync_slave: wait_infra (entering)")
                -- Patient polling: asm_wait re-checks VERIFY_ALL_INFRA_
                -- REACHABLE every TIMER tick. Master takes seconds to
                -- bring up infra in practice. ERR fires only at 24h cap.
                ct:asm_wait("VERIFY_ALL_INFRA_REACHABLE", {}, false,
                            86400, "CFL_TIMER_EVENT",
                            "ERR_INFRA_FAIL", {})
                ct:asm_one_shot_handler("OPEN_PG_CONNECTION", {})
                ct:asm_one_shot_handler("SLAVE_SYNC_INIT", {})
                ct:asm_one_shot_handler("SLAVE_SEND_JOIN", {})
                ct:change_state(sync_sm, "join")
                ct:asm_halt()
            ct:end_column(wait_infra_st)

            local join_st = ct:define_state("join", nil)
                ct:asm_log_message("sync_slave: join (entering)")
                -- Three parallel columns: scheduler drains inbox,
                -- heartbeat sends + missed-ACK detect, verify waits
                -- for own state == ACTIVE.
                local sched_col = ct:define_column("sync_slave_sched")
                    ct:asm_one_shot_handler("RPC_SCHEDULER_TICK", {})
                    ct:asm_one_shot_handler("RPC_KB_WRITEBACK_TICK", {})
                    ct:asm_wait_time(0.2)   -- 5 Hz drain
                    ct:asm_reset()
                ct:end_column(sched_col)

                local hb_col = ct:define_column("sync_slave_hb")
                    ct:asm_one_shot_handler("SLAVE_HEARTBEAT_TICK", {})
                    ct:asm_wait_time(1.0)
                    ct:asm_reset()
                ct:end_column(hb_col)

                local verify_col = ct:define_column("sync_slave_verify")
                    -- Wait for ACTIVE. No timeout: SLAVE_HEARTBEAT_TICK
                    -- handles fail-stop on missed-ACK threshold by
                    -- exiting the process; watchdog respawns us back
                    -- into wait_infra.
                    ct:asm_wait("VERIFY_OWN_ACTIVE", {}, false,
                                0, "CFL_TIMER_EVENT",
                                "CFL_NULL", {})
                    ct:change_state(sync_sm, "handoff")
                    ct:asm_halt()
                ct:end_column(verify_col)
            ct:end_column(join_st)

            local handoff_st = ct:define_state("handoff", nil)
                ct:asm_log_message("sync_slave: handoff (entering)")
                ct:asm_one_shot_handler("ENABLE_NODE_CONTROL_KB", {})
                -- Steady-state keepalive: HEARTBEAT every 5s ±10%; 3 missed
                -- ACKs trigger os.exit(0) inside SLAVE_HEARTBEAT_TICK and
                -- the watchdog respawns into wait_infra. This is the one-
                -- reset-path master-loss detector promised at the top of
                -- this file. Loses kb-self-disable; sync_control_slave_kb
                -- stays enabled for the lifetime of the dcs.lua process.
                local sched_col = ct:define_column("sync_slave_sched_keep")
                    ct:asm_one_shot_handler("RPC_SCHEDULER_TICK", {})
                    ct:asm_one_shot_handler("RPC_KB_WRITEBACK_TICK", {})
                    ct:asm_wait_time(0.2)
                    ct:asm_reset()
                ct:end_column(sched_col)

                local hb_col = ct:define_column("sync_slave_hb_keep")
                    ct:asm_one_shot_handler("SLAVE_HEARTBEAT_TICK", {})
                    ct:asm_wait_time(1.0)
                    ct:asm_reset()
                ct:end_column(hb_col)
            ct:end_column(handoff_st)

        ct:end_state_machine(sync_sm, "sync_slave_sm")

    ct:end_column(launch)
    ct:end_test()
end

-- =============================================================================
-- KB 2: system_control
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
            { "sync", "setup", "monitor", "restoring_infra",
              "request_shutdown", "teardown" },
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

                -- Phase 6.4: container-layer RPC scheduler. Drains
                -- container_inbox_<cpu_id>_q at 5 Hz, dispatches
                -- CONTAINER_READY/HEARTBEAT, runs missed-HB scan +
                -- two-tier escalation, flushes outbox. Master-only;
                -- the handlers no-op on non-master CPUs.
                local crpc_col = ct:define_column("sys_monitor_crpc")
                    ct:asm_one_shot_handler("CONTAINER_RPC_SCHEDULER_TICK",   {})
                    ct:asm_one_shot_handler("CONTAINER_RPC_KB_WRITEBACK_TICK",{})
                    ct:asm_wait_time(0.2)
                    ct:asm_reset()
                ct:end_column(crpc_col)

                local verify_col = ct:define_column("sys_monitor_verify")
                    ct:asm_wait_time(5.0)   -- settle before first verify
                    ct:asm_verify("VERIFY_NODE_CTRL_HEARTBEAT_FRESH", {}, false,
                                  "ERR_MONITOR_TRIP", {})
                    -- Phase 6.2: infra trip routes to restoring_infra
                    -- (per-node restart) instead of ERR_MONITOR_TRIP
                    -- (full teardown). Escalation back to teardown
                    -- happens after MAX_INFRA_RETRIES consecutive
                    -- failures via ERR_INFRA_RESTART_FAILED.
                    ct:asm_verify("VERIFY_SYSTEM_CONTAINERS_HEALTHY", {}, false,
                                  "ERR_INFRA_TRIP", {})
                    ct:asm_halt()
                ct:end_column(verify_col)
            ct:end_column(monitor_st)

            -- restoring_infra: identify failing infra container(s) via
            -- broker snapshot, restart each via broker (stop+start),
            -- wait 5s, re-verify. On success: reset retry counter and
            -- return to monitor. On failure: ERR_INFRA_RETRY handler
            -- bumps the retry counter and either re-enters this state
            -- (count < MAX_INFRA_RETRIES) or escalates to
            -- request_shutdown via ERR_INFRA_RESTART_FAILED.
            local restoring_infra_st = ct:define_state("restoring_infra", nil)
                ct:asm_log_message("sys state: restoring_infra (entering)")
                ct:asm_one_shot_handler("IDENTIFY_FAILED_INFRA", {})
                ct:asm_one_shot_handler("RESTART_FAILED_INFRA",  {})
                ct:asm_wait_time(5.0)
                ct:asm_verify("VERIFY_SYSTEM_CONTAINERS_HEALTHY", {}, false,
                              "ERR_INFRA_RETRY", {})
                ct:asm_one_shot_handler("RESET_INFRA_RETRY", {})
                ct:asm_log_message("sys state: restoring_infra -> monitor (recovered)")
                ct:change_state(sys_sm, "monitor")
                ct:asm_halt()
            ct:end_column(restoring_infra_st)

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

                -- Every CPU sets its own operational ready_bit here (not
                -- in system_control, which runs master-only). Master's
                -- system_control VERIFY_ALL_CPUS_READY then gates on the
                -- full mask before flipping system_ready.
                ct:asm_one_shot_handler("SET_OWN_READY_BIT", {})

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

                -- Phase 7b / X7: every 5s, diff each assignment's
                -- pg-declared maintenance_until against process-local
                -- last-seen; stop+deregister on enter, docker-run+
                -- register on exit (lease expiry or operator Start Now).
                local maint_col = ct:define_column("node_monitor_maint")
                    ct:asm_one_shot_handler(
                        "APPLY_MAINTENANCE_TRANSITIONS", {})
                    ct:asm_wait_time(5.0)
                    ct:asm_reset()
                ct:end_column(maint_col)

                -- Reconcile: cold-respawn any assigned container that has
                -- disappeared (docker rm, crash, OOM-kill). Wait THEN
                -- check so setup-phase starts can settle and a freshly
                -- respawned container has time to come up before the
                -- next pass. 10s cadence.
                local reconcile_col = ct:define_column("node_monitor_reconcile")
                    ct:asm_wait_time(10.0)
                    ct:asm_one_shot_handler(
                        "RECONCILE_ASSIGNED_CONTAINERS", {})
                    ct:asm_reset()
                ct:end_column(reconcile_col)

                -- Watchdog: HTTP-probe each running container's primary
                -- external port. 3 consecutive strikes -> docker stop +
                -- respawn + SYS_EXCEPTION container_hung. 15s cadence
                -- (≈45s worst-case detection). Wait-then-check for the
                -- same settle reason as reconcile.
                local watchdog_col = ct:define_column("node_monitor_watchdog")
                    ct:asm_wait_time(15.0)
                    ct:asm_one_shot_handler(
                        "WATCHDOG_CHECK_ASSIGNED_CONTAINERS", {})
                    ct:asm_reset()
                ct:end_column(watchdog_col)

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
                -- Symmetric cleanup of the bits this CPU owns so a
                -- watchdog-restart re-enters sync cleanly. Phase 6.1:
                -- cluster_sync_bit removed (replaced by RPC handshake);
                -- ready_bit retained (orthogonal to sync, gates
                -- system_ready aggregation).
                ct:asm_one_shot_handler("CLEAR_OWN_READY_BIT", {})
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

-- KB order matters for indexing; the runtime looks up by name though, so
-- reshuffling is safe as long as the bundled controller.db is rebuilt.
local test_list = {
    "sync_control_master",
    "sync_control_slave",
    "system_control",
    "node_control",
    "node_monitor",
}
local test_dict = {
    sync_control_master = sync_control_master,
    sync_control_slave  = sync_control_slave,
    system_control      = system_control,
    node_control        = node_control,
    node_monitor        = node_monitor,
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
