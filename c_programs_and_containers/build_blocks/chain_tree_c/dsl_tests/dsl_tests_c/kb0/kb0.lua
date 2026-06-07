-- KB0 — slow_bus core1 "background monitor" KB (chain_tree static-link).
--
-- Development home for the slow_bus core1 KB0 DSL; exercised natively on a Linux
-- host before embedding. The slow_bus firmware vendors the static runtime + this
-- generated chain and replaces the host stimulus with the real inter-core event
-- injection (core0 -> down-queue -> cfl_send_event at the 10 Hz tick).
--
-- This revision: the event-driven command_branch — arm on CMD_MON_PING, fire the
-- MON_PING_REPLY one-shot, re-arm. Grows into emit_branch / stream_sm / edge_watcher
-- (the full fork) next. When KB0_HOST_TEST is set, a host-only stimulus column fires
-- CMD_MON_PING twice then ends the engine, so the host test drives the dispatch.
local ChainTreeMaster = require("chain_tree_master")
local HOST_TEST = os.getenv("KB0_HOST_TEST") ~= nil

local function kb0(ct, name)
    ct:start_test(name)

    -- command_branch: dispatch host commands addressed to core1 (KB0).
    local cmd = ct:define_column("command_branch", nil, nil, nil, nil, nil, true)
    ct:asm_log_message("kb0: waiting for command")
    ct:asm_wait_for_event("CMD_MON_PING", 1, true, 3600,
        "MON_CMD_TIMEOUT", "CFL_SECOND_EVENT", { error_message = "cmd timeout" })
    ct:asm_one_shot_handler("MON_PING_REPLY", {})
    ct:asm_reset()
    ct:end_column(cmd)

    if HOST_TEST then
        -- Host-only stimulus: stand in for core0 injecting CMD_MON_PING.
        local stim = ct:define_column("host_stimulus", nil, nil, nil, nil, nil, true)
        ct:asm_wait_time(1.0)
        ct:asm_send_named_event(cmd, "CMD_MON_PING", { req_id = 17 })
        ct:asm_wait_time(1.0)
        ct:asm_send_named_event(cmd, "CMD_MON_PING", { req_id = 34 })
        ct:asm_wait_time(1.0)
        ct:asm_terminate_system()
        ct:end_column(stim)
    end

    ct:end_test()
end

local ct = ChainTreeMaster.new(arg[1])
kb0(ct, "kb0")
ct:check_and_generate_yaml()
