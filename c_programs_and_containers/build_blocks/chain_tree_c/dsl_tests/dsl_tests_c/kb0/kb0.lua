-- KB0 — slow_bus core1 "background monitor" KB (chain_tree static-link).
--
-- Development home for the slow_bus core1 KB0 DSL; exercised natively on a Linux
-- host before embedding. The firmware vendors the static runtime + this generated
-- chain and feeds the command events from core0 via the inter-core down-queue
-- (injected to the KB start node, which the engine walks to reach every waiter).
--
-- Per-command waiter columns run concurrently at top level (each waits, handles,
-- re-arms):
--   CMD_MON_PING     -> MON_PING_REPLY   (liveness round-trip)
--   CMD_MON_SNAPSHOT -> MON_SNAPSHOT     (ack + SYS/TASKS/MEM reports + END)
-- When KB0_HOST_TEST is set, a stimulus column fires both then ends the engine.
local ChainTreeMaster = require("chain_tree_master")
local HOST_TEST = os.getenv("KB0_HOST_TEST") ~= nil

local function waiter(ct, name, event_name, handler)
    local col = ct:define_column(name, nil, nil, nil, nil, nil, true)
    ct:asm_wait_for_event(event_name, 1, true, 3600,
        "MON_CMD_TIMEOUT", "CFL_SECOND_EVENT", { error_message = "cmd timeout" })
    ct:asm_one_shot_handler(handler, {})
    ct:asm_reset()
    ct:end_column(col)
    return col
end

local function kb0(ct, name)
    ct:start_test(name)

    local pingc = waiter(ct, "cmd_ping",     "CMD_MON_PING",     "MON_PING_REPLY")
    local snapc = waiter(ct, "cmd_snapshot", "CMD_MON_SNAPSHOT", "MON_SNAPSHOT")

    if HOST_TEST then
        local stim = ct:define_column("host_stimulus", nil, nil, nil, nil, nil, true)
        ct:asm_wait_time(1.0)
        ct:asm_send_named_event(pingc, "CMD_MON_PING", { req_id = 17 })
        ct:asm_wait_time(1.0)
        ct:asm_send_named_event(snapc, "CMD_MON_SNAPSHOT", { req_id = 34 })
        ct:asm_wait_time(1.0)
        ct:asm_terminate_system()
        ct:end_column(stim)
    end

    ct:end_test()
end

local ct = ChainTreeMaster.new(arg[1])
kb0(ct, "kb0")
ct:check_and_generate_yaml()
