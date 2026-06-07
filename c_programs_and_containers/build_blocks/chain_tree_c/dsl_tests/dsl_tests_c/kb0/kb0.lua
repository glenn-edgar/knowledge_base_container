-- KB0 — slow_bus core1 "background monitor" KB (chain_tree static-link).
--
-- This is the development home for the slow_bus core1 KB0 DSL. The slow_bus
-- firmware vendors the static runtime + this generated chain; this test exercises
-- the KB logic natively on a Linux host before embedding.
--
-- Milestone 1 (this revision): prove the DSL->json->C toolchain + user-fn binding
-- end to end. A single column logs, fires the MON_PING_REPLY one-shot, then
-- terminates the engine so the host run loop returns. (Grows into the event-driven
-- command_branch / emit_branch / stream_sm next.)
local ChainTreeMaster = require("chain_tree_master")

local function kb0(ct, name)
    ct:start_test(name)
    local c = ct:define_column("ping_test", nil, nil, nil, nil, nil, true)
    ct:asm_log_message("kb0 start")
    ct:asm_one_shot_handler("MON_PING_REPLY", { req_id = 4660 })   -- 0x1234
    ct:asm_log_message("kb0 ping handled")
    ct:asm_terminate_system()
    ct:end_column(c)
    ct:end_test()
end

local ct = ChainTreeMaster.new(arg[1])
kb0(ct, "kb0")
ct:check_and_generate_yaml()
