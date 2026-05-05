--[[
    common_tree.lua -- Common KB tree builder for hub virtual nodes.

    All hub KBs share this tree structure:
      1. One-shot: generate AVRC packet from current_test_json
      2. Event handler: HUB_ACK_HANDLER (wait for ack, timeout → error)
      3. Event handler: HUB_BITMAP_HANDLER (parse per-KB bitmask)
      4. Event handler: HUB_KB_DONE_HANDLER (wait for kb_done, timeout → error)

    The handlers stay alive at CFL_CONTINUE until their event arrives.
    HUB_KB_DONE_HANDLER returns CFL_DISABLE on receipt → column ends.
    Timeouts return CFL_TERMINATE → exception handling / error_recovery KB.

    KB plugins call: common_tree.build(ct, kb_name, one_shot_name, plugin)
]]

local M = {}

function M.build(ct, kb_name, one_shot_name, plugin)
    ct:start_test(kb_name)

    local col = ct:define_column(kb_name .. "_main", nil, nil, nil, nil, {}, true)

        -- Phase 1: Generate and send AVRC command packet
        ct:asm_one_shot_handler(one_shot_name,
            {"current_test_json", "command_packet"})

        -- Phase 2: Wait for ack from remote
        ct:define_column_link(
            "HUB_ACK_HANDLER", "HUB_ACK_INIT",
            "CFL_NULL", "CFL_NULL",
            { timeout_ticks = 50 }, "ACK_WAIT")

        -- Phase 3: Monitor bitmap updates (stays active, processes all)
        ct:define_column_link(
            "HUB_BITMAP_HANDLER", "HUB_BITMAP_INIT",
            "CFL_NULL", "CFL_NULL",
            { bitmask_fields = plugin.bitmask or {} }, "BITMAP_MON")

        -- Phase 4: Wait for kb_done (terminates column on receipt)
        ct:define_column_link(
            "HUB_KB_DONE_HANDLER", "HUB_KB_DONE_INIT",
            "CFL_NULL", "CFL_NULL",
            { timeout_ticks = 100, pose_fields = plugin.pose_fields or {} },
            "KB_DONE_WAIT")

    ct:end_column(col)

    ct:end_test()
end

-- Simplified tree for KBs that don't wait for response (idle)
function M.build_fire_and_forget(ct, kb_name, one_shot_name)
    ct:start_test(kb_name)

    local col = ct:define_column(kb_name .. "_main", nil, nil, nil, nil, {}, true)
        ct:asm_one_shot_handler(one_shot_name,
            {"current_test_json", "command_packet"})
    ct:end_column(col)

    ct:end_test()
end

return M
