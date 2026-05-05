--[[
    error_recovery KB -- Activated when any KB fails (timeout or fault).
    Custom tree: log fault → send stop → wait for kb_done (short timeout).
    Data fields (schema, bitmask, pose_fields) come from KB VN definitions.
]]

return {
    name         = "error_recovery",
    index        = 0,
    packet_ctype = "cmd_idle_t",

    define_tree = function(ct, kb_name, one_shot_name)
        ct:start_test(kb_name)

        local col = ct:define_column("error_recovery_main", nil, nil, nil, nil, {}, true)

            ct:asm_one_shot_handler("HUB_ERROR_RECOVERY_INIT",
                {"fault_reason", "fault_kb"})

            ct:asm_one_shot_handler(one_shot_name,
                {"current_test_json", "command_packet"})

            ct:define_column_link(
                "HUB_KB_DONE_HANDLER", "HUB_KB_DONE_INIT",
                "CFL_NULL", "CFL_NULL",
                { timeout_ticks = 30 }, "RECOVERY_WAIT")

        ct:end_column(col)

        ct:end_test()
    end,
}
