--[[
    error_recovery KB -- Activated when any KB fails (timeout or fault).

    Reads fault context from blackboard:
      fault_reason  — "ack_timeout" | "kb_done_timeout" | "fault"
      fault_kb      — name of the KB that failed

    Recovery actions:
      1. Log the fault
      2. Attempt retry (if ack_timeout, resend command)
      3. If retry fails, abort and report to planner
      4. Record partial pose if available

    This KB is separate from the normal plan. It gets activated
    by CFL_TERMINATE from a failed KB, or explicitly by the planner.
]]

return {
    name           = "error_recovery",
    index          = 0,  -- index 0: not part of normal plan sequence
    packet_ctype   = "cmd_idle_t",   -- sends idle/stop on abort
    packet_type_id = 7,

    json_schema = {},
    mapping = {},

    bitmask = {
        { name = "recovery_active",  bit = 0 },
        { name = "retry_attempted",  bit = 1 },
        { name = "abort_sent",       bit = 2 },
        { name = "recovery_done",    bit = 3 },
    },

    pose_fields = {},

    define_tree = function(ct, kb_name, one_shot_name)
        ct:start_test(kb_name)

        local col = ct:define_column("error_recovery_main", nil, nil, nil, nil, {}, true)

            -- One-shot: log fault and decide recovery strategy
            ct:asm_one_shot_handler("HUB_ERROR_RECOVERY_INIT",
                {"fault_reason", "fault_kb"})

            -- One-shot: send stop/idle command to remote
            ct:asm_one_shot_handler(one_shot_name,
                {"current_test_json", "command_packet"})

            -- Wait for ack of the stop command (with short timeout)
            ct:define_column_link(
                "HUB_KB_DONE_HANDLER", "HUB_KB_DONE_INIT",
                "CFL_NULL", "CFL_NULL",
                { timeout_ticks = 30 }, "RECOVERY_WAIT")

        ct:end_column(col)

        ct:end_test()
    end,
}
