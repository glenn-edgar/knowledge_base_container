--[[
    error_recovery.lua -- Error recovery one-shot function.

    Called when error_recovery KB activates.
    Reads fault context, logs it, stages a stop command.
]]

local json_util = require("json_util")

local M = {}
M.one_shot = {}

M.one_shot.HUB_ERROR_RECOVERY_INIT = function(handle, node)
    local bb = handle.blackboard
    local fault_reason = bb.fault_reason or "unknown"
    local fault_kb     = bb.fault_kb or "unknown"

    -- Log fault
    io.write(string.format("  ERROR_RECOVERY: fault=%s kb=%s\n",
        fault_reason, fault_kb))
    io.flush()

    -- Record recovery state
    bb.recovery_active = true
    bb.recovery_fault_reason = fault_reason
    bb.recovery_fault_kb = fault_kb

    -- Stage idle/stop command as current_test_json
    -- The generic packet mapper one-shot will pick this up
    bb.current_test_json = json_util.encode({
        test_id = 0,
        packet_type = 7,  -- TYPE_IDLE
        next_test = 0,
    })

    -- Flush to KeyStore if available
    if bb.flush then bb.flush() end
end

M.registry = {
    one_shot = M.one_shot,
}

return M
