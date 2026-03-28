--[[
    User VFT implementations for pump station test
--]]

local M = {}

function M.user_vft_motor_health_check(state, nid, h, inputs, n_inputs)
    local cur = h:buf_data(inputs[1].buf_id)[inputs[1].start]
    local thr = h:buf_data(inputs[2].buf_id)[inputs[2].start]
    return (cur < thr) and 1 or 0
end

return M
