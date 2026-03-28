--[[
    st_builtins.lua - Scan Tree VFT Builtins (LuaJIT)

    Port of builtins.c: all system VFT functions.
    Each VFT: function(state, handle, inputs, n_inputs) -> uint8 result

    state:    {[node_id]} table, state[node_id] is per-node persistent byte
    handle:   st_handle table with buf_data(buf_id) accessor
    inputs:   array of {buf_id, start, count, role}
    n_inputs: number of valid input entries
--]]

local M = {}

-- Role constants (match scan_tree.h)
local ROLE_DEFAULT   = 0
local ROLE_SET       = 1
local ROLE_CLEAR     = 2
local ROLE_THRESHOLD = 3
local ROLE_BITS      = 4
local ROLE_A         = 5
local ROLE_B         = 6
local ROLE_INPUT     = 7

-- buf_data returns a 0-based indexable array for the given buf_id
-- For raw buffers: current data; for layers: value array

function M.vft_and(state, nid, h, inputs, n)
    local d = h:buf_data(inputs[1].buf_id)
    local s = inputs[1].start
    for i = 0, inputs[1].count - 1 do
        if d[s + i] == 0 then return 0 end
    end
    return 1
end

function M.vft_or(state, nid, h, inputs, n)
    local d = h:buf_data(inputs[1].buf_id)
    local s = inputs[1].start
    for i = 0, inputs[1].count - 1 do
        if d[s + i] ~= 0 then return 1 end
    end
    return 0
end

function M.vft_not(state, nid, h, inputs, n)
    local d = h:buf_data(inputs[1].buf_id)
    return (d[inputs[1].start] ~= 0) and 0 or 1
end

function M.vft_latch(state, nid, h, inputs, n)
    local sd, sp, cd, cp
    for i = 1, n do
        local inp = inputs[i]
        if inp.role == ROLE_SET then
            sd = h:buf_data(inp.buf_id); sp = inp.start
        elseif inp.role == ROLE_CLEAR then
            cd = h:buf_data(inp.buf_id); cp = inp.start
        end
    end
    if cd and cd[cp] ~= 0 then
        state[nid] = 0
    elseif sd and sd[sp] ~= 0 then
        state[nid] = 1
    end
    return state[nid]
end

function M.vft_k_of_n(state, nid, h, inputs, n)
    local thresh = 0
    local bits, bs, bc
    for i = 1, n do
        local inp = inputs[i]
        if inp.role == ROLE_THRESHOLD then
            thresh = h:buf_data(inp.buf_id)[inp.start]
        elseif inp.role == ROLE_BITS then
            bits = h:buf_data(inp.buf_id)
            bs = inp.start; bc = inp.count
        end
    end
    local pop = 0
    if bits then
        for i = 0, bc - 1 do
            if bits[bs + i] ~= 0 then pop = pop + 1 end
        end
    end
    return (pop >= thresh) and 1 or 0
end

function M.vft_gt(state, nid, h, inputs, n)
    local a, b = 0, 0
    for i = 1, n do
        local inp = inputs[i]
        local fd = h:buf_data(inp.buf_id)
        if inp.role == ROLE_A then a = fd[inp.start] end
        if inp.role == ROLE_B then b = fd[inp.start] end
    end
    return (a > b) and 1 or 0
end

function M.vft_ge(state, nid, h, inputs, n)
    local a, b = 0, 0
    for i = 1, n do
        local inp = inputs[i]
        local fd = h:buf_data(inp.buf_id)
        if inp.role == ROLE_A then a = fd[inp.start] end
        if inp.role == ROLE_B then b = fd[inp.start] end
    end
    return (a >= b) and 1 or 0
end

function M.vft_eq(state, nid, h, inputs, n)
    local a, b = 0, 0
    for i = 1, n do
        local inp = inputs[i]
        local fd = h:buf_data(inp.buf_id)
        if inp.role == ROLE_A then a = fd[inp.start] end
        if inp.role == ROLE_B then b = fd[inp.start] end
    end
    return (a == b) and 1 or 0
end

function M.vft_lt(state, nid, h, inputs, n)
    local a, b = 0, 0
    for i = 1, n do
        local inp = inputs[i]
        local fd = h:buf_data(inp.buf_id)
        if inp.role == ROLE_A then a = fd[inp.start] end
        if inp.role == ROLE_B then b = fd[inp.start] end
    end
    return (a < b) and 1 or 0
end

function M.vft_le(state, nid, h, inputs, n)
    local a, b = 0, 0
    for i = 1, n do
        local inp = inputs[i]
        local fd = h:buf_data(inp.buf_id)
        if inp.role == ROLE_A then a = fd[inp.start] end
        if inp.role == ROLE_B then b = fd[inp.start] end
    end
    return (a <= b) and 1 or 0
end

function M.vft_range_check(state, nid, h, inputs, n)
    local val, lo, hi = 0, 0, 0
    for i = 1, n do
        local inp = inputs[i]
        local fd = h:buf_data(inp.buf_id)
        if inp.role == ROLE_DEFAULT then val = fd[inp.start] end
        if inp.role == ROLE_A then lo = fd[inp.start] end
        if inp.role == ROLE_B then hi = fd[inp.start] end
    end
    return (val >= lo and val <= hi) and 1 or 0
end

function M.vft_copy(state, nid, h, inputs, n)
    return h:buf_data(inputs[1].buf_id)[inputs[1].start] ~= 0 and 1 or 0
end

function M.vft_fuse(state, nid, h, inputs, n)
    local input_val, clear_val = 0, 0
    for i = 1, n do
        local inp = inputs[i]
        if inp.role == ROLE_INPUT then
            input_val = h:buf_data(inp.buf_id)[inp.start]
        elseif inp.role == ROLE_CLEAR then
            clear_val = h:buf_data(inp.buf_id)[inp.start]
        end
    end

    local s = state[nid]
    if s == 0 then -- intact
        if input_val ~= 0 then
            state[nid] = 1
            -- Fire fuse action callback
            if h.fuse_actions and h.fuse_actions[nid] then
                h.fuse_actions[nid](h.user_handle)
            end
            return 1
        end
        return 0
    elseif s == 1 then -- blown, waiting for clear high
        if clear_val ~= 0 then state[nid] = 2 end
        return 1
    elseif s == 2 then -- clearing, waiting for clear low
        if clear_val == 0 then
            state[nid] = 0
            return 0
        end
        return 1
    else
        state[nid] = 0
        return 0
    end
end

-- Export role constants
M.ROLE_DEFAULT   = ROLE_DEFAULT
M.ROLE_SET       = ROLE_SET
M.ROLE_CLEAR     = ROLE_CLEAR
M.ROLE_THRESHOLD = ROLE_THRESHOLD
M.ROLE_BITS      = ROLE_BITS
M.ROLE_A         = ROLE_A
M.ROLE_B         = ROLE_B
M.ROLE_INPUT     = ROLE_INPUT

return M
