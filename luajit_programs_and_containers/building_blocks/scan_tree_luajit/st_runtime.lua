--[[
    st_runtime.lua - Scan Tree Runtime Engine (LuaJIT)

    Port of scan_tree.c: init, swap, mark_dirty, evaluate, cycle.

    Architecture:
      - System descriptor is a Lua table (produced by codegen_luajit.lua)
      - Handle holds working storage: raw buffers, layer buffers, node state
      - All buffer data is 0-indexed (array[0..size-1]) for C compatibility
      - Raw buffers use double-buffering with change detection
--]]

local ffi = require("ffi")
local bit = require("bit")
local bor, band, lshift = bit.bor, bit.band, bit.lshift

local builtins = require("st_builtins")

-- =====================================================================
-- 0-indexed array helper: creates a table with __index/__newindex
-- that maps [0..n-1] to internal [1..n]
-- =====================================================================
local function new_array(size, fill)
    fill = fill or 0
    local t = {}
    for i = 1, size do t[i] = fill end
    return setmetatable({}, {
        __index = function(_, k) return t[k + 1] end,
        __newindex = function(_, k, v) t[k + 1] = v end,
        __len = function() return size end,
    })
end

-- Raw copy: assumes both are 0-indexed arrays of same size
local function array_copy(dst, src, size)
    for i = 0, size - 1 do dst[i] = src[i] end
end

local function array_equal(a, b, size)
    for i = 0, size - 1 do
        if a[i] ~= b[i] then return false end
    end
    return true
end

local function array_fill(arr, size, val)
    for i = 0, size - 1 do arr[i] = val end
end

-- =====================================================================
-- Handle object
-- =====================================================================
local Handle = {}
Handle.__index = Handle

--[[
    desc format (produced by codegen_luajit.lua):
    {
        name = "system_name",
        buf_descs = {  -- 1-indexed, but buf_id is 0-based
            [1] = {path, key, size, elem_size, is_layer, buf_index, level},
            ...
        },
        node_descs = {
            [1] = {func, output_buf_id, output_pos, inputs={...}, n_inputs, raw_deps={...}},
            ...
        },
        lookup = {  -- sorted by key
            [1] = {key, buf_id},
            ...
        },
        n_bufs, n_nodes, n_raw, n_layer,
        fuse_table = { [node_1based] = action_func, ... },
    }
--]]

function Handle.new(desc)
    local self = setmetatable({}, Handle)
    self.desc = desc

    -- Raw buffers: [buf_index] = {current, previous, size, elem_size, changed}
    self.raw_bufs = {}
    for i = 1, #desc.buf_descs do
        local bd = desc.buf_descs[i]
        if not bd.is_layer then
            self.raw_bufs[bd.buf_index] = {
                current  = new_array(bd.size, 0),
                previous = new_array(bd.size, 0),
                size      = bd.size,
                elem_size = bd.elem_size,
                changed   = true,
            }
        end
    end

    -- Layer buffers: [buf_index] = {value, not_active, shadow, states, size}
    self.layer_bufs = {}
    for i = 1, #desc.buf_descs do
        local bd = desc.buf_descs[i]
        if bd.is_layer then
            local val = new_array(bd.size, 0)
            local na  = new_array(bd.size, 1)
            local shd = new_array(bd.size, 0)
            local st  = new_array(bd.size, -1)
            self.layer_bufs[bd.buf_index] = {
                value      = val,
                not_active = na,
                shadow     = shd,
                states     = st,
                size       = bd.size,
            }
        end
    end

    -- Per-node state and dirty flags (1-indexed, matching node_descs)
    self.node_state = {}
    self.node_dirty = {}
    for i = 1, desc.n_nodes do
        self.node_state[i] = 0
        self.node_dirty[i] = true
    end

    -- Fuse action table: node_1based -> function
    self.fuse_actions = desc.fuse_table or {}

    -- User handle (application context)
    self.user_handle = nil

    return self
end

function Handle:buf_data(buf_id)
    local bd = self.desc.buf_descs[buf_id + 1]
    if bd.is_layer then
        return self.layer_bufs[bd.buf_index].value
    else
        return self.raw_bufs[bd.buf_index].current
    end
end

function Handle:raw_current(buf_id)
    local bd = self.desc.buf_descs[buf_id + 1]
    return self.raw_bufs[bd.buf_index].current
end

function Handle:get_state(buf_id, pos)
    local bd = self.desc.buf_descs[buf_id + 1]
    if not bd.is_layer or pos >= bd.size then return -1 end
    return self.layer_bufs[bd.buf_index].states[pos]
end

function Handle:layer_states(buf_id)
    local bd = self.desc.buf_descs[buf_id + 1]
    if not bd.is_layer then return nil, 0 end
    local l = self.layer_bufs[bd.buf_index]
    return l.states, l.size
end

function Handle:print_info()
    local d = self.desc
    print(string.format("%s: %d bufs, %d nodes, %d raw, %d layer",
        d.name, #d.buf_descs, d.n_nodes, d.n_raw, d.n_layer))
end

-- =====================================================================
-- Lookup
-- =====================================================================

local function fnv1a(str)
    local h = 2166136261
    for i = 1, #str do
        h = bor(0, bit.bxor(h, str:byte(i)))
        -- Multiply by 16777619 using 32-bit arithmetic
        -- h * 16777619 = h * (16777216 + 403) = h * 2^24 + h * 403
        h = h * 16777619
        -- Mask to 32 bits
        h = h % 4294967296
    end
    return h
end

function Handle:lookup_path(path)
    local key = fnv1a(path)
    local t = self.desc.lookup
    local lo, hi = 1, #t
    while lo <= hi do
        local mid = math.floor((lo + hi) / 2)
        if t[mid].key < key then
            lo = mid + 1
        elseif t[mid].key > key then
            hi = mid - 1
        else
            -- Verify path
            if self.desc.buf_descs[t[mid].buf_id + 1].path == path then
                return t[mid].buf_id
            end
            -- Linear probe for collisions
            for i = mid + 1, hi do
                if t[i].key ~= key then break end
                if self.desc.buf_descs[t[i].buf_id + 1].path == path then
                    return t[i].buf_id
                end
            end
            for i = mid - 1, lo, -1 do
                if t[i].key ~= key then break end
                if self.desc.buf_descs[t[i].buf_id + 1].path == path then
                    return t[i].buf_id
                end
            end
            return -1
        end
    end
    return -1
end

function Handle:lookup_key(key)
    local t = self.desc.lookup
    local lo, hi = 1, #t
    while lo <= hi do
        local mid = math.floor((lo + hi) / 2)
        if t[mid].key < key then lo = mid + 1
        elseif t[mid].key > key then hi = mid - 1
        else return t[mid].buf_id end
    end
    return -1
end

-- =====================================================================
-- Swap raw buffers
-- =====================================================================

function Handle:swap_raw()
    for idx, r in pairs(self.raw_bufs) do
        r.changed = not array_equal(r.current, r.previous, r.size)
        if r.changed then
            array_copy(r.previous, r.current, r.size)
        end
    end
end

-- =====================================================================
-- Mark dirty (bitmask-driven)
-- =====================================================================

function Handle:mark_dirty()
    local d = self.desc
    local n_raw_words = math.ceil(d.n_raw / 32)
    if n_raw_words < 1 then n_raw_words = 1 end

    -- Build changed bitmask
    local changed = {}
    for w = 1, n_raw_words do changed[w] = 0 end
    local any = false
    for r = 0, d.n_raw - 1 do
        if self.raw_bufs[r] and self.raw_bufs[r].changed then
            local w = math.floor(r / 32) + 1
            changed[w] = bor(changed[w], lshift(1, r % 32))
            any = true
        end
    end

    if not any then
        for i = 1, d.n_nodes do self.node_dirty[i] = false end
        return
    end

    -- Mark each node dirty if its raw_deps overlap changed set
    for i = 1, d.n_nodes do
        local hit = false
        local deps = d.node_descs[i].raw_deps
        local nw = math.ceil(d.n_raw / 32)
        if nw < 1 then nw = 1 end
        for w = 1, nw do
            if band(deps[w] or 0, changed[w]) ~= 0 then
                hit = true; break
            end
        end
        self.node_dirty[i] = hit
    end
end

-- =====================================================================
-- Evaluate
-- =====================================================================

function Handle:evaluate()
    local d = self.desc
    for i = 1, d.n_nodes do
        if self.node_dirty[i] then
            self.node_dirty[i] = false
            local nd = d.node_descs[i]
            local obd = d.buf_descs[nd.output_buf_id + 1]
            local out = self.layer_bufs[obd.buf_index]
            local pos = nd.output_pos

            local result = nd.func(self.node_state, i, self, nd.inputs, nd.n_inputs)

            -- layer_write
            out.not_active[pos] = 0
            out.value[pos] = result
            out.states[pos] = (result ~= 0) and 1 or 0
        end
    end

    -- Update layer shadows
    for idx, l in pairs(self.layer_bufs) do
        array_copy(l.shadow, l.value, l.size)
    end
end

-- =====================================================================
-- Cycle = swap + mark_dirty + evaluate
-- =====================================================================

function Handle:cycle()
    self:swap_raw()
    self:mark_dirty()
    self:evaluate()
end

-- =====================================================================
-- Module export
-- =====================================================================

return {
    Handle = Handle,
    fnv1a  = fnv1a,
    new_array = new_array,
}
