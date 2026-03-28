--[[
    st_display.lua - Hierarchical Colored Layer State Display (LuaJIT)

    Port of st_display.c: builds a cached display hierarchy from the
    handle's buffer descriptors, then prints colored state trees.
--]]

local M = {}

-- ANSI color codes
local GREEN = "\027[32m"
local RED   = "\027[31m"
local GREY  = "\027[90m"
local BOLD  = "\027[1m"
local RESET = "\027[0m"

-- Count dots in a path
local function path_depth(path)
    local d = 0
    for _ in path:gmatch("%.") do d = d + 1 end
    return d
end

-- Skip N dot-separated segments, return remainder
local function skip_segments(path, n)
    local d = 0
    local i = 1
    while i <= #path and d < n do
        if path:sub(i, i) == "." then d = d + 1 end
        i = i + 1
    end
    return path:sub(i)
end

-- Check if child_path is a direct sub-level child of parent_path
local function is_child_of(parent_path, child_path, parent_depth)
    -- Find the prefix up to the last dot in parent_path
    local last_dot = nil
    for i = 1, #parent_path do
        if parent_path:sub(i, i) == "." then last_dot = i end
    end
    local prefix_len = last_dot and (last_dot - 1) or 0
    if prefix_len == 0 then return false end

    local prefix = parent_path:sub(1, prefix_len)
    if child_path:sub(1, prefix_len) ~= prefix then return false end
    if child_path:sub(prefix_len + 1, prefix_len + 1) ~= "." then return false end

    return path_depth(child_path) == parent_depth + 1
end

-- =====================================================================
-- Display object
-- =====================================================================

local Display = {}
Display.__index = Display

-- Build display hierarchy recursively
local function build_recursive(handle, buf_id, layer_ids, indent, entries)
    local bd = handle.desc.buf_descs[buf_id + 1]
    entries[#entries + 1] = {
        buf_id = buf_id,
        indent = indent,
        title  = skip_segments(bd.path, 1),
    }

    local my_depth = path_depth(bd.path)
    local my_level = bd.level

    for _, lid in ipairs(layer_ids) do
        if lid ~= buf_id then
            local cbd = handle.desc.buf_descs[lid + 1]
            if cbd.level == my_level and is_child_of(bd.path, cbd.path, my_depth) then
                build_recursive(handle, lid, layer_ids, indent + 1, entries)
            end
        end
    end
end

function Display.new(handle)
    local self = setmetatable({}, Display)
    local d = handle.desc

    self.name = d.name

    -- Collect layer buffer ids (0-based)
    local layer_ids = {}
    for i = 1, #d.buf_descs do
        if d.buf_descs[i].is_layer then
            layer_ids[#layer_ids + 1] = i - 1
        end
    end

    self.entries = {}
    if #layer_ids == 0 then return self end

    -- Find max level
    local max_level = 0
    for _, lid in ipairs(layer_ids) do
        local lvl = d.buf_descs[lid + 1].level
        if lvl > max_level then max_level = lvl end
    end

    -- Build display order: levels from highest to lowest
    for lvl = max_level, 0, -1 do
        -- Find minimum path depth for this level
        local min_depth = 9999
        for _, lid in ipairs(layer_ids) do
            if d.buf_descs[lid + 1].level == lvl then
                local dep = path_depth(d.buf_descs[lid + 1].path)
                if dep < min_depth then min_depth = dep end
            end
        end

        -- Add root buffers of this level
        for _, lid in ipairs(layer_ids) do
            if d.buf_descs[lid + 1].level == lvl then
                if path_depth(d.buf_descs[lid + 1].path) == min_depth then
                    build_recursive(handle, lid, layer_ids, 0, self.entries)
                end
            end
        end
    end

    return self
end

function Display:print_tree(handle)
    io.write(BOLD .. "=== " .. self.name .. " ===" .. RESET .. "\n")

    for _, e in ipairs(self.entries) do
        local bd = handle.desc.buf_descs[e.buf_id + 1]
        local l = handle.layer_bufs[bd.buf_index]

        -- Indentation
        for _ = 1, e.indent do io.write("  ") end

        io.write(BOLD .. e.title .. RESET .. " [")

        for j = 0, bd.size - 1 do
            if j > 0 then io.write(" ") end
            local s
            if l.not_active[j] ~= 0 then
                s = -1
            elseif l.value[j] ~= 0 then
                s = 1
            else
                s = 0
            end

            if s == 1 then
                io.write(GREEN .. "T" .. RESET)
            elseif s == 0 then
                io.write(RED .. "F" .. RESET)
            else
                io.write(GREY .. "N" .. RESET)
            end
        end
        io.write("]\n")
    end
end

M.Display = Display

return M
