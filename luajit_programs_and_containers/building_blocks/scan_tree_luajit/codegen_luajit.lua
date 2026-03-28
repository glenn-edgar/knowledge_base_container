#!/usr/bin/env luajit
--[[
    codegen_luajit.lua - Scan Tree LuaJIT Code Generator

    Reads the same JSON intermediate as codegen_c.lua, produces a Lua
    module that returns a system descriptor table for st_runtime.lua.

    Output: {name}.lua — loadable const descriptor

    Usage: luajit codegen_luajit.lua <json_file> <output_dir>
--]]

-- JSON decoder (same minimal decoder as codegen_c.lua)
local json = {}
local function ws(s, p) return s:match("^%s*()", p) end
local function ds(s, p)
    local t = {}
    while p <= #s do
        local c = s:sub(p, p)
        if c == '"' then return table.concat(t), p + 1
        elseif c == '\\' then
            p = p + 1; local e = s:sub(p, p)
            if e == '"' then t[#t + 1] = '"'
            elseif e == '\\' then t[#t + 1] = '\\'
            elseif e == 'n' then t[#t + 1] = '\n'
            elseif e == 't' then t[#t + 1] = '\t'
            else t[#t + 1] = e end
            p = p + 1
        else t[#t + 1] = c; p = p + 1 end
    end
end
local dv
local function da(s, p)
    local a = {}; p = ws(s, p)
    if s:sub(p, p) == ']' then return a, p + 1 end
    while true do
        local v; v, p = dv(s, p); a[#a + 1] = v; p = ws(s, p)
        if s:sub(p, p) == ']' then return a, p + 1 end
        p = ws(s, p + 1)
    end
end
local function do_(s, p)
    local o = {}; p = ws(s, p)
    if s:sub(p, p) == '}' then return o, p + 1 end
    while true do
        p = ws(s, p); local k; k, p = ds(s, p + 1); p = ws(s, p); p = ws(s, p + 1)
        local v; v, p = dv(s, p); o[k] = v; p = ws(s, p)
        if s:sub(p, p) == '}' then return o, p + 1 end
        p = ws(s, p + 1)
    end
end
function dv(s, p)
    p = ws(s, p); local c = s:sub(p, p)
    if c == '"' then return ds(s, p + 1)
    elseif c == '{' then return do_(s, p + 1)
    elseif c == '[' then return da(s, p + 1)
    elseif c == 't' then return true, p + 4
    elseif c == 'f' then return false, p + 5
    elseif c == 'n' then return nil, p + 4
    else local n = s:match("^-?%d+%.?%d*[eE]?[+-]?%d*", p); return tonumber(n), p + #n end
end
function json.decode(s) return (dv(s, 1)) end

-- =====================================================================
-- Helpers
-- =====================================================================

local bit = require("bit")
local bor, lshift = bit.bor, bit.lshift

local ESIZES = {
    ["bool"] = 1, ["uint8_t"] = 1, ["uint16_t"] = 2, ["uint32_t"] = 4,
    ["uint64_t"] = 8, ["int8_t"] = 1, ["int16_t"] = 2, ["int32_t"] = 4,
    ["int64_t"] = 8, ["float"] = 4, ["double"] = 8,
}

-- FNV-1a in Lua (must match st_runtime.lua)
local function fnv1a(str)
    local h = 2166136261
    for i = 1, #str do
        h = bor(0, bit.bxor(h, str:byte(i)))
        h = h * 16777619
        h = h % 4294967296
    end
    return h
end

local function each_layer(node, fn)
    if node.output_buffer then fn(node.output_buffer) end
    for _, s in ipairs(node.sub_levels or {}) do each_layer(s, fn) end
end

local function collect_vfts(node, out)
    for _, s in ipairs(node.sub_levels or {}) do collect_vfts(s, out) end
    for _, v in ipairs(node.virtual_functions or {}) do out[#out + 1] = v end
end

-- =====================================================================
-- Build buf_id mapping
-- =====================================================================
local function build_buf_table(data)
    local bufs = {}
    local path2id = {}
    local raw_idx = 0
    local layer_idx = 0

    for _, buf in ipairs(data.raw_buffers) do
        local id = #bufs
        bufs[#bufs + 1] = {
            path = buf.path, key = fnv1a(buf.path),
            size = buf.size, elem_size = ESIZES[buf.type],
            is_layer = false, buf_index = raw_idx,
            level = 0xFF,
        }
        path2id[buf.path] = id
        raw_idx = raw_idx + 1
    end

    for _, lv in ipairs(data.levels) do
        local lvnum = lv.level_number or 0
        each_layer(lv, function(buf)
            local id = #bufs
            bufs[#bufs + 1] = {
                path = buf.path, key = fnv1a(buf.path),
                size = buf.size, elem_size = 1,
                is_layer = true, buf_index = layer_idx,
                level = lvnum,
            }
            path2id[buf.path] = id
            layer_idx = layer_idx + 1
        end)
    end

    return bufs, path2id, raw_idx, layer_idx
end

-- =====================================================================
-- VFT name -> builtin function mapping
-- =====================================================================
local LIB_FUNC_MAP = {
    st_vft_and         = "builtins.vft_and",
    st_vft_or          = "builtins.vft_or",
    st_vft_not         = "builtins.vft_not",
    st_vft_latch       = "builtins.vft_latch",
    st_vft_k_of_n      = "builtins.vft_k_of_n",
    st_vft_gt          = "builtins.vft_gt",
    st_vft_ge          = "builtins.vft_ge",
    st_vft_eq          = "builtins.vft_eq",
    st_vft_lt          = "builtins.vft_lt",
    st_vft_le          = "builtins.vft_le",
    st_vft_range_check = "builtins.vft_range_check",
    st_vft_copy        = "builtins.vft_copy",
    st_vft_fuse        = "builtins.vft_fuse",
}

-- =====================================================================
-- Generate Lua descriptor module
-- =====================================================================
local function generate(data, bufs, path2id, n_raw, n_layer)
    local allvf = {}
    for _, lv in ipairs(data.levels) do collect_vfts(lv, allvf) end

    -- Compute raw dependency bitmasks (same algorithm as codegen_c)
    local n_raw_words = math.ceil(n_raw / 32)
    if n_raw_words < 1 then n_raw_words = 1 end

    local layer_writers = {}
    for ni, vf in ipairs(allvf) do
        local obid = path2id[vf.output.buffer]
        if not layer_writers[obid] then layer_writers[obid] = {} end
        local t = layer_writers[obid]
        t[#t + 1] = ni
    end

    local node_raw_deps = {}
    for ni, vf in ipairs(allvf) do
        local deps = {}
        for w = 1, n_raw_words do deps[w] = 0 end
        for _, inp in ipairs(vf.inputs) do
            local ibid = path2id[inp.buffer]
            local bd = bufs[ibid + 1]
            if not bd.is_layer then
                local ri = bd.buf_index
                local word = math.floor(ri / 32) + 1
                deps[word] = bor(deps[word], lshift(1, ri % 32))
            else
                local writers = layer_writers[ibid]
                if writers then
                    for _, wi in ipairs(writers) do
                        local wd = node_raw_deps[wi]
                        if wd then
                            for w = 1, n_raw_words do
                                deps[w] = bor(deps[w], wd[w])
                            end
                        end
                    end
                end
            end
        end
        node_raw_deps[ni] = deps
    end

    -- Collect fuse entries
    local fuse_map = {}
    for i, vf in ipairs(allvf) do
        if vf.is_fuse then
            fuse_map[i] = vf.fuse_action
        end
    end

    -- Collect unique user VFTs
    local user_vfts = {}
    local seen_user = {}
    for _, vf in ipairs(allvf) do
        if vf.class == "user" and not seen_user[vf.user_func] then
            seen_user[vf.user_func] = true
            user_vfts[#user_vfts + 1] = vf.user_func
        end
    end

    -- Generate output
    local lines = {}
    local function w(s) lines[#lines + 1] = s end

    w("-- Generated scan tree descriptor: " .. data.name)
    w("-- DO NOT EDIT - produced by codegen_luajit.lua")
    w("")
    w("local builtins = require('st_builtins')")
    w("")

    -- User VFT forward declarations
    if #user_vfts > 0 then
        w("-- User VFTs must be provided via the user_vfts table parameter")
        w("local function make_desc(user_vfts)")
        w("    user_vfts = user_vfts or {}")
    else
        w("local function make_desc()")
    end

    w("")
    w("    local desc = {}")
    w(string.format("    desc.name = %q", data.name))
    w("")

    -- buf_descs (1-indexed array, buf_id = index - 1)
    w("    -- buf_descs: 1-indexed, buf_id = array_index - 1")
    w("    desc.buf_descs = {")
    for i, b in ipairs(bufs) do
        w(string.format(
            "        {path=%q, key=%u, size=%d, elem_size=%d, is_layer=%s, buf_index=%d, level=%d}, -- buf_id=%d",
            b.path, b.key, b.size, b.elem_size,
            b.is_layer and "true" or "false",
            b.buf_index, b.level, i - 1))
    end
    w("    }")
    w("")

    -- node_descs (1-indexed)
    w("    -- node_descs: 1-indexed")
    w("    desc.node_descs = {")
    for i, vf in ipairs(allvf) do
        local func_ref
        if vf.class == "system" then
            func_ref = LIB_FUNC_MAP[vf.lib_func]
            if not func_ref then
                error("Unknown lib_func: " .. tostring(vf.lib_func))
            end
        else
            func_ref = string.format("user_vfts.%s", vf.user_func)
        end

        local obid = path2id[vf.output.buffer]
        local opos = vf.output.start

        -- Build inputs array string
        local inp_parts = {}
        for _, inp in ipairs(vf.inputs) do
            local ibid = path2id[inp.buffer]
            inp_parts[#inp_parts + 1] = string.format(
                "{buf_id=%d, start=%d, count=%d, role=%d}",
                ibid, inp.start, inp.count, inp.role or 0)
        end

        -- Build raw_deps
        local deps = node_raw_deps[i]
        local dep_parts = {}
        for w2 = 1, n_raw_words do
            dep_parts[w2] = tostring(deps[w2] or 0)
        end

        w(string.format("        { -- [%d] %s (%s) -> buf[%d][%d]", i, vf.vft_name, vf.class, obid, opos))
        w(string.format("            func = %s,", func_ref))
        w(string.format("            output_buf_id = %d, output_pos = %d,", obid, opos))
        w(string.format("            inputs = {%s},", table.concat(inp_parts, ", ")))
        w(string.format("            n_inputs = %d,", #vf.inputs))
        w(string.format("            raw_deps = {%s},", table.concat(dep_parts, ", ")))
        w("        },")
    end
    w("    }")
    w("")

    -- lookup (1-indexed, sorted by key)
    local lkup = {}
    for i, b in ipairs(bufs) do
        lkup[#lkup + 1] = {key = b.key, buf_id = i - 1}
    end
    table.sort(lkup, function(a, b) return a.key < b.key end)

    w("    -- lookup: sorted by key, 1-indexed")
    w("    desc.lookup = {")
    for _, e in ipairs(lkup) do
        w(string.format("        {key=%u, buf_id=%d},", e.key, e.buf_id))
    end
    w("    }")
    w("")

    -- Counts
    w(string.format("    desc.n_bufs = %d", #bufs))
    w(string.format("    desc.n_nodes = %d", #allvf))
    w(string.format("    desc.n_raw = %d", n_raw))
    w(string.format("    desc.n_layer = %d", n_layer))
    w("")

    -- Fuse table
    local has_fuses = false
    for _ in pairs(fuse_map) do has_fuses = true; break end
    if has_fuses then
        w("    -- fuse_table: node_1based -> action name (resolved at test time)")
        w("    desc.fuse_table = {")
        for nid, action in pairs(fuse_map) do
            w(string.format("        [%d] = %q,  -- %s", nid, action, action))
        end
        w("    }")
    else
        w("    desc.fuse_table = {}")
    end
    w("")

    -- Key defines as constants
    w("    -- Buffer ID constants")
    w("    desc.IDS = {")
    for i, b in ipairs(bufs) do
        local cn = b.path:gsub("%.", "_"):upper()
        w(string.format("        %s = %d,", cn, i - 1))
    end
    w("    }")
    w("")

    w("    return desc")
    w("end")
    w("")
    w("return make_desc")

    return table.concat(lines, "\n") .. "\n"
end

-- =====================================================================
-- Main
-- =====================================================================
local f, err = io.open(arg[1], "r")
if not f then
    io.stderr:write(string.format("Error: cannot open %s: %s\n", arg[1], err))
    os.exit(1)
end
local s = f:read("*a")
f:close()
local data = json.decode(s)
local name = data.name or "scan_tree"
local dir = arg[2] or "lua_output"
os.execute("mkdir -p " .. dir)

local bufs, path2id, n_raw, n_layer = build_buf_table(data)
local output = generate(data, bufs, path2id, n_raw, n_layer)

local out_path = dir .. "/" .. name .. ".lua"
local fout = io.open(out_path, "w")
fout:write(output)
fout:close()
print("Generated: " .. out_path)
