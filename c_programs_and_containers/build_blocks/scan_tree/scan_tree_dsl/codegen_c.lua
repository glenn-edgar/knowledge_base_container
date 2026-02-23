#!/usr/bin/env luajit
--[[
    Scan Tree C Code Generator v2

    Produces CONST DATA TABLES that the library engine consumes.

    Output:
      {name}.h          - key defines, extern descriptor, accessor macros
      {name}.c          - const buf_descs[], node_descs[], lookup[], system_desc
      {name}_user_vft.h - prototypes for user VFTs
      {name}_test.c     - test
      Makefile
--]]

-- JSON decoder
local json={}
local function ws(s,p) return s:match("^%s*()",p) end
local function ds(s,p) local t={} while p<=#s do local c=s:sub(p,p)
    if c=='"' then return table.concat(t),p+1
    elseif c=='\\' then p=p+1; local e=s:sub(p,p)
        if e=='"' then t[#t+1]='"' elseif e=='\\' then t[#t+1]='\\'
        elseif e=='n' then t[#t+1]='\n' elseif e=='t' then t[#t+1]='\t'
        else t[#t+1]=e end; p=p+1
    else t[#t+1]=c; p=p+1 end end end
local dv
local function da(s,p) local a={}; p=ws(s,p); if s:sub(p,p)==']' then return a,p+1 end
    while true do local v; v,p=dv(s,p); a[#a+1]=v; p=ws(s,p)
        if s:sub(p,p)==']' then return a,p+1 end; p=ws(s,p+1) end end
local function do_(s,p) local o={}; p=ws(s,p); if s:sub(p,p)=='}' then return o,p+1 end
    while true do p=ws(s,p); local k; k,p=ds(s,p+1); p=ws(s,p); p=ws(s,p+1)
        local v; v,p=dv(s,p); o[k]=v; p=ws(s,p)
        if s:sub(p,p)=='}' then return o,p+1 end; p=ws(s,p+1) end end
function dv(s,p) p=ws(s,p); local c=s:sub(p,p)
    if c=='"' then return ds(s,p+1) elseif c=='{' then return do_(s,p+1)
    elseif c=='[' then return da(s,p+1) elseif c=='t' then return true,p+4
    elseif c=='f' then return false,p+5 elseif c=='n' then return nil,p+4
    else local n=s:match("^-?%d+%.?%d*[eE]?[+-]?%d*",p); return tonumber(n),p+#n end end
function json.decode(s) return (dv(s,1)) end

-- Helpers
local ffi = require("ffi")
ffi.cdef("uint32_t fnv1a_hash(const char *str);")
local script_dir = arg[0]:match("(.*/)") or "./"
local fnv_lib = ffi.load(script_dir .. "fnv1a.so")

local CTYPE={["bool"]="uint8_t",["uint8_t"]="uint8_t",["uint16_t"]="uint16_t",
    ["uint32_t"]="uint32_t",["uint64_t"]="uint64_t",["int8_t"]="int8_t",
    ["int16_t"]="int16_t",["int32_t"]="int32_t",["int64_t"]="int64_t",
    ["float"]="float",["double"]="double"}
local ESIZES={["bool"]=1,["uint8_t"]=1,["uint16_t"]=2,["uint32_t"]=4,["uint64_t"]=8,
    ["int8_t"]=1,["int16_t"]=2,["int32_t"]=4,["int64_t"]=8,["float"]=4,["double"]=8}
local function p2c(p) return p:gsub("%.","_") end
local function fnv1a(str) return tonumber(fnv_lib.fnv1a_hash(str)) end
local function fhex(v)
    -- Handle LuaJIT signed 32-bit: mask to unsigned via modular arithmetic
    if v < 0 then v = v + 4294967296 end
    return string.format("0x%08Xu", v)
end

local W={}; W.__index=W
function W.new() return setmetatable({l={}},W) end
function W:w(s) self.l[#self.l+1]=s end
function W:save(p) local f=io.open(p,"w"); f:write(table.concat(self.l,"\n")); f:write("\n"); f:close() end

local function each_layer(node, fn)
    if node.output_buffer then fn(node.output_buffer) end
    for _,s in ipairs(node.sub_levels or {}) do each_layer(s, fn) end
end
local function collect_vfts(node, out)
    for _,s in ipairs(node.sub_levels or {}) do collect_vfts(s, out) end
    for _,v in ipairs(node.virtual_functions or {}) do out[#out+1]=v end
end
local function unique_user_vfts(data)
    local seen,list={},{}; local all={}
    for _,lv in ipairs(data.levels) do collect_vfts(lv, all) end
    for _,v in ipairs(all) do if v.class=="user" and not seen[v.user_func] then
        seen[v.user_func]=true; list[#list+1]=v end end
    return list
end

-- =====================================================================
-- Build buf_id mapping: assign a sequential index to every buffer
-- =====================================================================
local function build_buf_table(data)
    local bufs = {}       -- array of {path, key, size, elem_size, is_layer, buf_index}
    local path2id = {}    -- path -> index into bufs[]
    local raw_idx = 0
    local layer_idx = 0

    -- Raw buffers first
    for _, buf in ipairs(data.raw_buffers) do
        local id = #bufs
        bufs[#bufs+1] = {
            path = buf.path, key = fnv1a(buf.path),
            size = buf.size, elem_size = ESIZES[buf.type],
            is_layer = false, buf_index = raw_idx,
            c_type = CTYPE[buf.type], pins = buf.pins,
            level = 0xFF,
        }
        path2id[buf.path] = id
        raw_idx = raw_idx + 1
    end

    -- Layer buffers
    for _, lv in ipairs(data.levels) do
        local lvnum = lv.level_number or 0
        each_layer(lv, function(buf)
            local id = #bufs
            bufs[#bufs+1] = {
                path = buf.path, key = fnv1a(buf.path),
                size = buf.size, elem_size = 1,
                is_layer = true, buf_index = layer_idx,
                c_type = "uint8_t", pins = buf.pins,
                level = lvnum,
            }
            path2id[buf.path] = id
            layer_idx = layer_idx + 1
        end)
    end

    return bufs, path2id, raw_idx, layer_idx
end

-- =====================================================================
-- Generate single .h (defines + const data tables)
-- =====================================================================
local function gen_h(data, name, bufs, path2id, n_raw, n_layer)
    local o = W.new()
    local g = name:upper().."_H"
    o:w("/* "..name..".h - Generated const data for: "..name.." */")
    o:w("#ifndef "..g) o:w("#define "..g) o:w("")
    o:w('#include "scan_tree.h"')

    local uvfts = unique_user_vfts(data)
    if #uvfts > 0 then o:w('#include "'..name..'_user_vft.h"') end

    -- Collect graph nodes and fuse entries
    local allvf = {}
    for _,lv in ipairs(data.levels) do collect_vfts(lv, allvf) end

    -- =================================================================
    -- Compute transitive raw dependency bitmasks per node.
    --
    -- Strategy: nodes are already in bottom-up topological order.
    -- Pass 1: for each node, collect direct input buf_ids.
    --   If input is raw -> set bit directly.
    --   If input is layer -> inherit raw_deps from all nodes that write
    --   to that layer buffer (which have already been processed).
    -- =================================================================
    local n_raw_words = math.ceil(n_raw / 32)
    if n_raw_words < 1 then n_raw_words = 1 end

    -- Build map: layer buf_id -> list of node indices that write to it
    local layer_writers = {}  -- [buf_id] = {node_idx, ...}
    for ni, vf in ipairs(allvf) do
        local obid = path2id[vf.output.buffer]
        if not layer_writers[obid] then layer_writers[obid] = {} end
        local t = layer_writers[obid]
        t[#t+1] = ni  -- 1-based
    end

    -- Compute raw_deps for each node (array of n_raw_words uint32 values)
    local node_raw_deps = {}  -- [node_idx] = {w1, w2, ...}
    local bor, lshift = bit.bor, bit.lshift
    for ni, vf in ipairs(allvf) do
        local deps = {}
        for w = 1, n_raw_words do deps[w] = 0 end

        for _, inp in ipairs(vf.inputs) do
            local ibid = path2id[inp.buffer]
            local bd = bufs[ibid + 1]  -- bufs is 0-indexed id, lua table is 1-indexed
            if not bd.is_layer then
                -- Direct raw dependency: set the bit for this raw buffer's buf_index
                local ri = bd.buf_index
                local word = math.floor(ri / 32) + 1
                local b = ri % 32
                deps[word] = bor(deps[word], lshift(1, b))
            else
                -- Layer dependency: inherit from all nodes that write to this layer
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

    -- Collect fuse entries (node_index, action_func_name)
    local fuses = {}
    for i, vf in ipairs(allvf) do
        if vf.is_fuse then
            fuses[#fuses+1] = {node_id = i-1, action = vf.fuse_action}
        end
    end

    -- Include fuse actions header if any fuses exist
    if #fuses > 0 then o:w('#include "'..name..'_fuse_actions.h"') end
    o:w("")

    -- Pin defines and key defines
    for i, b in ipairs(bufs) do
        local cn = p2c(b.path):upper()
        o:w(string.format("/* buf_id=%d  %s */", i-1, b.path))
        o:w(string.format("#define %s_ID %d", cn, i-1))
        o:w(string.format("#define %s_KEY %s", cn, fhex(b.key)))
        o:w(string.format("#define %s_SIZE %d", cn, b.size))
        for _, pin in ipairs(b.pins or {}) do
            o:w(string.format("#define %s_%s %d", cn, pin.name:upper(), pin.position))
        end
        o:w("")
    end

    -- buf_descs[]
    o:w("static const st_buf_desc_t "..name.."_buf_descs[] = {")
    for i, b in ipairs(bufs) do
        o:w(string.format('    /* [%d] */ {"%s", %s, %d, %d, %d, %d, %d},',
            i-1, b.path, fhex(b.key), b.size, b.elem_size,
            b.is_layer and 1 or 0, b.buf_index, b.level))
    end
    o:w("};")
    o:w("")

    -- node_descs[]
    o:w("static const st_node_desc_t "..name.."_node_descs[] = {")
    for i, vf in ipairs(allvf) do
        local fn = vf.lib_func or vf.user_func
        local obid = path2id[vf.output.buffer]
        local opos = vf.output.start
        local inp_parts = {}
        for _, inp in ipairs(vf.inputs) do
            local ibid = path2id[inp.buffer]
            inp_parts[#inp_parts+1] = string.format("{%d, %d, %d, %d}",
                ibid, inp.start, inp.count, inp.role or 0)
        end
        while #inp_parts < 8 do inp_parts[#inp_parts+1] = "{0,0,0,0}" end

        -- Format raw_deps bitmask
        local deps = node_raw_deps[i]
        local dep_parts = {}
        for w = 1, 8 do
            dep_parts[w] = fhex(deps[w] or 0)
        end

        o:w(string.format("    /* [%d] %s [%s] -> buf[%d][%d] */",
            i-1, vf.vft_name, vf.class, obid, opos))
        o:w(string.format("    {%s, %d, %d, {%s}, %d, {%s}},",
            fn, obid, opos, table.concat(inp_parts, ", "), #vf.inputs,
            table.concat(dep_parts, ", ")))
    end
    o:w("};")
    o:w("")

    -- lookup[] (sorted by key)
    local lkup = {}
    for i, b in ipairs(bufs) do
        lkup[#lkup+1] = {key = b.key, buf_id = i-1}
    end
    table.sort(lkup, function(a,b) return a.key < b.key end)

    o:w("static const st_lookup_entry_t "..name.."_lookup[] = {")
    for _, e in ipairs(lkup) do
        o:w(string.format("    {%s, %d},", fhex(e.key), e.buf_id))
    end
    o:w("};")
    o:w("")

    -- fuse_table[]
    if #fuses > 0 then
        o:w("static const st_fuse_entry_t "..name.."_fuse_table[] = {")
        for _, f in ipairs(fuses) do
            o:w(string.format("    {%d, %s},", f.node_id, f.action))
        end
        o:w("};")
        o:w("")
    end

    -- system_desc
    o:w(string.format("static const st_system_desc_t %s_desc = {", name))
    o:w(string.format('    "%s",', name))
    o:w(string.format("    %s_buf_descs, %d,", name, #bufs))
    o:w(string.format("    %s_node_descs, %d,", name, #allvf))
    o:w(string.format("    %s_lookup, %d,", name, #lkup))
    o:w(string.format("    %d, %d,", n_raw, n_layer))
    if #fuses > 0 then
        o:w(string.format("    %s_fuse_table, %d", name, #fuses))
    else
        o:w("    NULL, 0")
    end
    o:w("};")
    o:w("")

    o:w("#endif /* "..g.." */")
    return o
end

-- =====================================================================
-- Generate user VFT header
-- =====================================================================
local function gen_user_h(data, name)
    local o = W.new()
    local g = name:upper().."_USER_VFT_H"
    local uvfts = unique_user_vfts(data)
    o:w("/* "..name.."_user_vft.h - User VFT prototypes */")
    o:w("#ifndef "..g) o:w("#define "..g) o:w('#include "scan_tree.h"') o:w("")
    for _, vf in ipairs(uvfts) do
        o:w("/* "..vf.vft_name)
        if vf.input_specs then
            o:w(" * Inputs:")
            for _,sp in ipairs(vf.input_specs) do
                o:w(string.format(" *   %s (%s, count=%s)", sp.name, sp.type or "any", tostring(sp.count or "*")))
            end
        end
        o:w(" * Access data: st_buf_data(h, inputs[i].buf_id) + inputs[i].start")
        o:w(" */")
        o:w(string.format("uint8_t %s(uint8_t *state, const st_handle_t *h, const st_input_desc_t *inputs, uint32_t n_inputs);",
            vf.user_func))
        o:w("")
    end
    o:w("#endif")
    return o
end

-- =====================================================================
-- Generate fuse actions header
-- =====================================================================
local function gen_fuse_h(data, name)
    local o = W.new()
    local g = name:upper().."_FUSE_ACTIONS_H"
    o:w("/* "..name.."_fuse_actions.h - Fuse action callback prototypes */")
    o:w("#ifndef "..g) o:w("#define "..g) o:w("")

    -- Collect all fuse actions
    local allvf = {}
    for _,lv in ipairs(data.levels) do collect_vfts(lv, allvf) end

    local seen = {}
    for _, vf in ipairs(allvf) do
        if vf.is_fuse and not seen[vf.fuse_action] then
            o:w("/*")
            o:w(" * "..vf.fuse_action)
            o:w(" *   Triggered when fuse blows for: "..vf.vft_name)
            o:w(" *   Input: "..vf.inputs[1].buffer..":"..vf.inputs[1].start)
            o:w(" *   Clear: "..vf.inputs[2].buffer..":"..vf.inputs[2].start)
            o:w(" */")
            o:w(string.format("extern void %s(void *user_handle);", vf.fuse_action))
            o:w("")
            seen[vf.fuse_action] = true
        end
    end

    o:w("#endif")
    return o
end

-- =====================================================================
-- Generate test
-- =====================================================================
local function gen_test(data, name, bufs)
    local o = W.new()
    o:w("/* "..name.."_test.c */")
    o:w('#include "'..name..'.h"')
    o:w("#include <stdio.h>")
    o:w("#include <stdlib.h>")
    o:w("")

    -- Stub user VFTs
    for _, vf in ipairs(unique_user_vfts(data)) do
        o:w(string.format("uint8_t %s(uint8_t *state, const st_handle_t *h, const st_input_desc_t *inputs, uint32_t n_inputs)",
            vf.user_func))
        o:w("{")
        o:w("    (void)state; (void)n_inputs;")
        o:w("    float cur = ((const float*)st_buf_data(h, inputs[0].buf_id))[inputs[0].start];")
        o:w("    float thr = ((const float*)st_buf_data(h, inputs[1].buf_id))[inputs[1].start];")
        o:w("    return (cur < thr) ? 1 : 0;")
        o:w("}") o:w("")
    end

    o:w('static const char *sn(int8_t s){return s==1?"ACTIVE":s==0?"FAULT":"NOT_OP";}')
    o:w("")
    o:w("int main(void)")
    o:w("{")
    o:w("    st_handle_t h;")
    o:w(string.format("    if (st_init(&h, &%s_desc) != 0) { fprintf(stderr, \"init failed\\n\"); return 1; }", name))
    o:w("")
    o:w(string.format('    printf("%%s: %%u bufs, %%u nodes, %%u raw, %%u layer\\n",'))
    o:w("           h.desc->name, h.desc->n_bufs, h.desc->n_nodes, h.desc->n_raw, h.desc->n_layer);")
    o:w("")

    -- Print layer states
    o:w("    /* Print all layer states */")
    o:w("    #define PRINT_LAYERS() do { \\")
    o:w("        for (uint16_t _i = 0; _i < h.desc->n_bufs; _i++) { \\")
    o:w("            if (!h.desc->buf_descs[_i].is_layer) continue; \\")
    o:w('            printf("  %s:", h.desc->buf_descs[_i].path); \\')
    o:w("            for (uint16_t _j = 0; _j < h.desc->buf_descs[_i].size; _j++) \\")
    o:w('                printf(" [%u]=%s", _j, sn(st_get_state(&h, _i, _j))); \\')
    o:w('            printf("\\n"); \\')
    o:w("        } \\")
    o:w("    } while(0)")
    o:w("")

    o:w('    printf("\\n=== Initial ===\\n");')
    o:w("    PRINT_LAYERS();")
    o:w("")

    -- Power ON + thresholds
    o:w('    printf("\\n=== Power ON, thresholds=100A ===\\n");')
    o:w("    {")
    o:w(string.format('        int32_t ps = st_lookup_path(&h, "%s.power_status");', name))
    o:w('        if (ps >= 0) ((uint8_t*)st_raw_current(&h, ps))[0] = 1;')
    o:w(string.format('        int32_t mt = st_lookup_path(&h, "%s.motor_thresholds");', name))
    o:w('        if (mt >= 0) for(int i=0;i<4;i++) ((float*)st_raw_current(&h, mt))[i] = 100.0f;')
    o:w("    }")
    o:w("    st_cycle(&h);")
    o:w("    PRINT_LAYERS();")
    o:w("")

    -- Pump 0 overcurrent
    o:w('    printf("\\n=== Pump 0 overcurrent (150A) ===\\n");')
    o:w("    {")
    o:w(string.format('        int32_t mc = st_lookup_path(&h, "%s.motor_current");', name))
    o:w('        if (mc >= 0) ((float*)st_raw_current(&h, mc))[0] = 150.0f;')
    o:w("    }")
    o:w("    st_cycle(&h);")
    o:w("    PRINT_LAYERS();")
    o:w("")

    -- Key lookup
    o:w('    printf("\\n=== Key lookup ===\\n");')
    o:w("    {")
    -- Use the first layer buffer as example
    for _, b in ipairs(bufs) do
        if b.is_layer then
            local cn = p2c(b.path):upper()
            o:w(string.format('        int32_t id = st_lookup_key(&h, %s_KEY);', cn))
            o:w(string.format('        if (id >= 0) { printf("  %s[0] = %%s\\n", sn(st_get_state(&h, id, 0))); }', b.path))
            break
        end
    end
    o:w("    }")
    o:w("")

    o:w("    st_destroy(&h);")
    o:w('    printf("Done.\\n");')
    o:w("    return 0;")
    o:w("}")
    return o
end

-- =====================================================================
-- Makefile
-- =====================================================================
local function gen_mk(name)
    local o=W.new()
    o:w("CC = gcc")
    o:w("CFLAGS = -Wall -Wextra -std=c99 -O2")
    o:w("LIB = ../lib")
    o:w(string.format("all: %s_test",name))
    o:w(string.format("%s_test: %s_test.c %s.c %s.h $(LIB)/scan_tree_lib.c $(LIB)/scan_tree_lib.h",name,name,name,name))
    o:w(string.format("\t$(CC) $(CFLAGS) -I$(LIB) -I. -o $@ %s_test.c %s.c $(LIB)/scan_tree_lib.c",name,name))
    o:w(string.format("clean:\n\trm -f %s_test",name))
    o:w(".PHONY: all clean")
    return o
end

-- =====================================================================
-- Main
-- =====================================================================
local f=io.open(arg[1],"r"); local s=f:read("*a"); f:close()
local data=json.decode(s)
local name=data.name or "scan_tree"
local dir=arg[2] or "c_output"
os.execute("mkdir -p "..dir)

local bufs, path2id, n_raw, n_layer = build_buf_table(data)

gen_h(data, name, bufs, path2id, n_raw, n_layer):save(dir.."/"..name..".h")
print("Generated: "..dir.."/"..name..".h")

if #unique_user_vfts(data) > 0 then
    gen_user_h(data, name):save(dir.."/"..name.."_user_vft.h")
    print("Generated: "..dir.."/"..name.."_user_vft.h")
end

-- Check for fuses
local has_fuses = false
do
    local allvf = {}
    for _,lv in ipairs(data.levels) do collect_vfts(lv, allvf) end
    for _, vf in ipairs(allvf) do
        if vf.is_fuse then has_fuses = true; break end
    end
end
if has_fuses then
    gen_fuse_h(data, name):save(dir.."/"..name.."_fuse_actions.h")
    print("Generated: "..dir.."/"..name.."_fuse_actions.h")
end