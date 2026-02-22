#!/usr/bin/env luajit
--[[
    Scan Tree C Code Generator (LuaJIT)

    Reads the JSON intermediate file produced by the DSL and generates:
      1. A header file with buffer structs, pin defines, and function declarations
      2. A source file with VFT runtime implementations, buffer instances,
         the graph evaluation engine, and initialization
      3. A test main.c that exercises the generated code
      4. A Makefile

    Usage: luajit codegen_c.lua <json_file> [output_dir]
--]]

-- =====================================================================
-- Minimal JSON decoder (no external dependencies)
-- =====================================================================

local json = {}

local function skip_whitespace(str, pos)
    return str:match("^%s*()", pos)
end

local function decode_string(str, pos)
    -- pos should be right after opening "
    local parts = {}
    while pos <= #str do
        local c = str:sub(pos, pos)
        if c == '"' then
            return table.concat(parts), pos + 1
        elseif c == '\\' then
            pos = pos + 1
            local esc = str:sub(pos, pos)
            if esc == '"' then parts[#parts+1] = '"'
            elseif esc == '\\' then parts[#parts+1] = '\\'
            elseif esc == '/' then parts[#parts+1] = '/'
            elseif esc == 'n' then parts[#parts+1] = '\n'
            elseif esc == 'r' then parts[#parts+1] = '\r'
            elseif esc == 't' then parts[#parts+1] = '\t'
            elseif esc == 'u' then
                local hex = str:sub(pos+1, pos+4)
                parts[#parts+1] = string.char(tonumber(hex, 16))
                pos = pos + 4
            end
            pos = pos + 1
        else
            parts[#parts+1] = c
            pos = pos + 1
        end
    end
    error("Unterminated string")
end

local decode_value  -- forward declaration

local function decode_array(str, pos)
    local arr = {}
    pos = skip_whitespace(str, pos)
    if str:sub(pos, pos) == ']' then
        return arr, pos + 1
    end
    while true do
        local val
        val, pos = decode_value(str, pos)
        arr[#arr+1] = val
        pos = skip_whitespace(str, pos)
        local c = str:sub(pos, pos)
        if c == ']' then return arr, pos + 1 end
        if c == ',' then pos = skip_whitespace(str, pos + 1) end
    end
end

local function decode_object(str, pos)
    local obj = {}
    pos = skip_whitespace(str, pos)
    if str:sub(pos, pos) == '}' then
        return obj, pos + 1
    end
    while true do
        pos = skip_whitespace(str, pos)
        if str:sub(pos, pos) ~= '"' then error("Expected string key at pos " .. pos) end
        local key
        key, pos = decode_string(str, pos + 1)
        pos = skip_whitespace(str, pos)
        if str:sub(pos, pos) ~= ':' then error("Expected ':' at pos " .. pos) end
        pos = skip_whitespace(str, pos + 1)
        local val
        val, pos = decode_value(str, pos)
        obj[key] = val
        pos = skip_whitespace(str, pos)
        local c = str:sub(pos, pos)
        if c == '}' then return obj, pos + 1 end
        if c == ',' then pos = skip_whitespace(str, pos + 1) end
    end
end

function decode_value(str, pos)
    pos = skip_whitespace(str, pos)
    local c = str:sub(pos, pos)
    if c == '"' then
        return decode_string(str, pos + 1)
    elseif c == '{' then
        return decode_object(str, pos + 1)
    elseif c == '[' then
        return decode_array(str, pos + 1)
    elseif c == 't' then
        return true, pos + 4
    elseif c == 'f' then
        return false, pos + 5
    elseif c == 'n' then
        return nil, pos + 4
    else
        -- number
        local num_str = str:match("^-?%d+%.?%d*[eE]?[+-]?%d*", pos)
        if not num_str then error("Invalid value at pos " .. pos) end
        return tonumber(num_str), pos + #num_str
    end
end

function json.decode(str)
    local val, _ = decode_value(str, 1)
    return val
end

-- =====================================================================
-- C type mapping
-- =====================================================================

local C_TYPE_MAP = {
    ["bool"]     = "uint8_t",
    ["uint8_t"]  = "uint8_t",
    ["uint16_t"] = "uint16_t",
    ["uint32_t"] = "uint32_t",
    ["uint64_t"] = "uint64_t",
    ["int8_t"]   = "int8_t",
    ["int16_t"]  = "int16_t",
    ["int32_t"]  = "int32_t",
    ["int64_t"]  = "int64_t",
    ["float"]    = "float",
    ["double"]   = "double",
}

local function path_to_c_id(path)
    return path:gsub("%.", "_")
end

-- =====================================================================
-- File writer helper
-- =====================================================================

local Writer = {}
Writer.__index = Writer

function Writer.new()
    local self = setmetatable({}, Writer)
    self.lines = {}
    return self
end

function Writer:w(line)
    self.lines[#self.lines + 1] = line
end

function Writer:to_string()
    return table.concat(self.lines, "\n")
end

function Writer:write_file(path)
    local f = io.open(path, "w")
    f:write(self:to_string())
    f:write("\n")
    f:close()
end

-- =====================================================================
-- Tree traversal helpers
-- =====================================================================

local function for_each_layer(node, fn)
    local buf = node.output_buffer
    if buf then fn(buf) end
    for _, sub in ipairs(node.sub_levels or {}) do
        for_each_layer(sub, fn)
    end
end

local function count_latches_in(node)
    local count = 0
    for _, vf in ipairs(node.virtual_functions or {}) do
        if vf.vft_name == "VFT_latch" then count = count + 1 end
    end
    for _, sub in ipairs(node.sub_levels or {}) do
        count = count + count_latches_in(sub)
    end
    return count
end

-- =====================================================================
-- Header generator
-- =====================================================================

local function generate_header(data, tree_name)
    local o = Writer.new()
    local guard = tree_name:upper() .. "_H"

    o:w(string.format("/* %s.h", tree_name))
    o:w(" * Auto-generated by Scan Tree C Code Generator (LuaJIT)")
    o:w(string.format(" * Source: %s", tree_name))
    o:w(" *")
    o:w(" * DO NOT EDIT - generated from the Scan Tree JSON intermediate file.")
    o:w(" */")
    o:w("")
    o:w(string.format("#ifndef %s", guard))
    o:w(string.format("#define %s", guard))
    o:w("")
    o:w("#include <stdint.h>")
    o:w("#include <stdbool.h>")
    o:w("#include <string.h>")
    o:w("")

    -- Three-state layer bitmap struct
    o:w("/* =============================================================")
    o:w(" * Three-State Layer Bitmap")
    o:w(" *")
    o:w(" * value[i]:      0 = fault/false, 1 = active/true")
    o:w(" * not_active[i]: 1 = never written (initial), 0 = has been written")
    o:w(" * shadow[i]:     previous value for change detection")
    o:w(" *")
    o:w(" * Three states:")
    o:w(" *   not_active=1           -> Not Operational")
    o:w(" *   not_active=0, value=1  -> Active")
    o:w(" *   not_active=0, value=0  -> Fault")
    o:w(" * ============================================================= */")
    o:w("typedef struct {")
    o:w("    uint8_t *value;")
    o:w("    uint8_t *not_active;")
    o:w("    uint8_t *shadow;")
    o:w("    uint32_t size;")
    o:w("} scan_tree_layer_buf_t;")
    o:w("")

    -- Double-buffered raw buffer struct
    o:w("/* =============================================================")
    o:w(" * Double-Buffered Raw Buffer")
    o:w(" *")
    o:w(" * current:  current values (written by I/O)")
    o:w(" * previous: values from last scan (for change detection)")
    o:w(" * changed:  flag set by swap_raw_buffers()")
    o:w(" * ============================================================= */")
    o:w("typedef struct {")
    o:w("    void *current;")
    o:w("    void *previous;")
    o:w("    uint32_t size;")
    o:w("    uint32_t elem_size;")
    o:w("    uint8_t  changed;")
    o:w("} scan_tree_raw_buf_t;")
    o:w("")

    -- Raw buffer pin defines
    o:w("/* =============================================================")
    o:w(" * Raw Buffer Pin Indices")
    o:w(" * ============================================================= */")
    for _, buf in ipairs(data.raw_buffers) do
        local c_name = path_to_c_id(buf.path):upper()
        o:w("")
        o:w(string.format("/* %s */", buf.description))
        o:w(string.format("#define %s_SIZE %d", c_name, buf.size))
        for _, pin in ipairs(buf.pins) do
            o:w(string.format("#define %s_%s %d  /* %s */",
                c_name, pin.name:upper(), pin.position, pin.description))
        end
    end
    o:w("")

    -- Layer buffer pin defines
    o:w("/* =============================================================")
    o:w(" * Layer Buffer Pin Indices")
    o:w(" * ============================================================= */")

    for _, level in ipairs(data.levels) do
        for_each_layer(level, function(buf)
            local c_name = path_to_c_id(buf.path):upper()
            o:w("")
            o:w(string.format("/* %s */", buf.description))
            o:w(string.format("#define %s_SIZE %d", c_name, buf.size))
            for _, pin in ipairs(buf.pins) do
                o:w(string.format("#define %s_%s %d  /* %s */",
                    c_name, pin.name:upper(), pin.position, pin.description))
            end
        end)
    end
    o:w("")

    -- Context struct
    o:w("/* =============================================================")
    o:w(" * Scan Tree Context")
    o:w(" * ============================================================= */")
    o:w("typedef struct {")
    o:w("    /* Raw buffers (double-buffered) */")
    for _, buf in ipairs(data.raw_buffers) do
        o:w(string.format("    scan_tree_raw_buf_t %s;", path_to_c_id(buf.path)))
    end
    o:w("")
    o:w("    /* Layer buffers (three-state) */")
    for _, level in ipairs(data.levels) do
        for_each_layer(level, function(buf)
            o:w(string.format("    scan_tree_layer_buf_t %s;", path_to_c_id(buf.path)))
        end)
    end
    o:w("")
    o:w("    /* VFT latch internal state */")

    local total_latches = 0
    for _, level in ipairs(data.levels) do
        total_latches = total_latches + count_latches_in(level)
    end

    if total_latches > 0 then
        o:w(string.format("    uint8_t latch_state[%d];", total_latches))
    else
        o:w("    uint8_t _no_latches;  /* placeholder */")
    end

    o:w(string.format("} %s_ctx_t;", tree_name))
    o:w("")

    -- Function declarations
    o:w("/* =============================================================")
    o:w(" * API Functions")
    o:w(" * ============================================================= */")
    o:w("")
    o:w("/* Initialize all buffers. Layer buffers set to Not Active. */")
    o:w(string.format("void %s_init(%s_ctx_t *ctx);", tree_name, tree_name))
    o:w("")
    o:w("/* Swap raw double buffers and detect changes. Call before evaluate. */")
    o:w(string.format("void %s_swap_raw_buffers(%s_ctx_t *ctx);", tree_name, tree_name))
    o:w("")
    o:w("/* Evaluate the computational graph (change-driven). */")
    o:w(string.format("void %s_evaluate(%s_ctx_t *ctx);", tree_name, tree_name))
    o:w("")
    o:w("/* Read access: get three-state value for a layer buffer position. */")
    o:w("/*   Returns: 0 = Fault, 1 = Active, -1 = Not Operational */")
    o:w(string.format("int8_t %s_get_state(const scan_tree_layer_buf_t *buf, uint32_t pos);", tree_name))
    o:w("")

    -- Raw buffer accessor macros
    o:w("/* =============================================================")
    o:w(" * Raw Buffer Access Macros")
    o:w(" * ============================================================= */")
    for _, buf in ipairs(data.raw_buffers) do
        local c_id = path_to_c_id(buf.path)
        local c_type = C_TYPE_MAP[buf.type]
        o:w(string.format("#define %s_CURRENT(ctx) ((%s*)(ctx)->%s.current)",
            c_id:upper(), c_type, c_id))
        o:w(string.format("#define %s_PREVIOUS(ctx) ((%s*)(ctx)->%s.previous)",
            c_id:upper(), c_type, c_id))
    end
    o:w("")

    o:w(string.format("#endif /* %s */", guard))

    return o
end

-- =====================================================================
-- Source generator
-- =====================================================================

local function generate_source(data, tree_name)
    local o = Writer.new()

    o:w(string.format("/* %s.c", tree_name))
    o:w(" * Auto-generated by Scan Tree C Code Generator (LuaJIT)")
    o:w(string.format(" * Source: %s", tree_name))
    o:w(" *")
    o:w(" * DO NOT EDIT - generated from the Scan Tree JSON intermediate file.")
    o:w(" */")
    o:w("")
    o:w(string.format('#include "%s.h"', tree_name))
    o:w("#include <stdlib.h>")
    o:w("#include <string.h>")
    o:w("")

    -- Static storage for raw buffers
    o:w("/* =============================================================")
    o:w(" * Static Storage for Raw Buffer Double-Buffering")
    o:w(" * ============================================================= */")
    for _, buf in ipairs(data.raw_buffers) do
        local c_id = path_to_c_id(buf.path)
        local c_type = C_TYPE_MAP[buf.type]
        o:w(string.format("static %s %s_buf_a[%d];", c_type, c_id, buf.size))
        o:w(string.format("static %s %s_buf_b[%d];", c_type, c_id, buf.size))
    end
    o:w("")

    -- Static storage for layer buffers
    o:w("/* =============================================================")
    o:w(" * Static Storage for Layer Buffers")
    o:w(" * ============================================================= */")
    for _, level in ipairs(data.levels) do
        for_each_layer(level, function(buf)
            local c_id = path_to_c_id(buf.path)
            o:w(string.format("static uint8_t %s_value[%d];", c_id, buf.size))
            o:w(string.format("static uint8_t %s_not_active[%d];", c_id, buf.size))
            o:w(string.format("static uint8_t %s_shadow[%d];", c_id, buf.size))
        end)
    end
    o:w("")

    -- VFT runtime implementations
    o:w("/* =============================================================")
    o:w(" * VFT Runtime Implementations")
    o:w(" * ============================================================= */")
    o:w("")
    o:w("/* Write a value to a layer buffer position, clearing not_active. */")
    o:w("static inline void layer_write(scan_tree_layer_buf_t *buf, uint32_t pos, uint8_t val)")
    o:w("{")
    o:w("    buf->not_active[pos] = 0;")
    o:w("    buf->value[pos] = val;")
    o:w("}")
    o:w("")
    o:w("/* Check if a layer buffer position changed since last shadow copy. */")
    o:w("static inline uint8_t layer_changed(const scan_tree_layer_buf_t *buf, uint32_t pos)")
    o:w("{")
    o:w("    return (buf->value[pos] != buf->shadow[pos]) ||")
    o:w("           (buf->not_active[pos] != 1 && buf->shadow[pos] == 0 && buf->value[pos] == 0);")
    o:w("}")
    o:w("")
    o:w("/* Update shadow to current for a layer buffer position. */")
    o:w("static inline void layer_update_shadow(scan_tree_layer_buf_t *buf, uint32_t pos)")
    o:w("{")
    o:w("    buf->shadow[pos] = buf->value[pos];")
    o:w("}")
    o:w("")
    o:w("/* VFT_and: output = AND of all input bits */")
    o:w("static uint8_t vft_and(const uint8_t *bits, uint32_t start, uint32_t count)")
    o:w("{")
    o:w("    for (uint32_t i = 0; i < count; i++) {")
    o:w("        if (!bits[start + i]) return 0;")
    o:w("    }")
    o:w("    return 1;")
    o:w("}")
    o:w("")
    o:w("/* VFT_or: output = OR of all input bits */")
    o:w("static uint8_t vft_or(const uint8_t *bits, uint32_t start, uint32_t count)")
    o:w("{")
    o:w("    for (uint32_t i = 0; i < count; i++) {")
    o:w("        if (bits[start + i]) return 1;")
    o:w("    }")
    o:w("    return 0;")
    o:w("}")
    o:w("")
    o:w("/* VFT_not: output = NOT of single input bit */")
    o:w("static uint8_t vft_not(uint8_t bit)")
    o:w("{")
    o:w("    return bit ? 0 : 1;")
    o:w("}")
    o:w("")
    o:w("/* VFT_latch: output latches on set, clears on clear */")
    o:w("static uint8_t vft_latch(uint8_t *state, uint8_t set_bit, uint8_t clear_bit)")
    o:w("{")
    o:w("    if (clear_bit) {")
    o:w("        *state = 0;")
    o:w("    } else if (set_bit) {")
    o:w("        *state = 1;")
    o:w("    }")
    o:w("    return *state;")
    o:w("}")
    o:w("")
    o:w("/* VFT_k_of_n: output = (popcount of input bits >= threshold) */")
    o:w("static uint8_t vft_k_of_n(uint8_t threshold, const uint8_t *bits, uint32_t start, uint32_t count)")
    o:w("{")
    o:w("    uint8_t pop = 0;")
    o:w("    for (uint32_t i = 0; i < count; i++) {")
    o:w("        if (bits[start + i]) pop++;")
    o:w("    }")
    o:w("    return (pop >= threshold) ? 1 : 0;")
    o:w("}")
    o:w("")
    o:w("/* VFT_gt: output = (a > b), with auto type conversion via float */")
    o:w("static uint8_t vft_gt_float(float a, float b)")
    o:w("{")
    o:w("    return (a > b) ? 1 : 0;")
    o:w("}")
    o:w("")

    -- Get state
    o:w("/* =============================================================")
    o:w(" * Read Access")
    o:w(" * ============================================================= */")
    o:w(string.format("int8_t %s_get_state(const scan_tree_layer_buf_t *buf, uint32_t pos)", tree_name))
    o:w("{")
    o:w("    if (pos >= buf->size) return -1;")
    o:w("    if (buf->not_active[pos]) return -1;  /* Not Operational */")
    o:w("    return buf->value[pos] ? 1 : 0;       /* Active or Fault */")
    o:w("}")
    o:w("")

    -- Initialization
    o:w("/* =============================================================")
    o:w(" * Initialization")
    o:w(" * ============================================================= */")
    o:w(string.format("void %s_init(%s_ctx_t *ctx)", tree_name, tree_name))
    o:w("{")
    o:w("    memset(ctx, 0, sizeof(*ctx));")
    o:w("")

    for _, buf in ipairs(data.raw_buffers) do
        local c_id = path_to_c_id(buf.path)
        local c_type = C_TYPE_MAP[buf.type]
        o:w(string.format("    /* Raw: %s */", buf.description))
        o:w(string.format("    ctx->%s.current   = %s_buf_a;", c_id, c_id))
        o:w(string.format("    ctx->%s.previous  = %s_buf_b;", c_id, c_id))
        o:w(string.format("    ctx->%s.size      = %d;", c_id, buf.size))
        o:w(string.format("    ctx->%s.elem_size = sizeof(%s);", c_id, c_type))
        o:w(string.format("    ctx->%s.changed   = 1;  /* Force first evaluation */", c_id))
        o:w(string.format("    memset(%s_buf_a, 0, sizeof(%s_buf_a));", c_id, c_id))
        o:w(string.format("    memset(%s_buf_b, 0, sizeof(%s_buf_b));", c_id, c_id))
        o:w("")
    end

    for _, level in ipairs(data.levels) do
        for_each_layer(level, function(buf)
            local c_id = path_to_c_id(buf.path)
            o:w(string.format("    /* Layer: %s */", buf.description))
            o:w(string.format("    ctx->%s.value      = %s_value;", c_id, c_id))
            o:w(string.format("    ctx->%s.not_active  = %s_not_active;", c_id, c_id))
            o:w(string.format("    ctx->%s.shadow      = %s_shadow;", c_id, c_id))
            o:w(string.format("    ctx->%s.size        = %d;", c_id, buf.size))
            o:w(string.format("    memset(%s_value, 0, %d);", c_id, buf.size))
            o:w(string.format("    memset(%s_not_active, 1, %d);  /* All Not Active */", c_id, buf.size))
            o:w(string.format("    memset(%s_shadow, 0, %d);", c_id, buf.size))
            o:w("")
        end)
    end

    o:w("}")
    o:w("")

    -- Raw buffer swap
    o:w("/* =============================================================")
    o:w(" * Raw Buffer Swap (call before evaluate)")
    o:w(" * ============================================================= */")
    o:w(string.format("void %s_swap_raw_buffers(%s_ctx_t *ctx)", tree_name, tree_name))
    o:w("{")

    for _, buf in ipairs(data.raw_buffers) do
        local c_id = path_to_c_id(buf.path)
        local byte_size = string.format("ctx->%s.size * ctx->%s.elem_size", c_id, c_id)
        o:w(string.format("    /* %s: compare current vs previous */", buf.name))
        o:w(string.format("    ctx->%s.changed = (memcmp(ctx->%s.current, ctx->%s.previous, %s) != 0) ? 1 : 0;",
            c_id, c_id, c_id, byte_size))
        o:w(string.format("    if (ctx->%s.changed) {", c_id))
        o:w(string.format("        memcpy(ctx->%s.previous, ctx->%s.current, %s);",
            c_id, c_id, byte_size))
        o:w("    }")
        o:w("")
    end

    o:w("}")
    o:w("")

    -- Graph evaluation engine
    o:w("/* =============================================================")
    o:w(" * Graph Evaluation Engine")
    o:w(" *")
    o:w(" * Evaluation order: level 0 -> level N")
    o:w(" * Within a level: deepest sub-levels first, then parent")
    o:w(" * Change-driven: propagation stops when output unchanged")
    o:w(" * ============================================================= */")
    o:w(string.format("void %s_evaluate(%s_ctx_t *ctx)", tree_name, tree_name))
    o:w("{")
    o:w("    uint8_t result;")
    o:w("    (void)result;  /* suppress unused warning if no VFTs */")
    o:w("")

    -- Build buffer path -> accessor map
    local buf_map = {}
    for _, buf in ipairs(data.raw_buffers) do
        local c_id = path_to_c_id(buf.path)
        local c_type = C_TYPE_MAP[buf.type]
        buf_map[buf.path] = {
            accessor = string.format("((%s*)ctx->%s.current)", c_type, c_id),
            c_id = c_id,
            category = "raw",
        }
    end

    for _, level in ipairs(data.levels) do
        for_each_layer(level, function(buf)
            local c_id = path_to_c_id(buf.path)
            buf_map[buf.path] = {
                accessor = string.format("ctx->%s.value", c_id),
                c_id = c_id,
                category = "layer",
            }
        end)
    end

    local latch_idx = 0

    local function emit_vft_eval(node, pad)
        -- Sub-levels first (bottom-up)
        for _, sub in ipairs(node.sub_levels or {}) do
            o:w(string.format("%s/* Sub-level: %s */", pad, sub.name))
            emit_vft_eval(sub, pad)
        end

        -- Virtual functions in this scope
        for _, vf in ipairs(node.virtual_functions or {}) do
            local vft = vf.vft_name
            local out = vf.output
            local out_info = buf_map[out.buffer]
            local out_c_id = out_info.c_id
            local out_pos = out.start

            o:w(string.format("%s/* %s -> %s[%d] */", pad, vft, out.buffer, out_pos))

            if vft == "VFT_and" then
                local inp = vf.inputs[1]
                local inp_info = buf_map[inp.buffer]
                o:w(string.format("%sresult = vft_and(%s, %d, %d);",
                    pad, inp_info.accessor, inp.start, inp.count))
                o:w(string.format("%slayer_write(&ctx->%s, %d, result);",
                    pad, out_c_id, out_pos))

            elseif vft == "VFT_or" then
                local inp = vf.inputs[1]
                local inp_info = buf_map[inp.buffer]
                o:w(string.format("%sresult = vft_or(%s, %d, %d);",
                    pad, inp_info.accessor, inp.start, inp.count))
                o:w(string.format("%slayer_write(&ctx->%s, %d, result);",
                    pad, out_c_id, out_pos))

            elseif vft == "VFT_not" then
                local inp = vf.inputs[1]
                local inp_info = buf_map[inp.buffer]
                o:w(string.format("%sresult = vft_not(%s[%d]);",
                    pad, inp_info.accessor, inp.start))
                o:w(string.format("%slayer_write(&ctx->%s, %d, result);",
                    pad, out_c_id, out_pos))

            elseif vft == "VFT_latch" then
                local set_inp, clr_inp
                for _, inp in ipairs(vf.inputs) do
                    if inp.role == "set" then set_inp = inp end
                    if inp.role == "clear" then clr_inp = inp end
                end
                local set_info = buf_map[set_inp.buffer]
                local clr_info = buf_map[clr_inp.buffer]
                o:w(string.format("%sresult = vft_latch(&ctx->latch_state[%d],",
                    pad, latch_idx))
                o:w(string.format("%s                   %s[%d],",
                    pad, set_info.accessor, set_inp.start))
                o:w(string.format("%s                   %s[%d]);",
                    pad, clr_info.accessor, clr_inp.start))
                o:w(string.format("%slayer_write(&ctx->%s, %d, result);",
                    pad, out_c_id, out_pos))
                latch_idx = latch_idx + 1

            elseif vft == "VFT_k_of_n" then
                local thresh_inp, bits_inp
                for _, inp in ipairs(vf.inputs) do
                    if inp.role == "threshold" then thresh_inp = inp end
                    if inp.role == "bits" then bits_inp = inp end
                end
                local thresh_info = buf_map[thresh_inp.buffer]
                local bits_info = buf_map[bits_inp.buffer]
                o:w(string.format("%sresult = vft_k_of_n(%s[%d],",
                    pad, thresh_info.accessor, thresh_inp.start))
                o:w(string.format("%s                    %s, %d, %d);",
                    pad, bits_info.accessor, bits_inp.start, bits_inp.count))
                o:w(string.format("%slayer_write(&ctx->%s, %d, result);",
                    pad, out_c_id, out_pos))

            elseif vft == "VFT_gt" then
                local inp_a, inp_b
                for _, inp in ipairs(vf.inputs) do
                    if inp.role == "a" then inp_a = inp end
                    if inp.role == "b" then inp_b = inp end
                end
                local a_info = buf_map[inp_a.buffer]
                local b_info = buf_map[inp_b.buffer]
                o:w(string.format("%sresult = vft_gt_float((float)%s[%d],",
                    pad, a_info.accessor, inp_a.start))
                o:w(string.format("%s                     (float)%s[%d]);",
                    pad, b_info.accessor, inp_b.start))
                o:w(string.format("%slayer_write(&ctx->%s, %d, result);",
                    pad, out_c_id, out_pos))
            end

            o:w("")
        end
    end

    for _, level in ipairs(data.levels) do
        o:w(string.format("    /* ========== Level %d: %s ========== */",
            level.level_number, level.name))
        emit_vft_eval(level, "    ")
    end

    -- Update shadows
    o:w("    /* Update layer shadows for next change detection */")
    for _, level in ipairs(data.levels) do
        for_each_layer(level, function(buf)
            local c_id = path_to_c_id(buf.path)
            o:w(string.format("    memcpy(ctx->%s.shadow, ctx->%s.value, %d);",
                c_id, c_id, buf.size))
        end)
    end

    o:w("}")
    o:w("")

    return o
end

-- =====================================================================
-- Test main generator
-- =====================================================================

local function generate_test_main(data, tree_name)
    local o = Writer.new()

    o:w(string.format("/* %s_test.c", tree_name))
    o:w(string.format(" * Auto-generated test for Scan Tree: %s", tree_name))
    o:w(" */")
    o:w("")
    o:w(string.format('#include "%s.h"', tree_name))
    o:w("#include <stdio.h>")
    o:w("#include <string.h>")
    o:w("")
    o:w("static const char *state_name(int8_t state)")
    o:w("{")
    o:w('    switch (state) {')
    o:w('        case  1: return "ACTIVE";')
    o:w('        case  0: return "FAULT";')
    o:w('        case -1: return "NOT_OPERATIONAL";')
    o:w('        default: return "UNKNOWN";')
    o:w("    }")
    o:w("}")
    o:w("")
    o:w("static void print_layer(const char *name, const scan_tree_layer_buf_t *buf)")
    o:w("{")
    o:w('    printf("  %s: ", name);')
    o:w("    for (uint32_t i = 0; i < buf->size; i++) {")
    o:w('        int8_t s = buf->not_active[i] ? -1 : (buf->value[i] ? 1 : 0);')
    o:w('        printf("[%u]=%s ", i, state_name(s));')
    o:w("    }")
    o:w('    printf("\\n");')
    o:w("}")
    o:w("")
    o:w("int main(void)")
    o:w("{")
    o:w(string.format("    %s_ctx_t ctx;", tree_name))
    o:w(string.format("    %s_init(&ctx);", tree_name))
    o:w("")

    -- Collect layer print calls
    local function emit_prints(node)
        local buf = node.output_buffer
        if buf then
            local c_id = path_to_c_id(buf.path)
            o:w(string.format('    print_layer("%s", &ctx.%s);', buf.name, c_id))
        end
        for _, sub in ipairs(node.sub_levels or {}) do
            emit_prints(sub)
        end
    end

    -- Helper to find raw buffer by name
    local function find_raw(name)
        for _, buf in ipairs(data.raw_buffers) do
            if buf.name == name then return buf end
        end
        return nil
    end

    -- Initial state
    o:w('    printf("=== Initial State (all Not Operational) ===\\n");')
    for _, level in ipairs(data.levels) do emit_prints(level) end

    -- Scenario 1
    o:w("")
    o:w('    printf("\\n=== Scenario 1: Grid power ON, no faults ===\\n");')
    local ps = find_raw("power_status")
    if ps then
        local c_id = path_to_c_id(ps.path)
        local c_type = C_TYPE_MAP[ps.type]
        o:w(string.format("    ((%s*)ctx.%s.current)[0] = 1;  /* grid power ON */", c_type, c_id))
    end
    o:w(string.format("    %s_swap_raw_buffers(&ctx);", tree_name))
    o:w(string.format("    %s_evaluate(&ctx);", tree_name))
    for _, level in ipairs(data.levels) do emit_prints(level) end

    -- Scenario 2
    o:w("")
    o:w('    printf("\\n=== Scenario 2: Pump 0 faults ===\\n");')
    local pf = find_raw("pump_faults")
    if pf then
        local c_id = path_to_c_id(pf.path)
        local c_type = C_TYPE_MAP[pf.type]
        o:w(string.format("    ((%s*)ctx.%s.current)[0] = 1;  /* pump 0 fault */", c_type, c_id))
    end
    o:w(string.format("    %s_swap_raw_buffers(&ctx);", tree_name))
    o:w(string.format("    %s_evaluate(&ctx);", tree_name))
    for _, level in ipairs(data.levels) do emit_prints(level) end

    -- Scenario 3
    o:w("")
    o:w('    printf("\\n=== Scenario 3: Operator clears pump 0 fault ===\\n");')
    local ac = find_raw("alarm_clear")
    if ac then
        local c_id = path_to_c_id(ac.path)
        local c_type = C_TYPE_MAP[ac.type]
        o:w(string.format("    ((%s*)ctx.%s.current)[0] = 1;  /* clear pump 0 */", c_type, c_id))
    end
    if pf then
        local c_id = path_to_c_id(pf.path)
        local c_type = C_TYPE_MAP[pf.type]
        o:w(string.format("    ((%s*)ctx.%s.current)[0] = 0;  /* fault cleared */", c_type, c_id))
    end
    o:w(string.format("    %s_swap_raw_buffers(&ctx);", tree_name))
    o:w(string.format("    %s_evaluate(&ctx);", tree_name))
    for _, level in ipairs(data.levels) do emit_prints(level) end

    -- Scenario 4
    o:w("")
    o:w('    printf("\\n=== Scenario 4: Clear alarm_clear bit (normal operation) ===\\n");')
    if ac then
        local c_id = path_to_c_id(ac.path)
        local c_type = C_TYPE_MAP[ac.type]
        o:w(string.format("    ((%s*)ctx.%s.current)[0] = 0;  /* remove clear */", c_type, c_id))
    end
    o:w(string.format("    %s_swap_raw_buffers(&ctx);", tree_name))
    o:w(string.format("    %s_evaluate(&ctx);", tree_name))
    for _, level in ipairs(data.levels) do emit_prints(level) end

    o:w("")
    o:w('    printf("\\nDone.\\n");')
    o:w("    return 0;")
    o:w("}")

    return o
end

-- =====================================================================
-- Makefile generator
-- =====================================================================

local function generate_makefile(tree_name)
    local o = Writer.new()
    o:w(string.format("# Makefile for %s (auto-generated)", tree_name))
    o:w("CC = gcc")
    o:w("CFLAGS = -Wall -Wextra -std=c99 -O2")
    o:w("")
    o:w(string.format("all: %s_test", tree_name))
    o:w("")
    o:w(string.format("%s_test: %s_test.c %s.c %s.h", tree_name, tree_name, tree_name, tree_name))
    o:w(string.format("\t$(CC) $(CFLAGS) -o $@ %s_test.c %s.c", tree_name, tree_name))
    o:w("")
    o:w("clean:")
    o:w(string.format("\trm -f %s_test", tree_name))
    o:w("")
    o:w(".PHONY: all clean")
    return o
end

-- =====================================================================
-- Main
-- =====================================================================

local function main()
    if #arg < 1 then
        print(string.format("Usage: luajit %s <json_file> [output_dir]", arg[0]))
        os.exit(1)
    end

    local json_file = arg[1]
    local output_dir = arg[2] or json_file:match("(.*/)")  or "."

    -- Read JSON
    local f = io.open(json_file, "r")
    if not f then
        print(string.format("Error: cannot open %s", json_file))
        os.exit(1)
    end
    local content = f:read("*a")
    f:close()

    local data = json.decode(content)
    local tree_name = data.name or "scan_tree"

    -- Create output dir
    os.execute(string.format("mkdir -p %s", output_dir))

    -- Generate files
    local header = generate_header(data, tree_name)
    local header_path = string.format("%s/%s.h", output_dir, tree_name)
    header:write_file(header_path)
    print(string.format("Generated: %s", header_path))

    local source = generate_source(data, tree_name)
    local source_path = string.format("%s/%s.c", output_dir, tree_name)
    source:write_file(source_path)
    print(string.format("Generated: %s", source_path))

    local test = generate_test_main(data, tree_name)
    local test_path = string.format("%s/%s_test.c", output_dir, tree_name)
    test:write_file(test_path)
    print(string.format("Generated: %s", test_path))

    local makefile = generate_makefile(tree_name)
    local makefile_path = string.format("%s/Makefile", output_dir)
    makefile:write_file(makefile_path)
    print(string.format("Generated: %s", makefile_path))

    print("")
    print("To build and run:")
    print(string.format("  cd %s", output_dir))
    print("  make")
    print(string.format("  ./%s_test", tree_name))
end

main()

