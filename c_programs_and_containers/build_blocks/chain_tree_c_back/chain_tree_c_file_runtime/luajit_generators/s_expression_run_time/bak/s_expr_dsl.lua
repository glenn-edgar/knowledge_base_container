-- ============================================================================
-- s_expr_dsl.lua
-- ChainTree S-Expression DSL Core Library - Version 4.0
-- 
-- This is the main DSL library that provides:
--   1. DSL functions for defining modules, records, trees, etc.
--   2. C header generation (text output)
--   3. Binary module generation (binary output)
--
-- Usage: This file is loaded by s_compile.lua and sets up global DSL functions
-- ============================================================================

local ffi = require("ffi")
local bit = require("bit")

dofile("s_cfl_functions.lua")
local M = {}

-- ============================================================================
-- FNV-1a 32-bit HASH
-- ============================================================================

local FNV_OFFSET_BASIS = 0x811c9dc5
local FNV_PRIME = 0x01000193

function M.fnv1a_32(str)
    local hash = FNV_OFFSET_BASIS
    for i = 1, #str do
        hash = bit.bxor(hash, str:byte(i))
        hash = bit.band(hash * FNV_PRIME, 0xFFFFFFFF)
    end
    return bit.band(hash, 0xFFFFFFFF)
end

-- Format hash as proper 32-bit hex (avoids 64-bit sign extension in LuaJIT)
function M.fmt_hash(h)
    local u32 = ffi.new("uint32_t", h)
    return string.format("0x%08X", tonumber(u32))
end

-- Make hash function global
_G.fnv1a_32 = M.fnv1a_32

-- ============================================================================
-- TYPE SYSTEM
-- ============================================================================

local type_info = {
    int8    = { size = 1, align = 1, ctype = "int8_t",   tag = 0x01 },
    int16   = { size = 2, align = 2, ctype = "int16_t",  tag = 0x02 },
    int32   = { size = 4, align = 4, ctype = "int32_t",  tag = 0x03 },
    int64   = { size = 8, align = 8, ctype = "int64_t",  tag = 0x04 },
    uint8   = { size = 1, align = 1, ctype = "uint8_t",  tag = 0x05 },
    uint16  = { size = 2, align = 2, ctype = "uint16_t", tag = 0x06 },
    uint32  = { size = 4, align = 4, ctype = "uint32_t", tag = 0x07 },
    uint64  = { size = 8, align = 8, ctype = "uint64_t", tag = 0x08 },
    float   = { size = 4, align = 4, ctype = "float",    tag = 0x09 },
    double  = { size = 8, align = 8, ctype = "double",   tag = 0x0A },
    bool    = { size = 1, align = 1, ctype = "bool",     tag = 0x0B },
    char    = { size = 1, align = 1, ctype = "char",     tag = 0x0C },
}

M.type_info = type_info

-- ============================================================================
-- MODULE STATE (global during DSL execution)
-- ============================================================================

local current_module = nil
local current_record = nil
local current_tree = nil
local current_const = nil
local current_call_stack = {}
local in_bit_block = false
local debug_mode = false

-- ============================================================================
-- DSL ERROR HANDLING
-- ============================================================================

local function dsl_error(msg)
    error("[DSL Error] " .. msg, 3)
end

_G.dsl_error = dsl_error

-- ============================================================================
-- MODULE FUNCTIONS
-- ============================================================================

function _G.start_module(name)
    if current_module then
        dsl_error("Module already started: " .. current_module.name)
    end
    
    current_module = {
        name = name,
        name_hash = M.fnv1a_32(name),
        
        -- Records
        records = {},           -- name -> record definition
        record_order = {},      -- ordered list of record names
        
        -- Trees
        trees = {},             -- name -> tree definition
        tree_order = {},        -- ordered list of tree names
        
        -- Constants
        constants = {},         -- name -> constant definition
        const_order = {},       -- ordered list of constant names
        
        -- Function registry
        oneshot_funcs = {},     -- list of oneshot function names
        main_funcs = {},        -- list of main function names
        pred_funcs = {},        -- list of predicate function names
        
        -- String table
        string_table = {},      -- list of strings
        string_index = {},      -- string -> index
        
        -- Settings
        pointer_size = _G._pointer_size or 4,
        debug = false,
    }
    
    return current_module
end

function _G.end_module(mod)
    if not current_module then
        dsl_error("No module started")
    end
    
    local result = current_module
    current_module = nil
    current_record = nil
    current_tree = nil
    current_const = nil
    current_call_stack = {}
    
    return result
end

function _G.use_32bit()
    if current_module then
        current_module.pointer_size = 4
    end
    _G._pointer_size = 4
end

function _G.use_64bit()
    if current_module then
        current_module.pointer_size = 8
    end
    _G._pointer_size = 8
end

function _G.set_debug(val)
    debug_mode = val
    if current_module then
        current_module.debug = val
    end
end

function _G.is_debug()
    return debug_mode
end

-- ============================================================================
-- RECORD FUNCTIONS
-- ============================================================================

function _G.RECORD(name)
    if not current_module then
        dsl_error("No module started")
    end
    if current_record then
        dsl_error("Record already open: " .. current_record.name)
    end
    if current_module.records[name] then
        dsl_error("Record already defined: " .. name)
    end
    
    current_record = {
        name = name,
        name_hash = M.fnv1a_32(name),
        fields = {},
        size = 0,
        align = 1,
    }
end

function _G.FIELD(name, type_name)
    if not current_record then
        dsl_error("No record open")
    end
    
    local info = type_info[type_name]
    local field = {
        name = name,
        name_hash = M.fnv1a_32(name),
        type = type_name,
        is_pointer = false,
        is_char_array = false,
        is_embedded = false,
    }
    
    if info then
        field.size = info.size
        field.align = info.align
        field.type_tag = info.tag
    else
        -- Embedded record type
        local embedded = current_module.records[type_name]
        if embedded then
            field.size = embedded.size
            field.align = embedded.align or 4
            field.is_embedded = true
            field.embedded_record = type_name
            field.type_tag = 0x0F  -- EMBEDDED
        else
            dsl_error("Unknown type: " .. type_name)
        end
    end
    
    -- Calculate offset with alignment
    local offset = current_record.size
    local padding = (field.align - (offset % field.align)) % field.align
    offset = offset + padding
    field.offset = offset
    
    current_record.size = offset + field.size
    if field.align > current_record.align then
        current_record.align = field.align
    end
    
    table.insert(current_record.fields, field)
end

function _G.PTR_FIELD(name, target_type)
    if not current_record then
        dsl_error("No record open")
    end
    
    local ptr_size = current_module.pointer_size
    local field = {
        name = name,
        name_hash = M.fnv1a_32(name),
        type = "ptr",
        target_type = target_type,
        size = ptr_size,
        align = ptr_size,
        is_pointer = true,
        is_char_array = false,
        is_embedded = false,
        type_tag = 0x0E,  -- PTR
    }
    
    local offset = current_record.size
    local padding = (field.align - (offset % field.align)) % field.align
    offset = offset + padding
    field.offset = offset
    
    current_record.size = offset + field.size
    if field.align > current_record.align then
        current_record.align = field.align
    end
    
    table.insert(current_record.fields, field)
end

function _G.CHAR_ARRAY(name, length)
    if not current_record then
        dsl_error("No record open")
    end
    
    local field = {
        name = name,
        name_hash = M.fnv1a_32(name),
        type = "char_array",
        array_len = length,
        size = length,
        align = 1,
        is_pointer = false,
        is_char_array = true,
        is_embedded = false,
        type_tag = 0x0D,  -- CHAR_ARRAY
    }
    
    local offset = current_record.size
    field.offset = offset
    
    current_record.size = offset + field.size
    
    table.insert(current_record.fields, field)
end

function _G.END_RECORD()
    if not current_record then
        dsl_error("No record open")
    end
    
    -- Pad to alignment
    local padding = (current_record.align - (current_record.size % current_record.align)) % current_record.align
    current_record.size = current_record.size + padding
    
    current_module.records[current_record.name] = current_record
    table.insert(current_module.record_order, current_record.name)
    
    current_record = nil
end

-- ============================================================================
-- CONSTANT FUNCTIONS
-- ============================================================================

function _G.CONST(name, record_type)
    if not current_module then
        dsl_error("No module started")
    end
    if current_const then
        dsl_error("Constant already open: " .. current_const.name)
    end
    
    local rec = current_module.records[record_type]
    if not rec then
        dsl_error("Unknown record type for constant: " .. record_type)
    end
    
    current_const = {
        name = name,
        name_hash = M.fnv1a_32(name),
        record_type = record_type,
        values = {},
        data_bytes = nil,
    }
end

function _G.VALUE(field_path, value)
    if not current_const then
        dsl_error("No constant open")
    end
    
    table.insert(current_const.values, {
        path = field_path,
        value = value,
    })
end

function _G.END_CONST()
    if not current_const then
        dsl_error("No constant open")
    end
    
    -- Generate binary data for constant
    local rec = current_module.records[current_const.record_type]
    local data = {}
    for i = 1, rec.size do
        data[i] = 0
    end
    
    -- Fill in values
    for _, v in ipairs(current_const.values) do
        local offset, size, field = resolve_field_path(rec, v.path)
        if offset and field then
            write_value_to_buffer(data, offset, v.value, field)
        end
    end
    
    current_const.data_bytes = data
    
    current_module.constants[current_const.name] = current_const
    table.insert(current_module.const_order, current_const.name)
    
    current_const = nil
end

-- Helper to resolve nested field path like "motor.position.x"
function resolve_field_path(rec, path)
    local parts = {}
    for part in path:gmatch("[^.]+") do
        table.insert(parts, part)
    end
    
    local offset = 0
    local current_rec = rec
    local field = nil
    
    for i, part in ipairs(parts) do
        field = nil
        for _, f in ipairs(current_rec.fields) do
            if f.name == part then
                field = f
                break
            end
        end
        
        if not field then
            return nil, nil, nil
        end
        
        offset = offset + field.offset
        
        if i < #parts and field.is_embedded then
            current_rec = current_module.records[field.embedded_record]
            if not current_rec then
                return nil, nil, nil
            end
        end
    end
    
    return offset, field.size, field
end

-- Helper to write value to buffer
function write_value_to_buffer(buf, offset, value, field)
    local ftype = field.type
    
    if ftype == "float" then
        local fbuf = ffi.new("float[1]", value)
        local bytes = ffi.cast("uint8_t*", fbuf)
        for i = 0, 3 do
            buf[offset + i + 1] = bytes[i]
        end
    elseif ftype == "double" then
        local dbuf = ffi.new("double[1]", value)
        local bytes = ffi.cast("uint8_t*", dbuf)
        for i = 0, 7 do
            buf[offset + i + 1] = bytes[i]
        end
    elseif ftype == "bool" then
        buf[offset + 1] = value and 1 or 0
    elseif ftype == "int8" or ftype == "uint8" or ftype == "char" then
        buf[offset + 1] = bit.band(value, 0xFF)
    elseif ftype == "int16" or ftype == "uint16" then
        buf[offset + 1] = bit.band(value, 0xFF)
        buf[offset + 2] = bit.band(bit.rshift(value, 8), 0xFF)
    elseif ftype == "int32" or ftype == "uint32" then
        if value < 0 then value = 0x100000000 + value end
        buf[offset + 1] = bit.band(value, 0xFF)
        buf[offset + 2] = bit.band(bit.rshift(value, 8), 0xFF)
        buf[offset + 3] = bit.band(bit.rshift(value, 16), 0xFF)
        buf[offset + 4] = bit.band(bit.rshift(value, 24), 0xFF)
    elseif ftype == "int64" or ftype == "uint64" then
        local lo = bit.band(value, 0xFFFFFFFF)
        local hi = math.floor(value / 0x100000000)
        for i = 0, 3 do
            buf[offset + i + 1] = bit.band(bit.rshift(lo, i * 8), 0xFF)
        end
        for i = 0, 3 do
            buf[offset + i + 5] = bit.band(bit.rshift(hi, i * 8), 0xFF)
        end
    end
end

-- ============================================================================
-- TREE FUNCTIONS
-- ============================================================================

function _G.start_tree(name)
    if not current_module then
        dsl_error("No module started")
    end
    if current_tree then
        dsl_error("Tree already open: " .. current_tree.name)
    end
    
    current_tree = {
        name = name,
        name_hash = M.fnv1a_32(name),
        record_name = nil,
        record_index = 0,
        nodes = {},
        node_count = 0,
    }
    
    current_call_stack = {}
end

function _G.use_record(name)
    if not current_tree then
        dsl_error("No tree open")
    end
    
    current_tree.record_name = name
    
    -- Find record index
    for i, rname in ipairs(current_module.record_order) do
        if rname == name then
            current_tree.record_index = i - 1
            break
        end
    end
end

function _G.end_tree(name)
    if not current_tree then
        dsl_error("No tree open")
    end
    if name and current_tree.name ~= name then
        dsl_error("Tree name mismatch: expected " .. current_tree.name .. ", got " .. name)
    end
    
    current_module.trees[current_tree.name] = current_tree
    table.insert(current_module.tree_order, current_tree.name)
    
    current_tree = nil
    current_call_stack = {}
end

-- ============================================================================
-- CALL FUNCTIONS
-- ============================================================================

local function start_call(func_name, call_type)
    if not current_tree then
        dsl_error("No tree open")
    end
    
    local node = {
        func_name = func_name,
        func_hash = M.fnv1a_32(func_name),
        call_type = call_type,
        params = {},
        children = {},
        param_count = 0,
    }
    
    -- Register function
    local func_list = nil
    if call_type == "o_call" or call_type == "io_call" then
        func_list = current_module.oneshot_funcs
    elseif call_type == "m_call" or call_type == "pt_m_call" then
        func_list = current_module.main_funcs
    elseif call_type == "p_call" or call_type == "p_call_bit" then
        func_list = current_module.pred_funcs
    end
    
    if func_list then
        local found = false
        for _, n in ipairs(func_list) do
            if n == func_name then
                found = true
                break
            end
        end
        if not found then
            table.insert(func_list, func_name)
        end
    end
    
    -- Add to parent or tree root
    if #current_call_stack > 0 then
        local parent = current_call_stack[#current_call_stack]
        table.insert(parent.children, node)
    else
        table.insert(current_tree.nodes, node)
    end
    
    table.insert(current_call_stack, node)
    current_tree.node_count = current_tree.node_count + 1
    
    return node
end

function _G.o_call(func_name)
    return start_call(func_name, "o_call")
end

function _G.m_call(func_name)
    return start_call(func_name, "m_call")
end

function _G.p_call(func_name)
    return start_call(func_name, "p_call")
end

function _G.pt_m_call(func_name)
    return start_call(func_name, "pt_m_call")
end

function _G.io_call(func_name)
    return start_call(func_name, "io_call")
end

function _G.p_call_bit(func_name)
    in_bit_block = true
    return start_call(func_name, "p_call_bit")
end

function _G.end_call(node)
    if #current_call_stack == 0 then
        dsl_error("No call to end")
    end
    
    local top = current_call_stack[#current_call_stack]
    if top.call_type == "p_call_bit" then
        in_bit_block = false
    end
    
    table.remove(current_call_stack)
    return top
end

function _G.check_bit_block_only(func_name)
    if not in_bit_block then
        dsl_error(func_name .. "() can only be used inside a bit block (p_call_bit)")
    end
end

-- ============================================================================
-- PARAMETER FUNCTIONS
-- ============================================================================

local function add_param(ptype, value)
    if #current_call_stack == 0 then
        dsl_error("No call open for parameter")
    end
    
    local node = current_call_stack[#current_call_stack]
    table.insert(node.params, { type = ptype, value = value })
    node.param_count = node.param_count + 1
end

function _G.int(value)
    add_param("int", value)
end

function _G.uint(value)
    add_param("uint", value)
end

function _G.flt(value)
    add_param("float", value)
end

function _G.str(value)
    -- Add to string table
    if not current_module.string_index[value] then
        current_module.string_index[value] = #current_module.string_table
        table.insert(current_module.string_table, value)
    end
    add_param("str_idx", value)
end

function _G.str_ptr(value)
    -- Add to string table
    if not current_module.string_index[value] then
        current_module.string_index[value] = #current_module.string_table
        table.insert(current_module.string_table, value)
    end
    add_param("str_ptr", value)
end

function _G.field_ref(name)
    add_param("field_ref", name)
end

function _G.nested_field_ref(path)
    add_param("nested_field_ref", path)
end

function _G.const_ref(name)
    add_param("const_ref", name)
end

function _G.result(code)
    -- If we're inside a call, add as parameter
    if #current_call_stack > 0 then
        add_param("result", code)
    else
        -- Tree-level result
        if current_tree then
            current_tree.default_result = code
        end
    end
end

function _G.list_start(name)
    add_param("list_start", name or "")
    return { name = name }
end

function _G.list_end(marker)
    add_param("list_end", marker and marker.name or "")
end

-- ============================================================================
-- RESULT CODES
-- ============================================================================

_G.SE_CONTINUE = 0
_G.SE_HALT = 1
_G.SE_FUNCTION_TERMINATE = 2
_G.SE_RESET = 3
_G.SE_ERROR = 4

-- ============================================================================
-- MODULE GENERATOR CLASS
-- ============================================================================

local ModuleGenerator = {}
ModuleGenerator.__index = ModuleGenerator

function ModuleGenerator.new(module_data)
    local self = setmetatable({}, ModuleGenerator)
    self.module = module_data
    self.is_64bit = (module_data.pointer_size == 8)
    return self
end

-- ============================================================================
-- C HEADER GENERATION
-- ============================================================================

function ModuleGenerator:to_c_records_header(base_name)
    local lines = {}
    local mod = self.module
    local guard = base_name:upper() .. "_RECORDS_H"
    
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "// " .. base_name .. "_records.h")
    table.insert(lines, "// Generated record structures for " .. mod.name)
    table.insert(lines, "// DO NOT EDIT")
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "")
    table.insert(lines, "#ifndef " .. guard)
    table.insert(lines, "#define " .. guard)
    table.insert(lines, "")
    table.insert(lines, "#include <stdint.h>")
    table.insert(lines, "#include <stdbool.h>")
    table.insert(lines, "")
    
    -- Forward declarations for pointer types
    table.insert(lines, "// Forward declarations")
    for _, name in ipairs(mod.record_order) do
        table.insert(lines, "typedef struct " .. name .. "_s " .. name .. "_t;")
    end
    table.insert(lines, "")
    
    -- Record definitions
    for _, name in ipairs(mod.record_order) do
        local rec = mod.records[name]
        
        table.insert(lines, "// Record: " .. name .. " (size=" .. rec.size .. ", align=" .. rec.align .. ")")
        table.insert(lines, "struct " .. name .. "_s {")
        
        for _, field in ipairs(rec.fields) do
            local ctype
            if field.is_pointer then
                ctype = field.target_type .. "_t*"
            elseif field.is_char_array then
                ctype = "char"
            elseif field.is_embedded then
                ctype = field.embedded_record .. "_t"
            else
                local info = type_info[field.type]
                ctype = info and info.ctype or "uint32_t"
            end
            
            local decl
            if field.is_char_array then
                decl = string.format("    %s %s[%d];", ctype, field.name, field.array_len)
            else
                decl = string.format("    %s %s;", ctype, field.name)
            end
            
            table.insert(lines, decl .. string.format("  // offset=%d, size=%d", field.offset, field.size))
        end
        
        table.insert(lines, "};")
        table.insert(lines, "")
    end
    
    table.insert(lines, "#endif // " .. guard)
    
    return table.concat(lines, "\n")
end

function ModuleGenerator:to_c_header(base_name)
    local lines = {}
    local mod = self.module
    local guard = base_name:upper() .. "_H"
    
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "// " .. base_name .. ".h")
    table.insert(lines, "// Generated S-expression module for " .. mod.name)
    table.insert(lines, "// DO NOT EDIT")
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "")
    table.insert(lines, "#ifndef " .. guard)
    table.insert(lines, "#define " .. guard)
    table.insert(lines, "")
    table.insert(lines, "#ifdef __cplusplus")
    table.insert(lines, 'extern "C" {')
    table.insert(lines, "#endif")
    table.insert(lines, "")
    table.insert(lines, '#include "s_engine_types.h"')
    table.insert(lines, '#include "' .. base_name .. '_records.h"')
    table.insert(lines, "")
    
    -- Module info
    table.insert(lines, "// Module: " .. mod.name)
    table.insert(lines, string.format("#define %s_NAME_HASH %s", base_name:upper(), M.fmt_hash(mod.name_hash)))
    table.insert(lines, string.format("#define %s_TREE_COUNT %d", base_name:upper(), #mod.tree_order))
    table.insert(lines, string.format("#define %s_RECORD_COUNT %d", base_name:upper(), #mod.record_order))
    table.insert(lines, "")
    
    -- String table (for reference - actual strings are in binary)
    if #mod.string_table > 0 then
        table.insert(lines, "// String table")
        table.insert(lines, "static const char* const " .. base_name .. "_strings[] = {")
        for i, str in ipairs(mod.string_table) do
            local escaped = str:gsub("\\", "\\\\"):gsub('"', '\\"'):gsub("\n", "\\n"):gsub("\r", "\\r")
            local comma = (i < #mod.string_table) and "," or ""
            table.insert(lines, '    "' .. escaped .. '"' .. comma)
        end
        table.insert(lines, "};")
        table.insert(lines, string.format("#define %s_STRING_COUNT %d", base_name:upper(), #mod.string_table))
        table.insert(lines, "")
    end
    
    -- Function hashes
    local all_funcs = {}
    for _, name in ipairs(mod.oneshot_funcs) do table.insert(all_funcs, name) end
    for _, name in ipairs(mod.main_funcs) do table.insert(all_funcs, name) end
    for _, name in ipairs(mod.pred_funcs) do table.insert(all_funcs, name) end
    
    if #all_funcs > 0 then
        table.insert(lines, "// Function hashes")
        local seen = {}
        for _, name in ipairs(all_funcs) do
            if not seen[name] then
                seen[name] = true
                local hash = M.fnv1a_32(name)
                local def_name = name:upper():gsub("[^%w]", "_") .. "_HASH"
                table.insert(lines, string.format("#define %s %s", def_name, M.fmt_hash(hash)))
            end
        end
        table.insert(lines, "")
    end
    
    -- Tree hashes
    if #mod.tree_order > 0 then
        table.insert(lines, "// Tree hashes")
        for _, name in ipairs(mod.tree_order) do
            local hash = M.fnv1a_32(name)
            local def_name = name:upper():gsub("[^%w]", "_") .. "_HASH"
            table.insert(lines, string.format("#define %s %s", def_name, M.fmt_hash(hash)))
        end
        table.insert(lines, "")
    end
    
    -- Record hashes
    if #mod.record_order > 0 then
        table.insert(lines, "// Record hashes")
        for _, name in ipairs(mod.record_order) do
            local hash = M.fnv1a_32(name)
            local def_name = name:upper():gsub("[^%w]", "_") .. "_HASH"
            table.insert(lines, string.format("#define %s %s", def_name, M.fmt_hash(hash)))
        end
        table.insert(lines, "")
    end
    
    -- Field hashes (deduplicated)
    local field_hashes = {}
    local field_order = {}
    for _, rname in ipairs(mod.record_order) do
        local rec = mod.records[rname]
        for _, field in ipairs(rec.fields) do
            if not field_hashes[field.name] then
                field_hashes[field.name] = M.fnv1a_32(field.name)
                table.insert(field_order, field.name)
            end
        end
    end
    
    if #field_order > 0 then
        table.insert(lines, "// Field hashes")
        for _, name in ipairs(field_order) do
            local hash = field_hashes[name]
            local def_name = "FIELD_" .. name:upper():gsub("[^%w]", "_") .. "_HASH"
            table.insert(lines, string.format("#define %s %s", def_name, M.fmt_hash(hash)))
        end
        table.insert(lines, "")
    end
    
    -- Constant hashes
    if #mod.const_order > 0 then
        table.insert(lines, "// Constants")
        for _, name in ipairs(mod.const_order) do
            local cnst = mod.constants[name]
            local rec = mod.records[cnst.record_type]
            local type_name = cnst.record_type:lower():gsub("[^%w]", "_") .. "_t"
            table.insert(lines, "// Constant: " .. name .. " (type=" .. cnst.record_type .. ")")
            table.insert(lines, "static const " .. type_name .. " " .. name .. " = {")
            table.insert(lines, "    {0}  // Use binary data for actual initialization")
            table.insert(lines, "};")
            local def_name = name:upper():gsub("[^%w]", "_") .. "_HASH"
            table.insert(lines, string.format("#define %s %s", def_name, M.fmt_hash(cnst.name_hash)))
            table.insert(lines, "")
        end
    end
    
    table.insert(lines, "#ifdef __cplusplus")
    table.insert(lines, "}")
    table.insert(lines, "#endif")
    table.insert(lines, "")
    table.insert(lines, "#endif // " .. guard)
    
    return table.concat(lines, "\n")
end

-- Debug header with hash defines and function name comments
function ModuleGenerator:to_c_debug_header(base_name)
    local lines = {}
    local mod = self.module
    local guard = base_name:upper() .. "_DEBUG_H"
    
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "// " .. base_name .. "_debug.h")
    table.insert(lines, "// Debug hash reference for " .. mod.name)
    table.insert(lines, "// For development/debugging only - not needed for runtime")
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "")
    table.insert(lines, "#ifndef " .. guard)
    table.insert(lines, "#define " .. guard)
    table.insert(lines, "")
    table.insert(lines, "#include <stdint.h>")
    table.insert(lines, "")
    
    -- Function hashes with names as comments
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "// Function Hashes")
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "")
    
    -- Helper to get hash as 8-char hex string (no 0x prefix)
    local function hash_hex(h)
        local u32 = ffi.new("uint32_t", h)
        return string.format("%08X", tonumber(u32))
    end
    
    if #mod.oneshot_funcs > 0 then
        table.insert(lines, "// Oneshot functions")
        for _, name in ipairs(mod.oneshot_funcs) do
            local h = M.fnv1a_32(name)
            table.insert(lines, string.format("#define H_%s %s  // %s", 
                hash_hex(h), M.fmt_hash(h), name))
        end
        table.insert(lines, "")
    end
    
    if #mod.main_funcs > 0 then
        table.insert(lines, "// Main functions")
        for _, name in ipairs(mod.main_funcs) do
            local h = M.fnv1a_32(name)
            table.insert(lines, string.format("#define H_%s %s  // %s", 
                hash_hex(h), M.fmt_hash(h), name))
        end
        table.insert(lines, "")
    end
    
    if #mod.pred_funcs > 0 then
        table.insert(lines, "// Predicate functions")
        for _, name in ipairs(mod.pred_funcs) do
            local h = M.fnv1a_32(name)
            table.insert(lines, string.format("#define H_%s %s  // %s", 
                hash_hex(h), M.fmt_hash(h), name))
        end
        table.insert(lines, "")
    end
    
    -- Tree hashes
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "// Tree Hashes")
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "")
    for _, name in ipairs(mod.tree_order) do
        local h = M.fnv1a_32(name)
        table.insert(lines, string.format("#define H_%s %s  // %s", 
            hash_hex(h), M.fmt_hash(h), name))
    end
    table.insert(lines, "")
    
    -- Record hashes
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "// Record Hashes")
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "")
    for _, name in ipairs(mod.record_order) do
        local h = M.fnv1a_32(name)
        table.insert(lines, string.format("#define H_%s %s  // %s", 
            hash_hex(h), M.fmt_hash(h), name))
    end
    table.insert(lines, "")
    
    -- Field hashes
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "// Field Hashes")
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "")
    local field_hashes = {}
    for _, rname in ipairs(mod.record_order) do
        local rec = mod.records[rname]
        for _, field in ipairs(rec.fields) do
            if not field_hashes[field.name] then
                local h = M.fnv1a_32(field.name)
                field_hashes[field.name] = h
                table.insert(lines, string.format("#define H_%s %s  // %s", 
                    hash_hex(h), M.fmt_hash(h), field.name))
            end
        end
    end
    table.insert(lines, "")
    
    -- Constant hashes
    if #mod.const_order > 0 then
        table.insert(lines, "// ============================================================================")
        table.insert(lines, "// Constant Hashes")
        table.insert(lines, "// ============================================================================")
        table.insert(lines, "")
        for _, name in ipairs(mod.const_order) do
            local h = M.fnv1a_32(name)
            table.insert(lines, string.format("#define H_%s %s  // %s", 
                hash_hex(h), M.fmt_hash(h), name))
        end
        table.insert(lines, "")
    end
    
    -- String table (for reference)
    if #mod.string_table > 0 then
        table.insert(lines, "// ============================================================================")
        table.insert(lines, "// String Table (for reference)")
        table.insert(lines, "// ============================================================================")
        table.insert(lines, "//")
        for i, s in ipairs(mod.string_table) do
            local display = s:gsub("\n", "\\n")
            if #display > 60 then
                display = display:sub(1, 57) .. "..."
            end
            table.insert(lines, string.format("// [%d] \"%s\"", i - 1, display))
        end
        table.insert(lines, "")
    end
    
    table.insert(lines, "#endif // " .. guard)
    
    return table.concat(lines, "\n")
end

function ModuleGenerator:to_c_user_header(base_name)
    local lines = {}
    local mod = self.module
    local guard = base_name:upper() .. "_USER_FUNCTIONS_H"
    
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "// " .. base_name .. "_user_functions.h")
    table.insert(lines, "// User function prototypes for " .. mod.name)
    table.insert(lines, "// DO NOT EDIT")
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "")
    table.insert(lines, "#ifndef " .. guard)
    table.insert(lines, "#define " .. guard)
    table.insert(lines, "")
    table.insert(lines, "#ifdef __cplusplus")
    table.insert(lines, 'extern "C" {')
    table.insert(lines, "#endif")
    table.insert(lines, "")
    table.insert(lines, '#include "s_engine_types.h"')
    table.insert(lines, "")
    
    -- Oneshot functions
    if #mod.oneshot_funcs > 0 then
        table.insert(lines, "// Oneshot functions")
        for _, name in ipairs(mod.oneshot_funcs) do
            table.insert(lines, string.format("void %s(s_engine_ctx_t* ctx);", name))
        end
        table.insert(lines, "")
    end
    
    -- Main functions
    if #mod.main_funcs > 0 then
        table.insert(lines, "// Main functions")
        for _, name in ipairs(mod.main_funcs) do
            table.insert(lines, string.format("s_result_t %s(s_engine_ctx_t* ctx);", name))
        end
        table.insert(lines, "")
    end
    
    -- Predicate functions
    if #mod.pred_funcs > 0 then
        table.insert(lines, "// Predicate functions")
        for _, name in ipairs(mod.pred_funcs) do
            table.insert(lines, string.format("bool %s(s_engine_ctx_t* ctx);", name))
        end
        table.insert(lines, "")
    end
    
    table.insert(lines, "#ifdef __cplusplus")
    table.insert(lines, "}")
    table.insert(lines, "#endif")
    table.insert(lines, "")
    table.insert(lines, "#endif // " .. guard)
    
    return table.concat(lines, "\n")
end

function ModuleGenerator:to_c_user_registration(base_name)
    local lines = {}
    local mod = self.module
    
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "// " .. base_name .. "_user_registration.c")
    table.insert(lines, "// User function registration for " .. mod.name)
    table.insert(lines, "// DO NOT EDIT")
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "")
    table.insert(lines, '#include "' .. base_name .. '.h"')
    table.insert(lines, '#include "' .. base_name .. '_user_functions.h"')
    table.insert(lines, "")
    
    -- Registration function
    table.insert(lines, "void " .. base_name .. "_register_functions(s_engine_t* engine) {")
    
    for _, name in ipairs(mod.oneshot_funcs) do
        table.insert(lines, string.format("    s_engine_register_oneshot(engine, %s, %s);", 
            M.fmt_hash(M.fnv1a_32(name)), name))
    end
    
    for _, name in ipairs(mod.main_funcs) do
        table.insert(lines, string.format("    s_engine_register_main(engine, %s, %s);", 
            M.fmt_hash(M.fnv1a_32(name)), name))
    end
    
    for _, name in ipairs(mod.pred_funcs) do
        table.insert(lines, string.format("    s_engine_register_pred(engine, %s, %s);", 
            M.fmt_hash(M.fnv1a_32(name)), name))
    end
    
    table.insert(lines, "}")
    table.insert(lines, "")
    
    return table.concat(lines, "\n")
end

function ModuleGenerator:dump()
    local lines = {}
    local mod = self.module
    
    table.insert(lines, "=== MODULE DUMP: " .. mod.name .. " ===")
    table.insert(lines, string.format("Name hash: %s", M.fmt_hash(mod.name_hash)))
    table.insert(lines, string.format("Pointer size: %d", mod.pointer_size))
    table.insert(lines, "")
    
    table.insert(lines, "RECORDS (" .. #mod.record_order .. "):")
    for _, name in ipairs(mod.record_order) do
        local rec = mod.records[name]
        table.insert(lines, string.format("  %s (size=%d, align=%d, hash=%s)", 
            name, rec.size, rec.align, M.fmt_hash(rec.name_hash)))
        for _, field in ipairs(rec.fields) do
            table.insert(lines, string.format("    %s: %s @%d (size=%d)", 
                field.name, field.type, field.offset, field.size))
        end
    end
    table.insert(lines, "")
    
    table.insert(lines, "TREES (" .. #mod.tree_order .. "):")
    for _, name in ipairs(mod.tree_order) do
        local tree = mod.trees[name]
        table.insert(lines, string.format("  %s (nodes=%d, record=%s, hash=%s)", 
            name, tree.node_count, tree.record_name or "none", M.fmt_hash(tree.name_hash)))
    end
    table.insert(lines, "")
    
    table.insert(lines, "CONSTANTS (" .. #mod.const_order .. "):")
    for _, name in ipairs(mod.const_order) do
        local cnst = mod.constants[name]
        table.insert(lines, string.format("  %s (type=%s, hash=%s)", 
            name, cnst.record_type, M.fmt_hash(cnst.name_hash)))
    end
    table.insert(lines, "")
    
    table.insert(lines, "STRINGS (" .. #mod.string_table .. "):")
    for i, s in ipairs(mod.string_table) do
        local display = s:sub(1, 40)
        if #s > 40 then display = display .. "..." end
        table.insert(lines, string.format("  [%d] \"%s\"", i - 1, display))
    end
    table.insert(lines, "")
    
    table.insert(lines, "FUNCTIONS:")
    table.insert(lines, "  Oneshot (" .. #mod.oneshot_funcs .. "): " .. table.concat(mod.oneshot_funcs, ", "))
    table.insert(lines, "  Main (" .. #mod.main_funcs .. "): " .. table.concat(mod.main_funcs, ", "))
    table.insert(lines, "  Pred (" .. #mod.pred_funcs .. "): " .. table.concat(mod.pred_funcs, ", "))
    
    return table.concat(lines, "\n")
end

M.ModuleGenerator = ModuleGenerator

-- ============================================================================
-- BINARY GENERATOR
-- ============================================================================

-- Binary format constants
local SEXB_MAGIC = 0x42584553
local SEXB_VERSION = 0x0100

local SEXB_FLAG_32BIT = 0x0000
local SEXB_FLAG_64BIT = 0x0001
local SEXB_FLAG_DEBUG = 0x0002

-- Opcodes
local OP = {
    INT         = 0x01,
    UINT        = 0x02,
    FLOAT       = 0x03,
    STR_IDX     = 0x04,
    FIELD_REF   = 0x05,
    NESTED_REF  = 0x06,
    CONST_REF   = 0x07,
    RESULT      = 0x08,
    LIST_START  = 0x09,
    LIST_END    = 0x0A,
    CALL_START  = 0x0B,
    CALL_END    = 0x0C,
    INT64       = 0x0D,
    UINT64      = 0x0E,
    DOUBLE      = 0x0F,
}

-- Function types
local FUNC_TYPE = {
    o_call      = 0x01,
    m_call      = 0x02,
    p_call      = 0x03,
    pt_m_call   = 0x04,
    io_call     = 0x05,
    p_call_bit  = 0x06,
}

-- Binary emitter
local BinaryEmitter = {}
BinaryEmitter.__index = BinaryEmitter

function BinaryEmitter.new()
    local self = setmetatable({}, BinaryEmitter)
    self.buffer = {}
    self.pos = 0
    return self
end

function BinaryEmitter:emit_u8(v)
    table.insert(self.buffer, bit.band(v, 0xFF))
    self.pos = self.pos + 1
end

function BinaryEmitter:emit_u16(v)
    table.insert(self.buffer, bit.band(v, 0xFF))
    table.insert(self.buffer, bit.band(bit.rshift(v, 8), 0xFF))
    self.pos = self.pos + 2
end

function BinaryEmitter:emit_u32(v)
    if v < 0 then v = 0x100000000 + v end
    table.insert(self.buffer, bit.band(v, 0xFF))
    table.insert(self.buffer, bit.band(bit.rshift(v, 8), 0xFF))
    table.insert(self.buffer, bit.band(bit.rshift(v, 16), 0xFF))
    table.insert(self.buffer, bit.band(bit.rshift(v, 24), 0xFF))
    self.pos = self.pos + 4
end

function BinaryEmitter:emit_i32(v)
    if v < 0 then v = 0x100000000 + v end
    self:emit_u32(v)
end

function BinaryEmitter:emit_f32(v)
    local buf = ffi.new("float[1]", v)
    local bytes = ffi.cast("uint8_t*", buf)
    for i = 0, 3 do
        table.insert(self.buffer, bytes[i])
    end
    self.pos = self.pos + 4
end

function BinaryEmitter:emit_u64(v)
    local lo = bit.band(v, 0xFFFFFFFF)
    local hi = math.floor(v / 0x100000000)
    self:emit_u32(lo)
    self:emit_u32(hi)
end

function BinaryEmitter:emit_f64(v)
    local buf = ffi.new("double[1]", v)
    local bytes = ffi.cast("uint8_t*", buf)
    for i = 0, 7 do
        table.insert(self.buffer, bytes[i])
    end
    self.pos = self.pos + 8
end

function BinaryEmitter:emit_string(s)
    local len = #s
    self:emit_u16(len)
    for i = 1, len do
        self:emit_u8(s:byte(i))
    end
    -- Pad to 4-byte boundary
    local total = 2 + len
    local padding = (4 - (total % 4)) % 4
    for i = 1, padding do
        self:emit_u8(0)
    end
end

function BinaryEmitter:emit_bytes(bytes)
    for _, b in ipairs(bytes) do
        table.insert(self.buffer, bit.band(b, 0xFF))
    end
    self.pos = self.pos + #bytes
end

function BinaryEmitter:align(n)
    local padding = (n - (self.pos % n)) % n
    for i = 1, padding do
        self:emit_u8(0)
    end
end

function BinaryEmitter:patch_u16(offset, v)
    self.buffer[offset + 1] = bit.band(v, 0xFF)
    self.buffer[offset + 2] = bit.band(bit.rshift(v, 8), 0xFF)
end

function BinaryEmitter:patch_u32(offset, v)
    if v < 0 then v = 0x100000000 + v end
    self.buffer[offset + 1] = bit.band(v, 0xFF)
    self.buffer[offset + 2] = bit.band(bit.rshift(v, 8), 0xFF)
    self.buffer[offset + 3] = bit.band(bit.rshift(v, 16), 0xFF)
    self.buffer[offset + 4] = bit.band(bit.rshift(v, 24), 0xFF)
end

function BinaryEmitter:get_pos()
    return self.pos
end

function BinaryEmitter:to_bytes()
    return self.buffer
end

-- Binary module generator
local BinaryModuleGenerator = {}
BinaryModuleGenerator.__index = BinaryModuleGenerator

function BinaryModuleGenerator.new(module_data)
    local self = setmetatable({}, BinaryModuleGenerator)
    self.module = module_data
    self.is_64bit = (module_data.pointer_size == 8)
    self:build_lookups()
    return self
end

function BinaryModuleGenerator:build_lookups()
    local mod = self.module
    
    self.record_index = {}
    for i, name in ipairs(mod.record_order) do
        self.record_index[name] = i - 1
    end
    
    self.string_index = {}
    for i, s in ipairs(mod.string_table) do
        self.string_index[s] = i - 1
    end
    
    self.const_index = {}
    for i, name in ipairs(mod.const_order) do
        self.const_index[name] = i - 1
    end
end

function BinaryModuleGenerator:generate()
    local e = BinaryEmitter.new()
    local mod = self.module
    
    -- ========== HEADER (32 bytes) ==========
    e:emit_u32(SEXB_MAGIC)
    e:emit_u16(SEXB_VERSION)
    
    local flags = self.is_64bit and SEXB_FLAG_64BIT or SEXB_FLAG_32BIT
    if mod.debug then flags = bit.bor(flags, SEXB_FLAG_DEBUG) end
    e:emit_u16(flags)
    
    e:emit_u32(mod.name_hash)
    e:emit_u16(#mod.tree_order)
    e:emit_u16(#mod.record_order)
    e:emit_u16(#mod.string_table)
    e:emit_u16(#mod.const_order)
    e:emit_u16(#mod.oneshot_funcs)
    e:emit_u16(#mod.main_funcs)
    e:emit_u16(#mod.pred_funcs)
    e:emit_u16(0)  -- reserved
    
    local size_patch = e:get_pos()
    e:emit_u32(0)  -- total_size placeholder
    
    -- ========== DIRECTORY (32 bytes) ==========
    local dir_start = e:get_pos()
    for i = 1, 8 do
        e:emit_u32(0)  -- placeholders
    end
    
    -- ========== TREES ==========
    local tree_offset = e:get_pos()
    e:patch_u32(dir_start, tree_offset)
    
    local tree_bc_patches = {}
    for _, name in ipairs(mod.tree_order) do
        local tree = mod.trees[name]
        
        e:emit_u32(tree.name_hash)
        e:emit_u16(tree.record_index or 0)
        e:emit_u16(tree.node_count)
        
        local bc_patch = e:get_pos()
        e:emit_u32(0)  -- bytecode_offset placeholder
        e:emit_u32(0)  -- bytecode_size placeholder
        
        table.insert(tree_bc_patches, {
            tree = tree,
            offset_patch = bc_patch,
            size_patch = bc_patch + 4
        })
    end
    
    -- ========== RECORDS ==========
    local record_offset = e:get_pos()
    e:patch_u32(dir_start + 4, record_offset)
    
    local field_patches = {}
    for _, name in ipairs(mod.record_order) do
        local rec = mod.records[name]
        
        e:emit_u32(rec.name_hash)
        e:emit_u16(#rec.fields)
        e:emit_u16(rec.size)
        
        local field_patch = e:get_pos()
        e:emit_u32(0)  -- field_table_offset placeholder
        
        table.insert(field_patches, {
            record = rec,
            offset_patch = field_patch
        })
    end
    
    -- ========== FIELDS ==========
    local field_offset = e:get_pos()
    e:patch_u32(dir_start + 8, field_offset)
    
    for _, patch in ipairs(field_patches) do
        e:patch_u32(patch.offset_patch, e:get_pos())
        
        for _, field in ipairs(patch.record.fields) do
            e:emit_u32(field.name_hash)
            e:emit_u8(field.type_tag or 0x07)
            
            local flags = 0
            if field.is_pointer then flags = bit.bor(flags, 0x01) end
            if field.is_char_array then flags = bit.bor(flags, 0x02) end
            if field.is_embedded then flags = bit.bor(flags, 0x04) end
            e:emit_u8(flags)
            
            e:emit_u16(field.offset)
            e:emit_u16(field.size)
            
            -- aux field
            local aux = 0
            if field.is_char_array then
                aux = field.array_len
            elseif field.is_pointer and field.target_type then
                aux = self.record_index[field.target_type] or 0
            elseif field.is_embedded and field.embedded_record then
                aux = self.record_index[field.embedded_record] or 0
            end
            e:emit_u16(aux)
        end
    end
    
    -- ========== STRING BLOB ==========
    local string_offset = e:get_pos()
    e:patch_u32(dir_start + 12, string_offset)
    
    for _, s in ipairs(mod.string_table) do
        e:emit_string(s)
    end
    e:align(4)
    
    -- ========== CONSTANTS ==========
    local const_offset = e:get_pos()
    e:patch_u32(dir_start + 16, const_offset)
    
    local const_data_patches = {}
    for _, name in ipairs(mod.const_order) do
        local cnst = mod.constants[name]
        local rec = mod.records[cnst.record_type]
        
        e:emit_u32(cnst.name_hash)
        e:emit_u16(self.record_index[cnst.record_type] or 0)
        e:emit_u16(rec and rec.size or 0)
        
        local data_patch = e:get_pos()
        e:emit_u32(0)  -- data_offset placeholder
        
        table.insert(const_data_patches, {
            const = cnst,
            offset_patch = data_patch,
            size = rec and rec.size or 0
        })
    end
    
    -- ========== CONSTANT DATA ==========
    local const_data_offset = e:get_pos()
    e:patch_u32(dir_start + 20, const_data_offset)
    
    for _, patch in ipairs(const_data_patches) do
        e:patch_u32(patch.offset_patch, e:get_pos())
        
        if patch.const.data_bytes then
            e:emit_bytes(patch.const.data_bytes)
        else
            for i = 1, patch.size do
                e:emit_u8(0)
            end
        end
        e:align(4)
    end
    
    -- ========== FUNCTION TABLES ==========
    local func_offset = e:get_pos()
    e:patch_u32(dir_start + 24, func_offset)
    
    for _, name in ipairs(mod.oneshot_funcs) do
        e:emit_u32(M.fnv1a_32(name))
    end
    for _, name in ipairs(mod.main_funcs) do
        e:emit_u32(M.fnv1a_32(name))
    end
    for _, name in ipairs(mod.pred_funcs) do
        e:emit_u32(M.fnv1a_32(name))
    end
    
    -- ========== BYTECODE ==========
    local bytecode_offset = e:get_pos()
    e:patch_u32(dir_start + 28, bytecode_offset)
    
    for _, patch in ipairs(tree_bc_patches) do
        local bc_start = e:get_pos()
        e:patch_u32(patch.offset_patch, bc_start)
        
        -- Emit tree bytecode
        self:emit_tree_bytecode(e, patch.tree)
        
        local bc_size = e:get_pos() - bc_start
        e:patch_u32(patch.size_patch, bc_size)
        e:align(4)
    end
    
    -- ========== FINALIZE ==========
    e:patch_u32(size_patch, e:get_pos())
    
    return e:to_bytes(), e:get_pos()
end

function BinaryModuleGenerator:emit_tree_bytecode(e, tree)
    for _, node in ipairs(tree.nodes) do
        self:emit_node(e, node)
    end
end

function BinaryModuleGenerator:emit_node(e, node)
    -- Node header
    e:emit_u32(node.func_hash)
    e:emit_u8(FUNC_TYPE[node.call_type] or 0x01)
    e:emit_u8(#node.params)
    
    -- Size placeholder
    local size_patch = e:get_pos()
    e:emit_u16(0)
    
    local start_pos = e:get_pos()
    
    -- Emit parameters
    for _, param in ipairs(node.params) do
        self:emit_param(e, param)
    end
    
    -- Emit children (nested calls)
    for _, child in ipairs(node.children) do
        e:emit_u8(OP.CALL_START)
        self:emit_node(e, child)
        e:emit_u8(OP.CALL_END)
    end
    
    -- Patch size
    local size = e:get_pos() - start_pos + 8
    e:patch_u16(size_patch, size)
end

function BinaryModuleGenerator:emit_param(e, param)
    local ptype = param.type
    local value = param.value
    
    if ptype == "int" then
        e:emit_u8(OP.INT)
        e:emit_i32(value)
    elseif ptype == "uint" then
        e:emit_u8(OP.UINT)
        e:emit_u32(value)
    elseif ptype == "float" then
        e:emit_u8(OP.FLOAT)
        e:emit_f32(value)
    elseif ptype == "str_idx" or ptype == "str_ptr" then
        e:emit_u8(OP.STR_IDX)
        e:emit_u32(self.string_index[value] or 0)
    elseif ptype == "field_ref" then
        e:emit_u8(OP.FIELD_REF)
        e:emit_u32(M.fnv1a_32(value))
    elseif ptype == "nested_field_ref" then
        e:emit_u8(OP.NESTED_REF)
        e:emit_u32(M.fnv1a_32(value))
    elseif ptype == "const_ref" then
        e:emit_u8(OP.CONST_REF)
        e:emit_u32(self.const_index[value] or 0)
    elseif ptype == "result" then
        e:emit_u8(OP.RESULT)
        e:emit_u32(value)
    elseif ptype == "list_start" then
        e:emit_u8(OP.LIST_START)
    elseif ptype == "list_end" then
        e:emit_u8(OP.LIST_END)
    else
        e:emit_u8(OP.UINT)
        e:emit_u32(value or 0)
    end
end

function BinaryModuleGenerator:to_binary_file(output_path)
    local bytes, size = self:generate()
    
    local f = io.open(output_path, "wb")
    if not f then
        error("Cannot open output file: " .. output_path)
    end
    
    for _, b in ipairs(bytes) do
        f:write(string.char(b))
    end
    f:close()
    
    return size
end

function BinaryModuleGenerator:to_c_header(base_name)
    local bytes, size = self:generate()
    local lines = {}
    
    local guard = base_name:upper() .. "_BIN_H"
    local var_name = base_name:lower() .. "_module_bin"
    
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "// " .. base_name .. "_bin.h")
    table.insert(lines, "// Generated binary module data for " .. self.module.name)
    table.insert(lines, "// DO NOT EDIT")
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "")
    table.insert(lines, "#ifndef " .. guard)
    table.insert(lines, "#define " .. guard)
    table.insert(lines, "")
    table.insert(lines, "#include <stdint.h>")
    table.insert(lines, "")
    table.insert(lines, string.format("#define %s_SIZE %d", var_name:upper(), size))
    table.insert(lines, string.format("#define %s_HASH %s", var_name:upper(), M.fmt_hash(self.module.name_hash)))
    table.insert(lines, "")
    table.insert(lines, string.format("static const uint8_t %s[%d] = {", var_name, size))
    
    -- Emit bytes in rows of 16
    local row = {}
    for i, b in ipairs(bytes) do
        table.insert(row, string.format("0x%02X", b))
        if #row == 16 or i == size then
            local comma = (i == size) and "" or ","
            table.insert(lines, "    " .. table.concat(row, ", ") .. comma)
            row = {}
        end
    end
    
    table.insert(lines, "};")
    table.insert(lines, "")
    table.insert(lines, "#endif // " .. guard)
    
    return table.concat(lines, "\n")
end

M.BinaryModuleGenerator = BinaryModuleGenerator

return M