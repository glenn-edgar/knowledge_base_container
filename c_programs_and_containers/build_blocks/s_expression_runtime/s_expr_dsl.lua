-- ============================================================================
-- s_expr_dsl.lua
-- S-Expression Engine DSL Core Library - Version 5.1
-- 
-- This is the main DSL library that provides:
--   1. DSL functions for defining modules, records, trees, etc.
--   2. C header generation (text output)
--   3. Binary module generation (direct s_expr_param_t, zero-copy)
--
-- VERSION 5.1 CHANGES:
--   - Renamed p_call_bit to p_call_composite for generic predicate composition
--   - Updated result codes for proper caller/engine separation
--   - Standalone library (no ChainTree dependencies)
--
-- Usage: This file is loaded by s_compile.lua and sets up global DSL functions
-- ============================================================================

local ffi = require("ffi")
local bit = require("bit")
jit.off()
local M = {}

-- ============================================================================
-- FNV-1a 32-bit HASH
-- ============================================================================

function M.fnv1a_32(str)
local hash = 0x811c9dc5

for i = 1, #str do
    hash = bit.bxor(hash, str:byte(i))
    
    -- Split into 16-bit halves to avoid double precision overflow
    local lo = bit.band(hash, 0xFFFF)
    local hi = bit.band(bit.rshift(hash, 16), 0xFFFF)
    
    -- Multiply by FNV_PRIME (0x01000193 = 16777619)
    -- (hi*65536 + lo) * prime mod 2^32
    local prime = 0x01000193
    local lo_prod = lo * prime
    local hi_prod = hi * prime
    
    -- Combine: only lower 16 bits of hi_prod matter (shifted left 16)
    hash = lo_prod + bit.lshift(bit.band(hi_prod, 0xFFFF), 16)
    hash = bit.tobit(hash)  -- Wrap to signed 32-bit
end

-- Convert to unsigned for return
local u32 = ffi.new("uint32_t", hash)
return tonumber(u32)
end

-- JIT warmup - force compilation before real use
for i = 1, 10 do
    M.fnv1a_32("warmup_string_" .. i)
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
-- s_expr_param_t OPCODE DEFINITIONS (must match s_engine_types.h)
-- ============================================================================

local S_EXPR_PARAM = {
    INT         = 0x00,
    UINT        = 0x01,
    FLOAT       = 0x02,
    STR_HASH    = 0x03,
    SLOT        = 0x04,
    OPEN        = 0x05,
    CLOSE       = 0x06,
    OPEN_CALL   = 0x07,
    ONESHOT     = 0x08,
    MAIN        = 0x09,
    PRED        = 0x0A,
    FIELD       = 0x0B,
    RESULT      = 0x0C,
    STR_IDX     = 0x0D,
    CONST_REF   = 0x0E,
}

-- Flags
local S_EXPR_FLAG_SURVIVES_RESET = 0x40
local S_EXPR_FLAG_POINTER        = 0x80

M.S_EXPR_PARAM = S_EXPR_PARAM

-- ============================================================================
-- BUILTIN FUNCTION LIST (excluded from user registration)
-- These are implemented in s_engine_builtins.c
-- ============================================================================

local BUILTIN_FUNCTIONS = {
    -- Predicates
    "SE_PRED_AND",
    "SE_PRED_OR",
    "SE_PRED_NOT",
    "SE_PRED_NOR",
    "SE_PRED_NAND",
    "SE_PRED_XOR",
    "SE_TRUE",
    "SE_FALSE",
    "SE_CHECK_EVENT",
    -- Main functions
    "SE_PIPELINE",
    "SE_TICK_DELAY",
    "SE_TIME_DELAY",
    "SE_WAIT_EVENT",
    "SE_NOP",
    "SE_IF_THEN_ELSE",
    "SE_TRIGGER_ON_CHANGE",
    "SE_STATE_MACHINE",
    "SE_STATE_ACTIONS",
    "SE_FIELD_DISPATCH",
    "SE_EVENT_DISPATCH",
    "SE_DISPATCH",
    -- Result code functions
    "SE_RETURN_CONTINUE",
    "SE_RETURN_HALT",
    "SE_RETURN_TERMINATE",
    "SE_RETURN_RESET",
    "SE_RETURN_DISABLE",
    "SE_RETURN_SKIP_CONTINUE",
    "SE_RETURN_FUNCTION_HALT",
    "SE_RETURN_FUNCTION_RESET",
    "SE_RETURN_FUNCTION_TERMINATE",
    -- Oneshots
    "SE_LOG",
}

-- Build lookup table for O(1) check
local BUILTIN_SET = {}
for _, name in ipairs(BUILTIN_FUNCTIONS) do
    BUILTIN_SET[name] = true
end

M.BUILTIN_FUNCTIONS = BUILTIN_FUNCTIONS
M.BUILTIN_SET = BUILTIN_SET

-- Check if a function name is a builtin
local function is_builtin(name)
    return BUILTIN_SET[name] == true
end

M.is_builtin = is_builtin

-- ============================================================================
-- MODULE STATE (global during DSL execution)
-- ============================================================================

local current_module = nil
local current_record = nil
local current_tree = nil
local current_const = nil
local current_call_stack = {}
local in_composite_block = false
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
        
        -- User-defined events
        events = {},            -- list of {name, id} pairs
        event_names = {},       -- name -> id lookup
        
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
-- USER EVENT DEFINITIONS
-- ============================================================================

-- Define a single user event
-- Usage: EVENT("BUTTON_PRESS", 0x0001)
function _G.EVENT(name, id)
    if not current_module then
        dsl_error("No module started")
    end
    
    if type(name) ~= "string" then
        dsl_error("Event name must be a string")
    end
    
    if type(id) ~= "number" or id < 0 or id > 0xFFFA then
        dsl_error("Event ID must be a number in range 0x0000-0xFFFA (0xFFFB-0xFFFF reserved for system)")
    end
    
    if current_module.event_names[name] then
        dsl_error("Event '" .. name .. "' already defined")
    end
    
    table.insert(current_module.events, { name = name, id = id })
    current_module.event_names[name] = id
    
    return id
end

-- Define multiple events at once
-- Usage: EVENTS { BUTTON_PRESS = 0x0001, SENSOR_TRIGGER = 0x0002, ... }
function _G.EVENTS(event_table)
    if not current_module then
        dsl_error("No module started")
    end
    
    for name, id in pairs(event_table) do
        EVENT(name, id)
    end
end

-- Get event ID by name (for use in tree definitions)
function _G.EVENT_ID(name)
    if not current_module then
        dsl_error("No module started")
    end
    
    local id = current_module.event_names[name]
    if not id then
        dsl_error("Unknown event: " .. name)
    end
    
    return id
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
        pointer_count = 0,  -- Count of pt_m_call nodes
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
        pointer_index = nil,  -- Set for pt_m_call
    }
    
    -- Track pointer index for pt_m_call
    if call_type == "pt_m_call" then
        node.pointer_index = current_tree.pointer_count
        current_tree.pointer_count = current_tree.pointer_count + 1
    end
    
    -- Register function in correct table
    local func_list = nil
    if call_type == "o_call" or call_type == "io_call" then
        func_list = current_module.oneshot_funcs
    elseif call_type == "m_call" or call_type == "pt_m_call" then
        func_list = current_module.main_funcs
    elseif call_type == "p_call" or call_type == "p_call_composite" then
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

-- Composite predicate call (for boolean composition: AND, OR, NOT, etc.)
function _G.p_call_composite(func_name)
    in_composite_block = true
    return start_call(func_name, "p_call_composite")
end

-- Backward compatibility alias
function _G.p_call_bit(func_name)
    return _G.p_call_composite(func_name)
end

function _G.end_call(node)
    if #current_call_stack == 0 then
        dsl_error("No call to end")
    end
    
    local top = current_call_stack[#current_call_stack]
    if top.call_type == "p_call_composite" then
        in_composite_block = false
    end
    
    table.remove(current_call_stack)
    return top
end

-- Validation for composite-block-only functions
function _G.check_composite_block_only(func_name)
    if not in_composite_block then
        dsl_error(func_name .. "() can only be used inside a composite predicate block")
    end
end

-- Backward compatibility alias
function _G.check_bit_block_only(func_name)
    _G.check_composite_block_only(func_name)
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
-- RESULT CODES (S-Expression Engine Internal)
-- 
-- These codes are returned by the engine. The calling entity (ChainTree, 
-- state machine runtime, etc.) interprets them and decides the actual action.
-- ============================================================================

-- Propagate to caller - caller decides action
_G.SE_CONTINUE = 0           -- Continue execution, return CONTINUE to caller
_G.SE_TERMINATE = 1          -- Terminate, return TERMINATE to caller
_G.SE_RESET = 2              -- Reset, return RESET to caller
_G.SE_DISABLE = 3            -- Disable, return DISABLE to caller
_G.SE_HALT = 4               -- Halt, return HALT to caller
_G.SE_SKIP_CONTINUE = 5      -- Skip children, return CONTINUE to caller

-- Function-level - handled internally, mapped to caller codes
_G.SE_FUNCTION_HALT = 6      -- Halt this function, caller sees CONTINUE
_G.SE_FUNCTION_RESET = 7     -- Reset tree internally, caller sees CONTINUE
_G.SE_FUNCTION_TERMINATE = 8 -- Terminate function, caller sees DISABLE

-- ============================================================================
-- MODULE GENERATOR CLASS (for C headers)
-- ============================================================================

local ModuleGenerator = {}
ModuleGenerator.__index = ModuleGenerator

function ModuleGenerator.new(module_data)
    local self = setmetatable({}, ModuleGenerator)
    self.module = module_data
    self.is_64bit = (module_data.pointer_size == 8)
    return self
end

function ModuleGenerator:to_c_records_header(base_name)
    local lines = {}
    local mod = self.module
    local guard = base_name:upper() .. "_RECORDS_H"
    
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "// " .. base_name .. "_records.h")
    table.insert(lines, "// Generated record structures for " .. mod.name)
    table.insert(lines, "// DO NOT EDIT - Generated by s_expr_dsl v5.1")
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
                -- Handle primitive types that don't need _t suffix
                local target = field.target_type
                if target == "char" or target == "void" or 
                   target == "int" or target == "float" or target == "double" then
                    ctype = target .. "*"
                else
                    ctype = target .. "_t*"
                end
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
    table.insert(lines, "// DO NOT EDIT - Generated by s_expr_dsl v5.1")
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
    
    -- User-defined events
    if mod.events and #mod.events > 0 then
        table.insert(lines, "// User-defined event IDs")
        for _, evt in ipairs(mod.events) do
            local def_name = "SE_EVENT_" .. evt.name:upper()
            table.insert(lines, string.format("#define %s 0x%04X", def_name, evt.id))
        end
        table.insert(lines, "")
    end
    
    -- String table
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
    
    table.insert(lines, "#ifdef __cplusplus")
    table.insert(lines, "}")
    table.insert(lines, "#endif")
    table.insert(lines, "")
    table.insert(lines, "#endif // " .. guard)
    
    return table.concat(lines, "\n")
end

function ModuleGenerator:to_c_debug_header(base_name)
    local lines = {}
    local mod = self.module
    local guard = base_name:upper() .. "_DEBUG_H"
    
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "// " .. base_name .. "_debug.h")
    table.insert(lines, "// Debug hash reference for " .. mod.name)
    table.insert(lines, "// DO NOT EDIT - Generated by s_expr_dsl v5.1")
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "")
    table.insert(lines, "#ifndef " .. guard)
    table.insert(lines, "#define " .. guard)
    table.insert(lines, "")
    
    local function hash_hex(h)
        local u32 = ffi.new("uint32_t", h)
        return string.format("%08X", tonumber(u32))
    end
    
    if #mod.oneshot_funcs > 0 then
        table.insert(lines, "// Oneshot functions")
        for _, name in ipairs(mod.oneshot_funcs) do
            local h = M.fnv1a_32(name)
            table.insert(lines, string.format("#define H_%s %s  // %s", hash_hex(h), M.fmt_hash(h), name))
        end
        table.insert(lines, "")
    end
    
    if #mod.main_funcs > 0 then
        table.insert(lines, "// Main functions")
        for _, name in ipairs(mod.main_funcs) do
            local h = M.fnv1a_32(name)
            table.insert(lines, string.format("#define H_%s %s  // %s", hash_hex(h), M.fmt_hash(h), name))
        end
        table.insert(lines, "")
    end
    
    if #mod.pred_funcs > 0 then
        table.insert(lines, "// Predicate functions")
        for _, name in ipairs(mod.pred_funcs) do
            local h = M.fnv1a_32(name)
            table.insert(lines, string.format("#define H_%s %s  // %s", hash_hex(h), M.fmt_hash(h), name))
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
    table.insert(lines, "// DO NOT EDIT - Generated by s_expr_dsl v5.1")
    table.insert(lines, "// NOTE: Builtin functions (SE_*) are in s_engine_builtins.h")
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "")
    table.insert(lines, "#ifndef " .. guard)
    table.insert(lines, "#define " .. guard)
    table.insert(lines, "")
    table.insert(lines, '#include "s_engine_types.h"')
    table.insert(lines, "")
    table.insert(lines, "#ifdef __cplusplus")
    table.insert(lines, 'extern "C" {')
    table.insert(lines, "#endif")
    table.insert(lines, "")
    
    -- Helper to convert UPPER_CASE to lower_case
    local function to_c_name(name)
        return name:lower()
    end
    
    -- Filter out builtin functions
    local user_oneshot = {}
    local user_main = {}
    local user_pred = {}
    
    for _, name in ipairs(mod.oneshot_funcs) do
        if not is_builtin(name) then
            table.insert(user_oneshot, name)
        end
    end
    for _, name in ipairs(mod.main_funcs) do
        if not is_builtin(name) then
            table.insert(user_main, name)
        end
    end
    for _, name in ipairs(mod.pred_funcs) do
        if not is_builtin(name) then
            table.insert(user_pred, name)
        end
    end
    
    -- Common parameter list
    local param_list = [[
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
)]]
    
    if #user_oneshot > 0 then
        table.insert(lines, "// Oneshot functions")
        for _, name in ipairs(user_oneshot) do
            table.insert(lines, string.format("void %s(", to_c_name(name)))
            table.insert(lines, param_list .. ";")
            table.insert(lines, "")
        end
    end
    
    if #user_main > 0 then
        table.insert(lines, "// Main functions")
        for _, name in ipairs(user_main) do
            table.insert(lines, string.format("s_expr_result_t %s(", to_c_name(name)))
            table.insert(lines, param_list .. ";")
            table.insert(lines, "")
        end
    end
    
    if #user_pred > 0 then
        table.insert(lines, "// Predicate functions")
        for _, name in ipairs(user_pred) do
            table.insert(lines, string.format("bool %s(", to_c_name(name)))
            table.insert(lines, param_list .. ";")
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

function ModuleGenerator:to_c_user_registration(base_name)
    local lines = {}
    local mod = self.module
    
    -- Helper to convert UPPER_CASE to lower_case
    local function to_c_name(name)
        return name:lower()
    end
    
    -- Filter out builtin functions
    local user_oneshot = {}
    local user_main = {}
    local user_pred = {}
    
    for _, name in ipairs(mod.oneshot_funcs) do
        if not is_builtin(name) then
            table.insert(user_oneshot, name)
        end
    end
    for _, name in ipairs(mod.main_funcs) do
        if not is_builtin(name) then
            table.insert(user_main, name)
        end
    end
    for _, name in ipairs(mod.pred_funcs) do
        if not is_builtin(name) then
            table.insert(user_pred, name)
        end
    end
    
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "// " .. base_name .. "_user_registration.c")
    table.insert(lines, "// User function registration for " .. mod.name)
    table.insert(lines, "// DO NOT EDIT - Generated by s_expr_dsl v5.1")
    table.insert(lines, "// NOTE: Builtin functions (SE_*) are registered via s_engine_register_builtins()")
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "")
    table.insert(lines, '#include "' .. base_name .. '.h"')
    table.insert(lines, '#include "' .. base_name .. '_user_functions.h"')
    table.insert(lines, '#include "s_engine_module.h"')
    table.insert(lines, "")
    
    -- Generate oneshot table
    if #user_oneshot > 0 then
        table.insert(lines, "// Oneshot function entries")
        table.insert(lines, "static s_expr_fn_entry_t " .. base_name .. "_oneshot_entries[] = {")
        for _, name in ipairs(user_oneshot) do
            table.insert(lines, string.format("    { %s, (void*)%s },", 
                M.fmt_hash(M.fnv1a_32(name)), to_c_name(name)))
        end
        table.insert(lines, "};")
        table.insert(lines, "")
        table.insert(lines, "static const s_expr_fn_table_t " .. base_name .. "_oneshot_table = {")
        table.insert(lines, "    .entries = " .. base_name .. "_oneshot_entries,")
        table.insert(lines, "    .count = " .. #user_oneshot)
        table.insert(lines, "};")
        table.insert(lines, "")
    end
    
    -- Generate main table
    if #user_main > 0 then
        table.insert(lines, "// Main function entries")
        table.insert(lines, "static s_expr_fn_entry_t " .. base_name .. "_main_entries[] = {")
        for _, name in ipairs(user_main) do
            table.insert(lines, string.format("    { %s, (void*)%s },", 
                M.fmt_hash(M.fnv1a_32(name)), to_c_name(name)))
        end
        table.insert(lines, "};")
        table.insert(lines, "")
        table.insert(lines, "static const s_expr_fn_table_t " .. base_name .. "_main_table = {")
        table.insert(lines, "    .entries = " .. base_name .. "_main_entries,")
        table.insert(lines, "    .count = " .. #user_main)
        table.insert(lines, "};")
        table.insert(lines, "")
    end
    
    -- Generate pred table
    if #user_pred > 0 then
        table.insert(lines, "// Predicate function entries")
        table.insert(lines, "static s_expr_fn_entry_t " .. base_name .. "_pred_entries[] = {")
        for _, name in ipairs(user_pred) do
            table.insert(lines, string.format("    { %s, (void*)%s },", 
                M.fmt_hash(M.fnv1a_32(name)), to_c_name(name)))
        end
        table.insert(lines, "};")
        table.insert(lines, "")
        table.insert(lines, "static const s_expr_fn_table_t " .. base_name .. "_pred_table = {")
        table.insert(lines, "    .entries = " .. base_name .. "_pred_entries,")
        table.insert(lines, "    .count = " .. #user_pred)
        table.insert(lines, "};")
        table.insert(lines, "")
    end
    
    -- Generate table accessor functions
    table.insert(lines, "// Table accessors")
    
    if #user_oneshot > 0 then
        table.insert(lines, "const s_expr_fn_table_t* " .. base_name .. "_get_oneshot_table(void) {")
        table.insert(lines, "    return &" .. base_name .. "_oneshot_table;")
        table.insert(lines, "}")
    else
        table.insert(lines, "const s_expr_fn_table_t* " .. base_name .. "_get_oneshot_table(void) {")
        table.insert(lines, "    return NULL;")
        table.insert(lines, "}")
    end
    table.insert(lines, "")
    
    if #user_main > 0 then
        table.insert(lines, "const s_expr_fn_table_t* " .. base_name .. "_get_main_table(void) {")
        table.insert(lines, "    return &" .. base_name .. "_main_table;")
        table.insert(lines, "}")
    else
        table.insert(lines, "const s_expr_fn_table_t* " .. base_name .. "_get_main_table(void) {")
        table.insert(lines, "    return NULL;")
        table.insert(lines, "}")
    end
    table.insert(lines, "")
    
    if #user_pred > 0 then
        table.insert(lines, "const s_expr_fn_table_t* " .. base_name .. "_get_pred_table(void) {")
        table.insert(lines, "    return &" .. base_name .. "_pred_table;")
        table.insert(lines, "}")
    else
        table.insert(lines, "const s_expr_fn_table_t* " .. base_name .. "_get_pred_table(void) {")
        table.insert(lines, "    return NULL;")
        table.insert(lines, "}")
    end
    table.insert(lines, "")
    
    -- Generate convenience register-all function
    table.insert(lines, "// Register all user functions with module")
    table.insert(lines, "void " .. base_name .. "_register_all(s_expr_module_t* module) {")
    if #user_oneshot > 0 then
        table.insert(lines, "    s_expr_module_register_oneshot(module, &" .. base_name .. "_oneshot_table);")
    end
    if #user_main > 0 then
        table.insert(lines, "    s_expr_module_register_main(module, &" .. base_name .. "_main_table);")
    end
    if #user_pred > 0 then
        table.insert(lines, "    s_expr_module_register_pred(module, &" .. base_name .. "_pred_table);")
    end
    table.insert(lines, "}")
    
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
        table.insert(lines, string.format("  %s (size=%d, align=%d, hash=%s)", name, rec.size, rec.align, M.fmt_hash(rec.name_hash)))
    end
    table.insert(lines, "")
    
    table.insert(lines, "TREES (" .. #mod.tree_order .. "):")
    for _, name in ipairs(mod.tree_order) do
        local tree = mod.trees[name]
        table.insert(lines, string.format("  %s (nodes=%d, ptrs=%d, record=%s)", name, tree.node_count, tree.pointer_count, tree.record_name or "none"))
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
-- BINARY GENERATOR - VERSION 5.1 (DIRECT s_expr_param_t)
-- ============================================================================

-- Binary format constants
local SEXB_MAGIC = 0x42584553    -- "SEXB"
local SEXB_VERSION = 0x0501      -- Version 5.1

local SEXB_FLAG_32BIT = 0x0000
local SEXB_FLAG_64BIT = 0x0001
local SEXB_FLAG_DEBUG = 0x0002

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

function BinaryEmitter:emit_i64(v)
    if v < 0 then
        -- Handle negative: two's complement
        local abs_val = -v
        local lo = bit.band(abs_val, 0xFFFFFFFF)
        local hi = math.floor(abs_val / 0x100000000)
        -- Negate
        lo = bit.band(bit.bnot(lo), 0xFFFFFFFF)
        hi = bit.band(bit.bnot(hi), 0xFFFFFFFF)
        -- Add 1
        lo = lo + 1
        if lo > 0xFFFFFFFF then
            lo = 0
            hi = hi + 1
        end
        self:emit_u32(lo)
        self:emit_u32(hi)
    else
        self:emit_u64(v)
    end
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
    -- Add null terminator
    self:emit_u8(0)
    -- Pad to 4-byte boundary
    local total = 2 + len + 1
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

-- ============================================================================
-- BINARY MODULE GENERATOR - DIRECT s_expr_param_t EMISSION
-- ============================================================================

local BinaryModuleGenerator = {}
BinaryModuleGenerator.__index = BinaryModuleGenerator

function BinaryModuleGenerator.new(module_data)
    local self = setmetatable({}, BinaryModuleGenerator)
    self.module = module_data
    self.is_64bit = (module_data.pointer_size == 8)
    self.param_size = self.is_64bit and 16 or 8
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
    
    -- Build function hash -> index mappings
    self.oneshot_hash_index = {}
    for i, name in ipairs(mod.oneshot_funcs) do
        self.oneshot_hash_index[M.fnv1a_32(name)] = i - 1
    end
    
    self.main_hash_index = {}
    for i, name in ipairs(mod.main_funcs) do
        self.main_hash_index[M.fnv1a_32(name)] = i - 1
    end
    
    self.pred_hash_index = {}
    for i, name in ipairs(mod.pred_funcs) do
        self.pred_hash_index[M.fnv1a_32(name)] = i - 1
    end
end

function BinaryModuleGenerator:emit_param_struct(e, param_type, index_to_pointer, u16_a, u16_b, value_32_or_64)
    -- Byte 0: type
    e:emit_u8(param_type)
    
    -- Byte 1: index_to_pointer
    e:emit_u8(index_to_pointer or 0)
    
    if self.is_64bit then
        -- 64-bit layout: [type:1][idx:1][pad:6][union:8] = 16 bytes
        for i = 1, 6 do e:emit_u8(0) end
        
        if value_32_or_64 then
            if type(value_32_or_64) == "number" then
                if param_type == S_EXPR_PARAM.FLOAT then
                    e:emit_f64(value_32_or_64)
                elseif param_type == S_EXPR_PARAM.INT then
                    e:emit_i64(value_32_or_64)
                else
                    e:emit_u64(value_32_or_64)
                end
            else
                e:emit_u64(0)
            end
        else
            e:emit_u16(u16_a or 0)
            e:emit_u16(u16_b or 0)
            e:emit_u32(0)  -- padding
        end
    else
        -- 32-bit layout: [type:1][idx:1][union:4][pad:2] = 8 bytes
        -- Union starts at byte 2 (NO padding between idx and union!)
        if value_32_or_64 then
            if type(value_32_or_64) == "number" then
                if param_type == S_EXPR_PARAM.FLOAT then
                    e:emit_f32(value_32_or_64)
                elseif param_type == S_EXPR_PARAM.INT then
                    e:emit_i32(value_32_or_64)
                else
                    e:emit_u32(value_32_or_64)
                end
            else
                e:emit_u32(0)
            end
            e:emit_u16(0)  -- struct end padding (bytes 6-7)
        else
            e:emit_u16(u16_a or 0)  -- bytes 2-3 (node_index or brace_idx)
            e:emit_u16(u16_b or 0)  -- bytes 4-5 (func_index)
            e:emit_u16(0)           -- bytes 6-7 (struct end padding)
        end
    end
end
-- ============================================================================
-- ============================================================================
-- DEBUG HEADER GENERATOR
-- Call after finalize_module()
-- ============================================================================

-- ============================================================================
-- DEBUG HEADER GENERATOR
-- Call after end_module()
-- ============================================================================

function M.generate_debug_header(module)
    local lines = {}
    
    local guard = string.upper(module.name) .. "_DEBUG_H"
    local prefix = string.upper(module.name)
    
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "// " .. module.name .. "_debug.h")
    table.insert(lines, "// DEBUG INFORMATION - Auto-generated by s_expr_dsl.lua")
    table.insert(lines, "// Module: " .. module.name)
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "")
    table.insert(lines, "#ifndef " .. guard)
    table.insert(lines, "#define " .. guard)
    table.insert(lines, "")
    table.insert(lines, "#include <stdint.h>")
    table.insert(lines, "#include <stdio.h>")
    table.insert(lines, "")
    
    -- Get the actual arrays (handle both naming conventions)
    local tree_order = module.tree_order or {}
    local trees = module.trees or {}
    local record_order = module.record_order or {}
    local records = module.records or {}
    local oneshot_funcs = module.oneshot_funcs or {}
    local main_funcs = module.main_funcs or {}
    local pred_funcs = module.pred_funcs or {}
    
    -- ========================================================================
    -- Tree name table
    -- ========================================================================
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "// TREE DEBUG INFO")
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "")
    table.insert(lines, "typedef struct {")
    table.insert(lines, "    uint32_t    hash;")
    table.insert(lines, "    const char* name;")
    table.insert(lines, "    uint16_t    node_count;")
    table.insert(lines, "    uint16_t    pointer_count;")
    table.insert(lines, "    uint16_t    param_count;")
    table.insert(lines, "} " .. module.name .. "_tree_debug_t;")
    table.insert(lines, "")
    
    table.insert(lines, "static const " .. module.name .. "_tree_debug_t " .. module.name .. "_tree_debug[] = {")
    if #tree_order > 0 then
        for i, tree_name in ipairs(tree_order) do
            local tree = trees[tree_name] or {}
            local name_hash = tree.name_hash or M.fnv1a_32(tree_name)
            local node_count = tree.node_count or 0
            local pointer_count = tree.pointer_count or 0
            local param_count = tree.params and #tree.params or 0
            table.insert(lines, string.format(
                '    { 0x%08X, "%s", %d, %d, %d },',
                name_hash,
                tree_name,
                node_count,
                pointer_count,
                param_count
            ))
        end
    else
        table.insert(lines, '    { 0, "", 0, 0, 0 },  // Empty placeholder')
    end
    table.insert(lines, "};")
    table.insert(lines, string.format("#define %s_TREE_DEBUG_COUNT %d", prefix, math.max(1, #tree_order)))
    table.insert(lines, "")
    
    -- ========================================================================
    -- Function hash tables
    -- ========================================================================
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "// FUNCTION DEBUG INFO")
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "")
    table.insert(lines, "typedef struct {")
    table.insert(lines, "    uint32_t    hash;")
    table.insert(lines, "    const char* name;")
    table.insert(lines, "    uint16_t    index;")
    table.insert(lines, "} " .. module.name .. "_func_debug_t;")
    table.insert(lines, "")
    
    -- Helper to emit function table
    local function emit_func_table(func_list, table_name, count_name)
        table.insert(lines, "static const " .. module.name .. "_func_debug_t " .. module.name .. "_" .. table_name .. "[] = {")
        if #func_list > 0 then
            for i, name in ipairs(func_list) do
                local hash = M.fnv1a_32(name)
                table.insert(lines, string.format(
                    '    { 0x%08X, "%s", %d },',
                    hash,
                    name,
                    i - 1
                ))
            end
        else
            table.insert(lines, '    { 0, "", 0 },  // Empty placeholder')
        end
        table.insert(lines, "};")
        table.insert(lines, string.format("#define %s_%s %d", prefix, count_name, math.max(1, #func_list)))
        table.insert(lines, "")
    end
    
    table.insert(lines, "// Oneshot functions")
    emit_func_table(oneshot_funcs, "oneshot_debug", "ONESHOT_DEBUG_COUNT")
    
    table.insert(lines, "// Main functions")
    emit_func_table(main_funcs, "main_debug", "MAIN_DEBUG_COUNT")
    
    table.insert(lines, "// Predicate functions")
    emit_func_table(pred_funcs, "pred_debug", "PRED_DEBUG_COUNT")
    
    -- ========================================================================
    -- Record debug info
    -- ========================================================================
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "// RECORD DEBUG INFO")
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "")
    table.insert(lines, "typedef struct {")
    table.insert(lines, "    uint32_t    hash;")
    table.insert(lines, "    const char* name;")
    table.insert(lines, "    uint16_t    offset;")
    table.insert(lines, "    uint16_t    size;")
    table.insert(lines, "} " .. module.name .. "_field_debug_t;")
    table.insert(lines, "")
    table.insert(lines, "typedef struct {")
    table.insert(lines, "    uint32_t    hash;")
    table.insert(lines, "    const char* name;")
    table.insert(lines, "    uint16_t    size;")
    table.insert(lines, "    uint16_t    field_count;")
    table.insert(lines, "    const " .. module.name .. "_field_debug_t* fields;")
    table.insert(lines, "} " .. module.name .. "_record_debug_t;")
    table.insert(lines, "")
    
    -- Emit field tables for each record
    for _, record_name in ipairs(record_order) do
        local record = records[record_name] or {}
        local record_fields = record.fields or {}
        local field_table_name = module.name .. "_" .. string.lower(record_name) .. "_fields_debug"
        table.insert(lines, "static const " .. module.name .. "_field_debug_t " .. field_table_name .. "[] = {")
        if #record_fields > 0 then
            for _, field in ipairs(record_fields) do
                local field_name = field.name or "unknown"
                table.insert(lines, string.format(
                    '    { 0x%08X, "%s", %d, %d },',
                    M.fnv1a_32(field_name),
                    field_name,
                    field.offset or 0,
                    field.size or 0
                ))
            end
        else
            table.insert(lines, '    { 0, "", 0, 0 },  // Empty placeholder')
        end
        table.insert(lines, "};")
        table.insert(lines, "")
    end
    
    -- Emit record table
    table.insert(lines, "static const " .. module.name .. "_record_debug_t " .. module.name .. "_record_debug[] = {")
    if #record_order > 0 then
        for _, record_name in ipairs(record_order) do
            local record = records[record_name] or {}
            local record_fields = record.fields or {}
            local field_table_name = module.name .. "_" .. string.lower(record_name) .. "_fields_debug"
            table.insert(lines, string.format(
                '    { 0x%08X, "%s", %d, %d, %s },',
                M.fnv1a_32(record_name),
                record_name,
                record.size or 0,
                #record_fields,
                field_table_name
            ))
        end
    else
        table.insert(lines, '    { 0, "", 0, 0, NULL },  // Empty placeholder')
    end
    table.insert(lines, "};")
    table.insert(lines, string.format("#define %s_RECORD_DEBUG_COUNT %d", prefix, math.max(1, #record_order)))
    table.insert(lines, "")
    
    -- ========================================================================
    -- Lookup functions
    -- ========================================================================
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "// DEBUG LOOKUP FUNCTIONS")
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "")
    
    -- Tree lookup
    table.insert(lines, "static inline const char* " .. module.name .. "_find_tree_name(uint32_t hash) {")
    table.insert(lines, "    for (int i = 0; i < " .. prefix .. "_TREE_DEBUG_COUNT; i++) {")
    table.insert(lines, "        if (" .. module.name .. "_tree_debug[i].hash == hash) {")
    table.insert(lines, "            return " .. module.name .. "_tree_debug[i].name;")
    table.insert(lines, "        }")
    table.insert(lines, "    }")
    table.insert(lines, '    return "UNKNOWN";')
    table.insert(lines, "}")
    table.insert(lines, "")
    
    -- Main function lookup
    table.insert(lines, "static inline const char* " .. module.name .. "_find_main_name(uint32_t hash) {")
    table.insert(lines, "    for (int i = 0; i < " .. prefix .. "_MAIN_DEBUG_COUNT; i++) {")
    table.insert(lines, "        if (" .. module.name .. "_main_debug[i].hash == hash) {")
    table.insert(lines, "            return " .. module.name .. "_main_debug[i].name;")
    table.insert(lines, "        }")
    table.insert(lines, "    }")
    table.insert(lines, '    return "UNKNOWN";')
    table.insert(lines, "}")
    table.insert(lines, "")
    
    -- Oneshot function lookup
    table.insert(lines, "static inline const char* " .. module.name .. "_find_oneshot_name(uint32_t hash) {")
    table.insert(lines, "    for (int i = 0; i < " .. prefix .. "_ONESHOT_DEBUG_COUNT; i++) {")
    table.insert(lines, "        if (" .. module.name .. "_oneshot_debug[i].hash == hash) {")
    table.insert(lines, "            return " .. module.name .. "_oneshot_debug[i].name;")
    table.insert(lines, "        }")
    table.insert(lines, "    }")
    table.insert(lines, '    return "UNKNOWN";')
    table.insert(lines, "}")
    table.insert(lines, "")
    
    -- Predicate function lookup
    table.insert(lines, "static inline const char* " .. module.name .. "_find_pred_name(uint32_t hash) {")
    table.insert(lines, "    for (int i = 0; i < " .. prefix .. "_PRED_DEBUG_COUNT; i++) {")
    table.insert(lines, "        if (" .. module.name .. "_pred_debug[i].hash == hash) {")
    table.insert(lines, "            return " .. module.name .. "_pred_debug[i].name;")
    table.insert(lines, "        }")
    table.insert(lines, "    }")
    table.insert(lines, '    return "UNKNOWN";')
    table.insert(lines, "}")
    table.insert(lines, "")
    
    -- Record lookup
    table.insert(lines, "static inline const char* " .. module.name .. "_find_record_name(uint32_t hash) {")
    table.insert(lines, "    for (int i = 0; i < " .. prefix .. "_RECORD_DEBUG_COUNT; i++) {")
    table.insert(lines, "        if (" .. module.name .. "_record_debug[i].hash == hash) {")
    table.insert(lines, "            return " .. module.name .. "_record_debug[i].name;")
    table.insert(lines, "        }")
    table.insert(lines, "    }")
    table.insert(lines, '    return "UNKNOWN";')
    table.insert(lines, "}")
    table.insert(lines, "")
    
    -- Print all trees
    table.insert(lines, "static inline void " .. module.name .. "_print_trees(void) {")
    table.insert(lines, '    printf("' .. module.name .. ' Trees (%d):\\n", ' .. prefix .. '_TREE_DEBUG_COUNT);')
    table.insert(lines, "    for (int i = 0; i < " .. prefix .. "_TREE_DEBUG_COUNT; i++) {")
    table.insert(lines, "        const " .. module.name .. "_tree_debug_t* t = &" .. module.name .. "_tree_debug[i];")
    table.insert(lines, '        printf("  [%2d] 0x%08X %-32s nodes=%d ptrs=%d params=%d\\n",')
    table.insert(lines, "               i, t->hash, t->name, t->node_count, t->pointer_count, t->param_count);")
    table.insert(lines, "    }")
    table.insert(lines, "}")
    table.insert(lines, "")
    
    -- Print all functions
    table.insert(lines, "static inline void " .. module.name .. "_print_functions(void) {")
    table.insert(lines, '    printf("' .. module.name .. ' Main Functions (%d):\\n", ' .. prefix .. '_MAIN_DEBUG_COUNT);')
    table.insert(lines, "    for (int i = 0; i < " .. prefix .. "_MAIN_DEBUG_COUNT; i++) {")
    table.insert(lines, '        printf("  [%2d] 0x%08X %s\\n", i, ' .. module.name .. '_main_debug[i].hash, ' .. module.name .. '_main_debug[i].name);')
    table.insert(lines, "    }")
    table.insert(lines, '    printf("' .. module.name .. ' Oneshot Functions (%d):\\n", ' .. prefix .. '_ONESHOT_DEBUG_COUNT);')
    table.insert(lines, "    for (int i = 0; i < " .. prefix .. "_ONESHOT_DEBUG_COUNT; i++) {")
    table.insert(lines, '        printf("  [%2d] 0x%08X %s\\n", i, ' .. module.name .. '_oneshot_debug[i].hash, ' .. module.name .. '_oneshot_debug[i].name);')
    table.insert(lines, "    }")
    table.insert(lines, '    printf("' .. module.name .. ' Predicate Functions (%d):\\n", ' .. prefix .. '_PRED_DEBUG_COUNT);')
    table.insert(lines, "    for (int i = 0; i < " .. prefix .. "_PRED_DEBUG_COUNT; i++) {")
    table.insert(lines, '        printf("  [%2d] 0x%08X %s\\n", i, ' .. module.name .. '_pred_debug[i].hash, ' .. module.name .. '_pred_debug[i].name);')
    table.insert(lines, "    }")
    table.insert(lines, "}")
    table.insert(lines, "")
    
    -- Print all records
    table.insert(lines, "static inline void " .. module.name .. "_print_records(void) {")
    table.insert(lines, '    printf("' .. module.name .. ' Records (%d):\\n", ' .. prefix .. '_RECORD_DEBUG_COUNT);')
    table.insert(lines, "    for (int i = 0; i < " .. prefix .. "_RECORD_DEBUG_COUNT; i++) {")
    table.insert(lines, "        const " .. module.name .. "_record_debug_t* r = &" .. module.name .. "_record_debug[i];")
    table.insert(lines, '        printf("  [%2d] 0x%08X %-24s size=%d fields=%d\\n",')
    table.insert(lines, "               i, r->hash, r->name, r->size, r->field_count);")
    table.insert(lines, "        for (int j = 0; j < r->field_count; j++) {")
    table.insert(lines, "            const " .. module.name .. "_field_debug_t* f = &r->fields[j];")
    table.insert(lines, '            printf("       [%2d] 0x%08X %-20s offset=%d size=%d\\n",')
    table.insert(lines, "                   j, f->hash, f->name, f->offset, f->size);")
    table.insert(lines, "        }")
    table.insert(lines, "    }")
    table.insert(lines, "}")
    table.insert(lines, "")
    
    -- Print all debug info
    table.insert(lines, "static inline void " .. module.name .. "_print_debug_all(void) {")
    table.insert(lines, '    printf("\\n========== ' .. string.upper(module.name) .. ' DEBUG INFO ==========\\n\\n");')
    table.insert(lines, "    " .. module.name .. "_print_trees();")
    table.insert(lines, '    printf("\\n");')
    table.insert(lines, "    " .. module.name .. "_print_functions();")
    table.insert(lines, '    printf("\\n");')
    table.insert(lines, "    " .. module.name .. "_print_records();")
    table.insert(lines, '    printf("\\n");')
    table.insert(lines, "}")
    table.insert(lines, "")
    
    table.insert(lines, "#endif // " .. guard)
    table.insert(lines, "")
    
    return table.concat(lines, "\n")
end

-- Write debug header to file
function M.write_debug_header(module, output_dir)
    local content = M.generate_debug_header(module)
    local path = output_dir or "./" .. module.name .. "_debug.h"
    local f = io.open(path, "w")
    if f then
        f:write(content)
        f:close()
        print("Generated: " .. path)
    else
        error("Failed to write: " .. path)
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
    
    local tree_param_patches = {}
    for _, name in ipairs(mod.tree_order) do
        local tree = mod.trees[name]
        
        e:emit_u32(tree.name_hash)
        
        -- Record hash (not index - for lookup)
        local rec_hash = 0
        if tree.record_name and mod.records[tree.record_name] then
            rec_hash = mod.records[tree.record_name].name_hash
        end
        e:emit_u32(rec_hash)
        
        e:emit_u16(tree.node_count)
        e:emit_u16(tree.pointer_count or 0)
        
        local param_patch = e:get_pos()
        e:emit_u32(0)  -- param_offset placeholder
        e:emit_u16(0)  -- param_count placeholder
        e:emit_u16(0)  -- reserved
        
        table.insert(tree_param_patches, {
            tree = tree,
            offset_patch = param_patch,
            count_patch = param_patch + 4,
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
    
    -- ========== FUNCTION HASH TABLES ==========
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
    
    -- ========== PARAMETERS (direct s_expr_param_t arrays) ==========
    local params_offset = e:get_pos()
    e:patch_u32(dir_start + 28, params_offset)
    
    -- Align to param size for direct casting
    e:align(self.param_size)
    
    for _, patch in ipairs(tree_param_patches) do
        local tree = patch.tree
        local start_pos = e:get_pos()
        e:patch_u32(patch.offset_patch, start_pos)
        
        -- Flatten tree and emit params
        local param_count = self:emit_tree_params(e, tree)
        
        e:patch_u16(patch.count_patch, param_count)
        
        -- Align for next tree
        e:align(self.param_size)
    end
    
    -- ========== FINALIZE ==========
    e:patch_u32(size_patch, e:get_pos())
    
    return e:to_bytes(), e:get_pos()
end

function BinaryModuleGenerator:emit_tree_params(e, tree)
    local param_count = 0
    local current_node_idx = 0  -- Track node index as we emit
    
    local function emit_node(node)
        -- Determine opcode and func_index based on call_type
        local opcode, func_index
        
        if node.call_type == "o_call" then
            opcode = S_EXPR_PARAM.ONESHOT
            func_index = self.oneshot_hash_index[node.func_hash] or 0
        elseif node.call_type == "io_call" then
            opcode = bit.bor(S_EXPR_PARAM.ONESHOT, S_EXPR_FLAG_SURVIVES_RESET)
            func_index = self.oneshot_hash_index[node.func_hash] or 0
        elseif node.call_type == "m_call" then
            opcode = S_EXPR_PARAM.MAIN
            func_index = self.main_hash_index[node.func_hash] or 0
        elseif node.call_type == "pt_m_call" then
            opcode = bit.bor(S_EXPR_PARAM.MAIN, S_EXPR_FLAG_POINTER)
            func_index = self.main_hash_index[node.func_hash] or 0
        elseif node.call_type == "p_call" then
            opcode = S_EXPR_PARAM.PRED
            func_index = self.pred_hash_index[node.func_hash] or 0
        elseif node.call_type == "p_call_composite" then
            opcode = bit.bor(S_EXPR_PARAM.PRED, S_EXPR_FLAG_SURVIVES_RESET)
            func_index = self.pred_hash_index[node.func_hash] or 0
        else
            opcode = S_EXPR_PARAM.MAIN
            func_index = 0
        end
        
        -- Assign node index and increment
        local node_index = current_node_idx
        current_node_idx = current_node_idx + 1
        
        local idx_to_ptr = node.pointer_index or 0
        
        -- Calculate total params for brace matching
        local content_count = self:count_node_params(node)
        
        -- OPEN_CALL param
        self:emit_param_struct(e, S_EXPR_PARAM.OPEN_CALL, idx_to_ptr, content_count, 0)
        param_count = param_count + 1
        
        -- Function reference param: node_index first, then func_index (matches struct layout)
        self:emit_param_struct(e, opcode, idx_to_ptr, node_index, func_index)
        param_count = param_count + 1
        
        -- Emit parameters
        for _, param in ipairs(node.params) do
            param_count = param_count + self:emit_dsl_param(e, param)
        end
        
        -- Emit children recursively
        for _, child in ipairs(node.children) do
            emit_node(child)
        end
        
        -- CLOSE param
        self:emit_param_struct(e, S_EXPR_PARAM.CLOSE, 0, 0, 0)
        param_count = param_count + 1
    end
    
    -- Emit all top-level nodes
    for _, node in ipairs(tree.nodes) do
        emit_node(node)
    end
    
    return param_count
end

-- Count params in a node (for brace_idx)
function BinaryModuleGenerator:count_node_params(node)
    local count = 2  -- OPEN_CALL + func_ref
    
    count = count + #node.params
    
    for _, child in ipairs(node.children) do
        count = count + self:count_node_params(child) + 1  -- +1 for CLOSE
    end
    
    return count
end

-- Emit a DSL parameter as s_expr_param_t
function BinaryModuleGenerator:emit_dsl_param(e, param)
    local ptype = param.type
    local value = param.value
    
    if ptype == "int" then
        self:emit_param_struct(e, S_EXPR_PARAM.INT, 0, nil, nil, value)
        return 1
    elseif ptype == "uint" then
        self:emit_param_struct(e, S_EXPR_PARAM.UINT, 0, nil, nil, value)
        return 1
    elseif ptype == "float" then
        self:emit_param_struct(e, S_EXPR_PARAM.FLOAT, 0, nil, nil, value)
        return 1
    elseif ptype == "str_idx" or ptype == "str_ptr" then
        local idx = self.string_index[value] or 0
        local len = #value
        self:emit_param_struct(e, S_EXPR_PARAM.STR_IDX, 0, idx, len)
        return 1
    elseif ptype == "field_ref" or ptype == "nested_field_ref" then
        -- Resolve field offset/size from current tree's record
        local offset, size = 0, 0
        -- For now emit hash, runtime will resolve
        local hash = M.fnv1a_32(value)
        self:emit_param_struct(e, S_EXPR_PARAM.FIELD, 0, nil, nil, hash)
        return 1
    elseif ptype == "const_ref" then
        local idx = self.const_index[value] or 0
        self:emit_param_struct(e, S_EXPR_PARAM.CONST_REF, 0, idx, 0)
        return 1
    elseif ptype == "result" then
        self:emit_param_struct(e, S_EXPR_PARAM.RESULT, 0, nil, nil, value)
        return 1
    elseif ptype == "list_start" then
        self:emit_param_struct(e, S_EXPR_PARAM.OPEN, 0, 0, 0)
        return 1
    elseif ptype == "list_end" then
        self:emit_param_struct(e, S_EXPR_PARAM.CLOSE, 0, 0, 0)
        return 1
    else
        -- Unknown type, emit as uint
        self:emit_param_struct(e, S_EXPR_PARAM.UINT, 0, nil, nil, value or 0)
        return 1
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
    
    local mode_suffix = self.is_64bit and "_64" or "_32"
    local guard = base_name:upper() .. "_BIN" .. mode_suffix:upper() .. "_H"
    local var_name = base_name:lower() .. "_module_bin" .. mode_suffix
    
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "// " .. base_name .. "_bin" .. mode_suffix .. ".h")
    table.insert(lines, "// Generated binary module data for " .. self.module.name)
    table.insert(lines, "// Mode: " .. (self.is_64bit and "64-bit" or "32-bit"))
    table.insert(lines, "// Version: 5.1 (direct s_expr_param_t, zero-copy)")
    table.insert(lines, "// DO NOT EDIT - Generated by s_expr_dsl v5.1")
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "")
    table.insert(lines, "#ifndef " .. guard)
    table.insert(lines, "#define " .. guard)
    table.insert(lines, "")
    table.insert(lines, "#include <stdint.h>")
    table.insert(lines, "")
    table.insert(lines, string.format("#define %s_SIZE %d", var_name:upper(), size))
    table.insert(lines, string.format("#define %s_HASH %s", var_name:upper(), M.fmt_hash(self.module.name_hash)))
    table.insert(lines, string.format("#define %s_IS_64BIT %d", var_name:upper(), self.is_64bit and 1 or 0))
    table.insert(lines, "")
    table.insert(lines, string.format("static const uint8_t %s[%d] __attribute__((aligned(%d))) = {", 
                                       var_name, size, self.param_size))
    
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