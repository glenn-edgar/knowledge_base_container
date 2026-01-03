--============================================================================
-- S-EXPRESSION DSL v3.0
-- ChainTree Domain-Specific Language for Behavior Trees
--============================================================================

local bit = require("bit")
local ffi = require("ffi")

-- Load CFL helper functions (state machines, dispatchers, etc.)
-- This file should be in the same directory as this DSL
local cfl_path = debug.getinfo(1, "S").source:match("@?(.*/)")
if cfl_path then
    dofile(cfl_path .. "s_cfl_functions.lua")
else
    -- Fallback: try current directory
    local ok, err = pcall(dofile, "s_cfl_functions.lua")
    if not ok then
        -- Not fatal - CFL functions just won't be available
        -- print("Note: s_cfl_functions.lua not found - CFL helpers unavailable")
    end
end

--============================================================================
-- 64-BIT / 32-BIT MODE
--============================================================================

local _pointer_size = 4

function use_64bit()
    _pointer_size = 8
end

function use_32bit()
    _pointer_size = 4
end

function get_pointer_size()
    return _pointer_size
end

--============================================================================
-- FNV-1a 32-bit HASH
-- Pure Lua with explicit modular arithmetic to handle overflow correctly
--============================================================================

local FNV_PRIME_32 = 0x01000193
local FNV_OFFSET_32 = 0x811c9dc5

function hash32(str)
    local h = FNV_OFFSET_32
    for i = 1, #str do
        h = bit.bxor(h, str:byte(i))
        -- Keep positive after XOR (bit.bxor can return negative for high bit set)
        if h < 0 then h = h + 0x100000000 end
        -- Multiply in 16-bit parts to avoid double precision overflow
        local h_lo = h % 0x10000
        local h_hi = math.floor(h / 0x10000)
        local p_lo = FNV_PRIME_32 % 0x10000
        local p_hi = math.floor(FNV_PRIME_32 / 0x10000)
        
        local lo = h_lo * p_lo
        local mid = h_lo * p_hi + h_hi * p_lo
        -- Only keep lower 32 bits: lo + (mid_lo * 2^16), ignore mid_hi * 2^32
        h = (lo + (mid % 0x10000) * 0x10000) % 0x100000000
    end
    if h < 0 then h = h + 0x100000000 end
    return h
end

--============================================================================
-- FNV-1a 64-bit HASH
--============================================================================

local FNV_PRIME_64 = ffi.new("uint64_t", 0x00000100000001B3ULL)
local FNV_OFFSET_64 = ffi.new("uint64_t", 0xCBF29CE484222325ULL)

function hash64(str)
    local h = FNV_OFFSET_64
    for i = 1, #str do
        h = bit.bxor(h, str:byte(i))
        h = h * FNV_PRIME_64
    end
    return h
end

--============================================================================
-- AUTO-SELECT HASH BASED ON POINTER SIZE
--============================================================================

function hash_auto(str)
    if _pointer_size == 8 then
        return hash64(str)
    else
        return hash32(str)
    end
end

--============================================================================
-- FORMAT HELPERS
--============================================================================

function format_hash32(h)
    if h < 0 then
        return string.format("0x%08XU", h + 0x100000000)
    else
        return string.format("0x%08XU", h)
    end
end

function format_hash64(h)
    -- FFI uint64_t formatting
    local lo = tonumber(bit.band(h, 0xFFFFFFFF))
    local hi = tonumber(bit.rshift(h, 32))
    if lo < 0 then lo = lo + 0x100000000 end
    if hi < 0 then hi = hi + 0x100000000 end
    return string.format("0x%08X%08XULL", hi, lo)
end

function format_hash(h)
    if _pointer_size == 8 then
        return format_hash64(h)
    else
        return format_hash32(h)
    end
end

function get_hash_type()
    if _pointer_size == 8 then
        return "uint64_t"
    else
        return "uint32_t"
    end
end

--============================================================================
-- GENSYM
--============================================================================

local _gensym_counter = 0

function gensym(prefix)
    _gensym_counter = _gensym_counter + 1
    return string.format("%s_%d", prefix or "g", _gensym_counter)
end

function gensym_reset()
    _gensym_counter = 0
end

--============================================================================
-- PARAMETER OPCODES (must match C header)
--============================================================================

local PARAM_OPCODES = {
    INT       = 0x00,
    UINT      = 0x01,
    FLOAT     = 0x02,
    STR_HASH  = 0x03,
    SLOT      = 0x04,
    OPEN      = 0x05,
    CLOSE     = 0x06,
    OPEN_CALL = 0x07,
    ONESHOT   = 0x08,
    MAIN      = 0x09,
    PRED      = 0x0A,
    FIELD     = 0x0B,
    RESULT    = 0x0C,
}

local TYPE_FLAGS = {
    SURVIVES_RESET = 0x10,  -- bit 4: io_call
    POINTER        = 0x80,  -- bit 7: pt_m_call
}

--============================================================================
-- RETURN CODE CONSTANTS (must match C enum)
--============================================================================

SE_CONTINUE           = 0
SE_HALT               = 1
SE_TERMINATE          = 2
SE_RESET              = 3
SE_DISABLE            = 4
SE_FUNCTION_TERMINATE = 5
SE_SKIP_CONTINUE      = 6
SE_FUNCTION_HALT      = 7
SE_FUNCTION_RESET     = 8

--============================================================================
-- DEBUG FLAG
--============================================================================

local _debug_enabled = false

function set_debug(enabled)
    _debug_enabled = enabled
end

function is_debug()
    return _debug_enabled
end

function debug_print(...)
    if _debug_enabled then
        print("[DSL DEBUG]", ...)
    end
end

--============================================================================
-- BIT BLOCK TRACKING
--============================================================================

local _bit_block_depth = 0

function enter_bit_block()
    _bit_block_depth = _bit_block_depth + 1
end

function exit_bit_block()
    if _bit_block_depth > 0 then
        _bit_block_depth = _bit_block_depth - 1
    end
end

function in_bit_block()
    return _bit_block_depth > 0
end

function check_bit_block_only(fn_name)
    if not in_bit_block() then
        error(string.format("[DSL ERROR] %s() can only be called inside a bit block", fn_name), 3)
    end
end

function check_not_in_bit_block(fn_name)
    if in_bit_block() then
        error(string.format("[DSL ERROR] %s() cannot be called inside a bit block", fn_name), 3)
    end
end

--============================================================================
-- MODULE STATE
--============================================================================

local _module = nil

function dsl_error(msg)
    error("[DSL ERROR] " .. msg, 3)
end

function check_in_module(fn)
    if not _module then
        dsl_error(fn .. "() must be inside start_module()")
    end
end

function check_in_tree(fn)
    if not _module then
        dsl_error(fn .. "() must be inside start_module()")
    end
    if not _module.current_tree then
        dsl_error(fn .. "() must be inside start_tree()")
    end
end

--============================================================================
-- HASH TABLE MANAGEMENT
--============================================================================

function add_to_hash_table(tbl, name, collision_tbl, table_name)
    local h = hash_auto(name)
    
    if collision_tbl[h] and collision_tbl[h] ~= name then
        dsl_error(string.format(
            "HASH COLLISION in %s table: '%s' collides with '%s'",
            table_name, name, collision_tbl[h]
        ))
    end
    
    collision_tbl[h] = name
    
    for i, entry in ipairs(tbl) do
        if entry.name == name then
            return i - 1
        end
    end
    
    local idx = #tbl
    table.insert(tbl, { name = name, hash = h })
    return idx
end

function add_oneshot(name)
    return add_to_hash_table(
        _module.oneshot_funcs,
        name,
        _module.oneshot_collision,
        "oneshot"
    )
end

function add_main(name)
    return add_to_hash_table(
        _module.main_funcs,
        name,
        _module.main_collision,
        "main"
    )
end

function add_pred(name)
    return add_to_hash_table(
        _module.pred_funcs,
        name,
        _module.pred_collision,
        "pred"
    )
end

function add_string_hash(s)
    local h = hash_auto(s)
    _module.string_hashes[h] = s
    return h
end

--============================================================================
-- POOL / SLOT DEFINITIONS
--============================================================================

function defpool(pool_name, type_name)
    check_in_module("defpool")
    
    if _module.pools[pool_name] then
        dsl_error("Pool already defined: " .. pool_name)
    end
    
    local pool_id = _module.pool_counter
    _module.pool_counter = _module.pool_counter + 1
    
    _module.pools[pool_name] = {
        id = pool_id,
        type_name = type_name,
        slots = {},
        slot_counter = 0,
    }
    
    debug_print("defpool:", pool_name, "type=" .. type_name, "id=" .. pool_id)
end

function defslot(slot_name, pool_name)
    check_in_module("defslot")
    
    local pool = _module.pools[pool_name]
    if not pool then
        dsl_error("Unknown pool: " .. pool_name)
    end
    
    if _module.slots[slot_name] then
        dsl_error("Slot already defined: " .. slot_name)
    end
    
    local slot_index = pool.slot_counter
    pool.slot_counter = pool.slot_counter + 1
    
    table.insert(pool.slots, slot_name)
    _module.slots[slot_name] = {
        pool_name = pool_name,
        pool_id = pool.id,
        slot_index = slot_index,
    }
    
    debug_print("defslot:", slot_name, "pool=" .. pool_name, "index=" .. slot_index)
end

function resolve_slot(slot_name)
    local slot = _module.slots[slot_name]
    if not slot then
        dsl_error("Unknown slot: " .. slot_name)
    end
    return slot.pool_id, slot.slot_index
end

--============================================================================
-- RECORD / FIELD DEFINITIONS
--============================================================================

local FIELD_SIZES = {
    int8 = 1, uint8 = 1,
    int16 = 2, uint16 = 2,
    int32 = 4, uint32 = 4,
    int64 = 8, uint64 = 8,
    float = 4, double = 8,
    bool = 1,
}

local FIELD_C_NAMES = {
    int8 = "int8_t", uint8 = "uint8_t",
    int16 = "int16_t", uint16 = "uint16_t",
    int32 = "int32_t", uint32 = "uint32_t",
    int64 = "int64_t", uint64 = "uint64_t",
    float = "float", double = "double",
    bool = "bool",
}

local FIELD_ALIGNMENTS = {
    int8 = 1, uint8 = 1,
    int16 = 2, uint16 = 2,
    int32 = 4, uint32 = 4,
    int64 = 8, uint64 = 8,
    float = 4, double = 8,
    bool = 1,
}

function align_to(offset, alignment)
    return math.floor((offset + alignment - 1) / alignment) * alignment
end

function RECORD(name)
    check_in_module("RECORD")
    
    if _module.records[name] then
        dsl_error("Record already defined: " .. name)
    end
    
    debug_print("RECORD:", name)
    
    _module.current_record = name
    _module.records[name] = {
        name = name,
        fields = {},
        field_order = {},
        current_offset = 0,
        max_align = 1,
        total_size = 0,
    }
    table.insert(_module.record_order, name)
end

function FIELD(name, type_name, count)
    check_in_module("FIELD")
    
    if not _module.current_record then
        dsl_error("FIELD() must be inside RECORD()")
    end
    
    local record = _module.records[_module.current_record]
    count = count or 1
    
    local size, alignment, c_type, is_embedded, embedded_record
    
    if FIELD_SIZES[type_name] then
        size = FIELD_SIZES[type_name]
        alignment = FIELD_ALIGNMENTS[type_name]
        c_type = FIELD_C_NAMES[type_name]
        is_embedded = false
    elseif _module.records[type_name] then
        embedded_record = _module.records[type_name]
        size = embedded_record.total_size
        alignment = embedded_record.max_align
        c_type = type_name .. "_t"
        is_embedded = true
    else
        dsl_error("Unknown field type: " .. type_name)
    end
    
    local offset = align_to(record.current_offset, alignment)
    local total_size = size * count
    
    record.fields[name] = {
        name = name,
        type_name = type_name,
        offset = offset,
        size = size,
        count = count,
        total_size = total_size,
        c_type = c_type,
        is_embedded = is_embedded,
        embedded_record = embedded_record,
    }
    table.insert(record.field_order, name)
    
    record.current_offset = offset + total_size
    if alignment > record.max_align then
        record.max_align = alignment
    end
    
    if is_embedded then
        debug_print("  FIELD:", name, "type=" .. type_name .. " (embedded)", 
            "offset=" .. offset, "size=" .. total_size)
    else
        debug_print("  FIELD:", name, "type=" .. type_name, 
            "offset=" .. offset, "size=" .. total_size)
    end
end

function PTR_FIELD(name, record_type)
    check_in_module("PTR_FIELD")
    
    if not _module.current_record then
        dsl_error("PTR_FIELD() must be inside RECORD()")
    end
    
    local record = _module.records[_module.current_record]
    local size = _pointer_size
    local alignment = _pointer_size
    
    local offset = align_to(record.current_offset, alignment)
    
    record.fields[name] = {
        name = name,
        type_name = record_type,
        offset = offset,
        size = size,
        count = 1,
        total_size = size,
        c_type = record_type .. "_t*",
        is_pointer = true,
        target_record = record_type,
    }
    table.insert(record.field_order, name)
    
    record.current_offset = offset + size
    if alignment > record.max_align then
        record.max_align = alignment
    end
    
    table.insert(_module.ptr_field_refs, {
        record_name = _module.current_record,
        field_name = name,
        target_record = record_type,
    })
    
    debug_print("  PTR_FIELD:", name, "-> " .. record_type, 
        "offset=" .. offset, "size=" .. size)
end

function END_RECORD()
    check_in_module("END_RECORD")
    
    if not _module.current_record then
        dsl_error("END_RECORD() without matching RECORD()")
    end
    
    local record = _module.records[_module.current_record]
    record.total_size = align_to(record.current_offset, record.max_align)
    
    debug_print("END_RECORD:", _module.current_record, 
        "size=" .. record.total_size, "align=" .. record.max_align)
    
    _module.current_record = nil
end

--============================================================================
-- PARAMETER EMISSION
--============================================================================

function emit_param(param)
    table.insert(_module.params, param)
    return #_module.params
end

--============================================================================
-- RESULT PARAMETER (return code)
--============================================================================

function result(value)
    check_in_tree("result")
    check_not_in_bit_block("result")
    emit_param({
        type = PARAM_OPCODES.RESULT,
        index_to_pointer = 0,
        node_index = 0,
        value = value,
        value_type = "result",
    })
end

function int(value)
    check_in_tree("int")
    check_not_in_bit_block("int")
    emit_param({
        type = PARAM_OPCODES.INT,
        index_to_pointer = 0,
        node_index = 0,
        value = value,
        value_type = "int",
    })
end

function uint(value)
    check_in_tree("uint")
    -- NO bit block check - allowed for cfl_bit_entry
    emit_param({
        type = PARAM_OPCODES.UINT,
        index_to_pointer = 0,
        node_index = 0,
        value = value,
        value_type = "uint",
    })
end

function flt(value)
    check_in_tree("flt")
    check_not_in_bit_block("flt")
    emit_param({
        type = PARAM_OPCODES.FLOAT,
        index_to_pointer = 0,
        node_index = 0,
        value = value,
        value_type = "float",
    })
end

function str(value)
    check_in_tree("str")
    check_not_in_bit_block("str")
    local hash = add_string_hash(tostring(value))
    emit_param({
        type = PARAM_OPCODES.STR_HASH,
        index_to_pointer = 0,
        node_index = 0,
        value = hash,
        value_type = "hash",
        str_content = tostring(value),
    })
end

function slot_ref(slot_name)
    check_in_tree("slot_ref")
    check_not_in_bit_block("slot_ref")
    local pool_id, slot_index = resolve_slot(slot_name)
    emit_param({
        type = PARAM_OPCODES.SLOT,
        index_to_pointer = 0,
        node_index = 0,
        pool_id = pool_id,
        slot_index = slot_index,
        value_type = "slot",
        slot_name = slot_name,
    })
end

function field_ref(field_name)
    check_in_tree("field_ref")
    check_not_in_bit_block("field_ref")
    
    if not _module.tree_record then
        dsl_error("field_ref() requires use_record() first")
    end
    
    local record = _module.records[_module.tree_record]
    local field = record.fields[field_name]
    if not field then
        dsl_error("Unknown field '" .. field_name .. "' in record '" .. _module.tree_record .. "'")
    end
    
    emit_param({
        type = PARAM_OPCODES.FIELD,
        index_to_pointer = 0,
        node_index = 0,
        field_offset = field.offset,
        field_size = field.size,
        value_type = "field",
        field_name = field_name,
        record_name = _module.tree_record,
    })
end

function nested_field_ref(path)
    check_in_tree("nested_field_ref")
    check_not_in_bit_block("nested_field_ref")
    
    if not _module.tree_record then
        dsl_error("nested_field_ref() requires use_record() first")
    end
    
    local parts = {}
    for part in string.gmatch(path, "[^%.]+") do
        table.insert(parts, part)
    end
    
    if #parts < 2 then
        dsl_error("nested_field_ref() requires path with at least 2 parts: " .. path)
    end
    
    local record = _module.records[_module.tree_record]
    local cumulative_offset = 0
    local final_field = nil
    
    for i, part in ipairs(parts) do
        local field = record.fields[part]
        if not field then
            dsl_error("Unknown field '" .. part .. "' in record '" .. record.name .. "'")
        end
        
        cumulative_offset = cumulative_offset + field.offset
        
        if i < #parts then
            if not field.is_embedded then
                dsl_error("Field '" .. part .. "' is not an embedded record")
            end
            record = field.embedded_record
        else
            final_field = field
        end
    end
    
    emit_param({
        type = PARAM_OPCODES.FIELD,
        index_to_pointer = 0,
        node_index = 0,
        field_offset = cumulative_offset,
        field_size = final_field.size,
        value_type = "field",
        field_name = path,
        record_name = _module.tree_record,
    })
end

--============================================================================
-- LIST START/END
--============================================================================

function list_start(prefix)
    check_in_tree("list_start")
    check_not_in_bit_block("list_start")
    
    local name = gensym(prefix or "list")
    
    local open_idx = emit_param({
        type = PARAM_OPCODES.OPEN,
        index_to_pointer = 0,
        node_index = 0,
        value = 0,
        value_type = "brace",
    })
    
    table.insert(_module.brace_stack, { 
        type = "list", 
        name = name, 
        idx = open_idx,
        is_bit_block = false,
    })
    
    return name
end

function list_end(name)
    check_in_tree("list_end")
    
    if #_module.brace_stack == 0 then
        dsl_error("list_end() with no matching list_start")
    end
    
    local top = _module.brace_stack[#_module.brace_stack]
    if top.type ~= "list" then
        dsl_error(string.format("list_end('%s') but top of stack is %s('%s')",
            name, top.type, top.name))
    end
    if top.name ~= name then
        dsl_error(string.format("list_end('%s') does not match list_start('%s')",
            name, top.name))
    end
    
    table.remove(_module.brace_stack)
    
    local close_idx = emit_param({
        type = PARAM_OPCODES.CLOSE,
        index_to_pointer = 0,
        node_index = 0,
        value = 0,
        value_type = "brace",
    })
    
    local offset = close_idx - top.idx
    _module.params[top.idx].value = offset
    _module.params[close_idx].value = offset
end

--============================================================================
-- CALL START/END
--============================================================================

function start_call(call_type, prefix, func_name, survives_reset, is_bit_block)
    check_in_tree(call_type)
    
    if type(func_name) ~= "string" then
        dsl_error(call_type .. "() requires function name as first argument")
    end
    
    -- Enter bit block if this is one
    if is_bit_block then
        enter_bit_block()
    end
    
    local name = gensym(prefix or func_name)
    
    local open_idx = emit_param({
        type = PARAM_OPCODES.OPEN_CALL,
        index_to_pointer = 0,
        node_index = 0,
        value = 0,
        value_type = "brace",
    })
    
    local func_idx, type_byte
    local ptr_base = 0
    
    if call_type == "o_call" or call_type == "io_call" then
        func_idx = add_oneshot(func_name)
        type_byte = PARAM_OPCODES.ONESHOT
        if survives_reset then
            type_byte = bit.bor(type_byte, TYPE_FLAGS.SURVIVES_RESET)
        end
    elseif call_type == "m_call" then
        func_idx = add_main(func_name)
        type_byte = PARAM_OPCODES.MAIN
    elseif call_type == "pt_m_call" then
        func_idx = add_main(func_name)
        type_byte = bit.bor(PARAM_OPCODES.MAIN, TYPE_FLAGS.POINTER)
        ptr_base = _module.pointer_counter
    elseif call_type == "p_call" then
        func_idx = add_pred(func_name)
        type_byte = PARAM_OPCODES.PRED
    end
    
    local node_idx = _module.func_node_counter
    _module.func_node_counter = _module.func_node_counter + 1
    
    emit_param({
        type = type_byte,
        index_to_pointer = ptr_base,
        node_index = node_idx,
        value = func_idx,
        value_type = "func",
        func_name = func_name,
    })
    
    table.insert(_module.brace_stack, { 
        type = "call", 
        call_type = call_type,
        name = name, 
        idx = open_idx,
        func_name = func_name,
        is_pt_call = (call_type == "pt_m_call"),
        is_bit_block = is_bit_block or false,
        param_start = #_module.params + 1,
    })
    
    return name
end

function o_call(func_name, prefix)
    check_not_in_bit_block("o_call")
    return start_call("o_call", prefix, func_name, false, false)
end

function io_call(func_name, prefix)
    check_not_in_bit_block("io_call")
    return start_call("io_call", prefix, func_name, true, false)
end

function m_call(func_name, prefix)
    check_not_in_bit_block("m_call")
    if _module.pt_m_call_funcs[func_name] then
        dsl_error(string.format(
            "Function '%s' already registered as pt_m_call, cannot use m_call",
            func_name
        ))
    end
    _module.m_call_funcs[func_name] = true
    
    return start_call("m_call", prefix, func_name, false, false)
end

function pt_m_call(func_name, prefix)
    check_not_in_bit_block("pt_m_call")
    if _module.m_call_funcs[func_name] then
        dsl_error(string.format(
            "Function '%s' already registered as m_call, cannot use pt_m_call",
            func_name
        ))
    end
    _module.pt_m_call_funcs[func_name] = true
    
    return start_call("pt_m_call", prefix, func_name, false, false)
end

function p_call(func_name, prefix)
    check_not_in_bit_block("p_call")
    return start_call("p_call", prefix, func_name, false, false)
end

-- Only this one is allowed in bit blocks
function p_call_bit(func_name, prefix)
    return start_call("p_call", prefix, func_name, false, true)
end

function end_call(name)
    check_in_tree("end_call")
    
    if #_module.brace_stack == 0 then
        dsl_error("end_call() with no matching call")
    end
    
    local top = _module.brace_stack[#_module.brace_stack]
    if top.type ~= "call" then
        dsl_error(string.format("end_call('%s') but top of stack is %s('%s')",
            name, top.type, top.name))
    end
    if top.name ~= name then
        dsl_error(string.format("end_call('%s') does not match %s('%s')",
            name, top.call_type, top.name))
    end
    
    -- Exit bit block if this was one
    if top.is_bit_block then
        exit_bit_block()
    end
    
    if top.is_pt_call then
         _module.pointer_counter = _module.pointer_counter + 1  -- Always 1 slot now
        
    end
    
    table.remove(_module.brace_stack)
    
    local close_idx = emit_param({
        type = PARAM_OPCODES.CLOSE,
        index_to_pointer = 0,
        node_index = 0,
        value = 0,
        value_type = "brace",
    })
    
    local offset = close_idx - top.idx
    _module.params[top.idx].value = offset
    _module.params[close_idx].value = offset
end

--============================================================================
-- MODULE / TREE MANAGEMENT
--============================================================================

function start_module(name)
    if _module then
        dsl_error("start_module() while already in module '" .. _module.name .. "'")
    end
    
    debug_print("start_module:", name)
    
    _module = {
        name = name,
        trees = {},
        tree_order = {},
        records = {},
        record_order = {},
        pools = {},
        slots = {},
        pool_counter = 0,
        current_tree = nil,
        current_record = nil,
        params = {},
        func_node_counter = 0,
        pointer_counter = 0,
        brace_stack = {},
        oneshot_funcs = {},
        main_funcs = {},
        pred_funcs = {},
        oneshot_collision = {},
        main_collision = {},
        pred_collision = {},
        string_hashes = {},
        tree_record = nil,
        m_call_funcs = {},
        pt_m_call_funcs = {},
        ptr_field_refs = {},
    }
    
    return name
end

function use_record(record_name)
    check_in_tree("use_record")
    
    if not _module.records[record_name] then
        dsl_error("Unknown record: " .. record_name)
    end
    
    _module.tree_record = record_name
    debug_print("  use_record:", record_name)
end

function start_tree(name)
    if not name or name == "" then
        error("[DSL ERROR] start_tree() requires explicit name", 2)
    end
    
    if not _module then
        error("[DSL ERROR] start_tree() must be inside start_module()", 2)
    end
    
    if _module.trees[name] then
        error("[DSL ERROR] tree '" .. name .. "' already defined", 2)
    end
    
    debug_print("start_tree:", name)
    
    gensym_reset()
    _bit_block_depth = 0  -- Reset bit block depth
    
    _module.current_tree = name
    _module.params = {}
    _module.func_node_counter = 0
    _module.pointer_counter = 0
    _module.brace_stack = {}
    _module.tree_record = nil
    
    return name
end

function end_tree(name)
    check_in_tree("end_tree")
    
    if _module.current_tree ~= name then
        dsl_error("end_tree('" .. name .. "') does not match start_tree('" .. _module.current_tree .. "')")
    end
    
    if #_module.brace_stack > 0 then
        local unclosed = {}
        for _, item in ipairs(_module.brace_stack) do
            table.insert(unclosed, item.type .. "('" .. item.name .. "')")
        end
        dsl_error("unclosed: " .. table.concat(unclosed, ", "))
    end
    
    _module.trees[name] = {
        name = name,
        params = _module.params,
        func_node_count = _module.func_node_counter,
        pointer_count = _module.pointer_counter,
        record_name = _module.tree_record,
    }
    table.insert(_module.tree_order, name)
    
    debug_print("end_tree:", name, 
        "params=" .. #_module.params, 
        "func_nodes=" .. _module.func_node_counter)
    
    _module.current_tree = nil
    _module.params = {}
    _module.tree_record = nil
    
    return name
end

function end_module(name)
    check_in_module("end_module")
    
    if _module.name ~= name then
        dsl_error("end_module('" .. name .. "') does not match start_module('" .. _module.name .. "')")
    end
    
    if _module.current_tree then
        dsl_error("end_module() while still in tree '" .. _module.current_tree .. "'")
    end
    
    if _module.current_record then
        dsl_error("end_module() while still in record '" .. _module.current_record .. "' - missing END_RECORD()?")
    end
    
    -- Validate pointer field references
    for _, ref in ipairs(_module.ptr_field_refs) do
        if not _module.records[ref.target_record] then
            dsl_error(string.format(
                "PTR_FIELD '%s' in record '%s' references undefined record '%s'",
                ref.field_name, ref.record_name, ref.target_record
            ))
        end
    end
    
    -- Validate all trees that use records reference defined records
    for _, tree_name in ipairs(_module.tree_order) do
        local tree = _module.trees[tree_name]
        if tree.record_name and not _module.records[tree.record_name] then
            dsl_error(string.format(
                "Tree '%s' uses undefined record '%s' - define RECORD('%s') before use_record()",
                tree_name, tree.record_name, tree.record_name
            ))
        end
    end
    
    -- Warn if no records defined but trees exist (common oversight)
    if #_module.tree_order > 0 and #_module.record_order == 0 then
        debug_print("WARNING: Module has trees but no records defined")
    end
    
    debug_print("end_module:", name, 
        "trees=" .. #_module.tree_order, 
        "records=" .. #_module.record_order)
    
    local result = _module
    _module = nil
    
    return result
end

--============================================================================
-- MODULE GENERATOR CLASS
--============================================================================

ModuleGenerator = {}
ModuleGenerator.__index = ModuleGenerator

function ModuleGenerator.new(module_data)
    local self = setmetatable({}, ModuleGenerator)
    self.module = module_data
    return self
end

--============================================================================
-- PARAM TO C
--============================================================================

function ModuleGenerator:param_to_c(param)
    local type_hex = string.format("0x%02X", param.type)
    local struct, comment

    if param.value_type == "int" then
        struct = string.format("{ .type = %s, .int_val = %d }", type_hex, param.value)
        comment = nil

    elseif param.value_type == "uint" then
        struct = string.format("{ .type = %s, .uint_val = %uU }", type_hex, param.value)
        comment = nil

    elseif param.value_type == "float" then
        struct = string.format("{ .type = %s, .float_val = %ff }", type_hex, param.value)
        comment = nil

    elseif param.value_type == "hash" then
        struct = string.format("{ .type = %s, .str_hash = %s }", type_hex, format_hash(param.value))
        comment = param.str_content and ("\"" .. param.str_content .. "\"") or nil

    elseif param.value_type == "slot" then
        struct = string.format("{ .type = %s, .pool_id = %d, .slot_index = %d }",
            type_hex, param.pool_id, param.slot_index)
        comment = param.slot_name

    elseif param.value_type == "field" then
        struct = string.format("{ .type = %s, .field_offset = %d, .field_size = %d }",
            type_hex, param.field_offset, param.field_size)
        comment = param.field_name

    elseif param.value_type == "brace" then
        struct = string.format("{ .type = %s, .brace_idx = %d }", type_hex, param.value)
        comment = nil

    elseif param.value_type == "func" then
        struct = string.format("{ .type = %s, .index_to_pointer = %d, .node_index = %d, .func_index = %d }",
            type_hex, param.index_to_pointer, param.node_index, param.value)
        comment = param.func_name

    elseif param.value_type == "result" then
        local names = {
            [0] = "SE_CONTINUE",
            [1] = "SE_HALT",
            [2] = "SE_TERMINATE",
            [3] = "SE_RESET",
            [4] = "SE_DISABLE",
            [5] = "SE_FUNCTION_TERMINATE",
            [6] = "SE_SKIP_CONTINUE",
            [7] = "SE_FUNCTION_HALT",
            [8] = "SE_FUNCTION_RESET",
        }
        struct = string.format("{ .type = %s, .int_val = %d }", type_hex, param.value)
        comment = names[param.value] or "?"

    else
        struct = string.format("{ .type = %s, .uint_val = 0 }", type_hex)
        comment = "unknown"
    end

    return struct, comment
end

--============================================================================
-- C HEADER GENERATION
--============================================================================

function ModuleGenerator:to_c_header(base_name)
    local lines = {}
    local mod = self.module
    local guard = string.upper(base_name) .. "_H"
    
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "// " .. base_name .. ".h")
    table.insert(lines, "// Generated by ChainTree S-Expression DSL v3.0")
    table.insert(lines, "// DO NOT EDIT - regenerate from DSL")
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "")
    table.insert(lines, "#ifndef " .. guard)
    table.insert(lines, "#define " .. guard)
    table.insert(lines, "")
    table.insert(lines, "#include <stdint.h>")
    table.insert(lines, "#include <stdbool.h>")
    table.insert(lines, "#include \"s_engine_types.h\"")
    table.insert(lines, "")
    
    -- Forward declarations for all records
    if #mod.record_order > 0 then
        table.insert(lines, "// ============================================================================")
        table.insert(lines, "// FORWARD DECLARATIONS")
        table.insert(lines, "// ============================================================================")
        table.insert(lines, "")
        for _, name in ipairs(mod.record_order) do
            table.insert(lines, "typedef struct " .. name .. "_s " .. name .. "_t;")
        end
        table.insert(lines, "")
    end
    
    -- Record structures
    if #mod.record_order > 0 then
        table.insert(lines, "// ============================================================================")
        table.insert(lines, "// RECORD STRUCTURES")
        table.insert(lines, "// ============================================================================")
        table.insert(lines, "")
        
        for _, name in ipairs(mod.record_order) do
            local record = mod.records[name]
            table.insert(lines, "struct " .. name .. "_s {")
            
            for _, field_name in ipairs(record.field_order) do
                local field = record.fields[field_name]
                local comment = ""
                
                if field.is_pointer then
                    comment = string.format("  // offset=%d (ptr to %s - USER MANAGES MEMORY)", 
                        field.offset, field.target_record)
                elseif field.is_embedded then
                    comment = string.format("  // offset=%d, size=%d (embedded)", 
                        field.offset, field.total_size)
                else
                    comment = string.format("  // offset=%d, size=%d", 
                        field.offset, field.total_size)
                end
                
                if field.count > 1 then
                    table.insert(lines, string.format("    %s %s[%d];%s",
                        field.c_type, field_name, field.count, comment))
                else
                    table.insert(lines, string.format("    %s %s;%s",
                        field.c_type, field_name, comment))
                end
            end
            
            table.insert(lines, string.format("};  // total_size=%d, align=%d",
                record.total_size, record.max_align))
            table.insert(lines, "")
        end
    end
    
    -- Function hash tables
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "// FUNCTION HASH TABLES")
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "")
    
    local hash_type = get_hash_type()
    
    -- Oneshot hashes
    if #mod.oneshot_funcs > 0 then
        table.insert(lines, "static const " .. hash_type .. " " .. base_name .. "_oneshot_hashes[] = {")
        for i, entry in ipairs(mod.oneshot_funcs) do
            local comma = (i < #mod.oneshot_funcs) and "," or ""
            table.insert(lines, string.format("    %s%s  // %s", 
                format_hash(entry.hash), comma, entry.name))
        end
        table.insert(lines, "};")
        table.insert(lines, "")
    end
    
    -- Main hashes
    if #mod.main_funcs > 0 then
        table.insert(lines, "static const " .. hash_type .. " " .. base_name .. "_main_hashes[] = {")
        for i, entry in ipairs(mod.main_funcs) do
            local comma = (i < #mod.main_funcs) and "," or ""
            table.insert(lines, string.format("    %s%s  // %s", 
                format_hash(entry.hash), comma, entry.name))
        end
        table.insert(lines, "};")
        table.insert(lines, "")
    end
    
    -- Pred hashes
    if #mod.pred_funcs > 0 then
        table.insert(lines, "static const " .. hash_type .. " " .. base_name .. "_pred_hashes[] = {")
        for i, entry in ipairs(mod.pred_funcs) do
            local comma = (i < #mod.pred_funcs) and "," or ""
            table.insert(lines, string.format("    %s%s  // %s", 
                format_hash(entry.hash), comma, entry.name))
        end
        table.insert(lines, "};")
        table.insert(lines, "")
    end
    
    -- Record descriptors (field arrays + record array)
    if #mod.record_order > 0 then
        table.insert(lines, "// ============================================================================")
        table.insert(lines, "// RECORD DESCRIPTORS")
        table.insert(lines, "// ============================================================================")
        table.insert(lines, "")
        
        -- Generate field descriptor array for each record
        for _, record_name in ipairs(mod.record_order) do
            local record = mod.records[record_name]
            local fields_array_name = base_name .. "_" .. record_name .. "_fields"
            
            if #record.field_order > 0 then
                table.insert(lines, "static const s_expr_field_desc_t " .. fields_array_name .. "[] = {")
                for i, field_name in ipairs(record.field_order) do
                    local field = record.fields[field_name]
                    local comma = (i < #record.field_order) and "," or ""
                    local field_hash = hash_auto(field_name)
                    table.insert(lines, string.format(
                        "    { .name_hash = %s, .offset = %d, .size = %d }%s  // %s",
                        format_hash(field_hash), field.offset, field.size, comma, field_name))
                end
                table.insert(lines, "};")
                table.insert(lines, "")
            end
        end
        
        -- Generate record descriptor array
        table.insert(lines, "static const s_expr_record_desc_t " .. base_name .. "_records[] = {")
        for i, record_name in ipairs(mod.record_order) do
            local record = mod.records[record_name]
            local comma = (i < #mod.record_order) and "," or ""
            local record_hash = hash_auto(record_name)
            local fields_ptr = (#record.field_order > 0) and 
                (base_name .. "_" .. record_name .. "_fields") or "NULL"
            
            table.insert(lines, "    {")
            table.insert(lines, string.format("        .name_hash = %s,  // \"%s\"", 
                format_hash(record_hash), record_name))
            table.insert(lines, "        .total_size = " .. record.total_size .. ",")
            table.insert(lines, "        .field_count = " .. #record.field_order .. ",")
            table.insert(lines, "        .fields = " .. fields_ptr)
            table.insert(lines, "    }" .. comma)
        end
        table.insert(lines, "};")
        table.insert(lines, "")
    end
    
    -- Tree parameters and definitions
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "// TREE DEFINITIONS")
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "")
    
    for _, tree_name in ipairs(mod.tree_order) do
        local tree = mod.trees[tree_name]
        local param_array_name = base_name .. "_" .. tree_name .. "_params"
        
        -- Parameter array
        table.insert(lines, "static const s_expr_param_t " .. param_array_name .. "[] = {")
        for i, param in ipairs(tree.params) do
            local struct, comment = self:param_to_c(param)
            local comma = (i < #tree.params) and "," or ""
            local line = struct .. comma
            if comment then
                line = line .. "  // " .. comment
            end
            table.insert(lines, "    " .. line)
        end
        table.insert(lines, "};")
        table.insert(lines, "")
    end
    
    -- Tree definition structs
    for _, tree_name in ipairs(mod.tree_order) do
        local tree = mod.trees[tree_name]
        local param_array_name = base_name .. "_" .. tree_name .. "_params"
        local def_name = base_name .. "_" .. tree_name .. "_def"
        local tree_hash = hash_auto(tree_name)
        local record_hash = tree.record_name and hash_auto(tree.record_name) or 0
        
        table.insert(lines, "static const s_expr_tree_def_t " .. def_name .. " = {")
        table.insert(lines, string.format("    .name_hash = %s,  // \"%s\"", 
            format_hash(tree_hash), tree_name))
        table.insert(lines, string.format("    .record_hash = %s,", format_hash(record_hash)))
        table.insert(lines, "    .params = " .. param_array_name .. ",")
        table.insert(lines, "    .param_count = " .. #tree.params .. ",")
        table.insert(lines, "    .func_node_count = " .. tree.func_node_count .. ",")
        table.insert(lines, "    .pointer_count = " .. tree.pointer_count .. ",")
        table.insert(lines, "};")
        table.insert(lines, "")
    end
    
    -- Module definition
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "// MODULE DEFINITION")
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "")
    
    -- Tree definition array (values, not pointers)
    table.insert(lines, "static const s_expr_tree_def_t " .. base_name .. "_trees[] = {")
    for i, tree_name in ipairs(mod.tree_order) do
        local comma = (i < #mod.tree_order) and "," or ""
        table.insert(lines, "    " .. base_name .. "_" .. tree_name .. "_def" .. comma)
    end
    table.insert(lines, "};")
    table.insert(lines, "")
    
    -- Module struct
    local module_hash = hash_auto(mod.name)
    local is_64bit = (_pointer_size == 8) and "true" or "false"
    
    table.insert(lines, "static const s_expr_module_def_t " .. base_name .. "_module = {")
    table.insert(lines, string.format("    .name_hash = %s,  // \"%s\"", 
        format_hash(module_hash), mod.name))
    table.insert(lines, "    .trees = " .. base_name .. "_trees,")
    table.insert(lines, "    .tree_count = " .. #mod.tree_order .. ",")
    table.insert(lines, "    .is_64bit = " .. is_64bit .. ",")
    
    if #mod.oneshot_funcs > 0 then
        table.insert(lines, "    .oneshot_hashes = " .. base_name .. "_oneshot_hashes,")
        table.insert(lines, "    .oneshot_count = " .. #mod.oneshot_funcs .. ",")
    else
        table.insert(lines, "    .oneshot_hashes = NULL,")
        table.insert(lines, "    .oneshot_count = 0,")
    end
    
    if #mod.main_funcs > 0 then
        table.insert(lines, "    .main_hashes = " .. base_name .. "_main_hashes,")
        table.insert(lines, "    .main_count = " .. #mod.main_funcs .. ",")
    else
        table.insert(lines, "    .main_hashes = NULL,")
        table.insert(lines, "    .main_count = 0,")
    end
    
    if #mod.pred_funcs > 0 then
        table.insert(lines, "    .pred_hashes = " .. base_name .. "_pred_hashes,")
        table.insert(lines, "    .pred_count = " .. #mod.pred_funcs .. ",")
    else
        table.insert(lines, "    .pred_hashes = NULL,")
        table.insert(lines, "    .pred_count = 0,")
    end
    
    -- Max counts for pre-allocation
    local max_func_node = 0
    local max_pointer = 0
    local max_param = 0
    for _, tree_name in ipairs(mod.tree_order) do
        local tree = mod.trees[tree_name]
        if tree.func_node_count > max_func_node then
            max_func_node = tree.func_node_count
        end
        if tree.pointer_count > max_pointer then
            max_pointer = tree.pointer_count
        end
        if #tree.params > max_param then
            max_param = #tree.params
        end
    end
    
    table.insert(lines, "    .max_func_node_count = " .. max_func_node .. ",")
    table.insert(lines, "    .max_pointer_count = " .. max_pointer .. ",")
    table.insert(lines, "    .max_param_count = " .. max_param .. ",")
    
    -- Record descriptors (if any)
    if #mod.record_order > 0 then
        table.insert(lines, "    .records = " .. base_name .. "_records,")
        table.insert(lines, "    .record_count = " .. #mod.record_order .. ",")
    else
        table.insert(lines, "    .records = NULL,")
        table.insert(lines, "    .record_count = 0,")
    end
    
    table.insert(lines, "};")
    table.insert(lines, "")
    
    table.insert(lines, "#endif // " .. guard)
    
    return table.concat(lines, "\n")
end

--============================================================================
-- USER FUNCTION HEADER GENERATION
--============================================================================

function ModuleGenerator:to_c_user_header(base_name)
    local lines = {}
    local mod = self.module
    local guard = string.upper(base_name) .. "_USER_FUNCTIONS_H"
    
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "// " .. base_name .. "_user_functions.h")
    table.insert(lines, "// Generated by ChainTree S-Expression DSL v3.0")
    table.insert(lines, "// DO NOT EDIT - regenerate from DSL")
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "")
    table.insert(lines, "#ifndef " .. guard)
    table.insert(lines, "#define " .. guard)
    table.insert(lines, "")
    table.insert(lines, "#include <stdint.h>")
    table.insert(lines, "#include <stdbool.h>")
    table.insert(lines, "#include \"s_engine_types.h\"")
    table.insert(lines, "#include \"cfl_runtime.h\"")
    table.insert(lines, "")
    
    -- Collect user functions (non-CFL_ prefix)
    local user_oneshot = {}
    local user_main = {}
    local user_pred = {}
    
    for _, entry in ipairs(mod.oneshot_funcs) do
        if not entry.name:match("^CFL_") then
            table.insert(user_oneshot, entry)
        end
    end
    
    for _, entry in ipairs(mod.main_funcs) do
        if not entry.name:match("^CFL_") then
            table.insert(user_main, entry)
        end
    end
    
    for _, entry in ipairs(mod.pred_funcs) do
        if not entry.name:match("^CFL_") then
            table.insert(user_pred, entry)
        end
    end
    
    -- Oneshot prototypes
    if #user_oneshot > 0 then
        table.insert(lines, "// ============================================================================")
        table.insert(lines, "// USER ONESHOT FUNCTION PROTOTYPES")
        table.insert(lines, "// ============================================================================")
        table.insert(lines, "")
        
        for _, entry in ipairs(user_oneshot) do
            local func_name = string.lower(entry.name) .. "_oneshot"
            table.insert(lines, string.format("// DSL: %s  hash: %s", entry.name, format_hash(entry.hash)))
            table.insert(lines, "void " .. func_name .. "(")
            table.insert(lines, "    s_expr_tree_instance_t* inst,")
            table.insert(lines, "    const s_expr_param_t* params,")
            table.insert(lines, "    uint16_t param_count,")
            table.insert(lines, "    s_expr_event_type_t event_type,")
            table.insert(lines, "    uint16_t event_id,")
            table.insert(lines, "    void* event_data);")
            table.insert(lines, "")
        end
    end
    
    -- Main prototypes
    if #user_main > 0 then
        table.insert(lines, "// ============================================================================")
        table.insert(lines, "// USER MAIN FUNCTION PROTOTYPES")
        table.insert(lines, "// ============================================================================")
        table.insert(lines, "")
        
        for _, entry in ipairs(user_main) do
            local func_name = string.lower(entry.name) .. "_main"
            table.insert(lines, string.format("// DSL: %s  hash: %s", entry.name, format_hash(entry.hash)))
            table.insert(lines, "s_expr_result_t " .. func_name .. "(")
            table.insert(lines, "    s_expr_tree_instance_t* inst,")
            table.insert(lines, "    const s_expr_param_t* params,")
            table.insert(lines, "    uint16_t param_count,")
            table.insert(lines, "    s_expr_event_type_t event_type,")
            table.insert(lines, "    uint16_t event_id,")
            table.insert(lines, "    void* event_data);")
            table.insert(lines, "")
        end
    end
    
    -- Pred prototypes
    if #user_pred > 0 then
        table.insert(lines, "// ============================================================================")
        table.insert(lines, "// USER PREDICATE FUNCTION PROTOTYPES")
        table.insert(lines, "// ============================================================================")
        table.insert(lines, "")
        
        for _, entry in ipairs(user_pred) do
            local func_name = string.lower(entry.name) .. "_pred"
            table.insert(lines, string.format("// DSL: %s  hash: %s", entry.name, format_hash(entry.hash)))
            table.insert(lines, "bool " .. func_name .. "(")
            table.insert(lines, "    s_expr_tree_instance_t* inst,")
            table.insert(lines, "    const s_expr_param_t* params,")
            table.insert(lines, "    uint16_t param_count,")
            table.insert(lines, "    s_expr_event_type_t event_type,")
            table.insert(lines, "    uint16_t event_id,")
            table.insert(lines, "    void* event_data);")
            table.insert(lines, "")
        end
    end
    
    -- Load function declaration
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "// LOAD FUNCTION")
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "")
    table.insert(lines, "void load_user_s_functions(cfl_runtime_handle_t* handle);")
    table.insert(lines, "")
    
    table.insert(lines, "#endif // " .. guard)
    
    return table.concat(lines, "\n")
end

--============================================================================
-- USER REGISTRATION C FILE GENERATION
--============================================================================
function ModuleGenerator:to_c_user_registration(base_name)
    local lines = {}
    local mod = self.module
    
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "// " .. base_name .. "_user_registration.c")
    table.insert(lines, "// Generated by ChainTree S-Expression DSL v3.0")
    table.insert(lines, "// DO NOT EDIT - regenerate from DSL")
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "")
    table.insert(lines, "#include \"" .. base_name .. "_user_functions.h\"")
    table.insert(lines, "#include \"s_engine_module.h\"")
    table.insert(lines, "#include <stdio.h>")
    table.insert(lines, "")
    
    -- Collect user functions
    local user_oneshot = {}
    local user_main = {}
    local user_pred = {}
    
    for _, entry in ipairs(mod.oneshot_funcs) do
        if not entry.name:match("^CFL_") then
            table.insert(user_oneshot, entry)
        end
    end
    
    for _, entry in ipairs(mod.main_funcs) do
        if not entry.name:match("^CFL_") then
            table.insert(user_main, entry)
        end
    end
    
    for _, entry in ipairs(mod.pred_funcs) do
        if not entry.name:match("^CFL_") then
            table.insert(user_pred, entry)
        end
    end
    
    -- Check if any user functions exist
    local has_user_funcs = #user_oneshot > 0 or #user_main > 0 or #user_pred > 0
    
    if not has_user_funcs then
        -- Generate minimal stub file
        table.insert(lines, "// No user functions defined - stub implementation")
        table.insert(lines, "")
        table.insert(lines, "void load_user_s_functions(cfl_runtime_handle_t* handle) {")
        table.insert(lines, "    (void)handle;  // No user functions to register")
        table.insert(lines, "}")
        table.insert(lines, "")
        return table.concat(lines, "\n")
    end
    
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "// REGISTRATION TABLES")
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "")
    table.insert(lines, "#define ARRAY_COUNT(arr) (sizeof(arr) / sizeof((arr)[0]))")
    table.insert(lines, "")
    
    -- Oneshot entries (only if any exist)
    if #user_oneshot > 0 then
        table.insert(lines, "static const s_expr_fn_entry_named_t user_oneshot_entries_named[] = {")
        for _, entry in ipairs(user_oneshot) do
            local func_name = string.lower(entry.name) .. "_oneshot"
            table.insert(lines, string.format('    { "%s", (void*)%s },', entry.name, func_name))
        end
        table.insert(lines, "};")
        table.insert(lines, "static s_expr_fn_entry_t user_oneshot_entries[ARRAY_COUNT(user_oneshot_entries_named)];")
        table.insert(lines, "")
    end
    
    -- Main entries (only if any exist)
    if #user_main > 0 then
        table.insert(lines, "static const s_expr_fn_entry_named_t user_main_entries_named[] = {")
        for _, entry in ipairs(user_main) do
            local func_name = string.lower(entry.name) .. "_main"
            table.insert(lines, string.format('    { "%s", (void*)%s },', entry.name, func_name))
        end
        table.insert(lines, "};")
        table.insert(lines, "static s_expr_fn_entry_t user_main_entries[ARRAY_COUNT(user_main_entries_named)];")
        table.insert(lines, "")
    end
    
    -- Pred entries (only if any exist)
    if #user_pred > 0 then
        table.insert(lines, "static const s_expr_fn_entry_named_t user_pred_entries_named[] = {")
        for _, entry in ipairs(user_pred) do
            local func_name = string.lower(entry.name) .. "_pred"
            table.insert(lines, string.format('    { "%s", (void*)%s },', entry.name, func_name))
        end
        table.insert(lines, "};")
        table.insert(lines, "static s_expr_fn_entry_t user_pred_entries[ARRAY_COUNT(user_pred_entries_named)];")
        table.insert(lines, "")
    end
    
    -- Only declare tables that will be used
    if #user_oneshot > 0 then
        table.insert(lines, "static s_expr_fn_table_t user_oneshot_table;")
    end
    if #user_main > 0 then
        table.insert(lines, "static s_expr_fn_table_t user_main_table;")
    end
    if #user_pred > 0 then
        table.insert(lines, "static s_expr_fn_table_t user_pred_table;")
    end
    table.insert(lines, "")
    
    -- Init function
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "// INITIALIZATION")
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "")
    table.insert(lines, "static void init_user_function_tables(void) {")
    
    if #user_oneshot > 0 then
        table.insert(lines, "    s_expr_build_fn_table(")
        table.insert(lines, "        user_oneshot_entries_named,")
        table.insert(lines, "        user_oneshot_entries,")
        table.insert(lines, "        ARRAY_COUNT(user_oneshot_entries_named)")
        table.insert(lines, "    );")
        table.insert(lines, "    user_oneshot_table.entries = user_oneshot_entries;")
        table.insert(lines, "    user_oneshot_table.count = ARRAY_COUNT(user_oneshot_entries);")
        table.insert(lines, "")
    end
    
    if #user_main > 0 then
        table.insert(lines, "    s_expr_build_fn_table(")
        table.insert(lines, "        user_main_entries_named,")
        table.insert(lines, "        user_main_entries,")
        table.insert(lines, "        ARRAY_COUNT(user_main_entries_named)")
        table.insert(lines, "    );")
        table.insert(lines, "    user_main_table.entries = user_main_entries;")
        table.insert(lines, "    user_main_table.count = ARRAY_COUNT(user_main_entries);")
        table.insert(lines, "")
    end
    
    if #user_pred > 0 then
        table.insert(lines, "    s_expr_build_fn_table(")
        table.insert(lines, "        user_pred_entries_named,")
        table.insert(lines, "        user_pred_entries,")
        table.insert(lines, "        ARRAY_COUNT(user_pred_entries_named)")
        table.insert(lines, "    );")
        table.insert(lines, "    user_pred_table.entries = user_pred_entries;")
        table.insert(lines, "    user_pred_table.count = ARRAY_COUNT(user_pred_entries);")
        table.insert(lines, "")
    end
    
    table.insert(lines, "}")
    table.insert(lines, "")
    
    -- Load function
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "// LOAD FUNCTION")
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "")
    table.insert(lines, "void load_user_s_functions(cfl_runtime_handle_t* handle) {")
    table.insert(lines, "    if (!handle || !handle->s_expr_modules) {")
    table.insert(lines, '        printf("ERROR: load_user_s_functions: invalid handle\\n");')
    table.insert(lines, "        return;")
    table.insert(lines, "    }")
    table.insert(lines, "    if (handle->s_expr_module_count <= 0) {")
    table.insert(lines, '        printf("ERROR: load_user_s_functions: no modules\\n");')
    table.insert(lines, "        return;")
    table.insert(lines, "    }")
    table.insert(lines, "")
    table.insert(lines, "    static bool initialized = false;")
    table.insert(lines, "    if (!initialized) {")
    table.insert(lines, "        init_user_function_tables();")
    table.insert(lines, "        initialized = true;")
    table.insert(lines, "    }")
    table.insert(lines, "")
    table.insert(lines, "    s_expr_module_t** modules = (s_expr_module_t**)handle->s_expr_modules;")
    table.insert(lines, "    for (int i = 0; i < handle->s_expr_module_count; i++) {")
    table.insert(lines, "        if (!modules[i]) continue;")
    
    if #user_oneshot > 0 then
        table.insert(lines, "        s_expr_module_register_oneshot(modules[i], &user_oneshot_table);")
    end
    if #user_main > 0 then
        table.insert(lines, "        s_expr_module_register_main(modules[i], &user_main_table);")
    end
    if #user_pred > 0 then
        table.insert(lines, "        s_expr_module_register_pred(modules[i], &user_pred_table);")
    end
    
    table.insert(lines, "    }")
    table.insert(lines, "}")
    table.insert(lines, "")
    
    return table.concat(lines, "\n")
end
--============================================================================
-- DUMP FUNCTION
--============================================================================

function ModuleGenerator:dump()
    local mod = self.module
    local lines = {}
    
    table.insert(lines, "=== MODULE: " .. mod.name .. " ===")
    table.insert(lines, "")
    
    -- Records
    if #mod.record_order > 0 then
        table.insert(lines, "RECORDS:")
        for _, name in ipairs(mod.record_order) do
            local record = mod.records[name]
            table.insert(lines, "  " .. name .. " (size=" .. record.total_size .. ", align=" .. record.max_align .. ")")
            for _, field_name in ipairs(record.field_order) do
                local field = record.fields[field_name]
                local type_info = field.type_name
                if field.is_pointer then
                    type_info = type_info .. "*"
                elseif field.is_embedded then
                    type_info = type_info .. " (embedded)"
                end
                if field.count > 1 then
                    type_info = type_info .. "[" .. field.count .. "]"
                end
                table.insert(lines, string.format("    %s: %s (offset=%d, size=%d)",
                    field_name, type_info, field.offset, field.total_size))
            end
        end
        table.insert(lines, "")
    end
    
    -- Functions
    table.insert(lines, "ONESHOT FUNCTIONS:")
    for _, entry in ipairs(mod.oneshot_funcs) do
        table.insert(lines, string.format("  %s (%s)", entry.name, format_hash(entry.hash)))
    end
    table.insert(lines, "")
    
    table.insert(lines, "MAIN FUNCTIONS:")
    for _, entry in ipairs(mod.main_funcs) do
        table.insert(lines, string.format("  %s (%s)", entry.name, format_hash(entry.hash)))
    end
    table.insert(lines, "")
    
    table.insert(lines, "PRED FUNCTIONS:")
    for _, entry in ipairs(mod.pred_funcs) do
        table.insert(lines, string.format("  %s (%s)", entry.name, format_hash(entry.hash)))
    end
    table.insert(lines, "")
    
    -- Trees
    table.insert(lines, "TREES:")
    for _, tree_name in ipairs(mod.tree_order) do
        local tree = mod.trees[tree_name]
        table.insert(lines, "  " .. tree_name .. ":")
        table.insert(lines, "    params=" .. #tree.params)
        table.insert(lines, "    func_nodes=" .. tree.func_node_count)
        table.insert(lines, "    pointers=" .. tree.pointer_count)
        if tree.record_name then
            table.insert(lines, "    record=" .. tree.record_name)
        end
    end
    
    return table.concat(lines, "\n")
end

--============================================================================
-- EXPORT
--============================================================================

return {
    ModuleGenerator = ModuleGenerator,
}