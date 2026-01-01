--============================================================================
-- CHAINTREE S-EXPRESSION DSL
-- Version 3.0 - Flat parameter model, hash-based function tables
--               Stack-based list_start/list_end, direct parameter emission
--               Embedded records, pointer fields, debug support
--============================================================================

local ffi = require("ffi")
local bit = require("bit")

local bxor, band = bit.bxor, bit.band
local tobit = bit.tobit
local lshift, rshift = bit.lshift, bit.rshift

local lua_debug = debug

--============================================================================
-- FNV-1a 32-BIT HASH
--============================================================================

local FNV1A_32_INIT  = 0x811c9dc5
local FNV1A_32_PRIME = 0x01000193

local function mul32(a, b)
    local a_lo = band(a, 0xFFFF)
    local a_hi = band(rshift(a, 16), 0xFFFF)
    local b_lo = band(b, 0xFFFF)
    local b_hi = band(rshift(b, 16), 0xFFFF)
    
    local lo = a_lo * b_lo
    local mid = a_hi * b_lo + a_lo * b_hi
    
    return tobit(lo + lshift(mid, 16))
end

local function fnv1a_32(str)
    local hash = FNV1A_32_INIT
    
    for i = 1, #str do
        hash = bxor(hash, str:byte(i))
        hash = mul32(hash, FNV1A_32_PRIME)
    end
    
    if hash < 0 then
        hash = hash + 0x100000000
    end
    return hash
end

--============================================================================
-- GENSYM - Unique symbol generator
--============================================================================

local _gensym_counter = 0

function gensym(prefix)
    _gensym_counter = _gensym_counter + 1
    return (prefix or "g") .. "_" .. _gensym_counter
end

function gensym_reset()
    _gensym_counter = 0
end

--============================================================================
-- CONSTANTS
--============================================================================

-- Parameter type opcodes (bits 3:0)
local PARAM_OPCODES = {
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
}

-- Type flags
local TYPE_FLAGS = {
    SURVIVES_RESET = 0x10,
    POINTER        = 0x80,
}

-- Export control codes as globals
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
    if enabled == nil then enabled = true end
    _debug_enabled = enabled
end

function is_debug()
    return _debug_enabled
end

local function debug_print(...)
    if _debug_enabled then
        print("[DSL DEBUG]", ...)
    end
end

--============================================================================
-- DSL STATE
--============================================================================

local function new_hash_table()
    return {
        names = {},
        hashes = {},
        hash_to_idx = {},
        name_to_idx = {},
    }
end

local function new_string_hashes()
    return {
        hash_to_str = {},
    }
end

local function new_pools()
    return {
        pools = {},
        slots = {},
        pool_id = 0,
    }
end

local _module = nil

local function init_module(name, opts)
    opts = opts or {}
    _module = {
        name = name,
        trees = {},
        tree_order = {},
        oneshot_table = new_hash_table(),
        main_table = new_hash_table(),
        pred_table = new_hash_table(),
        string_hashes = new_string_hashes(),
        pools = new_pools(),
        records = {},
        record_order = {},
        is_64bit = opts.is_64bit or false,
        
        current_tree = nil,
        params = nil,
        func_node_counter = 0,
        pointer_counter = 0,
        tree_record = nil,
        
        brace_stack = {},
        
        pt_m_call_funcs = {},
        m_call_funcs = {},
    }
end

--============================================================================
-- ERROR HANDLING
--============================================================================

local function get_line()
    local info = lua_debug.getinfo(3, "l")
    if info then return info.currentline end
    return 0
end

local function dsl_error(msg)
    local line = get_line()
    error(string.format("[DSL ERROR] line %d: %s", line, msg), 3)
end

local function check_in_tree(fn_name)
    if not _module or not _module.current_tree then
        dsl_error(fn_name .. "() must be inside start_tree()")
    end
end

--============================================================================
-- HASH TABLE MANAGEMENT
--============================================================================

local function add_to_hash_table(tbl, name, table_name)
    if tbl.name_to_idx[name] then
        return tbl.name_to_idx[name]
    end
    
    local hash = fnv1a_32(name)
    
    if tbl.hash_to_idx[hash] then
        local existing_idx = tbl.hash_to_idx[hash]
        local existing_name = tbl.names[existing_idx + 1]
        if existing_name ~= name then
            dsl_error(string.format(
                "HASH COLLISION in %s table: '%s' (0x%08X) collides with '%s'",
                table_name, name, hash, existing_name
            ))
        end
        return existing_idx
    end
    
    local idx = #tbl.names
    table.insert(tbl.names, name)
    table.insert(tbl.hashes, hash)
    tbl.hash_to_idx[hash] = idx
    tbl.name_to_idx[name] = idx
    
    return idx
end

local function add_string_hash(str)
    local hash = fnv1a_32(str)
    local existing = _module.string_hashes.hash_to_str[hash]
    
    if existing and existing ~= str then
        dsl_error(string.format(
            "STRING HASH COLLISION: '%s' (0x%08X) collides with '%s'",
            str, hash, existing
        ))
    end
    
    _module.string_hashes.hash_to_str[hash] = str
    return hash
end

local function add_oneshot(name)
    return add_to_hash_table(_module.oneshot_table, name, "oneshot")
end

local function add_main(name)
    return add_to_hash_table(_module.main_table, name, "main")
end

local function add_pred(name)
    return add_to_hash_table(_module.pred_table, name, "pred")
end

--============================================================================
-- 64-BIT FLAG
--============================================================================

function use_64bit(enabled)
    if enabled == nil then enabled = true end
    if _module then _module.is_64bit = enabled end
end

function use_32bit()
    if _module then _module.is_64bit = false end
end

--============================================================================
-- POOL / SLOT DEFINITIONS
--============================================================================

function defpool(name, ctype)
    if not _module then
        error("[DSL ERROR] defpool() must be inside start_module()", 2)
    end
    
    local p = _module.pools
    if p.pools[name] then
        dsl_error("Pool already defined: " .. name)
    end
    
    debug_print("defpool:", name, "type=" .. ctype)
    
    p.pools[name] = {
        type = ctype,
        id = p.pool_id,
        slot_count = 0,
    }
    p.pool_id = p.pool_id + 1
end

function defslot(name, pool_name)
    if not _module then
        error("[DSL ERROR] defslot() must be inside start_module()", 2)
    end
    
    local p = _module.pools
    if p.slots[name] then
        dsl_error("Slot already defined: " .. name)
    end
    
    local pool = p.pools[pool_name]
    if not pool then
        dsl_error("Unknown pool: " .. pool_name)
    end
    
    local index = pool.slot_count
    pool.slot_count = index + 1
    
    debug_print("defslot:", name, "pool=" .. pool_name, "index=" .. index)
    
    p.slots[name] = {
        pool = pool_name,
        index = index,
    }
end

local function resolve_slot(slot_name)
    local p = _module.pools
    local slot = p.slots[slot_name]
    if not slot then
        dsl_error("Unknown slot: " .. slot_name)
    end
    
    local pool = p.pools[slot.pool]
    return pool.id, slot.index
end

--============================================================================
-- RECORD / FIELD DEFINITIONS (Blackboard Schema)
--============================================================================

-- Type sizes for offset calculation
local FIELD_TYPE_SIZES = {
    int8    = 1,  uint8   = 1,
    int16   = 2,  uint16  = 2,
    int32   = 4,  uint32  = 4,
    int64   = 8,  uint64  = 8,
    float   = 4,  double  = 8,
    bool    = 1,
}

local FIELD_TYPE_CNAMES = {
    int8    = "int8_t",   uint8   = "uint8_t",
    int16   = "int16_t",  uint16  = "uint16_t",
    int32   = "int32_t",  uint32  = "uint32_t",
    int64   = "int64_t",  uint64  = "uint64_t",
    float   = "float",    double  = "double",
    bool    = "bool",
}

local FIELD_TYPE_ALIGN = {
    int8    = 1,  uint8   = 1,
    int16   = 2,  uint16  = 2,
    int32   = 4,  uint32  = 4,
    int64   = 8,  uint64  = 8,
    float   = 4,  double  = 8,
    bool    = 1,
}

local _current_record = nil

function RECORD(name)
    if not _module then
        error("[DSL ERROR] RECORD() must be inside start_module()", 2)
    end
    if _current_record then
        error("[DSL ERROR] RECORD() called while already in a record", 2)
    end
    if _module.records[name] then
        error("[DSL ERROR] Record already defined: " .. name, 2)
    end
    
    debug_print("RECORD:", name)
    
    _current_record = {
        name = name,
        fields = {},
        field_order = {},
        current_offset = 0,
        max_align = 1,
    }
end

function FIELD(name, ftype, array_size)
    if not _current_record then
        error("[DSL ERROR] FIELD() must be inside RECORD()", 2)
    end
    if _current_record.fields[name] then
        error("[DSL ERROR] Field already defined: " .. name, 2)
    end
    
    local base_size = FIELD_TYPE_SIZES[ftype]
    local ctype = FIELD_TYPE_CNAMES[ftype]
    local align = FIELD_TYPE_ALIGN[ftype]
    local is_embedded_record = false
    local embedded_record_name = nil
    
    if not base_size then
        -- Check for embedded record type
        local embedded = _module.records[ftype]
        if embedded then
            base_size = embedded.total_size
            ctype = ftype .. "_t"
            align = embedded.max_align
            is_embedded_record = true
            embedded_record_name = ftype
        else
            error("[DSL ERROR] Unknown field type: " .. ftype .. 
                  " (record must be defined before use)", 2)
        end
    end
    
    local total_size = base_size
    if array_size then
        total_size = base_size * array_size
    end
    
    -- Align current offset
    local padding = (align - (_current_record.current_offset % align)) % align
    local offset = _current_record.current_offset + padding
    
    debug_print("  FIELD:", name, "type=" .. ftype, 
                array_size and ("array=" .. array_size) or "",
                "offset=" .. offset, "size=" .. total_size)
    
    _current_record.fields[name] = {
        name = name,
        type = ftype,
        ctype = ctype,
        array_size = array_size,
        offset = offset,
        size = total_size,
        base_size = base_size,
        align = align,
        hash = fnv1a_32(name),
        is_embedded_record = is_embedded_record,
        embedded_record_name = embedded_record_name,
        is_pointer = false,
    }
    table.insert(_current_record.field_order, name)
    
    _current_record.current_offset = offset + total_size
    if align > _current_record.max_align then
        _current_record.max_align = align
    end
end

-- Pointer field to another record
function PTR_FIELD(name, record_type)
    if not _current_record then
        error("[DSL ERROR] PTR_FIELD() must be inside RECORD()", 2)
    end
    if _current_record.fields[name] then
        error("[DSL ERROR] Field already defined: " .. name, 2)
    end
    
    -- Pointer size depends on 64-bit mode
    local ptr_size = _module.is_64bit and 8 or 4
    local align = ptr_size
    
    local padding = (align - (_current_record.current_offset % align)) % align
    local offset = _current_record.current_offset + padding
    
    debug_print("  PTR_FIELD:", name, "->", record_type, "offset=" .. offset, "size=" .. ptr_size)
    
    _current_record.fields[name] = {
        name = name,
        type = record_type,
        ctype = record_type .. "_t*",
        array_size = nil,
        offset = offset,
        size = ptr_size,
        base_size = ptr_size,
        align = align,
        hash = fnv1a_32(name),
        is_embedded_record = false,
        embedded_record_name = nil,
        is_pointer = true,
        pointed_record = record_type,
    }
    table.insert(_current_record.field_order, name)
    
    _current_record.current_offset = offset + ptr_size
    if align > _current_record.max_align then
        _current_record.max_align = align
    end
end

function END_RECORD()
    if not _current_record then
        error("[DSL ERROR] END_RECORD() without matching RECORD()", 2)
    end
    
    -- Align total size to max alignment
    local align = _current_record.max_align
    local padding = (align - (_current_record.current_offset % align)) % align
    _current_record.total_size = _current_record.current_offset + padding
    _current_record.hash = fnv1a_32(_current_record.name)
    
    debug_print("END_RECORD:", _current_record.name, "size=" .. _current_record.total_size, "align=" .. _current_record.max_align)
    
    _module.records[_current_record.name] = _current_record
    table.insert(_module.record_order, _current_record.name)
    
    _current_record = nil
end

-- Tree uses a record as its blackboard
function use_record(record_name)
    check_in_tree("use_record")
    
    local record = _module.records[record_name]
    if not record then
        dsl_error("Unknown record: " .. record_name)
    end
    
    if _module.tree_record then
        dsl_error("Tree already has a record: " .. _module.tree_record)
    end
    
    debug_print("  use_record:", record_name)
    
    _module.tree_record = record_name
end

--============================================================================
-- PARAMETER EMISSION
--============================================================================

local function emit_param(p)
    table.insert(_module.params, p)
    return #_module.params
end

function int(value)
    check_in_tree("int")
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

-- Reference a field in the tree's record (blackboard)
function field_ref(field_name)
    check_in_tree("field_ref")
    
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

-- Reference a nested field (e.g., "position.x" for embedded record)
function nested_field_ref(path)
    check_in_tree("nested_field_ref")
    
    if not _module.tree_record then
        dsl_error("nested_field_ref() requires use_record() first")
    end
    
    -- Parse path like "position.x" or "transform.position.x"
    local parts = {}
    for part in path:gmatch("[^.]+") do
        table.insert(parts, part)
    end
    
    if #parts < 2 then
        dsl_error("nested_field_ref() requires path like 'field.subfield'")
    end
    
    -- Walk the path to compute final offset and size
    local current_record = _module.records[_module.tree_record]
    local total_offset = 0
    local final_size = 0
    local final_field_name = path
    
    for i, part in ipairs(parts) do
        local field = current_record.fields[part]
        if not field then
            dsl_error("Unknown field '" .. part .. "' in record '" .. current_record.name .. "'")
        end
        
        total_offset = total_offset + field.offset
        
        if i == #parts then
            -- Last part - this is the actual field we want
            final_size = field.size
        else
            -- Intermediate part - must be an embedded record
            if not field.is_embedded_record then
                dsl_error("Field '" .. part .. "' is not an embedded record, cannot access subfields")
            end
            current_record = _module.records[field.embedded_record_name]
            if not current_record then
                dsl_error("Embedded record '" .. field.embedded_record_name .. "' not found")
            end
        end
    end
    
    emit_param({
        type = PARAM_OPCODES.FIELD,
        index_to_pointer = 0,
        node_index = 0,
        field_offset = total_offset,
        field_size = final_size,
        value_type = "field",
        field_name = final_field_name,
        record_name = _module.tree_record,
    })
end

--============================================================================
-- LIST START/END
--============================================================================

function list_start(prefix)
    check_in_tree("list_start")
    local name = gensym(prefix or "list")
    
    local idx = emit_param({
        type = PARAM_OPCODES.OPEN,
        index_to_pointer = 0,
        node_index = 0,
        value = 0,
        value_type = "brace",
    })
    
    table.insert(_module.brace_stack, { type = "list", name = name, idx = idx })
    return name
end

function list_end(name)
    check_in_tree("list_end")
    
    if #_module.brace_stack == 0 then
        dsl_error("list_end() with no matching list_start()")
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

local function start_call(call_type, prefix, func_name, survives_reset)
    check_in_tree(call_type)
    
    if type(func_name) ~= "string" then
        dsl_error(call_type .. "() requires function name as first argument")
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
        param_start = #_module.params + 1,
    })
    
    return name
end

function o_call(func_name, prefix)
    return start_call("o_call", prefix, func_name, false)
end

function io_call(func_name, prefix)
    return start_call("io_call", prefix, func_name, true)
end

function m_call(func_name, prefix)
    if _module.pt_m_call_funcs[func_name] then
        dsl_error(string.format(
            "Function '%s' already registered as pt_m_call, cannot use m_call",
            func_name
        ))
    end
    _module.m_call_funcs[func_name] = true
    
    return start_call("m_call", prefix, func_name, false)
end

function pt_m_call(func_name, prefix)
    if _module.m_call_funcs[func_name] then
        dsl_error(string.format(
            "Function '%s' already registered as m_call, cannot use pt_m_call",
            func_name
        ))
    end
    _module.pt_m_call_funcs[func_name] = true
    
    return start_call("pt_m_call", prefix, func_name, false)
end

function p_call(func_name, prefix)
    return start_call("p_call", prefix, func_name, false)
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
    
    if top.is_pt_call then
        local param_count = #_module.params - top.param_start + 1
        _module.pointer_counter = _module.pointer_counter + param_count
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

function start_module(name, opts)
    debug_print("start_module:", name)
    init_module(name, opts)
    return name
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
    
    _module.current_tree = name
    _module.params = {}
    _module.func_node_counter = 0
    _module.pointer_counter = 0
    _module.brace_stack = {}
    _module.tree_record = nil
    
    return name
end

function end_tree(name)
    if type(name) ~= "string" then
        error("[DSL ERROR] end_tree() requires string name", 2)
    end
    
    if not _module or name ~= _module.current_tree then
        dsl_error(string.format("end_tree('%s') does not match start_tree('%s')",
            name, _module and _module.current_tree or "nil"))
    end
    
    if #_module.brace_stack > 0 then
        local unclosed = {}
        for _, entry in ipairs(_module.brace_stack) do
            table.insert(unclosed, string.format("%s('%s')", entry.type, entry.name))
        end
        dsl_error("unclosed: " .. table.concat(unclosed, ", "))
    end
    
    if #_module.params == 0 then
        dsl_error("tree '" .. name .. "' has no parameters")
    end
    
    debug_print("end_tree:", name, "params=" .. #_module.params, "func_nodes=" .. _module.func_node_counter)
    
    _module.trees[name] = {
        params = _module.params,
        func_node_count = _module.func_node_counter,
        pointer_count = _module.pointer_counter,
        record_name = _module.tree_record,
    }
    table.insert(_module.tree_order, name)
    
    _module.current_tree = nil
    _module.params = nil
    _module.tree_record = nil
    
    return name
end

function end_module(name)
    if type(name) ~= "string" then
        error("[DSL ERROR] end_module() requires string name", 2)
    end
    
    if not _module or name ~= _module.name then
        error(string.format("[DSL ERROR] end_module('%s') does not match start_module('%s')",
            name, _module and _module.name or "nil"), 2)
    end
    
    if _module.current_tree then
        error("[DSL ERROR] end_module() called with unclosed tree: " .. _module.current_tree, 2)
    end
    
    if #_module.tree_order == 0 then
        error("[DSL ERROR] module has no trees", 2)
    end
    
    -- Validate pointer fields reference valid records
    for _, record_name in ipairs(_module.record_order) do
        local record = _module.records[record_name]
        for _, field_name in ipairs(record.field_order) do
            local field = record.fields[field_name]
            if field.is_pointer and field.pointed_record then
                if not _module.records[field.pointed_record] then
                    error(string.format(
                        "[DSL ERROR] PTR_FIELD '%s' in record '%s' references unknown record '%s'",
                        field_name, record_name, field.pointed_record), 2)
                end
            end
        end
    end
    
    debug_print("end_module:", name, "trees=" .. #_module.tree_order, "records=" .. #_module.record_order)
    
    return ModuleGenerator.new(_module)
end

--============================================================================
-- MODULE GENERATOR
--============================================================================

ModuleGenerator = {}
ModuleGenerator.__index = ModuleGenerator

function ModuleGenerator.new(mod_data)
    local self = setmetatable({}, ModuleGenerator)
    self.name = mod_data.name
    self.trees = mod_data.trees
    self.tree_order = mod_data.tree_order
    self.oneshot_table = mod_data.oneshot_table
    self.main_table = mod_data.main_table
    self.pred_table = mod_data.pred_table
    self.string_hashes = mod_data.string_hashes
    self.pools = mod_data.pools
    self.records = mod_data.records
    self.record_order = mod_data.record_order
    self.is_64bit = mod_data.is_64bit
    
    self.max_func_node_count = 0
    self.max_pointer_count = 0
    self.max_param_count = 0
    
    for _, tree_name in ipairs(self.tree_order) do
        local tree = self.trees[tree_name]
        if tree.func_node_count > self.max_func_node_count then
            self.max_func_node_count = tree.func_node_count
        end
        if tree.pointer_count > self.max_pointer_count then
            self.max_pointer_count = tree.pointer_count
        end
        if #tree.params > self.max_param_count then
            self.max_param_count = #tree.params
        end
    end
    
    return self
end

--============================================================================
-- C HEADER OUTPUT
--============================================================================

local function format_hash(h)
    return string.format("0x%08X", h)
end

function ModuleGenerator:to_c_header(base_name)
    local lines = {}
    local guard = string.upper(base_name) .. "_MODULE_H"
    local prefix = base_name
    
    local int_suffix = self.is_64bit and "LL" or ""
    local uint_suffix = self.is_64bit and "ULL" or "U"
    local float_suffix = self.is_64bit and "" or "f"
    
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "// " .. base_name .. "_module.h")
    table.insert(lines, "// Generated by ChainTree S-Expression DSL v3.0")
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
    
    table.insert(lines, "// Size configuration - must be defined before including s_engine_types.h")
    table.insert(lines, "#ifndef MODULE_IS_64BIT")
    table.insert(lines, "#define MODULE_IS_64BIT " .. (self.is_64bit and "1" or "0"))
    table.insert(lines, "#endif")
    table.insert(lines, "")
    
    table.insert(lines, '#include "s_engine_types.h"')
    table.insert(lines, "")
    
    -- Record structures - forward declarations first for pointer fields
    if #self.record_order > 0 then
        table.insert(lines, "// ============================================================================")
        table.insert(lines, "// RECORD FORWARD DECLARATIONS")
        table.insert(lines, "// ============================================================================")
        table.insert(lines, "")
        for _, record_name in ipairs(self.record_order) do
            table.insert(lines, "typedef struct " .. record_name .. "_s " .. record_name .. "_t;")
        end
        table.insert(lines, "")
        
        table.insert(lines, "// ============================================================================")
        table.insert(lines, "// RECORD (BLACKBOARD) STRUCTURES")
        table.insert(lines, "// ============================================================================")
        table.insert(lines, "")
        
        for _, record_name in ipairs(self.record_order) do
            local record = self.records[record_name]
            
            table.insert(lines, "// Record: " .. record_name .. " (size=" .. record.total_size .. " align=" .. record.max_align .. ")")
            table.insert(lines, "struct " .. record_name .. "_s {")
            for _, field_name in ipairs(record.field_order) do
                local field = record.fields[field_name]
                local type_str = field.ctype
                local comment = string.format("// offset=%d size=%d", field.offset, field.size)
                
                if field.is_embedded_record then
                    comment = comment .. " (embedded " .. field.embedded_record_name .. ")"
                elseif field.is_pointer then
                    comment = comment .. " (ptr to " .. field.pointed_record .. " - USER MANAGES MEMORY)"
                end
                
                if field.array_size then
                    table.insert(lines, string.format("    %s %s[%d];  %s",
                        type_str, field.name, field.array_size, comment))
                else
                    table.insert(lines, string.format("    %s %s;  %s",
                        type_str, field.name, comment))
                end
            end
            table.insert(lines, "};")
            table.insert(lines, "")
            
            -- Field descriptor array
            table.insert(lines, "static const s_expr_field_desc_t " .. prefix .. "_" .. record_name .. "_fields[] = {")
            for _, field_name in ipairs(record.field_order) do
                local field = record.fields[field_name]
                table.insert(lines, string.format('    { %s, %d, %d },  // %s',
                    format_hash(field.hash), field.offset, field.size, field.name))
            end
            table.insert(lines, "};")
            table.insert(lines, "")
        end
        
        -- Record descriptor array
        table.insert(lines, "static const s_expr_record_desc_t " .. prefix .. "_records[] = {")
        for _, record_name in ipairs(self.record_order) do
            local record = self.records[record_name]
            table.insert(lines, string.format('    { %s, %d, %d, %s_%s_fields },  // %s',
                format_hash(record.hash),
                record.total_size,
                #record.field_order,
                prefix,
                record_name,
                record_name))
        end
        table.insert(lines, "};")
        table.insert(lines, "#define " .. string.upper(prefix) .. "_RECORD_COUNT " .. #self.record_order)
        table.insert(lines, "")
    end
    
    -- Function hash arrays
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "// FUNCTION HASH TABLES")
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "")
    
    if #self.oneshot_table.hashes > 0 then
        table.insert(lines, "static const uint32_t " .. prefix .. "_oneshot_hashes[] = {")
        for i, hash in ipairs(self.oneshot_table.hashes) do
            local name = self.oneshot_table.names[i]
            table.insert(lines, string.format("    %s,  // [%d] %s", format_hash(hash), i - 1, name))
        end
        table.insert(lines, "};")
    else
        table.insert(lines, "static const uint32_t* " .. prefix .. "_oneshot_hashes = NULL;")
    end
    table.insert(lines, "#define " .. string.upper(prefix) .. "_ONESHOT_COUNT " .. #self.oneshot_table.hashes)
    table.insert(lines, "")
    
    if #self.main_table.hashes > 0 then
        table.insert(lines, "static const uint32_t " .. prefix .. "_main_hashes[] = {")
        for i, hash in ipairs(self.main_table.hashes) do
            local name = self.main_table.names[i]
            table.insert(lines, string.format("    %s,  // [%d] %s", format_hash(hash), i - 1, name))
        end
        table.insert(lines, "};")
    else
        table.insert(lines, "static const uint32_t* " .. prefix .. "_main_hashes = NULL;")
    end
    table.insert(lines, "#define " .. string.upper(prefix) .. "_MAIN_COUNT " .. #self.main_table.hashes)
    table.insert(lines, "")
    
    if #self.pred_table.hashes > 0 then
        table.insert(lines, "static const uint32_t " .. prefix .. "_pred_hashes[] = {")
        for i, hash in ipairs(self.pred_table.hashes) do
            local name = self.pred_table.names[i]
            table.insert(lines, string.format("    %s,  // [%d] %s", format_hash(hash), i - 1, name))
        end
        table.insert(lines, "};")
    else
        table.insert(lines, "static const uint32_t* " .. prefix .. "_pred_hashes = NULL;")
    end
    table.insert(lines, "#define " .. string.upper(prefix) .. "_PRED_COUNT " .. #self.pred_table.hashes)
    table.insert(lines, "")
    
    -- Per-tree parameter arrays
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "// TREE PARAMETERS")
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "")
    
    for _, tree_name in ipairs(self.tree_order) do
        local tree = self.trees[tree_name]
        local tree_prefix = prefix .. "_" .. tree_name
        
        table.insert(lines, "// Tree: " .. tree_name)
        table.insert(lines, "// func_node_count=" .. tree.func_node_count .. " pointer_count=" .. tree.pointer_count)
        if tree.record_name then
            table.insert(lines, "// record=" .. tree.record_name)
        end
        table.insert(lines, "static const s_expr_param_t " .. tree_prefix .. "_params[] = {")
        
        for i, p in ipairs(tree.params) do
            local type_str = string.format("0x%02X", p.type)
            local idx_ptr = p.index_to_pointer or 0
            local node_idx = p.node_index or 0
            local val_str = ""
            local comment = ""
            
            local base_type = band(p.type, 0x0F)
            
            if p.value_type == "int" then
                val_str = string.format(".i = %d%s", p.value, int_suffix)
            elseif p.value_type == "uint" then
                val_str = string.format(".u = %u%s", p.value, uint_suffix)
            elseif p.value_type == "float" then
                val_str = string.format(".f = %g%s", p.value, float_suffix)
            elseif p.value_type == "hash" then
                val_str = string.format(".str_hash = %s", format_hash(p.value))
                comment = p.str_content and (' // "' .. p.str_content .. '"') or ""
            elseif p.value_type == "slot" then
                val_str = string.format(".slot = { %d, %d }", p.pool_id or 0, p.slot_index or 0)
                comment = p.slot_name and (" // " .. p.slot_name) or ""
            elseif p.value_type == "field" then
                val_str = string.format(".field = { %d, %d }", p.field_offset or 0, p.field_size or 0)
                comment = p.field_name and (" // " .. p.field_name) or ""
            elseif p.value_type == "brace" then
                val_str = string.format(".brace_idx = %d", p.value)
                if base_type == PARAM_OPCODES.OPEN or base_type == PARAM_OPCODES.OPEN_CALL then
                    comment = string.format(" // -> +%d", p.value)
                else
                    comment = string.format(" // <- -%d", p.value)
                end
            elseif p.value_type == "func" then
                val_str = string.format(".func_idx = %d", p.value)
                comment = p.func_name and (" // " .. p.func_name) or ""
            else
                val_str = string.format(".u = %u%s", p.value or 0, uint_suffix)
            end
            
            table.insert(lines, string.format(
                "    { %s, %d, %d, {0}, %s },%s  // [%d]",
                type_str, idx_ptr, node_idx, val_str, comment, i - 1
            ))
        end
        
        table.insert(lines, "};")
        table.insert(lines, "#define " .. string.upper(tree_prefix) .. "_PARAM_COUNT " .. #tree.params)
        table.insert(lines, "")
    end
    
    -- Tree definitions
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "// TREE DEFINITIONS")
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "")
    
    table.insert(lines, "static const s_expr_tree_def_t " .. prefix .. "_trees[] = {")
    for _, tree_name in ipairs(self.tree_order) do
        local tree = self.trees[tree_name]
        local tree_prefix = prefix .. "_" .. tree_name
        
        local record_hash = "0"
        if tree.record_name and self.records[tree.record_name] then
            record_hash = format_hash(self.records[tree.record_name].hash)
        end
        
        table.insert(lines, string.format('    { %s, %s, %s_params, %d, %d, %d },  // "%s"',
            format_hash(fnv1a_32(tree_name)),
            record_hash,
            tree_prefix,
            #tree.params,
            tree.func_node_count,
            tree.pointer_count,
            tree_name
        ))
    end
    table.insert(lines, "};")
    table.insert(lines, "#define " .. string.upper(prefix) .. "_TREE_COUNT " .. #self.tree_order)
    table.insert(lines, "")
    
    -- Module definition
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "// MODULE DEFINITION")
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "")
    
    table.insert(lines, "static const s_expr_module_def_t " .. prefix .. "_module = {")
    table.insert(lines, '    .name_hash = ' .. format_hash(fnv1a_32(self.name)) .. ',  // "' .. self.name .. '"')
    table.insert(lines, '    .trees = ' .. prefix .. '_trees,')
    table.insert(lines, '    .tree_count = ' .. #self.tree_order .. ',')
    table.insert(lines, '    .is_64bit = ' .. (self.is_64bit and 'true' or 'false') .. ',')
    table.insert(lines, '    .oneshot_hashes = ' .. (self.oneshot_table.hashes[1] and (prefix .. '_oneshot_hashes') or 'NULL') .. ',')
    table.insert(lines, '    .main_hashes = ' .. (self.main_table.hashes[1] and (prefix .. '_main_hashes') or 'NULL') .. ',')
    table.insert(lines, '    .pred_hashes = ' .. (self.pred_table.hashes[1] and (prefix .. '_pred_hashes') or 'NULL') .. ',')
    table.insert(lines, '    .oneshot_count = ' .. #self.oneshot_table.hashes .. ',')
    table.insert(lines, '    .main_count = ' .. #self.main_table.hashes .. ',')
    table.insert(lines, '    .pred_count = ' .. #self.pred_table.hashes .. ',')
    table.insert(lines, '    .max_func_node_count = ' .. self.max_func_node_count .. ',')
    table.insert(lines, '    .max_pointer_count = ' .. self.max_pointer_count .. ',')
    table.insert(lines, '    .max_param_count = ' .. self.max_param_count .. ',')
    table.insert(lines, "};")
    table.insert(lines, "")
    
    table.insert(lines, "#ifdef __cplusplus")
    table.insert(lines, "}")
    table.insert(lines, "#endif")
    table.insert(lines, "")
    table.insert(lines, "#endif // " .. guard)
    
    return table.concat(lines, "\n")
end

--============================================================================
-- USER FUNCTION HEADER OUTPUT (.h)
--============================================================================

local function is_user_function(name)
    return not name:match("^CFL_")
end

local function to_c_func_name(name, suffix)
    return string.lower(name) .. "_" .. suffix
end

function ModuleGenerator:to_c_user_header(base_name)
    local lines = {}
    local guard = string.upper(base_name) .. "_USER_FUNCTIONS_H"
    
    local user_oneshot = {}
    local user_main = {}
    local user_pred = {}
    
    for i, name in ipairs(self.oneshot_table.names) do
        if is_user_function(name) then
            table.insert(user_oneshot, {
                name = name,
                hash = self.oneshot_table.hashes[i],
                c_name = to_c_func_name(name, "oneshot"),
            })
        end
    end
    
    for i, name in ipairs(self.main_table.names) do
        if is_user_function(name) then
            table.insert(user_main, {
                name = name,
                hash = self.main_table.hashes[i],
                c_name = to_c_func_name(name, "main"),
            })
        end
    end
    
    for i, name in ipairs(self.pred_table.names) do
        if is_user_function(name) then
            table.insert(user_pred, {
                name = name,
                hash = self.pred_table.hashes[i],
                c_name = to_c_func_name(name, "boolean"),
            })
        end
    end
    
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "// " .. base_name .. "_user_functions.h")
    table.insert(lines, "// Generated by ChainTree S-Expression DSL v3.0")
    table.insert(lines, "// User function declarations - implement these in your code")
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
    table.insert(lines, '#include "cfl_runtime.h"')
    table.insert(lines, "")
    
    if #user_oneshot > 0 then
        table.insert(lines, "// ============================================================================")
        table.insert(lines, "// ONESHOT FUNCTIONS (void return, run once)")
        table.insert(lines, "// ============================================================================")
        table.insert(lines, "")
        for _, fn in ipairs(user_oneshot) do
            table.insert(lines, string.format("// DSL: %s  hash: 0x%08X", fn.name, fn.hash))
            table.insert(lines, string.format("void %s(", fn.c_name))
            table.insert(lines, "    s_expr_tree_instance_t* inst,")
            table.insert(lines, "    const s_expr_param_t* params,")
            table.insert(lines, "    uint16_t param_count,")
            table.insert(lines, "    s_expr_event_type_t event_type,")
            table.insert(lines, "    uint16_t event_id,")
            table.insert(lines, "    void* event_data);")
            table.insert(lines, "")
        end
    end
    
    if #user_main > 0 then
        table.insert(lines, "// ============================================================================")
        table.insert(lines, "// MAIN FUNCTIONS (s_expr_result_t return)")
        table.insert(lines, "// ============================================================================")
        table.insert(lines, "")
        for _, fn in ipairs(user_main) do
            table.insert(lines, string.format("// DSL: %s  hash: 0x%08X", fn.name, fn.hash))
            table.insert(lines, string.format("s_expr_result_t %s(", fn.c_name))
            table.insert(lines, "    s_expr_tree_instance_t* inst,")
            table.insert(lines, "    const s_expr_param_t* params,")
            table.insert(lines, "    uint16_t param_count,")
            table.insert(lines, "    s_expr_event_type_t event_type,")
            table.insert(lines, "    uint16_t event_id,")
            table.insert(lines, "    void* event_data);")
            table.insert(lines, "")
        end
    end
    
    if #user_pred > 0 then
        table.insert(lines, "// ============================================================================")
        table.insert(lines, "// PREDICATE FUNCTIONS (bool return)")
        table.insert(lines, "// ============================================================================")
        table.insert(lines, "")
        for _, fn in ipairs(user_pred) do
            table.insert(lines, string.format("// DSL: %s  hash: 0x%08X", fn.name, fn.hash))
            table.insert(lines, string.format("bool %s(", fn.c_name))
            table.insert(lines, "    s_expr_tree_instance_t* inst,")
            table.insert(lines, "    const s_expr_param_t* params,")
            table.insert(lines, "    uint16_t param_count,")
            table.insert(lines, "    s_expr_event_type_t event_type,")
            table.insert(lines, "    uint16_t event_id,")
            table.insert(lines, "    void* event_data);")
            table.insert(lines, "")
        end
    end
    
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "// REGISTRATION FUNCTION")
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "")
    table.insert(lines, "void load_user_s_functions(cfl_runtime_handle_t* handle);")
    table.insert(lines, "")
    
    table.insert(lines, "#ifdef __cplusplus")
    table.insert(lines, "}")
    table.insert(lines, "#endif")
    table.insert(lines, "")
    table.insert(lines, "#endif // " .. guard)
    
    return table.concat(lines, "\n")
end

--============================================================================
-- USER FUNCTION REGISTRATION OUTPUT (.c)
--============================================================================

function ModuleGenerator:to_c_user_registration(base_name)
    local lines = {}
    
    local user_oneshot = {}
    local user_main = {}
    local user_pred = {}
    
    for i, name in ipairs(self.oneshot_table.names) do
        if is_user_function(name) then
            table.insert(user_oneshot, {
                name = name,
                hash = self.oneshot_table.hashes[i],
                c_name = to_c_func_name(name, "oneshot"),
            })
        end
    end
    
    for i, name in ipairs(self.main_table.names) do
        if is_user_function(name) then
            table.insert(user_main, {
                name = name,
                hash = self.main_table.hashes[i],
                c_name = to_c_func_name(name, "main"),
            })
        end
    end
    
    for i, name in ipairs(self.pred_table.names) do
        if is_user_function(name) then
            table.insert(user_pred, {
                name = name,
                hash = self.pred_table.hashes[i],
                c_name = to_c_func_name(name, "boolean"),
            })
        end
    end
    
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "// " .. base_name .. "_user_registration.c")
    table.insert(lines, "// Generated by ChainTree S-Expression DSL v3.0")
    table.insert(lines, "// DO NOT EDIT - regenerate from DSL")
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "")
    table.insert(lines, '#include "' .. base_name .. '_user_functions.h"')
    table.insert(lines, '#include <stdio.h>')
    table.insert(lines, "")
    
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "// REGISTRATION TABLES")
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "")
    table.insert(lines, "#define ARRAY_COUNT(arr) (sizeof(arr) / sizeof((arr)[0]))")
    table.insert(lines, "")
    
    if #user_oneshot > 0 then
        table.insert(lines, "static const s_expr_fn_entry_named_t user_oneshot_entries_named[] = {")
        for _, fn in ipairs(user_oneshot) do
            table.insert(lines, string.format('    { "%s", (void*)%s },', fn.name, fn.c_name))
        end
        table.insert(lines, "};")
        table.insert(lines, "static s_expr_fn_entry_t user_oneshot_entries[ARRAY_COUNT(user_oneshot_entries_named)];")
    else
        table.insert(lines, "static const s_expr_fn_entry_named_t* user_oneshot_entries_named = NULL;")
        table.insert(lines, "static s_expr_fn_entry_t* user_oneshot_entries = NULL;")
    end
    table.insert(lines, "")
    
    if #user_main > 0 then
        table.insert(lines, "static const s_expr_fn_entry_named_t user_main_entries_named[] = {")
        for _, fn in ipairs(user_main) do
            table.insert(lines, string.format('    { "%s", (void*)%s },', fn.name, fn.c_name))
        end
        table.insert(lines, "};")
        table.insert(lines, "static s_expr_fn_entry_t user_main_entries[ARRAY_COUNT(user_main_entries_named)];")
    else
        table.insert(lines, "static const s_expr_fn_entry_named_t* user_main_entries_named = NULL;")
        table.insert(lines, "static s_expr_fn_entry_t* user_main_entries = NULL;")
    end
    table.insert(lines, "")
    
    if #user_pred > 0 then
        table.insert(lines, "static const s_expr_fn_entry_named_t user_pred_entries_named[] = {")
        for _, fn in ipairs(user_pred) do
            table.insert(lines, string.format('    { "%s", (void*)%s },', fn.name, fn.c_name))
        end
        table.insert(lines, "};")
        table.insert(lines, "static s_expr_fn_entry_t user_pred_entries[ARRAY_COUNT(user_pred_entries_named)];")
    else
        table.insert(lines, "static const s_expr_fn_entry_named_t* user_pred_entries_named = NULL;")
        table.insert(lines, "static s_expr_fn_entry_t* user_pred_entries = NULL;")
    end
    table.insert(lines, "")
    
    table.insert(lines, "static s_expr_fn_table_t user_oneshot_table;")
    table.insert(lines, "static s_expr_fn_table_t user_main_table;")
    table.insert(lines, "static s_expr_fn_table_t user_pred_table;")
    table.insert(lines, "")
    
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
    else
        table.insert(lines, "    user_oneshot_table.entries = NULL;")
        table.insert(lines, "    user_oneshot_table.count = 0;")
    end
    table.insert(lines, "")
    
    if #user_main > 0 then
        table.insert(lines, "    s_expr_build_fn_table(")
        table.insert(lines, "        user_main_entries_named,")
        table.insert(lines, "        user_main_entries,")
        table.insert(lines, "        ARRAY_COUNT(user_main_entries_named)")
        table.insert(lines, "    );")
        table.insert(lines, "    user_main_table.entries = user_main_entries;")
        table.insert(lines, "    user_main_table.count = ARRAY_COUNT(user_main_entries);")
    else
        table.insert(lines, "    user_main_table.entries = NULL;")
        table.insert(lines, "    user_main_table.count = 0;")
    end
    table.insert(lines, "")
    
    if #user_pred > 0 then
        table.insert(lines, "    s_expr_build_fn_table(")
        table.insert(lines, "        user_pred_entries_named,")
        table.insert(lines, "        user_pred_entries,")
        table.insert(lines, "        ARRAY_COUNT(user_pred_entries_named)")
        table.insert(lines, "    );")
        table.insert(lines, "    user_pred_table.entries = user_pred_entries;")
        table.insert(lines, "    user_pred_table.count = ARRAY_COUNT(user_pred_entries);")
    else
        table.insert(lines, "    user_pred_table.entries = NULL;")
        table.insert(lines, "    user_pred_table.count = 0;")
    end
    
    table.insert(lines, "}")
    table.insert(lines, "")
    
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
    table.insert(lines, "        s_expr_module_register_oneshot(modules[i], &user_oneshot_table);")
    table.insert(lines, "        s_expr_module_register_main(modules[i], &user_main_table);")
    table.insert(lines, "        s_expr_module_register_pred(modules[i], &user_pred_table);")
    table.insert(lines, "    }")
    table.insert(lines, "}")
    
    return table.concat(lines, "\n")
end

--============================================================================
-- POOLS OUTPUT
--============================================================================

function ModuleGenerator:to_pools_header(base_name)
    local lines = {}
    local guard = string.upper(base_name) .. "_POOLS_H"
    local p = self.pools
    
    table.insert(lines, "// Generated by ChainTree S-Expression DSL v3.0")
    table.insert(lines, "#ifndef " .. guard)
    table.insert(lines, "#define " .. guard)
    table.insert(lines, "")
    
    local ordered_pools = {}
    for name, pool in pairs(p.pools) do
        ordered_pools[pool.id + 1] = { name = name, pool = pool }
    end
    
    for _, entry in ipairs(ordered_pools) do
        table.insert(lines, string.format("#define POOL_%s %d", string.upper(entry.name), entry.pool.id))
    end
    table.insert(lines, "")
    
    for _, entry in ipairs(ordered_pools) do
        table.insert(lines, string.format("#define %s_POOL_SIZE %d", string.upper(entry.name), entry.pool.slot_count))
    end
    table.insert(lines, "")
    
    for slot_name, slot in pairs(p.slots) do
        table.insert(lines, string.format("#define SLOT_%s POOL_%s, %d",
            string.upper(slot_name), string.upper(slot.pool), slot.index))
    end
    table.insert(lines, "")
    
    table.insert(lines, string.format("#define POOL_COUNT %d", p.pool_id))
    table.insert(lines, "")
    table.insert(lines, "#endif // " .. guard)
    
    return table.concat(lines, "\n")
end

--============================================================================
-- DUMP
--============================================================================

function ModuleGenerator:dump()
    print("MODULE: " .. self.name .. " (" .. format_hash(fnv1a_32(self.name)) .. ")")
    print("64-bit: " .. (self.is_64bit and "yes" or "no"))
    print("")
    
    if #self.record_order > 0 then
        print("RECORDS:")
        for _, record_name in ipairs(self.record_order) do
            local record = self.records[record_name]
            print(string.format("  %s (%s) size=%d align=%d",
                record_name, format_hash(record.hash), record.total_size, record.max_align))
            for _, field_name in ipairs(record.field_order) do
                local field = record.fields[field_name]
                local extra = ""
                if field.is_embedded_record then
                    extra = " [embedded " .. field.embedded_record_name .. "]"
                elseif field.is_pointer then
                    extra = " [ptr to " .. field.pointed_record .. "]"
                end
                if field.array_size then
                    print(string.format("    %s: %s[%d] offset=%d size=%d%s",
                        field_name, field.type, field.array_size, field.offset, field.size, extra))
                else
                    print(string.format("    %s: %s offset=%d size=%d%s",
                        field_name, field.type, field.offset, field.size, extra))
                end
            end
        end
        print("")
    end
    
    print("ONESHOT FUNCTIONS:")
    for i, name in ipairs(self.oneshot_table.names) do
        local marker = is_user_function(name) and "[USER]" or "[SYS] "
        print(string.format("  %s [%d] %s -> %s", marker, i - 1, name, format_hash(self.oneshot_table.hashes[i])))
    end
    print("")
    
    print("MAIN FUNCTIONS:")
    for i, name in ipairs(self.main_table.names) do
        local marker = is_user_function(name) and "[USER]" or "[SYS] "
        print(string.format("  %s [%d] %s -> %s", marker, i - 1, name, format_hash(self.main_table.hashes[i])))
    end
    print("")
    
    print("PREDICATE FUNCTIONS:")
    for i, name in ipairs(self.pred_table.names) do
        local marker = is_user_function(name) and "[USER]" or "[SYS] "
        print(string.format("  %s [%d] %s -> %s", marker, i - 1, name, format_hash(self.pred_table.hashes[i])))
    end
    print("")
    
    print("STRING HASHES:")
    for hash, s in pairs(self.string_hashes.hash_to_str) do
        print(string.format("  %s -> \"%s\"", format_hash(hash), s))
    end
    print("")
    
    print(string.format("MAX: func_nodes=%d pointers=%d params=%d",
        self.max_func_node_count, self.max_pointer_count, self.max_param_count))
    print("")
    
    for _, tree_name in ipairs(self.tree_order) do
        local tree = self.trees[tree_name]
        local record_info = ""
        if tree.record_name then
            record_info = " record=" .. tree.record_name
        end
        print("TREE: " .. tree_name .. " (" .. format_hash(fnv1a_32(tree_name)) .. ")" .. record_info)
        print(string.format("  func_nodes=%d pointers=%d params=%d",
            tree.func_node_count, tree.pointer_count, #tree.params))
        
        for i, p in ipairs(tree.params) do
            local type_name = "?"
            local base = band(p.type, 0x0F)
            local flags = ""
            
            if band(p.type, TYPE_FLAGS.POINTER) ~= 0 then flags = flags .. " PTR" end
            if band(p.type, TYPE_FLAGS.SURVIVES_RESET) ~= 0 then flags = flags .. " INIT" end
            
            local opcodes = {"INT","UINT","FLOAT","STR","SLOT","OPEN","CLOSE","CALL","ONESHOT","MAIN","PRED","FIELD"}
            type_name = opcodes[base + 1] or string.format("0x%02X", base)
            
            local detail = ""
            if p.value_type == "func" then
                detail = string.format("idx=%d node=%d %s", p.value, p.node_index, p.func_name or "")
            elseif p.value_type == "hash" then
                detail = string.format("%s \"%s\"", format_hash(p.value), p.str_content or "")
            elseif p.value_type == "slot" then
                detail = string.format("pool=%d slot=%d %s", p.pool_id, p.slot_index, p.slot_name or "")
            elseif p.value_type == "field" then
                detail = string.format("offset=%d size=%d %s", p.field_offset, p.field_size, p.field_name or "")
            elseif p.value_type == "brace" then
                detail = string.format("offset=%d", p.value)
            else
                detail = tostring(p.value)
            end
            
            print(string.format("  [%2d] %-6s%s %s", i - 1, type_name, flags, detail))
        end
        print("")
    end
end

--============================================================================
-- EXPORT HASH FUNCTION
--============================================================================

function hash32(s)
    return fnv1a_32(s)
end

print("ChainTree S-Expression DSL v3.0 loaded (embedded records, pointer fields, debug support)")