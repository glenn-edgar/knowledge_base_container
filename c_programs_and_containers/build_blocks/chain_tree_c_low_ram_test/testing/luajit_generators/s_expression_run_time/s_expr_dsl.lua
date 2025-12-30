--============================================================================
-- CHAINTREE S-EXPRESSION DSL
-- Version 3.0 - Flat parameter model, hash-based function tables
--               Stack-based list_start/list_end, direct parameter emission
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
}

-- Type flags
local TYPE_FLAGS = {
    SURVIVES_RESET = 0x10,  -- bit 4: io_call behavior
    POINTER        = 0x80,  -- bit 7: pointer capability
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
        is_64bit = opts.is_64bit or false,
        
        -- Current tree state
        current_tree = nil,
        params = nil,
        func_node_counter = 0,
        pointer_counter = 0,
        
        -- Stack for open braces (stores param index)
        brace_stack = {},
        
        -- Context tracking
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
-- PARAMETER EMISSION
--============================================================================

local function emit_param(p)
    table.insert(_module.params, p)
    return #_module.params  -- return 1-based index
end

-- Emit basic types directly

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
        value = 0,  -- patched by list_end
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
        value = 0,  -- patched below
        value_type = "brace",
    })
    
    -- Patch: relative offset
    local offset = close_idx - top.idx
    _module.params[top.idx].value = offset
    _module.params[close_idx].value = offset
end

--============================================================================
-- CALL START/END
--============================================================================

-- Internal: start a call
local function start_call(call_type, prefix, func_name, survives_reset)
    check_in_tree(call_type)
    
    if type(func_name) ~= "string" then
        dsl_error(call_type .. "() requires function name as first argument")
    end
    
    local name = gensym(prefix or func_name)
    
    -- Emit OPEN_CALL
    local open_idx = emit_param({
        type = PARAM_OPCODES.OPEN_CALL,
        index_to_pointer = 0,
        node_index = 0,
        value = 0,  -- patched by end_call
        value_type = "brace",
    })
    
    -- Emit function reference
    local func_idx, type_byte
    local ptr_base = 0  -- base pointer index for pt_m_call
    
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
        -- PTR flag on the MAIN func ref indicates pointer-capable call
        type_byte = bit.bor(PARAM_OPCODES.MAIN, TYPE_FLAGS.POINTER)
        -- Record base pointer index for this call
        ptr_base = _module.pointer_counter
    elseif call_type == "p_call" then
        func_idx = add_pred(func_name)
        type_byte = PARAM_OPCODES.PRED
    end
    
    local node_idx = _module.func_node_counter
    _module.func_node_counter = _module.func_node_counter + 1
    
    emit_param({
        type = type_byte,
        index_to_pointer = ptr_base,  -- base pointer index for pt_m_call
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
        param_start = #_module.params + 1,  -- track where params start
    })
    
    return name
end

-- o_call: oneshot, reset clears init flag
function o_call(func_name, prefix)
    return start_call("o_call", prefix, func_name, false)
end

-- io_call: init-once, survives reset
function io_call(func_name, prefix)
    return start_call("io_call", prefix, func_name, true)
end

-- m_call: main function, no pointers
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

-- pt_m_call: pointer-capable main function
-- PTR flag goes on the MAIN func ref, NOT on parameters
function pt_m_call(func_name, prefix)
    if _module.m_call_funcs[func_name] then
        dsl_error(string.format(
            "Function '%s' already registered as m_call, cannot use pt_m_call",
            func_name
        ))
    end
    _module.pt_m_call_funcs[func_name] = true
    
    -- Note: in_pt_call is NOT set - params don't get PTR flag
    -- PTR flag is set on the function ref in start_call
    return start_call("pt_m_call", prefix, func_name, false)
end

-- p_call: predicate function
function p_call(func_name, prefix)
    return start_call("p_call", prefix, func_name, false)
end

-- end_call: close any call type
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
    
    -- Count params for pt_m_call (for pointer array sizing)
    if top.is_pt_call then
        -- Params start after OPEN_CALL and func_ref, end before CLOSE
        -- param_start was set to #params+1 after func_ref was emitted
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
    
    -- Patch: relative offset
    local offset = close_idx - top.idx
    _module.params[top.idx].value = offset
    _module.params[close_idx].value = offset
end

--============================================================================
-- MODULE / TREE MANAGEMENT
--============================================================================

function start_module(name, opts)
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
    
    gensym_reset()  -- Reset for predictable names per tree
    
    _module.current_tree = name
    _module.params = {}
    _module.func_node_counter = 0
    _module.pointer_counter = 0
    _module.brace_stack = {}
    
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
    
    _module.trees[name] = {
        params = _module.params,
        func_node_count = _module.func_node_counter,
        pointer_count = _module.pointer_counter,
    }
    table.insert(_module.tree_order, name)
    
    _module.current_tree = nil
    _module.params = nil
    
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
    self.is_64bit = mod_data.is_64bit
    
    -- Compute max counts
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
    
    local int_type = self.is_64bit and "int64_t" or "int32_t"
    local uint_type = self.is_64bit and "uint64_t" or "uint32_t"
    local float_type = self.is_64bit and "double" or "float"
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
    table.insert(lines, "#include <stdint.h>")
    table.insert(lines, "#include <stdbool.h>")
    table.insert(lines, "")
    
    -- Size configuration
    table.insert(lines, "// Size configuration")
    table.insert(lines, "#define MODULE_IS_64BIT " .. (self.is_64bit and "1" or "0"))
    table.insert(lines, "")
    
    -- Type aliases
    table.insert(lines, "// Type aliases")
    table.insert(lines, "typedef " .. int_type .. " ct_int_t;")
    table.insert(lines, "typedef " .. uint_type .. " ct_uint_t;")
    table.insert(lines, "typedef " .. float_type .. " ct_float_t;")
    table.insert(lines, "")
    
    -- Parameter type constants
    table.insert(lines, "// Parameter type opcodes (bits 3:0)")
    table.insert(lines, "#define S_EXPR_PARAM_INT         0x00")
    table.insert(lines, "#define S_EXPR_PARAM_UINT        0x01")
    table.insert(lines, "#define S_EXPR_PARAM_FLOAT       0x02")
    table.insert(lines, "#define S_EXPR_PARAM_STR_HASH    0x03")
    table.insert(lines, "#define S_EXPR_PARAM_SLOT        0x04")
    table.insert(lines, "#define S_EXPR_PARAM_OPEN        0x05")
    table.insert(lines, "#define S_EXPR_PARAM_CLOSE       0x06")
    table.insert(lines, "#define S_EXPR_PARAM_OPEN_CALL   0x07")
    table.insert(lines, "#define S_EXPR_PARAM_ONESHOT     0x08")
    table.insert(lines, "#define S_EXPR_PARAM_MAIN        0x09")
    table.insert(lines, "#define S_EXPR_PARAM_PRED        0x0A")
    table.insert(lines, "")
    table.insert(lines, "// Type flags")
    table.insert(lines, "#define S_EXPR_FLAG_SURVIVES_RESET 0x10")
    table.insert(lines, "#define S_EXPR_FLAG_POINTER        0x80")
    table.insert(lines, "#define S_EXPR_OPCODE_MASK         0x0F")
    table.insert(lines, "")
    
    -- Slot reference structure
    table.insert(lines, "// Slot reference")
    table.insert(lines, "typedef struct {")
    table.insert(lines, "    uint16_t pool_id;")
    table.insert(lines, "    uint16_t slot_index;")
    table.insert(lines, "} s_expr_slot_ref_t;")
    table.insert(lines, "")
    
    -- Parameter structure
    table.insert(lines, "// Parameter structure")
    table.insert(lines, "typedef struct {")
    table.insert(lines, "    uint8_t  type;")
    table.insert(lines, "    uint8_t  index_to_pointer;")
    table.insert(lines, "    uint16_t node_index;")
    table.insert(lines, "    uint8_t  reserved[4];")
    table.insert(lines, "    union {")
    table.insert(lines, "        ct_int_t   i;")
    table.insert(lines, "        ct_uint_t  u;")
    table.insert(lines, "        ct_float_t f;")
    table.insert(lines, "        uint32_t   str_hash;")
    table.insert(lines, "        uint16_t   func_idx;")
    table.insert(lines, "        uint16_t   brace_idx;")
    table.insert(lines, "        s_expr_slot_ref_t slot;")
    table.insert(lines, "    };")
    table.insert(lines, "} s_expr_param_t;")
    table.insert(lines, "")
    
    -- Tree definition structure
    table.insert(lines, "// Tree definition")
    table.insert(lines, "typedef struct {")
    table.insert(lines, "    uint32_t name_hash;")
    table.insert(lines, "    const s_expr_param_t* params;")
    table.insert(lines, "    uint16_t param_count;")
    table.insert(lines, "    uint16_t func_node_count;")
    table.insert(lines, "    uint16_t pointer_count;")
    table.insert(lines, "} s_expr_tree_def_t;")
    table.insert(lines, "")
    
    -- Module definition structure
    table.insert(lines, "// Module definition")
    table.insert(lines, "typedef struct {")
    table.insert(lines, "    uint32_t name_hash;")
    table.insert(lines, "    const s_expr_tree_def_t* trees;")
    table.insert(lines, "    uint16_t tree_count;")
    table.insert(lines, "    bool is_64bit;")
    table.insert(lines, "    const uint32_t* oneshot_hashes;")
    table.insert(lines, "    const uint32_t* main_hashes;")
    table.insert(lines, "    const uint32_t* pred_hashes;")
    table.insert(lines, "    uint16_t oneshot_count;")
    table.insert(lines, "    uint16_t main_count;")
    table.insert(lines, "    uint16_t pred_count;")
    table.insert(lines, "    uint16_t max_func_node_count;")
    table.insert(lines, "    uint16_t max_pointer_count;")
    table.insert(lines, "    uint16_t max_param_count;")
    table.insert(lines, "} s_expr_module_def_t;")
    table.insert(lines, "")
    
    -- Function hash arrays
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "// FUNCTION HASH TABLES")
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "")
    
    -- Oneshot hashes
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
    
    -- Main hashes
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
    
    -- Pred hashes
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
        
        table.insert(lines, string.format('    { %s, %s_params, %d, %d, %d },  // "%s"',
            format_hash(fnv1a_32(tree_name)),
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
    
    print("ONESHOT FUNCTIONS:")
    for i, name in ipairs(self.oneshot_table.names) do
        print(string.format("  [%d] %s -> %s", i - 1, name, format_hash(self.oneshot_table.hashes[i])))
    end
    print("")
    
    print("MAIN FUNCTIONS:")
    for i, name in ipairs(self.main_table.names) do
        print(string.format("  [%d] %s -> %s", i - 1, name, format_hash(self.main_table.hashes[i])))
    end
    print("")
    
    print("PREDICATE FUNCTIONS:")
    for i, name in ipairs(self.pred_table.names) do
        print(string.format("  [%d] %s -> %s", i - 1, name, format_hash(self.pred_table.hashes[i])))
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
        print("TREE: " .. tree_name .. " (" .. format_hash(fnv1a_32(tree_name)) .. ")")
        print(string.format("  func_nodes=%d pointers=%d params=%d",
            tree.func_node_count, tree.pointer_count, #tree.params))
        
        for i, p in ipairs(tree.params) do
            local type_name = "?"
            local base = band(p.type, 0x0F)
            local flags = ""
            
            if band(p.type, TYPE_FLAGS.POINTER) ~= 0 then flags = flags .. " PTR" end
            if band(p.type, TYPE_FLAGS.SURVIVES_RESET) ~= 0 then flags = flags .. " INIT" end
            
            local opcodes = {"INT","UINT","FLOAT","STR","SLOT","OPEN","CLOSE","CALL","ONESHOT","MAIN","PRED"}
            type_name = opcodes[base + 1] or string.format("0x%02X", base)
            
            local detail = ""
            if p.value_type == "func" then
                detail = string.format("idx=%d node=%d %s", p.value, p.node_index, p.func_name or "")
            elseif p.value_type == "hash" then
                detail = string.format("%s \"%s\"", format_hash(p.value), p.str_content or "")
            elseif p.value_type == "slot" then
                detail = string.format("pool=%d slot=%d %s", p.pool_id, p.slot_index, p.slot_name or "")
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
-- EXPORT
--============================================================================

function hash32(s)
    return fnv1a_32(s)
end

print("ChainTree S-Expression DSL v3.0 loaded (stack-based, hash tables)")