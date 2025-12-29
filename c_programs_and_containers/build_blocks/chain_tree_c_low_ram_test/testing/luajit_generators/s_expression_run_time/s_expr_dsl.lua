--============================================================================
-- CHAINTREE S-EXPRESSION DSL
-- Version 2.7 - Two-tier architecture, s_expr_ type prefixes, slotted blackboards
--               Added flatten support for nested call/list helpers
--============================================================================

local ffi = require("ffi")
local bit = require("bit")

local lua_debug = debug

--============================================================================
-- GENSYM - Unique symbol generator
--============================================================================

local _gensym_counter = 0

function gensym(prefix)
    _gensym_counter = _gensym_counter + 1
    return (prefix or "g") .. "_" .. _gensym_counter
end

-- Reset counter (useful for testing)
function gensym_reset()
    _gensym_counter = 0
end

--============================================================================
-- CONSTANTS
--============================================================================

-- Table selector (upper 2 bits of type)
local TABLE_OPCODE   = 0x00
local TABLE_ONESHOT  = 0x40
local TABLE_BOOLEAN  = 0x80
local TABLE_MAIN     = 0xC0
local TABLE_MASK     = 0xC0
local OPCODE_MASK    = 0x3F

-- Built-in opcodes (when table == TABLE_OPCODE)
local OPCODES = {
    pipeline = 0x01,
    ["if"]   = 0x02,
    if_else  = 0x03,
    cond     = 0x04,
    dispatch = 0x05,
    ["and"]  = 0x06,
    ["or"]   = 0x07,
    ["not"]  = 0x08,
    quote    = 0x09,
    dbg      = 0x0A,
    clause   = 0x0B,
    case     = 0x0C,
    ["xor"]  = 0x0D,
    ["nand"] = 0x0E,
    ["nor"]  = 0x0F,
}

local CONTROL_CODES = {
    SE_CONTINUE           = 0,
    SE_HALT               = 1,
    SE_TERMINATE          = 2,
    SE_RESET              = 3,
    SE_DISABLE            = 4,
    SE_FUNCTION_TERMINATE = 5,
    SE_SKIP_CONTINUE      = 6,
    SE_FUNCTION_HALT      = 7,
    SE_FUNCTION_RESET     = 8,
}

-- Export control codes as globals for use in DSL
SE_CONTINUE           = 0
SE_HALT               = 1
SE_TERMINATE          = 2
SE_RESET              = 3
SE_DISABLE            = 4
SE_FUNCTION_TERMINATE = 5
SE_SKIP_CONTINUE      = 6
SE_FUNCTION_HALT      = 7
SE_FUNCTION_RESET     = 8

local NODE_TYPES = {
    QUOTE       = "quote",
    ONESHOT     = "oneshot",
    BOOLEAN     = "boolean",
    MAIN        = "main",
    PIPELINE    = "pipeline",
    IF          = "if",
    IF_ELSE     = "if_else",
    COND        = "cond",
    DISPATCH    = "dispatch",
    DEBUG       = "debug",
    AND         = "and",
    OR          = "or",
    NOT         = "not",
    XOR         = "xor",
    NAND        = "nand",
    NOR         = "nor",
    CLAUSE      = "clause",
    CASE        = "case",
    CONDITION   = "condition",
    ACTION      = "action",
}

-- Parameter types (generic, size determined by 64-bit flag)
local PARAM_INT       = 0x00   -- int32_t or int64_t
local PARAM_UINT      = 0x01   -- uint32_t or uint64_t
local PARAM_FLOAT     = 0x02   -- float or double
local PARAM_STRING    = 0x03   -- string index
local PARAM_MAIN      = 0x04   -- main function index
local PARAM_ONESHOT   = 0x05   -- oneshot function index
local PARAM_PRED      = 0x06   -- predicate function index
local PARAM_OPEN      = 0x07   -- open brace (data list)
local PARAM_CLOSE     = 0x08   -- close brace
local PARAM_OPEN_CALL = 0x09   -- open brace (callable S-expr)
local PARAM_SLOT      = 0x0A   -- slot reference (pool_id + slot_index)

local CONTEXTS = {
    CONTROL_FLOW = "control_flow",
    BOOLEAN      = "boolean",
    CLAUSE_LIST  = "clause_list",
    CASE_LIST    = "case_list",
    CONDITION    = "condition",
    ACTION       = "action",
}

local NO_SIBLING = 0xFFFF
local NO_CHILD   = 0xFFFF

--============================================================================
-- DSL STATE
--============================================================================

local function new_tables()
    return {
        oneshot_fns = {},
        oneshot_map = {},
        boolean_fns = {},
        boolean_map = {},
        main_fns = {},
        main_map = {},
        strings = {},
        string_map = {},
    }
end

local function new_pools()
    return {
        pools = {},      -- pool_name -> { type, id, slot_count }
        slots = {},      -- slot_name -> { pool, index }
        pool_id = 0,
    }
end

local _state = {
    stack = {},
    root = nil,
    test_name = nil,
    tables = new_tables(),
    line = 0,
    is_64bit = false,
    brace_depth = 0,
}

local _module = {
    name = nil,
    trees = {},
    tree_order = {},
    tables = new_tables(),
    pools = new_pools(),
    current_tree = nil,
    is_64bit = false,
}

--============================================================================
-- ERROR HANDLING
--============================================================================

local function get_line()
    local info = lua_debug.getinfo(3, "l")
    if info then
        return info.currentline
    end
    return _state.line
end

local function dsl_error(msg)
    local line = get_line()
    error(string.format("[DSL ERROR] line %d: %s", line, msg), 3)
end

--============================================================================
-- 64-BIT FLAG CONTROL
--============================================================================

function use_64bit(enabled)
    if enabled == nil then
        enabled = true
    end
    _state.is_64bit = enabled
    _module.is_64bit = enabled
end

function use_32bit()
    _state.is_64bit = false
    _module.is_64bit = false
end

--============================================================================
-- STACK OPERATIONS
--============================================================================

local function stack_push(node_type, name, context)
    local node = {
        type = node_type,
        name = name,
        children = {},
        context = context,
    }
    table.insert(_state.stack, {
        type = node_type,
        name = name,
        node = node,
        context = context,
    })
    return node
end

local function stack_pop(expected_type, expected_name)
    if #_state.stack == 0 then
        dsl_error(string.format("unexpected end_%s('%s'), stack is empty", 
                               expected_type, expected_name))
    end
    
    local top = _state.stack[#_state.stack]
    
    if top.type ~= expected_type then
        dsl_error(string.format("end_%s('%s') does not match top of stack: %s('%s')",
                               expected_type, expected_name, top.type, top.name))
    end
    
    if top.name ~= expected_name then
        dsl_error(string.format("end_%s('%s') name mismatch, expected '%s'",
                               expected_type, expected_name, top.name))
    end
    
    table.remove(_state.stack)
    return top.node
end

local function stack_peek()
    if #_state.stack == 0 then
        return nil
    end
    return _state.stack[#_state.stack]
end

local function current_context()
    local top = stack_peek()
    if top then
        return top.context
    end
    return CONTEXTS.CONTROL_FLOW
end

local function current_node()
    local top = stack_peek()
    if top then
        return top.node
    end
    return nil
end

--============================================================================
-- CONTEXT VALIDATION
--============================================================================

local function check_context(allowed, fn_name)
    local ctx = current_context()
    for _, a in ipairs(allowed) do
        if ctx == a then
            return true
        end
    end
    dsl_error(string.format("%s() not valid in context '%s'", fn_name, ctx))
end

local function add_child(child)
    local parent = current_node()
    if parent then
        table.insert(parent.children, child)
    else
        if _state.root then
            dsl_error("multiple root nodes not allowed")
        end
        _state.root = child
    end
end

--============================================================================
-- COMPOSITE NODE HELPER
--============================================================================

local function start_composite(node_type, name, context, init_fn)
    local parent = current_node()
    local node = stack_push(node_type, name, context)
    
    if init_fn then
        init_fn(node)
    end
    
    if parent then
        table.insert(parent.children, node)
    else
        _state.root = node
    end
    
    return node
end

--============================================================================
-- STRING TABLE FUNCTIONS
--============================================================================

local function add_oneshot_fn(s)
    local t = _state.tables
    if t.oneshot_map[s] then
        return t.oneshot_map[s]
    end
    local idx = #t.oneshot_fns
    table.insert(t.oneshot_fns, s)
    t.oneshot_map[s] = idx
    return idx
end

local function add_boolean_fn(s)
    local t = _state.tables
    if t.boolean_map[s] then
        return t.boolean_map[s]
    end
    local idx = #t.boolean_fns
    table.insert(t.boolean_fns, s)
    t.boolean_map[s] = idx
    return idx
end

local function add_main_fn(s)
    local t = _state.tables
    if t.main_map[s] then
        return t.main_map[s]
    end
    local idx = #t.main_fns
    table.insert(t.main_fns, s)
    t.main_map[s] = idx
    return idx
end

local function add_string(s)
    local t = _state.tables
    if t.string_map[s] then
        return t.string_map[s]
    end
    local idx = #t.strings
    table.insert(t.strings, s)
    t.string_map[s] = idx
    return idx
end

--============================================================================
-- POOL / SLOT DEFINITIONS (Slotted Blackboards)
--============================================================================

function defpool(name, ctype)
    if not _module.name then
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
    if not _module.name then
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

function resolve_slot(slot_name)
    if not _module.name then
        error("[DSL ERROR] resolve_slot() must be inside start_module()", 2)
    end
    
    local p = _module.pools
    local slot = p.slots[slot_name]
    if not slot then
        dsl_error("Unknown slot: " .. slot_name)
    end
    
    local pool = p.pools[slot.pool]
    return pool.id, slot.index
end

-- Slot reference parameter
function slot_ref(slot_name)
    return { _param_type = "slot_ref", value = slot_name }
end

--============================================================================
-- PARAMETER TYPE HELPERS (generic names)
--============================================================================

-- Generic int (becomes int32_t or int64_t based on flag)
function int(value)
    return { _param_type = "int", value = value }
end

-- Generic unsigned (becomes uint32_t or uint64_t based on flag)
function uint(value)
    return { _param_type = "uint", value = value }
end

-- Generic float (becomes float or double based on flag)
function flt(value)
    return { _param_type = "float", value = value }
end

-- String parameter
function str(value)
    return { _param_type = "string", value = tostring(value) }
end

-- Function reference parameters
function main_ref(fn_name)
    return { _param_type = "main_ref", value = fn_name }
end

function oneshot_ref(fn_name)
    return { _param_type = "oneshot_ref", value = fn_name }
end

function pred_ref(fn_name)
    return { _param_type = "pred_ref", value = fn_name }
end

-- Brace markers (store index in params array for lookahead)
function open_brace()
    _state.brace_depth = _state.brace_depth + 1
    return { _param_type = "open", value = _state.brace_depth }
end

function close_brace()
    if _state.brace_depth == 0 then
        dsl_error("unbalanced brace: extra close")
    end
    local depth = _state.brace_depth
    _state.brace_depth = _state.brace_depth - 1
    return { _param_type = "close", value = depth }
end

--============================================================================
-- CALLABLE / LIST HELPERS (return marked tables for flattening)
--============================================================================

-- Wrap function call with args in braces (callable S-expr)
function call(fn_ref, ...)
    local result = { open_brace(), fn_ref }
    local args = {...}
    for _, a in ipairs(args) do
        table.insert(result, a)
    end
    table.insert(result, close_brace())
    result._flatten = true  -- Set after building array to avoid index confusion
    return result
end

-- Wrap a list of items in braces (data, not callable)
function list(...)
    local result = { open_brace() }
    local args = {...}
    for _, a in ipairs(args) do
        table.insert(result, a)
    end
    table.insert(result, close_brace())
    result._flatten = true  -- Set after building array to avoid index confusion
    return result
end

-- Type-specific call helpers
-- Note: using p_call not pcall to avoid shadowing Lua's protected call
function p_call(name, ...)  -- predicate call
    return call(pred_ref(name), ...)
end

function m_call(name, ...)  -- main call
    return call(main_ref(name), ...)
end

function o_call(name, ...)  -- oneshot call
    return call(oneshot_ref(name), ...)
end

--============================================================================
-- PARAMETER FLATTENING
--============================================================================

local function flatten_args(args)
    local result = {}
    for _, a in ipairs(args) do
        if type(a) == "table" and a._flatten then
            -- Recursively flatten marked tables (skip _flatten key)
            local inner = {}
            for i, v in ipairs(a) do
                table.insert(inner, v)
            end
            local flattened = flatten_args(inner)
            for _, v in ipairs(flattened) do
                table.insert(result, v)
            end
        else
            table.insert(result, a)
        end
    end
    return result
end

local function encode_param(p)
    if type(p) == "table" and p._param_type then
        local pt = p._param_type
        if pt == "int" then
            return { type = "int", value = p.value }
        elseif pt == "uint" then
            return { type = "uint", value = p.value }
        elseif pt == "float" then
            return { type = "float", value = p.value }
        elseif pt == "string" then
            return { type = "string", value = p.value }
        elseif pt == "main_ref" then
            return { type = "main_ref", value = p.value }
        elseif pt == "oneshot_ref" then
            return { type = "oneshot_ref", value = p.value }
        elseif pt == "pred_ref" then
            return { type = "pred_ref", value = p.value }
        elseif pt == "slot_ref" then
            -- Resolve at encode time
            local pool_id, slot_index = resolve_slot(p.value)
            return { type = "slot_ref", pool_id = pool_id, slot_index = slot_index, name = p.value }
        elseif pt == "open" then
            return { type = "open", value = p.value }
        elseif pt == "close" then
            return { type = "close", value = p.value }
        end
    end
    
    local t = type(p)
    if t == "number" then
        if math.floor(p) == p then
            if p < 0 then
                return { type = "int", value = p }
            else
                return { type = "uint", value = p }
            end
        else
            return { type = "float", value = p }
        end
    elseif t == "string" then
        return { type = "string", value = p }
    else
        dsl_error(string.format("invalid parameter type: %s", t))
    end
end

local function encode_params(...)
    local params = {}
    local args = flatten_args({...})
    for _, p in ipairs(args) do
        table.insert(params, encode_param(p))
    end
    return params
end

--============================================================================
-- TEST/MODULE WRAPPERS
--============================================================================

function start_tree(name)
    if not name or name == "" then
        error("[DSL ERROR] start_tree() requires explicit name", 2)
    end
    
    if not _module.name then
        error("[DSL ERROR] start_tree() must be inside start_module()", 2)
    end
    
    if _module.trees[name] then
        error("[DSL ERROR] tree '" .. name .. "' already defined", 2)
    end
    
    _module.current_tree = name
    
    _state = {
        stack = {},
        root = nil,
        test_name = name,
        tables = _module.tables,
        line = 0,
        is_64bit = _module.is_64bit,
        brace_depth = 0,
    }
    
    return name
end

function end_test(name)
    if type(name) ~= "string" then
        error("[DSL ERROR] end_test() requires string name", 2)
    end
    
    if name ~= _state.test_name then
        dsl_error(string.format("end_test('%s') does not match start_test('%s')",
                               name, _state.test_name))
    end
    
    if #_state.stack > 0 then
        local unclosed = {}
        for _, entry in ipairs(_state.stack) do
            table.insert(unclosed, string.format("%s('%s')", entry.type, entry.name))
        end
        dsl_error(string.format("stack not empty, unclosed: %s", 
                               table.concat(unclosed, ", ")))
    end
    
    if _state.brace_depth ~= 0 then
        dsl_error(string.format("unbalanced braces: %d unclosed", _state.brace_depth))
    end
    
    if not _state.root then
        dsl_error("no root node defined")
    end
    
    return TreeGenerator.new(name, _state.root, _state.tables, _state.is_64bit)
end

function start_module(name, opts)
    prefix = prefix or "module"
    
    
    opts = opts or {}
    
    _module = {
        name = name,
        trees = {},
        tree_order = {},
        tables = new_tables(),
        pools = new_pools(),
        current_tree = nil,
        is_64bit = opts.is_64bit or false,
    }
    
    return name
end

function end_tree(name)
    if type(name) ~= "string" then
        error("[DSL ERROR] end_tree() requires string name", 2)
    end
    
    if name ~= _module.current_tree then
        dsl_error(string.format("end_tree('%s') does not match start_tree('%s')",
                               name, _module.current_tree))
    end
    
    if #_state.stack > 0 then
        local unclosed = {}
        for _, entry in ipairs(_state.stack) do
            table.insert(unclosed, string.format("%s('%s')", entry.type, entry.name))
        end
        dsl_error(string.format("stack not empty, unclosed: %s", 
                               table.concat(unclosed, ", ")))
    end
    
    if _state.brace_depth ~= 0 then
        dsl_error(string.format("unbalanced braces: %d unclosed", _state.brace_depth))
    end
    
    if not _state.root then
        dsl_error("no root node defined in tree '" .. name .. "'")
    end
    
    _module.trees[name] = _state.root
    table.insert(_module.tree_order, name)
    _module.current_tree = nil
    
    return name
end

function end_module(name)
    if type(name) ~= "string" then
        error("[DSL ERROR] end_module() requires string name", 2)
    end
    
    if name ~= _module.name then
        error(string.format("[DSL ERROR] end_module('%s') does not match start_module('%s')",
                           name, _module.name), 2)
    end
    
    if _module.current_tree then
        error("[DSL ERROR] end_module() called with unclosed tree: " .. _module.current_tree, 2)
    end
    
    if #_module.tree_order == 0 then
        error("[DSL ERROR] module has no trees", 2)
    end
    
    return ModuleGenerator.new(
        _module.name,
        _module.trees,
        _module.tree_order,
        _module.tables,
        _module.pools,
        _module.is_64bit
    )
end

--============================================================================
-- LEAF FUNCTIONS
--============================================================================

function oneshot(fn_name, ...)
    if type(fn_name) ~= "string" then
        dsl_error("oneshot() requires function name as first argument")
    end
    check_context({CONTEXTS.CONTROL_FLOW, CONTEXTS.ACTION}, "oneshot")
    
    local node = {
        type = NODE_TYPES.ONESHOT,
        fn_name = fn_name,
        params = encode_params(...),
        survives_reset = false,
    }
    add_child(node)
end

function init_once(fn_name, ...)
    if type(fn_name) ~= "string" then
        dsl_error("init_once() requires function name as first argument")
    end
    check_context({CONTEXTS.CONTROL_FLOW, CONTEXTS.ACTION}, "init_once")
    
    local node = {
        type = NODE_TYPES.ONESHOT,
        fn_name = fn_name,
        params = encode_params(...),
        survives_reset = true,  -- Bit 1 of reserved field
    }
    add_child(node)
end

function main(fn_name, ...)
    if type(fn_name) ~= "string" then
        dsl_error("main() requires function name as first argument")
    end
    check_context({CONTEXTS.CONTROL_FLOW, CONTEXTS.ACTION}, "main")
    
    local node = {
        type = NODE_TYPES.MAIN,
        fn_name = fn_name,
        params = encode_params(...),
    }
    add_child(node)
end

function bool_fn(fn_name, ...)
    if type(fn_name) ~= "string" then
        dsl_error("bool_fn() requires function name as first argument")
    end
    check_context({CONTEXTS.BOOLEAN, CONTEXTS.CONDITION}, "bool_fn")
    
    local node = {
        type = NODE_TYPES.BOOLEAN,
        fn_name = fn_name,
        params = encode_params(...),
    }
    add_child(node)
end

function quote(code)
    if type(code) ~= "string" then
        dsl_error("quote() requires control code string")
    end
    if not CONTROL_CODES[code] then
        dsl_error(string.format("quote() invalid control code: '%s'", code))
    end
    check_context({CONTEXTS.CONTROL_FLOW, CONTEXTS.ACTION}, "quote")
    
    local node = {
        type = NODE_TYPES.QUOTE,
        value = code,
    }
    add_child(node)
end

--============================================================================
-- COMPOSITE NODES - All return gensym'd name
--============================================================================

function pipeline(prefix)
    prefix = prefix or "pipeline"
    local name = gensym(prefix)
    check_context({CONTEXTS.CONTROL_FLOW, CONTEXTS.ACTION}, "pipeline")
    start_composite(NODE_TYPES.PIPELINE, name, CONTEXTS.CONTROL_FLOW)
    return name
end

function end_pipeline(name)
    if type(name) ~= "string" then
        dsl_error("end_pipeline() requires name")
    end
    
    local node = stack_pop(NODE_TYPES.PIPELINE, name)
    
    if #node.children == 0 then
        dsl_error(string.format("pipeline('%s') has no children", name))
    end
end

function if_then(prefix)
    prefix = prefix or "if"
    local name = gensym(prefix)
    check_context({CONTEXTS.CONTROL_FLOW, CONTEXTS.ACTION}, "if_then")
    start_composite(NODE_TYPES.IF, name, CONTEXTS.CONDITION, function(n)
        n.condition = nil
        n.then_action = nil
    end)
    return name
end

function end_if_then(name)
    if type(name) ~= "string" then
        dsl_error("end_if_then() requires name")
    end
    
    local node = stack_pop(NODE_TYPES.IF, name)
    
    if not node.condition then
        dsl_error(string.format("if_then('%s') missing condition", name))
    end
    if not node.then_action then
        dsl_error(string.format("if_then('%s') missing action", name))
    end
end

function if_then_else(prefix)
    prefix = prefix or "if_else"
    local name = gensym(prefix)
    check_context({CONTEXTS.CONTROL_FLOW, CONTEXTS.ACTION}, "if_then_else")
    start_composite(NODE_TYPES.IF_ELSE, name, CONTEXTS.CONDITION, function(n)
        n.condition = nil
        n.then_action = nil
        n.else_action = nil
    end)
    return name
end

function end_if_then_else(name)
    if type(name) ~= "string" then
        dsl_error("end_if_then_else() requires name")
    end
    
    local node = stack_pop(NODE_TYPES.IF_ELSE, name)
    
    if not node.condition then
        dsl_error(string.format("if_then_else('%s') missing condition", name))
    end
    if not node.then_action then
        dsl_error(string.format("if_then_else('%s') missing then action", name))
    end
    if not node.else_action then
        dsl_error(string.format("if_then_else('%s') missing else action", name))
    end
end

function cond(prefix)
    prefix = prefix or "cond"
    local name = gensym(prefix)
    check_context({CONTEXTS.CONTROL_FLOW, CONTEXTS.ACTION}, "cond")
    start_composite(NODE_TYPES.COND, name, CONTEXTS.CLAUSE_LIST, function(n)
        n.clauses = {}
    end)
    return name
end

function end_cond(name)
    if type(name) ~= "string" then
        dsl_error("end_cond() requires name")
    end
    
    local node = stack_pop(NODE_TYPES.COND, name)
    
    if #node.clauses == 0 then
        dsl_error(string.format("cond('%s') has no clauses", name))
    end
end

function clause(prefix)
    prefix = prefix or "clause"
    local name = gensym(prefix)
    check_context({CONTEXTS.CLAUSE_LIST}, "clause")
    
    local node = stack_push(NODE_TYPES.CLAUSE, name, CONTEXTS.CONDITION)
    node.condition = nil
    node.action = nil
    node.is_default = false
    return name
end

function end_clause(name)
    if type(name) ~= "string" then
        dsl_error("end_clause() requires name")
    end
    
    local node = stack_pop(NODE_TYPES.CLAUSE, name)
    
    if not node.condition then
        dsl_error(string.format("clause('%s') missing condition", name))
    end
    if not node.action then
        dsl_error(string.format("clause('%s') missing action", name))
    end
    
    local parent = current_node()
    table.insert(parent.clauses, node)
end

function default_clause(prefix)
    prefix = prefix or "default_clause"
    local name = gensym(prefix)
    check_context({CONTEXTS.CLAUSE_LIST}, "default_clause")
    
    local node = stack_push(NODE_TYPES.CLAUSE, name, CONTEXTS.ACTION)
    node.condition = nil
    node.action = nil
    node.is_default = true
    return name
end

function end_default_clause(name)
    if type(name) ~= "string" then
        dsl_error("end_default_clause() requires name")
    end
    
    local node = stack_pop(NODE_TYPES.CLAUSE, name)
    
    if not node.action then
        dsl_error(string.format("default_clause('%s') missing action", name))
    end
    
    local parent = current_node()
    table.insert(parent.clauses, node)
end

function dispatch(key, prefix)
    if type(key) ~= "string" then
        dsl_error("dispatch() requires key parameter as first argument")
    end
    prefix = prefix or "dispatch"
    local name = gensym(prefix)
    check_context({CONTEXTS.CONTROL_FLOW, CONTEXTS.ACTION}, "dispatch")
    start_composite(NODE_TYPES.DISPATCH, name, CONTEXTS.CASE_LIST, function(n)
        n.key = key
        n.cases = {}
    end)
    return name
end

function end_dispatch(name)
    if type(name) ~= "string" then
        dsl_error("end_dispatch() requires name")
    end
    
    local node = stack_pop(NODE_TYPES.DISPATCH, name)
    
    if #node.cases == 0 then
        dsl_error(string.format("dispatch('%s') has no cases", name))
    end
end

function case(pattern, prefix)
    if pattern == nil then
        dsl_error("case() requires pattern as first argument")
    end
    prefix = prefix or "case"
    local name = gensym(prefix)
    check_context({CONTEXTS.CASE_LIST}, "case")
    
    local node = stack_push(NODE_TYPES.CASE, name, CONTEXTS.ACTION)
    node.pattern = pattern
    node.action = nil
    node.is_default = false
    return name
end

function end_case(name)
    if type(name) ~= "string" then
        dsl_error("end_case() requires name")
    end
    
    local node = stack_pop(NODE_TYPES.CASE, name)
    
    if not node.action then
        dsl_error(string.format("case('%s') missing action", name))
    end
    
    local parent = current_node()
    table.insert(parent.cases, node)
end

function default_case(prefix)
    prefix = prefix or "default_case"
    local name = gensym(prefix)
    check_context({CONTEXTS.CASE_LIST}, "default_case")
    
    local node = stack_push(NODE_TYPES.CASE, name, CONTEXTS.ACTION)
    node.pattern = nil
    node.action = nil
    node.is_default = true
    return name
end

function end_default_case(name)
    if type(name) ~= "string" then
        dsl_error("end_default_case() requires name")
    end
    
    local node = stack_pop(NODE_TYPES.CASE, name)
    
    if not node.action then
        dsl_error(string.format("default_case('%s') missing action", name))
    end
    
    local parent = current_node()
    table.insert(parent.cases, node)
end

function condition(prefix)
    prefix = prefix or "cond"
    local name = gensym(prefix)
    check_context({CONTEXTS.CONDITION}, "condition")
    
    local node = stack_push(NODE_TYPES.CONDITION, name, CONTEXTS.BOOLEAN)
    node.expr = nil
    return name
end

function end_condition(name)
    if type(name) ~= "string" then
        dsl_error("end_condition() requires name")
    end
    
    local node = stack_pop(NODE_TYPES.CONDITION, name)
    
    if #node.children == 0 then
        dsl_error(string.format("condition('%s') has no boolean expression", name))
    end
    if #node.children > 1 then
        dsl_error(string.format("condition('%s') has multiple expressions", name))
    end
    
    local parent = current_node()
    parent.condition = node.children[1]
    
    local top = stack_peek()
    if top then
        top.context = CONTEXTS.ACTION
    end
end

function action(prefix)
    prefix = prefix or "action"
    local name = gensym(prefix)
    check_context({CONTEXTS.ACTION, CONTEXTS.CONDITION}, "action")
    
    stack_push(NODE_TYPES.ACTION, name, CONTEXTS.CONTROL_FLOW)
    return name
end

function end_action(name)
    if type(name) ~= "string" then
        dsl_error("end_action() requires name")
    end
    
    local node = stack_pop(NODE_TYPES.ACTION, name)
    
    if #node.children == 0 then
        dsl_error(string.format("action('%s') has no content", name))
    end
    if #node.children > 1 then
        dsl_error(string.format("action('%s') has multiple children", name))
    end
    
    local parent = current_node()
    local parent_type = parent.type
    
    if parent_type == NODE_TYPES.CLAUSE or parent_type == NODE_TYPES.CASE then
        parent.action = node.children[1]
    elseif parent_type == NODE_TYPES.IF then
        parent.then_action = node.children[1]
    elseif parent_type == NODE_TYPES.IF_ELSE then
        if not parent.then_action then
            parent.then_action = node.children[1]
        else
            parent.else_action = node.children[1]
        end
    end
end

function bool_and(prefix)
    prefix = prefix or "and"
    local name = gensym(prefix)
    check_context({CONTEXTS.BOOLEAN, CONTEXTS.CONDITION}, "bool_and")
    start_composite(NODE_TYPES.AND, name, CONTEXTS.BOOLEAN)
    return name
end

function end_bool_and(name)
    if type(name) ~= "string" then
        dsl_error("end_bool_and() requires name")
    end
    
    local node = stack_pop(NODE_TYPES.AND, name)
    
    if #node.children < 2 then
        dsl_error(string.format("bool_and('%s') requires at least 2 children", name))
    end
end

function bool_or(prefix)
    prefix = prefix or "or"
    local name = gensym(prefix)
    check_context({CONTEXTS.BOOLEAN, CONTEXTS.CONDITION}, "bool_or")
    start_composite(NODE_TYPES.OR, name, CONTEXTS.BOOLEAN)
    return name
end

function end_bool_or(name)
    if type(name) ~= "string" then
        dsl_error("end_bool_or() requires name")
    end
    
    local node = stack_pop(NODE_TYPES.OR, name)
    
    if #node.children < 2 then
        dsl_error(string.format("bool_or('%s') requires at least 2 children", name))
    end
end

function bool_not(prefix)
    prefix = prefix or "not"
    local name = gensym(prefix)
    check_context({CONTEXTS.BOOLEAN, CONTEXTS.CONDITION}, "bool_not")
    start_composite(NODE_TYPES.NOT, name, CONTEXTS.BOOLEAN)
    return name
end

function end_bool_not(name)
    if type(name) ~= "string" then
        dsl_error("end_bool_not() requires name")
    end
    
    local node = stack_pop(NODE_TYPES.NOT, name)
    
    if #node.children ~= 1 then
        dsl_error(string.format("bool_not('%s') requires exactly 1 child", name))
    end
end

function bool_xor(prefix)
    prefix = prefix or "xor"
    local name = gensym(prefix)
    check_context({CONTEXTS.BOOLEAN, CONTEXTS.CONDITION}, "bool_xor")
    start_composite(NODE_TYPES.XOR, name, CONTEXTS.BOOLEAN)
    return name
end

function end_bool_xor(name)
    if type(name) ~= "string" then
        dsl_error("end_bool_xor() requires name")
    end
    
    local node = stack_pop(NODE_TYPES.XOR, name)
    
    if #node.children < 2 then
        dsl_error(string.format("bool_xor('%s') requires at least 2 children", name))
    end
end

function bool_nand(prefix)
    prefix = prefix or "nand"
    local name = gensym(prefix)
    check_context({CONTEXTS.BOOLEAN, CONTEXTS.CONDITION}, "bool_nand")
    start_composite(NODE_TYPES.NAND, name, CONTEXTS.BOOLEAN)
    return name
end

function end_bool_nand(name)
    if type(name) ~= "string" then
        dsl_error("end_bool_nand() requires name")
    end
    
    local node = stack_pop(NODE_TYPES.NAND, name)
    
    if #node.children < 2 then
        dsl_error(string.format("bool_nand('%s') requires at least 2 children", name))
    end
end

function bool_nor(prefix)
    prefix = prefix or "nor"
    local name = gensym(prefix)
    check_context({CONTEXTS.BOOLEAN, CONTEXTS.CONDITION}, "bool_nor")
    start_composite(NODE_TYPES.NOR, name, CONTEXTS.BOOLEAN)
    return name
end

function end_bool_nor(name)
    if type(name) ~= "string" then
        dsl_error("end_bool_nor() requires name")
    end
    
    local node = stack_pop(NODE_TYPES.NOR, name)
    
    if #node.children < 2 then
        dsl_error(string.format("bool_nor('%s') requires at least 2 children", name))
    end
end

function dbg(message, prefix)
    if type(message) ~= "string" then
        dsl_error("dbg() requires message string as first argument")
    end
    prefix = prefix or "dbg"
    local name = gensym(prefix)
    check_context({CONTEXTS.CONTROL_FLOW, CONTEXTS.ACTION}, "dbg")
    start_composite(NODE_TYPES.DEBUG, name, CONTEXTS.CONTROL_FLOW, function(n)
        n.message = message
    end)
    return name
end

function end_dbg(name)
    if type(name) ~= "string" then
        dsl_error("end_dbg() requires name")
    end
    
    local node = stack_pop(NODE_TYPES.DEBUG, name)
    
    if #node.children ~= 1 then
        dsl_error(string.format("dbg('%s') requires exactly 1 child", name))
    end
    
    node.child = node.children[1]
    node.children = nil
end

--============================================================================
-- TREE GENERATOR
--============================================================================

TreeGenerator = {}
TreeGenerator.__index = TreeGenerator

function TreeGenerator.new(name, root, tables, is_64bit)
    local self = setmetatable({}, TreeGenerator)
    self.name = name
    self.root = root
    self.tables = tables
    self.is_64bit = is_64bit or false
    self.nodes = {}
    self.params = {}
    self.node_count = 0
    self.compiled = false
    return self
end

-- String table accessors
function TreeGenerator:add_oneshot_fn(s)
    local t = self.tables
    if t.oneshot_map[s] then return t.oneshot_map[s] end
    local idx = #t.oneshot_fns
    table.insert(t.oneshot_fns, s)
    t.oneshot_map[s] = idx
    return idx
end

function TreeGenerator:add_boolean_fn(s)
    local t = self.tables
    if t.boolean_map[s] then return t.boolean_map[s] end
    local idx = #t.boolean_fns
    table.insert(t.boolean_fns, s)
    t.boolean_map[s] = idx
    return idx
end

function TreeGenerator:add_main_fn(s)
    local t = self.tables
    if t.main_map[s] then return t.main_map[s] end
    local idx = #t.main_fns
    table.insert(t.main_fns, s)
    t.main_map[s] = idx
    return idx
end

function TreeGenerator:add_string(s)
    local t = self.tables
    if t.string_map[s] then return t.string_map[s] end
    local idx = #t.strings
    table.insert(t.strings, s)
    t.string_map[s] = idx
    return idx
end

--============================================================================
-- PASS 1: Assign node indices
--============================================================================

function TreeGenerator:assign_indices(node, index)
    node._node_index = index
    local next_index = index + 1
    
    local children = self:get_node_children(node)
    
    for _, child in ipairs(children) do
        next_index = self:assign_indices(child, next_index)
    end
    
    return next_index
end

function TreeGenerator:get_node_children(node)
    local t = node.type
    local children = {}
    
    if t == NODE_TYPES.PIPELINE then
        children = node.children
        
    elseif t == NODE_TYPES.IF then
        table.insert(children, node.condition)
        table.insert(children, node.then_action)
        
    elseif t == NODE_TYPES.IF_ELSE then
        table.insert(children, node.condition)
        table.insert(children, node.then_action)
        table.insert(children, node.else_action)
        
    elseif t == NODE_TYPES.COND then
        -- Reuse existing clause nodes (created once, then reused)
        if not node._clause_nodes then
            node._clause_nodes = {}
            for _, cl in ipairs(node.clauses) do
                local clause_node = {
                    type = NODE_TYPES.CLAUSE,
                    condition = cl.condition,
                    action = cl.action,
                    is_default = cl.is_default,
                }
                table.insert(node._clause_nodes, clause_node)
            end
        end
        for _, cn in ipairs(node._clause_nodes) do
            table.insert(children, cn)
        end
        
    elseif t == NODE_TYPES.CLAUSE then
        if not node.is_default and node.condition then
            table.insert(children, node.condition)
        end
        table.insert(children, node.action)
        
    elseif t == NODE_TYPES.DISPATCH then
        -- Reuse existing case nodes (created once, then reused)
        if not node._case_nodes then
            node._case_nodes = {}
            for _, cs in ipairs(node.cases) do
                local case_node = {
                    type = NODE_TYPES.CASE,
                    pattern = cs.pattern,
                    action = cs.action,
                    is_default = cs.is_default,
                }
                table.insert(node._case_nodes, case_node)
            end
        end
        for _, cn in ipairs(node._case_nodes) do
            table.insert(children, cn)
        end
        
    elseif t == NODE_TYPES.CASE then
        table.insert(children, node.action)
        
    elseif t == NODE_TYPES.AND or t == NODE_TYPES.OR then
        children = node.children
        
    elseif t == NODE_TYPES.XOR or t == NODE_TYPES.NAND or t == NODE_TYPES.NOR then
        children = node.children
        
    elseif t == NODE_TYPES.NOT then
        children = node.children
        
    elseif t == NODE_TYPES.DEBUG then
        table.insert(children, node.child)
    end
    
    return children
end

--============================================================================
-- PASS 2: Emit flat node array
--============================================================================

function TreeGenerator:emit_nodes(node, next_sibling_index)
    local t = node.type
    local children = self:get_node_children(node)
    
    local n = {
        type = 0,
        child_count = #children,
        node_index = node._node_index,
        first_child = NO_CHILD,
        next_sibling = next_sibling_index or NO_SIBLING,
        fn_index = 0,
        param_offset = #self.params,
        param_count = 0,
        is_default = false,
    }
    
    if t == NODE_TYPES.QUOTE then
        n.type = TABLE_OPCODE + OPCODES.quote
        n.fn_index = CONTROL_CODES[node.value]
        
    elseif t == NODE_TYPES.ONESHOT then
        n.type = TABLE_ONESHOT
        n.fn_index = self:add_oneshot_fn(node.fn_name)
        self:emit_params(node.params or {})
        n.param_count = #(node.params or {})
        -- Bit 1 (0x02) = survives reset (init_once vs oneshot)
        if node.survives_reset then
            n.is_default = 2  -- becomes reserved field in C
        end
        
    elseif t == NODE_TYPES.BOOLEAN then
        n.type = TABLE_BOOLEAN
        n.fn_index = self:add_boolean_fn(node.fn_name)
        self:emit_params(node.params or {})
        n.param_count = #(node.params or {})
        
    elseif t == NODE_TYPES.MAIN then
        n.type = TABLE_MAIN
        n.fn_index = self:add_main_fn(node.fn_name)
        self:emit_params(node.params or {})
        n.param_count = #(node.params or {})
        
    elseif t == NODE_TYPES.PIPELINE then
        n.type = TABLE_OPCODE + OPCODES.pipeline
        
    elseif t == NODE_TYPES.IF then
        n.type = TABLE_OPCODE + OPCODES["if"]
        
    elseif t == NODE_TYPES.IF_ELSE then
        n.type = TABLE_OPCODE + OPCODES.if_else
        
    elseif t == NODE_TYPES.COND then
        n.type = TABLE_OPCODE + OPCODES.cond
        
    elseif t == NODE_TYPES.CLAUSE then
        n.type = TABLE_OPCODE + OPCODES.clause
        n.is_default = node.is_default
        
    elseif t == NODE_TYPES.DISPATCH then
        n.type = TABLE_OPCODE + OPCODES.dispatch
        local key_idx = self:add_string(node.key)
        table.insert(self.params, { type = PARAM_STRING, value = key_idx })
        n.param_count = 1
        
    elseif t == NODE_TYPES.CASE then
        n.type = TABLE_OPCODE + OPCODES.case
        n.is_default = node.is_default
        if not node.is_default then
            if type(node.pattern) == "table" then
                for _, p in ipairs(node.pattern) do
                    local pidx = self:add_string(p)
                    table.insert(self.params, { type = PARAM_STRING, value = pidx })
                end
                n.param_count = #node.pattern
            else
                local pidx = self:add_string(node.pattern)
                table.insert(self.params, { type = PARAM_STRING, value = pidx })
                n.param_count = 1
            end
        end
        
    elseif t == NODE_TYPES.AND then
        n.type = TABLE_OPCODE + OPCODES["and"]
        
    elseif t == NODE_TYPES.OR then
        n.type = TABLE_OPCODE + OPCODES["or"]
        
    elseif t == NODE_TYPES.NOT then
        n.type = TABLE_OPCODE + OPCODES["not"]
        
    elseif t == NODE_TYPES.XOR then
        n.type = TABLE_OPCODE + OPCODES["xor"]
        
    elseif t == NODE_TYPES.NAND then
        n.type = TABLE_OPCODE + OPCODES["nand"]
        
    elseif t == NODE_TYPES.NOR then
        n.type = TABLE_OPCODE + OPCODES["nor"]
        
    elseif t == NODE_TYPES.DEBUG then
        n.type = TABLE_OPCODE + OPCODES.dbg
        local msg_idx = self:add_string(node.message)
        table.insert(self.params, { type = PARAM_STRING, value = msg_idx })
        n.param_count = 1
    end
    
    if #children > 0 then
        n.first_child = children[1]._node_index
    end
    
    table.insert(self.nodes, n)
    
    for i, child in ipairs(children) do
        local child_next_sibling = NO_SIBLING
        if i < #children then
            child_next_sibling = children[i + 1]._node_index
        end
        self:emit_nodes(child, child_next_sibling)
    end
end

function TreeGenerator:emit_params(params)
    -- Two-pass: first emit, then patch brace indices
    local open_stack = {}   -- stack of {param_index} for open braces
    local patch_list = {}   -- {open_idx, close_idx} pairs
    local base_offset = #self.params
    
    -- First pass: emit all params
    for i, p in ipairs(params) do
        local pt = {
            type = PARAM_INT,
            value = 0,
        }
        
        if p.type == "int" then
            pt.type = PARAM_INT
            pt.value = p.value
        elseif p.type == "uint" then
            pt.type = PARAM_UINT
            pt.value = p.value
        elseif p.type == "float" then
            pt.type = PARAM_FLOAT
            pt.value = p.value
        elseif p.type == "string" then
            pt.type = PARAM_STRING
            pt.value = self:add_string(p.value)
        elseif p.type == "main_ref" then
            pt.type = PARAM_MAIN
            pt.value = self:add_main_fn(p.value)
        elseif p.type == "oneshot_ref" then
            pt.type = PARAM_ONESHOT
            pt.value = self:add_oneshot_fn(p.value)
        elseif p.type == "pred_ref" then
            pt.type = PARAM_PRED
            pt.value = self:add_boolean_fn(p.value)
        elseif p.type == "slot_ref" then
            pt.type = PARAM_SLOT
            pt.pool_id = p.pool_id
            pt.slot_index = p.slot_index
            pt.name = p.name
        elseif p.type == "open" then
            -- Check if next param is a function type
            local next_p = params[i + 1]
            local is_callable = false
            if next_p then
                local nt = next_p.type
                if nt == "main_ref" or nt == "oneshot_ref" or nt == "pred_ref" then
                    is_callable = true
                end
            end
            
            if is_callable then
                pt.type = PARAM_OPEN_CALL
            else
                pt.type = PARAM_OPEN
            end
            pt.value = 0  -- placeholder, patched later
            
            -- Push current index onto stack (relative to this node's params)
            table.insert(open_stack, #self.params - base_offset)
            
        elseif p.type == "close" then
            pt.type = PARAM_CLOSE
            -- Pop matching open
            local open_rel_idx = table.remove(open_stack)
            local close_rel_idx = #self.params - base_offset
            -- Record for patching - store RELATIVE OFFSET (close - open)
            table.insert(patch_list, {open_idx = open_rel_idx, close_idx = close_rel_idx})
            -- Close stores offset back to open (relative)
            pt.value = close_rel_idx - open_rel_idx
        end
        
        table.insert(self.params, pt)
    end
    
    -- Second pass: patch open braces with RELATIVE OFFSET to close
    for _, patch in ipairs(patch_list) do
        local abs_open_idx = base_offset + patch.open_idx + 1  -- +1 for Lua 1-indexing
        -- Store relative offset: close_idx - open_idx
        self.params[abs_open_idx].value = patch.close_idx - patch.open_idx
    end
end

--============================================================================
-- COMPILE
--============================================================================

function TreeGenerator:compile()
    if self.compiled then
        return
    end
    
    self.node_count = self:assign_indices(self.root, 0)
    self:emit_nodes(self.root, NO_SIBLING)
    
    self.compiled = true
end

function TreeGenerator:get_node_count()
    self:compile()
    return self.node_count
end

function TreeGenerator:get_nodes()
    self:compile()
    return self.nodes
end

function TreeGenerator:get_params()
    self:compile()
    return self.params
end

--============================================================================
-- MODULE GENERATOR
--============================================================================

ModuleGenerator = {}
ModuleGenerator.__index = ModuleGenerator

function ModuleGenerator.new(name, trees, tree_order, tables, pools, is_64bit)
    local self = setmetatable({}, ModuleGenerator)
    self.name = name
    self.trees = trees
    self.tree_order = tree_order
    self.tables = tables
    self.pools = pools
    self.is_64bit = is_64bit or false
    self.tree_generators = {}
    self.max_node_count = 0
    self.compiled = false
    return self
end

function ModuleGenerator:compile()
    if self.compiled then
        return
    end
    
    for _, tree_name in ipairs(self.tree_order) do
        local gen = TreeGenerator.new(tree_name, self.trees[tree_name], self.tables, self.is_64bit)
        gen:compile()
        self.tree_generators[tree_name] = gen
        
        local count = gen:get_node_count()
        if count > self.max_node_count then
            self.max_node_count = count
        end
    end
    
    self.compiled = true
end

function ModuleGenerator:get_max_node_count()
    self:compile()
    return self.max_node_count
end

--============================================================================
-- POOLS HEADER OUTPUT
--============================================================================

function ModuleGenerator:to_pools_header(base_name)
    self:compile()
    
    local lines = {}
    local guard = string.upper(base_name) .. "_POOLS_H"
    local prefix = base_name
    local p = self.pools
    
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "// " .. base_name .. "_pools.h")
    table.insert(lines, "// Generated by ChainTree S-Expression DSL v2.7")
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
    table.insert(lines, '#include "pool_types.h"')
    table.insert(lines, "")
    
    -- Order pools by ID
    local ordered_pools = {}
    for name, pool in pairs(p.pools) do
        ordered_pools[pool.id + 1] = { name = name, pool = pool }
    end
    
    -- Pool IDs
    table.insert(lines, "// Pool IDs")
    for _, entry in ipairs(ordered_pools) do
        table.insert(lines, string.format(
            "#define POOL_%s %d",
            string.upper(entry.name), entry.pool.id
        ))
    end
    table.insert(lines, "")
    
    -- Pool sizes
    table.insert(lines, "// Pool sizes")
    for _, entry in ipairs(ordered_pools) do
        table.insert(lines, string.format(
            "#define %s_POOL_SIZE %d",
            string.upper(entry.name), entry.pool.slot_count
        ))
    end
    table.insert(lines, "")
    
    -- Slot defines (SLOT_X expands to POOL_Y, index)
    table.insert(lines, "// Slot defines (pool_id, slot_index)")
    for slot_name, slot in pairs(p.slots) do
        table.insert(lines, string.format(
            "#define SLOT_%s POOL_%s, %d",
            string.upper(slot_name),
            string.upper(slot.pool),
            slot.index
        ))
    end
    table.insert(lines, "")
    
    -- Pool count
    table.insert(lines, string.format("#define POOL_COUNT %d", p.pool_id))
    table.insert(lines, "")
    
    -- Extern declarations
    table.insert(lines, "// Pool table (defined in .c)")
    if p.pool_id > 0 then
        table.insert(lines, "extern void* pool_table[POOL_COUNT];")
    else
        table.insert(lines, "// No pools defined")
    end
    table.insert(lines, "")
    
    table.insert(lines, "#ifdef __cplusplus")
    table.insert(lines, "}")
    table.insert(lines, "#endif")
    table.insert(lines, "")
    table.insert(lines, "#endif // " .. guard)
    
    return table.concat(lines, "\n")
end

--============================================================================
-- POOLS SOURCE OUTPUT
--============================================================================

function ModuleGenerator:to_pools_source(base_name)
    self:compile()
    
    local lines = {}
    local prefix = base_name
    local p = self.pools
    
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "// " .. base_name .. "_pools.c")
    table.insert(lines, "// Generated by ChainTree S-Expression DSL v2.7")
    table.insert(lines, "// DO NOT EDIT")
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "")
    table.insert(lines, '#include "pool_types.h"')
    table.insert(lines, '#include "' .. base_name .. '_pools.h"')
    table.insert(lines, "")
    
    -- Order pools by ID
    local ordered_pools = {}
    for name, pool in pairs(p.pools) do
        ordered_pools[pool.id + 1] = { name = name, pool = pool }
    end
    
    if p.pool_id > 0 then
        -- Pool arrays
        table.insert(lines, "// Pool arrays")
        for _, entry in ipairs(ordered_pools) do
            if entry.pool.slot_count > 0 then
                table.insert(lines, string.format(
                    "%s %s_pool[%s_POOL_SIZE];",
                    entry.pool.type,
                    entry.name,
                    string.upper(entry.name)
                ))
            else
                table.insert(lines, string.format(
                    "%s %s_pool[1];  // placeholder, no slots defined",
                    entry.pool.type,
                    entry.name
                ))
            end
        end
        table.insert(lines, "")
        
        -- Pool table
        table.insert(lines, "// Pool table")
        table.insert(lines, "void* pool_table[POOL_COUNT] = {")
        for _, entry in ipairs(ordered_pools) do
            table.insert(lines, string.format("    %s_pool,", entry.name))
        end
        table.insert(lines, "};")
    else
        table.insert(lines, "// No pools defined")
    end
    
    return table.concat(lines, "\n")
end

--============================================================================
-- C HEADER OUTPUT
--============================================================================

local function escape_string(s)
    local escaped = string.gsub(s, '\\', '\\\\')
    escaped = string.gsub(escaped, '"', '\\"')
    return escaped
end

local function emit_string_array(lines, name, arr)
    if #arr == 0 then
        table.insert(lines, "static const char* const " .. name .. "[] = { NULL };")
    else
        table.insert(lines, "static const char* const " .. name .. "[] = {")
        for _, s in ipairs(arr) do
            table.insert(lines, '    "' .. escape_string(s) .. '",')
        end
        table.insert(lines, "};")
    end
    table.insert(lines, "#define " .. string.upper(name) .. "_COUNT " .. #arr)
end

function ModuleGenerator:to_c_header(base_name)
    self:compile()
    
    local lines = {}
    local guard = string.upper(base_name) .. "_MODULE_H"
    local prefix = base_name
    
    -- Type names based on 64-bit flag
    local int_type = self.is_64bit and "int64_t" or "int32_t"
    local uint_type = self.is_64bit and "uint64_t" or "uint32_t"
    local float_type = self.is_64bit and "double" or "float"
    local int_suffix = self.is_64bit and "LL" or ""
    local uint_suffix = self.is_64bit and "ULL" or "U"
    local float_suffix = self.is_64bit and "" or "f"
    
    -- Header
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "// " .. base_name .. "_module.h")
    table.insert(lines, "// Generated by ChainTree S-Expression DSL v2.7")
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
    
    -- 64-bit flag defines (MODULE_IS_64BIT required by s_engine_types.h)
    table.insert(lines, "// Size configuration")
    table.insert(lines, "#define MODULE_IS_64BIT " .. (self.is_64bit and "1" or "0"))
    table.insert(lines, "#define " .. string.upper(prefix) .. "_IS_64BIT " .. (self.is_64bit and "1" or "0"))
    table.insert(lines, "")
    
    -- Type aliases (CT_TYPES_DEFINED required by s_engine_types.h)
    table.insert(lines, "// Type aliases (based on 64-bit flag)")
    table.insert(lines, "typedef " .. int_type .. " ct_int_t;")
    table.insert(lines, "typedef " .. uint_type .. " ct_uint_t;")
    table.insert(lines, "typedef " .. float_type .. " ct_float_t;")
    table.insert(lines, "#define CT_TYPES_DEFINED 1")
    table.insert(lines, "")
    
    -- Include engine types (defines s_expr_param_t, s_expr_node_t, S_EXPR_PARAM_* constants, etc.)
    table.insert(lines, '#include "s_engine_types.h"')
    table.insert(lines, "")
    
    -- Include pools header if pools defined
    if self.pools.pool_id > 0 then
        table.insert(lines, '#include "' .. base_name .. '_pools.h"')
        table.insert(lines, "")
    end
    
    -- Function name tables
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "// FUNCTION NAME TABLES (shared by all trees)")
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "")
    
    table.insert(lines, "// Oneshot (@) function names")
    emit_string_array(lines, prefix .. "_oneshot_names", self.tables.oneshot_fns)
    table.insert(lines, "")
    
    table.insert(lines, "// Boolean (?) function names")
    emit_string_array(lines, prefix .. "_boolean_names", self.tables.boolean_fns)
    table.insert(lines, "")
    
    table.insert(lines, "// Main (!) function names")
    emit_string_array(lines, prefix .. "_main_names", self.tables.main_fns)
    table.insert(lines, "")
    
    table.insert(lines, "// Data strings")
    emit_string_array(lines, prefix .. "_strings", self.tables.strings)
    table.insert(lines, "")
    
    -- Per-tree structures
    for _, tree_name in ipairs(self.tree_order) do
        local gen = self.tree_generators[tree_name]
        local nodes = gen:get_nodes()
        local params = gen:get_params()
        local tree_prefix = prefix .. "_" .. tree_name
        
        table.insert(lines, "// ============================================================================")
        table.insert(lines, "// TREE: " .. tree_name)
        table.insert(lines, "// ============================================================================")
        table.insert(lines, "")
        
        -- Parameters for this tree
        if #params > 0 then
            table.insert(lines, "static const s_expr_param_t " .. tree_prefix .. "_params[] = {")
            for idx, p in ipairs(params) do
                local type_str = "S_EXPR_PARAM_INT"
                local val_str = ""
                local comment = ""
                if p.type == PARAM_INT then
                    type_str = "S_EXPR_PARAM_INT"
                    val_str = string.format(".reserved = {0}, .i = %d%s", p.value, int_suffix)
                elseif p.type == PARAM_UINT then
                    type_str = "S_EXPR_PARAM_UINT"
                    val_str = string.format(".reserved = {0}, .u = %u%s", p.value, uint_suffix)
                elseif p.type == PARAM_FLOAT then
                    type_str = "S_EXPR_PARAM_FLOAT"
                    val_str = string.format(".reserved = {0}, .f = %g%s", p.value, float_suffix)
                elseif p.type == PARAM_STRING then
                    type_str = "S_EXPR_PARAM_STRING"
                    val_str = string.format(".reserved = {0}, .str_index = %d", p.value)
                elseif p.type == PARAM_MAIN then
                    type_str = "S_EXPR_PARAM_MAIN"
                    val_str = string.format(".reserved = {0}, .func_idx = %d", p.value)
                elseif p.type == PARAM_ONESHOT then
                    type_str = "S_EXPR_PARAM_ONESHOT"
                    val_str = string.format(".reserved = {0}, .func_idx = %d", p.value)
                elseif p.type == PARAM_PRED then
                    type_str = "S_EXPR_PARAM_PRED"
                    val_str = string.format(".reserved = {0}, .func_idx = %d", p.value)
                elseif p.type == PARAM_SLOT then
                    type_str = "S_EXPR_PARAM_SLOT"
                    val_str = string.format(".reserved = {0}, .slot = { .pool_id = %d, .slot_index = %d }",
                        p.pool_id, p.slot_index)
                    comment = string.format("  // %s", p.name or "")
                elseif p.type == PARAM_OPEN then
                    type_str = "S_EXPR_PARAM_OPEN"
                    val_str = string.format(".reserved = {0}, .brace_idx = %d", p.value)
                    comment = string.format("  // offset to close: +%d", p.value)
                elseif p.type == PARAM_OPEN_CALL then
                    type_str = "S_EXPR_PARAM_OPEN_CALL"
                    val_str = string.format(".reserved = {0}, .brace_idx = %d", p.value)
                    comment = string.format("  // callable, offset to close: +%d", p.value)
                elseif p.type == PARAM_CLOSE then
                    type_str = "S_EXPR_PARAM_CLOSE"
                    val_str = string.format(".reserved = {0}, .brace_idx = %d", p.value)
                    comment = string.format("  // offset to open: -%d", p.value)
                end
                table.insert(lines, string.format("    { .type = %s, %s },%s  // [%d]", 
                    type_str, val_str, comment, idx - 1))
            end
            table.insert(lines, "};")
        else
            table.insert(lines, "static const s_expr_param_t* " .. tree_prefix .. "_params = NULL;")
        end
        table.insert(lines, "#define " .. string.upper(tree_prefix) .. "_PARAM_COUNT " .. #params)
        table.insert(lines, "")
        
        -- Nodes for this tree
        table.insert(lines, "static const s_expr_node_t " .. tree_prefix .. "_nodes[] = {")
        for i, n in ipairs(nodes) do
            local comment = string.format("// [%d]", i - 1)
            table.insert(lines, string.format(
                "    { .type = 0x%02X, .child_count = %d, .node_index = %d, " ..
                ".first_child = 0x%04X, .next_sibling = 0x%04X, " ..
                ".fn_index = %d, .param_offset = %d, .param_count = %d, .reserved = %d }, %s",
                n.type or 0, n.child_count or 0, n.node_index or 0,
                n.first_child or 0xFFFF, n.next_sibling or 0xFFFF,
                n.fn_index or 0, n.param_offset or 0, n.param_count or 0,
                type(n.is_default) == "number" and n.is_default or (n.is_default and 1 or 0),
                comment
            ))
        end
        table.insert(lines, "};")
        table.insert(lines, "#define " .. string.upper(tree_prefix) .. "_NODE_COUNT " .. #nodes)
        table.insert(lines, "")
    end
    
    -- Tree definitions array
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "// TREE DEFINITIONS")
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "")
    
    table.insert(lines, "static const s_expr_tree_def_t " .. prefix .. "_trees[] = {")
    for _, tree_name in ipairs(self.tree_order) do
        local tree_prefix = prefix .. "_" .. tree_name
        local gen = self.tree_generators[tree_name]
        local node_count = gen:get_node_count()
        local param_count = #gen:get_params()
        
        table.insert(lines, "    {")
        table.insert(lines, '        .name = "' .. tree_name .. '",')
        table.insert(lines, '        .nodes = ' .. tree_prefix .. '_nodes,')
        table.insert(lines, '        .node_count = ' .. node_count .. ',')
        table.insert(lines, '        .root_index = 0,')
        if param_count > 0 then
            table.insert(lines, '        .params = ' .. tree_prefix .. '_params,')
        else
            table.insert(lines, '        .params = NULL,')
        end
        table.insert(lines, '        .param_count = ' .. param_count .. ',')
        table.insert(lines, "    },")
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
    table.insert(lines, '    .name = "' .. self.name .. '",')
    table.insert(lines, '    .trees = ' .. prefix .. '_trees,')
    table.insert(lines, '    .tree_count = ' .. #self.tree_order .. ',')
    table.insert(lines, '    .is_64bit = ' .. (self.is_64bit and 'true' or 'false') .. ',')
    table.insert(lines, "")
    table.insert(lines, '    .oneshot_names = ' .. prefix .. '_oneshot_names,')
    table.insert(lines, '    .boolean_names = ' .. prefix .. '_boolean_names,')
    table.insert(lines, '    .main_names = ' .. prefix .. '_main_names,')
    table.insert(lines, '    .strings = ' .. prefix .. '_strings,')
    table.insert(lines, "")
    table.insert(lines, '    .oneshot_count = ' .. #self.tables.oneshot_fns .. ',')
    table.insert(lines, '    .boolean_count = ' .. #self.tables.boolean_fns .. ',')
    table.insert(lines, '    .main_count = ' .. #self.tables.main_fns .. ',')
    table.insert(lines, '    .string_count = ' .. #self.tables.strings .. ',')
    table.insert(lines, "")
    table.insert(lines, '    .max_node_count = ' .. self.max_node_count .. ',')
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
-- BINARY OUTPUT
--============================================================================

function ModuleGenerator:to_bin()
    self:compile()
    
    local out = {}
    local value_size = self.is_64bit and 8 or 4
    
    local function emit_u8(v)
        table.insert(out, bit.band(v, 0xFF))
    end
    
    local function emit_u16(v)
        table.insert(out, bit.band(v, 0xFF))
        table.insert(out, bit.band(bit.rshift(v, 8), 0xFF))
    end
    
    local function emit_u32(v)
        for i = 0, 3 do
            table.insert(out, bit.band(bit.rshift(v, i * 8), 0xFF))
        end
    end
    
    local function emit_i32(v)
        if v < 0 then
            v = 0x100000000 + v
        end
        emit_u32(v)
    end
    
    local function emit_u64(v)
        local lo = bit.band(v, 0xFFFFFFFF)
        local hi = bit.rshift(v, 32)
        emit_u32(lo)
        emit_u32(hi)
    end
    
    local function emit_i64(v)
        local lo, hi
        if v >= 0 then
            lo = bit.band(v, 0xFFFFFFFF)
            hi = math.floor(v / 0x100000000)
        else
            local abs_v = -v
            local abs_lo = bit.band(abs_v, 0xFFFFFFFF)
            local abs_hi = math.floor(abs_v / 0x100000000)
            lo = bit.band(bit.bnot(abs_lo) + 1, 0xFFFFFFFF)
            local carry = (lo == 0) and 1 or 0
            hi = bit.band(bit.bnot(abs_hi) + carry, 0xFFFFFFFF)
        end
        emit_u32(lo)
        emit_u32(hi)
    end
    
    local function emit_f32(v)
        local buf = ffi.new("float[1]", v)
        local bytes = ffi.cast("uint8_t*", buf)
        for i = 0, 3 do
            table.insert(out, bytes[i])
        end
    end
    
    local function emit_f64(v)
        local buf = ffi.new("double[1]", v)
        local bytes = ffi.cast("uint8_t*", buf)
        for i = 0, 7 do
            table.insert(out, bytes[i])
        end
    end
    
    -- Emit value based on 64-bit flag
    local function emit_int(v)
        if self.is_64bit then
            emit_i64(v)
        else
            emit_i32(v)
        end
    end
    
    local function emit_uint(v)
        if self.is_64bit then
            emit_u64(v)
        else
            emit_u32(v)
        end
    end
    
    local function emit_float(v)
        if self.is_64bit then
            emit_f64(v)
        else
            emit_f32(v)
        end
    end
    
    -- Build string blob
    local string_blob = {}
    local string_offsets = {}
    local blob_pos = 0
    
    local function add_to_blob(s)
        if string_offsets[s] then
            return string_offsets[s]
        end
        local offset = blob_pos
        string_offsets[s] = offset
        local len = #s
        table.insert(string_blob, bit.band(len, 0xFF))
        table.insert(string_blob, bit.band(bit.rshift(len, 8), 0xFF))
        for c in s:gmatch(".") do
            table.insert(string_blob, string.byte(c))
        end
        blob_pos = blob_pos + 2 + len
        return offset
    end
    
    local module_name_blob_offset = add_to_blob(self.name)
    
    local oneshot_blob_offsets = {}
    for _, s in ipairs(self.tables.oneshot_fns) do
        table.insert(oneshot_blob_offsets, add_to_blob(s))
    end
    
    local boolean_blob_offsets = {}
    for _, s in ipairs(self.tables.boolean_fns) do
        table.insert(boolean_blob_offsets, add_to_blob(s))
    end
    
    local main_blob_offsets = {}
    for _, s in ipairs(self.tables.main_fns) do
        table.insert(main_blob_offsets, add_to_blob(s))
    end
    
    local string_blob_offsets = {}
    for _, s in ipairs(self.tables.strings) do
        table.insert(string_blob_offsets, add_to_blob(s))
    end
    
    local tree_name_blob_offsets = {}
    for _, tree_name in ipairs(self.tree_order) do
        table.insert(tree_name_blob_offsets, add_to_blob(tree_name))
    end
    
    -- Calculate layout
    local header_size = 32
    local string_index_size = (#self.tables.oneshot_fns + #self.tables.boolean_fns + 
                               #self.tables.main_fns + #self.tables.strings) * 4
    local string_blob_file_offset = header_size + string_index_size
    local tree_dir_file_offset = string_blob_file_offset + #string_blob
    local tree_dir_size = #self.tree_order * 16
    
    local tree_data_offset = tree_dir_file_offset + tree_dir_size
    local tree_offsets = {}
    local current_offset = tree_data_offset
    
    -- Param size: 4 bytes type/reserved + value_size
    local param_size = 4 + value_size
    
    for _, tree_name in ipairs(self.tree_order) do
        local gen = self.tree_generators[tree_name]
        local nodes = gen:get_nodes()
        local params = gen:get_params()
        
        local nodes_offset = current_offset
        current_offset = current_offset + #nodes * 14
        
        local params_offset = current_offset
        current_offset = current_offset + #params * param_size
        
        table.insert(tree_offsets, {
            nodes_offset = nodes_offset,
            params_offset = params_offset,
        })
    end
    
    -- Emit header (32 bytes)
    emit_u32(0x32444D53)  -- "SMD2" magic
    emit_u16(0x0002)      -- version 2 (with 64-bit support)
    emit_u16(self.is_64bit and 0x0001 or 0x0000)  -- flags: bit 0 = 64-bit
    emit_u16(#self.tree_order)
    emit_u16(#self.tables.oneshot_fns)
    emit_u16(#self.tables.boolean_fns)
    emit_u16(#self.tables.main_fns)
    emit_u16(#self.tables.strings)
    emit_u16(self.max_node_count)
    emit_u32(module_name_blob_offset)
    emit_u32(string_blob_file_offset)
    emit_u32(tree_dir_file_offset)
    
    -- Emit string index tables
    for _, off in ipairs(oneshot_blob_offsets) do emit_u32(off) end
    for _, off in ipairs(boolean_blob_offsets) do emit_u32(off) end
    for _, off in ipairs(main_blob_offsets) do emit_u32(off) end
    for _, off in ipairs(string_blob_offsets) do emit_u32(off) end
    
    -- Emit string blob
    for _, b in ipairs(string_blob) do
        table.insert(out, b)
    end
    
    -- Emit tree directory
    for i, tree_name in ipairs(self.tree_order) do
        local gen = self.tree_generators[tree_name]
        local offsets = tree_offsets[i]
        
        emit_u32(tree_name_blob_offsets[i])
        emit_u16(gen:get_node_count())
        emit_u16(#gen:get_params())
        emit_u32(offsets.nodes_offset)
        emit_u32(offsets.params_offset)
    end
    
    -- Emit tree data
    for _, tree_name in ipairs(self.tree_order) do
        local gen = self.tree_generators[tree_name]
        local nodes = gen:get_nodes()
        local params = gen:get_params()
        
        -- Emit nodes (14 bytes each)
        for _, n in ipairs(nodes) do
            emit_u8(n.type)
            emit_u8(n.child_count)
            emit_u16(n.node_index)
            emit_u16(n.first_child)
            emit_u16(n.next_sibling)
            emit_u16(n.fn_index)
            emit_u16(n.param_offset)
            emit_u8(n.param_count)
            emit_u8(type(n.is_default) == "number" and n.is_default or (n.is_default and 1 or 0))
        end
        
        -- Emit params (4 + value_size bytes each)
        for _, p in ipairs(params) do
            emit_u8(p.type)
            emit_u8(0)  -- reserved
            emit_u8(0)
            emit_u8(0)
            if p.type == PARAM_FLOAT then
                emit_float(p.value)
            elseif p.type == PARAM_INT then
                emit_int(p.value)
            elseif p.type == PARAM_SLOT then
                -- Pack pool_id (16 bits) + slot_index (16 bits) for 32-bit
                -- or pool_id (32 bits) + slot_index (32 bits) for 64-bit
                if self.is_64bit then
                    emit_u32(p.pool_id)
                    emit_u32(p.slot_index)
                else
                    emit_u16(p.pool_id)
                    emit_u16(p.slot_index)
                end
            else
                emit_uint(p.value or 0)
            end
        end
    end
    
    return out
end

--============================================================================
-- DUMP
--============================================================================

function ModuleGenerator:dump()
    self:compile()
    
    print("MODULE: " .. self.name)
    print("64-bit: " .. (self.is_64bit and "yes" or "no"))
    print("")
    
    -- Dump pools
    print("POOLS:")
    local ordered_pools = {}
    for name, pool in pairs(self.pools.pools) do
        ordered_pools[pool.id + 1] = { name = name, pool = pool }
    end
    for _, entry in ipairs(ordered_pools) do
        print(string.format("  [%d] %s (%s) - %d slots",
            entry.pool.id, entry.name, entry.pool.type, entry.pool.slot_count))
    end
    print("")
    
    print("SLOTS:")
    for slot_name, slot in pairs(self.pools.slots) do
        local pool = self.pools.pools[slot.pool]
        print(string.format("  %s -> %s[%d]", slot_name, slot.pool, slot.index))
    end
    print("")
    
    print("ONESHOT FUNCTIONS (@):")
    for i, s in ipairs(self.tables.oneshot_fns) do
        print(string.format("  [%d] @%s", i - 1, s))
    end
    print("")
    print("BOOLEAN FUNCTIONS (?):")
    for i, s in ipairs(self.tables.boolean_fns) do
        print(string.format("  [%d] ?%s", i - 1, s))
    end
    print("")
    print("MAIN FUNCTIONS (!):")
    for i, s in ipairs(self.tables.main_fns) do
        print(string.format("  [%d] !%s", i - 1, s))
    end
    print("")
    print("DATA STRINGS:")
    for i, s in ipairs(self.tables.strings) do
        print(string.format("  [%d] \"%s\"", i - 1, s))
    end
    print("")
    print("TREES: " .. #self.tree_order)
    print("MAX NODE COUNT: " .. self.max_node_count)
    
    for _, tree_name in ipairs(self.tree_order) do
        print("")
        print("TREE: " .. tree_name)
        local gen = self.tree_generators[tree_name]
        local nodes = gen:get_nodes()
        local params = gen:get_params()
        print("  Node count: " .. #nodes)
        for i, n in ipairs(nodes) do
            print(string.format(
                "  [%d] type=0x%02X children=%d first=%d next=%d fn=%d params=%d+%d",
                i - 1, n.type or 0, n.child_count or 0, n.first_child or 0xFFFF, n.next_sibling or 0xFFFF,
                n.fn_index or 0, n.param_offset or 0, n.param_count or 0
            ))
        end
        print("  Params: " .. #params)
        for i, p in ipairs(params) do
            if p.type == PARAM_SLOT then
                print(string.format("    [%d] SLOT pool=%d index=%d (%s)",
                    i - 1, p.pool_id, p.slot_index, p.name or ""))
            else
                print(string.format("    [%d] type=%d value=%s",
                    i - 1, p.type, tostring(p.value)))
            end
        end
    end
end

--============================================================================
-- EXPORT
--============================================================================

print("ChainTree S-Expression DSL v2.7 loaded (two-tier architecture, s_expr_ types, slotted blackboards, flatten support)")