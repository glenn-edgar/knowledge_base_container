--============================================================================
-- CHAINTREE S-EXPRESSION DSL
-- Version 2.0 - Flat node_t array output
--============================================================================

local ffi = require("ffi")
local bit = require("bit")

local lua_debug = debug

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
    clause   = 0x0B,   -- NEW: cond clause wrapper
    case     = 0x0C,   -- NEW: dispatch case wrapper
}

local CONTROL_CODES = {
    CFL_CONTINUE           = 0,
    CFL_HALT               = 1,
    CFL_TERMINATE          = 2,
    CFL_RESET              = 3,
    CFL_DISABLE            = 4,
    CFL_FUNCTION_TERMINATE = 5,
}

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
    CLAUSE      = "clause",
    CASE        = "case",
    CONDITION   = "condition",
    ACTION      = "action",
}

local PARAM_INT32   = 0x00
local PARAM_UINT32  = 0x01
local PARAM_FLOAT32 = 0x02
local PARAM_STRING  = 0x03

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
-- DSL STATE (unchanged from original)
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

local _state = {
    stack = {},
    root = nil,
    test_name = nil,
    tables = new_tables(),
    line = 0,
}

local _module = {
    name = nil,
    trees = {},
    tree_order = {},
    tables = new_tables(),
    current_tree = nil,
}

--============================================================================
-- ERROR HANDLING (unchanged)
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
-- STACK OPERATIONS (unchanged)
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
-- CONTEXT VALIDATION (unchanged)
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
-- COMPOSITE NODE HELPER (unchanged)
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
-- STRING TABLE FUNCTIONS (unchanged)
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
-- PARAMETER TYPE HELPERS (unchanged)
--============================================================================

function int32(value)
    return { _param_type = "int32", value = value }
end

function uint32(value)
    return { _param_type = "uint32", value = value }
end

function float32(value)
    return { _param_type = "float32", value = value }
end

function str(value)
    return { _param_type = "string", value = tostring(value) }
end

local function encode_param(p)
    if type(p) == "table" and p._param_type then
        return { type = p._param_type, value = p.value }
    end
    
    local t = type(p)
    if t == "number" then
        if math.floor(p) == p then
            if p < 0 then
                return { type = "int32", value = p }
            else
                return { type = "uint32", value = p }
            end
        else
            return { type = "float32", value = p }
        end
    elseif t == "string" then
        return { type = "string", value = p }
    else
        dsl_error(string.format("invalid parameter type: %s", t))
    end
end

local function encode_params(...)
    local params = {}
    local args = {...}
    for _, p in ipairs(args) do
        table.insert(params, encode_param(p))
    end
    return params
end

--============================================================================
-- ALL DSL FUNCTIONS (unchanged from original)
-- start_test, end_test, start_module, start_tree, end_tree, end_module
-- oneshot, main, bool_fn, quote
-- pipeline, end_pipeline
-- if_then, end_if_then, if_then_else, end_if_then_else
-- cond, end_cond, clause, end_clause, default_clause, end_default_clause
-- dispatch, end_dispatch, case, end_case, default_case, end_default_case
-- condition, end_condition, action, end_action
-- bool_and, end_bool_and, bool_or, end_bool_or, bool_not, end_bool_not
-- dbg, end_dbg
--============================================================================

-- [Include all the DSL functions from original - they don't change]
-- ... (keeping same as original for brevity)

--============================================================================
-- TEST/MODULE WRAPPERS (unchanged structure, different return)
--============================================================================

function start_test(name)
    if type(name) ~= "string" then
        error("[DSL ERROR] start_test() requires string name", 2)
    end
    
    _state = {
        stack = {},
        root = nil,
        test_name = name,
        tables = new_tables(),
        line = 0,
    }
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
    
    if not _state.root then
        dsl_error("no root node defined")
    end
    
    return TreeGenerator.new(name, _state.root, _state.tables)
end

function start_module(name)
    if type(name) ~= "string" then
        error("[DSL ERROR] start_module() requires string name", 2)
    end
    
    _module = {
        name = name,
        trees = {},
        tree_order = {},
        tables = new_tables(),
        current_tree = nil,
    }
end

function start_tree(name)
    if type(name) ~= "string" then
        error("[DSL ERROR] start_tree() requires string name", 2)
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
    }
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
    
    if not _state.root then
        dsl_error("no root node defined in tree '" .. name .. "'")
    end
    
    _module.trees[name] = _state.root
    table.insert(_module.tree_order, name)
    _module.current_tree = nil
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
        _module.tables
    )
end

--============================================================================
-- LEAF FUNCTIONS (unchanged)
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
-- COMPOSITE NODES (unchanged)
--============================================================================

function pipeline(name)
    if type(name) ~= "string" then
        dsl_error("pipeline() requires name")
    end
    check_context({CONTEXTS.CONTROL_FLOW, CONTEXTS.ACTION}, "pipeline")
    start_composite(NODE_TYPES.PIPELINE, name, CONTEXTS.CONTROL_FLOW)
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

function if_then(name)
    if type(name) ~= "string" then
        dsl_error("if_then() requires name")
    end
    check_context({CONTEXTS.CONTROL_FLOW, CONTEXTS.ACTION}, "if_then")
    start_composite(NODE_TYPES.IF, name, CONTEXTS.CONDITION, function(n)
        n.condition = nil
        n.then_action = nil
    end)
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

function if_then_else(name)
    if type(name) ~= "string" then
        dsl_error("if_then_else() requires name")
    end
    check_context({CONTEXTS.CONTROL_FLOW, CONTEXTS.ACTION}, "if_then_else")
    start_composite(NODE_TYPES.IF_ELSE, name, CONTEXTS.CONDITION, function(n)
        n.condition = nil
        n.then_action = nil
        n.else_action = nil
    end)
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

function cond(name)
    if type(name) ~= "string" then
        dsl_error("cond() requires name")
    end
    check_context({CONTEXTS.CONTROL_FLOW, CONTEXTS.ACTION}, "cond")
    start_composite(NODE_TYPES.COND, name, CONTEXTS.CLAUSE_LIST, function(n)
        n.clauses = {}
    end)
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

function clause(name)
    if type(name) ~= "string" then
        dsl_error("clause() requires name")
    end
    check_context({CONTEXTS.CLAUSE_LIST}, "clause")
    
    local node = stack_push(NODE_TYPES.CLAUSE, name, CONTEXTS.CONDITION)
    node.condition = nil
    node.action = nil
    node.is_default = false
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

function default_clause(name)
    if type(name) ~= "string" then
        dsl_error("default_clause() requires name")
    end
    check_context({CONTEXTS.CLAUSE_LIST}, "default_clause")
    
    local node = stack_push(NODE_TYPES.CLAUSE, name, CONTEXTS.ACTION)
    node.condition = nil
    node.action = nil
    node.is_default = true
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

function dispatch(name, key)
    if type(name) ~= "string" then
        dsl_error("dispatch() requires name")
    end
    if type(key) ~= "string" then
        dsl_error("dispatch() requires key parameter")
    end
    check_context({CONTEXTS.CONTROL_FLOW, CONTEXTS.ACTION}, "dispatch")
    start_composite(NODE_TYPES.DISPATCH, name, CONTEXTS.CASE_LIST, function(n)
        n.key = key
        n.cases = {}
    end)
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

function case(name, pattern)
    if type(name) ~= "string" then
        dsl_error("case() requires name")
    end
    if pattern == nil then
        dsl_error("case() requires pattern")
    end
    check_context({CONTEXTS.CASE_LIST}, "case")
    
    local node = stack_push(NODE_TYPES.CASE, name, CONTEXTS.ACTION)
    node.pattern = pattern
    node.action = nil
    node.is_default = false
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

function default_case(name)
    if type(name) ~= "string" then
        dsl_error("default_case() requires name")
    end
    check_context({CONTEXTS.CASE_LIST}, "default_case")
    
    local node = stack_push(NODE_TYPES.CASE, name, CONTEXTS.ACTION)
    node.pattern = nil
    node.action = nil
    node.is_default = true
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

function condition(name)
    if type(name) ~= "string" then
        dsl_error("condition() requires name")
    end
    check_context({CONTEXTS.CONDITION}, "condition")
    
    local node = stack_push(NODE_TYPES.CONDITION, name, CONTEXTS.BOOLEAN)
    node.expr = nil
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

function action(name)
    if type(name) ~= "string" then
        dsl_error("action() requires name")
    end
    check_context({CONTEXTS.ACTION, CONTEXTS.CONDITION}, "action")
    
    stack_push(NODE_TYPES.ACTION, name, CONTEXTS.CONTROL_FLOW)
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

function bool_and(name)
    if type(name) ~= "string" then
        dsl_error("bool_and() requires name")
    end
    check_context({CONTEXTS.BOOLEAN, CONTEXTS.CONDITION}, "bool_and")
    start_composite(NODE_TYPES.AND, name, CONTEXTS.BOOLEAN)
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

function bool_or(name)
    if type(name) ~= "string" then
        dsl_error("bool_or() requires name")
    end
    check_context({CONTEXTS.BOOLEAN, CONTEXTS.CONDITION}, "bool_or")
    start_composite(NODE_TYPES.OR, name, CONTEXTS.BOOLEAN)
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

function bool_not(name)
    if type(name) ~= "string" then
        dsl_error("bool_not() requires name")
    end
    check_context({CONTEXTS.BOOLEAN, CONTEXTS.CONDITION}, "bool_not")
    start_composite(NODE_TYPES.NOT, name, CONTEXTS.BOOLEAN)
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

function dbg(name, message)
    if type(name) ~= "string" then
        dsl_error("dbg() requires name")
    end
    if type(message) ~= "string" then
        dsl_error("dbg() requires message string")
    end
    check_context({CONTEXTS.CONTROL_FLOW, CONTEXTS.ACTION}, "dbg")
    start_composite(NODE_TYPES.DEBUG, name, CONTEXTS.CONTROL_FLOW, function(n)
        n.message = message
    end)
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
-- TREE GENERATOR (single tree -> flat node_t array)
--============================================================================

TreeGenerator = {}
TreeGenerator.__index = TreeGenerator

function TreeGenerator.new(name, root, tables)
    local self = setmetatable({}, TreeGenerator)
    self.name = name
    self.root = root
    self.tables = tables
    self.nodes = {}        -- flat array of node_t
    self.params = {}       -- flat array of param_t
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
-- PASS 1: Assign node indices (pre-order traversal)
--============================================================================

function TreeGenerator:assign_indices(node, index)
    node._node_index = index
    local next_index = index + 1
    
    local t = node.type
    
    -- Get children based on node type
    local children = self:get_node_children(node)
    
    for _, child in ipairs(children) do
        next_index = self:assign_indices(child, next_index)
    end
    
    return next_index
end

-- Returns ordered list of children for a node
function TreeGenerator:get_node_children(node)
    local t = node.type
    local children = {}
    
    if t == NODE_TYPES.PIPELINE then
        children = node.children
        
    elseif t == NODE_TYPES.IF then
        -- condition, then_action
        table.insert(children, node.condition)
        table.insert(children, node.then_action)
        
    elseif t == NODE_TYPES.IF_ELSE then
        -- condition, then_action, else_action
        table.insert(children, node.condition)
        table.insert(children, node.then_action)
        table.insert(children, node.else_action)
        
    elseif t == NODE_TYPES.COND then
        -- clauses become children (each clause is a node)
        for _, cl in ipairs(node.clauses) do
            -- Create clause wrapper node
            local clause_node = {
                type = NODE_TYPES.CLAUSE,
                condition = cl.condition,
                action = cl.action,
                is_default = cl.is_default,
            }
            table.insert(children, clause_node)
        end
        
    elseif t == NODE_TYPES.CLAUSE then
        -- condition (if not default), action
        if not node.is_default and node.condition then
            table.insert(children, node.condition)
        end
        table.insert(children, node.action)
        
    elseif t == NODE_TYPES.DISPATCH then
        -- cases become children
        for _, cs in ipairs(node.cases) do
            local case_node = {
                type = NODE_TYPES.CASE,
                pattern = cs.pattern,
                action = cs.action,
                is_default = cs.is_default,
            }
            table.insert(children, case_node)
        end
        
    elseif t == NODE_TYPES.CASE then
        -- just action
        table.insert(children, node.action)
        
    elseif t == NODE_TYPES.AND or t == NODE_TYPES.OR then
        children = node.children
        
    elseif t == NODE_TYPES.NOT then
        children = node.children
        
    elseif t == NODE_TYPES.DEBUG then
        table.insert(children, node.child)
    end
    
    -- Leaf nodes: QUOTE, ONESHOT, BOOLEAN, MAIN have no children
    
    return children
end

--============================================================================
-- PASS 2: Emit flat node array
--============================================================================

function TreeGenerator:emit_nodes(node, next_sibling_index)
    local t = node.type
    local children = self:get_node_children(node)
    
    -- Build node_t structure
    local n = {
        type = 0,
        child_count = #children,
        node_index = node._node_index,
        first_child = NO_CHILD,
        next_sibling = next_sibling_index or NO_SIBLING,
        fn_index = 0,
        param_offset = #self.params,
        param_count = 0,
        is_default = false,  -- for clause/case
    }
    
    -- Set type and fn_index based on node type
    if t == NODE_TYPES.QUOTE then
        n.type = TABLE_OPCODE + OPCODES.quote
        n.fn_index = CONTROL_CODES[node.value]
        
    elseif t == NODE_TYPES.ONESHOT then
        n.type = TABLE_ONESHOT
        n.fn_index = self:add_oneshot_fn(node.fn_name)
        self:emit_params(node.params or {})
        n.param_count = #(node.params or {})
        
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
        -- key is first param
        local key_idx = self:add_string(node.key)
        table.insert(self.params, { type = PARAM_STRING, value = key_idx })
        n.param_count = 1
        
    elseif t == NODE_TYPES.CASE then
        n.type = TABLE_OPCODE + OPCODES.case
        n.is_default = node.is_default
        if not node.is_default then
            -- pattern(s) as params
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
        
    elseif t == NODE_TYPES.DEBUG then
        n.type = TABLE_OPCODE + OPCODES.dbg
        local msg_idx = self:add_string(node.message)
        table.insert(self.params, { type = PARAM_STRING, value = msg_idx })
        n.param_count = 1
    end
    
    -- Set first_child
    if #children > 0 then
        n.first_child = children[1]._node_index
    end
    
    -- Add this node to array
    table.insert(self.nodes, n)
    
    -- Recursively emit children with correct next_sibling
    for i, child in ipairs(children) do
        local child_next_sibling = NO_SIBLING
        if i < #children then
            child_next_sibling = children[i + 1]._node_index
        end
        self:emit_nodes(child, child_next_sibling)
    end
end

function TreeGenerator:emit_params(params)
    for _, p in ipairs(params) do
        local pt = {
            type = PARAM_INT32,
            value = 0,
        }
        if p.type == "int32" then
            pt.type = PARAM_INT32
            pt.value = p.value
        elseif p.type == "uint32" then
            pt.type = PARAM_UINT32
            pt.value = p.value
        elseif p.type == "float32" then
            pt.type = PARAM_FLOAT32
            pt.value = p.value
        elseif p.type == "string" then
            pt.type = PARAM_STRING
            pt.value = self:add_string(p.value)
        end
        table.insert(self.params, pt)
    end
end

--============================================================================
-- COMPILE
--============================================================================

function TreeGenerator:compile()
    if self.compiled then
        return
    end
    
    -- Pass 1: assign indices
    self.node_count = self:assign_indices(self.root, 0)
    
    -- Pass 2: emit flat array
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
-- MODULE GENERATOR (multi-tree)
--============================================================================

ModuleGenerator = {}
ModuleGenerator.__index = ModuleGenerator

function ModuleGenerator.new(name, trees, tree_order, tables)
    local self = setmetatable({}, ModuleGenerator)
    self.name = name
    self.trees = trees
    self.tree_order = tree_order
    self.tables = tables
    self.tree_generators = {}
    self.max_node_count = 0
    self.compiled = false
    return self
end

function ModuleGenerator:compile()
    if self.compiled then
        return
    end
    
    -- Create generator for each tree
    for _, tree_name in ipairs(self.tree_order) do
        local gen = TreeGenerator.new(tree_name, self.trees[tree_name], self.tables)
        gen:compile()
        self.tree_generators[tree_name] = gen
        
        -- Track max node count
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
    
    -- Header
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "// " .. base_name .. "_module.h")
    table.insert(lines, "// Generated by ChainTree S-Expression DSL")
    table.insert(lines, "// DO NOT EDIT")
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "")
    table.insert(lines, "#ifndef " .. guard)
    table.insert(lines, "#define " .. guard)
    table.insert(lines, "")
    table.insert(lines, "#include <stdint.h>")
    table.insert(lines, "#include <stdbool.h>")
    table.insert(lines, "")
    
    -- Include the engine types (assume they exist)
    table.insert(lines, '#include "s_engine_types.h"')
    table.insert(lines, "")
    
    -- Function name tables (shared across module)
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
            table.insert(lines, "static const param_t " .. tree_prefix .. "_params[] = {")
            for _, p in ipairs(params) do
                local type_str = "PARAM_INT32"
                local val_str = ""
                if p.type == PARAM_INT32 then
                    type_str = "PARAM_INT32"
                    val_str = string.format(".i32 = %d", p.value)
                elseif p.type == PARAM_UINT32 then
                    type_str = "PARAM_UINT32"
                    val_str = string.format(".u32 = %uU", p.value)
                elseif p.type == PARAM_FLOAT32 then
                    type_str = "PARAM_FLOAT32"
                    val_str = string.format(".f32 = %ff", p.value)
                elseif p.type == PARAM_STRING then
                    type_str = "PARAM_STRING"
                    val_str = string.format(".str_index = %d", p.value)
                end
                table.insert(lines, string.format("    { .type = %s, %s },", type_str, val_str))
            end
            table.insert(lines, "};")
        else
            table.insert(lines, "static const param_t* " .. tree_prefix .. "_params = NULL;")
        end
        table.insert(lines, "#define " .. string.upper(tree_prefix) .. "_PARAM_COUNT " .. #params)
        table.insert(lines, "")
        
        -- Nodes for this tree
        table.insert(lines, "static const node_t " .. tree_prefix .. "_nodes[] = {")
        for i, n in ipairs(nodes) do
            local comment = string.format("// [%d]", i - 1)
            table.insert(lines, string.format(
                "    { .type = 0x%02X, .child_count = %d, .node_index = %d, " ..
                ".first_child = 0x%04X, .next_sibling = 0x%04X, " ..
                ".fn_index = %d, .param_offset = %d, .param_count = %d, .reserved = %d }, %s",
                n.type or 0, n.child_count or 0, n.node_index or 0,
                n.first_child or 0xFFFF, n.next_sibling or 0xFFFF,
                n.fn_index or 0, n.param_offset or 0, n.param_count or 0,
                n.is_default and 1 or 0,
                comment
                  ))
        end
        table.insert(lines, "};")
        table.insert(lines, "#define " .. string.upper(tree_prefix) .. "_NODE_COUNT " .. #nodes)
        table.insert(lines, "")
    end
    
    -- Tree definitions array
    
    -- Tree definitions array
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "// TREE DEFINITIONS")
    table.insert(lines, "// ============================================================================")
    table.insert(lines, "")
    
    table.insert(lines, "static const tree_def_t " .. prefix .. "_trees[] = {")
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
    
    table.insert(lines, "static const module_def_t " .. prefix .. "_module = {")
    table.insert(lines, '    .name = "' .. self.name .. '",')
    table.insert(lines, '    .trees = ' .. prefix .. '_trees,')
    table.insert(lines, '    .tree_count = ' .. #self.tree_order .. ',')
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
    
    -- Footer
    table.insert(lines, "#endif // " .. guard)
    
    return table.concat(lines, "\n")
end

--============================================================================
-- BINARY OUTPUT
--============================================================================

function ModuleGenerator:to_bin()
    self:compile()
    
    local out = {}
    
    -- Helper functions
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
    
    local function emit_f32(v)
        local buf = ffi.new("float[1]", v)
        local bytes = ffi.cast("uint8_t*", buf)
        for i = 0, 3 do
            table.insert(out, bytes[i])
        end
    end
    
    -- Build string blob and collect offsets
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
    
    -- Add all strings to blob
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
    
    -- Calculate tree data offsets
    local tree_data_offset = tree_dir_file_offset + tree_dir_size
    local tree_offsets = {}
    local current_offset = tree_data_offset
    
    for _, tree_name in ipairs(self.tree_order) do
        local gen = self.tree_generators[tree_name]
        local nodes = gen:get_nodes()
        local params = gen:get_params()
        
        local nodes_offset = current_offset
        current_offset = current_offset + #nodes * 14
        
        local params_offset = current_offset
        current_offset = current_offset + #params * 8
        
        table.insert(tree_offsets, {
            nodes_offset = nodes_offset,
            params_offset = params_offset,
        })
    end
    
    -- Emit header (32 bytes)
    emit_u32(0x32444D53)  -- "SMD2" magic
    emit_u16(0x0001)      -- version
    emit_u16(0x0000)      -- flags
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
    
    -- Emit tree data (nodes and params)
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
            emit_u8(n.is_default and 1 or 0)
        end
        
        -- Emit params (8 bytes each)
        for _, p in ipairs(params) do
            emit_u8(p.type)
            emit_u8(0)  -- reserved
            emit_u8(0)
            emit_u8(0)
            if p.type == PARAM_FLOAT32 then
                emit_f32(p.value)
            elseif p.type == PARAM_INT32 then
                emit_i32(p.value)
            else
                emit_u32(p.value)
            end
        end
    end
    
    return out
end

--============================================================================
-- DUMP (for debugging)
--============================================================================

function ModuleGenerator:dump()
    self:compile()
    
    print("MODULE: " .. self.name)
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
        print("  Node count: " .. #nodes)
        for i, n in ipairs(nodes) do
            print(string.format(
                "  [%d] type=0x%02X children=%d first=%d next=%d fn=%d params=%d+%d",
                i - 1, n.type, n.child_count, n.first_child, n.next_sibling,
                n.fn_index, n.param_offset, n.param_count
            ))
        end
    end
end

--============================================================================
-- EXPORT
--============================================================================

print("ChainTree S-Expression DSL v2.0 loaded (flat node_t output)")