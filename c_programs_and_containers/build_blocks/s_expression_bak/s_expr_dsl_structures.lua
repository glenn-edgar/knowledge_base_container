-- ============================================================================
-- s_expr_dsl_structures.lua
-- DSL Functions for List, Dictionary, Array, and Tuple Structures
-- Add these to s_expr_dsl.lua
-- ============================================================================

-- ============================================================================
-- OPCODE CONSTANTS (add to existing opcodes section)
-- ============================================================================

local S_EXPR_PARAM_OPEN_DICT   = 0x10
local S_EXPR_PARAM_CLOSE_DICT  = 0x11
local S_EXPR_PARAM_OPEN_KEY    = 0x12
local S_EXPR_PARAM_CLOSE_KEY   = 0x13
local S_EXPR_PARAM_OPEN_ARRAY  = 0x14
local S_EXPR_PARAM_CLOSE_ARRAY = 0x15
local S_EXPR_PARAM_OPEN_TUPLE  = 0x16
local S_EXPR_PARAM_CLOSE_TUPLE = 0x17

-- ============================================================================
-- DICTIONARY STRUCTURES
-- ============================================================================

-- Start a dictionary (hash-based key-value collection)
function dict_start()
    local ctx = get_tree_context()
    local param = {
        type = "dict_start",
        opcode = S_EXPR_PARAM_OPEN_DICT,
        brace_idx = 0  -- will be patched by emit_tree_params
    }
    table.insert(ctx.params, param)
    -- Push onto brace stack for matching
    table.insert(ctx.brace_stack, { type = "dict", index = #ctx.params })
    return param
end

-- End a dictionary
function dict_end()
    local ctx = get_tree_context()
    local param = {
        type = "dict_end",
        opcode = S_EXPR_PARAM_CLOSE_DICT,
        brace_idx = 0
    }
    table.insert(ctx.params, param)
    
    -- Pop and patch matching open
    local open = table.remove(ctx.brace_stack)
    if not open or open.type ~= "dict" then
        error("dict_end() without matching dict_start()")
    end
    
    -- Patch brace indices
    local open_idx = open.index
    local close_idx = #ctx.params
    ctx.params[open_idx].brace_idx = close_idx - open_idx
    param.brace_idx = close_idx - open_idx
    
    return param
end

-- Start a dictionary key (with string key name)
function key(name)
    local ctx = get_tree_context()
    local hash = fnv1a_hash(name)
    local param = {
        type = "key_start",
        opcode = S_EXPR_PARAM_OPEN_KEY,
        str_hash = hash,
        key_name = name,  -- for debug output
        brace_idx = 0
    }
    table.insert(ctx.params, param)
    table.insert(ctx.brace_stack, { type = "key", index = #ctx.params })
    return param
end

-- Start a dictionary key (with pre-computed hash)
function key_hash(hash_value)
    local ctx = get_tree_context()
    local param = {
        type = "key_start",
        opcode = S_EXPR_PARAM_OPEN_KEY,
        str_hash = hash_value,
        brace_idx = 0
    }
    table.insert(ctx.params, param)
    table.insert(ctx.brace_stack, { type = "key", index = #ctx.params })
    return param
end

-- End a dictionary key
function key_end()
    local ctx = get_tree_context()
    local param = {
        type = "key_end",
        opcode = S_EXPR_PARAM_CLOSE_KEY,
        brace_idx = 0
    }
    table.insert(ctx.params, param)
    
    local open = table.remove(ctx.brace_stack)
    if not open or open.type ~= "key" then
        error("key_end() without matching key()")
    end
    
    local open_idx = open.index
    local close_idx = #ctx.params
    ctx.params[open_idx].brace_idx = close_idx - open_idx
    param.brace_idx = close_idx - open_idx
    
    return param
end

-- ============================================================================
-- ARRAY STRUCTURES
-- ============================================================================

-- Start an array (indexed collection)
function array_start()
    local ctx = get_tree_context()
    local param = {
        type = "array_start",
        opcode = S_EXPR_PARAM_OPEN_ARRAY,
        brace_idx = 0
    }
    table.insert(ctx.params, param)
    table.insert(ctx.brace_stack, { type = "array", index = #ctx.params })
    return param
end

-- End an array
function array_end()
    local ctx = get_tree_context()
    local param = {
        type = "array_end",
        opcode = S_EXPR_PARAM_CLOSE_ARRAY,
        brace_idx = 0
    }
    table.insert(ctx.params, param)
    
    local open = table.remove(ctx.brace_stack)
    if not open or open.type ~= "array" then
        error("array_end() without matching array_start()")
    end
    
    local open_idx = open.index
    local close_idx = #ctx.params
    ctx.params[open_idx].brace_idx = close_idx - open_idx
    param.brace_idx = close_idx - open_idx
    
    return param
end

-- ============================================================================
-- TUPLE STRUCTURES
-- ============================================================================

-- Start a tuple (fixed-size heterogeneous collection)
function tuple_start()
    local ctx = get_tree_context()
    local param = {
        type = "tuple_start",
        opcode = S_EXPR_PARAM_OPEN_TUPLE,
        brace_idx = 0
    }
    table.insert(ctx.params, param)
    table.insert(ctx.brace_stack, { type = "tuple", index = #ctx.params })
    return param
end

-- End a tuple
function tuple_end()
    local ctx = get_tree_context()
    local param = {
        type = "tuple_end",
        opcode = S_EXPR_PARAM_CLOSE_TUPLE,
        brace_idx = 0
    }
    table.insert(ctx.params, param)
    
    local open = table.remove(ctx.brace_stack)
    if not open or open.type ~= "tuple" then
        error("tuple_end() without matching tuple_start()")
    end
    
    local open_idx = open.index
    local close_idx = #ctx.params
    ctx.params[open_idx].brace_idx = close_idx - open_idx
    param.brace_idx = close_idx - open_idx
    
    return param
end

-- ============================================================================
-- STRING HASH PARAMETER
-- ============================================================================

-- Emit a pre-computed string hash value
function str_hash(s)
    local ctx = get_tree_context()
    local hash = fnv1a_hash(s)
    local param = {
        type = "str_hash",
        opcode = S_EXPR_PARAM_STR_HASH,
        str_hash = hash,
        str_value = s  -- for debug output
    }
    table.insert(ctx.params, param)
    return param
end

-- ============================================================================
-- BINARY OUTPUT ADDITIONS (add to emit_tree_params)
-- ============================================================================

--[[
Add these cases to the emit_tree_params function's type switch:

    elseif p.type == "dict_start" then
        emit_param_binary(out, S_EXPR_PARAM_OPEN_DICT, 0, p.brace_idx, 0)
        
    elseif p.type == "dict_end" then
        emit_param_binary(out, S_EXPR_PARAM_CLOSE_DICT, 0, p.brace_idx, 0)
        
    elseif p.type == "key_start" then
        -- OPEN_KEY stores the key hash in the str_hash field
        emit_param_binary_hash(out, S_EXPR_PARAM_OPEN_KEY, 0, p.str_hash)
        
    elseif p.type == "key_end" then
        emit_param_binary(out, S_EXPR_PARAM_CLOSE_KEY, 0, p.brace_idx, 0)
        
    elseif p.type == "array_start" then
        emit_param_binary(out, S_EXPR_PARAM_OPEN_ARRAY, 0, p.brace_idx, 0)
        
    elseif p.type == "array_end" then
        emit_param_binary(out, S_EXPR_PARAM_CLOSE_ARRAY, 0, p.brace_idx, 0)
        
    elseif p.type == "tuple_start" then
        emit_param_binary(out, S_EXPR_PARAM_OPEN_TUPLE, 0, p.brace_idx, 0)
        
    elseif p.type == "tuple_end" then
        emit_param_binary(out, S_EXPR_PARAM_CLOSE_TUPLE, 0, p.brace_idx, 0)
        
    elseif p.type == "str_hash" then
        emit_param_binary_hash(out, S_EXPR_PARAM_STR_HASH, 0, p.str_hash)

--]]

-- ============================================================================
-- C OUTPUT ADDITIONS (add to emit_tree_params_c)
-- ============================================================================

--[[
Add these cases to generate C struct initializers:

    elseif p.type == "dict_start" then
        return string.format("{ .type = 0x%02X, .brace_idx = %d }", 
            S_EXPR_PARAM_OPEN_DICT, p.brace_idx)
            
    elseif p.type == "dict_end" then
        return string.format("{ .type = 0x%02X, .brace_idx = %d }",
            S_EXPR_PARAM_CLOSE_DICT, p.brace_idx)
            
    elseif p.type == "key_start" then
        return string.format("{ .type = 0x%02X, .str_hash = 0x%08X } /* %s */",
            S_EXPR_PARAM_OPEN_KEY, p.str_hash, p.key_name or "")
            
    elseif p.type == "key_end" then
        return string.format("{ .type = 0x%02X, .brace_idx = %d }",
            S_EXPR_PARAM_CLOSE_KEY, p.brace_idx)
            
    elseif p.type == "array_start" then
        return string.format("{ .type = 0x%02X, .brace_idx = %d }",
            S_EXPR_PARAM_OPEN_ARRAY, p.brace_idx)
            
    elseif p.type == "array_end" then
        return string.format("{ .type = 0x%02X, .brace_idx = %d }",
            S_EXPR_PARAM_CLOSE_ARRAY, p.brace_idx)
            
    elseif p.type == "tuple_start" then
        return string.format("{ .type = 0x%02X, .brace_idx = %d }",
            S_EXPR_PARAM_OPEN_TUPLE, p.brace_idx)
            
    elseif p.type == "tuple_end" then
        return string.format("{ .type = 0x%02X, .brace_idx = %d }",
            S_EXPR_PARAM_CLOSE_TUPLE, p.brace_idx)
            
    elseif p.type == "str_hash" then
        return string.format("{ .type = 0x%02X, .str_hash = 0x%08X } /* %s */",
            S_EXPR_PARAM_STR_HASH, p.str_hash, p.str_value or "")

--]]

-- ============================================================================
-- EXPORTS (add to module exports)
-- ============================================================================

--[[
Add to the module's export table:

    -- Dictionary structures
    dict_start = dict_start,
    dict_end = dict_end,
    key = key,
    key_hash = key_hash,
    key_end = key_end,
    
    -- Array structures
    array_start = array_start,
    array_end = array_end,
    
    -- Tuple structures
    tuple_start = tuple_start,
    tuple_end = tuple_end,
    
    -- String hash
    str_hash = str_hash,

--]]

-- ============================================================================
-- BRACE STACK INITIALIZATION
-- Add this to start_tree():
--   ctx.brace_stack = {}
-- ============================================================================

print("Structure DSL extensions loaded")
return {
    -- Opcodes
    S_EXPR_PARAM_OPEN_DICT = S_EXPR_PARAM_OPEN_DICT,
    S_EXPR_PARAM_CLOSE_DICT = S_EXPR_PARAM_CLOSE_DICT,
    S_EXPR_PARAM_OPEN_KEY = S_EXPR_PARAM_OPEN_KEY,
    S_EXPR_PARAM_CLOSE_KEY = S_EXPR_PARAM_CLOSE_KEY,
    S_EXPR_PARAM_OPEN_ARRAY = S_EXPR_PARAM_OPEN_ARRAY,
    S_EXPR_PARAM_CLOSE_ARRAY = S_EXPR_PARAM_CLOSE_ARRAY,
    S_EXPR_PARAM_OPEN_TUPLE = S_EXPR_PARAM_OPEN_TUPLE,
    S_EXPR_PARAM_CLOSE_TUPLE = S_EXPR_PARAM_CLOSE_TUPLE,
    
    -- Functions
    dict_start = dict_start,
    dict_end = dict_end,
    key = key,
    key_hash = key_hash,
    key_end = key_end,
    array_start = array_start,
    array_end = array_end,
    tuple_start = tuple_start,
    tuple_end = tuple_end,
    str_hash = str_hash,
}