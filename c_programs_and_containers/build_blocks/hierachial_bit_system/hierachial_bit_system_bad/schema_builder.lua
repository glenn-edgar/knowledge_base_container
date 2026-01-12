--[[
  schema_builder.lua - Stack-based DSL for ChainTree Hierarchical Bit Map
  
  Features:
    - Stack-based start/end matching with auto-unique tokens
    - Meaningful error messages with user-provided names
    - Modular - functions can define subtrees
    - No paren/brace hell
    
  Usage:
    local S = require("schema_builder")
    
    local schema = S.start_schema("schema", "MySchema", "1.0.0")
    ...
    S.end_(schema)
    
    return S.build()
]]

local M = {}

-- Global state
local _counter = 0
local _stack = {}
local _schema = nil

-- Result accumulators
local _bitspaces = {}
local _classes = {}
local _nodes = {}
local _config = {}
local _options = {}

-- Current context
local _current_class = nil
local _current_bits = nil
local _current_node_stack = {}  -- For building paths

--------------------------------------------------------------------------------
-- Token Management
--------------------------------------------------------------------------------

local function make_token(user_name, token_type, data)
    _counter = _counter + 1
    local token = {
        id = user_name .. "_" .. _counter,
        user_name = user_name,
        token_type = token_type,
        data = data or {},
        depth = #_stack,
    }
    return token
end

local function push(token)
    table.insert(_stack, token)
    return token
end

local function pop(expected_token)
    if #_stack == 0 then
        error(string.format(
            "Stack empty - end_() called with '%s' (%s) but no open blocks",
            expected_token.user_name, expected_token.id
        ))
    end
    
    local top = _stack[#_stack]
    
    if top.id ~= expected_token.id then
        error(string.format(
            "Mismatched end_()\n  Expected: '%s' (%s, type=%s)\n  Got:      '%s' (%s, type=%s)",
            top.user_name, top.id, top.token_type,
            expected_token.user_name, expected_token.id, expected_token.token_type
        ))
    end
    
    table.remove(_stack)
    return top
end

local function current_token()
    return _stack[#_stack]
end

local function expect_context(expected_type, operation)
    local top = current_token()
    if not top or top.token_type ~= expected_type then
        local got = top and top.token_type or "none"
        error(string.format(
            "%s requires '%s' context, but current context is '%s'",
            operation, expected_type, got
        ))
    end
    return top
end

--------------------------------------------------------------------------------
-- Reset State (for multiple schemas)
--------------------------------------------------------------------------------

function M.reset()
    _counter = 0
    _stack = {}
    _schema = nil
    _bitspaces = {}
    _classes = {}
    _nodes = {}
    _config = {}
    _options = {}
    _current_class = nil
    _current_bits = nil
    _current_node_stack = {}
end

-- Auto-reset on require
M.reset()

--------------------------------------------------------------------------------
-- Schema
--------------------------------------------------------------------------------

function M.start_schema(user_name, name, version)
    M.reset()
    _schema = {
        name = name,
        version = version or "1.0.0",
    }
    return push(make_token(user_name, "schema", {name = name, version = version}))
end

--------------------------------------------------------------------------------
-- Options (simple key-value call, no block needed)
--------------------------------------------------------------------------------

function M.options(...)
    local args = {...}
    if #args % 2 ~= 0 then
        error("options() requires key-value pairs")
    end
    
    for i = 1, #args, 2 do
        local key = args[i]
        local value = args[i + 1]
        if type(key) ~= "string" then
            error("options() keys must be strings")
        end
        _options[key] = value
    end
end

--------------------------------------------------------------------------------
-- Bitspaces
--------------------------------------------------------------------------------

function M.start_bitspaces(user_name)
    expect_context("schema", "start_bitspaces")
    return push(make_token(user_name, "bitspaces", {}))
end

-- Low-level block form (rarely needed)
function M.start_bitspace(user_name, name, merge, base_merge)
    expect_context("bitspaces", "start_bitspace")
    local bs = {
        name = name,
        merge = merge,
        base_merge = base_merge,
        latch = false,
        clear_requires_inactive = false,
        priority_order = {},
    }
    table.insert(_bitspaces, bs)
    return push(make_token(user_name, "bitspace", {bitspace = bs}))
end

function M.latch()
    local tok = expect_context("bitspace", "latch")
    tok.data.bitspace.latch = true
end

function M.clear_requires_inactive()
    local tok = expect_context("bitspace", "clear_requires_inactive")
    tok.data.bitspace.clear_requires_inactive = true
end

function M.priority_order(...)
    local tok = expect_context("bitspace", "priority_order")
    tok.data.bitspace.priority_order = {...}
end

-- Convenience functions (one-liners, no block needed)

local function add_bitspace(name, merge, base_merge, latch, clear_req, priority_order)
    expect_context("bitspaces", "bitspace helper")
    table.insert(_bitspaces, {
        name = name,
        merge = merge,
        base_merge = base_merge,
        latch = latch or false,
        clear_requires_inactive = clear_req or false,
        priority_order = priority_order or {},
    })
end

function M.bitspace_or(name)
    add_bitspace(name, "OR")
end

function M.bitspace_or_latch(name)
    add_bitspace(name, "OR", nil, true)
end

function M.bitspace_or_latch_safe(name)
    add_bitspace(name, "OR", nil, true, true)
end

function M.bitspace_and(name)
    add_bitspace(name, "AND")
end

function M.bitspace_and_latch(name)
    add_bitspace(name, "AND", nil, true)
end

function M.bitspace_mask(name, base_merge)
    add_bitspace(name, "MASK", base_merge or "OR")
end

function M.bitspace_mask_latch(name, base_merge)
    add_bitspace(name, "MASK", base_merge or "OR", true)
end

function M.bitspace_priority(name, ...)
    add_bitspace(name, "PRIORITY", nil, false, false, {...})
end

function M.bitspace_priority_latch(name, ...)
    add_bitspace(name, "PRIORITY", nil, true, false, {...})
end

--------------------------------------------------------------------------------
-- Classes
--------------------------------------------------------------------------------

function M.start_classes(user_name)
    expect_context("schema", "start_classes")
    return push(make_token(user_name, "classes", {}))
end

-- Helper to find similar bitspace name (for typo suggestions)
local function find_similar_bitspace(name)
    local best_match = nil
    local best_score = 0
    
    for _, bs in ipairs(_bitspaces) do
        -- Simple similarity: count matching characters
        local score = 0
        local lower_name = name:lower()
        local lower_bs = bs.name:lower()
        for i = 1, math.min(#lower_name, #lower_bs) do
            if lower_name:sub(i,i) == lower_bs:sub(i,i) then
                score = score + 1
            end
        end
        -- Bonus for same length
        if #name == #bs.name then score = score + 2 end
        -- Bonus for same start
        if lower_name:sub(1,3) == lower_bs:sub(1,3) then score = score + 3 end
        
        if score > best_score then
            best_score = score
            best_match = bs.name
        end
    end
    
    return best_match, best_score
end

-- Leaf class: user defines all bank sizes explicitly
-- Format: start_class("token", "ClassName", "BANK1", size1, "BANK2", size2, ...)
function M.start_class(user_name, name, ...)
    expect_context("classes", "start_class")
    
    local args = {...}
    if #args % 2 ~= 0 then
        error(string.format("Class '%s': expected key-value pairs (got odd number of arguments)", name))
    end
    
    -- Build banks table from pairs
    local banks = {}
    local provided = {}
    
    for i = 1, #args, 2 do
        local bank_name = args[i]
        local bank_size = args[i + 1]
        
        if type(bank_name) ~= "string" then
            error(string.format("Class '%s': bank name must be string, got %s", name, type(bank_name)))
        end
        if type(bank_size) ~= "number" then
            error(string.format("Class '%s': bank '%s' size must be number, got %s", name, bank_name, type(bank_size)))
        end
        
        -- Check if valid bitspace name
        local found = false
        for _, bs in ipairs(_bitspaces) do
            if bs.name == bank_name then
                found = true
                break
            end
        end
        
        if not found then
            local suggestion, score = find_similar_bitspace(bank_name)
            if score >= 5 then
                error(string.format("Class '%s': unknown bank '%s' - did you mean '%s'?", name, bank_name, suggestion))
            else
                error(string.format("Class '%s': unknown bank '%s' (not a defined bitspace)", name, bank_name))
            end
        end
        
        banks[bank_name] = bank_size
        provided[bank_name] = true
    end
    
    -- Check for missing bitspaces
    local missing = {}
    for _, bs in ipairs(_bitspaces) do
        if not provided[bs.name] then
            table.insert(missing, bs.name)
        end
    end
    
    if #missing > 0 then
        error(string.format("Class '%s' missing banks: %s\n       (use 0 to explicitly opt-out)", 
            name, table.concat(missing, ", ")))
    end
    
    local cls = {
        name = name,
        banks = banks,
        bits = {},
        is_leaf = true,  -- Mark as leaf class (user-defined)
    }
    _current_class = cls
    table.insert(_classes, cls)
    return push(make_token(user_name, "class", {class = cls}))
end

-- Remove bank() - no longer needed, sizes in start_class
-- function M.bank() removed

-- Remove default_mask() - masks are runtime only now
-- function M.default_mask() removed

--------------------------------------------------------------------------------
-- Bits (within class)
--------------------------------------------------------------------------------

function M.start_bits(user_name, bitspace_name)
    expect_context("class", "start_bits")
    _current_bits = {
        bitspace_name = bitspace_name,
        bits = {},
    }
    return push(make_token(user_name, "bits", {bits_def = _current_bits}))
end

function M.bit(name)
    expect_context("bits", "bit")
    table.insert(_current_bits.bits, name)
end

local function finalize_bits(tok)
    local bits_def = tok.data.bits_def
    _current_class.bits[bits_def.bitspace_name] = bits_def.bits
    _current_bits = nil
end

--------------------------------------------------------------------------------
-- Nodes
--------------------------------------------------------------------------------

function M.start_nodes(user_name)
    expect_context("schema", "start_nodes")
    _current_node_stack = {}
    return push(make_token(user_name, "nodes", {}))
end

function M.start_node(user_name, name, class_name)
    -- Can be in "nodes" section or nested in another "node"
    local tok = current_token()
    if not tok or (tok.token_type ~= "nodes" and tok.token_type ~= "node") then
        error(string.format(
            "start_node requires 'nodes' or 'node' context, but current context is '%s'",
            tok and tok.token_type or "none"
        ))
    end
    
    -- Build full path
    table.insert(_current_node_stack, name)
    local path = table.concat(_current_node_stack, ".")
    
    local node = {
        path = path,
        class = class_name,
        config = {},
    }
    table.insert(_nodes, node)
    
    return push(make_token(user_name, "node", {node = node, name = name}))
end

function M.config(key, value)
    local tok = expect_context("node", "config")
    tok.data.node.config[key] = value
end

local function finalize_node(tok)
    -- Pop from path stack
    table.remove(_current_node_stack)
    
    -- Merge node config into global config
    local node = tok.data.node
    if next(node.config) then
        _config[node.path] = node.config
    end
end

--------------------------------------------------------------------------------
-- End (universal)
--------------------------------------------------------------------------------

function M.end_(token)
    local tok = pop(token)
    
    -- Finalization hooks
    if tok.token_type == "bits" then
        finalize_bits(tok)
    elseif tok.token_type == "class" then
        _current_class = nil
    elseif tok.token_type == "node" then
        finalize_node(tok)
    end
    
    return tok
end

--------------------------------------------------------------------------------
-- Build Final Schema
--------------------------------------------------------------------------------

function M.build()
    -- Check for unclosed blocks
    if #_stack > 0 then
        local unclosed = {}
        for _, tok in ipairs(_stack) do
            table.insert(unclosed, string.format("'%s' (%s, type=%s)", 
                tok.user_name, tok.id, tok.token_type))
        end
        error("Unclosed blocks: " .. table.concat(unclosed, ", "))
    end
    
    -- Build class lookup
    local class_by_name = {}
    for _, cls in ipairs(_classes) do
        class_by_name[cls.name] = cls
    end
    
    -- Identify which classes are used at leaf positions vs aggregate positions
    -- A node is a leaf if it has no children
    local leaf_classes = {}      -- classes used at leaf positions
    local aggregate_classes = {} -- classes used at non-leaf positions
    
    -- First pass: identify parent-child relationships
    local node_children = {}  -- node path -> list of child paths
    for _, node in ipairs(_nodes) do
        node_children[node.path] = {}
    end
    
    for _, node in ipairs(_nodes) do
        -- Find parent path (everything before last dot)
        local parent_path = node.path:match("(.+)%.[^.]+$")
        if parent_path and node_children[parent_path] then
            table.insert(node_children[parent_path], node.path)
        end
    end
    
    -- Second pass: classify nodes as leaf or aggregate
    for _, node in ipairs(_nodes) do
        local is_leaf = #node_children[node.path] == 0
        if is_leaf then
            leaf_classes[node.class] = true
        else
            aggregate_classes[node.class] = true
        end
    end
    
    -- Validate leaf classes are defined
    for class_name, _ in pairs(leaf_classes) do
        if not class_by_name[class_name] then
            error(string.format("Leaf class '%s' not defined", class_name))
        end
    end
    
    -- Auto-generate aggregate classes
    -- For each aggregate class, compute bank sizes as max of children
    local function compute_aggregate_banks(node_path)
        local max_banks = {}
        for _, bs in ipairs(_bitspaces) do
            max_banks[bs.name] = 0
        end
        
        local children = node_children[node_path]
        for _, child_path in ipairs(children) do
            -- Find child node
            local child_node = nil
            for _, n in ipairs(_nodes) do
                if n.path == child_path then
                    child_node = n
                    break
                end
            end
            
            if child_node then
                local child_class = class_by_name[child_node.class]
                if child_class then
                    -- Take max of child's banks
                    for bs_name, size in pairs(child_class.banks) do
                        if size > (max_banks[bs_name] or 0) then
                            max_banks[bs_name] = size
                        end
                    end
                else
                    -- Child class not defined yet - recurse to compute it first
                    -- This handles nested aggregates
                    local child_banks = compute_aggregate_banks(child_path)
                    for bs_name, size in pairs(child_banks) do
                        if size > (max_banks[bs_name] or 0) then
                            max_banks[bs_name] = size
                        end
                    end
                end
            end
        end
        
        return max_banks
    end
    
    -- Process nodes bottom-up to compute aggregate classes
    -- Sort by depth (deepest first)
    local nodes_by_depth = {}
    for _, node in ipairs(_nodes) do
        local depth = select(2, node.path:gsub("%.", "")) + 1
        nodes_by_depth[#nodes_by_depth + 1] = {node = node, depth = depth}
    end
    table.sort(nodes_by_depth, function(a, b) return a.depth > b.depth end)
    
    -- Generate aggregate classes
    for _, item in ipairs(nodes_by_depth) do
        local node = item.node
        local is_aggregate = aggregate_classes[node.class]
        
        if is_aggregate and not class_by_name[node.class] then
            -- Auto-generate this aggregate class
            local banks = compute_aggregate_banks(node.path)
            local cls = {
                name = node.class,
                banks = banks,
                bits = {},
                is_leaf = false,
            }
            table.insert(_classes, cls)
            class_by_name[node.class] = cls
        end
    end
    
    -- Build the schema table that codegen.lua expects
    local schema = {
        name = _schema.name,
        version = _schema.version,
        options = _options,
        bitspaces = _bitspaces,
        classes = _classes,
        nodes = _nodes,
        config = _config,
    }
    
    return schema
end

--------------------------------------------------------------------------------
-- Convenience: Get current path (for debugging)
--------------------------------------------------------------------------------

function M.current_path()
    return table.concat(_current_node_stack, ".")
end

function M.stack_depth()
    return #_stack
end

return M