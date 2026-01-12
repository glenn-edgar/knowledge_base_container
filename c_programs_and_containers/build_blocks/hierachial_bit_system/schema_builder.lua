--[[
  schema_builder.lua - Simplified DSL for Hierarchical Bit Map
  
  Three buffer types only:
    1. OR_LATCH  - OR merge, bits latch until cleared
    2. OR_MASK   - OR merge with mask controlling participation  
    3. AND       - AND merge, no mask, no latch
    
  Usage:
    local S = require("schema_builder")
    
    S.schema("MySchema", "1.0.0")
    
    S.buffer("alarms", "OR_LATCH")
    S.buffer("enables", "OR_MASK") 
    S.buffer("ready", "AND")
    
    S.class("Valve", {alarms = 8, enables = 4, ready = 2})
      S.bits("alarms", "leak", "stuck", "overtemp")
      S.bits("enables", "manual", "auto")
    S.end_class()
    
    S.node("system", "System")
      S.node("valve1", "Valve")
      S.node("valve2", "Valve")
    S.end_node()
    
    return S.build()
]]

local M = {}

--------------------------------------------------------------------------------
-- State
--------------------------------------------------------------------------------

local _schema = nil
local _buffers = {}           -- Array of buffer definitions
local _buffer_by_name = {}    -- name -> buffer
local _classes = {}           -- Array of class definitions  
local _class_by_name = {}     -- name -> class
local _nodes = {}             -- Array of node definitions
local _current_class = nil    -- Class being defined
local _node_stack = {}        -- Stack of node names for path building
local _errors = {}            -- Collected errors

--------------------------------------------------------------------------------
-- Error Handling
--------------------------------------------------------------------------------

local function add_error(fmt, ...)
    table.insert(_errors, string.format(fmt, ...))
end

local function check_errors()
    if #_errors > 0 then
        error("Schema errors:\n  " .. table.concat(_errors, "\n  "))
    end
end

--------------------------------------------------------------------------------
-- Reset
--------------------------------------------------------------------------------

function M.reset()
    _schema = nil
    _buffers = {}
    _buffer_by_name = {}
    _classes = {}
    _class_by_name = {}
    _nodes = {}
    _current_class = nil
    _node_stack = {}
    _errors = {}
end

M.reset()

--------------------------------------------------------------------------------
-- Schema Definition
--------------------------------------------------------------------------------

function M.schema(name, version)
    M.reset()
    _schema = {
        name = name,
        version = version or "1.0.0"
    }
end

--------------------------------------------------------------------------------
-- Buffer Types
--------------------------------------------------------------------------------

local VALID_BUFFER_TYPES = {
    OR_LATCH = true,
    OR_MASK = true,
    AND = true,
}

function M.buffer(name, buffer_type)
    if not _schema then
        add_error("buffer() called before schema()")
        return
    end
    
    if _buffer_by_name[name] then
        add_error("Duplicate buffer name: '%s'", name)
        return
    end
    
    if not VALID_BUFFER_TYPES[buffer_type] then
        add_error("Invalid buffer type '%s' for buffer '%s'. Valid: OR_LATCH, OR_MASK, AND", 
                  buffer_type, name)
        return
    end
    
    local buf = {
        index = #_buffers,
        name = name,
        type = buffer_type,
    }
    
    table.insert(_buffers, buf)
    _buffer_by_name[name] = buf
end

--------------------------------------------------------------------------------
-- Classes
--------------------------------------------------------------------------------

-- M.class("ClassName", {buffer1 = size1, buffer2 = size2, ...})
function M.class(name, banks)
    if not _schema then
        add_error("class() called before schema()")
        return
    end
    
    if _current_class then
        add_error("class() called while already defining class '%s'", _current_class.name)
        return
    end
    
    if _class_by_name[name] then
        add_error("Duplicate class name: '%s'", name)
        return
    end
    
    -- Validate all buffer names
    local validated_banks = {}
    for buf_name, size in pairs(banks) do
        if not _buffer_by_name[buf_name] then
            add_error("Class '%s' references unknown buffer '%s'", name, buf_name)
        else
            if type(size) ~= "number" or size < 0 or size ~= math.floor(size) then
                add_error("Class '%s' buffer '%s' size must be non-negative integer, got: %s",
                          name, buf_name, tostring(size))
            else
                validated_banks[buf_name] = size
            end
        end
    end
    
    -- Check all buffers are specified
    for _, buf in ipairs(_buffers) do
        if validated_banks[buf.name] == nil then
            add_error("Class '%s' missing buffer '%s' (use 0 to opt out)", name, buf.name)
        end
    end
    
    local cls = {
        index = #_classes,
        name = name,
        banks = validated_banks,
        bits = {},  -- buffer_name -> {bit_names}
    }
    
    table.insert(_classes, cls)
    _class_by_name[name] = cls
    _current_class = cls
end

function M.bits(buffer_name, ...)
    if not _current_class then
        add_error("bits() called outside of class definition")
        return
    end
    
    if not _buffer_by_name[buffer_name] then
        add_error("bits() references unknown buffer '%s'", buffer_name)
        return
    end
    
    local bank_size = _current_class.banks[buffer_name] or 0
    local bit_names = {...}
    
    if #bit_names > bank_size then
        add_error("Class '%s' buffer '%s' has %d bits but %d names provided",
                  _current_class.name, buffer_name, bank_size, #bit_names)
        return
    end
    
    _current_class.bits[buffer_name] = bit_names
end

function M.end_class()
    if not _current_class then
        add_error("end_class() called but no class being defined")
        return
    end
    _current_class = nil
end

--------------------------------------------------------------------------------
-- Nodes (Tree Structure)
--------------------------------------------------------------------------------

function M.node(name, class_name)
    if not _schema then
        add_error("node() called before schema()")
        return
    end
    
    if _current_class then
        add_error("node() called while defining class '%s'", _current_class.name)
        return
    end
    
    -- Build path
    table.insert(_node_stack, name)
    local path = table.concat(_node_stack, ".")
    
    -- Validate class exists (defer full validation to build)
    local node = {
        path = path,
        name = name,
        class_name = class_name,
        depth = #_node_stack - 1,
    }
    
    table.insert(_nodes, node)
end

function M.end_node()
    if #_node_stack == 0 then
        add_error("end_node() called but no node being defined")
        return
    end
    table.remove(_node_stack)
end

--------------------------------------------------------------------------------
-- Build and Validate
--------------------------------------------------------------------------------

function M.build()
    -- Check for unclosed structures
    if _current_class then
        add_error("Unclosed class: '%s'", _current_class.name)
    end
    
    if #_node_stack > 0 then
        add_error("Unclosed nodes: %s", table.concat(_node_stack, "."))
    end
    
    -- Build parent-child relationships
    local node_by_path = {}
    for _, node in ipairs(_nodes) do
        node_by_path[node.path] = node
    end
    
    -- Find parent paths and identify leaf vs aggregate nodes
    local children_count = {}  -- path -> number of children
    for _, node in ipairs(_nodes) do
        children_count[node.path] = 0
    end
    
    for _, node in ipairs(_nodes) do
        -- Find parent path
        local parent_path = node.path:match("(.+)%.[^.]+$")
        if parent_path then
            if not node_by_path[parent_path] then
                add_error("Node '%s' parent path '%s' not found", node.path, parent_path)
            else
                node.parent_path = parent_path
                children_count[parent_path] = (children_count[parent_path] or 0) + 1
            end
        end
    end
    
    -- Identify leaf nodes and validate their classes
    local leaf_classes = {}      -- classes used at leaf positions
    local aggregate_classes = {} -- classes used at non-leaf positions
    
    for _, node in ipairs(_nodes) do
        local is_leaf = children_count[node.path] == 0
        node.is_leaf = is_leaf
        
        if is_leaf then
            leaf_classes[node.class_name] = true
            -- Leaf nodes MUST have defined class
            if not _class_by_name[node.class_name] then
                add_error("Leaf node '%s' uses undefined class '%s'", node.path, node.class_name)
            end
        else
            aggregate_classes[node.class_name] = true
        end
    end
    
    check_errors()
    
    -- Auto-generate aggregate classes by computing max of children
    local function compute_aggregate_banks(node_path)
        local max_banks = {}
        for _, buf in ipairs(_buffers) do
            max_banks[buf.name] = 0
        end
        
        for _, node in ipairs(_nodes) do
            if node.parent_path == node_path then
                local child_class = _class_by_name[node.class_name]
                if child_class then
                    for buf_name, size in pairs(child_class.banks) do
                        if size > (max_banks[buf_name] or 0) then
                            max_banks[buf_name] = size
                        end
                    end
                else
                    -- Recurse for nested aggregates
                    local child_banks = compute_aggregate_banks(node.path)
                    for buf_name, size in pairs(child_banks) do
                        if size > (max_banks[buf_name] or 0) then
                            max_banks[buf_name] = size
                        end
                    end
                end
            end
        end
        
        return max_banks
    end
    
    -- Process nodes deepest first
    local sorted_nodes = {}
    for _, node in ipairs(_nodes) do
        table.insert(sorted_nodes, node)
    end
    table.sort(sorted_nodes, function(a, b) return a.depth > b.depth end)
    
    for _, node in ipairs(sorted_nodes) do
        if aggregate_classes[node.class_name] and not _class_by_name[node.class_name] then
            local banks = compute_aggregate_banks(node.path)
            local cls = {
                index = #_classes,
                name = node.class_name,
                banks = banks,
                bits = {},
                is_auto = true,
            }
            table.insert(_classes, cls)
            _class_by_name[node.class_name] = cls
        end
    end
    
    -- Final validation
    for _, node in ipairs(_nodes) do
        if not _class_by_name[node.class_name] then
            add_error("Node '%s' uses undefined class '%s'", node.path, node.class_name)
        end
    end
    
    check_errors()
    
    -- Build final schema
    return {
        name = _schema.name,
        version = _schema.version,
        buffers = _buffers,
        classes = _classes,
        nodes = _nodes,
    }
end

--------------------------------------------------------------------------------
-- Debug Helpers
--------------------------------------------------------------------------------

function M.dump()
    print("=== Schema: " .. (_schema and _schema.name or "(none)") .. " ===")
    print("\nBuffers:")
    for _, buf in ipairs(_buffers) do
        print(string.format("  [%d] %s (%s)", buf.index, buf.name, buf.type))
    end
    print("\nClasses:")
    for _, cls in ipairs(_classes) do
        print(string.format("  [%d] %s%s", cls.index, cls.name, cls.is_auto and " (auto)" or ""))
        for buf_name, size in pairs(cls.banks) do
            local bits_str = ""
            if cls.bits[buf_name] and #cls.bits[buf_name] > 0 then
                bits_str = " -> " .. table.concat(cls.bits[buf_name], ", ")
            end
            print(string.format("      %s: %d bits%s", buf_name, size, bits_str))
        end
    end
    print("\nNodes:")
    for _, node in ipairs(_nodes) do
        local indent = string.rep("  ", node.depth + 1)
        print(string.format("%s%s (%s)%s", indent, node.path, node.class_name,
                            node.is_leaf and " [leaf]" or ""))
    end
end

return M