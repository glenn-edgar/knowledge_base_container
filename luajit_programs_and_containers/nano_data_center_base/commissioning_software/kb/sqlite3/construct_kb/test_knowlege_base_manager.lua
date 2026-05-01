--[[
    Unit test for KnowledgeBaseManager (LuaJIT)
    Mirrors the Python __main__ test block.
    
    Usage:
        luajit test_knowledge_base.lua
--]]

local KBM = require('knowledge_base_manager')

local db_path = 'knowledge_base.db'

-- Auto-detect ltree extension (checks ./ltree, /usr/local/lib/ltree, /usr/lib/ltree)
local kb_manager = KBM.new('knowledge_base', db_path)

print("Starting unit test")

local ok, err = pcall(function()

    -- Add knowledge bases
    kb_manager:add_kb('kb1', 'First knowledge base')
    kb_manager:add_kb('kb2', 'Second knowledge base')

    -- Add nodes with hierarchical paths
    kb_manager:add_node('kb1', 'person', 'John Doe',
        { age = 30 }, { email = 'john@example.com' }, 'people.john')
    kb_manager:add_node('kb1', 'person', 'Jane Doe',
        { age = 28 }, { email = 'jane@example.com' }, 'people.jane')
    kb_manager:add_node('kb1', 'child', 'Little John',
        { age = 5 }, { parent = 'john' }, 'people.john.children.little_john')

    kb_manager:add_node('kb2', 'gate', 'Root Gate',
        { type = 'selector' }, {}, 'kb.second_test.GATE_root._0')
    kb_manager:add_node('kb2', 'collection', 'Wait Collection',
        { type = 'wait' }, {}, 'kb.second_test.GATE_root._0.COL_wait._1')

    -- Add link mount
    kb_manager:add_link_mount('kb1', 'people.john', 'link1', 'link1 description')

    -- Add link
    kb_manager:add_link('kb1', 'people.john', 'link1')

    print("\n=== Testing ltree queries ===")

    -- Test pattern matching
    print("\n1. Find all nodes with 'people' in path:")
    local results = kb_manager:find_by_pattern('people.*', 'kb1')
    for _, row in ipairs(results) do
        print(string.format("   %s - %s", row.path, row.name))
    end

    -- Test wildcards
    print("\n2. Find nodes matching kb.*.GATE*.*:")
    results = kb_manager:find_by_pattern('kb.*.GATE*.*', 'kb2')
    for _, row in ipairs(results) do
        print(string.format("   %s - %s", row.path, row.name))
    end

    -- Test descendants
    print("\n3. Find all descendants of 'people.john':")
    results = kb_manager:find_descendants('people.john', 'kb1')
    for _, row in ipairs(results) do
        print(string.format("   %s - %s", row.path, row.name))
    end

    -- Test depth
    print("\n4. Get depth of 'people.john.children.little_john':")
    local depth = kb_manager:get_node_depth('people.john.children.little_john')
    print(string.format("   Depth: %d", depth))

    -- Test find by depth
    print("\n5. Find all nodes at depth 2:")
    results = kb_manager:find_by_depth(2, 'kb1')
    for _, row in ipairs(results) do
        print(string.format("   %s - %s", row.path, row.name))
    end

    -- Test find children
    print("\n6. Find immediate children of 'people':")
    results = kb_manager:find_children('people', 'kb1')
    for _, row in ipairs(results) do
        print(string.format("   %s - %s", row.path, row.name))
    end

    print("\nUnit test completed successfully")
end)

if not ok then
    print(string.format("Unit test failed: %s", err))
end

kb_manager:disconnect()
print("Ending unit test")

