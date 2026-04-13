--[[
    Unit test for Construct_KB (LuaJIT)
    Mirrors the Python __main__ test block.
    
    Usage:
        luajit test_construct_kb.lua
--]]

local Construct_KB = require('construct_kb')

local DB_PATH  = 'knowledge_base.db'
local DB_TABLE = 'knowledge_base'

print("starting unit test")

-- Initialize with auto-detection of ltree extension
local kb = Construct_KB.new(DB_PATH, DB_TABLE)

-- Test with first knowledge base
kb:add_kb('kb1', 'First knowledge base')
kb:select_kb('kb1')
kb:add_header_node('header1_link', 'header1_name', { prop1 = 'val1' }, { data = 'header1_data' })

kb:add_info_node('info1_link', 'info1_name', { prop2 = 'val2' }, { data = 'info1_data' })

kb:leave_header_node('header1_link', 'header1_name')

kb:add_header_node('header2_link', 'header2_name', { prop3 = 'val3' }, { data = 'header2_data' })
kb:add_info_node('info2_link', 'info2_name', { prop4 = 'val4' }, { data = 'info2_data' })
kb:add_link_mount('link1', 'link1 description')
kb:leave_header_node('header2_link', 'header2_name')

-- Test with second knowledge base
kb:add_kb('kb2', 'Second knowledge base')
kb:select_kb('kb2')
kb:add_header_node('header1_link', 'header1_name', { prop1 = 'val1' }, { data = 'header1_data' })

kb:add_info_node('info1_link', 'info1_name', { prop2 = 'val2' }, { data = 'info1_data' })

kb:leave_header_node('header1_link', 'header1_name')

kb:add_header_node('header2_link', 'header2_name', { prop3 = 'val3' }, { data = 'header2_data' })
kb:add_info_node('info2_link', 'info2_name', { prop4 = 'val4' }, { data = 'info2_data' })
kb:add_link_node('link1')
kb:leave_header_node('header2_link', 'header2_name')

-- Check installation
local ok, err = pcall(function()
    kb:check_installation()
    print("✓ Installation check passed")
    kb:disconnect()
    print("✓ Database connection closed")
end)

if not ok then
    print(string.format("Error during installation check: %s", err))
end

print("ending unit test")

