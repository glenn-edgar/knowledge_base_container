--[[
    Test driver for Construct_Data_Tables (LuaJIT)
    Mirrors the Python __main__ test block.
    
    Usage:
        luajit test_construct_data_tables.lua <database_file.db> [upload_flag] [unit_test]
    
    Examples:
        luajit test_construct_data_tables.lua knowledge_base.db
        luajit test_construct_data_tables.lua knowledge_base.db True True
--]]

local Construct_Data_Tables = require('construct_data_tables')
local json = require('sqlite3_helpers').json

local DATABASE = "knowledge_base"

-- Parse command-line arguments
if #arg < 1 then
    print("Usage: luajit test_construct_data_tables.lua <database_file.db> [upload_flag: True/False] [unit_test: True/False]")
    print("Example: luajit test_construct_data_tables.lua knowledge_base.db True")
    os.exit(1)
end

print(string.format("args: %s", json.encode(arg)))
local db_file = arg[1]

local upload_flag = false
if arg[2] and arg[2] == "True" then
    upload_flag = true
end

local unit_test = false
if arg[3] and arg[3] == "True" then
    unit_test = true
end

-- ============================================================
-- Test 1: Complete functionality test
-- ============================================================
print(string.rep("=", 70))
print("Test 1: Complete functionality test")
print(string.rep("=", 70))

local kb = Construct_Data_Tables.new(db_file, DATABASE, nil, upload_flag)

if not upload_flag then
    print("\nInitial state:")
    print(string.format("Path: %s", json.encode(kb.path)))

    kb:add_kb("kb1", "First knowledge base")
    kb:select_kb("kb1")

    kb:add_header_node("header1_link", "header1_name", { prop1 = "val1" }, { data = "header1_data" })
    print("\nAfter add_header_node:")
    print(string.format("Path: %s", json.encode(kb.path)))

    kb:add_info_node("info1_link", "info1_name", { prop2 = "val2" }, { data = "info1_data" })
    print("\nAfter add_info_node:")
    print(string.format("Path: %s", json.encode(kb.path)))

    kb:add_rpc_server_field("info1_server", 25, "info1_server_data")
    kb:add_status_field("info1_status", { prop3 = "val3" }, "info1_status_description", { prop3 = "val3" })
    kb:add_status_field("info2_status", { prop3 = "val3" }, "info2_status_description", { prop3 = "val3" })
    kb:add_status_field("info3_status", { prop3 = "val3" }, "info3_status_description", { prop3 = "val3" })
    kb:add_job_field("info1_job", 100, "info1_job_description")
    kb:add_stream_field("info1_stream", 95, "info1_stream")
    kb:add_rpc_client_field("info1_client", 10, "info1_client_description")
    kb:add_link_mount("info1_link_mount", "info1_link_mount_description")

    kb:leave_header_node("header1_link", "header1_name")
    print("\nAfter leave_header_node:")
    print(string.format("Path: %s", json.encode(kb.path)))

    kb:add_header_node("header2_link", "header2_name", { prop3 = "val3" }, { data = "header2_data" })
    kb:add_info_node("info2_link", "info2_name", { prop4 = "val4" }, { data = "info2_data" })
    kb:add_link_node("info1_link_mount")

    kb:clear_bit_mask_flags()
    kb:add_bit_mask_flag("A", 0, "A_description")
    kb:add_bit_mask_flag("B", 1, "B_description")
    kb:add_bit_mask_flag("C", 2, "C_description")
    kb:add_bit_mask_flag("D", 3, "D_description")
    kb:add_bit_mask_flag("E", 4, "E_description")
    kb:create_bit_mask_entry("user_2", "info2_bit_mask", 5, 0, "info2_bit_mask_description")

    kb:clear_bit_mask_flags()
    kb:add_bit_mask_flag("F", 0, "F_description")
    kb:add_bit_mask_flag("G", 1, "G_description")
    kb:add_bit_mask_flag("H", 2, "H_description")
    kb:add_bit_mask_flag("I", 3, "I_description")
    kb:add_bit_mask_flag("J", 4, "J_description")
    kb:create_bit_mask_entry("user_1", "info1_bit_mask", 5, 0, "info1_bit_mask_description")

    kb:leave_header_node("header2_link", "header2_name")
    print("\nAfter adding and leaving another header node:")
    print(string.format("Path: %s", json.encode(kb.path)))
end

-- Check installation
local ok, err = pcall(function()
    kb:check_installation()
    print("\n✓ Test 1 check_installation passed")
    kb:disconnect()
    print("✓ Test 1 completed successfully")
end)
if not ok then
    print(string.format("✗ Error during installation check: %s", err))
end

if not unit_test then
    os.exit(0)
end

-- ============================================================
-- Test 2: Modified fields test
-- ============================================================
print("\n" .. string.rep("=", 70))
print("Test 2: Modified fields test")
print(string.rep("=", 70))

kb = Construct_Data_Tables.new(db_file, DATABASE, nil, upload_flag)

print("\nInitial state:")
print(string.format("Path: %s", json.encode(kb.path)))

kb:add_kb("kb1", "First knowledge base")
kb:select_kb("kb1")

kb:add_header_node("header1_link", "header1_name", { prop1 = "val1" }, { data = "header1_data" })
print("\nAfter add_header_node:")
print(string.format("Path: %s", json.encode(kb.path)))

kb:add_info_node("info1_link", "info1_name", { prop2 = "val2" }, { data = "info1_data" })
print("\nAfter add_info_node:")
print(string.format("Path: %s", json.encode(kb.path)))

kb:add_rpc_server_field("info1_server", 25, "info1_server_data")
kb:add_status_field("info1_status", { prop3 = "val3" }, "info1_status_description", { prop3 = "val3" })
kb:add_status_field("info2_status", { prop3 = "val3" }, "info2_status_description", { prop3 = "val3" })
kb:add_status_field("info3_status", { prop3 = "val3" }, "info3_status_description", { prop3 = "val3" })

kb:add_job_field("info2_job", 100, "info1_job_description")
kb:add_stream_field("info2_status", 100, "info1_stream")
kb:add_rpc_client_field("info2_client", 10, "info1_client_description")

kb:leave_header_node("header1_link", "header1_name")
print("\nAfter leave_header_node:")
print(string.format("Path: %s", json.encode(kb.path)))

kb:add_header_node("header2_link", "header2_name", { prop3 = "val3" }, { data = "header2_data" })
kb:add_info_node("info2_link", "info2_name", { prop4 = "val4" }, { data = "info2_data" })
kb:leave_header_node("header2_link", "header2_name")
print("\nAfter adding and leaving another header node:")
print(string.format("Path: %s", json.encode(kb.path)))

ok, err = pcall(function()
    kb:check_installation()
    print("\n✓ Test 2 check_installation passed")
    kb:disconnect()
    print("✓ Test 2 completed successfully")
end)
if not ok then
    print(string.format("✗ Error during installation check: %s", err))
end

-- ============================================================
-- Test 3: Reduced queue sizes test
-- ============================================================
print("\n" .. string.rep("=", 70))
print("Test 3: Reduced queue sizes test")
print(string.rep("=", 70))

kb = Construct_Data_Tables.new(db_file, DATABASE, nil, upload_flag)

print("\nInitial state:")
print(string.format("Path: %s", json.encode(kb.path)))

kb:add_kb("kb1", "Second knowledge base")
kb:select_kb("kb1")

kb:add_header_node("header1_link", "header1_name", { prop1 = "val1" }, { data = "header1_data" })
print("\nAfter add_header_node:")
print(string.format("Path: %s", json.encode(kb.path)))

kb:add_info_node("info1_link", "info1_name", { prop2 = "val2" }, { data = "info1_data" })
print("\nAfter add_info_node:")
print(string.format("Path: %s", json.encode(kb.path)))

kb:add_rpc_server_field("info1_server", 25, "info1_server_data")

kb:add_job_field("info1_job", 50, "info1_job_description")
kb:add_stream_field("info1_status", 50, "info1_stream")
kb:add_rpc_client_field("info1_client", 5, "info1_client_description")

kb:leave_header_node("header1_link", "header1_name")
print("\nAfter leave_header_node:")
print(string.format("Path: %s", json.encode(kb.path)))

kb:add_header_node("header2_link", "header2_name", { prop3 = "val3" }, { data = "header2_data" })
kb:add_info_node("info2_link", "info2_name", { prop4 = "val4" }, { data = "info2_data" })
kb:leave_header_node("header2_link", "header2_name")
print("\nAfter adding and leaving another header node:")
print(string.format("Path: %s", json.encode(kb.path)))

ok, err = pcall(function()
    kb:check_installation()
    print("\n✓ Test 3 check_installation passed")
    kb:disconnect()
    print("✓ Test 3 completed successfully")
end)
if not ok then
    print(string.format("✗ Error during installation check: %s", err))
end

print("\n" .. string.rep("=", 70))
print("All tests completed!")
print(string.rep("=", 70))
print(string.format("\nDatabase file: %s", db_file))
print("You can inspect it with: sqlite3 " .. db_file)


