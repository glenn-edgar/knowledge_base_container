#!/usr/bin/env luajit
--[[
  test_construct_data_tables.lua
  
  Standalone test driver for construct_data_tables.lua
  Translated from the if __name__ == "__main__" block in construct_data_tables.py
  
  Usage:
    POSTGRES_PASSWORD=mypass luajit test_construct_data_tables.lua [upload_flag] [unit_test]
    
    Examples:
      luajit test_construct_data_tables.lua                  -- fresh build, no unit test
      luajit test_construct_data_tables.lua True             -- upload mode (skip table creation)
      luajit test_construct_data_tables.lua False True       -- fresh build + full unit tests
]]

local Construct_Data_Tables = require("construct_data_tables")

---------------------------------------------------------------------------
-- Parse arguments
---------------------------------------------------------------------------

local password = os.getenv("POSTGRES_PASSWORD")
if not password then
  error("POSTGRES_PASSWORD environment variable is not set")
end

local upload_flag = false
local unit_test   = false

if arg[1] == "True" then upload_flag = true end
if arg[2] == "True" then unit_test   = true end

local DB_HOST     = "localhost"
local DB_PORT     = "5432"
local DB_NAME     = "knowledge_base"
local DB_USER     = "gedgar"
local DB_PASSWORD = password
local DATABASE    = "knowledge_base"

---------------------------------------------------------------------------
-- Helper: dump path table
---------------------------------------------------------------------------
local function dump_path(path)
  local parts = {}
  for kb_name, segments in pairs(path) do
    parts[#parts + 1] = kb_name .. ": {" .. table.concat(segments, ", ") .. "}"
  end
  return "{" .. table.concat(parts, ", ") .. "}"
end

---------------------------------------------------------------------------
-- First run (may be upload-only or full build)
---------------------------------------------------------------------------

local kb = Construct_Data_Tables.new(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, DATABASE, upload_flag)

if not upload_flag then
  print("Initial state:")
  print("Path: " .. dump_path(kb.path))

  kb:add_kb("kb1", "First knowledge base")
  kb:select_kb("kb1")
  kb:add_header_node("header1_link", "header1_name", { prop1 = "val1" }, { data = "header1_data" })
  print("\nAfter add_header_node:")
  print("Path: " .. dump_path(kb.path))

  kb:add_info_node("info1_link", "info1_name", { prop2 = "val2" }, { data = "info1_data" })
  print("\nAfter add_info_node:")
  print("Path: " .. dump_path(kb.path))

  kb:add_rpc_server_field("info1_server", 25, "info1_server_data")
  kb:add_status_field("info1_status", { prop3 = "val3" }, "info1_status_description", { prop3 = "val3" })
  kb:add_status_field("info2_status", { prop3 = "val3" }, "info2_status_description", { prop3 = "val3" })
  kb:add_status_field("info3_status", { prop3 = "val3" }, "info3_status_description", { prop3 = "val3" })
  kb:add_job_field("info1_job", 100, "info1_job_description")
  kb:add_stream_field("info1_stream", 95, "info1_stream")
  kb:add_rpc_client_field("info1_client", 10, "info1_client_description")
  kb:add_jsonb_field("info1_jsonb", "type_1", "info1_jsonb_description", { data = "info1_jsonb_data" })
  kb:add_jsonb_field("info2_jsonb", "type_1", "info2_jsonb_description", { data = "info2_jsonb_data" })
  kb:add_jsonb_field("info3_jsonb", "type_2", "info3_jsonb_description", { data = "info3_jsonb_data" })
  kb:add_link_mount("info1_link_mount", "info1_link_mount_description")

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

  kb:leave_header_node("header1_link", "header1_name")
  print("\nAfter leave_header_node:")
  print("Path: " .. dump_path(kb.path))

  kb:add_header_node("header2_link", "header2_name", { prop3 = "val3" }, { data = "header2_data" })
  kb:add_info_node("info2_link", "info2_name", { prop4 = "val4" }, { data = "info2_data" })
  kb:add_link_node("info1_link_mount")
  kb:leave_header_node("header2_link", "header2_name")
  print("\nAfter adding and leaving another header node:")
  print("Path: " .. dump_path(kb.path))
end

-- Check installation
local ok, err = pcall(function()
  kb:check_installation()
  kb:disconnect()
end)
if not ok then
  print("Error during installation check: " .. tostring(err))
end

if not unit_test then
  os.exit(0)
end

---------------------------------------------------------------------------
-- Second run (unit test: modified fields)
---------------------------------------------------------------------------

kb = Construct_Data_Tables.new(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, DATABASE)

print("Initial state:")
print("Path: " .. dump_path(kb.path))

kb:add_kb("kb1", "First knowledge base")
kb:select_kb("kb1")
kb:add_header_node("header1_link", "header1_name", { prop1 = "val1" }, { data = "header1_data" })
print("\nAfter add_header_node:")
print("Path: " .. dump_path(kb.path))

kb:add_info_node("info1_link", "info1_name", { prop2 = "val2" }, { data = "info1_data" })
print("\nAfter add_info_node:")
print("Path: " .. dump_path(kb.path))

kb:add_rpc_server_field("info1_server", 25, "info1_server_data")
kb:add_status_field("info1_status", { prop3 = "val3" }, "info1_status_description", { prop3 = "val3" })
kb:add_status_field("info2_status", { prop3 = "val3" }, "info2_status_description", { prop3 = "val3" })
kb:add_status_field("info3_status", { prop3 = "val3" }, "info3_status_description", { prop3 = "val3" })

kb:add_job_field("info2_job", 100, "info1_job_description")
kb:add_stream_field("info2_status", 100, "info1_stream")
kb:add_rpc_client_field("info2_client", 10, "info1_client_description")

kb:leave_header_node("header1_link", "header1_name")
print("\nAfter leave_header_node:")
print("Path: " .. dump_path(kb.path))

kb:add_header_node("header2_link", "header2_name", { prop3 = "val3" }, { data = "header2_data" })
kb:add_info_node("info2_link", "info2_name", { prop4 = "val4" }, { data = "info2_data" })
kb:leave_header_node("header2_link", "header2_name")
print("\nAfter adding and leaving another header node:")
print("Path: " .. dump_path(kb.path))

ok, err = pcall(function()
  kb:check_installation()
  kb:disconnect()
end)
if not ok then
  print("Error during installation check: " .. tostring(err))
end

---------------------------------------------------------------------------
-- Third run (unit test: reduced sizes)
---------------------------------------------------------------------------

kb = Construct_Data_Tables.new(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, DATABASE)

print("Initial state:")
print("Path: " .. dump_path(kb.path))

kb:add_kb("kb1", "First knowledge base")
kb:select_kb("kb1")
kb:add_header_node("header1_link", "header1_name", { prop1 = "val1" }, { data = "header1_data" })
print("\nAfter add_header_node:")
print("Path: " .. dump_path(kb.path))

kb:add_info_node("info1_link", "info1_name", { prop2 = "val2" }, { data = "info1_data" })
print("\nAfter add_info_node:")
print("Path: " .. dump_path(kb.path))

kb:add_rpc_server_field("info1_server", 25, "info1_server_data")
kb:add_job_field("info1_job", 50, "info1_job_description")
kb:add_stream_field("info1_status", 50, "info1_stream")
kb:add_rpc_client_field("info1_client", 5, "info1_client_description")

kb:leave_header_node("header1_link", "header1_name")
print("\nAfter leave_header_node:")
print("Path: " .. dump_path(kb.path))

kb:add_header_node("header2_link", "header2_name", { prop3 = "val3" }, { data = "header2_data" })
kb:add_info_node("info2_link", "info2_name", { prop4 = "val4" }, { data = "info2_data" })
kb:leave_header_node("header2_link", "header2_name")
print("\nAfter adding and leaving another header node:")
print("Path: " .. dump_path(kb.path))

ok, err = pcall(function()
  kb:check_installation()
  kb:disconnect()
end)
if not ok then
  print("Error during installation check: " .. tostring(err))
end

