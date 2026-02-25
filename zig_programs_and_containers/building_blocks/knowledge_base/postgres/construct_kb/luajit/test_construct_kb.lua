--[[
  test_construct_kb.lua
  
  Standalone test for construct_kb.lua
  Translated from the if __name__ == "__main__" block in construct_kb.py
  
  Usage:
    luajit test_construct_kb.lua
  
  You will be prompted for your PostgreSQL password.
]]

local Construct_KB = require("construct_kb")

-- Configuration
local DB_HOST  = "localhost"
local DB_PORT  = "5432"
local DB_NAME  = "knowledge_base"
local DB_USER  = "gedgar"
local DB_TABLE = "knowledge_base"

io.write("Enter your password: ")
io.flush()
local DB_PASSWORD = io.read("*l")

print("starting unit test")

local ok, err = pcall(function()
  local kb = Construct_KB.new(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, DB_TABLE)

  -- Build kb1
  kb:add_kb("kb1", "First knowledge base")
  kb:select_kb("kb1")

  kb:add_header_node("header1_link", "header1_name",
                     { prop1 = "val1" }, { data = "header1_data" })

  kb:add_info_node("info1_link", "info1_name",
                   { prop2 = "val2" }, { data = "info1_data" })

  kb:leave_header_node("header1_link", "header1_name")

  kb:add_header_node("header2_link", "header2_name",
                     { prop3 = "val3" }, { data = "header2_data" })

  kb:add_info_node("info2_link", "info2_name",
                   { prop4 = "val4" }, { data = "info2_data" })

  kb:add_link_mount("link1", "link1 description")

  kb:leave_header_node("header2_link", "header2_name")

  -- Build kb2
  kb:add_kb("kb2", "Second knowledge base")
  kb:select_kb("kb2")

  kb:add_header_node("header1_link", "header1_name",
                     { prop1 = "val1" }, { data = "header1_data" })

  kb:add_info_node("info1_link", "info1_name",
                   { prop2 = "val2" }, { data = "info1_data" })

  kb:leave_header_node("header1_link", "header1_name")

  kb:add_header_node("header2_link", "header2_name",
                     { prop3 = "val3" }, { data = "header2_data" })

  kb:add_info_node("info2_link", "info2_name",
                   { prop4 = "val4" }, { data = "info2_data" })

  kb:add_link_node("link1")

  kb:leave_header_node("header2_link", "header2_name")

  -- Check installation
  kb:check_installation()
  kb:disconnect()
end)

if not ok then
  print("Error during installation check: " .. tostring(err))
end

print("ending unit test")