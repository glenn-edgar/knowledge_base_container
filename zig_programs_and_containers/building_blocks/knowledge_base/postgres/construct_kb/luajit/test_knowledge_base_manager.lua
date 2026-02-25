--[[
  test_knowledge_base_manager.lua
  
  Standalone test for knowledge_base_manager.lua
  Translated from the if __name__ == "__main__" block in base_construct_kb.py
  
  Usage:
    luajit test_knowledge_base_manager.lua
  
  You will be prompted for your PostgreSQL password.
]]

local KnowledgeBaseManager = require("knowledge_base_manager")

-- Prompt for password
io.write("Enter PostgreSQL password: ")
io.flush()
local password = io.read("*l")

local conn_params = {
  host     = "localhost",
  database = "knowledge_base",
  user     = "gedgar",
  password = password,
  port     = 5432,
}

print("starting unit test")

local ok, err = pcall(function()
  local kb_manager = KnowledgeBaseManager.new("knowledge_base", conn_params)

  -- Add knowledge bases
  kb_manager:add_kb("kb1", "First knowledge base")
  kb_manager:add_kb("kb2", "Second knowledge base")

  -- Add nodes
  kb_manager:add_node("kb1", "person", "John Doe",
                      { age = 30 }, { email = "john@example.com" }, "people.john")
  kb_manager:add_node("kb2", "person", "Jane Smith",
                      { age = 25 }, { email = "jane@example.com" }, "people.jane")

  -- Add link mount
  kb_manager:add_link_mount("kb1", "people.john", "link1", "link1 description")

  -- Add link
  kb_manager:add_link("kb1", "people.john", "link1")

  kb_manager:disconnect()
end)

if not ok then
  print("Error during test: " .. tostring(err))
end

print("ending unit test")