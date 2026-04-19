--[[
  construct_data_tables.lua
  
  LuaJIT translation of construct_data_tables.py
  Facade class that composes all table constructors into a single interface.
  
  Dependencies:
    - construct_kb
    - construct_status_table
    - construct_job_table
    - construct_stream_table
    - construct_rpc_client_table
    - construct_rpc_server_table
    - construct_bit_mask_store
    - construct_jsonb_table
  
  Usage:
    local CDT = require("construct_data_tables")
    local kb = CDT.new(host, port, dbname, user, password, database)
]]

local Construct_KB              = require("construct_kb")
local Construct_Status_Table    = require("construct_status_table")
local Construct_Job_Table       = require("construct_job_table")
local Construct_Stream_Table    = require("construct_stream_table")
local Construct_RPC_Client_Table = require("construct_rpc_client_table")
local Construct_RPC_Server_Table = require("construct_rpc_server_table")
local Construct_Bit_Mask_Store  = require("construct_bit_mask_store")
local Construct_Jsonb_Table     = require("construct_jsonb_table")
local Construct_Doc_Store       = require("construct_doc_store")
local Construct_Stream_Store    = require("construct_stream_store")

local Construct_Data_Tables = {}
Construct_Data_Tables.__index = Construct_Data_Tables

---------------------------------------------------------------------------
-- Constructor
---------------------------------------------------------------------------

--- Create a new Construct_Data_Tables facade.
-- @param host string
-- @param port string|number
-- @param dbname string
-- @param user string
-- @param password string
-- @param database string       base knowledge base table name
-- @param upload_flag boolean   (default false)
function Construct_Data_Tables.new(host, port, dbname, user, password, database, upload_flag)
  upload_flag = upload_flag or false

  local self = setmetatable({}, Construct_Data_Tables)

  -- Core knowledge base
  self.kb = Construct_KB.new(host, port, dbname, user, password, database, upload_flag)

  -- Shared DBI connection from the KB
  local conn = self.kb.conn

  -- Satellite table constructors (all share the same connection)
  self.status_table     = Construct_Status_Table.new(conn, self.kb, database, upload_flag)
  self.job_table        = Construct_Job_Table.new(conn, self.kb, database, upload_flag)
  self.stream_table     = Construct_Stream_Table.new(conn, self.kb, database, upload_flag)
  self.rpc_client_table = Construct_RPC_Client_Table.new(conn, self.kb, database, upload_flag)
  self.rpc_server_table = Construct_RPC_Server_Table.new(conn, self.kb, database, upload_flag)
  self.bit_mask_store   = Construct_Bit_Mask_Store.new(conn, self.kb, upload_flag)
  self.jsonb_table      = Construct_Jsonb_Table.new(conn, self.kb, database, upload_flag)
  self.doc_store        = Construct_Doc_Store.new(conn, self.kb, database, upload_flag)
  self.stream_store     = Construct_Stream_Store.new(conn, self.kb, database, upload_flag)

  -- Expose KB path for inspection
  self.path = self.kb.path

  return self
end

---------------------------------------------------------------------------
-- Delegated KB methods
---------------------------------------------------------------------------

function Construct_Data_Tables:add_kb(...)
  return self.kb:add_kb(...)
end

function Construct_Data_Tables:select_kb(...)
  return self.kb:select_kb(...)
end

function Construct_Data_Tables:add_header_node(...)
  return self.kb:add_header_node(...)
end

function Construct_Data_Tables:add_info_node(...)
  return self.kb:add_info_node(...)
end

function Construct_Data_Tables:leave_header_node(...)
  return self.kb:leave_header_node(...)
end

function Construct_Data_Tables:add_link_node(...)
  return self.kb:add_link_node(...)
end

function Construct_Data_Tables:add_link_mount(...)
  return self.kb:add_link_mount(...)
end

function Construct_Data_Tables:disconnect()
  return self.kb:disconnect()
end

---------------------------------------------------------------------------
-- Delegated satellite table methods
---------------------------------------------------------------------------

function Construct_Data_Tables:add_stream_field(...)
  return self.stream_table:add_stream_field(...)
end

function Construct_Data_Tables:add_rpc_client_field(...)
  return self.rpc_client_table:add_rpc_client_field(...)
end

function Construct_Data_Tables:add_rpc_server_field(...)
  return self.rpc_server_table:add_rpc_server_field(...)
end

function Construct_Data_Tables:add_status_field(...)
  return self.status_table:add_status_field(...)
end

function Construct_Data_Tables:add_job_field(...)
  return self.job_table:add_job_field(...)
end

function Construct_Data_Tables:add_jsonb_field(...)
  return self.jsonb_table:add_jsonb_field(...)
end

function Construct_Data_Tables:add_doc_class(...)
  return self.doc_store:add_doc_class(...)
end

function Construct_Data_Tables:add_stream_class(...)
  return self.stream_store:add_stream_class(...)
end

function Construct_Data_Tables:create_bit_mask_entry(...)
  return self.bit_mask_store:create_bit_mask_entry(...)
end

function Construct_Data_Tables:add_bit_mask_flag(...)
  return self.bit_mask_store:add_flag(...)
end

function Construct_Data_Tables:clear_bit_mask_flags()
  return self.bit_mask_store:clear_flags()
end

---------------------------------------------------------------------------
-- Installation check
---------------------------------------------------------------------------

--- Check installation of all components.
function Construct_Data_Tables:check_installation()
  self.kb:check_installation()
  self.status_table:check_installation()
  self.job_table:check_installation()
  self.stream_table:check_installation()
  self.rpc_client_table:check_installation()
  self.rpc_server_table:check_installation()
  self.jsonb_table:check_installation()
  self.doc_store:check_installation()
  self.stream_store:check_installation()
end

return Construct_Data_Tables
