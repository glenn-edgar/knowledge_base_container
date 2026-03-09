--[[
  construct_bit_mask_store.lua
  
  LuaJIT translation of construct_bit_mask_store.py
  Bit mask store construction for knowledge base.
  
  Dependencies:
    - bit_mask_operations (local module)
    - dkjson
  
  Usage:
    local ConstructBitMaskStore = require("construct_bit_mask_store")
    local bms = ConstructBitMaskStore.new(conn, construct_kb)
]]

local BitMaskOperations = require("bit_mask_operations")
local json = require("dkjson")

local Construct_Bit_Mask_Store = {}
Construct_Bit_Mask_Store.__index = Construct_Bit_Mask_Store

---------------------------------------------------------------------------
-- Constructor
---------------------------------------------------------------------------

--- Create a new Construct_Bit_Mask_Store.
-- @param conn          DBI connection object
-- @param construct_kb  Construct_KB instance
-- @param upload_flag   boolean (default false)
function Construct_Bit_Mask_Store.new(conn, construct_kb, upload_flag)
  local self = setmetatable({}, Construct_Bit_Mask_Store)
  self.bit_mask_operations = BitMaskOperations.new(conn)
  self.construct_kb = construct_kb
  self.conn = conn
  self.upload_flag = upload_flag or false
  self.bit_mask_flags = {}

  if not self.upload_flag then
    self.bit_mask_operations:create_table()
  end

  return self
end

---------------------------------------------------------------------------
-- Public API
---------------------------------------------------------------------------

--- Clear all accumulated flags.
function Construct_Bit_Mask_Store:clear_flags()
  self.bit_mask_flags = {}
end

--- Add a flag definition.
-- @param flag_name string
-- @param bit_position number
-- @param flag_description string
function Construct_Bit_Mask_Store:add_flag(flag_name, bit_position, flag_description)
  self.bit_mask_flags[flag_name] = {
    bit = bit_position,
    description = flag_description,
  }
end

--- Count entries in a table (keys only).
-- @param tbl table
-- @return number
local function table_len(tbl)
  local n = 0
  for _ in pairs(tbl) do n = n + 1 end
  return n
end

--- Create a bit mask entry in both the bit_mask table and the knowledge base.
-- @param user_name string
-- @param name string
-- @param mask_size number   1..64
-- @param bit_mask number    initial mask value
-- @param description string (default "")
function Construct_Bit_Mask_Store:create_bit_mask_entry(user_name, name, mask_size, bit_mask, description)
  description = description or ""

  assert(type(name) == "string", "name must be a string")
  assert(type(mask_size) == "number", "mask_size must be a number")
  assert(type(bit_mask) == "number", "bit_mask must be a number")

  if mask_size < 1 or mask_size > 64 then
    error("mask_size must be between 1 and 64")
  end

  -- 2^mask_size - 1  (max value for the given size)
  local max_val = (2 ^ mask_size) - 1
  if bit_mask < 0 or bit_mask > max_val then
    error("bit_mask must be between 0 and " .. tostring(max_val))
  end

  if mask_size ~= table_len(self.bit_mask_flags) then
    error("bit_mask size must be equal to the number of flags")
  end

  -- Check for duplicate bit positions
  local temp_mask = {}
  for i = 0, mask_size - 1 do
    temp_mask[i] = 0
  end

  for flag_name, flag_data in pairs(self.bit_mask_flags) do
    if temp_mask[flag_data.bit] == 1 then
      error("flag already exists in bit_mask")
    end
    temp_mask[flag_data.bit] = 1
  end

  -- Build the ltree node name
  local label = "KB_BIT_MASK"
  local path_segments = self.construct_kb.path[self.construct_kb.working_kb]
  local ltree_node_name = table.concat(path_segments, ".") .. "." .. label .. "." .. name
  ltree_node_name = ltree_node_name:gsub("%.", "_"):lower()

  -- Create the bit mask table entry
  self.bit_mask_operations:create_entry(ltree_node_name, bit_mask)

  -- Build node properties
  local node_properties = {
    user_name       = user_name,
    mask_size       = mask_size,
    bit_mask        = bit_mask,
    flag_dictionary = json.encode(self.bit_mask_flags),
    record_id       = ltree_node_name,
  }

  -- Add to knowledge base
  self.construct_kb:add_info_node(label, name, node_properties, {}, description)
end

return Construct_Bit_Mask_Store

