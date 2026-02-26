--[[
Binary Writer Utility for LuaJIT

Provides methods for writing binary data in little-endian format.
]]

local ffi = require("ffi")
local bit = require("bit")
local band, rshift = bit.band, bit.rshift

local M = {}

-- Binary writer class
local Writer = {}
Writer.__index = Writer

--- Create a new binary writer
-- @return Writer instance
function M.new()
    local self = setmetatable({}, Writer)
    self.buffer = {}
    self.pos = 0
    return self
end

--- Get current write position (0-based)
-- @return Current position in bytes
function Writer:position()
    return self.pos
end

--- Seek to a position (for overwriting)
-- @param pos Position to seek to (0-based)
function Writer:seek(pos)
    self.pos = pos
end

--- Write a single byte
-- @param value Byte value (0-255)
function Writer:write_u8(value)
    self.pos = self.pos + 1
    self.buffer[self.pos] = string.char(band(value, 0xFF))
end

--- Write a 16-bit unsigned integer (little-endian)
-- @param value Integer value
function Writer:write_u16(value)
    value = band(value, 0xFFFF)
    self.pos = self.pos + 1
    self.buffer[self.pos] = string.char(band(value, 0xFF))
    self.pos = self.pos + 1
    self.buffer[self.pos] = string.char(band(rshift(value, 8), 0xFF))
end

--- Write a 32-bit unsigned integer (little-endian)
-- @param value Integer value
function Writer:write_u32(value)
    -- Handle negative values (from bit operations)
    if value < 0 then
        value = value + 0x100000000
    end
    value = band(value, 0xFFFFFFFF)
    
    self.pos = self.pos + 1
    self.buffer[self.pos] = string.char(band(value, 0xFF))
    self.pos = self.pos + 1
    self.buffer[self.pos] = string.char(band(rshift(value, 8), 0xFF))
    self.pos = self.pos + 1
    self.buffer[self.pos] = string.char(band(rshift(value, 16), 0xFF))
    self.pos = self.pos + 1
    self.buffer[self.pos] = string.char(band(rshift(value, 24), 0xFF))
end

--- Write a 16-bit signed integer (little-endian)
-- @param value Integer value
function Writer:write_i16(value)
    if value < 0 then
        value = value + 0x10000
    end
    self:write_u16(value)
end

--- Write a 32-bit signed integer (little-endian)
-- @param value Integer value
function Writer:write_i32(value)
    if value < 0 then
        value = value + 0x100000000
    end
    self:write_u32(value)
end

--- Write a 32-bit float (little-endian)
-- @param value Float value
function Writer:write_f32(value)
    -- Use FFI to get the bit representation
    local buf = ffi.new("float[1]", value)
    local int_buf = ffi.cast("uint32_t*", buf)
    self:write_u32(int_buf[0])
end

--- Write raw bytes from a string
-- @param str String containing bytes to write
function Writer:write_bytes(str)
    for i = 1, #str do
        self.pos = self.pos + 1
        self.buffer[self.pos] = str:sub(i, i)
    end
end

--- Write padding bytes to align to boundary
-- @param alignment Alignment boundary (e.g., 4 for 4-byte alignment)
function Writer:align(alignment)
    local padding = (alignment - (self.pos % alignment)) % alignment
    for _ = 1, padding do
        self.pos = self.pos + 1
        self.buffer[self.pos] = string.char(0)
    end
end

--- Write a null-terminated string
-- @param str String to write
function Writer:write_cstring(str)
    self:write_bytes(str)
    self:write_u8(0)
end

--- Get the complete binary data as a string
-- @return Binary data string
function Writer:get_data()
    return table.concat(self.buffer)
end

--- Get current size of written data
-- @return Size in bytes
function Writer:size()
    return self.pos
end

--- Reserve space (writes zeros)
-- @param count Number of bytes to reserve
function Writer:reserve(count)
    for _ = 1, count do
        self.pos = self.pos + 1
        self.buffer[self.pos] = string.char(0)
    end
end

--- Overwrite data at a specific position
-- Note: Must seek first, then write
-- Example:
--   local save_pos = writer:position()
--   writer:seek(target_pos)
--   writer:write_u32(value)
--   writer:seek(save_pos)

return M