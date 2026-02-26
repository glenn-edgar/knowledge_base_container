--[[
CRC32 Implementation for LuaJIT

Uses the standard CRC32 polynomial (IEEE 802.3):
  0xEDB88320 (reversed form of 0x04C11DB7)
]]

local bit = require("bit")
local band, bxor, rshift = bit.band, bit.bxor, bit.rshift

local M = {}

-- CRC32 lookup table (computed once at module load)
local crc_table = {}

local function init_crc_table()
    local POLYNOMIAL = 0xEDB88320
    
    for i = 0, 255 do
        local crc = i
        for _ = 1, 8 do
            if band(crc, 1) == 1 then
                crc = bxor(rshift(crc, 1), POLYNOMIAL)
            else
                crc = rshift(crc, 1)
            end
        end
        crc_table[i] = crc
    end
end

-- Initialize table on module load
init_crc_table()

--- Compute CRC32 of a string
-- @param data Input data as string
-- @return 32-bit CRC value as unsigned integer
function M.compute(data)
    if data == nil or #data == 0 then
        return 0
    end
    
    local crc = 0xFFFFFFFF
    
    for i = 1, #data do
        local byte = data:byte(i)
        local index = band(bxor(crc, byte), 0xFF)
        crc = bxor(rshift(crc, 8), crc_table[index])
    end
    
    crc = bxor(crc, 0xFFFFFFFF)
    
    -- Ensure unsigned result
    if crc < 0 then
        crc = crc + 0x100000000
    end
    
    return crc
end

--- Compute CRC32 incrementally (start)
-- @return Initial CRC state
function M.init()
    return 0xFFFFFFFF
end

--- Compute CRC32 incrementally (update)
-- @param crc Current CRC state
-- @param data Data to add
-- @return Updated CRC state
function M.update(crc, data)
    if data == nil or #data == 0 then
        return crc
    end
    
    for i = 1, #data do
        local byte = data:byte(i)
        local index = band(bxor(crc, byte), 0xFF)
        crc = bxor(rshift(crc, 8), crc_table[index])
    end
    
    return crc
end

--- Compute CRC32 incrementally (finalize)
-- @param crc Current CRC state
-- @return Final CRC value
function M.finalize(crc)
    crc = bxor(crc, 0xFFFFFFFF)
    
    if crc < 0 then
        crc = crc + 0x100000000
    end
    
    return crc
end

--- Format CRC as hexadecimal string
-- @param crc CRC value
-- @return Formatted string like "0x1A2B3C4D"
function M.format(crc)
    return string.format("0x%08X", crc)
end

--- Verify data against expected CRC
-- @param data Input data
-- @param expected Expected CRC value
-- @return true if CRC matches
function M.verify(data, expected)
    return M.compute(data) == expected
end

return M