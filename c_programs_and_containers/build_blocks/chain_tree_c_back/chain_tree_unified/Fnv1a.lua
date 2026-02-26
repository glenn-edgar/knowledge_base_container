--[[
FNV-1a Hash Implementation for LuaJIT

Uses the standard FNV-1a parameters:
  FNV_PRIME  = 0x01000193
  FNV_OFFSET = 0x811C9DC5
]]

local bit = require("bit")
local bxor, band, tobit = bit.bxor, bit.band, bit.tobit

local FNV_PRIME  = 0x01000193
local FNV_OFFSET = 0x811C9DC5

local M = {}

--- Compute FNV-1a hash of a string
-- @param str Input string
-- @return 32-bit hash value as unsigned integer
function M.hash(str)
    if str == nil then
        return 0
    end
    
    -- Use tobit to ensure 32-bit operations
    local hash = tobit(FNV_OFFSET)
    
    for i = 1, #str do
        local byte = str:byte(i)
        -- XOR with byte (tobit ensures 32-bit)
        hash = bxor(hash, byte)
        -- Multiply by prime - use modular arithmetic for 32-bit
        -- Split multiplication to avoid overflow issues
        local lo = band(hash, 0xFFFF) * FNV_PRIME
        local hi = band(bit.rshift(hash, 16), 0xFFFF) * FNV_PRIME
        hash = tobit(lo + bit.lshift(hi, 16))
    end
    
    -- Convert to unsigned
    if hash < 0 then
        hash = hash + 0x100000000
    end
    
    return hash
end

--- Compute FNV-1a hash with custom seed
-- @param str Input string
-- @param seed Initial hash value (default: FNV_OFFSET)
-- @return 32-bit hash value
function M.hash_with_seed(str, seed)
    if str == nil then
        return seed or FNV_OFFSET
    end
    
    local hash = tobit(seed or FNV_OFFSET)
    
    for i = 1, #str do
        local byte = str:byte(i)
        hash = bxor(hash, byte)
        local lo = band(hash, 0xFFFF) * FNV_PRIME
        local hi = band(bit.rshift(hash, 16), 0xFFFF) * FNV_PRIME
        hash = tobit(lo + bit.lshift(hi, 16))
    end
    
    if hash < 0 then
        hash = hash + 0x100000000
    end
    
    return hash
end

--- Compute FNV-1a hash of multiple strings (concatenated)
-- @param ... Variable number of strings
-- @return 32-bit hash value
function M.hash_multi(...)
    local hash = tobit(FNV_OFFSET)
    
    for _, str in ipairs({...}) do
        if str ~= nil then
            for i = 1, #str do
                local byte = str:byte(i)
                hash = bxor(hash, byte)
                local lo = band(hash, 0xFFFF) * FNV_PRIME
                local hi = band(bit.rshift(hash, 16), 0xFFFF) * FNV_PRIME
                hash = tobit(lo + bit.lshift(hi, 16))
            end
        end
    end
    
    if hash < 0 then
        hash = hash + 0x100000000
    end
    
    return hash
end

--- Compute FNV-1a hash of raw bytes (same as hash but clearer name for binary data)
-- @param data Input data as string (treated as raw bytes)
-- @return 32-bit hash value as unsigned integer
function M.hash_bytes(data)
    return M.hash(data)
end

--- Format hash as hexadecimal string
-- @param hash Hash value
-- @return Formatted string like "0x1A2B3C4D"
function M.format(hash)
    return string.format("0x%08X", hash)
end

-- Constants exposed for reference
M.FNV_PRIME = FNV_PRIME
M.FNV_OFFSET = FNV_OFFSET

return M