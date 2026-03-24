#!/usr/bin/env luajit
--[[
  ctb_dump.lua - Dump ChainTree binary image contents for verification.
  
  Usage:
    luajit ctb_dump.lua <file.ctb>
--]]

local ffi = require("ffi")
local bit = require("bit")

ffi.cdef[[ uint32_t fnv1a_32(const char *str); ]]

-- Section type names
local SECT_NAMES = {
    [0x0001] = "NODE", [0x0002] = "LINK", [0x0003] = "MFHT",
    [0x0004] = "OSHT", [0x0005] = "BFHT", [0x0006] = "FSTR",
    [0x0007] = "JREC", [0x0008] = "JCTL", [0x0009] = "JSTR",
    [0x000A] = "EVNT", [0x000B] = "BMSK", [0x000C] = "KBIN",
    [0x000D] = "KBAL", [0x000E] = "GSTR",
}

-- Read helpers (little-endian from string)
local function u16(data, offset)
    local a, b = data:byte(offset + 1, offset + 2)
    return a + b * 256
end

local function u32(data, offset)
    local a, b, c, d = data:byte(offset + 1, offset + 4)
    return a + b * 256 + c * 65536 + d * 16777216
end

-- CRC32
local crc32_table = {}
for i = 0, 255 do
    local c = i
    for _ = 1, 8 do
        if bit.band(c, 1) == 1 then
            c = bit.bxor(0xEDB88320, bit.rshift(c, 1))
        else
            c = bit.rshift(c, 1)
        end
    end
    crc32_table[i] = c
end

local function crc32_compute(data)
    local crc = 0xFFFFFFFF
    for i = 1, #data do
        local b = data:byte(i)
        local idx = bit.band(bit.bxor(crc, b), 0xFF)
        crc = bit.bxor(crc32_table[idx], bit.rshift(crc, 8))
    end
    return bit.bxor(crc, 0xFFFFFFFF)
end

-- Read string from pool at offset
local function read_string(data, pool_offset, str_offset)
    local start = pool_offset + str_offset + 1
    local e = data:find("\0", start, true)
    if not e then return "?" end
    return data:sub(start, e - 1)
end

-- =========================================================================

if #arg < 1 then
    print("Usage: luajit ctb_dump.lua <file.ctb>")
    os.exit(1)
end

local f = io.open(arg[1], "rb")
if not f then
    print("Error: cannot open " .. arg[1])
    os.exit(1)
end
local data = f:read("*a")
f:close()

print(string.format("File: %s (%d bytes)", arg[1], #data))
print(string.rep("=", 70))

-- Header
local magic = u32(data, 0)
if magic ~= 0x43544231 then
    print(string.format("ERROR: Bad magic: 0x%08X (expected 0x43544231)", magic))
    os.exit(1)
end

print("HEADER:")
print(string.format("  Magic:            CTB1 (0x%08X)", magic))
print(string.format("  Version:          %d.%d", u16(data, 4), u16(data, 6)))
print(string.format("  Flags:            0x%08X", u32(data, 8)))
print(string.format("  Total size:       %d", u32(data, 12)))
print(string.format("  Checksum:         0x%08X", u32(data, 16)))

-- Verify checksum
local zeroed = data:sub(1, 16) .. "\0\0\0\0" .. data:sub(21)
local computed = crc32_compute(zeroed)
local stored = u32(data, 16)
if computed == stored then
    print("  Checksum:         VALID")
else
    print(string.format("  Checksum:         INVALID (computed 0x%08X)", computed))
end

print(string.format("  Sections:         %d", u16(data, 20)))
print(string.format("  Nodes:            %d total, %d active", u16(data, 22), u16(data, 24)))
print(string.format("  Links:            %d", u16(data, 26)))
print(string.format("  Main funcs:       %d", u16(data, 28)))
print(string.format("  One-shot funcs:   %d", u16(data, 30)))
print(string.format("  Boolean funcs:    %d", u16(data, 32)))
print(string.format("  Events:           %d", u16(data, 34)))
print(string.format("  Bitmasks:         %d", u16(data, 36)))
print(string.format("  KBs:              %d", u16(data, 38)))
print(string.format("  JSON records:     %d", u16(data, 40)))
print(string.format("  JSON controls:    %d", u16(data, 42)))
print(string.format("  JSON str size:    %d", u32(data, 44)))

-- Section directory
local section_count = u16(data, 20)
local main_count = u16(data, 28)
local os_count = u16(data, 30)
local bool_count = u16(data, 32)

print()
print("SECTION DIRECTORY:")
print(string.format("  %-6s  %-10s  %-10s  %-8s  %-8s", "Type", "Offset", "Size", "Count", "EntSize"))

local section_map = {}  -- type -> {offset, size, count, esize}

for i = 0, section_count - 1 do
    local base = 64 + i * 16
    local stype = u32(data, base)
    local soffset = u32(data, base + 4)
    local ssize = u32(data, base + 8)
    local scount = u16(data, base + 12)
    local sesize = u16(data, base + 14)
    
    local name = SECT_NAMES[stype] or string.format("0x%04X", stype)
    print(string.format("  %-6s  %-10d  %-10d  %-8d  %-8d", name, soffset, ssize, scount, sesize))
    
    section_map[stype] = { offset = soffset, size = ssize, count = scount, esize = sesize }
end

-- Dump function hash tables with names
local function dump_hash_table(label, sect_type, count)
    local sect = section_map[sect_type]
    if not sect or sect.size == 0 then return end
    
    -- Find FSTR section for names
    local fstr = section_map[0x0006]
    
    print()
    print(label .. " HASH TABLE:")
    
    -- Calculate name offset within FSTR
    local name_skip = 0
    if sect_type == 0x0004 then name_skip = main_count
    elseif sect_type == 0x0005 then name_skip = main_count + os_count end
    
    for i = 0, count - 1 do
        local hash = u32(data, sect.offset + i * 4)
        local name = "?"
        if fstr then
            name = read_string(data, fstr.offset - 1, 0) -- simplified
            -- Walk names to find the right one
            local p = fstr.offset
            local idx = 0
            local target = name_skip + i
            while idx < target and p < fstr.offset + fstr.size do
                local e = data:find("\0", p + 1, true)
                if not e then break end
                p = e + 1
                idx = idx + 1
            end
            if p < fstr.offset + fstr.size then
                local e = data:find("\0", p + 1, true)
                if e then name = data:sub(p + 1, e - 1) end
            end
        end
        print(string.format("  [%3d] 0x%08X  %s", i, hash, name))
    end
end

dump_hash_table("MAIN FUNCTION", 0x0003, main_count)
dump_hash_table("ONE-SHOT FUNCTION", 0x0004, os_count)
dump_hash_table("BOOLEAN FUNCTION", 0x0005, bool_count)

-- Dump nodes (first 10)
local node_sect = section_map[0x0001]
if node_sect and node_sect.size > 0 then
    local node_count = u16(data, 22)
    local show = math.min(node_count, 10)
    print()
    print(string.format("NODE ARRAY (first %d of %d):", show, node_count))
    for i = 0, show - 1 do
        local base = node_sect.offset + i * 20
        local ni = u16(data, base)
        local pi = u16(data, base + 2)
        local depth = u16(data, base + 4)
        local ls = u16(data, base + 6)
        local lc_raw = u16(data, base + 8)
        local lc = bit.band(lc_raw, 0x7FFF)
        local auto = bit.band(lc_raw, 0x8000) ~= 0
        local mf = u16(data, base + 10)
        local inf = u16(data, base + 12)
        local aux = u16(data, base + 14)
        local tf = u16(data, base + 16)
        local did = u16(data, base + 18)
        
        local auto_tag = auto and " [AS]" or ""
        if pi == 0xFFFF and mf == 0 and depth == 0 and lc == 0 and did == 0xFFFF then
            print(string.format("  [%3d] FILTERED", ni))
        else
            print(string.format("  [%3d] parent=%d depth=%d links=%d+%d main=%d init=%d aux=%d term=%d data=%d%s",
                ni, pi, depth, ls, lc, mf, inf, aux, tf, did, auto_tag))
        end
    end
    if node_count > show then
        print(string.format("  ... (%d more)", node_count - show))
    end
end

print()
print("Done.")