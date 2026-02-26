#!/usr/bin/env luajit
-- ============================================================================
-- extract_from_binary.lua
-- Extract tree info and find which tree contains the target hash
-- Also attempts to reverse-lookup function name from string table
-- ============================================================================
local ffi = require("ffi")
local bit = require("bit")

local function read_u8(d, p) return d:byte(p+1), p+1 end
local function read_u16(d, p) 
    local b1,b2 = d:byte(p+1, p+2)
    return b1 + b2*256, p+2 
end
local function read_u32(d, p)
    local b1,b2,b3,b4 = d:byte(p+1, p+4)
    local val = b1 + b2*256 + b3*65536 + b4*16777216
    return tonumber(ffi.cast("uint32_t", val)), p+4
end
local function hex(v)
    local n = tonumber(ffi.cast("uint32_t", v))
    return string.format("0x%08X", n)
end

local FNV_OFFSET_BASIS = 0x811c9dc5
local FNV_PRIME = 0x01000193
local function fnv1a_32(str)
    local hash = FNV_OFFSET_BASIS
    for i = 1, #str do
        hash = bit.bxor(hash, str:byte(i))
        hash = bit.band(hash * FNV_PRIME, 0xFFFFFFFF)
    end
    local u32 = ffi.new("uint32_t", hash)
    return tonumber(u32)
end

local TYPE_NAMES = {
    [1]="o_call", [2]="m_call", [3]="p_call",
    [4]="pt_m_call", [5]="io_call", [6]="p_call_bit"
}

local filename = arg[1]
local target = tonumber(arg[2] or "0xDCF5CA2C")

if not filename then
    print("Usage: luajit extract_from_binary.lua <file.bin> [hash]")
    print("Example: luajit extract_from_binary.lua module.bin 0xDCF5CA2C")
    os.exit(1)
end

local f = io.open(filename, "rb")
if not f then print("Cannot open " .. filename); os.exit(1) end
local data = f:read("*a")
f:close()

-- Parse header
local pos = 0
local magic; magic, pos = read_u32(data, pos)
if magic ~= 0x42584553 then
    print("Error: Not a valid SEXB file")
    os.exit(1)
end

pos = 8
local name_hash; name_hash, pos = read_u32(data, pos)
local tree_count; tree_count, pos = read_u16(data, pos)
local record_count; record_count, pos = read_u16(data, pos)
local string_count; string_count, pos = read_u16(data, pos)
local const_count; const_count, pos = read_u16(data, pos)
local oneshot_count; oneshot_count, pos = read_u16(data, pos)
local main_count; main_count, pos = read_u16(data, pos)
local pred_count; pred_count, pos = read_u16(data, pos)

-- Directory at offset 32
pos = 32
local tree_off; tree_off, pos = read_u32(data, pos)
local record_off; record_off, pos = read_u32(data, pos)
local field_off; field_off, pos = read_u32(data, pos)
local string_off; string_off, pos = read_u32(data, pos)
local const_off; const_off, pos = read_u32(data, pos)
local const_data_off; const_data_off, pos = read_u32(data, pos)
local func_off; func_off, pos = read_u32(data, pos)

-- Read strings
local strings = {}
pos = string_off
for i = 1, string_count do
    local len; len, pos = read_u16(data, pos)
    local str = data:sub(pos+1, pos+len)
    pos = pos + len + 1
    pos = math.floor((pos + 3) / 4) * 4
    strings[i] = str
end

-- Read trees
local trees = {}
pos = tree_off
for i = 1, tree_count do
    local t = {}
    t.hash, pos = read_u32(data, pos)
    t.rec_idx, pos = read_u16(data, pos)
    t.node_count, pos = read_u16(data, pos)
    t.bc_offset, pos = read_u32(data, pos)
    t.bc_size, pos = read_u32(data, pos)
    trees[i] = t
end

-- Try to find tree name by matching hash to strings
local function find_name_by_hash(target_hash)
    for _, s in ipairs(strings) do
        if fnv1a_32(s) == target_hash then
            return s
        end
    end
    return nil
end

print("Searching for hash " .. hex(target) .. " in " .. filename)
print(string.format("Module: %s, Trees: %d, Strings: %d", hex(name_hash), tree_count, string_count))
print()

local found = false

-- Search each tree
for ti, tree in ipairs(trees) do
    local tree_name = find_name_by_hash(tree.hash) or "???"
    pos = tree.bc_offset
    local end_pos = tree.bc_offset + tree.bc_size
    local node_idx = 0
    
    while pos < end_pos do
        local start_pos = pos
        local func_hash; func_hash, pos = read_u32(data, pos)
        local func_type; func_type, pos = read_u8(data, pos)
        local param_count; param_count, pos = read_u8(data, pos)
        local node_size; node_size, pos = read_u16(data, pos)
        
        if func_hash == target then
            found = true
            print("=" .. string.rep("=", 60))
            print("FOUND!")
            print("=" .. string.rep("=", 60))
            print(string.format("Tree[%d]: %s", ti-1, hex(tree.hash)))
            print(string.format("Tree name: %s", tree_name))
            print(string.format("Node[%d]: %s", node_idx, hex(func_hash)))
            print(string.format("Call type: %d = %s", func_type, TYPE_NAMES[func_type] or "???"))
            print()
            
            -- Try to find function name in strings
            local func_name = find_name_by_hash(func_hash)
            if func_name then
                print(string.format("FUNCTION NAME: \"%s\"", func_name))
            else
                print("Function name NOT in string table")
            end
            print()
            print("Look in your DSL source for:")
            print(string.format("  - A tree named \"%s\"", tree_name))
            print(string.format("  - The %s() call at position %d in that tree", 
                               TYPE_NAMES[func_type] or "???", node_idx))
            print()
        end
        
        pos = start_pos + node_size
        node_idx = node_idx + 1
    end
end

if not found then
    print("Hash " .. hex(target) .. " not found in any tree bytecode.")
end

-- Also list all io_call functions found
print()
print("-" .. string.rep("-", 60))
print("All io_call (type 5) functions in binary:")
print("-" .. string.rep("-", 60))

for ti, tree in ipairs(trees) do
    local tree_name = find_name_by_hash(tree.hash) or "???"
    pos = tree.bc_offset
    local end_pos = tree.bc_offset + tree.bc_size
    local node_idx = 0
    
    while pos < end_pos do
        local start_pos = pos
        local func_hash; func_hash, pos = read_u32(data, pos)
        local func_type; func_type, pos = read_u8(data, pos)
        local param_count; param_count, pos = read_u8(data, pos)
        local node_size; node_size, pos = read_u16(data, pos)
        
        if func_type == 5 then
            local func_name = find_name_by_hash(func_hash) or "???"
            local marker = (func_hash == target) and " <-- TARGET" or ""
            print(string.format("  Tree[%d] %s Node[%d]: %s (%s)%s", 
                               ti-1, tree_name, node_idx, hex(func_hash), func_name, marker))
        end
        
        pos = start_pos + node_size
        node_idx = node_idx + 1
    end
end