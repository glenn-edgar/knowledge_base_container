#!/usr/bin/env luajit
-- ============================================================================
-- locate_missing_hash.lua
-- Find exactly where a missing hash appears in the binary (which tree, which node)
-- 
-- Usage: luajit locate_missing_hash.lua <binary.bin> <hash>
-- Example: luajit locate_missing_hash.lua module.bin 0xDCF5CA2C
-- ============================================================================

local ffi = require("ffi")
local bit = require("bit")

local function read_u8(data, pos)
    return data:byte(pos + 1), pos + 1
end

local function read_u16(data, pos)
    local b1, b2 = data:byte(pos + 1, pos + 2)
    return b1 + b2 * 256, pos + 2
end

local function read_u32(data, pos)
    local b1, b2, b3, b4 = data:byte(pos + 1, pos + 4)
    return b1 + b2 * 256 + b3 * 65536 + b4 * 16777216, pos + 4
end

local function hex(v)
    local u32 = ffi.new("uint32_t", v)
    return string.format("0x%08X", tonumber(u32))
end

local TYPE_NAMES = {
    [1] = "ONESHOT (o_call)",
    [2] = "MAIN (m_call)",
    [3] = "PRED (p_call)",
    [4] = "PT_MAIN (pt_m_call)",
    [5] = "INIT_ONE (io_call)",
    [6] = "BIT_PRED (p_call_bit)",
}

local filename = arg[1]
local target_str = arg[2]

if not filename or not target_str then
    print("Usage: luajit locate_missing_hash.lua <binary.bin> <hash>")
    print("Example: luajit locate_missing_hash.lua module.bin 0xDCF5CA2C")
    os.exit(1)
end

local target = tonumber(target_str)
if not target then
    print("Error: Invalid hash: " .. target_str)
    os.exit(1)
end

local f = io.open(filename, "rb")
if not f then
    print("Error: Cannot open " .. filename)
    os.exit(1)
end
local data = f:read("*a")
f:close()

print("Searching for hash " .. hex(target) .. " in " .. filename)
print()

-- Read header
local pos = 0
local magic
magic, pos = read_u32(data, pos)

if magic ~= 0x42584553 then
    print("Error: Not a valid SEXB binary")
    os.exit(1)
end

pos = 8  -- Skip to name_hash
local name_hash
name_hash, pos = read_u32(data, pos)

local tree_count, record_count, string_count, const_count
tree_count, pos = read_u16(data, pos)
record_count, pos = read_u16(data, pos)
string_count, pos = read_u16(data, pos)
const_count, pos = read_u16(data, pos)

local oneshot_count, main_count, pred_count
oneshot_count, pos = read_u16(data, pos)
main_count, pos = read_u16(data, pos)
pred_count, pos = read_u16(data, pos)

-- Skip to directory
pos = 32
local tree_offset, record_offset, field_offset, string_offset
local const_offset, const_data_offset, func_offset, bytecode_offset

tree_offset, pos = read_u32(data, pos)
record_offset, pos = read_u32(data, pos)
field_offset, pos = read_u32(data, pos)
string_offset, pos = read_u32(data, pos)
const_offset, pos = read_u32(data, pos)
const_data_offset, pos = read_u32(data, pos)
func_offset, pos = read_u32(data, pos)
bytecode_offset, pos = read_u32(data, pos)

-- Read string table to get tree names
local strings = {}
pos = string_offset
for i = 1, string_count do
    local len
    len, pos = read_u16(data, pos)
    local str = data:sub(pos + 1, pos + len)
    pos = pos + len + 1  -- skip null
    pos = math.floor((pos + 3) / 4) * 4  -- align to 4
    table.insert(strings, str)
end

-- Read tree definitions
local trees = {}
pos = tree_offset
for i = 1, tree_count do
    local t = {}
    t.hash, pos = read_u32(data, pos)
    t.rec_idx, pos = read_u16(data, pos)
    t.node_count, pos = read_u16(data, pos)
    t.bc_offset, pos = read_u32(data, pos)
    t.bc_size, pos = read_u32(data, pos)
    t.index = i - 1
    table.insert(trees, t)
end

-- Search bytecode for target hash
local found = false

for _, tree in ipairs(trees) do
    pos = tree.bc_offset
    local end_pos = tree.bc_offset + tree.bc_size
    local node_idx = 0
    
    while pos < end_pos do
        local func_hash, func_type, param_count, node_size
        local node_start = pos
        
        func_hash, pos = read_u32(data, pos)
        func_type, pos = read_u8(data, pos)
        param_count, pos = read_u8(data, pos)
        node_size, pos = read_u16(data, pos)
        
        if func_hash == target then
            found = true
            print("=" .. string.rep("=", 60))
            print("FOUND TARGET HASH!")
            print("=" .. string.rep("=", 60))
            print()
            print(string.format("  Tree index: %d", tree.index))
            print(string.format("  Tree hash:  %s", hex(tree.hash)))
            print(string.format("  Node index: %d (within tree)", node_idx))
            print(string.format("  Func hash:  %s", hex(func_hash)))
            print(string.format("  Func type:  %d = %s", func_type, TYPE_NAMES[func_type] or "???"))
            print(string.format("  Bytecode offset: %d (0x%X)", node_start, node_start))
            print()
            
            -- Determine which table it should be in
            local expected_table = "???"
            if func_type == 1 or func_type == 5 then
                expected_table = "oneshot_hashes"
            elseif func_type == 2 or func_type == 4 then
                expected_table = "main_hashes"
            elseif func_type == 3 or func_type == 6 then
                expected_table = "pred_hashes"
            end
            
            print(string.format("  Should be registered in: %s", expected_table))
            print()
            print("To find this in your DSL code:")
            print(string.format("  1. Look for tree with hash %s", hex(tree.hash)))
            print(string.format("  2. It's the %d-th function call in that tree", node_idx + 1))
            print(string.format("  3. The call type is: %s", TYPE_NAMES[func_type] or "???"))
            print()
        end
        
        -- Skip to next node
        pos = node_start + node_size
        node_idx = node_idx + 1
    end
end

if not found then
    print("Hash " .. hex(target) .. " not found in bytecode.")
end

-- Also check if hash exists in function tables
print()
print("-" .. string.rep("-", 60))
print("Checking function hash tables...")
print("-" .. string.rep("-", 60))

pos = func_offset
local in_oneshot, in_main, in_pred = false, false, false

for i = 1, oneshot_count do
    local h
    h, pos = read_u32(data, pos)
    if h == target then
        in_oneshot = true
        print(string.format("  Found in oneshot_hashes[%d]", i - 1))
    end
end

for i = 1, main_count do
    local h
    h, pos = read_u32(data, pos)
    if h == target then
        in_main = true
        print(string.format("  Found in main_hashes[%d]", i - 1))
    end
end

for i = 1, pred_count do
    local h
    h, pos = read_u32(data, pos)
    if h == target then
        in_pred = true
        print(string.format("  Found in pred_hashes[%d]", i - 1))
    end
end

if not in_oneshot and not in_main and not in_pred then
    print("  NOT FOUND in any hash table!")
    print()
    print("This confirms the bug: The function is used in bytecode but")
    print("was never registered in the function hash tables during compilation.")
end