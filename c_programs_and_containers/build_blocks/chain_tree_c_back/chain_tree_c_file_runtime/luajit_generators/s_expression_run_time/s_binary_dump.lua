#!/usr/bin/env luajit
-- ============================================================================
-- dump_binary_full.lua
-- Full dump of binary module showing hash table contents vs bytecode usage
-- ============================================================================

local ffi = require("ffi")
local bit = require("bit")
jit.off()
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

local FUNC_TYPE_INFO = {
    [1] = { name = "ONESHOT",   table = "oneshot" },
    [2] = { name = "MAIN",      table = "main" },
    [3] = { name = "PRED",      table = "pred" },
    [4] = { name = "PT_MAIN",   table = "main" },
    [5] = { name = "INIT_ONE",  table = "oneshot" },
    [6] = { name = "BIT_PRED",  table = "pred" },
}

local function analyze_binary(filename)
    local f = io.open(filename, "rb")
    if not f then
        print("Error: Cannot open " .. filename)
        return
    end
    local data = f:read("*a")
    f:close()
    
    print("=" .. string.rep("=", 78))
    print("FULL BINARY ANALYSIS: " .. filename)
    print("=" .. string.rep("=", 78))
    print()
    
    -- Read header
    local pos = 0
    local magic, version, flags, name_hash
    magic, pos = read_u32(data, pos)
    version, pos = read_u16(data, pos)
    flags, pos = read_u16(data, pos)
    name_hash, pos = read_u32(data, pos)
    
    local tree_count, record_count, string_count, const_count
    tree_count, pos = read_u16(data, pos)
    record_count, pos = read_u16(data, pos)
    string_count, pos = read_u16(data, pos)
    const_count, pos = read_u16(data, pos)
    
    local oneshot_count, main_count, pred_count, reserved
    oneshot_count, pos = read_u16(data, pos)
    main_count, pos = read_u16(data, pos)
    pred_count, pos = read_u16(data, pos)
    reserved, pos = read_u16(data, pos)
    
    local total_size
    total_size, pos = read_u32(data, pos)
    
    print(string.format("Module hash: %s", hex(name_hash)))
    print(string.format("Counts: trees=%d records=%d strings=%d consts=%d", 
                        tree_count, record_count, string_count, const_count))
    print(string.format("Functions: oneshot=%d main=%d pred=%d", 
                        oneshot_count, main_count, pred_count))
    print()
    
    -- Read directory
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
    
    -- Read function hash tables
    local oneshot_hashes = {}
    local main_hashes = {}
    local pred_hashes = {}
    
    pos = func_offset
    print("-" .. string.rep("-", 78))
    print("ONESHOT HASH TABLE (" .. oneshot_count .. " entries):")
    print("-" .. string.rep("-", 78))
    for i = 1, oneshot_count do
        local h
        h, pos = read_u32(data, pos)
        oneshot_hashes[h] = i - 1
        print(string.format("  [%2d] %s", i - 1, hex(h)))
    end
    print()
    
    print("-" .. string.rep("-", 78))
    print("MAIN HASH TABLE (" .. main_count .. " entries):")
    print("-" .. string.rep("-", 78))
    for i = 1, main_count do
        local h
        h, pos = read_u32(data, pos)
        main_hashes[h] = i - 1
        print(string.format("  [%2d] %s", i - 1, hex(h)))
    end
    print()
    
    print("-" .. string.rep("-", 78))
    print("PRED HASH TABLE (" .. pred_count .. " entries):")
    print("-" .. string.rep("-", 78))
    for i = 1, pred_count do
        local h
        h, pos = read_u32(data, pos)
        pred_hashes[h] = i - 1
        print(string.format("  [%2d] %s", i - 1, hex(h)))
    end
    print()
    
    -- Read tree info
    local trees = {}
    pos = tree_offset
    for i = 1, tree_count do
        local t = {}
        t.hash, pos = read_u32(data, pos)
        t.rec_idx, pos = read_u16(data, pos)
        t.node_count, pos = read_u16(data, pos)
        t.bc_offset, pos = read_u32(data, pos)
        t.bc_size, pos = read_u32(data, pos)
        table.insert(trees, t)
    end
    
    -- Analyze bytecode and collect all function hashes used
    print("-" .. string.rep("-", 78))
    print("BYTECODE FUNCTION USAGE:")
    print("-" .. string.rep("-", 78))
    
    local missing_oneshot = {}
    local missing_main = {}
    local missing_pred = {}
    local all_used = {}
    
    for tree_idx, tree in ipairs(trees) do
        print(string.format("\nTree[%d] %s (%d nodes):", tree_idx-1, hex(tree.hash), tree.node_count))
        
        pos = tree.bc_offset
        local end_pos = tree.bc_offset + tree.bc_size
        local node_idx = 0
        
        while pos < end_pos do
            local func_hash, func_type, param_count, node_size
            func_hash, pos = read_u32(data, pos)
            func_type, pos = read_u8(data, pos)
            param_count, pos = read_u8(data, pos)
            node_size, pos = read_u16(data, pos)
            
            local type_info = FUNC_TYPE_INFO[func_type] or { name = "???", table = "???" }
            local target_table = type_info.table
            
            -- Check if hash exists in correct table
            local lookup_table, found
            if target_table == "oneshot" then
                lookup_table = oneshot_hashes
            elseif target_table == "main" then
                lookup_table = main_hashes
            elseif target_table == "pred" then
                lookup_table = pred_hashes
            end
            
            found = lookup_table and lookup_table[func_hash]
            local status = found and "OK" or "MISSING!"
            
            -- Track missing
            if not found then
                if target_table == "oneshot" then
                    missing_oneshot[func_hash] = true
                elseif target_table == "main" then
                    missing_main[func_hash] = true
                elseif target_table == "pred" then
                    missing_pred[func_hash] = true
                end
            end
            
            -- Track all used
            local key = target_table .. ":" .. hex(func_hash)
            all_used[key] = (all_used[key] or 0) + 1
            
            print(string.format("  [%2d] %s type=%d(%s) -> %s [%s]",
                               node_idx, hex(func_hash), func_type, type_info.name,
                               target_table, status))
            
            -- Skip to next node
            pos = pos + (node_size - 8)
            node_idx = node_idx + 1
        end
    end
    
    -- Summary
    print()
    print("=" .. string.rep("=", 78))
    print("SUMMARY")
    print("=" .. string.rep("=", 78))
    
    local count_missing_oneshot = 0
    local count_missing_main = 0
    local count_missing_pred = 0
    
    for _ in pairs(missing_oneshot) do count_missing_oneshot = count_missing_oneshot + 1 end
    for _ in pairs(missing_main) do count_missing_main = count_missing_main + 1 end
    for _ in pairs(missing_pred) do count_missing_pred = count_missing_pred + 1 end
    
    if count_missing_oneshot > 0 then
        print()
        print("MISSING FROM ONESHOT TABLE (" .. count_missing_oneshot .. "):")
        for h in pairs(missing_oneshot) do
            print("  " .. hex(h) .. "  <- This function uses io_call() but wasn't registered!")
        end
    end
    
    if count_missing_main > 0 then
        print()
        print("MISSING FROM MAIN TABLE (" .. count_missing_main .. "):")
        for h in pairs(missing_main) do
            print("  " .. hex(h))
        end
    end
    
    if count_missing_pred > 0 then
        print()
        print("MISSING FROM PRED TABLE (" .. count_missing_pred .. "):")
        for h in pairs(missing_pred) do
            print("  " .. hex(h))
        end
    end
    
    local total_missing = count_missing_oneshot + count_missing_main + count_missing_pred
    print()
    if total_missing == 0 then
        print("RESULT: ALL HASHES FOUND - Binary is valid!")
    else
        print("RESULT: " .. total_missing .. " HASHES MISSING - Binary has registration bugs!")
        print()
        print("FIX: In your Lua DSL generator, ensure that:")
        print("  - io_call(func) registers func in oneshot_funcs")
        print("  - pt_m_call(func) registers func in main_funcs")
        print("  - p_call_bit(func) registers func in pred_funcs")
    end
    print("=" .. string.rep("=", 78))
end

-- Main
local filename = arg[1]
if not filename then
    print("Usage: luajit dump_binary_full.lua <file.bin>")
    os.exit(1)
end

analyze_binary(filename)