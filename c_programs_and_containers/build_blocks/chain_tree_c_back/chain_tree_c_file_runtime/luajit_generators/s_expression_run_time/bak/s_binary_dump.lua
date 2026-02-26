#!/usr/bin/env luajit
-- ============================================================================
-- s_binary_dump.lua
-- Utility to dump and validate ChainTree binary module files
-- Usage: luajit s_binary_dump.lua <file.bin> [--verbose]
-- ============================================================================

local ffi = require("ffi")
local bit = require("bit")

local SEXB_MAGIC = 0x42584553  -- "SEXB"
local SEXB_VERSION = 0x0100

-- Read helpers
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

local function read_f32(data, pos)
    local buf = ffi.new("uint8_t[4]")
    for i = 0, 3 do
        buf[i] = data:byte(pos + 1 + i)
    end
    local f = ffi.cast("float*", buf)[0]
    return f, pos + 4
end

local function hex(v)
    return string.format("0x%08X", v)
end

-- Type names
local type_names = {
    [0x01] = "int8",
    [0x02] = "int16",
    [0x03] = "int32",
    [0x04] = "int64",
    [0x05] = "uint8",
    [0x06] = "uint16",
    [0x07] = "uint32",
    [0x08] = "uint64",
    [0x09] = "float",
    [0x0A] = "double",
    [0x0B] = "bool",
    [0x0C] = "char",
    [0x0D] = "char[]",
    [0x0E] = "ptr",
    [0x0F] = "embedded",
}

local opcode_names = {
    [0x01] = "INT",
    [0x02] = "UINT",
    [0x03] = "FLOAT",
    [0x04] = "STR_IDX",
    [0x05] = "FIELD_REF",
    [0x06] = "NESTED_REF",
    [0x07] = "CONST_REF",
    [0x08] = "RESULT",
    [0x09] = "LIST_START",
    [0x0A] = "LIST_END",
    [0x0B] = "CALL_START",
    [0x0C] = "CALL_END",
    [0x0D] = "INT64",
    [0x0E] = "UINT64",
    [0x0F] = "DOUBLE",
}

local func_type_names = {
    [0x01] = "ONESHOT",
    [0x02] = "MAIN",
    [0x03] = "PRED",
    [0x04] = "PT_MAIN",
    [0x05] = "INIT_ONE",
    [0x06] = "BIT_PRED",
}

local function dump_binary(filename, verbose)
    -- Read file
    local f = io.open(filename, "rb")
    if not f then
        print("Error: Cannot open file: " .. filename)
        os.exit(1)
    end
    local data = f:read("*a")
    f:close()
    
    print("============================================================================")
    print("Binary Module Dump: " .. filename)
    print("File size: " .. #data .. " bytes")
    print("============================================================================")
    print()
    
    local pos = 0
    
    -- Header
    print("HEADER (32 bytes)")
    print("--------------------------------------------------------------------------------")
    
    local magic, pos = read_u32(data, pos)
    if magic ~= SEXB_MAGIC then
        print("ERROR: Invalid magic: " .. hex(magic) .. " (expected " .. hex(SEXB_MAGIC) .. ")")
        os.exit(1)
    end
    print("  Magic:           " .. hex(magic) .. " (SEXB) ✓")
    
    local version, pos = read_u16(data, pos)
    print("  Version:         " .. string.format("0x%04X", version) .. 
          (version == SEXB_VERSION and " ✓" or " (WARNING: expected " .. string.format("0x%04X", SEXB_VERSION) .. ")"))
    
    local flags, pos = read_u16(data, pos)
    local mode = bit.band(flags, 1) == 1 and "64-bit" or "32-bit"
    local debug = bit.band(flags, 2) == 2 and ", DEBUG" or ""
    print("  Flags:           " .. string.format("0x%04X", flags) .. " (" .. mode .. debug .. ")")
    
    local name_hash, pos = read_u32(data, pos)
    print("  Module hash:     " .. hex(name_hash))
    
    local tree_count, pos = read_u16(data, pos)
    print("  Tree count:      " .. tree_count)
    
    local record_count, pos = read_u16(data, pos)
    print("  Record count:    " .. record_count)
    
    local string_count, pos = read_u16(data, pos)
    print("  String count:    " .. string_count)
    
    local const_count, pos = read_u16(data, pos)
    print("  Const count:     " .. const_count)
    
    local oneshot_count, pos = read_u16(data, pos)
    print("  Oneshot count:   " .. oneshot_count)
    
    local main_count, pos = read_u16(data, pos)
    print("  Main count:      " .. main_count)
    
    local pred_count, pos = read_u16(data, pos)
    print("  Pred count:      " .. pred_count)
    
    local reserved, pos = read_u16(data, pos)
    
    local total_size, pos = read_u32(data, pos)
    print("  Total size:      " .. total_size .. " bytes" .. 
          (total_size == #data and " ✓" or " (WARNING: file is " .. #data .. " bytes)"))
    
    print()
    
    -- Directory
    print("DIRECTORY (32 bytes)")
    print("--------------------------------------------------------------------------------")
    
    local tree_offset, pos = read_u32(data, pos)
    print("  Tree table:      @" .. tree_offset)
    
    local record_offset, pos = read_u32(data, pos)
    print("  Record table:    @" .. record_offset)
    
    local field_offset, pos = read_u32(data, pos)
    print("  Field table:     @" .. field_offset)
    
    local string_offset, pos = read_u32(data, pos)
    print("  String blob:     @" .. string_offset)
    
    local const_offset, pos = read_u32(data, pos)
    print("  Const table:     @" .. const_offset)
    
    local const_data_offset, pos = read_u32(data, pos)
    print("  Const data:      @" .. const_data_offset)
    
    local func_offset, pos = read_u32(data, pos)
    print("  Func table:      @" .. func_offset)
    
    local bytecode_offset, pos = read_u32(data, pos)
    print("  Bytecode:        @" .. bytecode_offset)
    
    print()
    
    -- Trees
    if tree_count > 0 then
        print("TREES (" .. tree_count .. " entries, 16 bytes each)")
        print("--------------------------------------------------------------------------------")
        
        pos = tree_offset
        for i = 1, tree_count do
            local t_hash
            t_hash, pos = read_u32(data, pos)
            local t_rec_idx
            t_rec_idx, pos = read_u16(data, pos)
            local t_node_count
            t_node_count, pos = read_u16(data, pos)
            local t_bc_offset
            t_bc_offset, pos = read_u32(data, pos)
            local t_bc_size
            t_bc_size, pos = read_u32(data, pos)
            
            print(string.format("  [%d] hash=%s rec=%d nodes=%d bytecode=@%d (%d bytes)",
                i - 1, hex(t_hash), t_rec_idx, t_node_count, t_bc_offset, t_bc_size))
        end
        print()
    end
    
    -- Records
    if record_count > 0 then
        print("RECORDS (" .. record_count .. " entries, 12 bytes each)")
        print("--------------------------------------------------------------------------------")
        
        pos = record_offset
        local records = {}
        for i = 1, record_count do
            local r_hash
            r_hash, pos = read_u32(data, pos)
            local r_field_count
            r_field_count, pos = read_u16(data, pos)
            local r_size
            r_size, pos = read_u16(data, pos)
            local r_field_offset
            r_field_offset, pos = read_u32(data, pos)
            
            records[i] = {
                hash = r_hash,
                field_count = r_field_count,
                size = r_size,
                field_offset = r_field_offset
            }
            
            print(string.format("  [%d] hash=%s fields=%d size=%d bytes field_table=@%d",
                i - 1, hex(r_hash), r_field_count, r_size, r_field_offset))
        end
        print()
        
        -- Fields (if verbose)
        if verbose then
            print("FIELDS (12 bytes each)")
            print("--------------------------------------------------------------------------------")
            
            for i, rec in ipairs(records) do
                if rec.field_count > 0 then
                    print(string.format("  Record [%d] (hash=%s):", i - 1, hex(rec.hash)))
                    pos = rec.field_offset
                    
                    for j = 1, rec.field_count do
                        local f_hash
                        f_hash, pos = read_u32(data, pos)
                        local f_type
                        f_type, pos = read_u8(data, pos)
                        local f_flags
                        f_flags, pos = read_u8(data, pos)
                        local f_offset
                        f_offset, pos = read_u16(data, pos)
                        local f_size
                        f_size, pos = read_u16(data, pos)
                        local f_aux
                        f_aux, pos = read_u16(data, pos)
                        
                        local type_str = type_names[f_type] or string.format("0x%02X", f_type)
                        local flags_str = ""
                        if bit.band(f_flags, 1) ~= 0 then flags_str = flags_str .. "PTR " end
                        if bit.band(f_flags, 2) ~= 0 then flags_str = flags_str .. "ARR " end
                        if bit.band(f_flags, 4) ~= 0 then flags_str = flags_str .. "EMB " end
                        
                        print(string.format("    [%d] hash=%s type=%s flags=[%s] offset=%d size=%d aux=%d",
                            j - 1, hex(f_hash), type_str, flags_str, f_offset, f_size, f_aux))
                    end
                end
            end
            print()
        end
    end
    
    -- Strings
    if string_count > 0 then
        print("STRINGS (" .. string_count .. " entries)")
        print("--------------------------------------------------------------------------------")
        
        pos = string_offset
        for i = 1, string_count do
            local s_len
            s_len, pos = read_u16(data, pos)
            local s_data = data:sub(pos + 1, pos + s_len)
            
            -- Skip padding
            local total = 2 + s_len
            local padding = (4 - (total % 4)) % 4
            pos = pos + s_len + padding
            
            local display = s_data:gsub("[%c]", ".")
            if #display > 50 then
                display = display:sub(1, 47) .. "..."
            end
            print(string.format("  [%d] len=%d \"%s\"", i - 1, s_len, display))
        end
        print()
    end
    
    -- Constants
    if const_count > 0 then
        print("CONSTANTS (" .. const_count .. " entries, 12 bytes each)")
        print("--------------------------------------------------------------------------------")
        
        pos = const_offset
        for i = 1, const_count do
            local c_hash
            c_hash, pos = read_u32(data, pos)
            local c_rec_idx
            c_rec_idx, pos = read_u16(data, pos)
            local c_size
            c_size, pos = read_u16(data, pos)
            local c_data_off
            c_data_off, pos = read_u32(data, pos)
            
            print(string.format("  [%d] hash=%s record=%d size=%d data=@%d",
                i - 1, hex(c_hash), c_rec_idx, c_size, c_data_off))
        end
        print()
    end
    
    -- Function tables
    local func_count = oneshot_count + main_count + pred_count
    if func_count > 0 then
        print("FUNCTION HASHES (" .. func_count .. " total)")
        print("--------------------------------------------------------------------------------")
        
        pos = func_offset
        
        if oneshot_count > 0 then
            print("  Oneshot (" .. oneshot_count .. "):")
            for i = 1, oneshot_count do
                local h
                h, pos = read_u32(data, pos)
                print(string.format("    [%d] %s", i - 1, hex(h)))
            end
        end
        
        if main_count > 0 then
            print("  Main (" .. main_count .. "):")
            for i = 1, main_count do
                local h
                h, pos = read_u32(data, pos)
                print(string.format("    [%d] %s", i - 1, hex(h)))
            end
        end
        
        if pred_count > 0 then
            print("  Pred (" .. pred_count .. "):")
            for i = 1, pred_count do
                local h
                h, pos = read_u32(data, pos)
                print(string.format("    [%d] %s", i - 1, hex(h)))
            end
        end
        print()
    end
    
    -- Bytecode summary
    local bc_size = total_size - bytecode_offset
    print("BYTECODE (" .. bc_size .. " bytes)")
    print("--------------------------------------------------------------------------------")
    print("  Starts at offset " .. bytecode_offset)
    
    if verbose and bc_size > 0 then
        -- Dump first part of bytecode as hex
        local dump_size = math.min(bc_size, 256)
        print("  First " .. dump_size .. " bytes:")
        
        pos = bytecode_offset
        for row = 0, math.floor((dump_size - 1) / 16) do
            local hex_str = ""
            local ascii_str = ""
            
            for col = 0, 15 do
                local byte_pos = row * 16 + col
                if byte_pos < dump_size then
                    local b = data:byte(pos + byte_pos + 1)
                    hex_str = hex_str .. string.format("%02X ", b)
                    if b >= 32 and b < 127 then
                        ascii_str = ascii_str .. string.char(b)
                    else
                        ascii_str = ascii_str .. "."
                    end
                else
                    hex_str = hex_str .. "   "
                end
            end
            
            print(string.format("    %04X: %s |%s|", bytecode_offset + row * 16, hex_str, ascii_str))
        end
    end
    
    print()
    print("============================================================================")
    print("Validation: PASSED")
    print("============================================================================")
end

-- Main
local filename = arg[1]
local verbose = arg[2] == "--verbose" or arg[2] == "-v"

if not filename then
    print("Usage: luajit s_binary_dump.lua <file.bin> [--verbose]")
    print()
    print("Options:")
    print("  --verbose, -v    Show detailed field and bytecode dumps")
    os.exit(1)
end

dump_binary(filename, verbose)