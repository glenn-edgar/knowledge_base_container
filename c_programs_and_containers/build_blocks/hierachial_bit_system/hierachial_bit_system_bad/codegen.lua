#!/usr/bin/env luajit
--[[
  ChainTree Hierarchical Bit Map DSL Code Generator
  
  Generates:
    - generated_<name>.bin      : Binary descriptor data
    - generated_<name>.bin.h    : Binary as C array
    - generated_<name>.h        : Main API header
    - generated_<name>_hashes.h : String-to-hash reference
    
  Usage:
    luajit codegen.lua schema.lua [output_dir]
]]

local bit = require("bit")
local ffi = require("ffi")

--------------------------------------------------------------------------------
-- FNV-1a Hash (32-bit)
--------------------------------------------------------------------------------

local FNV_PRIME = 0x01000193ULL
local FNV_OFFSET = 0x811c9dc5ULL
local MASK32 = 0xFFFFFFFFULL

local function fnv1a_string(str)
    local hash = FNV_OFFSET
    for i = 1, #str do
        hash = bit.bxor(hash, string.byte(str, i))
        hash = bit.band(hash * FNV_PRIME, MASK32)
    end
    -- Ensure we return a positive 32-bit value
    return tonumber(bit.band(hash, MASK32))
end

-- Hash a path string
local function hash_path(path)
    return fnv1a_string(path)
end

--------------------------------------------------------------------------------
-- Binary Writer Helper
--------------------------------------------------------------------------------

local BinaryWriter = {}
BinaryWriter.__index = BinaryWriter

function BinaryWriter.new()
    local self = setmetatable({}, BinaryWriter)
    self.buffer = {}
    self.pos = 0
    return self
end

function BinaryWriter:u8(val)
    self.buffer[#self.buffer + 1] = string.char(bit.band(val, 0xFF))
    self.pos = self.pos + 1
end

function BinaryWriter:u16(val)
    self:u8(bit.band(val, 0xFF))
    self:u8(bit.band(bit.rshift(val, 8), 0xFF))
end

function BinaryWriter:u32(val)
    self:u8(bit.band(val, 0xFF))
    self:u8(bit.band(bit.rshift(val, 8), 0xFF))
    self:u8(bit.band(bit.rshift(val, 16), 0xFF))
    self:u8(bit.band(bit.rshift(val, 24), 0xFF))
end

function BinaryWriter:i32(val)
    self:u32(val)
end

function BinaryWriter:f32(val)
    local buf = ffi.new("float[1]", val)
    local bytes = ffi.cast("uint8_t*", buf)
    for i = 0, 3 do
        self:u8(bytes[i])
    end
end

function BinaryWriter:str(s, max_len)
    max_len = max_len or 256
    local len = math.min(#s, max_len - 1)
    for i = 1, len do
        self:u8(string.byte(s, i))
    end
    -- Null terminator and padding
    for i = len + 1, max_len do
        self:u8(0)
    end
end

function BinaryWriter:pad(alignment)
    while self.pos % alignment ~= 0 do
        self:u8(0)
    end
end

function BinaryWriter:get_pos()
    return self.pos
end

function BinaryWriter:to_string()
    return table.concat(self.buffer)
end

--------------------------------------------------------------------------------
-- DSL Runtime Support
--------------------------------------------------------------------------------

-- Inject schema_builder into package.loaded so schemas can require it
local schema_builder_path = debug.getinfo(1, "S").source:match("@?(.*/)")
if schema_builder_path then
    package.path = schema_builder_path .. "?.lua;" .. package.path
end

-- Also provide legacy dsl_runtime for old schemas
local dsl_runtime = {}
function dsl_runtime.Schema(t) return t end
function dsl_runtime.Bitspace(t) return t end
function dsl_runtime.DeviceClass(t) return t end
function dsl_runtime.Class(t) return t end
function dsl_runtime.Node(t) return t end
package.loaded["dsl_runtime"] = dsl_runtime

--------------------------------------------------------------------------------
-- Schema Processing
--------------------------------------------------------------------------------

local function process_schema(schema)
    local result = {
        name = schema.name,
        version = schema.version or "1.0.0",
        options = schema.options or {},
        bitspaces = {},
        bitspace_by_name = {},
        classes = {},
        class_by_name = {},
        nodes = {},
        node_by_path = {},
        node_by_hash = {},
        config = schema.config or {},
        hashes = {},  -- All path/string hashes for reference
    }
    
    -- Process bitspaces
    for i, bs in ipairs(schema.bitspaces) do
        local bitspace = {
            index = i - 1,
            name = bs.name,
            merge = bs.merge,
            base_merge = bs.base_merge,
            latch = bs.latch or false,
            clear_requires_inactive = bs.clear_requires_inactive or false,
            priority_order = bs.priority_order or {},
        }
        result.bitspaces[i] = bitspace
        result.bitspace_by_name[bs.name] = bitspace
        
        -- Hash bitspace name
        result.hashes[bs.name] = hash_path(bs.name)
    end
    
    -- Process classes
    for i, cls in ipairs(schema.classes) do
        -- Normalize bits: convert {"Name"} to {{name="Name", idx=0}}
        local normalized_bits = {}
        for bs_name, bit_list in pairs(cls.bits or {}) do
            normalized_bits[bs_name] = {}
            for idx, bit_def in ipairs(bit_list) do
                local bit_name = type(bit_def) == "string" and bit_def or bit_def.name
                normalized_bits[bs_name][idx] = {
                    name = bit_name,
                    idx = idx - 1  -- 0-based index
                }
            end
        end
        
        local class = {
            index = i - 1,
            name = cls.name,
            banks = cls.banks or {},
            bits = normalized_bits,
            default_masks = cls.default_masks or {},
        }
        result.classes[i] = class
        result.class_by_name[cls.name] = class
        
        -- Hash class name
        result.hashes[cls.name] = hash_path(cls.name)
        
        -- Hash bit names
        for bs_name, bit_list in pairs(normalized_bits) do
            for _, bit_def in ipairs(bit_list) do
                local full_name = cls.name .. "." .. bs_name .. "." .. bit_def.name
                result.hashes[full_name] = hash_path(full_name)
            end
        end
    end
    
    -- Process nodes and build tree structure
    for i, node in ipairs(schema.nodes) do
        local path = node.path
        local class = result.class_by_name[node.class]
        if not class then
            error("Unknown class '" .. node.class .. "' for node '" .. path .. "'")
        end
        
        local path_hash = hash_path(path)
        
        -- Find parent path
        local parent_path = nil
        local dot_pos = path:match(".*()%.")
        if dot_pos then
            parent_path = path:sub(1, dot_pos - 1)
        end
        
        local n = {
            index = i - 1,
            path = path,
            path_hash = path_hash,
            class = class,
            class_index = class.index,
            masks = node.masks or {},
            parent_path = parent_path,
            parent_index = -1,  -- Filled in later
            children = {},
            depth = 0,
        }
        
        result.nodes[i] = n
        result.node_by_path[path] = n
        result.node_by_hash[path_hash] = n
        
        -- Hash the path
        result.hashes[path] = path_hash
    end
    
    -- Resolve parent indices and build children lists
    for _, node in ipairs(result.nodes) do
        if node.parent_path then
            local parent = result.node_by_path[node.parent_path]
            if parent then
                node.parent_index = parent.index
                table.insert(parent.children, node)
            end
        end
    end
    
    -- Calculate depths
    local function calc_depth(node, depth)
        node.depth = depth
        for _, child in ipairs(node.children) do
            calc_depth(child, depth + 1)
        end
    end
    for _, node in ipairs(result.nodes) do
        if node.parent_index == -1 then
            calc_depth(node, 0)
        end
    end
    
    -- Hash config paths recursively
    local function hash_config(prefix, tbl)
        for k, v in pairs(tbl) do
            local path = prefix == "" and k or (prefix .. "." .. k)
            result.hashes[path] = hash_path(path)
            if type(v) == "table" then
                hash_config(path, v)
            end
        end
    end
    hash_config("", result.config)
    
    return result
end

--------------------------------------------------------------------------------
-- Calculate Arena Sizes
--------------------------------------------------------------------------------

local function calculate_arenas(schema)
    local arenas = {}
    
    for i, bs in ipairs(schema.bitspaces) do
        local arena = {
            bitspace_index = bs.index,
            bitspace_name = bs.name,
            total_bits = 0,
            total_bytes = 0,
            node_offsets = {},  -- node_index -> offset
        }
        
        local offset = 0
        for _, node in ipairs(schema.nodes) do
            local bank_size = node.class.banks[bs.name] or 0
            local byte_size = math.ceil(bank_size / 8)
            
            arena.node_offsets[node.index] = offset
            offset = offset + byte_size
            arena.total_bits = arena.total_bits + bank_size
        end
        arena.total_bytes = offset
        
        -- For latched bitspaces, we need 2x (live + latched)
        if bs.latch then
            arena.total_bytes_with_latch = arena.total_bytes * 2
        else
            arena.total_bytes_with_latch = arena.total_bytes
        end
        
        -- Store with 1-based indexing for Lua compatibility
        arenas[i] = arena
    end
    
    return arenas
end

--------------------------------------------------------------------------------
-- Binary Generation
--------------------------------------------------------------------------------

-- Magic number: "HBIT" in little-endian
local MAGIC = 0x54494248

-- Binary format version
local FORMAT_VERSION = 1

-- Merge type codes
local MERGE_CODES = {
    OR = 0,
    AND = 1,
    PRIORITY = 2,
    MASK = 3,
}

-- Config value type codes
local CONFIG_TYPE = {
    NULL = 0,
    INT = 1,
    FLOAT = 2,
    BOOL = 3,
    STRING = 4,
    TABLE = 5,
}

local function generate_binary(schema, arenas)
    local w = BinaryWriter.new()
    
    -- Count config entries
    local config_entries = {}
    local function collect_config(prefix, tbl)
        for k, v in pairs(tbl) do
            local path = prefix == "" and k or (prefix .. "." .. k)
            local vtype = type(v)
            if vtype == "table" then
                collect_config(path, v)
            else
                table.insert(config_entries, {
                    path = path,
                    hash = hash_path(path),
                    value = v,
                    vtype = vtype,
                })
            end
        end
    end
    collect_config("", schema.config)
    
    -- Sort config entries by hash for binary search
    table.sort(config_entries, function(a, b) return a.hash < b.hash end)
    
    -- ========== HEADER ==========
    -- Offset 0x00: Magic
    w:u32(MAGIC)
    -- Offset 0x04: Version
    w:u32(FORMAT_VERSION)
    -- Offset 0x08: Bitspace count
    w:u16(#schema.bitspaces)
    -- Offset 0x0A: Class count
    w:u16(#schema.classes)
    -- Offset 0x0C: Node count
    w:u16(#schema.nodes)
    -- Offset 0x0E: Config entry count
    w:u16(#config_entries)
    -- Offset 0x10: Max depth
    w:u16(schema.options.max_depth or 16)
    -- Offset 0x12: Reserved
    w:u16(0)
    
    -- Offset 0x14: Section offsets (filled in later)
    local header_offset_pos = w:get_pos()
    w:u32(0)  -- bitspaces_offset
    w:u32(0)  -- classes_offset
    w:u32(0)  -- nodes_offset
    w:u32(0)  -- arenas_offset
    w:u32(0)  -- config_offset
    w:u32(0)  -- strings_offset
    
    w:pad(16)
    
    -- ========== BITSPACES SECTION ==========
    local bitspaces_offset = w:get_pos()
    
    for _, bs in ipairs(schema.bitspaces) do
        -- Bitspace descriptor (32 bytes)
        w:u32(schema.hashes[bs.name])     -- name_hash
        w:u8(MERGE_CODES[bs.merge] or 0)  -- merge_type
        w:u8(bs.base_merge and MERGE_CODES[bs.base_merge] or 0)  -- base_merge_type
        w:u8(bs.latch and 1 or 0)         -- latch flag
        w:u8(bs.clear_requires_inactive and 1 or 0)  -- clear_requires_inactive
        w:u8(#bs.priority_order)          -- priority_count
        w:u8(0)                           -- reserved
        w:u16(0)                          -- reserved
        
        -- Priority order hashes (up to 8)
        for i = 1, 8 do
            local prio_name = bs.priority_order[i]
            if prio_name then
                w:u32(hash_path(prio_name))
            else
                w:u32(0)
            end
        end
    end
    
    w:pad(16)
    
    -- ========== CLASSES SECTION ==========
    local classes_offset = w:get_pos()
    
    for _, cls in ipairs(schema.classes) do
        -- Class descriptor header
        w:u32(schema.hashes[cls.name])    -- name_hash
        w:u16(#schema.bitspaces)          -- bank_count (one per bitspace)
        w:u16(0)                          -- reserved
        
        -- Bank sizes (bits) for each bitspace
        for _, bs in ipairs(schema.bitspaces) do
            local size = cls.banks[bs.name] or 0
            w:u16(size)
        end
        
        -- Note: default_masks removed - masks are now runtime-only for leaf nodes
    end
    
    w:pad(16)
    
    -- ========== NODES SECTION ==========
    -- Sort nodes by hash for binary search
    local sorted_nodes = {}
    for _, node in ipairs(schema.nodes) do
        table.insert(sorted_nodes, node)
    end
    table.sort(sorted_nodes, function(a, b) return a.path_hash < b.path_hash end)
    
    -- Create hash-sorted index mapping
    local hash_sorted_index = {}
    for i, node in ipairs(sorted_nodes) do
        hash_sorted_index[node.index] = i - 1
    end
    
    local nodes_offset = w:get_pos()
    
    for _, node in ipairs(sorted_nodes) do
        -- Node descriptor
        w:u32(node.path_hash)             -- path_hash
        w:u16(node.class_index)           -- class_index
        w:u16(node.depth)                 -- depth
        
        -- Parent index in hash-sorted order (-1 if root)
        if node.parent_index >= 0 then
            w:i32(hash_sorted_index[node.parent_index])
        else
            w:i32(-1)
        end
        
        -- Child count and first child index (for iteration)
        w:u16(#node.children)
        w:u16(0)  -- reserved
        
        -- Note: masks removed from binary - now runtime-only for leaf nodes
    end
    
    w:pad(16)
    
    -- ========== ARENAS SECTION ==========
    local arenas_offset = w:get_pos()
    
    for _, arena in ipairs(arenas) do
        -- Arena descriptor
        w:u32(arena.total_bytes)          -- size
        w:u32(arena.total_bytes_with_latch)  -- size_with_latch
        
        -- Node offsets within arena (in hash-sorted order)
        for _, node in ipairs(sorted_nodes) do
            w:u32(arena.node_offsets[node.index])
        end
    end
    
    w:pad(16)
    
    -- ========== CONFIG SECTION ==========
    local config_offset = w:get_pos()
    local string_table = {}
    local string_offset = 0
    
    for _, entry in ipairs(config_entries) do
        w:u32(entry.hash)  -- path_hash
        
        if entry.vtype == "number" then
            -- Check if it's an integer or float using string representation
            local str = tostring(entry.value)
            local is_float = str:find("%.") ~= nil or str:find("e") ~= nil or str:find("E") ~= nil
            
            if not is_float and math.floor(entry.value) == entry.value and 
               entry.value >= -2147483648 and entry.value <= 2147483647 then
                w:u8(CONFIG_TYPE.INT)
                w:u8(0)
                w:u16(0)
                w:i32(entry.value)
            else
                w:u8(CONFIG_TYPE.FLOAT)
                w:u8(0)
                w:u16(0)
                w:f32(entry.value)
            end
        elseif entry.vtype == "boolean" then
            w:u8(CONFIG_TYPE.BOOL)
            w:u8(0)
            w:u16(0)
            w:u32(entry.value and 1 or 0)
        elseif entry.vtype == "string" then
            w:u8(CONFIG_TYPE.STRING)
            w:u8(0)
            w:u16(0)
            w:u32(string_offset)
            table.insert(string_table, entry.value)
            string_offset = string_offset + #entry.value + 1
        else
            w:u8(CONFIG_TYPE.NULL)
            w:u8(0)
            w:u16(0)
            w:u32(0)
        end
    end
    
    w:pad(16)
    
    -- ========== STRINGS SECTION ==========
    local strings_offset = w:get_pos()
    
    for _, str in ipairs(string_table) do
        for i = 1, #str do
            w:u8(string.byte(str, i))
        end
        w:u8(0)  -- null terminator
    end
    
    w:pad(16)
    
    -- ========== PATCH HEADER OFFSETS ==========
    local binary = w:to_string()
    local function patch_u32(data, offset, value)
        local b0 = string.char(bit.band(value, 0xFF))
        local b1 = string.char(bit.band(bit.rshift(value, 8), 0xFF))
        local b2 = string.char(bit.band(bit.rshift(value, 16), 0xFF))
        local b3 = string.char(bit.band(bit.rshift(value, 24), 0xFF))
        return data:sub(1, offset) .. b0 .. b1 .. b2 .. b3 .. data:sub(offset + 5)
    end
    
    binary = patch_u32(binary, header_offset_pos, bitspaces_offset)
    binary = patch_u32(binary, header_offset_pos + 4, classes_offset)
    binary = patch_u32(binary, header_offset_pos + 8, nodes_offset)
    binary = patch_u32(binary, header_offset_pos + 12, arenas_offset)
    binary = patch_u32(binary, header_offset_pos + 16, config_offset)
    binary = patch_u32(binary, header_offset_pos + 20, strings_offset)
    
    return binary
end

--------------------------------------------------------------------------------
-- Binary Header Generation (.bin.h) - Single Runtime Header
--------------------------------------------------------------------------------

local function generate_binary_header(schema, arenas, binary)
    local lines = {}
    local function emit(fmt, ...)
        table.insert(lines, string.format(fmt, ...))
    end
    
    local guard = string.upper(schema.name) .. "_H"
    local prefix = string.upper(schema.name)
    
    emit("/**")
    emit(" * @file generated_%s.bin.h", schema.name)
    emit(" * @brief ChainTree Hierarchical Bit Map - %s", schema.name)
    emit(" * @version %s", schema.version)
    emit(" *")
    emit(" * Single-file runtime header containing all definitions and binary data.")
    emit(" * Include this file in exactly one .c file in your project.")
    emit(" *")
    emit(" * Auto-generated by codegen.lua - DO NOT EDIT")
    emit(" */")
    emit("")
    emit("#ifndef %s", guard)
    emit("#define %s", guard)
    emit("")
    emit("#include <stdint.h>")
    emit("#include <stdbool.h>")
    emit("")
    emit("#ifdef __cplusplus")
    emit("extern \"C\" {")
    emit("#endif")
    emit("")
    
    -- Schema info
    emit("/* ============================================ */")
    emit("/* Schema Info                                  */")
    emit("/* ============================================ */")
    emit("")
    emit("#define %s_VERSION \"%s\"", prefix, schema.version)
    emit("#define %s_NODE_COUNT %d", prefix, #schema.nodes)
    emit("#define %s_BITSPACE_COUNT %d", prefix, #schema.bitspaces)
    emit("#define %s_CLASS_COUNT %d", prefix, #schema.classes)
    emit("#define %s_MAX_DEPTH %d", prefix, schema.options.max_depth or 16)
    emit("")
    
    -- Bitspace indices
    emit("/* ============================================ */")
    emit("/* Bitspace Indices                             */")
    emit("/* ============================================ */")
    emit("")
    emit("typedef enum {")
    for i, bs in ipairs(schema.bitspaces) do
        emit("    %s_BS_%s = %d,", prefix, string.upper(bs.name), bs.index)
    end
    emit("} %s_bitspace_t;", schema.name)
    emit("")
    
    -- Merge types
    emit("/* ============================================ */")
    emit("/* Merge Types                                  */")
    emit("/* ============================================ */")
    emit("")
    emit("#ifndef CFL_HBIT_MERGE_DEFINED")
    emit("#define CFL_HBIT_MERGE_DEFINED")
    emit("typedef enum {")
    emit("    CFL_HBIT_MERGE_OR = 0,")
    emit("    CFL_HBIT_MERGE_AND = 1,")
    emit("    CFL_HBIT_MERGE_PRIORITY = 2,")
    emit("    CFL_HBIT_MERGE_MASK = 3,")
    emit("} cfl_hbit_merge_t;")
    emit("#endif")
    emit("")
    
    -- Class indices
    emit("/* ============================================ */")
    emit("/* Class Indices                                */")
    emit("/* ============================================ */")
    emit("")
    emit("typedef enum {")
    for i, cls in ipairs(schema.classes) do
        emit("    %s_CLASS_%s = %d,", prefix, string.upper(cls.name), cls.index)
    end
    emit("} %s_class_t;", schema.name)
    emit("")
    
    -- Bit definitions per class
    emit("/* ============================================ */")
    emit("/* Bit Definitions                              */")
    emit("/* ============================================ */")
    emit("")
    
    for _, cls in ipairs(schema.classes) do
        local cls_prefix = prefix .. "_" .. string.upper(cls.name)
        local has_bits = false
        
        for bs_name, bit_list in pairs(cls.bits) do
            if #bit_list > 0 then
                has_bits = true
                emit("/* %s - %s */", cls.name, bs_name)
                for _, bit_def in ipairs(bit_list) do
                    emit("#define %s_%s_%s %d", cls_prefix, string.upper(bs_name), 
                         string.upper(bit_def.name), bit_def.idx)
                end
                emit("")
            end
        end
    end
    
    -- Arena sizes
    emit("/* ============================================ */")
    emit("/* Arena Sizes (bytes)                          */")
    emit("/* ============================================ */")
    emit("")
    
    for _, arena in ipairs(arenas) do
        local bs = schema.bitspaces[arena.bitspace_index + 1]
        emit("#define %s_ARENA_%s_SIZE %d", prefix, string.upper(bs.name), arena.total_bytes)
        if bs.latch then
            emit("#define %s_ARENA_%s_SIZE_WITH_LATCH %d", prefix, string.upper(bs.name), 
                 arena.total_bytes_with_latch)
        end
    end
    emit("")
    
    -- Total RAM required
    local total_ram = 0
    for _, arena in ipairs(arenas) do
        local bs = schema.bitspaces[arena.bitspace_index + 1]
        -- Shadow + current
        total_ram = total_ram + arena.total_bytes_with_latch * 2
    end
    emit("#define %s_TOTAL_RAM_BYTES %d", prefix, total_ram)
    emit("")
    
    -- Priority state indices (if any PRIORITY bitspaces)
    local has_priority = false
    for _, bs in ipairs(schema.bitspaces) do
        if bs.merge == "PRIORITY" and #bs.priority_order > 0 then
            has_priority = true
            break
        end
    end
    
    if has_priority then
        emit("/* ============================================ */")
        emit("/* Priority States                              */")
        emit("/* ============================================ */")
        emit("")
        
        for _, bs in ipairs(schema.bitspaces) do
            if bs.merge == "PRIORITY" and #bs.priority_order > 0 then
                emit("/* %s */", bs.name)
                for i, state in ipairs(bs.priority_order) do
                    emit("#define %s_%s_%s %d", prefix, string.upper(bs.name), 
                         string.upper(state), i - 1)
                end
                emit("")
            end
        end
    end
    
    -- Binary descriptor data
    emit("/* ============================================ */")
    emit("/* Binary Descriptor Data                       */")
    emit("/* ============================================ */")
    emit("")
    emit("static const uint32_t %s_descriptor_size = %d;", schema.name, #binary)
    emit("")
    emit("static const uint8_t %s_descriptor[%d] = {", schema.name, #binary)
    
    -- Output bytes, 16 per line
    local bytes_per_line = 16
    for i = 1, #binary, bytes_per_line do
        local line_bytes = {}
        for j = i, math.min(i + bytes_per_line - 1, #binary) do
            table.insert(line_bytes, string.format("0x%02X", string.byte(binary, j)))
        end
        local suffix = (i + bytes_per_line - 1 < #binary) and "," or ""
        emit("    %s%s", table.concat(line_bytes, ", "), suffix)
    end
    
    emit("};")
    emit("")
    
    emit("#ifdef __cplusplus")
    emit("}")
    emit("#endif")
    emit("")
    emit("#endif /* %s */", guard)
    
    return table.concat(lines, "\n")
end

--------------------------------------------------------------------------------
-- Hash Reference Header Generation
--------------------------------------------------------------------------------

local function generate_hash_header(schema)
    local lines = {}
    local function emit(fmt, ...)
        table.insert(lines, string.format(fmt, ...))
    end
    
    local guard = string.upper(schema.name) .. "_HASHES_H"
    local prefix = string.upper(schema.name)
    
    emit("/**")
    emit(" * @file generated_%s_hashes.h", schema.name)
    emit(" * @brief Definitions and hashes for %s", schema.name)
    emit(" *")
    emit(" * Contains:")
    emit(" *   - Bitspace IDs (indices)")
    emit(" *   - Node indices")
    emit(" *   - Bit indices per class/bitspace")
    emit(" *   - Path string hashes (for debugging)")
    emit(" *")
    emit(" * Auto-generated by codegen.lua - DO NOT EDIT")
    emit(" */")
    emit("")
    emit("#ifndef %s", guard)
    emit("#define %s", guard)
    emit("")
    emit("#include <stdint.h>")
    emit("")
    
    -- Schema info
    emit("/* ============================================ */")
    emit("/* Schema Info                                  */")
    emit("/* ============================================ */")
    emit("")
    emit("#define %s_NODE_COUNT %d", prefix, #schema.nodes)
    emit("#define %s_BITSPACE_COUNT %d", prefix, #schema.bitspaces)
    emit("#define %s_CLASS_COUNT %d", prefix, #schema.classes)
    emit("")
    
    -- Bitspace IDs (indices)
    emit("/* ============================================ */")
    emit("/* Bitspace IDs (indices)                       */")
    emit("/* ============================================ */")
    emit("")
    for i, bs in ipairs(schema.bitspaces) do
        emit("#define %s_BS_%s %d", prefix, string.upper(bs.name), i - 1)
    end
    emit("")
    
    -- Node indices
    emit("/* ============================================ */")
    emit("/* Node Indices                                 */")
    emit("/* ============================================ */")
    emit("")
    -- Build node index map
    local node_index = {}
    for i, node in ipairs(schema.nodes) do
        node_index[node.path] = i - 1
    end
    -- Sort by path
    local sorted_nodes = {}
    for _, node in ipairs(schema.nodes) do
        table.insert(sorted_nodes, node)
    end
    table.sort(sorted_nodes, function(a, b) return a.path < b.path end)
    for _, node in ipairs(sorted_nodes) do
        local macro_name = node.path:gsub("%.", "_"):upper()
        emit("#define %s_NODE_%s %d", prefix, macro_name, node_index[node.path])
    end
    emit("")
    
    -- Bit indices per class/bitspace
    emit("/* ============================================ */")
    emit("/* Bit Indices (Class.Bitspace.BitName)         */")
    emit("/* ============================================ */")
    emit("")
    for _, cls in ipairs(schema.classes) do
        local has_bits = false
        for bs_name, bits in pairs(cls.bits or {}) do
            if type(bits) == "table" and #bits > 0 then
                has_bits = true
                emit("/* %s.%s */", cls.name, bs_name)
                for _, bit_def in ipairs(bits) do
                    -- Handle both normalized ({name=, idx=}) and simple (string) formats
                    local bit_name, bit_idx
                    if type(bit_def) == "table" then
                        bit_name = bit_def.name
                        bit_idx = bit_def.idx
                    elseif type(bit_def) == "string" then
                        bit_name = bit_def
                        bit_idx = _ - 1  -- 0-based from loop index
                    end
                    if bit_name then
                        emit("#define %s_BIT_%s_%s_%s %d", 
                             prefix, 
                             string.upper(cls.name), 
                             string.upper(bs_name), 
                             string.upper(bit_name), 
                             bit_idx)
                    end
                end
                emit("")
            end
        end
    end
    
    -- Bank sizes per class/bitspace
    emit("/* ============================================ */")
    emit("/* Bank Sizes (bits) per Class/Bitspace         */")
    emit("/* ============================================ */")
    emit("")
    for _, cls in ipairs(schema.classes) do
        local has_banks = false
        for bs_name, size in pairs(cls.banks or {}) do
            if size > 0 then
                has_banks = true
            end
        end
        if has_banks then
            emit("/* %s */", cls.name)
            for _, bs in ipairs(schema.bitspaces) do
                local size = cls.banks[bs.name] or 0
                if size > 0 then
                    emit("#define %s_BANK_%s_%s %d", 
                         prefix, 
                         string.upper(cls.name), 
                         string.upper(bs.name), 
                         size)
                end
            end
            emit("")
        end
    end
    
    -- Node paths
    emit("/* ============================================ */")
    emit("/* Node Path Hashes                             */")
    emit("/* ============================================ */")
    emit("")
    
    -- Sort nodes by path for readability
    local sorted_paths = {}
    for path, hash in pairs(schema.hashes) do
        -- Only include node paths (contain dots or are root)
        if schema.node_by_path[path] then
            table.insert(sorted_paths, {path = path, hash = hash})
        end
    end
    table.sort(sorted_paths, function(a, b) return a.path < b.path end)
    
    for _, entry in ipairs(sorted_paths) do
        local macro_name = entry.path:gsub("%.", "_"):upper()
        emit("#define %s_HASH_%s 0x%08XU  /* \"%s\" */", 
             prefix, macro_name, entry.hash, entry.path)
    end
    emit("")
    
    -- Bitspace names
    emit("/* ============================================ */")
    emit("/* Bitspace Name Hashes                         */")
    emit("/* ============================================ */")
    emit("")
    
    for _, bs in ipairs(schema.bitspaces) do
        emit("#define %s_HASH_BS_%s 0x%08XU  /* \"%s\" */",
             prefix, string.upper(bs.name), schema.hashes[bs.name], bs.name)
    end
    emit("")
    
    -- Class names
    emit("/* ============================================ */")
    emit("/* Class Name Hashes                            */")
    emit("/* ============================================ */")
    emit("")
    
    for _, cls in ipairs(schema.classes) do
        emit("#define %s_HASH_CLASS_%s 0x%08XU  /* \"%s\" */",
             prefix, string.upper(cls.name), schema.hashes[cls.name], cls.name)
    end
    emit("")
    
    -- Config paths
    emit("/* ============================================ */")
    emit("/* Config Path Hashes                           */")
    emit("/* ============================================ */")
    emit("")
    
    local config_paths = {}
    for path, hash in pairs(schema.hashes) do
        if not schema.node_by_path[path] and 
           not schema.bitspace_by_name[path] and
           not schema.class_by_name[path] then
            table.insert(config_paths, {path = path, hash = hash})
        end
    end
    table.sort(config_paths, function(a, b) return a.path < b.path end)
    
    for _, entry in ipairs(config_paths) do
        local macro_name = entry.path:gsub("%.", "_"):upper()
        emit("#define %s_HASH_CFG_%s 0x%08XU  /* \"%s\" */",
             prefix, macro_name, entry.hash, entry.path)
    end
    emit("")
    
    -- Priority state hashes
    local has_priority = false
    for _, bs in ipairs(schema.bitspaces) do
        if bs.merge == "PRIORITY" and #bs.priority_order > 0 then
            has_priority = true
            break
        end
    end
    
    if has_priority then
        emit("/* ============================================ */")
        emit("/* Priority State Hashes                        */")
        emit("/* ============================================ */")
        emit("")
        
        local seen = {}
        for _, bs in ipairs(schema.bitspaces) do
            if bs.merge == "PRIORITY" then
                for _, state in ipairs(bs.priority_order) do
                    if not seen[state] then
                        emit("#define %s_HASH_PRIO_%s 0x%08XU  /* \"%s\" */",
                             prefix, string.upper(state), hash_path(state), state)
                        seen[state] = true
                    end
                end
            end
        end
        emit("")
    end
    
    -- Lookup table for runtime debugging (optional)
    emit("/* ============================================ */")
    emit("/* Debug Lookup Table (define %s_INCLUDE_DEBUG_STRINGS) */", prefix)
    emit("/* ============================================ */")
    emit("")
    emit("#ifdef %s_INCLUDE_DEBUG_STRINGS", prefix)
    emit("")
    emit("typedef struct {")
    emit("    uint32_t hash;")
    emit("    const char* str;")
    emit("} %s_hash_entry_t;", schema.name)
    emit("")
    emit("static const %s_hash_entry_t %s_hash_table[] = {", schema.name, schema.name)
    
    -- Combine all hashes and sort by hash value
    local all_hashes = {}
    for path, hash in pairs(schema.hashes) do
        table.insert(all_hashes, {path = path, hash = hash})
    end
    table.sort(all_hashes, function(a, b) return a.hash < b.hash end)
    
    for i, entry in ipairs(all_hashes) do
        local comma = (i < #all_hashes) and "," or ""
        emit("    { 0x%08XU, \"%s\" }%s", entry.hash, entry.path, comma)
    end
    
    emit("};")
    emit("")
    emit("static const uint32_t %s_hash_table_size = %d;", schema.name, #all_hashes)
    emit("")
    emit("#endif /* %s_INCLUDE_DEBUG_STRINGS */", prefix)
    emit("")
    
    emit("#endif /* %s */", guard)
    
    return table.concat(lines, "\n")
end

--------------------------------------------------------------------------------
-- Memory Summary Generation
--------------------------------------------------------------------------------

local function generate_memory_summary(schema, arenas)
    local lines = {}
    local function emit(fmt, ...)
        table.insert(lines, string.format(fmt, ...))
    end
    
    emit("/*")
    emit(" * Memory Summary for %s", schema.name)
    emit(" * ================================")
    emit(" *")
    emit(" * Node count:     %d", #schema.nodes)
    emit(" * Bitspace count: %d", #schema.bitspaces)
    emit(" * Class count:    %d", #schema.classes)
    emit(" * Max depth:      %d", schema.options.max_depth or 16)
    emit(" *")
    emit(" * Arena Sizes (per bitspace):")
    
    local total_ram = 0
    for _, arena in ipairs(arenas) do
        local bs = schema.bitspaces[arena.bitspace_index + 1]
        local ram = arena.total_bytes_with_latch * 2  -- shadow + current
        total_ram = total_ram + ram
        
        if bs.latch then
            emit(" *   %-20s %6d bytes (×2 shadow/current, ×2 live/latch)", 
                 bs.name, ram)
        else
            emit(" *   %-20s %6d bytes (×2 shadow/current)", bs.name, ram)
        end
    end
    
    emit(" *")
    emit(" * Total RAM:  %d bytes", total_ram)
    emit(" *")
    
    -- Check constraints
    local max_ram = schema.options.max_ram
    local max_depth = schema.options.max_depth or 16
    
    if max_ram then
        local status = total_ram <= max_ram and "PASS" or "FAIL"
        emit(" * Constraint max_ram (%d): %s (%d used)", max_ram, status, total_ram)
    end
    
    local actual_depth = 0
    for _, node in ipairs(schema.nodes) do
        actual_depth = math.max(actual_depth, node.depth)
    end
    local depth_status = actual_depth <= max_depth and "PASS" or "FAIL"
    emit(" * Constraint max_depth (%d): %s (%d actual)", max_depth, depth_status, actual_depth)
    
    emit(" */")
    
    return table.concat(lines, "\n")
end

--------------------------------------------------------------------------------
-- Main
--------------------------------------------------------------------------------

local function print_usage()
    print([[
ChainTree Hierarchical Bit Map DSL Code Generator

Usage: luajit codegen.lua [options] <schema.lua>

Options:
  -o, --output <dir>      Output directory (default: current directory)
  -p, --prefix <name>     Override output file prefix (default: schema name)
  -n, --no-bin            Skip binary file generation (.bin)
  -b, --bin-only          Generate only binary file (.bin)
  -d, --debug             Include debug string tables in _hashes.h
  -c, --c-only            Generate only C headers (no .bin)
  -j, --json              Generate JSON sidecar file
  -v, --verbose           Verbose output
  -q, --quiet             Suppress output except errors
  -h, --help              Show this help message
  --no-hashes             Skip hash reference header generation
  --validate-only         Validate schema without generating files
  --dump-tree             Print tree structure to stdout
  --dump-hashes           Print all hashes to stdout
  --endian <le|be>        Binary endianness (default: le)

Output Files:
  generated_<name>.bin       Binary descriptor data
  generated_<name>.bin.h     Binary as C array
  generated_<name>.h         Main API header
  generated_<name>_hashes.h  String-to-hash reference

Examples:
  luajit codegen.lua schema.lua
  luajit codegen.lua -o build/ -v schema.lua
  luajit codegen.lua --prefix MyProject -d schema.lua
  luajit codegen.lua --validate-only schema.lua
  luajit codegen.lua --dump-hashes schema.lua
]])
end

local function parse_args(args)
    local opts = {
        output_dir = ".",
        prefix = nil,
        generate_bin = true,
        generate_headers = true,
        generate_hashes = true,
        generate_json = false,
        include_debug = false,
        verbose = false,
        quiet = false,
        validate_only = false,
        dump_tree = false,
        dump_hashes = false,
        endian = "le",
        schema_file = nil,
    }
    
    local i = 1
    while i <= #args do
        local arg = args[i]
        
        if arg == "-h" or arg == "--help" then
            print_usage()
            os.exit(0)
        elseif arg == "-o" or arg == "--output" then
            i = i + 1
            opts.output_dir = args[i]
        elseif arg == "-p" or arg == "--prefix" then
            i = i + 1
            opts.prefix = args[i]
        elseif arg == "-n" or arg == "--no-bin" then
            opts.generate_bin = false
        elseif arg == "-b" or arg == "--bin-only" then
            opts.generate_headers = false
            opts.generate_hashes = false
        elseif arg == "-c" or arg == "--c-only" then
            opts.generate_bin = false
        elseif arg == "-d" or arg == "--debug" then
            opts.include_debug = true
        elseif arg == "-j" or arg == "--json" then
            opts.generate_json = true
        elseif arg == "-v" or arg == "--verbose" then
            opts.verbose = true
        elseif arg == "-q" or arg == "--quiet" then
            opts.quiet = true
        elseif arg == "--no-hashes" then
            opts.generate_hashes = false
        elseif arg == "--validate-only" then
            opts.validate_only = true
        elseif arg == "--dump-tree" then
            opts.dump_tree = true
        elseif arg == "--dump-hashes" then
            opts.dump_hashes = true
        elseif arg == "--endian" then
            i = i + 1
            opts.endian = args[i]
            if opts.endian ~= "le" and opts.endian ~= "be" then
                print("Error: --endian must be 'le' or 'be'")
                os.exit(1)
            end
        elseif arg:sub(1,1) == "-" then
            print("Error: Unknown option: " .. arg)
            print("Use --help for usage information")
            os.exit(1)
        else
            opts.schema_file = arg
        end
        
        i = i + 1
    end
    
    if not opts.schema_file then
        print("Error: No schema file specified")
        print("Use --help for usage information")
        os.exit(1)
    end
    
    return opts
end

local function log(opts, fmt, ...)
    if not opts.quiet then
        print(string.format(fmt, ...))
    end
end

local function log_verbose(opts, fmt, ...)
    if opts.verbose and not opts.quiet then
        print(string.format("  [verbose] " .. fmt, ...))
    end
end

local function dump_tree(schema, indent)
    indent = indent or 0
    local spaces = string.rep("  ", indent)
    
    -- Find root nodes
    local function print_node(node, depth)
        local prefix = string.rep("  ", depth)
        local mask_info = ""
        for bs_name, mask in pairs(node.masks) do
            mask_info = mask_info .. string.format(" [%s:0x%08X]", bs_name, mask)
        end
        print(string.format("%s├── %s (%s) hash=0x%08X%s", 
              prefix, node.path, node.class.name, node.path_hash, mask_info))
        
        for _, child in ipairs(node.children) do
            print_node(child, depth + 1)
        end
    end
    
    print("Tree Structure:")
    for _, node in ipairs(schema.nodes) do
        if node.parent_index == -1 then
            print_node(node, 0)
        end
    end
end

local function dump_hashes(schema)
    print("All Hashes (sorted by hash value):")
    print(string.format("%-12s  %s", "Hash", "Path"))
    print(string.rep("-", 60))
    
    local sorted = {}
    for path, hash in pairs(schema.hashes) do
        table.insert(sorted, {path = path, hash = hash})
    end
    table.sort(sorted, function(a, b) return a.hash < b.hash end)
    
    for _, entry in ipairs(sorted) do
        print(string.format("0x%08X  %s", entry.hash, entry.path))
    end
    print(string.format("\nTotal: %d hashes", #sorted))
end

local function generate_json_sidecar(schema, arenas)
    local json_parts = {}
    
    local function escape_json(s)
        return s:gsub('\\', '\\\\'):gsub('"', '\\"'):gsub('\n', '\\n')
    end
    
    local function to_json(val, indent)
        indent = indent or 0
        local spaces = string.rep("  ", indent)
        local t = type(val)
        
        if t == "nil" then
            return "null"
        elseif t == "boolean" then
            return val and "true" or "false"
        elseif t == "number" then
            return tostring(val)
        elseif t == "string" then
            return '"' .. escape_json(val) .. '"'
        elseif t == "table" then
            -- Check if array
            local is_array = #val > 0
            local parts = {}
            
            if is_array then
                for _, v in ipairs(val) do
                    table.insert(parts, to_json(v, indent + 1))
                end
                return "[\n" .. spaces .. "  " .. table.concat(parts, ",\n" .. spaces .. "  ") .. "\n" .. spaces .. "]"
            else
                for k, v in pairs(val) do
                    if type(k) == "string" then
                        table.insert(parts, '"' .. k .. '": ' .. to_json(v, indent + 1))
                    end
                end
                table.sort(parts)
                return "{\n" .. spaces .. "  " .. table.concat(parts, ",\n" .. spaces .. "  ") .. "\n" .. spaces .. "}"
            end
        end
        return "null"
    end
    
    local sidecar = {
        schema_name = schema.name,
        version = schema.version,
        node_count = #schema.nodes,
        bitspace_count = #schema.bitspaces,
        class_count = #schema.classes,
        bitspaces = {},
        nodes = {},
        config = schema.config,
    }
    
    for _, bs in ipairs(schema.bitspaces) do
        table.insert(sidecar.bitspaces, {
            name = bs.name,
            merge = bs.merge,
            latch = bs.latch,
            hash = string.format("0x%08X", schema.hashes[bs.name]),
        })
    end
    
    for _, node in ipairs(schema.nodes) do
        table.insert(sidecar.nodes, {
            path = node.path,
            class = node.class.name,
            hash = string.format("0x%08X", node.path_hash),
            depth = node.depth,
        })
    end
    
    return to_json(sidecar, 0)
end

local function main(args)
    local opts = parse_args(args)
    
    -- Load schema
    log(opts, "Loading schema: %s", opts.schema_file)
    local schema_func, err = loadfile(opts.schema_file)
    if not schema_func then
        print("Error loading schema: " .. err)
        os.exit(1)
    end
    
    local raw_schema = schema_func()
    if not raw_schema or not raw_schema.name or not raw_schema.bitspaces or not raw_schema.classes or not raw_schema.nodes then
        print("Error: File does not return a valid schema (must have name, bitspaces, classes, nodes)")
        os.exit(1)
    end
    
    -- Process schema
    log(opts, "Processing schema...")
    local schema = process_schema(raw_schema)
    
    -- Override prefix if specified
    if opts.prefix then
        schema.name = opts.prefix
    end
    
    log_verbose(opts, "Schema name: %s", schema.name)
    log_verbose(opts, "Nodes: %d, Bitspaces: %d, Classes: %d", 
                #schema.nodes, #schema.bitspaces, #schema.classes)
    
    -- Calculate arenas
    log(opts, "Calculating arena sizes...")
    local arenas = calculate_arenas(schema)
    
    -- Handle special modes
    if opts.dump_tree then
        dump_tree(schema)
        return
    end
    
    if opts.dump_hashes then
        dump_hashes(schema)
        return
    end
    
    if opts.validate_only then
        print("Schema validation: PASSED")
        print(generate_memory_summary(schema, arenas))
        return
    end
    
    -- Ensure output directory exists
    os.execute("mkdir -p " .. opts.output_dir)
    
    -- Generate outputs
    local base_name = "generated_" .. schema.name
    
    if opts.generate_bin then
        log(opts, "Generating binary...")
        local binary = generate_binary(schema, arenas)
        
        local bin_path = opts.output_dir .. "/" .. base_name .. ".bin"
        local f = io.open(bin_path, "wb")
        f:write(binary)
        f:close()
        log(opts, "  Written: %s (%d bytes)", bin_path, #binary)
        
        if opts.generate_headers then
            local bin_h_path = opts.output_dir .. "/" .. base_name .. ".bin.h"
            local binary_header = generate_binary_header(schema, arenas, binary)
            local memory_summary = generate_memory_summary(schema, arenas)
            f = io.open(bin_h_path, "w")
            f:write(binary_header)
            f:write("\n\n")
            f:write(memory_summary)
            f:close()
            log(opts, "  Written: %s", bin_h_path)
        end
    end
    
    if opts.generate_hashes then
        log(opts, "Generating hash reference...")
        local hash_h_path = opts.output_dir .. "/" .. base_name .. "_hashes.h"
        local hash_header = generate_hash_header(schema)
        
        -- If debug mode, auto-define the debug strings macro
        if opts.include_debug then
            hash_header = "#define " .. string.upper(schema.name) .. "_INCLUDE_DEBUG_STRINGS\n\n" .. hash_header
        end
        
        local f = io.open(hash_h_path, "w")
        f:write(hash_header)
        f:close()
        log(opts, "  Written: %s", hash_h_path)
    end
    
    if opts.generate_json then
        local json_path = opts.output_dir .. "/" .. base_name .. ".json"
        local json_content = generate_json_sidecar(schema, arenas)
        local f = io.open(json_path, "w")
        f:write(json_content)
        f:close()
        log(opts, "  Written: %s", json_path)
    end
    
    log(opts, "")
    log(opts, "Generation complete!")
    if not opts.quiet then
        print(generate_memory_summary(schema, arenas))
    end
end

main(arg)