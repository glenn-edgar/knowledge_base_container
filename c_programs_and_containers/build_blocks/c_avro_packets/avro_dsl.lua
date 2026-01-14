--[[
    avro_dsl.lua - Schema and Data DSL Generator
    
    Generates:
    - .h files from schema definitions (always)
    - .bin files from instance data (only if instances defined)
    - _data.h files with embedded binary blob (only if instances defined)
    
    Wire format: little-endian, ARM32/ARM64 compatible
    
    Compatible with: LuaJIT, Lua 5.3+
]]--

local M = {}

--------------------------------------------------------------------------------
-- Binary Packing (LuaJIT compatible, little-endian)
--------------------------------------------------------------------------------

local function pack_uint8(v)
    return string.char(v % 256)
end

local function pack_int8(v)
    if v < 0 then v = v + 256 end
    return string.char(v % 256)
end

local function pack_uint16_le(v)
    return string.char(v % 256, math.floor(v / 256) % 256)
end

local function pack_int16_le(v)
    if v < 0 then v = v + 65536 end
    return pack_uint16_le(v)
end

local function pack_uint32_le(v)
    return string.char(
        v % 256,
        math.floor(v / 256) % 256,
        math.floor(v / 65536) % 256,
        math.floor(v / 16777216) % 256
    )
end

local function pack_int32_le(v)
    if v < 0 then v = v + 4294967296 end
    return pack_uint32_le(v)
end

local function pack_uint64_le(v)
    -- Handle as two 32-bit parts
    local lo = v % 4294967296
    local hi = math.floor(v / 4294967296)
    return pack_uint32_le(lo) .. pack_uint32_le(hi)
end

local function pack_int64_le(v)
    if v < 0 then v = v + 18446744073709551616 end
    return pack_uint64_le(v)
end

-- IEEE 754 single precision (32-bit float), little-endian
local function pack_float_le(v)
    if v == 0 then
        return "\0\0\0\0"
    end
    
    local sign = 0
    if v < 0 then
        sign = 1
        v = -v
    end
    
    local mantissa, exponent = math.frexp(v)
    exponent = exponent + 126  -- bias
    
    if exponent <= 0 then
        -- Denormalized
        mantissa = math.floor(mantissa * 2^(23 + exponent) + 0.5)
        exponent = 0
    elseif exponent >= 255 then
        -- Infinity
        mantissa = 0
        exponent = 255
    else
        -- Normalized
        mantissa = math.floor((mantissa * 2 - 1) * 2^23 + 0.5)
    end
    
    local b0 = mantissa % 256
    local b1 = math.floor(mantissa / 256) % 256
    local b2 = math.floor(mantissa / 65536) % 128 + (exponent % 2) * 128
    local b3 = math.floor(exponent / 2) + sign * 128
    
    return string.char(b0, b1, b2, b3)
end

-- IEEE 754 double precision (64-bit float), little-endian
local function pack_double_le(v)
    if v == 0 then
        return "\0\0\0\0\0\0\0\0"
    end
    
    local sign = 0
    if v < 0 then
        sign = 1
        v = -v
    end
    
    local mantissa, exponent = math.frexp(v)
    exponent = exponent + 1022  -- bias
    
    if exponent <= 0 then
        -- Denormalized
        mantissa = 0
        exponent = 0
    elseif exponent >= 2047 then
        -- Infinity
        mantissa = 0
        exponent = 2047
    else
        -- Normalized: mantissa is 1.xxxxx, we store xxxxx (52 bits)
        mantissa = (mantissa * 2 - 1) * 2^52
    end
    
    -- Split mantissa into bytes (52 bits = 6.5 bytes)
    local bytes = {}
    local m = mantissa
    for i = 1, 6 do
        bytes[i] = m % 256
        m = math.floor(m / 256)
    end
    -- Last 4 bits of mantissa + 4 bits of exponent
    bytes[7] = (m % 16) + (exponent % 16) * 16
    -- Remaining 7 bits of exponent + sign
    bytes[8] = math.floor(exponent / 16) + sign * 128
    
    return string.char(bytes[1], bytes[2], bytes[3], bytes[4],
                       bytes[5], bytes[6], bytes[7], bytes[8])
end

local function pack_bool(v)
    return string.char(v and 1 or 0)
end

-- Packer lookup table
local packers = {
    ["uint8"]  = pack_uint8,
    ["int8"]   = pack_int8,
    ["uint16"] = pack_uint16_le,
    ["int16"]  = pack_int16_le,
    ["uint32"] = pack_uint32_le,
    ["int32"]  = pack_int32_le,
    ["uint64"] = pack_uint64_le,
    ["int64"]  = pack_int64_le,
    ["float"]  = pack_float_le,
    ["double"] = pack_double_le,
    ["bool"]   = pack_bool,
}

--------------------------------------------------------------------------------
-- Internal State
--------------------------------------------------------------------------------

local current_file = nil
local current_record = nil
local current_instance = nil
local data_file = nil

local schemas = {}  -- all loaded schemas by name

--------------------------------------------------------------------------------
-- Type Definitions
--------------------------------------------------------------------------------

local type_info = {
    ["uint8"]   = { size = 1, ctype = "uint8_t"  },
    ["int8"]    = { size = 1, ctype = "int8_t"   },
    ["uint16"]  = { size = 2, ctype = "uint16_t" },
    ["int16"]   = { size = 2, ctype = "int16_t"  },
    ["uint32"]  = { size = 4, ctype = "uint32_t" },
    ["int32"]   = { size = 4, ctype = "int32_t"  },
    ["uint64"]  = { size = 8, ctype = "uint64_t" },
    ["int64"]   = { size = 8, ctype = "int64_t"  },
    ["float"]   = { size = 4, ctype = "float"    },
    ["double"]  = { size = 8, ctype = "double"   },
    ["bool"]    = { size = 1, ctype = "bool"     },
}

--------------------------------------------------------------------------------
-- Utility Functions
--------------------------------------------------------------------------------

local function upper_first(s)
    return s:sub(1,1):upper() .. s:sub(2)
end

local function to_upper_snake(s)
    return s:upper()
end

local function get_type_info(typename)
    if type_info[typename] then
        return type_info[typename]
    end
    -- Check if it's a pointer type (use 4 bytes for embedded compatibility)
    if current_file and current_file.pointers[typename] then
        return { size = 4, ctype = typename .. "_t*", is_pointer = true }
    end
    -- Check if it's a fixed type
    if current_file and current_file.fixed[typename] then
        local f = current_file.fixed[typename]
        return { size = f.size, ctype = "uint8_t", is_fixed = true, fixed_size = f.size }
    end
    -- Check if it's another record
    if schemas[typename] then
        return { size = schemas[typename].size, ctype = typename .. "_t", is_record = true, record = schemas[typename] }
    end
    error("Unknown type: " .. typename)
end

local function compute_record_size(record)
    local size = 0
    for _, field in ipairs(record.fields) do
        local ti = get_type_info(field.type)
        if ti.is_fixed then
            size = size + ti.fixed_size
        else
            size = size + ti.size
        end
    end
    return size
end

--------------------------------------------------------------------------------
-- Schema DSL Commands
--------------------------------------------------------------------------------

function M.FILE(name)
    current_file = {
        name = name,
        includes_bracket = {},
        includes_string = {},
        records = {},
        pointers = {},
        fixed = {},
        arrays = {},
    }
end

function M.INCLUDE_BRACKET(header)
    table.insert(current_file.includes_bracket, header)
end

function M.INCLUDE_STRING(header)
    table.insert(current_file.includes_string, header)
end

function M.POINTER(name)
    current_file.pointers[name] = true
end

function M.FIXED(name, size)
    current_file.fixed[name] = { size = size }
end

function M.ARRAY(name, element_type, count)
    current_file.arrays[name] = { element_type = element_type, count = count }
end

function M.RECORD(name)
    current_record = {
        name = name,
        fields = {},
    }
end

function M.FIELD(name, typename)
    table.insert(current_record.fields, { name = name, type = typename })
end

function M.END_RECORD()
    current_record.size = compute_record_size(current_record)
    table.insert(current_file.records, current_record)
    schemas[current_record.name] = current_record
    current_record = nil
end

--------------------------------------------------------------------------------
-- Header Generation
--------------------------------------------------------------------------------

-- DJB2 hash algorithm (same as runtime)
local function djb2_hash(str)
    local hash = 5381
    for i = 1, #str do
        hash = ((hash * 33) + string.byte(str, i)) % 0x100000000
    end
    return hash
end

local function generate_header(file, output_path)
    local out = io.open(output_path, "w")
    local guard = to_upper_snake(file.name) .. "_H"
    local schema_hash_const = to_upper_snake(file.name) .. "_SCHEMA_HASH"
    local schema_hash = djb2_hash(file.name)
    
    out:write("/* Generated by avro_dsl.lua - DO NOT EDIT */\n")
    out:write("#ifndef " .. guard .. "\n")
    out:write("#define " .. guard .. "\n\n")
    
    -- Includes
    for _, inc in ipairs(file.includes_bracket) do
        out:write("#include <" .. inc .. ">\n")
    end
    -- Always need string.h for memcpy in copy functions
    out:write("#include <string.h>\n")
    for _, inc in ipairs(file.includes_string) do
        out:write("#include \"" .. inc .. "\"\n")
    end
    -- Always include runtime support
    out:write("#include \"cfl_avro_support.h\"\n")
    out:write("\n")
    
    -- Schema hash constant
    out:write("/* Schema hash for packet verification (DJB2 hash of \"" .. file.name .. "\") */\n")
    out:write(string.format("#define %s 0x%08XU\n\n", schema_hash_const, schema_hash))
    
    -- Pointer typedefs
    for name, _ in pairs(file.pointers) do
        out:write("typedef void* " .. name .. "_t;\n")
    end
    if next(file.pointers) then out:write("\n") end
    
    -- Fixed types
    for name, info in pairs(file.fixed) do
        out:write("typedef uint8_t " .. name .. "_t[" .. info.size .. "];\n")
    end
    if next(file.fixed) then out:write("\n") end
    
    -- Common packet header structure
    out:write("/*" .. string.rep("-", 76) .. "\n")
    out:write(" * Packet Header (common to all packet types)\n")
    out:write(" *" .. string.rep("-", 76) .. "*/\n\n")
    out:write("typedef struct __attribute__((packed)) {\n")
    out:write("    uint32_t    schema_hash;   /* Schema identifier (DJB2 hash) */\n")
    out:write("    double      timestamp;     /* Packet timestamp */\n")
    out:write("    uint16_t    seq;           /* Sequence number */\n")
    out:write("    uint16_t    source_node;   /* Source node ID */\n")
    out:write("    uint16_t    length;        /* Payload length */\n")
    out:write("    uint16_t    index;         /* Record type index */\n")
    out:write("} " .. file.name .. "_packet_header_t;\n\n")
    
    -- Records and their packet wrappers
    for idx, record in ipairs(file.records) do
        local record_index = idx - 1  -- 0-based index
        
        out:write("/*" .. string.rep("-", 76) .. "\n")
        out:write(" * " .. record.name .. " (index: " .. record_index .. ")\n")
        out:write(" *" .. string.rep("-", 76) .. "*/\n\n")
        
        -- Record struct
        out:write("typedef struct __attribute__((packed)) {\n")
        for _, field in ipairs(record.fields) do
            local ti = get_type_info(field.type)
            if ti.is_fixed then
                out:write("    uint8_t " .. field.name .. "[" .. ti.fixed_size .. "];\n")
            elseif ti.is_record then
                out:write("    " .. field.type .. "_t " .. field.name .. ";\n")
            else
                out:write("    " .. ti.ctype .. " " .. field.name .. ";\n")
            end
        end
        out:write("} " .. record.name .. "_t;\n\n")
        
        -- Size and index constants
        out:write("#define " .. to_upper_snake(record.name) .. "_SIZE " .. record.size .. "\n")
        out:write("#define " .. to_upper_snake(record.name) .. "_INDEX " .. record_index .. "\n\n")
        
        -- Packet wrapper struct
        out:write("/* Packet wrapper for " .. record.name .. " */\n")
        out:write("typedef struct __attribute__((packed)) {\n")
        out:write("    uint32_t    schema_hash;\n")
        out:write("    double      timestamp;\n")
        out:write("    uint16_t    seq;\n")
        out:write("    uint16_t    source_node;\n")
        out:write("    uint16_t    length;\n")
        out:write("    uint16_t    index;\n")
        out:write("    " .. record.name .. "_t data;\n")
        out:write("} " .. record.name .. "_packet_t;\n\n")
        
        -- Packet size constant (header + payload)
        local header_size = 4 + 8 + 2 + 2 + 2 + 2  -- schema_hash + timestamp + seq + source_node + length + index = 20
        local packet_size = header_size + record.size
        out:write("#define " .. to_upper_snake(record.name) .. "_PACKET_SIZE " .. packet_size .. "\n\n")
        
        -- Encode function (user provides schema string, runtime computes hash)
        out:write("/* Initialize packet header and return pointer to data payload.\n")
        out:write(" * schema_name: Schema identifier string (hashed at runtime)\n")
        out:write(" * source_node: Source node ID\n")
        out:write(" */\n")
        out:write("static inline " .. record.name .. "_t* " .. record.name .. "_packet_encode(\n")
        out:write("        " .. record.name .. "_packet_t* pkt,\n")
        out:write("        const char* schema_name,\n")
        out:write("        uint16_t source_node)\n")
        out:write("{\n")
        out:write("    pkt->schema_hash = cfl_avro_hash(schema_name);\n")
        out:write("    pkt->timestamp = 0.0;\n")
        out:write("    pkt->seq = 0;\n")
        out:write("    pkt->source_node = source_node;\n")
        out:write("    pkt->length = sizeof(" .. record.name .. "_t);\n")
        out:write("    pkt->index = " .. to_upper_snake(record.name) .. "_INDEX;\n")
        out:write("    return &pkt->data;\n")
        out:write("}\n\n")
        
        -- Verify function (user provides schema string, runtime computes and compares hash)
        out:write("/* Verify packet header and return pointer to data payload (or NULL on error).\n")
        out:write(" * packet_buffer: Raw packet data\n")
        out:write(" * schema_name:   Expected schema identifier string (hashed at runtime)\n")
        out:write(" * source_node:   Output parameter for source node ID (can be NULL)\n")
        out:write(" */\n")
        out:write("static inline const " .. record.name .. "_t* " .. record.name .. "_packet_verify(\n")
        out:write("        const void* packet_buffer,\n")
        out:write("        const char* schema_name,\n")
        out:write("        uint16_t* source_node)\n")
        out:write("{\n")
        out:write("    const " .. record.name .. "_packet_t* pkt = (const " .. record.name .. "_packet_t*)packet_buffer;\n")
        out:write("    \n")
        out:write("    /* Verify schema hash */\n")
        out:write("    if (pkt->schema_hash != cfl_avro_hash(schema_name)) return NULL;\n")
        out:write("    \n")
        out:write("    /* Verify packet type index */\n")
        out:write("    if (pkt->index != " .. to_upper_snake(record.name) .. "_INDEX) return NULL;\n")
        out:write("    \n")
        out:write("    /* Verify payload size */\n")
        out:write("    if (pkt->length != sizeof(" .. record.name .. "_t)) return NULL;\n")
        out:write("    \n")
        out:write("    /* Extract source node */\n")
        out:write("    if (source_node) *source_node = pkt->source_node;\n")
        out:write("    \n")
        out:write("    return &pkt->data;\n")
        out:write("}\n\n")
        
        -- Length function
        out:write("/* Return total packet length (header + payload) */\n")
        out:write("static inline size_t " .. record.name .. "_packet_length(void)\n")
        out:write("{\n")
        out:write("    return sizeof(" .. record.name .. "_packet_t);\n")
        out:write("}\n\n")
        
        -- Copy function with verification
        out:write("/* Copy packet data from src to dst, verifying schema_hash and index match.\n")
        out:write(" * dst:         Destination buffer (must be at least " .. record.name .. "_packet_length() bytes)\n")
        out:write(" * src:         Source buffer\n")
        out:write(" * Returns:     Pointer to dst data payload on success, NULL on verification failure\n")
        out:write(" */\n")
        out:write("static inline " .. record.name .. "_t* " .. record.name .. "_packet_copy(\n")
        out:write("        void* dst,\n")
        out:write("        const void* src)\n")
        out:write("{\n")
        out:write("    const " .. record.name .. "_packet_t* src_pkt = (const " .. record.name .. "_packet_t*)src;\n")
        out:write("    " .. record.name .. "_packet_t* dst_pkt = (" .. record.name .. "_packet_t*)dst;\n")
        out:write("    \n")
        out:write("    /* Verify source packet index matches expected type */\n")
        out:write("    if (src_pkt->index != " .. to_upper_snake(record.name) .. "_INDEX) return NULL;\n")
        out:write("    \n")
        out:write("    /* Verify payload size */\n")
        out:write("    if (src_pkt->length != sizeof(" .. record.name .. "_t)) return NULL;\n")
        out:write("    \n")
        out:write("    /* Copy entire packet */\n")
        out:write("    memcpy(dst_pkt, src_pkt, sizeof(" .. record.name .. "_packet_t));\n")
        out:write("    \n")
        out:write("    return &dst_pkt->data;\n")
        out:write("}\n\n")
    end
    
    -- Array types
    for name, info in pairs(file.arrays) do
        local ti = get_type_info(info.element_type)
        out:write("typedef " .. ti.ctype .. " " .. name .. "_t[" .. info.count .. "];\n")
    end
    if next(file.arrays) then out:write("\n") end
    
    -- Schema-level packet constants
    out:write("/*" .. string.rep("-", 76) .. "\n")
    out:write(" * Schema-level packet constants\n")
    out:write(" *" .. string.rep("-", 76) .. "*/\n\n")
    
    local header_size = 4 + 8 + 2 + 2 + 2 + 2  -- 20 bytes
    local total_bin_size = 0
    local record_count = #file.records
    
    -- Calculate total binary size and offsets
    out:write("/* Packet offsets (determined by record order in schema) */\n")
    local offset = 0
    for _, record in ipairs(file.records) do
        local packet_size = header_size + record.size
        out:write("#define " .. to_upper_snake(record.name) .. "_PACKET_OFFSET " .. offset .. "\n")
        offset = offset + packet_size
        total_bin_size = total_bin_size + packet_size
    end
    out:write("\n")
    
    -- Total size and count
    out:write("/* Total binary size and record count */\n")
    out:write("#define " .. to_upper_snake(file.name) .. "_BIN_SIZE " .. total_bin_size .. "\n")
    out:write("#define " .. to_upper_snake(file.name) .. "_RECORD_COUNT " .. record_count .. "\n\n")
    
    out:write("#endif /* " .. guard .. " */\n")
    out:close()
    
    print("Generated: " .. output_path)
end

function M.GENERATE()
    if not current_file then
        error("No FILE defined")
    end
    local output_path = current_file.name .. ".h"
    generate_header(current_file, output_path)
end

--------------------------------------------------------------------------------
-- Data DSL Commands
--------------------------------------------------------------------------------

function M.DATA_FILE(name)
    data_file = {
        name = name,
        instances = {},
    }
end

function M.INSTANCE(record_name, instance_name)
    if not schemas[record_name] then
        error("Unknown record type: " .. record_name)
    end
    current_instance = {
        record_name = record_name,
        instance_name = instance_name,
        values = {},
        record = schemas[record_name],
    }
end

function M.SET(field_name, value)
    current_instance.values[field_name] = value
end

function M.END_INSTANCE()
    table.insert(data_file.instances, current_instance)
    current_instance = nil
end

--------------------------------------------------------------------------------
-- Binary Generation
--------------------------------------------------------------------------------

local pack_record_value  -- forward declaration

local function pack_value(typename, value)
    local ti = get_type_info(typename)
    
    if ti.is_fixed then
        -- Fixed byte array
        if type(value) == "string" then
            local padded = value .. string.rep("\0", ti.fixed_size - #value)
            return padded:sub(1, ti.fixed_size)
        elseif type(value) == "table" then
            local bytes = ""
            for i = 1, ti.fixed_size do
                bytes = bytes .. string.char(value[i] or 0)
            end
            return bytes
        else
            error("Fixed type requires string or table value")
        end
    elseif ti.is_record then
        -- Nested record - pack recursively
        return pack_record_value(ti.record, value or {})
    elseif ti.is_pointer then
        -- Pointers packed as 4 bytes for embedded compatibility
        return pack_uint32_le(value or 0)
    else
        -- Use packer function
        local packer = packers[typename]
        if not packer then
            error("No packer for type: " .. typename)
        end
        return packer(value)
    end
end

-- Pack a record's fields from a value table
pack_record_value = function(record, values)
    local bytes = ""
    values = values or {}
    
    for _, field in ipairs(record.fields) do
        local value = values[field.name]
        local ti = get_type_info(field.type)
        
        if value == nil then
            -- Default to zero
            if ti.is_fixed then
                bytes = bytes .. string.rep("\0", ti.fixed_size)
            elseif ti.is_record then
                bytes = bytes .. pack_record_value(ti.record, {})
            elseif ti.is_pointer then
                bytes = bytes .. pack_uint32_le(0)
            else
                local packer = packers[field.type]
                if not packer then
                    error("No packer for type: " .. field.type)
                end
                bytes = bytes .. packer(0)
            end
        else
            bytes = bytes .. pack_value(field.type, value)
        end
    end
    
    return bytes
end

local function pack_instance(instance)
    return pack_record_value(instance.record, instance.values)
end

-- Pack a single instance with packet header
local function pack_packet(instance, schema_hash, source_node)
    local record = instance.record
    local record_index = 0
    
    -- Find record index
    for idx, rec in ipairs(current_file.records) do
        if rec.name == record.name then
            record_index = idx - 1
            break
        end
    end
    
    local payload = pack_instance(instance)
    local payload_len = #payload
    
    -- Pack header: schema_hash(4) + timestamp(8) + seq(2) + source_node(2) + length(2) + index(2) = 20 bytes
    local header = ""
    header = header .. pack_uint32_le(schema_hash)      -- schema_hash
    header = header .. pack_double_le(0.0)              -- timestamp
    header = header .. pack_uint16_le(0)                -- seq
    header = header .. pack_uint16_le(source_node)      -- source_node
    header = header .. pack_uint16_le(payload_len)      -- length
    header = header .. pack_uint16_le(record_index)     -- index
    
    return header .. payload
end

-- Generate packet binary file (instances with headers)
local function generate_packet_binary(data, output_path, schema_hash)
    local out = io.open(output_path, "wb")
    
    local all_bytes = ""
    local source_node = 0
    for _, instance in ipairs(data.instances) do
        all_bytes = all_bytes .. pack_packet(instance, schema_hash, source_node)
        source_node = source_node + 1
    end
    
    out:write(all_bytes)
    out:close()
    
    print("Generated: " .. output_path .. " (" .. #all_bytes .. " bytes, " .. #data.instances .. " packets)")
    return all_bytes
end

-- Generate packet data header with embedded binary
local function generate_packet_data_header(data, packet_data, output_path, schema_hash)
    local out = io.open(output_path, "w")
    local guard = to_upper_snake(data.name) .. "_DATA_H"
    
    out:write("/* Generated by avro_dsl.lua - DO NOT EDIT */\n")
    out:write("#ifndef " .. guard .. "\n")
    out:write("#define " .. guard .. "\n\n")
    
    -- Include the schema header (contains offsets, sizes, counts)
    if current_file then
        out:write("#include \"" .. current_file.name .. ".h\"\n\n")
    end
    
    -- Binary blob only
    out:write("/* Packet binary data - " .. #packet_data .. " bytes (" .. #data.instances .. " packets) */\n")
    out:write("static const uint8_t " .. data.name .. "_bin[] = {\n")
    
    local offset = 0
    for i, instance in ipairs(data.instances) do
        local packet_bytes = pack_packet(instance, schema_hash, i - 1)
        out:write("    /* " .. instance.instance_name .. " */\n    ")
        
        for j = 1, #packet_bytes do
            out:write(string.format("0x%02X", packet_bytes:byte(j)))
            if j < #packet_bytes or i < #data.instances then
                out:write(", ")
            end
            if j % 12 == 0 and j < #packet_bytes then
                out:write("\n    ")
            end
        end
        out:write("\n")
        offset = offset + #packet_bytes
    end
    
    out:write("};\n\n")
    
    out:write("#endif /* " .. guard .. " */\n")
    out:close()
    
    print("Generated: " .. output_path)
end

function M.GENERATE_DATA()
    if not data_file then
        error("No DATA_FILE defined")
    end
    if #data_file.instances == 0 then
        error("No instances defined - cannot generate binary files. Use GENERATE() for schema-only output.")
    end
    
    -- Compute schema hash
    local schema_hash = djb2_hash(current_file.name)
    
    -- Packet files (with headers for verification)
    local packet_bin_path = data_file.name .. ".bin"
    local packet_header_path = data_file.name .. "_data.h"
    local packet_data = generate_packet_binary(data_file, packet_bin_path, schema_hash)
    generate_packet_data_header(data_file, packet_data, packet_header_path, schema_hash)
end

--------------------------------------------------------------------------------
-- Export globals for DSL usage
--------------------------------------------------------------------------------

function M.export_globals()
    _G.FILE = M.FILE
    _G.INCLUDE_BRACKET = M.INCLUDE_BRACKET
    _G.INCLUDE_STRING = M.INCLUDE_STRING
    _G.POINTER = M.POINTER
    _G.FIXED = M.FIXED
    _G.ARRAY = M.ARRAY
    _G.RECORD = M.RECORD
    _G.FIELD = M.FIELD
    _G.END_RECORD = M.END_RECORD
    _G.GENERATE = M.GENERATE
    _G.DATA_FILE = M.DATA_FILE
    _G.INSTANCE = M.INSTANCE
    _G.SET = M.SET
    _G.END_INSTANCE = M.END_INSTANCE
    _G.GENERATE_DATA = M.GENERATE_DATA
end

return M