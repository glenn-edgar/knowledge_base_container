#!/usr/bin/env luajit
-- avro_dsl.lua
-- LuaJIT DSL for generating C header files and binary schema from Avro-like definitions
-- Outputs: .h (types + wire packets), _bin.h (embedded binary), .bin (loadable binary)

-- Bit operations - compatible with LuaJIT and Lua 5.3+
-- LuaJIT has the 'bit' library; Lua 5.3+ has native operators
-- We must avoid Lua 5.3 syntax (~ & >> <<) which causes parse errors in LuaJIT
local bxor, band, rshift, lshift, tobit

local ok, bit = pcall(require, "bit")
if ok then
    -- LuaJIT
    bxor, band, rshift, lshift, tobit = bit.bxor, bit.band, bit.rshift, bit.lshift, bit.tobit
else
    -- Lua 5.3+ - use load() to avoid parse errors in LuaJIT
    bxor   = load("return function(a,b) return a ~ b end")()
    band   = load("return function(a,b) return a & b end")()
    rshift = load("return function(a,n) return a >> n end")()
    lshift = load("return function(a,n) return a << n end")()
    tobit  = load("return function(a) return a & 0xFFFFFFFF end")()
end

local M = {}

-- Current state
local current_file = nil
local current_container = nil

--------------------------------------------------------------------------------
-- FNV-1a 32-BIT HASH (matches s_engine C implementation)
--------------------------------------------------------------------------------

local FNV_PRIME_32  = 0x01000193
local FNV_OFFSET_32 = 0x811C9DC5

-- 32-bit multiply with proper wrapping
local function mul32(a, b)
    local a_lo = band(a, 0xFFFF)
    local a_hi = band(rshift(a, 16), 0xFFFF)
    local b_lo = band(b, 0xFFFF)
    local b_hi = band(rshift(b, 16), 0xFFFF)
    
    local lo = a_lo * b_lo
    local mid = a_hi * b_lo + a_lo * b_hi
    
    return tobit(lo + lshift(mid, 16))
end

local function fnv1a_32(str)
    local hash = FNV_OFFSET_32
    for i = 1, #str do
        hash = bxor(hash, str:byte(i))
        hash = mul32(hash, FNV_PRIME_32)
    end
    -- Return as unsigned
    if hash < 0 then
        hash = hash + 0x100000000
    end
    return hash
end

M.fnv1a_32 = fnv1a_32  -- Export for testing

--------------------------------------------------------------------------------
-- TYPE DEFINITIONS
--------------------------------------------------------------------------------

local type_sizes = {
    int8    = 1,  uint8   = 1,
    int16   = 2,  uint16  = 2,
    int32   = 4,  uint32  = 4,
    int64   = 8,  uint64  = 8,
    float   = 4,  double  = 8,
    bool    = 1,
}

local type_cnames = {
    int8    = "int8_t",   uint8   = "uint8_t",
    int16   = "int16_t",  uint16  = "uint16_t",
    int32   = "int32_t",  uint32  = "uint32_t",
    int64   = "int64_t",  uint64  = "uint64_t",
    float   = "float",    double  = "double",
    bool    = "bool",
}

-- Type tags for binary encoding
local type_tags = {
    int8    = 1,  uint8   = 2,
    int16   = 3,  uint16  = 4,
    int32   = 5,  uint32  = 6,
    int64   = 7,  uint64  = 8,
    float   = 9,  double  = 10,
    bool    = 11,
    enum    = 20,
    fixed   = 21,
    string  = 22,
    pointer = 23,
    struct  = 30,
    record  = 31,
}

--------------------------------------------------------------------------------
-- DSL COMMANDS
--------------------------------------------------------------------------------

function M.FILE(name)
    current_file = {
        name = name,
        includes_bracket = {},
        includes_string = {},
        enums = {},
        fixed = {},
        strings = {},
        pointers = {},
        structs = {},
        records = {},
    }
end

function M.INCLUDE_BRACKET(header)
    table.insert(current_file.includes_bracket, header)
end

function M.INCLUDE_STRING(header)
    table.insert(current_file.includes_string, header)
end

function M.ENUM(name)
    current_container = {
        kind = "enum",
        name = name,
        values = {},
    }
end

function M.VALUE(name, val)
    table.insert(current_container.values, { name = name, val = val })
end

function M.END_ENUM()
    table.insert(current_file.enums, current_container)
    current_container = nil
end

function M.FIXED(name, size)
    table.insert(current_file.fixed, { name = name, size = size })
end

function M.STRING(name, length)
    table.insert(current_file.strings, { name = name, length = length })
end

function M.POINTER(name)
    table.insert(current_file.pointers, { name = name })
end

function M.STRUCT(name)
    current_container = {
        kind = "struct",
        name = name,
        fields = {},
    }
end

function M.RECORD(name)
    current_container = {
        kind = "record",
        name = name,
        index = #current_file.records,
        fields = {},
    }
end

function M.FIELD(name, ftype, array_size)
    table.insert(current_container.fields, {
        name = name,
        type = ftype,
        array_size = array_size,
    })
end

function M.END_STRUCT()
    table.insert(current_file.structs, current_container)
    current_container = nil
end

function M.END_RECORD()
    table.insert(current_file.records, current_container)
    current_container = nil
end

--------------------------------------------------------------------------------
-- BINARY FORMAT HELPERS
--------------------------------------------------------------------------------

-- Binary schema magic and version
local SCHEMA_MAGIC   = 0x41565244  -- "AVRD" in little-endian
local SCHEMA_VERSION = 1

-- Pack little-endian integers
local function pack_u8(val)
    return string.char(band(val, 0xFF))
end

local function pack_u16(val)
    return string.char(
        band(val, 0xFF),
        band(rshift(val, 8), 0xFF)
    )
end

local function pack_u32(val)
    return string.char(
        band(val, 0xFF),
        band(rshift(val, 8), 0xFF),
        band(rshift(val, 16), 0xFF),
        band(rshift(val, 24), 0xFF)
    )
end

-- Null-terminated string
local function pack_string(str)
    return str .. "\0"
end

-- Resolve type tag for binary encoding
local function resolve_type_tag(ftype)
    if type_tags[ftype] then
        return type_tags[ftype]
    end
    for _, e in ipairs(current_file.enums) do
        if e.name == ftype then return type_tags.enum end
    end
    for _, f in ipairs(current_file.fixed) do
        if f.name == ftype then return type_tags.fixed end
    end
    for _, s in ipairs(current_file.strings) do
        if s.name == ftype then return type_tags.string end
    end
    for _, p in ipairs(current_file.pointers) do
        if p.name == ftype then return type_tags.pointer end
    end
    for _, st in ipairs(current_file.structs) do
        if st.name == ftype then return type_tags.struct end
    end
    for _, r in ipairs(current_file.records) do
        if r.name == ftype then return type_tags.record end
    end
    return 0  -- Unknown
end

-- Resolve field size
local function resolve_field_size(ftype)
    if type_sizes[ftype] then
        return type_sizes[ftype]
    end
    for _, e in ipairs(current_file.enums) do
        if e.name == ftype then return 4 end  -- Enums are int
    end
    for _, f in ipairs(current_file.fixed) do
        if f.name == ftype then return f.size end
    end
    for _, s in ipairs(current_file.strings) do
        if s.name == ftype then return s.length + 4 end  -- buffer + length + max_length
    end
    for _, p in ipairs(current_file.pointers) do
        if p.name == ftype then return 8 end  -- void* on 64-bit
    end
    -- For structs/records, would need recursive calculation
    return 0
end

-- Compute struct/record size (simplified - assumes packed)
local function compute_container_size(container)
    local size = 0
    for _, f in ipairs(container.fields) do
        local fsize = resolve_field_size(f.type)
        local count = f.array_size or 1
        size = size + fsize * count
    end
    return size
end

--------------------------------------------------------------------------------
-- BINARY GENERATION
--------------------------------------------------------------------------------

function M.GENERATE_BINARY(output_path)
    output_path = output_path or (current_file.name .. ".bin")
    
    local chunks = {}
    local header_name = current_file.name .. ".h"
    local schema_hash = fnv1a_32(header_name)
    
    -- Header: magic(4) + version(2) + record_count(2) + schema_hash(4) + total_size(4)
    table.insert(chunks, pack_u32(SCHEMA_MAGIC))
    table.insert(chunks, pack_u16(SCHEMA_VERSION))
    table.insert(chunks, pack_u16(#current_file.records))
    table.insert(chunks, pack_u32(schema_hash))
    -- Placeholder for total_size - will patch
    local size_placeholder_pos = #chunks + 1
    table.insert(chunks, pack_u32(0))
    
    -- Schema name (null-terminated)
    table.insert(chunks, pack_string(current_file.name))
    
    -- Enums
    table.insert(chunks, pack_u16(#current_file.enums))
    for _, e in ipairs(current_file.enums) do
        table.insert(chunks, pack_string(e.name))
        table.insert(chunks, pack_u32(fnv1a_32(e.name)))
        table.insert(chunks, pack_u8(#e.values))
        for _, v in ipairs(e.values) do
            table.insert(chunks, pack_string(v.name))
            table.insert(chunks, pack_u32(v.val))
        end
    end
    
    -- Fixed arrays
    table.insert(chunks, pack_u16(#current_file.fixed))
    for _, f in ipairs(current_file.fixed) do
        table.insert(chunks, pack_string(f.name))
        table.insert(chunks, pack_u32(fnv1a_32(f.name)))
        table.insert(chunks, pack_u16(f.size))
    end
    
    -- Strings
    table.insert(chunks, pack_u16(#current_file.strings))
    for _, s in ipairs(current_file.strings) do
        table.insert(chunks, pack_string(s.name))
        table.insert(chunks, pack_u32(fnv1a_32(s.name)))
        table.insert(chunks, pack_u16(s.length))
    end
    
    -- Pointers
    table.insert(chunks, pack_u16(#current_file.pointers))
    for _, p in ipairs(current_file.pointers) do
        table.insert(chunks, pack_string(p.name))
        table.insert(chunks, pack_u32(fnv1a_32(p.name)))
    end
    
    -- Structs
    table.insert(chunks, pack_u16(#current_file.structs))
    for _, st in ipairs(current_file.structs) do
        table.insert(chunks, pack_string(st.name))
        table.insert(chunks, pack_u32(fnv1a_32(st.name)))
        table.insert(chunks, pack_u16(compute_container_size(st)))
        table.insert(chunks, pack_u8(#st.fields))
        
        local offset = 0
        for _, f in ipairs(st.fields) do
            table.insert(chunks, pack_string(f.name))
            table.insert(chunks, pack_u8(resolve_type_tag(f.type)))
            table.insert(chunks, pack_u16(offset))
            local fsize = resolve_field_size(f.type)
            table.insert(chunks, pack_u16(fsize))
            table.insert(chunks, pack_u16(f.array_size or 0))
            offset = offset + fsize * math.max(1, f.array_size or 1)
        end
    end
    
    -- Records
    table.insert(chunks, pack_u16(#current_file.records))
    for i, r in ipairs(current_file.records) do
        table.insert(chunks, pack_string(r.name))
        table.insert(chunks, pack_u32(fnv1a_32(r.name)))
        table.insert(chunks, pack_u8(i - 1))  -- 0-based index
        table.insert(chunks, pack_u16(compute_container_size(r)))
        table.insert(chunks, pack_u8(#r.fields))
        
        local offset = 0
        for _, f in ipairs(r.fields) do
            table.insert(chunks, pack_string(f.name))
            table.insert(chunks, pack_u8(resolve_type_tag(f.type)))
            table.insert(chunks, pack_u16(offset))
            local fsize = resolve_field_size(f.type)
            table.insert(chunks, pack_u16(fsize))
            table.insert(chunks, pack_u16(f.array_size or 0))
            offset = offset + fsize * math.max(1, f.array_size or 1)
        end
    end
    
    -- Concatenate and patch total size
    local blob = table.concat(chunks)
    local total_size = #blob
    
    -- Patch total_size at offset 12 (after magic+version+record_count+schema_hash)
    blob = blob:sub(1, 12) .. pack_u32(total_size) .. blob:sub(17)
    
    -- Write binary file
    local out = io.open(output_path, "wb")
    if not out then
        error("Cannot open output file: " .. output_path)
    end
    out:write(blob)
    out:close()
    print("Generated binary: " .. output_path .. " (" .. total_size .. " bytes)")
    
    return blob, schema_hash
end

--------------------------------------------------------------------------------
-- BINARY HEADER GENERATION (embeddable const array)
--------------------------------------------------------------------------------

function M.GENERATE_BINARY_HEADER(output_path, blob, schema_hash)
    -- Generate binary if not provided
    if not blob then
        blob, schema_hash = M.GENERATE_BINARY()
    end
    
    output_path = output_path or (current_file.name .. "_bin.h")
    local name_upper = current_file.name:upper()
    
    local out = io.open(output_path, "w")
    if not out then
        error("Cannot open output file: " .. output_path)
    end
    
    out:write("// " .. output_path .. "\n")
    out:write("// Generated binary schema - DO NOT EDIT\n")
    out:write("#pragma once\n\n")
    out:write("#include <stdint.h>\n\n")
    
    out:write(string.format("#define %s_SCHEMA_HASH    0x%08XU\n", name_upper, schema_hash))
    out:write(string.format("#define %s_BIN_SIZE       %d\n", name_upper, #blob))
    out:write(string.format("#define %s_RECORD_COUNT   %d\n\n", name_upper, #current_file.records))
    
    out:write(string.format("static const uint8_t %s_schema_bin[%d] = {\n", 
        current_file.name, #blob))
    
    -- Write hex bytes, 16 per line
    for i = 1, #blob, 16 do
        out:write("    ")
        for j = i, math.min(i + 15, #blob) do
            out:write(string.format("0x%02X", blob:byte(j)))
            if j < #blob then out:write(", ") end
        end
        out:write("\n")
    end
    out:write("};\n")
    
    out:close()
    print("Generated binary header: " .. output_path)
end

--------------------------------------------------------------------------------
-- C HEADER GENERATION
--------------------------------------------------------------------------------

local function upper_name(name)
    return name:upper()
end

local function resolve_ctype(ftype)
    if type_cnames[ftype] then
        return type_cnames[ftype]
    end
    for _, e in ipairs(current_file.enums) do
        if e.name == ftype then return ftype .. "_t" end
    end
    for _, f in ipairs(current_file.fixed) do
        if f.name == ftype then return ftype .. "_t" end
    end
    for _, s in ipairs(current_file.strings) do
        if s.name == ftype then return ftype .. "_t" end
    end
    for _, p in ipairs(current_file.pointers) do
        if p.name == ftype then return ftype .. "_t" end
    end
    for _, st in ipairs(current_file.structs) do
        if st.name == ftype then return ftype .. "_t" end
    end
    for _, r in ipairs(current_file.records) do
        if r.name == ftype then return ftype .. "_t" end
    end
    return ftype .. "_t"
end

local function emit_header(out)
    out:write("// " .. current_file.name .. ".h\n")
    out:write("// Generated by avro_dsl.lua - DO NOT EDIT\n")
    out:write("#pragma once\n\n")
    
    for _, inc in ipairs(current_file.includes_bracket) do
        out:write(string.format("#include <%s>\n", inc))
    end
    for _, inc in ipairs(current_file.includes_string) do
        out:write(string.format("#include \"%s\"\n", inc))
    end
    if #current_file.includes_string > 0 or #current_file.includes_bracket > 0 then
        out:write("\n")
    end
    
    out:write("#ifdef __cplusplus\n")
    out:write("extern \"C\" {\n")
    out:write("#endif\n\n")
end

local function emit_footer(out)
    out:write("\n#ifdef __cplusplus\n")
    out:write("}\n")
    out:write("#endif\n")
end

local function emit_file_metadata(out, schema_hash)
    local name_upper = upper_name(current_file.name)
    local header_name = current_file.name .. ".h"
    out:write("// ============ FILE METADATA ============\n")
    out:write(string.format("#define %s_SCHEMA_HASH   0x%08XU\n", name_upper, schema_hash))
    out:write(string.format("#define %s_RECORD_COUNT  %d\n", name_upper, #current_file.records))
    out:write(string.format("#define %s_SCHEMA_FILE   \"%s\"\n\n", name_upper, header_name))
end

local function emit_enums(out)
    if #current_file.enums == 0 then return end
    out:write("// ============ ENUMS ============\n")
    for _, e in ipairs(current_file.enums) do
        out:write(string.format("typedef enum {\n"))
        for i, v in ipairs(e.values) do
            local comma = (i < #e.values) and "," or ""
            out:write(string.format("    %s_%s = %d%s\n", upper_name(e.name), v.name, v.val, comma))
        end
        out:write(string.format("} %s_t;\n\n", e.name))
    end
end

local function emit_fixed(out)
    if #current_file.fixed == 0 then return end
    out:write("// ============ FIXED ARRAYS ============\n")
    for _, f in ipairs(current_file.fixed) do
        out:write(string.format("typedef uint8_t %s_t[%d];\n", f.name, f.size))
    end
    out:write("\n")
end

local function emit_strings(out)
    if #current_file.strings == 0 then return end
    out:write("// ============ FIXED STRINGS ============\n")
    for _, s in ipairs(current_file.strings) do
        out:write(string.format("typedef struct {\n"))
        out:write(string.format("    char buffer[%d];\n", s.length))
        out:write(string.format("    uint16_t length;\n"))
        out:write(string.format("    uint16_t max_length;\n"))
        out:write(string.format("} %s_t;\n\n", s.name))
    end
end

local function emit_pointers(out)
    if #current_file.pointers == 0 then return end
    out:write("// ============ USER POINTERS ============\n")
    for _, p in ipairs(current_file.pointers) do
        out:write(string.format("typedef struct {\n"))
        out:write(string.format("    void *ptr;\n"))
        out:write(string.format("} %s_t;\n\n", p.name))
    end
end

local function emit_struct_def(out, st)
    out:write(string.format("typedef struct {\n"))
    for _, f in ipairs(st.fields) do
        local ctype = resolve_ctype(f.type)
        if f.array_size then
            out:write(string.format("    %s %s[%d];\n", ctype, f.name, f.array_size))
        else
            out:write(string.format("    %s %s;\n", ctype, f.name))
        end
    end
    out:write(string.format("} %s_t;\n\n", st.name))
end

local function emit_structs(out)
    if #current_file.structs == 0 then return end
    out:write("// ============ STRUCTS ============\n")
    for _, st in ipairs(current_file.structs) do
        emit_struct_def(out, st)
    end
end

local function emit_records(out)
    if #current_file.records == 0 then return end
    out:write("// ============ RECORDS ============\n")
    out:write("// Note: For cross-platform wire safety, use the _wire_t variants\n\n")
    for _, r in ipairs(current_file.records) do
        emit_struct_def(out, r)
    end
end

-- Generate wire-safe record structs with explicit layout
local function emit_wire_records(out)
    if #current_file.records == 0 then return end
    
    out:write("// ============ WIRE-SAFE RECORDS ============\n")
    out:write("// Packed structs with fixed-size enums for cross-platform compatibility\n")
    out:write("// Use these for 32-bit <-> 64-bit communication\n\n")
    
    for _, r in ipairs(current_file.records) do
        out:write(string.format("#pragma pack(push, 1)\n"))
        out:write(string.format("typedef struct {\n"))
        for _, f in ipairs(r.fields) do
            local ctype = resolve_ctype(f.type)
            local is_enum = false
            
            -- Check if this is an enum type
            for _, e in ipairs(current_file.enums) do
                if e.name == f.type then
                    is_enum = true
                    break
                end
            end
            
            if is_enum then
                -- Use fixed-size int32_t for enums in wire format
                if f.array_size then
                    out:write(string.format("    int32_t %s[%d];  // enum %s\n", f.name, f.array_size, f.type))
                else
                    out:write(string.format("    int32_t %s;  // enum %s\n", f.name, f.type))
                end
            else
                if f.array_size then
                    out:write(string.format("    %s %s[%d];\n", ctype, f.name, f.array_size))
                else
                    out:write(string.format("    %s %s;\n", ctype, f.name))
                end
            end
        end
        out:write(string.format("} %s_wire_t;\n", r.name))
        out:write(string.format("#pragma pack(pop)\n\n"))
    end
    
    -- Generate conversion helpers
    out:write("// ============ WIRE CONVERSION HELPERS ============\n\n")
    for _, r in ipairs(current_file.records) do
        -- Native to wire
        out:write(string.format("static inline void %s_to_wire(const %s_t* src, %s_wire_t* dst) {\n", 
            r.name, r.name, r.name))
        for _, f in ipairs(r.fields) do
            local is_enum = false
            for _, e in ipairs(current_file.enums) do
                if e.name == f.type then is_enum = true; break end
            end
            
            if f.array_size then
                if is_enum then
                    out:write(string.format("    for (int i = 0; i < %d; i++) dst->%s[i] = (int32_t)src->%s[i];\n",
                        f.array_size, f.name, f.name))
                else
                    out:write(string.format("    memcpy(dst->%s, src->%s, sizeof(dst->%s));\n", 
                        f.name, f.name, f.name))
                end
            else
                if is_enum then
                    out:write(string.format("    dst->%s = (int32_t)src->%s;\n", f.name, f.name))
                else
                    out:write(string.format("    dst->%s = src->%s;\n", f.name, f.name))
                end
            end
        end
        out:write("}\n\n")
        
        -- Wire to native
        out:write(string.format("static inline void %s_from_wire(const %s_wire_t* src, %s_t* dst) {\n",
            r.name, r.name, r.name))
        for _, f in ipairs(r.fields) do
            local is_enum = false
            local enum_name = nil
            for _, e in ipairs(current_file.enums) do
                if e.name == f.type then is_enum = true; enum_name = e.name; break end
            end
            
            if f.array_size then
                if is_enum then
                    out:write(string.format("    for (int i = 0; i < %d; i++) dst->%s[i] = (%s_t)src->%s[i];\n",
                        f.array_size, f.name, enum_name, f.name))
                else
                    out:write(string.format("    memcpy(dst->%s, src->%s, sizeof(dst->%s));\n",
                        f.name, f.name, f.name))
                end
            else
                if is_enum then
                    out:write(string.format("    dst->%s = (%s_t)src->%s;\n", f.name, enum_name, f.name))
                else
                    out:write(string.format("    dst->%s = src->%s;\n", f.name, f.name))
                end
            end
        end
        out:write("}\n\n")
    end
end

local function emit_codecs(out)
    if #current_file.records == 0 then return end
    out:write("// ============ ENCODE/DECODE ============\n")
    for _, r in ipairs(current_file.records) do
        local tname = r.name .. "_t"
        out:write(string.format("static inline size_t %s_encode(const %s* src, uint8_t* buf) {\n", r.name, tname))
        out:write(string.format("    memcpy(buf, src, sizeof(%s));\n", tname))
        out:write(string.format("    return sizeof(%s);\n", tname))
        out:write("}\n\n")
        out:write(string.format("static inline void %s_decode(const uint8_t* buf, %s* dst) {\n", r.name, tname))
        out:write(string.format("    memcpy(dst, buf, sizeof(%s));\n", tname))
        out:write("}\n\n")
    end
end

local function emit_wire_header_type(out, schema_hash)
    local name_upper = upper_name(current_file.name)
    
    out:write("// ============ WIRE HEADER ============\n")
    out:write("// Common header for all wire packets (16 bytes, packed)\n")
    out:write("// schema_hash replaces string pointer for socket-safe transmission\n\n")
    
    out:write("#pragma pack(push, 1)\n")
    out:write("typedef struct {\n")
    out:write("    double      timestamp;     // 8: message timestamp (set by transport)\n")
    out:write("    uint32_t    schema_hash;   // 4: FNV-1a hash of schema .h filename\n")
    out:write("    uint16_t    seq;           // 2: sequence number (set by transport)\n")
    out:write("    uint8_t     source_node;   // 1: originating node ID\n")
    out:write("    uint8_t     index;         // 1: record type index\n")
    out:write(string.format("} %s_wire_header_t;\n", current_file.name))
    out:write("#pragma pack(pop)\n\n")
    
    -- Static assert for header size
    out:write(string.format("_Static_assert(sizeof(%s_wire_header_t) == 16, \"Wire header must be 16 bytes\");\n\n",
        current_file.name))
    
    -- Verify header helper
    out:write(string.format("static inline bool %s_verify_header(const %s_wire_header_t* hdr) {\n",
        current_file.name, current_file.name))
    out:write(string.format("    return hdr->schema_hash == %s_SCHEMA_HASH;\n", name_upper))
    out:write("}\n\n")
end

local function emit_wire_packets(out, schema_hash)
    if #current_file.records == 0 then return end
    
    local name_upper = upper_name(current_file.name)
    
    out:write("// ============ WIRE PACKETS ============\n")
    out:write("// Per-record packet types with unified header\n")
    out:write("// Socket-safe: no pointers, fixed size, hash-based identification\n")
    out:write("// Uses packed _wire_t records for cross-platform compatibility\n\n")
    
    for _, r in ipairs(current_file.records) do
        out:write(string.format("#pragma pack(push, 1)\n"))
        out:write(string.format("typedef struct {\n"))
        out:write(string.format("    %s_wire_header_t header;\n", current_file.name))
        out:write(string.format("    %s_wire_t        data;\n", r.name))
        out:write(string.format("} %s_packet_t;\n", r.name))
        out:write(string.format("#pragma pack(pop)\n\n"))
    end
    
    -- Static asserts for wire record sizes
    out:write("// Static assertions for wire format sizes\n")
    for _, r in ipairs(current_file.records) do
        local size = compute_container_size(r)
        out:write(string.format("_Static_assert(sizeof(%s_wire_t) == %d, \"%s_wire_t size mismatch\");\n",
            r.name, size, r.name))
    end
    out:write("\n")
    
    -- Generate encode helper per record
    out:write("// Packet encode helpers - populate header and return pointer to wire data\n")
    out:write("// Note: seq and timestamp are zeroed; set by transport layer before sending\n\n")
    for i, r in ipairs(current_file.records) do
        local idx = i - 1  -- 0-based
        out:write(string.format("static inline %s_wire_t* %s_packet_init(\n", r.name, r.name))
        out:write(string.format("        %s_packet_t* pkt,\n", r.name))
        out:write("        uint8_t source_node)\n")
        out:write("{\n")
        out:write(string.format("    pkt->header.schema_hash = %s_SCHEMA_HASH;\n", name_upper))
        out:write("    pkt->header.timestamp = 0.0;\n")
        out:write("    pkt->header.seq = 0;\n")
        out:write("    pkt->header.source_node = source_node;\n")
        out:write(string.format("    pkt->header.index = %d;\n", idx))
        out:write("    return &pkt->data;\n")
        out:write("}\n\n")
    end
    
    -- Generate verify helpers
    out:write("// Packet verify helpers - validate schema hash and index, return wire data pointer\n\n")
    for i, r in ipairs(current_file.records) do
        local idx = i - 1
        out:write(string.format("static inline const %s_wire_t* %s_packet_verify(\n", r.name, r.name))
        out:write(string.format("        const %s_packet_t* pkt)\n", r.name))
        out:write("{\n")
        out:write(string.format("    if (pkt->header.schema_hash != %s_SCHEMA_HASH) return NULL;\n", name_upper))
        out:write(string.format("    if (pkt->header.index != %d) return NULL;\n", idx))
        out:write("    return &pkt->data;\n")
        out:write("}\n\n")
    end
    
    -- Generic dispatch helper
    out:write("// Generic packet dispatch - returns record index or -1 on error\n")
    out:write(string.format("static inline int %s_packet_dispatch(\n", current_file.name))
    out:write("        const void* packet_buffer,\n")
    out:write("        uint8_t* source_node_out,\n")
    out:write("        const void** data_out)\n")
    out:write("{\n")
    out:write(string.format("    const %s_wire_header_t* hdr = (const %s_wire_header_t*)packet_buffer;\n",
        current_file.name, current_file.name))
    out:write(string.format("    if (hdr->schema_hash != %s_SCHEMA_HASH) return -1;\n", name_upper))
    out:write(string.format("    if (hdr->index >= %s_RECORD_COUNT) return -1;\n", name_upper))
    out:write("    if (source_node_out) *source_node_out = hdr->source_node;\n")
    out:write(string.format("    if (data_out) *data_out = ((const uint8_t*)packet_buffer) + sizeof(%s_wire_header_t);\n",
        current_file.name))
    out:write("    return hdr->index;\n")
    out:write("}\n\n")
    
    -- Wire record sizes array
    out:write("// Wire record payload sizes (for buffer allocation)\n")
    out:write(string.format("static const uint16_t %s_wire_sizes[%s_RECORD_COUNT] = {\n",
        current_file.name, name_upper))
    for _, r in ipairs(current_file.records) do
        out:write(string.format("    sizeof(%s_wire_t),  // %s\n", r.name, r.name))
    end
    out:write("};\n\n")
    
    -- Packet sizes array
    out:write("// Full packet sizes including header (for socket send/recv)\n")
    out:write(string.format("static const uint16_t %s_packet_sizes[%s_RECORD_COUNT] = {\n",
        current_file.name, name_upper))
    for _, r in ipairs(current_file.records) do
        out:write(string.format("    sizeof(%s_packet_t),  // %s\n", r.name, r.name))
    end
    out:write("};\n")
end

function M.GENERATE(output_path)
    output_path = output_path or (current_file.name .. ".h")
    
    -- Compute schema hash first
    local header_name = current_file.name .. ".h"
    local schema_hash = fnv1a_32(header_name)
    
    local out = io.open(output_path, "w")
    if not out then
        error("Cannot open output file: " .. output_path)
    end
    
    emit_header(out)
    emit_file_metadata(out, schema_hash)
    emit_enums(out)
    emit_fixed(out)
    emit_strings(out)
    emit_pointers(out)
    emit_structs(out)
    emit_records(out)
    emit_wire_records(out)
    emit_wire_header_type(out, schema_hash)
    emit_wire_packets(out, schema_hash)
    emit_footer(out)
    out:close()
    print("Generated: " .. output_path)
    
    return schema_hash
end

-- Generate all outputs
function M.GENERATE_ALL(base_path)
    base_path = base_path or current_file.name
    
    local schema_hash = M.GENERATE(base_path .. ".h")
    local blob = nil
    blob, schema_hash = M.GENERATE_BINARY(base_path .. ".bin")
    M.GENERATE_BINARY_HEADER(base_path .. "_bin.h", blob, schema_hash)
    
    return schema_hash
end

--------------------------------------------------------------------------------
-- MODULE EXPORT
--------------------------------------------------------------------------------

function M.export_globals()
    _G.FILE            = M.FILE
    _G.INCLUDE_BRACKET = M.INCLUDE_BRACKET
    _G.INCLUDE_STRING  = M.INCLUDE_STRING
    _G.ENUM            = M.ENUM
    _G.VALUE           = M.VALUE
    _G.END_ENUM        = M.END_ENUM
    _G.FIXED           = M.FIXED
    _G.STRING          = M.STRING
    _G.POINTER         = M.POINTER
    _G.STRUCT          = M.STRUCT
    _G.RECORD          = M.RECORD
    _G.FIELD           = M.FIELD
    _G.END_STRUCT      = M.END_STRUCT
    _G.END_RECORD      = M.END_RECORD
    _G.GENERATE        = M.GENERATE
    _G.GENERATE_BINARY = M.GENERATE_BINARY
    _G.GENERATE_BINARY_HEADER = M.GENERATE_BINARY_HEADER
    _G.GENERATE_ALL    = M.GENERATE_ALL
end

return M