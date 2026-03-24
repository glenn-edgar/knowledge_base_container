--[[
  stage6_binary.lua - ChainTree Binary Image Emitter

  Produces .ctb binary images consumed by cfl_image_loader.c.
  
  CRITICAL DESIGN: All arrays use ORIGINAL INDEXER ORDER, identical
  to what stage6_codegen.lua produces for .h/.c. Hash tables are a
  side structure for function registration only.

  - Node array: function indices = original indexer order
  - FSTR: KB names in original order → handle->main_function_names
  - MUSG: usage counts in original order → handle->main_function_usage_count
  - MFHT/OSHT/BFHT: {hash, slot_index} pairs sorted by hash
    Registration: hash(typed_name) → binary_search → slot_index → fn_ptrs[slot]

  Requires libfnv1a.so for FNV-1a hashing via FFI.
--]]

local ffi = require("ffi")
local bit = require("bit")

-- Load FNV-1a shared library
ffi.cdef[[ uint32_t fnv1a_32(const char *str); ]]
local fnv1a_lib = ffi.load("fnv1a")

local function fnv1a(str)
    return tonumber(fnv1a_lib.fnv1a_32(str))
end

-- =========================================================================
-- CRC32 (pure Lua, table-driven, ISO 3309 compatible)
-- =========================================================================

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
        local b = string.byte(data, i)
        local idx = bit.band(bit.bxor(crc, b), 0xFF)
        crc = bit.bxor(crc32_table[idx], bit.rshift(crc, 8))
    end
    return bit.bxor(crc, 0xFFFFFFFF)
end

-- =========================================================================
-- Binary writer helpers
-- =========================================================================

local function pack_u16(val)
    return string.char(bit.band(val, 0xFF), bit.band(bit.rshift(val, 8), 0xFF))
end

local function pack_u32(val)
    return string.char(
        bit.band(val, 0xFF),
        bit.band(bit.rshift(val, 8), 0xFF),
        bit.band(bit.rshift(val, 16), 0xFF),
        bit.band(bit.rshift(val, 24), 0xFF)
    )
end

local function align4(offset)
    return bit.band(offset + 3, bit.bnot(3))
end

-- FFI helpers for float/int32 bit reinterpretation
local ffi_float = ffi.new("float[1]")
local ffi_u32   = ffi.cast("uint32_t*", ffi_float)
local ffi_i32   = ffi.new("int32_t[1]")

local function float_to_u32(f)
    ffi_float[0] = f
    return tonumber(ffi_u32[0])
end

local function int32_to_u32(i)
    ffi_i32[0] = i
    return tonumber(ffi.cast("uint32_t*", ffi_i32)[0])
end

-- =========================================================================
-- String Pool Builder (deduplicated)
-- =========================================================================

local StringPool = {}
StringPool.__index = StringPool

function StringPool.new()
    local self = setmetatable({}, StringPool)
    self.strings = {}
    self.offsets = {}
    self.next_offset = 0
    return self
end

function StringPool:add(str)
    if self.offsets[str] then return self.offsets[str] end
    local offset = self.next_offset
    self.offsets[str] = offset
    self.strings[#self.strings + 1] = str
    self.next_offset = offset + #str + 1
    return offset
end

function StringPool:to_binary()
    local parts = {}
    for _, s in ipairs(self.strings) do
        parts[#parts + 1] = s .. "\0"
    end
    return table.concat(parts)
end

function StringPool:size()
    return self.next_offset
end

-- =========================================================================
-- Section Types
-- =========================================================================

local SECT_NODE = 0x0001
local SECT_LINK = 0x0002
local SECT_MFHT = 0x0003    -- main function hash table: {hash, slot}[]
local SECT_OSHT = 0x0004    -- one_shot hash table: {hash, slot}[]
local SECT_BFHT = 0x0005    -- boolean hash table: {hash, slot}[]
local SECT_FSTR = 0x0006    -- function KB names, original order
local SECT_JREC = 0x0007
local SECT_JCTL = 0x0008
local SECT_JSTR = 0x0009
local SECT_EVNT = 0x000A
local SECT_BMSK = 0x000B
local SECT_KBIN = 0x000C
local SECT_KBAL = 0x000D
local SECT_GSTR = 0x000E
local SECT_MUSG = 0x000F    -- main function usage count, original order
local SECT_BBRD = 0x0010    -- blackboard record descriptor + defaults
local SECT_CREC = 0x0011    -- constant records

-- =========================================================================
-- Binary Image Emitter
-- =========================================================================

local BinaryImageEmitter = {}
BinaryImageEmitter.__index = BinaryImageEmitter

function BinaryImageEmitter.new(opts)
    local self = setmetatable({}, BinaryImageEmitter)
    self.output_dir = opts.output_dir
    self.handle_name = opts.handle_name
    self.handle = opts.handle
    self.node_builder = opts.node_builder
    self.function_builder = opts.function_builder
    self.link_builder = opts.link_builder
    self.data_encoder = opts.data_encoder
    self.main_function_usage = opts.main_function_usage or {}
    self.emit_c_header = opts.emit_c_header or false
    return self
end

-- =========================================================================
-- Hash table building
--
-- Builds {hash, slot_index} entries sorted by hash.
-- slot_index = original indexer position (0-based).
-- Node array uses original indices — NO remapping.
-- =========================================================================

function BinaryImageEmitter:_build_hash_table(indexer, type_name_fn)
    local entries = {}
    local all_funcs = indexer:get_all_functions()

    for i, kb_name in ipairs(all_funcs) do
        local typed_name = type_name_fn(kb_name)
        local hash = fnv1a(typed_name)
        entries[#entries + 1] = {
            hash = hash,
            slot_index = i - 1,   -- original 0-based index
            typed_name = typed_name,
            kb_name = kb_name,
        }
    end

    -- Check for hash collisions
    local hash_set = {}
    for _, e in ipairs(entries) do
        if hash_set[e.hash] then
            error(string.format(
                "FNV-1a collision: '%s' and '%s' both hash to 0x%08X",
                hash_set[e.hash], e.typed_name, e.hash))
        end
        hash_set[e.hash] = e.typed_name
    end

    -- Sort by hash value for binary search at runtime
    table.sort(entries, function(a, b) return a.hash < b.hash end)

    return entries
end

-- =========================================================================
-- Hash table binary: each entry = {u32 hash, u16 slot_index, u16 reserved}
-- Sorted by hash. 8 bytes per entry.
-- =========================================================================

local function hash_table_to_binary(entries)
    local parts = {}
    for _, e in ipairs(entries) do
        parts[#parts + 1] = pack_u32(e.hash)
        parts[#parts + 1] = pack_u16(e.slot_index)
        parts[#parts + 1] = pack_u16(0)  -- reserved
    end
    return table.concat(parts)
end

-- =========================================================================
-- Node array binary — uses ORIGINAL indexer order, no remapping
-- =========================================================================

function BinaryImageEmitter:_build_node_array()
    local parts = {}
    local array_size = self.node_builder:get_array_size()

    for i = 0, array_size - 1 do
        local ltree_name = self.node_builder:get_node_by_index(i)

        if not ltree_name then
            -- Empty/hole node
            parts[#parts + 1] = pack_u16(i)
            parts[#parts + 1] = pack_u16(0xFFFF)
            parts[#parts + 1] = pack_u16(0)
            parts[#parts + 1] = pack_u16(0)
            parts[#parts + 1] = pack_u16(0)
            parts[#parts + 1] = pack_u16(0)  -- main = CFL_NULL slot 0
            parts[#parts + 1] = pack_u16(0)  -- init = CFL_NULL slot 0
            parts[#parts + 1] = pack_u16(0)  -- aux  = CFL_NULL slot 0
            parts[#parts + 1] = pack_u16(0)  -- term = CFL_NULL slot 0
            parts[#parts + 1] = pack_u16(0xFFFF)
        else
            local functions = self.handle:get_node_functions(ltree_name)
            local node_data = self.handle:get_node_data(ltree_name)

            -- Original indexer indices — same as .h/.c path
            local main_idx = self.function_builder.main_indexer:get_index(functions.main)
            local init_idx = self.function_builder.one_shot_indexer:get_index(functions.init)
            local aux_idx  = self.function_builder.boolean_indexer:get_index(functions.aux)
            local term_idx = self.function_builder.one_shot_indexer:get_index(functions.term)

            local link_info = self.link_builder:get_node_link_info(ltree_name)
            local link_count = link_info.link_count

            local node_dict = (type(node_data) == "table") and (node_data.node_dict or {}) or {}
            local auto_start = node_dict.auto_start or false

            local packed_lc = bit.band(link_count, 0x7FFF)
            if auto_start then
                packed_lc = bit.bor(packed_lc, 0x8000)
            end

            local parent_ltree = self.handle:get_node_parent(ltree_name)
            local parent_idx = 0xFFFF
            if parent_ltree and self.node_builder.ltree_to_final_index[parent_ltree] then
                parent_idx = self.node_builder:get_node_final_index(parent_ltree)
            end

            local depth = self.node_builder:get_node_depth(ltree_name)
            local data_id = 0xFFFF
            if self.data_encoder then
                data_id = self.data_encoder:get_node_data_id(ltree_name)
            end

            parts[#parts + 1] = pack_u16(i)
            parts[#parts + 1] = pack_u16(parent_idx)
            parts[#parts + 1] = pack_u16(depth)
            parts[#parts + 1] = pack_u16(link_info.link_start)
            parts[#parts + 1] = pack_u16(packed_lc)
            parts[#parts + 1] = pack_u16(main_idx)
            parts[#parts + 1] = pack_u16(init_idx)
            parts[#parts + 1] = pack_u16(aux_idx)
            parts[#parts + 1] = pack_u16(term_idx)
            parts[#parts + 1] = pack_u16(data_id)
        end
    end

    return table.concat(parts)
end

-- =========================================================================
-- Link table binary
-- =========================================================================

function BinaryImageEmitter:_build_link_table()
    local parts = {}
    for _, child_index in ipairs(self.link_builder.link_table) do
        parts[#parts + 1] = pack_u16(child_index)
    end
    return table.concat(parts)
end

-- =========================================================================
-- Function KB names — ORIGINAL ORDER (matches .h/.c name arrays)
-- Packed NUL-delimited: main[0]\0main[1]\0...os[0]\0...bool[0]\0...
-- =========================================================================

function BinaryImageEmitter:_build_func_names()
    local fb = self.function_builder
    local parts = {}

    for _, kb_name in ipairs(fb.main_indexer:get_all_functions()) do
        parts[#parts + 1] = kb_name .. "\0"
    end
    for _, kb_name in ipairs(fb.one_shot_indexer:get_all_functions()) do
        parts[#parts + 1] = kb_name .. "\0"
    end
    for _, kb_name in ipairs(fb.boolean_indexer:get_all_functions()) do
        parts[#parts + 1] = kb_name .. "\0"
    end

    return table.concat(parts)
end

-- =========================================================================
-- Main function usage count — ORIGINAL ORDER
-- =========================================================================

function BinaryImageEmitter:_build_usage_count()
    local fb = self.function_builder
    local all_main = fb.main_indexer:get_all_functions()
    local parts = {}

    for i = 1, #all_main do
        local usage = self.main_function_usage[i - 1] or 0  -- 0-based key
        parts[#parts + 1] = pack_u16(usage)
    end

    return table.concat(parts)
end

-- =========================================================================
-- JSON data sections
-- =========================================================================

function BinaryImageEmitter:_build_json_sections()
    if not self.data_encoder then
        return "", "", ""
    end

    local enc = self.data_encoder.encoder

    local rec_parts = {}
    for _, rec in ipairs(enc.records) do
        rec_parts[#rec_parts + 1] = pack_u32(rec[1])
        rec_parts[#rec_parts + 1] = pack_u32(rec[2])
    end
    local records_bin = table.concat(rec_parts)

    local ctrl_parts = {}
    for _, ctrl in ipairs(enc.record_controls) do
        ctrl_parts[#ctrl_parts + 1] = pack_u32(ctrl.start_position)
        ctrl_parts[#ctrl_parts + 1] = pack_u32(ctrl.num_records)
    end
    local controls_bin = table.concat(ctrl_parts)

    local str_parts = {}
    for _, s in ipairs(enc.string_data) do
        str_parts[#str_parts + 1] = s .. "\0"
    end
    local strings_bin = table.concat(str_parts)

    return records_bin, controls_bin, strings_bin
end

-- =========================================================================
-- Event string table
-- =========================================================================

function BinaryImageEmitter:_build_event_section(string_pool)
    local events = self.handle:get_event_string_table()
    local count = 0
    for _ in pairs(events) do count = count + 1 end
    if count == 0 then return "", 0 end

    local sorted = {}
    for name, idx in pairs(events) do
        sorted[#sorted + 1] = { name = name, idx = idx }
    end
    table.sort(sorted, function(a, b) return a.idx < b.idx end)

    local parts = {}
    for _, entry in ipairs(sorted) do
        local offset = string_pool:add(entry.name)
        parts[#parts + 1] = pack_u32(offset)
    end

    return table.concat(parts), count
end

-- =========================================================================
-- Bitmask table
-- =========================================================================

function BinaryImageEmitter:_build_bitmask_section(string_pool)
    local bitmasks = self.handle:get_bitmask_table()
    local count = 0
    for _ in pairs(bitmasks) do count = count + 1 end
    if count == 0 then return "", 0 end

    local sorted = {}
    for name, bit_num in pairs(bitmasks) do
        sorted[#sorted + 1] = { name = name, bit_num = bit_num }
    end
    table.sort(sorted, function(a, b) return a.bit_num < b.bit_num end)

    local parts = {}
    for _, entry in ipairs(sorted) do
        local offset = string_pool:add(entry.name)
        parts[#parts + 1] = pack_u32(offset)
        parts[#parts + 1] = pack_u16(entry.bit_num)
        parts[#parts + 1] = pack_u16(0)
    end

    return table.concat(parts), count
end

-- =========================================================================
-- KB info sections
-- =========================================================================

local function _filter_executable_kbs(kb_names, node_builder)
    local result = {}
    for _, name in ipairs(kb_names) do
        if not name:match("_test_functions$")
            and name ~= "complete_functions_kb"
            and name ~= "event_string_table_kb"
            and name ~= "bitmask_table_kb"
        then
            result[#result + 1] = name
        end
    end
    -- Sort by start_index — must match .h/.c codegen order
    table.sort(result, function(a, b)
        local sa = node_builder:get_kb_range(a)
        local sb = node_builder:get_kb_range(b)
        return sa < sb
    end)
    return result
end
function BinaryImageEmitter:_build_kb_sections(string_pool)
    local kb_names = _filter_executable_kbs(self.handle:get_kb_names(), self.node_builder)
    local kb_parts = {}
    local alias_parts = {}
    local total_aliases = 0

    for _, kb_name in ipairs(kb_names) do
        local name_offset = string_pool:add(kb_name)
        local start_idx, end_idx = self.node_builder:get_kb_range(kb_name)
        local node_count = end_idx - start_idx

        local max_depth = 0
        for j = start_idx, end_idx - 1 do
            local lt = self.node_builder:get_node_by_index(j)
            if lt then
                local d = self.node_builder:get_node_depth(lt)
                if d > max_depth then max_depth = d end
            end
        end

        local memory_factor = self.handle:get_kb_metadata_value(kb_name, "node_memory_factor", 10)
        local aliases = self.handle:get_kb_node_aliases(kb_name)
        local alias_count = 0
        for _ in pairs(aliases) do alias_count = alias_count + 1 end
        local alias_start = total_aliases

        -- KB entry: 24 bytes
        kb_parts[#kb_parts + 1] = pack_u32(name_offset)
        kb_parts[#kb_parts + 1] = pack_u16(start_idx)
        kb_parts[#kb_parts + 1] = pack_u16(start_idx)
        kb_parts[#kb_parts + 1] = pack_u16(node_count)
        kb_parts[#kb_parts + 1] = pack_u16(max_depth)
        kb_parts[#kb_parts + 1] = pack_u16(memory_factor)
        kb_parts[#kb_parts + 1] = pack_u16(alias_start)
        kb_parts[#kb_parts + 1] = pack_u16(alias_count)
        kb_parts[#kb_parts + 1] = string.rep("\0", 6)

        if alias_count > 0 then
            local sorted_aliases = {}
            for aname, aindex in pairs(aliases) do
                sorted_aliases[#sorted_aliases + 1] = { name = aname, index = aindex }
            end
            table.sort(sorted_aliases, function(a, b) return a.name < b.name end)

            for _, a in ipairs(sorted_aliases) do
                local aname_offset = string_pool:add(a.name)
                alias_parts[#alias_parts + 1] = pack_u32(aname_offset)
                alias_parts[#alias_parts + 1] = pack_u16(a.index)
                alias_parts[#alias_parts + 1] = pack_u16(0)
            end
            total_aliases = total_aliases + alias_count
        end
    end

    return table.concat(kb_parts), table.concat(alias_parts),
           #kb_names, total_aliases
end

-- =========================================================================
-- Blackboard type sizes and alignment
-- =========================================================================

local bb_type_info = {
    int32   = { size = 4, align = 4 },
    uint32  = { size = 4, align = 4 },
    uint16  = { size = 2, align = 2 },
    float   = { size = 4, align = 4 },
    uint64  = { size = 8, align = 8 },
}

local function compute_field_layout(fields)
    local layout = {}
    local offset = 0
    for _, f in ipairs(fields) do
        local info = bb_type_info[f.type]
        if not info then error("Unknown blackboard field type: " .. f.type) end
        local a = info.align
        local padding = (a - (offset % a)) % a
        offset = offset + padding
        layout[#layout + 1] = {
            name = f.name,
            type = f.type,
            offset = offset,
            size = info.size,
            default = f.default or f.value or 0,
        }
        offset = offset + info.size
    end
    local pad = (4 - (offset % 4)) % 4
    offset = offset + pad
    return layout, offset
end

-- =========================================================================
-- BBRD section: blackboard record descriptor + field descriptors + defaults
--
-- Binary layout:
--   Header (12 bytes):
--     u32 name_hash
--     u16 total_size (blackboard allocation size)
--     u16 field_count
--     u32 defaults_offset (offset from section start to defaults blob)
--   Field descriptors (8 bytes each):
--     u32 name_hash
--     u16 offset
--     u16 size
--   Defaults blob (total_size bytes, 4-byte aligned)
-- =========================================================================

function BinaryImageEmitter:_build_blackboard_section()
    local bb = self.handle.blackboard
    if not bb or not bb.record then return "", 0 end

    local layout, total = compute_field_layout(bb.record.fields)
    local field_count = #layout

    -- Defaults blob (typed writes into byte buffer)
    local defaults = ffi.new("uint8_t[?]", total)
    ffi.fill(defaults, total, 0)
    for _, f in ipairs(layout) do
        if f.default ~= 0 then
            local info = bb_type_info[f.type]
            if f.type == "float" then
                local fp = ffi.cast("float*", defaults + f.offset)
                fp[0] = f.default
            elseif f.type == "int32" then
                local ip = ffi.cast("int32_t*", defaults + f.offset)
                ip[0] = f.default
            elseif f.type == "uint32" then
                local up = ffi.cast("uint32_t*", defaults + f.offset)
                up[0] = f.default
            elseif f.type == "uint16" then
                local sp = ffi.cast("uint16_t*", defaults + f.offset)
                sp[0] = f.default
            elseif f.type == "uint64" then
                local lp = ffi.cast("uint64_t*", defaults + f.offset)
                lp[0] = f.default
            end
        end
    end

    local defaults_offset = 12 + field_count * 8  -- after header + fields
    defaults_offset = align4(defaults_offset)

    local parts = {}
    -- Header
    parts[#parts + 1] = pack_u32(fnv1a(bb.record.name))
    parts[#parts + 1] = pack_u16(total)
    parts[#parts + 1] = pack_u16(field_count)
    parts[#parts + 1] = pack_u32(defaults_offset)

    -- Field descriptors
    for _, f in ipairs(layout) do
        parts[#parts + 1] = pack_u32(fnv1a(f.name))
        parts[#parts + 1] = pack_u16(f.offset)
        parts[#parts + 1] = pack_u16(f.size)
    end

    -- Padding to defaults_offset
    local current = 12 + field_count * 8
    if current < defaults_offset then
        parts[#parts + 1] = string.rep("\0", defaults_offset - current)
    end

    -- Defaults blob
    parts[#parts + 1] = ffi.string(defaults, total)

    return table.concat(parts), field_count
end

-- =========================================================================
-- CREC section: constant records
--
-- Binary layout:
--   Directory (8 bytes per record):
--     u32 name_hash
--     u16 total_size
--     u16 field_count
--   For each record, sequentially:
--     Field descriptors (8 bytes each):
--       u32 name_hash
--       u16 offset
--       u16 size
--     Data blob (total_size bytes, 4-byte aligned)
-- =========================================================================

function BinaryImageEmitter:_build_const_records_section()
    local bb = self.handle.blackboard
    if not bb or not bb.const_records or #bb.const_records == 0 then
        return "", 0
    end

    local records = bb.const_records
    local record_count = #records

    -- First pass: compute layouts
    local layouts = {}
    for i, cr in ipairs(records) do
        local layout, total = compute_field_layout(cr.fields)
        layouts[i] = { layout = layout, total = total, name = cr.name }
    end

    -- Directory (8 bytes per record)
    local dir_parts = {}
    for _, l in ipairs(layouts) do
        dir_parts[#dir_parts + 1] = pack_u32(fnv1a(l.name))
        dir_parts[#dir_parts + 1] = pack_u16(l.total)
        dir_parts[#dir_parts + 1] = pack_u16(#l.layout)
    end

    -- Field descriptors + data blobs for each record
    local body_parts = {}
    for _, l in ipairs(layouts) do
        -- Field descriptors
        for _, f in ipairs(l.layout) do
            body_parts[#body_parts + 1] = pack_u32(fnv1a(f.name))
            body_parts[#body_parts + 1] = pack_u16(f.offset)
            body_parts[#body_parts + 1] = pack_u16(f.size)
        end

        -- Data blob (typed writes)
        local data = ffi.new("uint8_t[?]", l.total)
        ffi.fill(data, l.total, 0)
        for _, f in ipairs(l.layout) do
            if f.default ~= 0 then
                if f.type == "float" then
                    ffi.cast("float*", data + f.offset)[0] = f.default
                elseif f.type == "int32" then
                    ffi.cast("int32_t*", data + f.offset)[0] = f.default
                elseif f.type == "uint32" then
                    ffi.cast("uint32_t*", data + f.offset)[0] = f.default
                elseif f.type == "uint16" then
                    ffi.cast("uint16_t*", data + f.offset)[0] = f.default
                elseif f.type == "uint64" then
                    ffi.cast("uint64_t*", data + f.offset)[0] = f.default
                end
            end
        end
        body_parts[#body_parts + 1] = ffi.string(data, l.total)
    end

    local result = table.concat(dir_parts) .. table.concat(body_parts)
    return result, record_count
end

-- =========================================================================
-- Blackboard offset header (.h with #defines only, no runtime data)
-- =========================================================================

function BinaryImageEmitter:_write_bb_offset_header()
    local bb = self.handle.blackboard
    if not bb then return end

    local guard = self.handle_name:upper() .. "_BLACKBOARD_H"
    local lines = {}
    lines[#lines + 1] = "/* Auto-generated by ChainTree Pipeline (Binary) - Blackboard offsets */"
    lines[#lines + 1] = "#ifndef " .. guard
    lines[#lines + 1] = "#define " .. guard
    lines[#lines + 1] = ""
    lines[#lines + 1] = '#include "cfl_blackboard.h"'
    lines[#lines + 1] = ""

    if bb.record then
        local layout, total = compute_field_layout(bb.record.fields)
        lines[#lines + 1] = "/* ===== Blackboard: " .. bb.record.name .. " ===== */"
        lines[#lines + 1] = string.format("#define BB_%s_SIZE %d", bb.record.name:upper(), total)
        lines[#lines + 1] = ""
        for _, f in ipairs(layout) do
            local def_name = "BB_" .. f.name:upper():gsub("%.", "_") .. "_OFFSET"
            lines[#lines + 1] = string.format("#define %-40s %d", def_name, f.offset)
        end
        lines[#lines + 1] = ""
    end

    if bb.const_records then
        for _, cr in ipairs(bb.const_records) do
            local layout, total = compute_field_layout(cr.fields)
            lines[#lines + 1] = "/* ===== Const Record: " .. cr.name .. " ===== */"
            lines[#lines + 1] = string.format("#define CONST_%s_SIZE %d", cr.name:upper(), total)
            lines[#lines + 1] = ""
            for _, f in ipairs(layout) do
                local def_name = "CONST_" .. cr.name:upper() .. "_" .. f.name:upper():gsub("%.", "_") .. "_OFFSET"
                lines[#lines + 1] = string.format("#define %-40s %d", def_name, f.offset)
            end
            lines[#lines + 1] = ""
        end
    end

    lines[#lines + 1] = "#endif /* " .. guard .. " */"
    lines[#lines + 1] = ""

    local path = self.output_dir .. "/" .. self.handle_name .. "_blackboard.h"
    local fh = io.open(path, "w")
    fh:write(table.concat(lines, "\n"))
    fh:close()
    print("  Generated: " .. path)
end

-- =========================================================================
-- Main emit function
-- =========================================================================

function BinaryImageEmitter:emit()
    print("\n  Building hash tables...")

    local fb = self.function_builder

    -- Step 1: Build hash→slot lookup tables (sorted by hash)
    local main_ht = self:_build_hash_table(
        fb.main_indexer,
        function(kb_name) return fb:get_typed_main_name(kb_name) end)

    local os_ht = self:_build_hash_table(
        fb.one_shot_indexer,
        function(kb_name) return fb:get_typed_one_shot_name(kb_name) end)

    local bool_ht = self:_build_hash_table(
        fb.boolean_indexer,
        function(kb_name) return fb:get_typed_boolean_name(kb_name) end)

    print(string.format("    Main: %d functions",     #main_ht))
    print(string.format("    One-shot: %d functions", #os_ht))
    print(string.format("    Boolean: %d functions",  #bool_ht))

    -- Step 2: Build all binary sections
    print("  Building sections...")

    -- Node array uses ORIGINAL indices — no remapping
    local node_bin        = self:_build_node_array()
    local link_bin        = self:_build_link_table()

    -- Hash tables: {hash, slot_index} sorted by hash, 8 bytes each
    local main_hash_bin   = hash_table_to_binary(main_ht)
    local os_hash_bin     = hash_table_to_binary(os_ht)
    local bool_hash_bin   = hash_table_to_binary(bool_ht)

    -- KB names in ORIGINAL order
    local func_names_bin  = self:_build_func_names()

    -- Usage counts in ORIGINAL order
    local usage_bin       = self:_build_usage_count()

    local json_rec_bin, json_ctrl_bin, json_str_bin = self:_build_json_sections()

    local string_pool = StringPool.new()
    local event_bin,   event_count   = self:_build_event_section(string_pool)
    local bitmask_bin, bitmask_count = self:_build_bitmask_section(string_pool)
    local kb_info_bin, kb_alias_bin, kb_count, alias_count =
        self:_build_kb_sections(string_pool)
    local pool_bin = string_pool:to_binary()

    -- Blackboard sections
    local bb_bin, bb_field_count     = self:_build_blackboard_section()
    local crec_bin, crec_count       = self:_build_const_records_section()

    -- Step 3: Counts
    local json_rec_count  = 0
    local json_ctrl_count = 0
    local json_str_size   = #json_str_bin
    if self.data_encoder then
        json_rec_count  = #self.data_encoder.encoder.records
        json_ctrl_count = #self.data_encoder.encoder.record_controls
    end

    -- Step 4: Section list (15 sections)
    local sections = {
        { type = SECT_NODE, data = node_bin,        count = self.node_builder:get_array_size(),                             esize = 20 },
        { type = SECT_LINK, data = link_bin,        count = self.link_builder:get_link_table_size(),                        esize = 2  },
        { type = SECT_MFHT, data = main_hash_bin,   count = #main_ht,                                                      esize = 8  },
        { type = SECT_OSHT, data = os_hash_bin,     count = #os_ht,                                                        esize = 8  },
        { type = SECT_BFHT, data = bool_hash_bin,   count = #bool_ht,                                                      esize = 8  },
        { type = SECT_FSTR, data = func_names_bin,  count = fb.main_indexer:get_count() + fb.one_shot_indexer:get_count() + fb.boolean_indexer:get_count(), esize = 0 },
        { type = SECT_JREC, data = json_rec_bin,    count = json_rec_count,                                                 esize = 8  },
        { type = SECT_JCTL, data = json_ctrl_bin,   count = json_ctrl_count,                                                esize = 8  },
        { type = SECT_JSTR, data = json_str_bin,    count = 0,                                                              esize = 0  },
        { type = SECT_EVNT, data = event_bin,       count = event_count,                                                    esize = 4  },
        { type = SECT_BMSK, data = bitmask_bin,     count = bitmask_count,                                                  esize = 8  },
        { type = SECT_KBIN, data = kb_info_bin,     count = kb_count,                                                       esize = 24 },
        { type = SECT_KBAL, data = kb_alias_bin,    count = alias_count,                                                    esize = 8  },
        { type = SECT_GSTR, data = pool_bin,        count = 0,                                                              esize = 0  },
        { type = SECT_MUSG, data = usage_bin,       count = fb.main_indexer:get_count(),                                    esize = 2  },
        { type = SECT_BBRD, data = bb_bin,         count = bb_field_count,                                                  esize = 0  },
        { type = SECT_CREC, data = crec_bin,       count = crec_count,                                                      esize = 0  },
    }

    local section_count = #sections
    local header_size = 64
    local dir_size = section_count * 16
    local data_start = align4(header_size + dir_size)

    -- Step 5: Compute section offsets
    local current_offset = data_start
    for _, sect in ipairs(sections) do
        sect.offset = current_offset
        sect.size   = #sect.data
        current_offset = align4(current_offset + #sect.data)
    end
    local total_size = current_offset

    -- Step 6: Flags
    local flags = 0
    if #json_rec_bin > 0  then flags = flags + 1 end
    if event_count > 0    then flags = flags + 2 end
    if bitmask_count > 0  then flags = flags + 4 end

    -- Step 7: Build header (64 bytes)
    local header = table.concat({
        pack_u32(0x43544231),            -- magic "CTB1"
        pack_u16(1),                      -- version_major
        pack_u16(1),                      -- version_minor (bumped for new format)
        pack_u32(flags),                  -- flags
        pack_u32(total_size),             -- total_image_size
        pack_u32(0),                      -- checksum (patched later)
        pack_u16(section_count),          -- section_count
        pack_u16(self.node_builder:get_array_size()),
        pack_u16(self.node_builder:get_total_nodes()),
        pack_u16(self.link_builder:get_link_table_size()),
        pack_u16(fb.main_indexer:get_count()),
        pack_u16(fb.one_shot_indexer:get_count()),
        pack_u16(fb.boolean_indexer:get_count()),
        pack_u16(event_count),
        pack_u16(bitmask_count),
        pack_u16(kb_count),
        pack_u16(json_rec_count),
        pack_u16(json_ctrl_count),
        pack_u32(json_str_size),
        string.rep("\0", 16),             -- reserved
    })

    assert(#header == 64, "Header size mismatch: " .. #header)

    -- Step 8: Build section directory (16 bytes per section)
    local dir_parts = {}
    for _, sect in ipairs(sections) do
        dir_parts[#dir_parts + 1] = pack_u32(sect.type)
        dir_parts[#dir_parts + 1] = pack_u32(sect.offset)
        dir_parts[#dir_parts + 1] = pack_u32(sect.size)
        dir_parts[#dir_parts + 1] = pack_u16(sect.count)
        dir_parts[#dir_parts + 1] = pack_u16(sect.esize)
    end
    local dir_bin = table.concat(dir_parts)

    -- Step 9: Assemble full image
    local image_parts = { header, dir_bin }

    local after_dir = header_size + dir_size
    if after_dir < data_start then
        image_parts[#image_parts + 1] = string.rep("\0", data_start - after_dir)
    end

    for si, sect in ipairs(sections) do
        image_parts[#image_parts + 1] = sect.data
        local next_offset
        if si < #sections then
            next_offset = sections[si + 1].offset
        else
            next_offset = total_size
        end
        local end_of_data = sect.offset + sect.size
        if end_of_data < next_offset then
            image_parts[#image_parts + 1] = string.rep("\0", next_offset - end_of_data)
        end
    end

    local image = table.concat(image_parts)
    assert(#image == total_size,
        string.format("Image size mismatch: got %d, expected %d", #image, total_size))

    -- Step 10: Compute CRC32 and patch (checksum at bytes 17-20, 1-based)
    local crc = crc32_compute(image)
    local patched = image:sub(1, 16) .. pack_u32(crc) .. image:sub(21)
    assert(#patched == total_size)

    -- Step 11: Write output files
    os.execute("mkdir -p " .. self.output_dir)

    local ctb_path = self.output_dir .. "/" .. self.handle_name .. ".ctb"
    local f = io.open(ctb_path, "wb")
    f:write(patched)
    f:close()
    print(string.format("  Generated: %s (%d bytes)", ctb_path, #patched))

    if self.emit_c_header then
        self:_write_c_header(patched)
    end

    -- Emit blackboard offset header if blackboard is defined
    if self.handle.blackboard then
        self:_write_bb_offset_header()
    end

    return #patched
end

-- =========================================================================
-- C header emitter
-- =========================================================================

function BinaryImageEmitter:_write_c_header(image_data)
    local path  = self.output_dir .. "/" .. self.handle_name .. "_image.h"
    local guard = self.handle_name:upper() .. "_IMAGE_H"

    local lines = {}
    lines[#lines + 1] = "/* Auto-generated ChainTree binary image */"
    lines[#lines + 1] = "#ifndef " .. guard
    lines[#lines + 1] = "#define " .. guard
    lines[#lines + 1] = ""
    lines[#lines + 1] = "#include <stdint.h>"
    lines[#lines + 1] = ""
    lines[#lines + 1] = string.format("#define %s_IMAGE_SIZE %d", self.handle_name:upper(), #image_data)
    lines[#lines + 1] = ""
    lines[#lines + 1] = string.format("const uint8_t %s_image[%d] = {", self.handle_name, #image_data)

    for i = 1, #image_data, 16 do
        local chunk = {}
        for j = i, math.min(i + 15, #image_data) do
            chunk[#chunk + 1] = string.format("0x%02x", string.byte(image_data, j))
        end
        local line = "    " .. table.concat(chunk, ", ")
        if i + 16 <= #image_data then line = line .. "," end
        lines[#lines + 1] = line
    end

    lines[#lines + 1] = "};"
    lines[#lines + 1] = ""
    lines[#lines + 1] = "#endif /* " .. guard .. " */"
    lines[#lines + 1] = ""

    local fh = io.open(path, "w")
    fh:write(table.concat(lines, "\n"))
    fh:close()
    print("  Generated: " .. path)
end

return BinaryImageEmitter