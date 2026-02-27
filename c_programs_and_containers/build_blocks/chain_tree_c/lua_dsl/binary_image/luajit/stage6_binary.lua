--[[
  stage6_binary.lua - ChainTree Binary Image Emitter
  
  Replaces stage6_codegen.lua when generating .ctb binary images.
  Consumes identical stage 1-5 output. Produces a single binary file
  that the C runtime (ct_runtime.c) loads directly.
  
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

local function pack_u8(val)
    return string.char(bit.band(val, 0xFF))
end

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

local function pad_to_align4(data)
    local rem = #data % 4
    if rem == 0 then return data end
    return data .. string.rep("\0", 4 - rem)
end

-- =========================================================================
-- String Pool Builder (deduplicated)
-- =========================================================================

local StringPool = {}
StringPool.__index = StringPool

function StringPool.new()
    local self = setmetatable({}, StringPool)
    self.strings = {}     -- ordered list of unique strings
    self.offsets = {}      -- str -> offset
    self.next_offset = 0
    return self
end

function StringPool:add(str)
    if self.offsets[str] then return self.offsets[str] end
    local offset = self.next_offset
    self.offsets[str] = offset
    self.strings[#self.strings + 1] = str
    self.next_offset = offset + #str + 1  -- +1 for null terminator
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
local SECT_MFHT = 0x0003
local SECT_OSHT = 0x0004
local SECT_BFHT = 0x0005
local SECT_FSTR = 0x0006
local SECT_JREC = 0x0007
local SECT_JCTL = 0x0008
local SECT_JSTR = 0x0009
local SECT_EVNT = 0x000A
local SECT_BMSK = 0x000B
local SECT_KBIN = 0x000C
local SECT_KBAL = 0x000D
local SECT_GSTR = 0x000E

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
    self.main_function_usage = opts.main_function_usage
    self.emit_c_header = opts.emit_c_header or false
    return self
end

-- =========================================================================
-- Hash table building
-- =========================================================================

function BinaryImageEmitter:_build_hash_table(indexer)
    -- Build { hash, original_index, name } for each function
    local entries = {}
    local all_funcs = indexer:get_all_functions()
    
    for i, name in ipairs(all_funcs) do
        local hash = fnv1a(name)
        entries[#entries + 1] = {
            hash = hash,
            orig_index = i - 1,  -- 0-based
            name = name,
        }
    end
    
    -- Check for collisions
    local hash_set = {}
    for _, e in ipairs(entries) do
        if hash_set[e.hash] then
            error(string.format(
                "FNV-1a collision in %s: '%s' and '%s' both hash to 0x%08X",
                indexer.name, hash_set[e.hash], e.name, e.hash))
        end
        hash_set[e.hash] = e.name
    end
    
    -- Sort by hash value
    table.sort(entries, function(a, b) return a.hash < b.hash end)
    
    -- Build remap: orig_index -> sorted_position
    local remap = {}
    for sorted_pos, e in ipairs(entries) do
        remap[e.orig_index] = sorted_pos - 1  -- 0-based sorted position
    end
    
    return entries, remap
end

-- =========================================================================
-- Node array with remapped function indices
-- =========================================================================

function BinaryImageEmitter:_build_node_array(main_remap, one_shot_remap, boolean_remap)
    local array_size = self.node_builder:get_array_size()
    local parts = {}
    
    for i = 0, array_size - 1 do
        local ltree_name = self.node_builder:get_node_by_index(i)
        
        if not ltree_name then
            -- Gap / filtered node
            parts[#parts + 1] = pack_u16(i)            -- node_index
            parts[#parts + 1] = pack_u16(0xFFFF)       -- parent_index
            parts[#parts + 1] = pack_u16(0)            -- depth
            parts[#parts + 1] = pack_u16(0)            -- link_start
            parts[#parts + 1] = pack_u16(0)            -- link_count
            parts[#parts + 1] = pack_u16(main_remap[0] or 0)     -- main (CFL_NULL)
            parts[#parts + 1] = pack_u16(one_shot_remap[0] or 0) -- init
            parts[#parts + 1] = pack_u16(boolean_remap[0] or 0)  -- aux
            parts[#parts + 1] = pack_u16(one_shot_remap[0] or 0) -- term
            parts[#parts + 1] = pack_u16(0xFFFF)       -- node_data_id
        else
            local node_data = self.handle:get_node_data(ltree_name)
            local functions = self.handle:get_node_functions(ltree_name)
            
            -- Get original indices and remap
            local main_orig = self.function_builder.main_indexer:get_index(functions.main)
            local init_orig = self.function_builder.one_shot_indexer:get_index(functions.init)
            local aux_orig = self.function_builder.boolean_indexer:get_index(functions.aux)
            local term_orig = self.function_builder.one_shot_indexer:get_index(functions.term)
            
            local main_idx = main_remap[main_orig] or 0
            local init_idx = one_shot_remap[init_orig] or 0
            local aux_idx = boolean_remap[aux_orig] or 0
            local term_idx = one_shot_remap[term_orig] or 0
            
            local link_info = self.link_builder:get_node_link_info(ltree_name)
            local link_count = link_info.link_count
            
            local node_dict = node_data and node_data.node_dict or {}
            local auto_start = false
            if type(node_dict) == "table" then
                auto_start = node_dict.auto_start and true or false
            end
            
            local packed_lc = bit.band(link_count, 0x7FFF)
            if auto_start then packed_lc = bit.bor(packed_lc, 0x8000) end
            
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
-- Hash table binary (sorted uint32_t array)
-- =========================================================================

local function hash_table_to_binary(entries)
    local parts = {}
    for _, e in ipairs(entries) do
        parts[#parts + 1] = pack_u32(e.hash)
    end
    return table.concat(parts)
end

-- =========================================================================
-- Function name strings (in sorted order for each table)
-- =========================================================================

local function func_names_to_binary(main_entries, one_shot_entries, boolean_entries)
    local parts = {}
    for _, e in ipairs(main_entries) do
        parts[#parts + 1] = e.name .. "\0"
    end
    for _, e in ipairs(one_shot_entries) do
        parts[#parts + 1] = e.name .. "\0"
    end
    for _, e in ipairs(boolean_entries) do
        parts[#parts + 1] = e.name .. "\0"
    end
    return table.concat(parts)
end

-- =========================================================================
-- JSON data sections
-- =========================================================================

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

function BinaryImageEmitter:_build_json_sections()
    if not self.data_encoder then
        return "", "", ""
    end
    
    local enc = self.data_encoder.encoder
    
    -- Records: each is 8 bytes (uint32 type + uint32 value)
    local rec_parts = {}
    for _, rec in ipairs(enc.records) do
        rec_parts[#rec_parts + 1] = pack_u32(rec[1])  -- type
        rec_parts[#rec_parts + 1] = pack_u32(rec[2])  -- value
    end
    local records_bin = table.concat(rec_parts)
    
    -- Controls: each is 8 bytes (uint32 start + uint32 count)
    local ctrl_parts = {}
    for _, ctrl in ipairs(enc.record_controls) do
        ctrl_parts[#ctrl_parts + 1] = pack_u32(ctrl.start_position)
        ctrl_parts[#ctrl_parts + 1] = pack_u32(ctrl.num_records)
    end
    local controls_bin = table.concat(ctrl_parts)
    
    -- Strings: raw packed null-terminated
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
    
    -- Sort by value (index)
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
    
    -- Sort by bit position (value)
    local sorted = {}
    for name, bit_pos in pairs(bitmasks) do
        sorted[#sorted + 1] = { name = name, bit_pos = bit_pos }
    end
    table.sort(sorted, function(a, b) return a.bit_pos < b.bit_pos end)
    
    local parts = {}
    for _, entry in ipairs(sorted) do
        local offset = string_pool:add(entry.name)
        parts[#parts + 1] = pack_u32(offset)           -- string_pool_offset
        parts[#parts + 1] = pack_u8(entry.bit_pos)     -- bit_position
        parts[#parts + 1] = string.rep("\0", 3)         -- reserved
    end
    
    return table.concat(parts), count
end

-- =========================================================================
-- KB info and alias tables
-- =========================================================================

local function table_len(t)
    local n = 0; for _ in pairs(t) do n = n + 1 end; return n
end

function BinaryImageEmitter:_filter_executable_kbs(all_kb_names)
    local executable = {}
    for _, kb in ipairs(all_kb_names) do
        if not kb:match("_test_functions$") and kb ~= "complete_functions_kb" then
            executable[#executable + 1] = kb
        end
    end
    return executable
end

function BinaryImageEmitter:_build_kb_sections(string_pool)
    local kb_names = self:_filter_executable_kbs(self.handle:get_kb_names())
    
    local kb_parts = {}
    local alias_parts = {}
    local total_aliases = 0
    
    for _, kb_name in ipairs(kb_names) do
        local name_offset = string_pool:add(kb_name)
        local start_idx, end_idx = self.node_builder:get_kb_range(kb_name)
        local node_count = end_idx - start_idx
        local root_node_index = start_idx
        
        local max_depth = 0
        for j = start_idx, end_idx - 1 do
            local ltree_name = self.node_builder:get_node_by_index(j)
            if ltree_name then
                local d = self.node_builder:get_node_depth(ltree_name)
                if d > max_depth then max_depth = d end
            end
        end
        
        local memory_factor = self.handle:get_kb_metadata_value(kb_name, "node_memory_factor", 10)
        
        local aliases = self.handle:get_kb_node_aliases(kb_name)
        local alias_count = table_len(aliases)
        local alias_start = total_aliases
        
        -- KB info entry: 24 bytes
        kb_parts[#kb_parts + 1] = pack_u32(name_offset)
        kb_parts[#kb_parts + 1] = pack_u16(root_node_index)
        kb_parts[#kb_parts + 1] = pack_u16(start_idx)
        kb_parts[#kb_parts + 1] = pack_u16(node_count)
        kb_parts[#kb_parts + 1] = pack_u16(max_depth)
        kb_parts[#kb_parts + 1] = pack_u16(memory_factor)
        kb_parts[#kb_parts + 1] = pack_u16(alias_start)
        kb_parts[#kb_parts + 1] = pack_u16(alias_count)
        kb_parts[#kb_parts + 1] = string.rep("\0", 6)  -- reserved
        
        -- Alias entries
        if alias_count > 0 then
            -- Sort aliases by name for determinism
            local sorted_aliases = {}
            for aname, aindex in pairs(aliases) do
                sorted_aliases[#sorted_aliases + 1] = { name = aname, index = aindex }
            end
            table.sort(sorted_aliases, function(a, b) return a.name < b.name end)
            
            for _, a in ipairs(sorted_aliases) do
                local aname_offset = string_pool:add(a.name)
                alias_parts[#alias_parts + 1] = pack_u32(aname_offset)
                alias_parts[#alias_parts + 1] = pack_u16(a.index)
                alias_parts[#alias_parts + 1] = pack_u16(0)  -- reserved
            end
            total_aliases = total_aliases + alias_count
        end
    end
    
    return table.concat(kb_parts), table.concat(alias_parts),
           #kb_names, total_aliases
end

-- =========================================================================
-- Main emit function
-- =========================================================================

function BinaryImageEmitter:emit()
    print("\n  Building hash tables...")
    
    -- Step 1: Build sorted hash tables with remapping
    local main_entries, main_remap = self:_build_hash_table(self.function_builder.main_indexer)
    local os_entries, os_remap = self:_build_hash_table(self.function_builder.one_shot_indexer)
    local bool_entries, bool_remap = self:_build_hash_table(self.function_builder.boolean_indexer)
    
    print(string.format("    Main: %d functions", #main_entries))
    print(string.format("    One-shot: %d functions", #os_entries))
    print(string.format("    Boolean: %d functions", #bool_entries))
    
    -- Step 2: Build all binary sections
    print("  Building sections...")
    
    local node_bin = self:_build_node_array(main_remap, os_remap, bool_remap)
    local link_bin = self:_build_link_table()
    local main_hash_bin = hash_table_to_binary(main_entries)
    local os_hash_bin = hash_table_to_binary(os_entries)
    local bool_hash_bin = hash_table_to_binary(bool_entries)
    local func_names_bin = func_names_to_binary(main_entries, os_entries, bool_entries)
    local json_rec_bin, json_ctrl_bin, json_str_bin = self:_build_json_sections()
    
    -- String pool for events, bitmasks, KB info
    local string_pool = StringPool.new()
    local event_bin, event_count = self:_build_event_section(string_pool)
    local bitmask_bin, bitmask_count = self:_build_bitmask_section(string_pool)
    local kb_info_bin, kb_alias_bin, kb_count, alias_count = self:_build_kb_sections(string_pool)
    local string_pool_bin = string_pool:to_binary()
    
    -- Step 3: Build section directory and compute offsets
    local sections = {
        { type = SECT_NODE, data = node_bin, count = self.node_builder:get_array_size(), esize = 20 },
        { type = SECT_LINK, data = link_bin, count = self.link_builder:get_link_table_size(), esize = 2 },
        { type = SECT_MFHT, data = main_hash_bin, count = #main_entries, esize = 4 },
        { type = SECT_OSHT, data = os_hash_bin, count = #os_entries, esize = 4 },
        { type = SECT_BFHT, data = bool_hash_bin, count = #bool_entries, esize = 4 },
        { type = SECT_FSTR, data = func_names_bin, count = #main_entries + #os_entries + #bool_entries, esize = 0 },
        { type = SECT_JREC, data = json_rec_bin, count = self.data_encoder and #self.data_encoder.encoder.records or 0, esize = 8 },
        { type = SECT_JCTL, data = json_ctrl_bin, count = self.data_encoder and #self.data_encoder.encoder.record_controls or 0, esize = 8 },
        { type = SECT_JSTR, data = json_str_bin, count = 0, esize = 0 },
        { type = SECT_EVNT, data = event_bin, count = event_count, esize = 4 },
        { type = SECT_BMSK, data = bitmask_bin, count = bitmask_count, esize = 8 },
        { type = SECT_KBIN, data = kb_info_bin, count = kb_count, esize = 24 },
        { type = SECT_KBAL, data = kb_alias_bin, count = alias_count, esize = 8 },
        { type = SECT_GSTR, data = string_pool_bin, count = 0, esize = 0 },
    }
    
    local section_count = #sections
    local header_size = 64
    local dir_size = section_count * 16
    local data_start = align4(header_size + dir_size)
    
    -- Compute section offsets
    local current_offset = data_start
    for _, sect in ipairs(sections) do
        sect.offset = current_offset
        sect.size = #sect.data
        current_offset = align4(current_offset + sect.size)
    end
    
    local total_size = current_offset
    
    -- Step 4: Build flags
    local flags = 0
    if #json_rec_bin > 0 then flags = bit.bor(flags, 1) end    -- has_node_data
    if event_count > 0 then flags = bit.bor(flags, 2) end       -- has_events
    if bitmask_count > 0 then flags = bit.bor(flags, 4) end     -- has_bitmasks
    
    -- Step 5: Build header (64 bytes, checksum = 0 initially)
    local json_rec_count = self.data_encoder and #self.data_encoder.encoder.records or 0
    local json_ctrl_count = self.data_encoder and #self.data_encoder.encoder.record_controls or 0
    local json_str_size = #json_str_bin
    
    local header = table.concat({
        pack_u32(0x43544231),           -- magic "CTB1"
        pack_u16(1),                     -- version_major
        pack_u16(0),                     -- version_minor
        pack_u32(flags),                 -- flags
        pack_u32(total_size),            -- total_image_size
        pack_u32(0),                     -- checksum (patched later)
        pack_u16(section_count),         -- section_count
        pack_u16(self.node_builder:get_array_size()),  -- node_count
        pack_u16(self.node_builder:get_total_nodes()), -- node_active_count
        pack_u16(self.link_builder:get_link_table_size()), -- link_table_size
        pack_u16(#main_entries),         -- main_func_count
        pack_u16(#os_entries),           -- one_shot_func_count
        pack_u16(#bool_entries),         -- boolean_func_count
        pack_u16(event_count),           -- event_count
        pack_u16(bitmask_count),         -- bitmask_count
        pack_u16(kb_count),              -- kb_count
        pack_u16(json_rec_count),        -- json_records_count
        pack_u16(json_ctrl_count),       -- json_controls_count
        pack_u32(json_str_size),         -- json_strings_size
        string.rep("\0", 16),            -- reserved
    })
    
    assert(#header == 64, "Header size mismatch: " .. #header)
    
    -- Step 6: Build section directory
    local dir_parts = {}
    for _, sect in ipairs(sections) do
        dir_parts[#dir_parts + 1] = pack_u32(sect.type)
        dir_parts[#dir_parts + 1] = pack_u32(sect.offset)
        dir_parts[#dir_parts + 1] = pack_u32(sect.size)
        dir_parts[#dir_parts + 1] = pack_u16(sect.count)
        dir_parts[#dir_parts + 1] = pack_u16(sect.esize)
    end
    local dir_bin = table.concat(dir_parts)
    
    -- Step 7: Assemble full image
    local image_parts = { header, dir_bin }
    
    -- Pad between directory and first section
    local after_dir = header_size + dir_size
    if after_dir < data_start then
        image_parts[#image_parts + 1] = string.rep("\0", data_start - after_dir)
    end
    
    -- Write sections with inter-section padding
    for si, sect in ipairs(sections) do
        image_parts[#image_parts + 1] = sect.data
        -- Pad to next section's offset (or total size for last)
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
    assert(#image == total_size, string.format("Image size mismatch: got %d, expected %d", #image, total_size))
    
    -- Step 8: Compute CRC32 and patch header
    local crc = crc32_compute(image)  -- checksum field is already 0
    local patched = image:sub(1, 16) .. pack_u32(crc) .. image:sub(21)
    assert(#patched == total_size)
    
    -- Step 9: Write output files
    os.execute("mkdir -p " .. self.output_dir)
    
    -- Write .ctb binary
    local ctb_path = self.output_dir .. "/" .. self.handle_name .. ".ctb"
    local f = io.open(ctb_path, "wb")
    f:write(patched)
    f:close()
    print(string.format("  Generated: %s (%d bytes)", ctb_path, #patched))
    
    -- Optionally write .h C array
    if self.emit_c_header then
        self:_write_c_header(patched)
    end
    
    return #patched
end

function BinaryImageEmitter:_write_c_header(image_data)
    local path = self.output_dir .. "/" .. self.handle_name .. "_image.h"
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
    
    local f = io.open(path, "w")
    f:write(table.concat(lines, "\n"))
    f:close()
    print("  Generated: " .. path)
end

return BinaryImageEmitter