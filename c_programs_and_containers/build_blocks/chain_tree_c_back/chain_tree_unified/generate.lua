#!/usr/bin/env luajit
--[[
ChainTree Binary Generator
Converts YAML configuration to binary format for embedded systems.

Usage: luajit generate.lua <input.yaml> <output_dir> [options]

Options:
  --name=<name>    Base name for output files (default: from yaml filename)
  --verbose        Enable verbose output
]]

-- Disable JIT for deterministic behavior
if jit then
    jit.off()
end

local yaml_parser = require("lib.yaml_parser")
local binary_writer = require("lib.binary_writer")
local header_gen = require("lib.header_gen")
local fnv1a = require("lib.fnv1a")
local crc32 = require("lib.crc32")

-- Configuration
local CONFIG = {
    MAGIC = "CTRB",
    VERSION = 0x0001,
    CFL_NULL_HASH = 0x00000000,
}

--------------------------------------------------------------------------------
-- Utility Functions
--------------------------------------------------------------------------------

local function printf(fmt, ...)
    print(string.format(fmt, ...))
end

local function fatal(fmt, ...)
    io.stderr:write(string.format("ERROR: " .. fmt .. "\n", ...))
    os.exit(1)
end

local function parse_args(args)
    local opts = {
        verbose = false,
        name = nil,
    }
    local positional = {}
    
    for _, arg in ipairs(args) do
        if arg:match("^%-%-verbose$") then
            opts.verbose = true
        elseif arg:match("^%-%-name=") then
            opts.name = arg:match("^%-%-name=(.+)$")
        elseif arg:match("^%-") then
            fatal("Unknown option: %s", arg)
        else
            table.insert(positional, arg)
        end
    end
    
    if #positional < 2 then
        fatal("Usage: luajit generate.lua <input.yaml> <output_dir> [options]")
    end
    
    opts.input_file = positional[1]
    opts.output_dir = positional[2]
    
    if not opts.name then
        -- Extract base name from input file
        opts.name = opts.input_file:match("([^/\\]+)%.yaml$") or "chaintree"
    end
    
    return opts
end

local function ensure_dir(path)
    os.execute(string.format("mkdir -p '%s'", path))
end

--------------------------------------------------------------------------------
-- Data Processing
--------------------------------------------------------------------------------

local function compute_function_hash(name, suffix)
    if name == "CFL_NULL" then
        return CONFIG.CFL_NULL_HASH
    end
    -- Hash the typed name (lowercase with suffix)
    local typed_name = string.lower(name) .. "_" .. suffix
    return fnv1a.hash(typed_name)
end

local function build_function_tables(yaml_data, opts)
    local tables = {
        main = {},           -- {name, hash, index}
        one_shot = {},
        boolean = {},
        main_by_name = {},   -- name -> index
        one_shot_by_name = {},
        boolean_by_name = {},
    }
    
    -- Collect all functions from nodes
    local main_set = {CFL_NULL = true}
    local one_shot_set = {CFL_NULL = true}
    local boolean_set = {CFL_NULL = true}
    
    for ltree_name, node_data in pairs(yaml_data) do
        if type(node_data) == "table" and node_data.label_dict then
            local ld = node_data.label_dict
            
            if ld.main_function_name and ld.main_function_name ~= "CFL_NULL" then
                main_set[ld.main_function_name] = true
            end
            if ld.initialization_function_name and ld.initialization_function_name ~= "CFL_NULL" then
                one_shot_set[ld.initialization_function_name] = true
            end
            if ld.termination_function_name and ld.termination_function_name ~= "CFL_NULL" then
                one_shot_set[ld.termination_function_name] = true
            end
            if ld.aux_function_name and ld.aux_function_name ~= "CFL_NULL" then
                boolean_set[ld.aux_function_name] = true
            end
        end
    end
    
    -- Build sorted arrays (CFL_NULL always first)
    local function build_array(set, suffix, array, by_name)
        -- CFL_NULL is always index 0
        table.insert(array, {
            name = "CFL_NULL",
            hash = CONFIG.CFL_NULL_HASH,
            index = 0
        })
        by_name["CFL_NULL"] = 0
        
        -- Sort remaining functions
        local sorted = {}
        for name in pairs(set) do
            if name ~= "CFL_NULL" then
                table.insert(sorted, name)
            end
        end
        table.sort(sorted)
        
        for i, name in ipairs(sorted) do
            local hash = compute_function_hash(name, suffix)
            table.insert(array, {
                name = name,
                hash = hash,
                index = i
            })
            by_name[name] = i
        end
    end
    
    build_array(main_set, "main", tables.main, tables.main_by_name)
    build_array(one_shot_set, "one_shot", tables.one_shot, tables.one_shot_by_name)
    build_array(boolean_set, "boolean", tables.boolean, tables.boolean_by_name)
    
    if opts.verbose then
        printf("  Main functions: %d", #tables.main)
        printf("  One-shot functions: %d", #tables.one_shot)
        printf("  Boolean functions: %d", #tables.boolean)
    end
    
    return tables
end

local function build_event_table(yaml_data, opts)
    local events = {}
    local event_data = yaml_data["kb.event_string_table_kb"]
    
    if event_data and event_data.node_dict then
        for event_name, index in pairs(event_data.node_dict) do
            table.insert(events, {
                name = event_name,
                hash = fnv1a.hash(event_name),
                index = index
            })
        end
        table.sort(events, function(a, b) return a.index < b.index end)
    end
    
    if opts.verbose then
        printf("  Events: %d", #events)
    end
    
    return events
end

local function build_bitmask_table(yaml_data, opts)
    local bitmasks = {}
    local bitmask_data = yaml_data["kb.bitmask_table_kb"]
    
    if bitmask_data and bitmask_data.node_dict then
        for name, bit_num in pairs(bitmask_data.node_dict) do
            table.insert(bitmasks, {
                name = name,
                hash = fnv1a.hash(name),
                bit = bit_num
            })
        end
        table.sort(bitmasks, function(a, b) return a.bit < b.bit end)
    end
    
    if opts.verbose then
        printf("  Bitmasks: %d", #bitmasks)
    end
    
    return bitmasks
end

local function is_metadata_kb(kb_name)
    -- Filter out function mapping KBs
    return kb_name:match("_test_functions$") or 
           kb_name == "complete_functions_kb" or
           kb_name == "event_string_table_kb" or
           kb_name == "bitmask_table_kb"
end

local function build_node_tables(yaml_data, func_tables, opts)
    local result = {
        nodes = {},              -- Array of node data, indexed by original index
        ltree_to_index = {},     -- ltree_name -> index
        link_table = {},         -- Flat array of child indices
        kb_table = {},           -- KB info array
        kb_aliases = {},         -- All aliases flattened
        max_index = 0,
        main_func_usage = {},    -- function_index -> usage count
    }
    
    -- Initialize usage counts
    for i = 0, #func_tables.main - 1 do
        result.main_func_usage[i] = 0
    end
    
    -- Get ltree_to_index from yaml
    local ltree_to_index = yaml_data.ltree_to_index or {}
    result.ltree_to_index = ltree_to_index
    
    -- Find max index and build reverse mapping
    local index_to_ltree = {}
    for ltree_name, index in pairs(ltree_to_index) do
        index_to_ltree[index] = ltree_name
        if index > result.max_index then
            result.max_index = index
        end
    end
    
    -- Metadata node labels to filter
    local metadata_labels = {
        virtual_functions = true,
        complete_functions = true,
        main_functions = true,
        one_shot_functions = true,
        boolean_functions = true,
    }
    
    -- Build node array (sparse, preserving original indices)
    for i = 0, result.max_index do
        local ltree_name = index_to_ltree[i]
        local node_data = ltree_name and yaml_data[ltree_name]
        
        if node_data and type(node_data) == "table" then
            -- Check if metadata node
            if metadata_labels[node_data.label] then
                result.nodes[i] = nil  -- Skip metadata nodes
            else
                -- Get function indices
                local ld = node_data.label_dict or {}
                local main_idx = func_tables.main_by_name[ld.main_function_name or "CFL_NULL"] or 0
                local init_idx = func_tables.one_shot_by_name[ld.initialization_function_name or "CFL_NULL"] or 0
                local term_idx = func_tables.one_shot_by_name[ld.termination_function_name or "CFL_NULL"] or 0
                local aux_idx = func_tables.boolean_by_name[ld.aux_function_name or "CFL_NULL"] or 0
                
                -- Track main function usage
                result.main_func_usage[main_idx] = (result.main_func_usage[main_idx] or 0) + 1
                
                -- Get parent index
                local parent_ltree = ld.parent_ltree_name
                local parent_idx = 0xFFFF
                if parent_ltree and ltree_to_index[parent_ltree] then
                    parent_idx = ltree_to_index[parent_ltree]
                end
                
                -- Calculate depth
                local parts = {}
                for part in ltree_name:gmatch("[^.]+") do
                    table.insert(parts, part)
                end
                local depth = math.max(0, math.floor((#parts - 3) / 2))
                
                -- Get auto_start flag
                local node_dict = node_data.node_dict or {}
                local auto_start = node_dict.auto_start or false
                
                -- Build link info (will be filled in second pass)
                local children = ld.links or {}
                
                result.nodes[i] = {
                    node_index = i,
                    parent_index = parent_idx,
                    depth = depth,
                    link_start = 0,  -- Filled later
                    link_count = #children,
                    auto_start = auto_start,
                    main_function_index = main_idx,
                    init_function_index = init_idx,
                    aux_function_index = aux_idx,
                    term_function_index = term_idx,
                    node_data_id = 0xFFFF,  -- TODO: node data encoding
                    children = children,  -- Temporary, for link building
                    ltree_name = ltree_name,
                }
            end
        end
    end
    
    -- Build link table
    for i = 0, result.max_index do
        local node = result.nodes[i]
        if node then
            node.link_start = #result.link_table
            for _, child_ltree in ipairs(node.children or {}) do
                local child_idx = ltree_to_index[child_ltree]
                if child_idx and result.nodes[child_idx] then
                    table.insert(result.link_table, child_idx)
                end
            end
            node.link_count = #result.link_table - node.link_start
            node.children = nil  -- Clean up temporary data
        end
    end
    
    -- Build KB table
    local kb_metadata = yaml_data.kb_metadata or {}
    local kb_names = {}
    
    for ltree_name in pairs(yaml_data) do
        if type(ltree_name) == "string" and ltree_name:match("^kb%.") then
            local parts = {}
            for part in ltree_name:gmatch("[^.]+") do
                table.insert(parts, part)
            end
            if #parts >= 2 then
                local kb_name = parts[2]
                if not is_metadata_kb(kb_name) then
                    kb_names[kb_name] = true
                end
            end
        end
    end
    
    -- Sort KB names and build info
    local sorted_kbs = {}
    for kb_name in pairs(kb_names) do
        table.insert(sorted_kbs, kb_name)
    end
    table.sort(sorted_kbs)
    
    local alias_offset = 0
    for _, kb_name in ipairs(sorted_kbs) do
        local meta = kb_metadata[kb_name] or {}
        local aliases = meta.node_aliases or {}
        
        -- Find root and node range for this KB
        local min_idx, max_idx = math.huge, 0
        local root_idx = 0xFFFF
        local max_depth = 0
        
        for ltree_name, idx in pairs(ltree_to_index) do
            if ltree_name:match("^kb%." .. kb_name .. "%.") or ltree_name == "kb." .. kb_name then
                if idx < min_idx then min_idx = idx end
                if idx > max_idx then max_idx = idx end
                
                local node = result.nodes[idx]
                if node then
                    if node.depth > max_depth then max_depth = node.depth end
                    -- Root is the node with smallest index in this KB
                    if root_idx == 0xFFFF or idx < root_idx then
                        root_idx = idx
                    end
                end
            end
        end
        
        if min_idx == math.huge then min_idx = 0 end
        
        local kb_info = {
            kb_name = kb_name,
            kb_name_hash = fnv1a.hash(kb_name),
            root_node_index = root_idx,
            start_index = min_idx,
            node_count = (max_idx >= min_idx) and (max_idx - min_idx + 1) or 0,
            max_depth = max_depth,
            memory_factor = meta.node_memory_factor or 10,
            alias_count = 0,
            aliases_offset = 0,
        }
        
        -- Add aliases
        if next(aliases) then
            kb_info.alias_count = 0
            kb_info.aliases_offset = alias_offset
            
            for alias_name, node_idx in pairs(aliases) do
                table.insert(result.kb_aliases, {
                    alias_hash = fnv1a.hash(alias_name),
                    node_index = node_idx,
                })
                kb_info.alias_count = kb_info.alias_count + 1
                alias_offset = alias_offset + 1
            end
        end
        
        table.insert(result.kb_table, kb_info)
    end
    
    if opts.verbose then
        local node_count = 0
        for _ in pairs(result.nodes) do node_count = node_count + 1 end
        printf("  Nodes: %d (max index: %d)", node_count, result.max_index)
        printf("  Link table entries: %d", #result.link_table)
        printf("  Knowledge bases: %d", #result.kb_table)
        printf("  Total aliases: %d", #result.kb_aliases)
    end
    
    return result
end

local function build_node_data(yaml_data, node_tables, func_tables, opts)
    -- TODO: Implement JSON record encoding for node_dict data
    -- For now, return empty structures
    local result = {
        records = {},
        controls = {},
        strings = "",
        node_data_ids = {},  -- node_index -> data_id
    }
    
    if opts.verbose then
        printf("  Node data records: %d", #result.records)
        printf("  Node data strings: %d bytes", #result.strings)
    end
    
    return result
end

--------------------------------------------------------------------------------
-- Binary Generation
--------------------------------------------------------------------------------

local function generate_binary(node_tables, func_tables, events, bitmasks, node_data, opts)
    local bw = binary_writer.new()
    
    -- Calculate section sizes and offsets
    local header_size = 96  -- Fixed header size (padded to 4-byte alignment)
    
    local nodes_size = (node_tables.max_index + 1) * 20  -- 20 bytes per node
    local link_table_size = #node_tables.link_table * 2  -- uint16_t each
    local kb_table_size = #node_tables.kb_table * 20     -- 20 bytes per KB info
    local kb_aliases_size = #node_tables.kb_aliases * 8  -- 8 bytes per alias
    
    local main_hashes_size = #func_tables.main * 4
    local one_shot_hashes_size = #func_tables.one_shot * 4
    local boolean_hashes_size = #func_tables.boolean * 4
    local main_usage_size = #func_tables.main * 2
    
    local event_hashes_size = #events * 4
    local bitmask_hashes_size = #bitmasks * 4
    
    local node_data_records_size = #node_data.records * 8
    local node_data_controls_size = #node_data.controls * 8
    local node_data_strings_size = #node_data.strings
    
    -- Helper to align to 4 bytes
    local function align4(offset)
        return math.ceil(offset / 4) * 4
    end
    
    -- Calculate offsets
    local offset = header_size
    
    local nodes_offset = offset
    offset = align4(offset + nodes_size)
    
    local link_table_offset = offset
    offset = align4(offset + link_table_size)
    
    local kb_table_offset = offset
    offset = align4(offset + kb_table_size)
    
    local kb_aliases_offset = offset
    offset = align4(offset + kb_aliases_size)
    
    local main_func_hashes_offset = offset
    offset = align4(offset + main_hashes_size)
    
    local one_shot_func_hashes_offset = offset
    offset = align4(offset + one_shot_hashes_size)
    
    local boolean_func_hashes_offset = offset
    offset = align4(offset + boolean_hashes_size)
    
    local main_func_usage_offset = offset
    offset = align4(offset + main_usage_size)
    
    local event_hashes_offset = offset
    offset = align4(offset + event_hashes_size)
    
    local bitmask_hashes_offset = offset
    offset = align4(offset + bitmask_hashes_size)
    
    local node_data_records_offset = offset
    offset = align4(offset + node_data_records_size)
    
    local node_data_controls_offset = offset
    offset = align4(offset + node_data_controls_size)
    
    local node_data_strings_offset = offset
    offset = align4(offset + node_data_strings_size)
    
    local data_hash_offset = offset
    local total_size = offset + 4  -- +4 for data CRC
    
    -- Write header (without CRCs first)
    bw:write_bytes(CONFIG.MAGIC)                           -- magic[4]
    bw:write_u16(CONFIG.VERSION)                           -- version
    bw:write_u16(0)                                        -- flags
    bw:write_u32(total_size)                               -- total_size
    
    -- Section offsets
    bw:write_u32(nodes_offset)
    bw:write_u32(link_table_offset)
    bw:write_u32(kb_table_offset)
    bw:write_u32(kb_aliases_offset)
    bw:write_u32(main_func_hashes_offset)
    bw:write_u32(one_shot_func_hashes_offset)
    bw:write_u32(boolean_func_hashes_offset)
    bw:write_u32(main_func_usage_offset)
    bw:write_u32(event_hashes_offset)
    bw:write_u32(bitmask_hashes_offset)
    bw:write_u32(node_data_records_offset)
    bw:write_u32(node_data_controls_offset)
    bw:write_u32(node_data_strings_offset)
    
    -- Counts
    bw:write_u16(node_tables.max_index + 1)                -- node_count
    bw:write_u16(#node_tables.link_table)                  -- link_table_size
    bw:write_u16(#node_tables.kb_table)                    -- kb_count
    bw:write_u16(#func_tables.main)                        -- main_function_count
    bw:write_u16(#func_tables.one_shot)                    -- one_shot_function_count
    bw:write_u16(#func_tables.boolean)                     -- boolean_function_count
    bw:write_u16(#events)                                  -- event_count
    bw:write_u16(#bitmasks)                                -- bitmask_count
    bw:write_u16(#node_data.records)                       -- node_data_records_count
    bw:write_u16(#node_data.controls)                      -- node_data_controls_count
    bw:write_u16(#node_data.strings)                       -- node_data_strings_size
    bw:write_u16(#node_tables.kb_aliases)                  -- total_aliases_count
    
    -- Unique ID hash
    local unique_id = string.format("ct_%08x", os.time())
    bw:write_u32(fnv1a.hash(unique_id))                    -- unique_id_hash
    
    -- Placeholder for header CRC (will be filled later)
    local header_hash_pos = bw:position()
    bw:write_u32(0)                                        -- header_hash placeholder
    
    -- Pad header to alignment
    while bw:position() < header_size do
        bw:write_u8(0)
    end
    
    -- Write nodes section
    assert(bw:position() == nodes_offset, "Nodes offset mismatch")
    for i = 0, node_tables.max_index do
        local node = node_tables.nodes[i]
        if node then
            bw:write_u16(node.node_index)
            bw:write_u16(node.parent_index)
            bw:write_u16(node.depth)
            bw:write_u16(node.link_start)
            -- Pack link_count with auto_start flag
            local packed_link_count = node.link_count
            if node.auto_start then
                packed_link_count = packed_link_count + 0x8000
            end
            bw:write_u16(packed_link_count)
            bw:write_u16(node.main_function_index)
            bw:write_u16(node.init_function_index)
            bw:write_u16(node.aux_function_index)
            bw:write_u16(node.term_function_index)
            bw:write_u16(node.node_data_id)
        else
            -- Write placeholder for filtered/gap node
            bw:write_u16(i)          -- node_index
            bw:write_u16(0xFFFF)     -- parent_index (invalid)
            bw:write_u16(0)          -- depth
            bw:write_u16(0)          -- link_start
            bw:write_u16(0)          -- link_count
            bw:write_u16(0)          -- main_function_index (CFL_NULL)
            bw:write_u16(0)          -- init_function_index (CFL_NULL)
            bw:write_u16(0)          -- aux_function_index (CFL_NULL)
            bw:write_u16(0)          -- term_function_index (CFL_NULL)
            bw:write_u16(0xFFFF)     -- node_data_id (invalid)
        end
    end
    bw:align(4)
    
    -- Write link table
    assert(bw:position() == link_table_offset, "Link table offset mismatch")
    for _, child_idx in ipairs(node_tables.link_table) do
        bw:write_u16(child_idx)
    end
    bw:align(4)
    
    -- Write KB table
    assert(bw:position() == kb_table_offset, "KB table offset mismatch")
    for _, kb in ipairs(node_tables.kb_table) do
        bw:write_u32(kb.kb_name_hash)
        bw:write_u16(kb.root_node_index)
        bw:write_u16(kb.start_index)
        bw:write_u16(kb.node_count)
        bw:write_u16(kb.max_depth)
        bw:write_u16(kb.memory_factor)
        bw:write_u16(kb.alias_count)
        bw:write_u32(kb.aliases_offset)  -- Offset into aliases array (not byte offset)
    end
    bw:align(4)
    
    -- Write KB aliases
    assert(bw:position() == kb_aliases_offset, "KB aliases offset mismatch")
    for _, alias in ipairs(node_tables.kb_aliases) do
        bw:write_u32(alias.alias_hash)
        bw:write_u16(alias.node_index)
        bw:write_u16(0)  -- padding
    end
    bw:align(4)
    
    -- Write main function hashes
    assert(bw:position() == main_func_hashes_offset, "Main func hashes offset mismatch")
    for _, func in ipairs(func_tables.main) do
        bw:write_u32(func.hash)
    end
    bw:align(4)
    
    -- Write one-shot function hashes
    assert(bw:position() == one_shot_func_hashes_offset, "One-shot func hashes offset mismatch")
    for _, func in ipairs(func_tables.one_shot) do
        bw:write_u32(func.hash)
    end
    bw:align(4)
    
    -- Write boolean function hashes
    assert(bw:position() == boolean_func_hashes_offset, "Boolean func hashes offset mismatch")
    for _, func in ipairs(func_tables.boolean) do
        bw:write_u32(func.hash)
    end
    bw:align(4)
    
    -- Write main function usage counts
    assert(bw:position() == main_func_usage_offset, "Main func usage offset mismatch")
    for i = 0, #func_tables.main - 1 do
        bw:write_u16(node_tables.main_func_usage[i] or 0)
    end
    bw:align(4)
    
    -- Write event hashes
    assert(bw:position() == event_hashes_offset, "Event hashes offset mismatch")
    for _, event in ipairs(events) do
        bw:write_u32(event.hash)
    end
    bw:align(4)
    
    -- Write bitmask hashes
    assert(bw:position() == bitmask_hashes_offset, "Bitmask hashes offset mismatch")
    for _, bitmask in ipairs(bitmasks) do
        bw:write_u32(bitmask.hash)
    end
    bw:align(4)
    
    -- Write node data records (placeholder - TODO)
    assert(bw:position() == node_data_records_offset, "Node data records offset mismatch")
    for _, record in ipairs(node_data.records) do
        bw:write_u32(record.type_tag)
        bw:write_u32(record.value)
    end
    bw:align(4)
    
    -- Write node data controls (placeholder - TODO)
    assert(bw:position() == node_data_controls_offset, "Node data controls offset mismatch")
    for _, ctrl in ipairs(node_data.controls) do
        bw:write_u32(ctrl.start_position)
        bw:write_u32(ctrl.num_records)
    end
    bw:align(4)
    
    -- Write node data strings
    assert(bw:position() == node_data_strings_offset, "Node data strings offset mismatch")
    bw:write_bytes(node_data.strings)
    bw:align(4)
    
    -- Calculate and write integrity hashes using FNV-1a
    local data = bw:get_data()
    
    -- Header hash covers bytes 0..(header_hash_pos-1) = 92 bytes (0..91)
    -- In Lua 1-indexed strings: data:sub(1, header_hash_pos) = bytes 1..92 = C bytes 0..91
    local header_hash = fnv1a.hash_bytes(data:sub(1, header_hash_pos))
    
    -- Data hash (covers bytes after header through end, excluding data hash itself)
    local data_hash = fnv1a.hash_bytes(data:sub(header_size + 1))
    
    -- Rewrite header hash
    bw:seek(header_hash_pos)
    bw:write_u32(header_hash)
    
    -- Write data hash at end
    bw:seek(data_hash_offset)
    bw:write_u32(data_hash)
    
    return bw:get_data(), unique_id
end

--------------------------------------------------------------------------------
-- Main
--------------------------------------------------------------------------------

local function main(args)
    local opts = parse_args(args)
    
    printf("ChainTree Binary Generator")
    printf("  Input: %s", opts.input_file)
    printf("  Output: %s/", opts.output_dir)
    printf("  Name: %s", opts.name)
    
    -- Load YAML
    printf("\nLoading YAML...")
    local yaml_data = yaml_parser.load_file(opts.input_file)
    
    -- Build data structures
    printf("\nBuilding data structures...")
    local func_tables = build_function_tables(yaml_data, opts)
    local events = build_event_table(yaml_data, opts)
    local bitmasks = build_bitmask_table(yaml_data, opts)
    local node_tables = build_node_tables(yaml_data, func_tables, opts)
    local node_data = build_node_data(yaml_data, node_tables, func_tables, opts)
    
    -- Generate binary
    printf("\nGenerating binary...")
    local binary_data, unique_id = generate_binary(
        node_tables, func_tables, events, bitmasks, node_data, opts
    )
    printf("  Binary size: %d bytes", #binary_data)
    printf("  Unique ID: %s", unique_id)
    
    -- Ensure output directory exists
    ensure_dir(opts.output_dir)
    
    -- Write binary file
    local bin_path = string.format("%s/%s.bin", opts.output_dir, opts.name)
    local f = io.open(bin_path, "wb")
    f:write(binary_data)
    f:close()
    printf("  Written: %s", bin_path)
    
    -- Generate header files
    printf("\nGenerating header files...")
    
    local bin_h_path = string.format("%s/%s_bin.h", opts.output_dir, opts.name)
    header_gen.generate_bin_h(bin_h_path, opts.name, binary_data)
    printf("  Written: %s", bin_h_path)
    
    local hashes_h_path = string.format("%s/%s_hashes.h", opts.output_dir, opts.name)
    header_gen.generate_hashes_h(hashes_h_path, opts.name, func_tables, events, bitmasks)
    printf("  Written: %s", hashes_h_path)
    
    local resolver_h_path = string.format("%s/%s_resolver.h", opts.output_dir, opts.name)
    header_gen.generate_resolver_h(resolver_h_path, opts.name, func_tables)
    printf("  Written: %s", resolver_h_path)
    
    printf("\n✓ Generation complete!")
end

-- Run
main(arg)