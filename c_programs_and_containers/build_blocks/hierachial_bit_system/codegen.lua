#!/usr/bin/env luajit
--[[
  codegen.lua - Code Generator for Hierarchical Bit Map
  
  Generates C headers from schema definitions.
  
  Three buffer types:
    1. OR_LATCH  - OR merge, bits latch until cleared
    2. OR_MASK   - OR merge with mask controlling participation  
    3. AND       - AND merge, no mask, no latch
    
  Output files:
    generated_<name>.h        - Main header with all definitions
    generated_<name>_data.h   - Static data tables (include in one .c file)
    
  Usage:
    luajit codegen.lua schema.lua [output_dir]
]]

local bit = require("bit")

--------------------------------------------------------------------------------
-- FNV-1a Hash (32-bit)
--------------------------------------------------------------------------------

local FNV_PRIME = 0x01000193ULL
local FNV_OFFSET = 0x811c9dc5ULL
local MASK32 = 0xFFFFFFFFULL

local function fnv1a(str)
    local hash = FNV_OFFSET
    for i = 1, #str do
        hash = bit.bxor(hash, string.byte(str, i))
        hash = bit.band(hash * FNV_PRIME, MASK32)
    end
    -- Return as positive number
    return tonumber(bit.band(hash, MASK32))
end

--------------------------------------------------------------------------------
-- Schema Processing
--------------------------------------------------------------------------------

local function process_schema(raw)
    local schema = {
        name = raw.name,
        version = raw.version,
        buffers = {},
        buffer_by_name = {},
        classes = {},
        class_by_name = {},
        nodes = {},
        node_by_path = {},
        hashes = {},
    }
    
    -- Process buffers
    for i, buf in ipairs(raw.buffers) do
        local b = {
            index = i - 1,
            name = buf.name,
            type = buf.type,
            hash = fnv1a(buf.name),
        }
        schema.buffers[i] = b
        schema.buffer_by_name[buf.name] = b
        schema.hashes[buf.name] = b.hash
    end
    
    -- Process classes
    for i, cls in ipairs(raw.classes) do
        local c = {
            index = i - 1,
            name = cls.name,
            banks = cls.banks,
            bits = cls.bits or {},
            is_auto = cls.is_auto or false,
            hash = fnv1a(cls.name),
        }
        schema.classes[i] = c
        schema.class_by_name[cls.name] = c
        schema.hashes[cls.name] = c.hash
        
        -- Hash bit names
        for buf_name, bit_list in pairs(c.bits) do
            for bit_idx, bit_name in ipairs(bit_list) do
                local full = cls.name .. "." .. buf_name .. "." .. bit_name
                schema.hashes[full] = fnv1a(full)
            end
        end
    end
    
    -- Process nodes
    local node_by_path = {}
    for i, node in ipairs(raw.nodes) do
        local cls = schema.class_by_name[node.class_name]
        if not cls then
            error("Node '" .. node.path .. "' references undefined class '" .. node.class_name .. "'")
        end
        
        local n = {
            index = i - 1,
            path = node.path,
            name = node.name,
            class = cls,
            class_index = cls.index,
            depth = node.depth,
            is_leaf = node.is_leaf,
            parent_path = node.parent_path,
            parent_index = -1,
            children = {},
            hash = fnv1a(node.path),
        }
        schema.nodes[i] = n
        node_by_path[node.path] = n
        schema.node_by_path[node.path] = n
        schema.hashes[node.path] = n.hash
    end
    
    -- Resolve parent/child relationships
    for _, node in ipairs(schema.nodes) do
        if node.parent_path then
            local parent = node_by_path[node.parent_path]
            if parent then
                node.parent_index = parent.index
                table.insert(parent.children, node)
            end
        end
    end
    
    return schema
end

--------------------------------------------------------------------------------
-- Calculate Buffer Arenas
--------------------------------------------------------------------------------

local function calculate_arenas(schema)
    local arenas = {}
    
    for i, buf in ipairs(schema.buffers) do
        local arena = {
            buffer = buf,
            total_bytes = 0,
            node_offsets = {},   -- node_index -> byte offset
            node_sizes = {},     -- node_index -> byte size
        }
        
        local offset = 0
        for _, node in ipairs(schema.nodes) do
            local bank_bits = node.class.banks[buf.name] or 0
            local bank_bytes = math.ceil(bank_bits / 8)
            
            arena.node_offsets[node.index] = offset
            arena.node_sizes[node.index] = bank_bytes
            offset = offset + bank_bytes
        end
        
        arena.total_bytes = offset
        
        -- Latch buffers need 2x storage (current + latched)
        if buf.type == "OR_LATCH" then
            arena.total_with_latch = offset * 2
        else
            arena.total_with_latch = offset
        end
        
        arenas[i] = arena
    end
    
    return arenas
end

--------------------------------------------------------------------------------
-- C Header Generation
--------------------------------------------------------------------------------

local function generate_main_header(schema, arenas)
    local lines = {}
    local function emit(fmt, ...)
        table.insert(lines, string.format(fmt, ...))
    end
    
    local guard = string.upper(schema.name) .. "_H"
    local prefix = string.upper(schema.name)
    
    emit("/**")
    emit(" * @file generated_%s.h", schema.name)
    emit(" * @brief Hierarchical Bit Map - %s v%s", schema.name, schema.version)
    emit(" *")
    emit(" * Buffer Types:")
    emit(" *   OR_LATCH - OR merge, bits latch until cleared")
    emit(" *   OR_MASK  - OR merge with mask for selective propagation")
    emit(" *   AND      - AND merge, all children must set bit")
    emit(" *")
    emit(" * Auto-generated - DO NOT EDIT")
    emit(" */")
    emit("")
    emit("#ifndef %s", guard)
    emit("#define %s", guard)
    emit("")
    emit("#include <stdint.h>")
    emit("#include <stdbool.h>")
    emit("#include <stddef.h>")
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
    emit("#define %s_BUFFER_COUNT %d", prefix, #schema.buffers)
    emit("#define %s_CLASS_COUNT %d", prefix, #schema.classes)
    emit("#define %s_NODE_COUNT %d", prefix, #schema.nodes)
    emit("")
    
    -- Buffer type enum
    emit("/* ============================================ */")
    emit("/* Buffer Types                                 */")
    emit("/* ============================================ */")
    emit("")
    emit("typedef enum {")
    emit("    %s_BUF_OR_LATCH = 0,  /* OR merge, bits latch until cleared */", prefix)
    emit("    %s_BUF_OR_MASK  = 1,  /* OR merge with mask */", prefix)
    emit("    %s_BUF_AND      = 2,  /* AND merge */", prefix)
    emit("} %s_buffer_type_t;", schema.name)
    emit("")
    
    -- Buffer indices
    emit("/* ============================================ */")
    emit("/* Buffer Indices                               */")
    emit("/* ============================================ */")
    emit("")
    emit("typedef enum {")
    for _, buf in ipairs(schema.buffers) do
        emit("    %s_BUF_%s = %d,", prefix, string.upper(buf.name), buf.index)
    end
    emit("} %s_buffer_id_t;", schema.name)
    emit("")
    
    -- Buffer type lookup
    emit("/* Buffer type for each buffer */")
    emit("static const %s_buffer_type_t %s_buffer_types[%d] = {", 
         schema.name, schema.name, #schema.buffers)
    for _, buf in ipairs(schema.buffers) do
        emit("    %s_BUF_%s,  /* %s */", prefix, string.upper(buf.type), buf.name)
    end
    emit("};")
    emit("")
    
    -- Buffer hash lookup
    emit("/* Buffer hash lookup */")
    emit("typedef struct {")
    emit("    uint32_t hash;")
    emit("    int16_t  index;")
    emit("} %s_buffer_hash_entry_t;", schema.name)
    emit("")
    
    -- Collect and sort by hash
    local buf_entries = {}
    for _, buf in ipairs(schema.buffers) do
        table.insert(buf_entries, {
            hash = fnv1a(string.upper(buf.name)),
            index = buf.index,
            name = buf.name
        })
    end
    table.sort(buf_entries, function(a, b) return a.hash < b.hash end)
    
    emit("static const %s_buffer_hash_entry_t %s_buffer_hashes[%d] = {",
         schema.name, schema.name, #buf_entries)
    for _, e in ipairs(buf_entries) do
        emit("    { 0x%08XU, %d },  /* %s */", e.hash, e.index, string.upper(e.name))
    end
    emit("};")
    emit("")
    
    emit("/* Find buffer index by name (e.g., \"ALARM_LATCHED\") */")
    emit("static inline int16_t %s_find_buffer(const char* name) {", schema.name)
    emit("    uint32_t hash = cfl_hbit_hash_string(name);")
    emit("    int lo = 0, hi = %d;", #schema.buffers - 1)
    emit("    while (lo <= hi) {")
    emit("        int mid = (lo + hi) / 2;")
    emit("        if (%s_buffer_hashes[mid].hash == hash) return %s_buffer_hashes[mid].index;", schema.name, schema.name)
    emit("        if (%s_buffer_hashes[mid].hash < hash) lo = mid + 1; else hi = mid - 1;", schema.name)
    emit("    }")
    emit("    return -1;")
    emit("}")
    emit("")
    
    -- Class indices
    emit("/* ============================================ */")
    emit("/* Class Indices                                */")
    emit("/* ============================================ */")
    emit("")
    emit("typedef enum {")
    for _, cls in ipairs(schema.classes) do
        emit("    %s_CLASS_%s = %d,", prefix, string.upper(cls.name), cls.index)
    end
    emit("} %s_class_id_t;", schema.name)
    emit("")
    
    -- Node indices
    emit("/* ============================================ */")
    emit("/* Node Indices                                 */")
    emit("/* ============================================ */")
    emit("")
    emit("typedef enum {")
    for _, node in ipairs(schema.nodes) do
        local macro_name = node.path:gsub("%.", "_"):upper()
        emit("    %s_NODE_%s = %d,", prefix, macro_name, node.index)
    end
    emit("} %s_node_id_t;", schema.name)
    emit("")
    
    -- Node hashes for runtime lookup
    emit("/* ============================================ */")
    emit("/* Node Hashes (for cfl_hbit_find_node)         */")
    emit("/* ============================================ */")
    emit("")
    for _, node in ipairs(schema.nodes) do
        local macro_name = node.path:gsub("%.", "_"):upper()
        emit("#define %s_HASH_%s 0x%08XU", prefix, macro_name, node.hash)
    end
    emit("")
    
    -- Bit definitions per class
    emit("/* ============================================ */")
    emit("/* Bit Definitions                              */")
    emit("/* ============================================ */")
    emit("")
    
    for _, cls in ipairs(schema.classes) do
        local has_bits = false
        for _, bits in pairs(cls.bits) do
            if #bits > 0 then has_bits = true break end
        end
        
        if has_bits then
            emit("/* %s */", cls.name)
            for buf_name, bit_list in pairs(cls.bits) do
                for bit_idx, bit_name in ipairs(bit_list) do
                    emit("#define %s_%s_%s_%s %d", 
                         prefix, string.upper(cls.name), 
                         string.upper(buf_name), string.upper(bit_name),
                         bit_idx - 1)
                end
            end
            emit("")
        end
    end
    
    -- Bank sizes per class
    emit("/* ============================================ */")
    emit("/* Bank Sizes (bits per buffer per class)       */")
    emit("/* ============================================ */")
    emit("")
    emit("static const uint8_t %s_bank_sizes[%d][%d] = {", 
         schema.name, #schema.classes, #schema.buffers)
    for _, cls in ipairs(schema.classes) do
        local sizes = {}
        for _, buf in ipairs(schema.buffers) do
            table.insert(sizes, tostring(cls.banks[buf.name] or 0))
        end
        emit("    { %s },  /* %s */", table.concat(sizes, ", "), cls.name)
    end
    emit("};")
    emit("")
    
    -- Bit hash tables for runtime lookup
    emit("/* ============================================ */")
    emit("/* Bit Hash Tables (for runtime lookup)         */")
    emit("/* ============================================ */")
    emit("")
    
    -- Collect all unique class/buffer bit combinations
    local bit_tables = {}
    for ci, cls in ipairs(schema.classes) do
        for bi, buf in ipairs(schema.buffers) do
            local bit_list = cls.bits[buf.name]
            if bit_list and #bit_list > 0 then
                local entries = {}
                for bit_idx, bit_name in ipairs(bit_list) do
                    local hash = fnv1a(string.upper(bit_name))
                    table.insert(entries, {
                        hash = hash,
                        name = string.upper(bit_name),
                        index = bit_idx - 1
                    })
                end
                -- Sort by hash
                table.sort(entries, function(a, b) return a.hash < b.hash end)
                table.insert(bit_tables, {
                    class_idx = ci - 1,
                    buf_idx = bi - 1,
                    class_name = cls.name,
                    buf_name = buf.name,
                    entries = entries
                })
            end
        end
    end
    
    -- Emit bit hash entry type
    emit("typedef struct {")
    emit("    uint32_t hash;")
    emit("    uint8_t  bit_index;")
    emit("} %s_bit_hash_entry_t;", schema.name)
    emit("")
    
    -- Emit tables
    for _, tbl in ipairs(bit_tables) do
        emit("static const %s_bit_hash_entry_t %s_%s_%s_bits[%d] = {",
             schema.name, schema.name, 
             string.lower(tbl.class_name), string.lower(tbl.buf_name),
             #tbl.entries)
        for _, e in ipairs(tbl.entries) do
            emit("    { 0x%08XU, %d },  /* %s */", e.hash, e.index, e.name)
        end
        emit("};")
        emit("")
    end
    
    -- Emit bit lookup info struct
    emit("typedef struct {")
    emit("    uint16_t class_idx;")
    emit("    uint16_t buf_idx;")
    emit("    uint8_t  count;")
    emit("    const %s_bit_hash_entry_t* entries;", schema.name)
    emit("} %s_bit_table_t;", schema.name)
    emit("")
    
    -- Emit lookup table
    emit("static const %s_bit_table_t %s_bit_tables[%d] = {",
         schema.name, schema.name, #bit_tables)
    for _, tbl in ipairs(bit_tables) do
        emit("    { %d, %d, %d, %s_%s_%s_bits },",
             tbl.class_idx, tbl.buf_idx, #tbl.entries,
             schema.name, string.lower(tbl.class_name), string.lower(tbl.buf_name))
    end
    emit("};")
    emit("")
    emit("#define %s_BIT_TABLE_COUNT %d", prefix, #bit_tables)
    emit("")
    
    -- Generate bit lookup function
    emit("/* Find bit index by hash for given class and buffer */")
    emit("static inline int8_t %s_find_bit_by_hash(uint16_t class_idx, uint16_t buf_idx, uint32_t hash) {", schema.name)
    emit("    for (int i = 0; i < %s_BIT_TABLE_COUNT; i++) {", prefix)
    emit("        if (%s_bit_tables[i].class_idx == class_idx && %s_bit_tables[i].buf_idx == buf_idx) {", schema.name, schema.name)
    emit("            const %s_bit_hash_entry_t* entries = %s_bit_tables[i].entries;", schema.name, schema.name)
    emit("            int lo = 0, hi = %s_bit_tables[i].count - 1;", schema.name)
    emit("            while (lo <= hi) {")
    emit("                int mid = (lo + hi) / 2;")
    emit("                if (entries[mid].hash == hash) return (int8_t)entries[mid].bit_index;")
    emit("                if (entries[mid].hash < hash) lo = mid + 1; else hi = mid - 1;")
    emit("            }")
    emit("            return -1;")
    emit("        }")
    emit("    }")
    emit("    return -1;")
    emit("}")
    emit("")
    emit("/* Find bit index by name for given class and buffer */")
    emit("static inline int8_t %s_find_bit(uint16_t class_idx, uint16_t buf_idx, const char* name) {", schema.name)
    emit("    return %s_find_bit_by_hash(class_idx, buf_idx, cfl_hbit_hash_string(name));", schema.name)
    emit("}")
    emit("")
    emit("/* Find bit index by name for a node */")
    emit("static inline int8_t %s_find_node_bit(const cfl_hbit_instance_t* inst, uint16_t node, uint16_t buf, const char* name) {", schema.name)
    emit("    uint16_t class_idx = inst->config->nodes[node].class_index;")
    emit("    return %s_find_bit(class_idx, buf, name);", schema.name)
    emit("}")
    emit("")
    
    -- Arena sizes
    emit("/* ============================================ */")
    emit("/* Arena Sizes                                  */")
    emit("/* ============================================ */")
    emit("")
    
    local total_ram = 0
    for _, arena in ipairs(arenas) do
        local buf = arena.buffer
        emit("#define %s_ARENA_%s_SIZE %d", prefix, string.upper(buf.name), arena.total_bytes)
        if buf.type == "OR_LATCH" then
            emit("#define %s_ARENA_%s_SIZE_WITH_LATCH %d", prefix, string.upper(buf.name), arena.total_with_latch)
            total_ram = total_ram + arena.total_with_latch * 2  -- current + shadow
        else
            total_ram = total_ram + arena.total_bytes * 2  -- current + shadow
        end
    end
    emit("")
    emit("#define %s_TOTAL_RAM_BYTES %d", prefix, total_ram)
    emit("")
    
    -- Node descriptors - use typedefs for compatibility with runtime
    emit("/* ============================================ */")
    emit("/* Node Descriptors                             */")
    emit("/* ============================================ */")
    emit("")
    emit("/* Common node structure (compatible with hbit_runtime.h) */")
    emit("typedef struct {")
    emit("    uint32_t path_hash;")
    emit("    uint16_t class_index;")
    emit("    int16_t  parent_index;   /* -1 if root */")
    emit("    uint16_t child_count;")
    emit("    uint16_t first_child;    /* Index of first child, or 0 */")
    emit("    uint8_t  depth;")
    emit("    uint8_t  is_leaf;")
    emit("} %s_node_t;", schema.name)
    emit("")
    emit("/* Per-buffer arena offsets for each node */")
    emit("typedef struct {")
    emit("    uint16_t offset;  /* Byte offset in arena */")
    emit("    uint8_t  size;    /* Size in bytes */")
    emit("} %s_arena_info_t;", schema.name)
    emit("")
    
    emit("#ifdef __cplusplus")
    emit("}")
    emit("#endif")
    emit("")
    emit("#endif /* %s */", guard)
    
    return table.concat(lines, "\n")
end

--------------------------------------------------------------------------------
-- Data Header Generation (include in one .c file)
--------------------------------------------------------------------------------

local function generate_data_header(schema, arenas)
    local lines = {}
    local function emit(fmt, ...)
        table.insert(lines, string.format(fmt, ...))
    end
    
    local guard = string.upper(schema.name) .. "_DATA_H"
    local prefix = string.upper(schema.name)
    
    emit("/**")
    emit(" * @file generated_%s_data.h", schema.name)
    emit(" * @brief Static data tables for %s", schema.name)
    emit(" *")
    emit(" * Include this file in exactly ONE .c file in your project.")
    emit(" *")
    emit(" * Two node tables:")
    emit(" *   - %s_nodes_by_hash: sorted by hash for O(log n) lookup", schema.name)
    emit(" *   - %s_nodes_tree: in DSL order for tree walking", schema.name)
    emit(" *")
    emit(" * Auto-generated - DO NOT EDIT")
    emit(" */")
    emit("")
    emit("#ifndef %s", guard)
    emit("#define %s", guard)
    emit("")
    emit("#include \"generated_%s.h\"", schema.name)
    emit("")
    
    -- TREE ORDER TABLE (preserves DSL structure)
    -- Nodes are in the order they appear in the DSL, with children contiguous after parent
    
    -- The schema.nodes are already in DSL order (depth-first)
    -- We need to track first_child as index into this tree-order array
    
    -- Build children index ranges for tree-order
    -- Children of a node are contiguous in tree order
    local tree_first_child = {}   -- node.index -> first child index in tree order
    local tree_child_count = {}   -- node.index -> child count
    
    for _, node in ipairs(schema.nodes) do
        tree_child_count[node.index] = #node.children
        if #node.children > 0 then
            -- First child is the one that appears first in DSL order (lowest index)
            local min_child_idx = #schema.nodes
            for _, child in ipairs(node.children) do
                if child.index < min_child_idx then
                    min_child_idx = child.index
                end
            end
            tree_first_child[node.index] = min_child_idx
        else
            tree_first_child[node.index] = 0
        end
    end
    
    emit("/* ============================================ */")
    emit("/* Tree-Order Node Table (for walking)          */")
    emit("/* ============================================ */")
    emit("")
    emit("/* Nodes in DSL definition order - children are contiguous */")
    emit("static const %s_node_t %s_nodes_tree[%d] = {", schema.name, schema.name, #schema.nodes)
    for _, node in ipairs(schema.nodes) do
        emit("    { 0x%08XU, %d, %d, %d, %d, %d, %d },  /* [%d] %s */",
             node.hash,
             node.class_index,
             node.parent_index,
             tree_child_count[node.index],
             tree_first_child[node.index],
             node.depth,
             node.is_leaf and 1 or 0,
             node.index,
             node.path)
    end
    emit("};")
    emit("")
    
    -- HASH-SORTED TABLE (for lookup by path hash)
    local sorted_nodes = {}
    for _, node in ipairs(schema.nodes) do
        table.insert(sorted_nodes, node)
    end
    table.sort(sorted_nodes, function(a, b) return a.hash < b.hash end)
    
    -- Create mappings between tree index and hash-sorted index
    local tree_to_hash = {}  -- tree index -> hash-sorted index
    local hash_to_tree = {}  -- hash-sorted index -> tree index
    for i, node in ipairs(sorted_nodes) do
        tree_to_hash[node.index] = i - 1
        hash_to_tree[i - 1] = node.index
    end
    
    emit("/* ============================================ */")
    emit("/* Hash-Sorted Lookup Table                     */")
    emit("/* ============================================ */")
    emit("")
    emit("/* Hash lookup entry - maps hash to tree index */")
    emit("typedef struct {")
    emit("    uint32_t hash;")
    emit("    uint16_t tree_index;  /* Index into %s_nodes_tree */", schema.name)
    emit("} %s_hash_entry_t;", schema.name)
    emit("")
    emit("/* Sorted by hash for binary search */")
    emit("static const %s_hash_entry_t %s_nodes_by_hash[%d] = {", 
         schema.name, schema.name, #schema.nodes)
    for _, node in ipairs(sorted_nodes) do
        emit("    { 0x%08XU, %d },  /* %s */",
             node.hash,
             node.index,
             node.path)
    end
    emit("};")
    emit("")
    
    -- Arena offsets (in TREE order, matching nodes_tree)
    emit("/* ============================================ */")
    emit("/* Arena Offsets (tree order)                   */")
    emit("/* ============================================ */")
    emit("")
    
    for i, arena in ipairs(arenas) do
        local buf = arena.buffer
        emit("/* Arena offsets for %s buffer */", buf.name)
        emit("static const %s_arena_info_t %s_%s_arena[%d] = {",
             schema.name, schema.name, buf.name, #schema.nodes)
        for _, node in ipairs(schema.nodes) do
            emit("    { %d, %d },  /* [%d] %s */",
                 arena.node_offsets[node.index],
                 arena.node_sizes[node.index],
                 node.index,
                 node.path)
        end
        emit("};")
        emit("")
    end
    
    -- Root nodes list (nodes with parent_index == -1)
    local roots = {}
    for _, node in ipairs(schema.nodes) do
        if node.parent_index == -1 then
            table.insert(roots, node.index)
        end
    end
    
    emit("/* ============================================ */")
    emit("/* Root Nodes                                   */")
    emit("/* ============================================ */")
    emit("")
    emit("#define %s_ROOT_COUNT %d", prefix, #roots)
    emit("static const uint16_t %s_roots[%d] = { %s };", 
         schema.name, #roots, table.concat(roots, ", "))
    emit("")
    
    -- Runtime configuration for cfl_hbit.h
    emit("/* ============================================ */")
    emit("/* Runtime Configuration (for cfl_hbit.h)       */")
    emit("/* ============================================ */")
    emit("")
    emit("/* Buffer type values - use enum from cfl_hbit.h if available */")
    emit("#ifdef CFL_HBIT_H")
    emit("  /* cfl_hbit.h included - use its enum */")
    emit("#else")
    emit("  /* Standalone - define values */")
    emit("  #define CFL_HBIT_BUF_OR_LATCH 0")
    emit("  #define CFL_HBIT_BUF_OR_MASK  1")
    emit("  #define CFL_HBIT_BUF_AND      2")
    emit("#endif")
    emit("")
    
    -- Buffer configs array (ROM)
    emit("/* Buffer configurations (ROM) */")
    emit("static const struct {")
    emit("    uint8_t  type;")
    emit("    uint16_t arena_size;")
    emit("    const %s_arena_info_t* arena_info;", schema.name)
    emit("} %s_buffer_configs[%d] = {", schema.name, #schema.buffers)
    
    for i, buf in ipairs(schema.buffers) do
        local type_val = buf.type == "OR_LATCH" and "CFL_HBIT_BUF_OR_LATCH" 
                      or buf.type == "OR_MASK" and "CFL_HBIT_BUF_OR_MASK"
                      or "CFL_HBIT_BUF_AND"
        emit("    { %s, %s_ARENA_%s_SIZE, %s_%s_arena },  /* %s */",
             type_val, prefix, buf.name, schema.name, buf.name, buf.name)
    end
    emit("};")
    emit("")
    
    -- Calculate RAM needed for runtime
    -- Layout: pointer arrays [4 * buffer_count * sizeof(ptr)] + buffer arenas + dirty bits
    local ptr_size = 8  -- sizeof(uint8_t*) on 64-bit, but we need to be flexible
    local ram_needed = 4 * #schema.buffers * ptr_size  -- current, shadow, latched, mask ptrs
    
    for _, arena in ipairs(arenas) do
        local buf = arena.buffer
        ram_needed = ram_needed + arena.total_bytes * 2  -- current + shadow
        if buf.type == "OR_LATCH" then
            ram_needed = ram_needed + arena.total_bytes  -- latched
        elseif buf.type == "OR_MASK" then
            ram_needed = ram_needed + arena.total_bytes  -- mask
        end
    end
    ram_needed = ram_needed + math.floor((#schema.nodes + 7) / 8)  -- dirty bits
    
    emit("/* RAM size calculation:")
    emit(" *   Pointer arrays: 4 * %d * sizeof(void*)", #schema.buffers)
    emit(" *   Buffer arenas: see below")
    emit(" *   Dirty bits: %d bytes", math.floor((#schema.nodes + 7) / 8))
    emit(" * Note: Assumes 64-bit pointers. For 32-bit, size will be smaller. */")
    emit("#define %s_RAM_SIZE %d", prefix, ram_needed)
    emit("")
    
    -- Full config struct that matches cfl_hbit_config_t
    emit("/* Complete configuration for cfl_hbit_create() */")
    emit("/* Cast to (const cfl_hbit_config_t*) when calling */")
    emit("static const struct {")
    emit("    uint16_t node_count;")
    emit("    uint16_t buffer_count;")
    emit("    uint16_t root_count;")
    emit("    uint16_t ram_size;")
    emit("    const %s_node_t* nodes;", schema.name)
    emit("    const %s_hash_entry_t* nodes_by_hash;", schema.name)
    emit("    const uint16_t* roots;")
    emit("    const void* buffer_configs;")
    emit("} %s_config = {", schema.name)
    emit("    %d,  /* node_count */", #schema.nodes)
    emit("    %d,  /* buffer_count */", #schema.buffers)
    emit("    %d,  /* root_count */", #roots)
    emit("    %s_RAM_SIZE,", prefix)
    emit("    %s_nodes_tree,", schema.name)
    emit("    %s_nodes_by_hash,", schema.name)
    emit("    %s_roots,", schema.name)
    emit("    %s_buffer_configs,", schema.name)
    emit("};")
    emit("")
    
    -- Debug path strings (in tree order)
    emit("#ifdef %s_INCLUDE_PATH_STRINGS", prefix)
    emit("")
    emit("/* Path strings for debugging (tree order) */")
    emit("static const char* %s_node_paths[%d] = {", schema.name, #schema.nodes)
    for _, node in ipairs(schema.nodes) do
        emit("    \"%s\",  /* [%d] */", node.path, node.index)
    end
    emit("};")
    emit("")
    emit("#endif /* %s_INCLUDE_PATH_STRINGS */", prefix)
    emit("")
    
    emit("#endif /* %s */", guard)
    
    return table.concat(lines, "\n")
end

--------------------------------------------------------------------------------
-- Memory Summary
--------------------------------------------------------------------------------

local function generate_memory_summary(schema, arenas)
    local lines = {}
    local function emit(fmt, ...)
        table.insert(lines, string.format(fmt, ...))
    end
    
    emit("")
    emit("/* ============================================ */")
    emit("/* Memory Summary                               */")
    emit("/* ============================================ */")
    emit("/*")
    emit(" * Schema: %s v%s", schema.name, schema.version)
    emit(" * Nodes: %d, Classes: %d, Buffers: %d", 
         #schema.nodes, #schema.classes, #schema.buffers)
    emit(" *")
    emit(" * Buffer Arenas:")
    
    local total_ram = 0
    for _, arena in ipairs(arenas) do
        local buf = arena.buffer
        local ram
        if buf.type == "OR_LATCH" then
            ram = arena.total_with_latch * 2
            emit(" *   %-12s %4d bytes (type=%s, ×2 current/latched, ×2 shadow)",
                 buf.name, ram, buf.type)
        else
            ram = arena.total_bytes * 2
            emit(" *   %-12s %4d bytes (type=%s, ×2 shadow)",
                 buf.name, ram, buf.type)
        end
        total_ram = total_ram + ram
    end
    
    emit(" *")
    emit(" * Total RAM: %d bytes", total_ram)
    emit(" */")
    
    return table.concat(lines, "\n")
end

--------------------------------------------------------------------------------
-- Main
--------------------------------------------------------------------------------

local function print_usage()
    print([[
Hierarchical Bit Map Code Generator

Usage: luajit codegen.lua <schema.lua> [output_dir]

Options:
  -h, --help     Show this help
  -v, --verbose  Verbose output
  -q, --quiet    Suppress output except errors

Output Files:
  generated_<name>.h       - Main header with definitions
  generated_<name>_data.h  - Static data tables

Example:
  luajit codegen.lua test_schema.lua ./output
]])
end

local function main(args)
    local schema_file = nil
    local output_dir = "."
    local verbose = false
    local quiet = false
    
    -- Parse arguments
    local i = 1
    while i <= #args do
        local arg = args[i]
        if arg == "-h" or arg == "--help" then
            print_usage()
            os.exit(0)
        elseif arg == "-v" or arg == "--verbose" then
            verbose = true
        elseif arg == "-q" or arg == "--quiet" then
            quiet = true
        elseif arg:sub(1,1) == "-" then
            print("Unknown option: " .. arg)
            os.exit(1)
        elseif not schema_file then
            schema_file = arg
        else
            output_dir = arg
        end
        i = i + 1
    end
    
    if not schema_file then
        print("Error: No schema file specified")
        print_usage()
        os.exit(1)
    end
    
    local function log(fmt, ...)
        if not quiet then print(string.format(fmt, ...)) end
    end
    
    -- Load schema
    log("Loading schema: %s", schema_file)
    
    -- Set up package path for schema_builder
    local dir = schema_file:match("(.*/)")
    if dir then
        package.path = dir .. "?.lua;" .. package.path
    else
        package.path = "./?.lua;" .. package.path
    end
    
    local schema_func, err = loadfile(schema_file)
    if not schema_func then
        print("Error loading schema: " .. err)
        os.exit(1)
    end
    
    local ok, raw = pcall(schema_func)
    if not ok then
        print("Error executing schema: " .. tostring(raw))
        os.exit(1)
    end
    
    if not raw or not raw.name then
        print("Error: Schema file must return a table with 'name' field")
        os.exit(1)
    end
    
    -- Process schema
    log("Processing schema: %s v%s", raw.name, raw.version)
    local schema = process_schema(raw)
    
    log("  %d buffers, %d classes, %d nodes", 
        #schema.buffers, #schema.classes, #schema.nodes)
    
    -- Calculate arenas
    local arenas = calculate_arenas(schema)
    
    -- Create output directory
    os.execute("mkdir -p " .. output_dir)
    
    -- Generate headers
    log("Generating headers...")
    
    local main_h = generate_main_header(schema, arenas)
    local data_h = generate_data_header(schema, arenas)
    local summary = generate_memory_summary(schema, arenas)
    
    -- Write main header
    local main_path = output_dir .. "/generated_" .. schema.name .. ".h"
    local f = io.open(main_path, "w")
    f:write(main_h)
    f:write(summary)
    f:close()
    log("  Written: %s", main_path)
    
    -- Write data header  
    local data_path = output_dir .. "/generated_" .. schema.name .. "_data.h"
    f = io.open(data_path, "w")
    f:write(data_h)
    f:close()
    log("  Written: %s", data_path)
    
    log("")
    log("Generation complete!")
    if not quiet then
        print(summary)
    end
end

main(arg)