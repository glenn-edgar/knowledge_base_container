-- =============================================================================
-- 0. PREAMBLE: DISABLE JIT & SETUP
-- =============================================================================
-- Turn off JIT to ensure deterministic behavior and prevent optimization artifacts
if jit then
    jit.off()
    jit.flush()
  end
  local ffi = require("ffi")
  local bit = require("bit")
  local json = require("dkjson")
  
  -- =============================================================================
  -- 1. LOAD SCHEMA
  -- =============================================================================
  local D = {
    Schema = function(t) return t end,
    Bitspace = function(t) return t end,
    DeviceClass = function(t) return t end,
    Node = function(t) return t end,
    Rollup = function(t) return t end
  }
  package.loaded["dsl_runtime"] = D
  local schema = require("example_schema")
  
  -- Mangle the name (e.g., "Robot Arm" -> "Robot_Arm")
  local safe_name = schema.name:gsub("%s+", "_")
  
  -- =============================================================================
  -- 2. HELPER FUNCTIONS
  -- =============================================================================
 -- Force C-style 32-bit unsigned arithmetic
 -- Safe 32-bit Multiply function (Handles overflow manually)
-- Performs (a * b) % 2^32 without using double-precision math
local function mul32(a, b)
    local ah, al = bit.rshift(a, 16), bit.band(a, 0xFFFF)
    local bh, bl = bit.rshift(b, 16), bit.band(b, 0xFFFF)
  
    -- 1. Multiply low parts
    local lo = al * bl
    
    -- 2. Multiply cross terms (for the high 16 bits)
    -- We ignore ah * bh because it shifts out of 32 bits entirely
    local hi = (ah * bl) + (al * bh)
  
    -- 3. Combine and truncate to 32 bits
    -- bit.lshift automatically discards bits above 32
    return bit.tobit(lo + bit.lshift(hi, 16))
  end
  
  -- =============================================================================
-- 2. HELPER FUNCTIONS
-- =============================================================================
local bit = require("bit")

-- Safe 32-bit Multiply function (Handles overflow manually)
-- Performs (a * b) % 2^32 without using double-precision math
local function mul32(a, b)
  local ah, al = bit.rshift(a, 16), bit.band(a, 0xFFFF)
  local bh, bl = bit.rshift(b, 16), bit.band(b, 0xFFFF)

  -- 1. Multiply low parts
  local lo = al * bl
  
  -- 2. Multiply cross terms (for the high 16 bits)
  -- We ignore ah * bh because it shifts out of 32 bits entirely
  local hi = (ah * bl) + (al * bh)

  -- 3. Combine and truncate to 32 bits
  -- bit.lshift automatically discards bits above 32
  return bit.tobit(lo + bit.lshift(hi, 16))
end

local function fnv1a_32(str)
  local hash = 0x811c9dc5
  local prime = 0x01000193

  for i = 1, #str do
    local byte = string.byte(str, i)
    hash = bit.bxor(hash, byte)
    hash = mul32(hash, prime) -- Use manual 32-bit multiply
  end
  
  -- Ensure result is treated as a 32-bit integer for hex formatting
  return bit.tobit(hash)
end

-- Helper to iterate tables in deterministic order (sorted keys)
local function spairs(t)
  local keys = {}
  for k in pairs(t) do table.insert(keys, k) end
  table.sort(keys)
  local i = 0
  return function()
    i = i + 1
    if keys[i] then return keys[i], t[keys[i]] end
  end
end
  
  -- =============================================================================
  -- 3. PROCESS DATA (Memory Layout & Config)
  -- =============================================================================
  
  -- --- 3a. Config Flattening ---
  local cfg_records = {}
  local cfg_index = {}
  
  local function get_json_type(val)
    local t = type(val)
    if t == "string" then return "JSON_TYPE_STRING_HASH", fnv1a_32(val) end
    if t == "boolean" then return "JSON_TYPE_BOOL", val and 1 or 0 end
    if t == "number" then
      if math.floor(val) == val then return "JSON_TYPE_INT32", val end
      return "JSON_TYPE_FLOAT32", val
    end
    if t == "table" then
      return (#val > 0) and "JSON_TYPE_ARRAY" or "JSON_TYPE_OBJECT", 0
    end
    return "JSON_TYPE_NULL", 0
  end
  
  local function traverse_config(node, path)
    local rec_idx = #cfg_records
    local type_enum, simple_val = get_json_type(node)
    
    if path ~= "" then
      table.insert(cfg_index, { hash = fnv1a_32(path), rec_idx = rec_idx, debug = path })
    end
  
    local rec = { type = type_enum, value = simple_val, comment = path }
    
    if type(node) == "table" then
      local count = 0
      -- Use sorted pairs (spairs) to ensure the array order is always identical
      for k, v in spairs(node) do
        count = count + 1
        local subpath = (path == "") and k or (path .. "." .. k)
        traverse_config(v, subpath)
      end
      rec.container_count = count
    end
    table.insert(cfg_records, rec)
  end
  
  print("Processing Config...")
  traverse_config(schema.config, "")
  table.sort(cfg_index, function(a,b) return a.hash < b.hash end)
  
  -- --- 3b. Bitspace Logic ---
  local bitspace_rules = {}
  local arenas = {} 
  local bitspace_map = {}
  
  for i, bs in ipairs(schema.bitspaces) do
    local id = i - 1
    bitspace_map[bs.name] = id
    arenas[id] = 0
    
    local op = "MERGE_OR"
    if bs.merge == "AND" then op = "MERGE_AND"
    elseif bs.merge == "PRIORITY" then op = "MERGE_PRIORITY" end
    
    table.insert(bitspace_rules, { name=bs.name, op=op })
  end
  
  -- --- 3c. Node Topology ---
  local node_layouts = {}
  print("Processing Nodes...")
  
  for i, node in ipairs(schema.nodes) do
    local cls = nil
    for _, c in ipairs(schema.classes) do if c.name == node.class then cls = c break end end
    
    if cls then
      local layout = {
        hash = fnv1a_32(node.path),
        debug = node.path,
        parent_idx = -1,
        first_child_idx = -1,
        next_sibling_idx = -1,
        offsets = {}
      }
      
      -- Calculate Memory Offsets
      for _, bs in ipairs(schema.bitspaces) do
        local bid = bitspace_map[bs.name]
        local bits = cls.banks[bs.name] or 0
        if bits > 0 then
          layout.offsets[bid] = arenas[bid]
          arenas[bid] = arenas[bid] + math.ceil(bits / 8)
        else
          layout.offsets[bid] = -1
        end
      end
      table.insert(node_layouts, layout)
    end
  end
  
  -- 1. Sort Layouts by Hash for Binary Search (Deterministic order)
  table.sort(node_layouts, function(a,b) return a.hash < b.hash end)
  
  -- 2. Re-calculate Parent/Child Links based on sorted indices
  local hash_to_idx = {}
  for i, l in ipairs(node_layouts) do hash_to_idx[l.hash] = i - 1 end
  
  local children_map = {} -- parent_idx -> list of child_indices
  
  for i, l in ipairs(node_layouts) do
    local last_dot = l.debug:match("^.*().")
    if last_dot then
      local parent_str = l.debug:sub(1, last_dot - 1)
      local p_hash = fnv1a_32(parent_str)
      
      if hash_to_idx[p_hash] then
        l.parent_idx = hash_to_idx[p_hash]
        
        -- Register as child
        if not children_map[l.parent_idx] then children_map[l.parent_idx] = {} end
        table.insert(children_map[l.parent_idx], i - 1)
      end
    end
  end
  
  -- 3. Flatten Children Map into Sibling Linked Lists
  -- We iterate sorted keys to ensure the sibling order is deterministic
  local sorted_parents = {}
  for k in pairs(children_map) do table.insert(sorted_parents, k) end
  table.sort(sorted_parents)
  
  for _, p_idx in ipairs(sorted_parents) do
    local child_list = children_map[p_idx]
    
    -- Sort children by index (so they appear in Hash order)
    table.sort(child_list)
    
    -- Link Parent -> First Child
    node_layouts[p_idx + 1].first_child_idx = child_list[1]
    
    -- Link Sibling -> Sibling
    for k = 1, #child_list - 1 do
      local curr = child_list[k]
      local next = child_list[k+1]
      node_layouts[curr + 1].next_sibling_idx = next
    end
  end
  
  
  -- =============================================================================
  -- 4. GENERATE SPECIFIC .H FILE
  -- =============================================================================
  local h_name = "generated_" .. safe_name .. ".h"
  print("Generating " .. h_name)
  local h = io.open(h_name, "w")
  h:write([[
  #pragma once
  #include "chain_tree.h"
  
  // Expose ONLY the descriptor for this specific schema.
  extern const chain_desc_t ]] .. safe_name .. [[_desc;
  ]])
  h:close()
  
  
  -- =============================================================================
  -- 5. GENERATE SPECIFIC .C FILE
  -- =============================================================================
  local c_name = "generated_" .. safe_name .. ".c"
  print("Generating " .. c_name)
  local c = io.open(c_name, "w")
  
  c:write('#include "' .. h_name .. '"\n\n')
  
  -- 5a. Static Arena Sizes
  c:write("static const uint32_t s_arena_sizes[] = {\n")
  for id=0, #schema.bitspaces-1 do c:write(string.format("  %d,\n", arenas[id])) end
  c:write("};\n\n")
  
  -- 5b. Static Rules
  c:write("static const bitspace_rule_t s_rules[] = {\n")
  for _, r in ipairs(bitspace_rules) do
    c:write(string.format("  { .op = %s }, // %s\n", r.op, r.name))
  end
  c:write("};\n\n")
  
  -- 5c. Static Offsets (The Flat Array)
  c:write("static const int32_t s_all_offsets[] = {\n")
  for _, l in ipairs(node_layouts) do
    c:write("  // " .. l.debug .. "\n  ")
    for i=0, #schema.bitspaces-1 do
      local off = l.offsets[i] or -1
      c:write(string.format("%d, ", off))
    end
    c:write("\n")
  end
  c:write("};\n\n")
  
  -- 5d. Static Layouts (Includes Topology)
  c:write("static const node_layout_t s_layouts[] = {\n")
  local num_bs = #schema.bitspaces
  for i, l in ipairs(node_layouts) do
    local offset_ptr = (i-1) * num_bs
    c:write(string.format("  { .hash=0x%08X, .parent_idx=%d, .first_child_idx=%d, .next_sibling_idx=%d, .offsets=&s_all_offsets[%d] }, // %s\n", 
      l.hash, l.parent_idx, l.first_child_idx, l.next_sibling_idx, offset_ptr, l.debug))
  end
  c:write("};\n\n")
  
  -- 5e. Static Config
  c:write(string.format("static const json_record_t s_cfg_recs[%d] = {\n", #cfg_records))
  for _, r in ipairs(cfg_records) do
    local val_str = ""
    if r.type == "JSON_TYPE_STRING_HASH" then val_str = string.format(".value.hash32=0x%08X", r.value)
    elseif r.type == "JSON_TYPE_FLOAT32" then val_str = string.format(".value.f32_value=%.4f", r.value)
    elseif r.type == "JSON_TYPE_BOOL"    then val_str = string.format(".value.bool_value=%d", r.value)
    elseif r.type == "JSON_TYPE_OBJECT" or r.type == "JSON_TYPE_ARRAY" then
       val_str = string.format(".value.container_count=%d", r.container_count)
    else val_str = string.format(".value.i32_value=%d", r.value) end
    c:write(string.format("  { .type=%s, %s }, // %s\n", r.type, val_str, r.comment))
  end
  c:write("};\n\n")
  
  c:write(string.format("static const json_path_index_t s_cfg_index[%d] = {\n", #cfg_index))
  for _, idx in ipairs(cfg_index) do
    c:write(string.format("  { .hash=0x%08X, .rec_idx=%d }, // %s\n", idx.hash, idx.rec_idx, idx.debug))
  end
  c:write("};\n\n")
  
  -- 5f. THE PUBLIC DESCRIPTOR
  c:write(string.format([[
  const chain_desc_t %s_desc = {
    .schema_name = "%s",
    .bitspace_count = %d,
    .arena_sizes = s_arena_sizes,
    .rules = s_rules,
    .layouts = s_layouts,
    .layout_count = %d,
    .cfg_recs = s_cfg_recs,
    .cfg_index = s_cfg_index,
    .cfg_index_len = %d
  };
  ]], safe_name, schema.name, num_bs, #node_layouts, #cfg_index))
  
  c:close()
  
  -- =============================================================================
  -- 6. GENERATE JSON SIDECAR
  -- =============================================================================
  print("Generating config_sidecar.json")
  local j = io.open("config_sidecar.json", "w")
  j:write(json.encode(schema.config, { indent = true }))
  j:close()
  
  print("Done. JIT was disabled.")