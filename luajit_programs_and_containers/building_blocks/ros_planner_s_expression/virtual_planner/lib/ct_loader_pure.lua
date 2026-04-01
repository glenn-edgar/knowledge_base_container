-- ct_loader_pure.lua -- ct_loader using pure Lua JSON (no cjson dependency)
--
-- Drop-in replacement for ct_loader.lua that uses json_util instead of cjson.
-- Everything else is identical to the original ct_loader.

local json = require("json_util")

local M = {}

-- No cjson.null to scrub — json_util returns nil for JSON null
local function scrub_nulls(t)
  return t
end

-- Metadata KB names to filter out
local METADATA_SUFFIXES = { "_functions" }
local METADATA_EXACT = {
  complete_functions_kb = true,
  bitmask_table_kb = true,
  event_string_table_kb = true,
}

local function is_metadata_kb(name)
  if METADATA_EXACT[name] then return true end
  for _, suffix in ipairs(METADATA_SUFFIXES) do
    if name:sub(-#suffix) == suffix then return true end
  end
  return false
end

local METADATA_LABELS = {
  virtual_functions = true,
  complete_functions = true,
  main_functions = true,
  one_shot_functions = true,
  boolean_functions = true,
}

local function is_metadata_node(node)
  return METADATA_LABELS[node.label] or false
end

local function normalize_links(links)
  if not links then return {} end
  if type(links) == "table" and #links == 0 then
    return {}
  end
  return links
end

function M.load(json_path)
  local f = assert(io.open(json_path, "r"))
  local raw = f:read("*a")
  f:close()

  local ir = json.decode(raw)
  assert(ir.schema_version == "1.0", "unsupported schema: " .. tostring(ir.schema_version))

  local idx_to_ltree = {}
  for ltree, idx in pairs(ir.ltree_to_index) do
    idx_to_ltree[idx] = ltree
  end

  local operational_kbs = {}
  for kb_name in pairs(ir.kb_log_dict) do
    if not is_metadata_kb(kb_name) then
      operational_kbs[#operational_kbs + 1] = kb_name
    end
  end
  table.sort(operational_kbs)

  local nodes = {}
  local main_names = {}
  local oneshot_names = {}
  local boolean_names = {}

  local metadata_ltree_prefixes = {}
  for kb_name in pairs(ir.kb_metadata) do
    if is_metadata_kb(kb_name) then
      metadata_ltree_prefixes[#metadata_ltree_prefixes + 1] = "kb." .. kb_name .. "."
    end
  end

  local function is_in_metadata_kb(ltree)
    for _, prefix in ipairs(metadata_ltree_prefixes) do
      if ltree:sub(1, #prefix) == prefix then return true end
    end
    return false
  end

  for ltree, node in pairs(ir.nodes) do
    if not is_in_metadata_kb(ltree) and not is_metadata_node(node) then
      if node.label_dict then
        node.label_dict.links = normalize_links(node.label_dict.links)
      end

      node.ct_control = { enabled = false, initialized = false }

      if node.node_dict then
        local nd = node.node_dict
        if type(nd.node_id) == "number" then
          nd.node_id = idx_to_ltree[nd.node_id] or nd.node_id
        end
        if type(nd.parent_node_name) == "number" then
          nd.parent_node_name = idx_to_ltree[nd.parent_node_name] or nd.parent_node_name
        end
        if type(nd.target_node_id) == "number" then
          nd.target_node_id = idx_to_ltree[nd.target_node_id] or nd.target_node_id
        end
        if type(nd.sm_node_id) == "number" then
          nd.sm_node_id = idx_to_ltree[nd.sm_node_id] or nd.sm_node_id
        end
        if type(nd.node_index) == "number" then
          nd.node_index = idx_to_ltree[nd.node_index] or nd.node_index
        end
        if type(nd.server_node_index) == "number" then
          nd.server_node_index = idx_to_ltree[nd.server_node_index] or nd.server_node_index
        end
        if type(nd.event_column) == "number" then
          nd.event_column = idx_to_ltree[nd.event_column] or nd.event_column
        end
        if type(nd.output_event_column_id) == "number" then
          nd.output_event_column_id = idx_to_ltree[nd.output_event_column_id] or nd.output_event_column_id
        end
        if type(nd.nodes) == "table" then
          for i, v in ipairs(nd.nodes) do
            if type(v) == "number" then
              nd.nodes[i] = idx_to_ltree[v] or v
            end
          end
        end
      end

      nodes[ltree] = node

      local ld = node.label_dict
      if ld then
        if ld.main_function_name then main_names[ld.main_function_name] = true end
        if ld.initialization_function_name then oneshot_names[ld.initialization_function_name] = true end
        if ld.termination_function_name then oneshot_names[ld.termination_function_name] = true end
        if ld.aux_function_name then boolean_names[ld.aux_function_name] = true end
      end
    end
  end

  local kb_table = {}
  for _, kb_name in ipairs(operational_kbs) do
    local prefix = "kb." .. kb_name .. "."
    local root_node = nil
    local root_depth = math.huge
    local kb_node_ids = {}
    for ltree, _ in pairs(nodes) do
      if ltree:sub(1, #prefix) == prefix then
        kb_node_ids[#kb_node_ids + 1] = ltree
        local depth = select(2, ltree:gsub("%.", ""))
        if depth < root_depth then
          root_depth = depth
          root_node = ltree
        end
      end
    end
    kb_table[kb_name] = {
      name = kb_name,
      root_node = root_node,
      node_ids = kb_node_ids,
    }
  end

  -- Parse blackboard
  local bb_defaults = {}
  local bb_const = {}
  local bb_raw = ir.blackboard
  if bb_raw then
    if bb_raw.record and bb_raw.record.fields then
      for _, fld in ipairs(bb_raw.record.fields) do
        local name = fld.name
        local val = fld.default or 0
        local dot = name:find("%.")
        if dot then
          local parent = name:sub(1, dot - 1)
          local child = name:sub(dot + 1)
          if not bb_defaults[parent] then bb_defaults[parent] = {} end
          bb_defaults[parent][child] = val
        else
          bb_defaults[name] = val
        end
      end
    end
    if bb_raw.const_records then
      for _, rec in ipairs(bb_raw.const_records) do
        local tbl = {}
        for _, fld in ipairs(rec.fields) do
          tbl[fld.name] = fld.value or fld.default or 0
        end
        bb_const[rec.name] = tbl
      end
    end
  end

  return {
    nodes = nodes,
    kb_table = kb_table,
    event_strings = ir.event_string_table or {},
    bitmask_names = ir.bitmask_table or {},
    idx_to_ltree = idx_to_ltree,
    ltree_to_index = ir.ltree_to_index or {},
    main_names = main_names,
    oneshot_names = oneshot_names,
    boolean_names = boolean_names,
    blackboard = {
      field_defaults = bb_defaults,
      const_records = bb_const,
    },
    main_functions = {},
    one_shot_functions = {},
    boolean_functions = {},
  }
end

-- register_functions and validate from fn_registry.lua
local fn_registry = require("fn_registry")
M.register_functions = fn_registry.register_functions
M.validate = fn_registry.validate

return M
