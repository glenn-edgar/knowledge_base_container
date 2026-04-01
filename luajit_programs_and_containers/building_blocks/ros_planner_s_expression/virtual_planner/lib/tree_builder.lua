-- tree_builder.lua -- Build ChainTree JSON IR programmatically
--
-- Constructs the node/KB/event structure that ct_loader normally
-- reads from JSON. Allows templates to build trees in Lua and
-- feed them directly to ct_runtime.create().
--
-- Each template calls builder methods to define its tree, then
-- calls build() to get a handle_data table compatible with
-- ct_loader's output format.

local M = {}

function M.new(name)
  local b = {
    name          = name,
    nodes         = {},
    kb_table      = {},
    event_strings = {},
    idx_to_ltree  = {},
    ltree_to_index = {},
    next_index    = 0,
    next_event    = 20,  -- start after system events (0-19)
    -- Function name tracking
    main_names     = {},
    oneshot_names   = {},
    boolean_names   = {},
    -- Blackboard
    bb_defaults    = {},
    bb_const       = {},
    -- Current KB context
    current_kb     = nil,
    -- Parent stack for nesting
    parent_stack   = {},
  }
  return setmetatable(b, { __index = M })
end

---------------------------------------------------------------------------
-- KB (test) management
---------------------------------------------------------------------------
function M.begin_kb(self, kb_name)
  self.current_kb = kb_name
  self.kb_table[kb_name] = {
    name = kb_name,
    root_node = nil,
    node_ids = {},
  }
end

function M.end_kb(self)
  self.current_kb = nil
  self.parent_stack = {}
end

---------------------------------------------------------------------------
-- Node creation
---------------------------------------------------------------------------
-- Creates a node and adds it to the current KB.
-- Returns the ltree path of the created node.
function M.add_node(self, label, main_fn, init_fn, term_fn, aux_fn, node_dict, auto_start)
  local kb_name = self.current_kb
  assert(kb_name, "add_node: no current KB (call begin_kb first)")

  -- Build ltree path
  local ltree
  if #self.parent_stack == 0 then
    ltree = "kb." .. kb_name .. "." .. label
  else
    ltree = self.parent_stack[#self.parent_stack] .. "." .. label
  end

  -- Assign index
  local idx = self.next_index
  self.next_index = idx + 1
  self.idx_to_ltree[idx] = ltree
  self.ltree_to_index[ltree] = idx

  -- Parent ltree
  local parent_ltree = nil
  if #self.parent_stack > 0 then
    parent_ltree = self.parent_stack[#self.parent_stack]
  end

  -- Default function names
  main_fn = main_fn or "CFL_NULL"
  init_fn = init_fn or "CFL_NULL"
  term_fn = term_fn or "CFL_NULL"
  aux_fn  = aux_fn  or "CFL_NULL"

  -- Track function names
  if main_fn ~= "CFL_NULL" then self.main_names[main_fn] = true end
  if init_fn ~= "CFL_NULL" then self.oneshot_names[init_fn] = true end
  if term_fn ~= "CFL_NULL" then self.oneshot_names[term_fn] = true end
  if aux_fn  ~= "CFL_NULL" then self.boolean_names[aux_fn] = true end

  -- Node dict defaults
  node_dict = node_dict or {}
  if auto_start == nil then auto_start = false end
  node_dict.auto_start = auto_start

  local node = {
    label = label,
    label_dict = {
      ltree_name              = ltree,
      parent_ltree_name       = parent_ltree,
      main_function_name      = main_fn,
      initialization_function_name = init_fn,
      termination_function_name    = term_fn,
      aux_function_name       = aux_fn,
      links                   = {},  -- filled by push_parent/pop_parent
    },
    node_dict  = node_dict,
    ct_control = { enabled = false, initialized = false },
  }

  self.nodes[ltree] = node

  -- Add to KB
  local kb = self.kb_table[kb_name]
  kb.node_ids[#kb.node_ids + 1] = ltree
  if not kb.root_node then
    kb.root_node = ltree
  end

  -- Add as child of parent
  if parent_ltree and self.nodes[parent_ltree] then
    local parent = self.nodes[parent_ltree]
    parent.label_dict.links[#parent.label_dict.links + 1] = ltree
  end

  return ltree
end

---------------------------------------------------------------------------
-- Nesting: push/pop parent for building tree hierarchy
---------------------------------------------------------------------------
function M.push_parent(self, ltree)
  self.parent_stack[#self.parent_stack + 1] = ltree
end

function M.pop_parent(self)
  self.parent_stack[#self.parent_stack] = nil
end

---------------------------------------------------------------------------
-- Event registration
---------------------------------------------------------------------------
function M.register_event(self, event_name)
  if self.event_strings[event_name] then
    return self.event_strings[event_name]
  end
  local id = self.next_event
  self.next_event = id + 1
  self.event_strings[event_name] = id
  return id
end

---------------------------------------------------------------------------
-- Blackboard fields
---------------------------------------------------------------------------
function M.add_bb_field(self, name, default)
  self.bb_defaults[name] = default
end

---------------------------------------------------------------------------
-- Build: return handle_data compatible with ct_runtime.create()
---------------------------------------------------------------------------
function M.build(self)
  return {
    nodes              = self.nodes,
    kb_table           = self.kb_table,
    event_strings      = self.event_strings,
    idx_to_ltree       = self.idx_to_ltree,
    ltree_to_index     = self.ltree_to_index,
    main_names         = self.main_names,
    oneshot_names      = self.oneshot_names,
    boolean_names      = self.boolean_names,
    blackboard         = {
      field_defaults = self.bb_defaults,
      const_records  = self.bb_const,
    },
    -- Empty function dicts — filled by register_functions
    main_functions     = {},
    one_shot_functions = {},
    boolean_functions  = {},
  }
end

return M
