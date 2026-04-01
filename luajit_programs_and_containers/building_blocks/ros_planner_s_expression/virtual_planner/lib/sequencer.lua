-- sequencer.lua -- Virtual node sequencer using ChainTree runtime
--
-- Walks the global plan. For each virtual action:
--   1. Selects the template (KB) by action_type
--   2. Loads action params onto the blackboard
--   3. Activates the KB (ct_runtime.add_test)
--   4. Ticks the runtime until the KB's root disables (action complete)
--   5. Reads status from blackboard
--   6. Advances to next action or handles failure
--
-- The sequencer drives the runtime tick-by-tick rather than using
-- ct_runtime.run(), so it can inspect state between ticks.

local defs   = require("ct_definitions")
local engine = require("ct_engine")

local M = {}

---------------------------------------------------------------------------
-- Create sequencer
---------------------------------------------------------------------------
-- plan: global plan from global_planner
-- handle: ct_runtime handle (already created with all KBs loaded)
-- template_map: { action_type = kb_name, ... }
function M.new(plan, handle, template_map)
  local seq = {
    plan          = plan,
    actions       = plan.actions,
    handle        = handle,
    template_map  = template_map,
    current_index = 0,
    status        = "idle",
    tick_count    = 0,
  }
  return setmetatable(seq, { __index = M })
end

---------------------------------------------------------------------------
-- Load virtual action params onto blackboard
---------------------------------------------------------------------------
local function load_action_to_bb(handle, action)
  local bb = handle.blackboard

  -- Clear previous state
  bb.current_command       = nil
  bb.command_sent          = false
  bb.bitmap_seg_complete   = false
  bb.bitmap_obstacle       = false
  bb.bitmap_motor_fault    = false
  bb.bitmap_action_complete = false
  bb.bitmap_action_fault   = false
  bb.path_status           = nil
  bb.path_reason           = nil
  bb.mission_status        = nil
  bb.mission_reason        = nil

  -- Load action-specific data
  if action.action_type == "path" then
    bb.segments      = action.segments
    bb.nav_method    = action.nav_method
    bb.speed         = action.speed
    bb.max_distance  = action.max_distance
    bb.from          = action.from
    bb.to            = action.to
  elseif action.action_type == "mission" then
    bb.catalog_key      = action.catalog_key
    bb.board_node       = action.board_node
    bb.approach_heading = action.approach_heading
    bb.params           = action.params
    bb.points           = action.points
  end
end

---------------------------------------------------------------------------
-- Activate a KB for the current action
---------------------------------------------------------------------------
function M.activate(self, action)
  local kb_name = self.template_map[action.action_type]
  if not kb_name then
    error(string.format("sequencer: no template for action_type '%s'",
      action.action_type))
  end

  -- Load params to blackboard
  load_action_to_bb(self.handle, action)

  -- Reset node states for this KB
  local kb = self.handle.kb_table[kb_name]
  if kb then
    for _, nid in ipairs(kb.node_ids) do
      local node = self.handle.nodes[nid]
      if node then
        node.ct_control.enabled = false
        node.ct_control.initialized = false
      end
      self.handle.node_state[nid] = nil
    end
  end

  -- Activate
  engine.init_test(self.handle, kb_name)
  self.handle.active_tests[kb_name] = true
  self.handle.active_test_count = (self.handle.active_test_count or 0) + 1
end

---------------------------------------------------------------------------
-- Deactivate a KB
---------------------------------------------------------------------------
function M.deactivate(self, action)
  local kb_name = self.template_map[action.action_type]
  if kb_name and self.handle.active_tests[kb_name] then
    engine.terminate_all_nodes_in_kb(self.handle, kb_name)
    self.handle.active_tests[kb_name] = nil
    self.handle.active_test_count = self.handle.active_test_count - 1
  end
end

---------------------------------------------------------------------------
-- Check if current KB is still running
---------------------------------------------------------------------------
function M.kb_is_running(self, action)
  local kb_name = self.template_map[action.action_type]
  if not kb_name then return false end
  local kb = self.handle.kb_table[kb_name]
  if not kb then return false end
  return engine.node_is_enabled(self.handle, kb.root_node)
end

---------------------------------------------------------------------------
-- Read completion status from blackboard
---------------------------------------------------------------------------
function M.read_status(self, action)
  local bb = self.handle.blackboard
  if action.action_type == "path" then
    return bb.path_status or "unknown"
  elseif action.action_type == "mission" then
    return bb.mission_status or "unknown"
  end
  return "unknown"
end

---------------------------------------------------------------------------
-- Start
---------------------------------------------------------------------------
function M.start(self)
  if #self.actions == 0 then
    self.status = "complete"
    return
  end
  self.status = "running"
  self.current_index = 1
  self:activate(self.actions[1])
end

---------------------------------------------------------------------------
-- Tick: advance one cycle
---------------------------------------------------------------------------
-- Returns: "running" | "complete" | "failed"
function M.tick(self)
  if self.status ~= "running" then
    return self.status
  end

  self.tick_count = self.tick_count + 1
  local action = self.actions[self.current_index]
  if not action then
    self.status = "complete"
    return self.status
  end

  -- Generate timer event for active KBs
  for kb_name, _ in pairs(self.handle.active_tests) do
    local kb = self.handle.kb_table[kb_name]
    if kb then
      table.insert(self.handle.event_queue, {
        node_id  = kb.root_node,
        event_id = defs.CFL_TIMER_EVENT,
      })
    end
  end

  -- Drain event queue
  while #self.handle.event_queue > 0 do
    local event = table.remove(self.handle.event_queue, 1)
    engine.execute_event(
      self.handle,
      event.node_id,
      event.event_id,
      event.event_data,
      event.event_type
    )
  end

  -- Check if KB finished (root disabled)
  if not self:kb_is_running(action) then
    local status = self:read_status(action)
    self:deactivate(action)

    if status == "success" then
      -- Advance to next action
      self.current_index = self.current_index + 1
      if self.current_index > #self.actions then
        self.status = "complete"
      else
        self:activate(self.actions[self.current_index])
      end
    else
      -- Failure
      self.status = "failed"
      self.fail_action = action
      self.fail_reason = status
    end
  end

  return self.status
end

---------------------------------------------------------------------------
-- Current action info
---------------------------------------------------------------------------
function M.current_action(self)
  if self.current_index > 0 and self.current_index <= #self.actions then
    return self.actions[self.current_index]
  end
  return nil
end

return M
