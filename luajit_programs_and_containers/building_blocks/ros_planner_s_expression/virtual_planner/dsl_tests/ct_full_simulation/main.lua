--[[
    ct_full_simulation -- Full system test using real ChainTree DSL trees.

    Loads compiled JSON IR for hub and remote, registers user functions,
    generates global plan, sequences virtual actions through the hub
    runtime connected to the remote runtime via channels.

    Uses ct_runtime event loop driven tick-by-tick by the test harness.
]]

-- Load config
local config = dofile("config.lua")

local vp_root     = os.getenv("VP_BOARDS") and os.getenv("VP_BOARDS") .. "/../" or "../../"
local boards_dir  = os.getenv("VP_BOARDS") or (vp_root .. "boards/")
local hub_json    = vp_root .. config.hub_json
local remote_json = vp_root .. config.remote_json

-- Modules
local ct_runtime     = require("ct_runtime")
local ct_loader      = require("ct_loader_pure")
local builtins       = require("ct_builtins")
local fn_registry    = require("fn_registry")
local global_planner = require("global_planner")
local json_util      = require("json_util")
local channels_mod   = require("channels")
local defs           = require("ct_definitions")
local engine         = require("ct_engine")

-- Planner functions
local planner_fns    = require("planner_start_next_test")

---------------------------------------------------------------------------
-- Generate global plan
---------------------------------------------------------------------------
local vn       = dofile(boards_dir .. config.board)
local strategy = dofile(boards_dir .. config.strategy)
local plan     = global_planner.plan(strategy, vn)

print(string.format("Global plan: %d virtual actions\n", #plan.actions))
global_planner.print_plan(plan)
print()

---------------------------------------------------------------------------
-- KB name to index mapping (matches hub_dsl.lua test_list order)
---------------------------------------------------------------------------
local kb_name_to_index = {
  init_check      = 1,
  path_spline     = 2,
  path_line       = 3,
  path_wall       = 4,
  path_rotate     = 5,
  deliver_part    = 6,
  paint_sample    = 7,
  load_shipping   = 8,
  pass_gate       = 9,
  inspection_scan = 10,
  idle            = 11,
}

-- Map action to KB name
local function action_to_kb(action)
  if action.action_type == "init_check"  then return "init_check" end
  if action.action_type == "path_rotate" then return "path_rotate" end
  if action.action_type == "path" then
    local nav = action.nav_method
    if nav == "spline_follow" then return "path_spline" end
    if nav == "line_follow"   then return "path_line" end
    if nav == "wall_ride"     then return "path_wall" end
    return "path_spline"  -- default
  end
  if action.action_type == "mission" then
    return action.catalog_key
  end
  return nil
end

-- Build JSON blob for an action with test_id and next_test embedded
local function action_to_json(action_index)
  local action = plan.actions[action_index]
  if not action then return "" end
  local kb_name = action_to_kb(action)
  if not kb_name then return "" end

  -- Shallow copy action table and add test_id + next_test
  local blob = {}
  for k, v in pairs(action) do blob[k] = v end
  blob.test_id = kb_name_to_index[kb_name] or 0

  -- Determine next_test
  local next_idx = action_index + 1
  if next_idx <= #plan.actions then
    local next_action = plan.actions[next_idx]
    local next_kb = action_to_kb(next_action)
    blob.next_test = kb_name_to_index[next_kb] or 0
  else
    blob.next_test = kb_name_to_index["idle"]
  end

  return json_util.encode(blob)
end

---------------------------------------------------------------------------
-- Load HUB runtime
---------------------------------------------------------------------------
print("Loading hub: " .. hub_json)
local hub_data = ct_loader.load(hub_json)

-- Register builtins + planner functions
fn_registry.register_functions(hub_data, builtins, planner_fns.registry)

-- Validate (only gate node functions needed for empty KB stubs)
local ok, missing = fn_registry.validate(hub_data)
if not ok then
  print("Hub missing functions (expected for stub KBs):")
  for _, m in ipairs(missing) do print("  " .. m) end
  -- Don't exit — stubs are expected to have missing functions
end

local hub_handle = ct_runtime.create({ delta_time = 0.1, max_ticks = 5000 }, hub_data)

---------------------------------------------------------------------------
-- Load REMOTE runtime
---------------------------------------------------------------------------
print("Loading remote: " .. remote_json)
local remote_data = ct_loader.load(remote_json)

-- Remote user functions (simulated stubs)
local remote_fns = { main = {}, one_shot = {}, boolean = {} }

remote_fns.main.REMOTE_RECV_MAIN = function(handle, bool_fn, node, event_id, event_data)
  if event_id ~= defs.CFL_TIMER_EVENT then return defs.CFL_CONTINUE end
  local bb = handle.blackboard
  if bb.command_pending then
    bb.active_command = bb.incoming_command
    bb.incoming_command = nil
    bb.command_pending = false
    bb.exec_start = true
    bb.stream_seg_complete    = false
    bb.stream_action_complete = false
    bb.stream_obstacle        = false
    bb.stream_motor_fault     = false
    bb.stream_action_fault    = false
  end
  return defs.CFL_CONTINUE
end
remote_fns.one_shot.REMOTE_RECV_INIT = function(handle, node) end

-- Action durations (ticks per command type)
local action_durations = {
  path_segment    = 3,  arm_command       = 4,
  rpc_request     = 2,  drive_command     = 3,
  sensor_command  = 1,  wait              = 2,
  circumnavigation = 5, rotate            = 2,
  init_check      = 2,
}

remote_fns.main.REMOTE_EXEC_MAIN = function(handle, bool_fn, node, event_id, event_data)
  if event_id ~= defs.CFL_TIMER_EVENT then return defs.CFL_CONTINUE end
  local node_id = node.label_dict.ltree_name
  local ns = handle.node_state[node_id]
  if not ns then return defs.CFL_CONTINUE end
  local bb = handle.blackboard

  if bb.exec_start then
    bb.exec_start = false
    local cmd = bb.active_command
    if cmd then
      local cmd_type = cmd.type or "unknown"
      ns.command_type = cmd_type
      ns.ticks_remaining = action_durations[cmd_type] or 2
      ns.active = true
      bb.last_action_started = cmd_type
    end
  end

  if ns.active then
    ns.ticks_remaining = ns.ticks_remaining - 1
    if ns.ticks_remaining <= 0 then
      local cmd_type = ns.command_type
      if cmd_type == "path_segment" or cmd_type == "circumnavigation" then
        bb.stream_seg_complete = true
      else
        bb.stream_action_complete = true
      end
      ns.active = false
      bb.last_action_completed = cmd_type
    end
  end
  return defs.CFL_CONTINUE
end

remote_fns.one_shot.REMOTE_EXEC_INIT = function(handle, node)
  local node_id = node.label_dict.ltree_name
  handle.node_state[node_id] = { ticks_remaining = 0, active = false }
end
remote_fns.one_shot.REMOTE_EXEC_TERM = function(handle, node)
  local node_id = node.label_dict.ltree_name
  handle.node_state[node_id] = nil
end
remote_fns.boolean.REMOTE_EXEC_CHECK = function(handle, node, event_id, event_data)
  local node_id = node.label_dict.ltree_name
  local ns = handle.node_state[node_id]
  if not ns then return false end
  return ns.active and ns.ticks_remaining <= 0
end

remote_fns.main.REMOTE_STREAM_MAIN = function(handle, bool_fn, node, event_id, event_data)
  if event_id ~= defs.CFL_TIMER_EVENT then return defs.CFL_CONTINUE end
  local bb = handle.blackboard
  if bb.sim_obstacle then
    bb.stream_obstacle = true
    bb.sim_obstacle = false
  end
  if bb.sim_motor_fault then
    bb.stream_motor_fault = true
    bb.sim_motor_fault = false
  end
  return defs.CFL_CONTINUE
end
remote_fns.one_shot.REMOTE_STREAM_INIT = function(handle, node) end

fn_registry.register_functions(remote_data, builtins, remote_fns)
local ok2, missing2 = fn_registry.validate(remote_data)
if not ok2 then
  print("Remote missing functions:")
  for _, m in ipairs(missing2) do print("  " .. m) end
  os.exit(1)
end

local remote_handle = ct_runtime.create({ delta_time = 0.1, max_ticks = 5000 }, remote_data)

-- Activate remote (always running)
engine.init_test(remote_handle, "remote_unit")
remote_handle.active_tests["remote_unit"] = true
remote_handle.active_test_count = 1

---------------------------------------------------------------------------
-- Channels
---------------------------------------------------------------------------
local channels = channels_mod.new(hub_handle, remote_handle)

---------------------------------------------------------------------------
-- Tick helpers
---------------------------------------------------------------------------
local function tick_remote()
  local kb = remote_handle.kb_table["remote_unit"]
  if kb then
    table.insert(remote_handle.event_queue, {
      node_id  = kb.root_node,
      event_id = defs.CFL_TIMER_EVENT,
    })
  end
  while #remote_handle.event_queue > 0 do
    local event = table.remove(remote_handle.event_queue, 1)
    engine.execute_event(remote_handle, event.node_id,
      event.event_id, event.event_data, event.event_type)
  end
end

local function tick_hub()
  for kb_name, _ in pairs(hub_handle.active_tests) do
    local kb = hub_handle.kb_table[kb_name]
    if kb then
      table.insert(hub_handle.event_queue, {
        node_id  = kb.root_node,
        event_id = defs.CFL_TIMER_EVENT,
      })
    end
  end
  while #hub_handle.event_queue > 0 do
    local event = table.remove(hub_handle.event_queue, 1)
    engine.execute_event(hub_handle, event.node_id,
      event.event_id, event.event_data, event.event_type)
  end
end

---------------------------------------------------------------------------
-- Sequencer: stages current/next JSON and chains KBs
---------------------------------------------------------------------------
local function stage_current_and_next(action_index)
  hub_handle.blackboard.current_test_json = action_to_json(action_index)
  if action_index + 1 <= #plan.actions then
    hub_handle.blackboard.next_test_json = action_to_json(action_index + 1)
  else
    -- Last action — next is idle
    hub_handle.blackboard.next_test_json = json_util.encode({
      test_id = kb_name_to_index["idle"], next_test = 0
    })
  end
end

---------------------------------------------------------------------------
-- Run simulation
---------------------------------------------------------------------------
print("\n=== Running CT Full Simulation ===\n")

-- Stage first action as current, second as next
stage_current_and_next(1)

-- Activate first KB
local first_kb = action_to_kb(plan.actions[1])
engine.init_test(hub_handle, first_kb)
hub_handle.active_tests[first_kb] = true
hub_handle.active_test_count = 1

local tick = 0
local max_ticks = 2000
local current_action_idx = 1
local completed_count = 0

while tick < max_ticks do
  tick = tick + 1

  -- Tick system
  channels:tick()
  tick_remote()
  channels:tick()
  tick_hub()

  -- Check which KBs completed (root disabled)
  local to_remove = {}
  for kb_name, _ in pairs(hub_handle.active_tests) do
    local kb = hub_handle.kb_table[kb_name]
    if kb and not engine.node_is_enabled(hub_handle, kb.root_node) then
      to_remove[#to_remove + 1] = kb_name
    end
  end

  for _, kb_name in ipairs(to_remove) do
    -- KB completed
    hub_handle.active_tests[kb_name] = nil
    hub_handle.active_test_count = hub_handle.active_test_count - 1
    completed_count = completed_count + 1

    local action = plan.actions[current_action_idx]
    local label = action.action_type
    if action.from_name then
      label = label .. " " .. action.from_name .. " -> " .. (action.to_name or "")
    elseif action.catalog_key then
      label = label .. " " .. action.catalog_key
    end
    print(string.format("  tick %3d: completed [%s] (KB: %s)", tick, label, kb_name))

    current_action_idx = current_action_idx + 1

    -- Stage current/next JSON for the new action (external chaining for now —
    -- once KBs have real trees, PLANNER_START_NEXT_TEST in finalize handles this)
    if current_action_idx <= #plan.actions then
      stage_current_and_next(current_action_idx)
      local next_action = plan.actions[current_action_idx]
      local next_kb = action_to_kb(next_action)
      if next_kb then
        -- Reset KB nodes
        local nkb = hub_handle.kb_table[next_kb]
        if nkb then
          for _, nid in ipairs(nkb.node_ids) do
            local n = hub_handle.nodes[nid]
            if n then
              n.ct_control.enabled = false
              n.ct_control.initialized = false
            end
            hub_handle.node_state[nid] = nil
          end
          engine.init_test(hub_handle, next_kb)
          hub_handle.active_tests[next_kb] = true
          hub_handle.active_test_count = hub_handle.active_test_count + 1
        end
      end
    else
      -- All actions done — stage idle
      hub_handle.blackboard.current_test_json = json_util.encode({
        test_id = kb_name_to_index["idle"], next_test = 0
      })
      hub_handle.blackboard.next_test_json = ""
    end
  end

  -- Check if done (no active tests and we've processed all actions)
  if hub_handle.active_test_count == 0 then
    if current_action_idx > #plan.actions then
      break  -- all done
    end
  end
end

---------------------------------------------------------------------------
-- Results
---------------------------------------------------------------------------
local status = (completed_count >= #plan.actions) and "complete" or "incomplete"

print(string.format("\n--- Results ---"))
print(string.format("Status: %s", status))
print(string.format("Ticks: %d", tick))
print(string.format("Actions completed: %d / %d", completed_count, #plan.actions))
channels:print_stats()

-- Write plan output
global_planner.write_json(plan, "global_plan.json")
global_planner.write_yaml(plan, "global_plan.yaml")

if status == "complete" then
  print("\nPASSED")
else
  print("\nFAILED")
  os.exit(1)
end
