-- remote_template.lua -- ChainTree template for the remote (robot) unit
--
-- Simulates the robot-side ChainTree runtime.
-- Receives command packets via RPC channel, executes simulated actions,
-- streams bitmap state back to the hub.
--
-- In a real robot, the user functions here would be replaced by
-- C functions that drive actual motors, read sensors, etc.
-- The tree structure stays the same.
--
-- Tree structure:
--   root (COLUMN_MAIN, auto_start=true)
--     receiver (REMOTE_RECV — picks up RPC command packets)
--     executor (REMOTE_EXEC — simulates physical action, ticks down)
--     streamer (REMOTE_STREAM — writes bitmap state for streaming channel)
--
-- Execution flow per command:
--   1. receiver sees command_pending, copies command to node state
--   2. executor starts countdown based on command type
--   3. each tick, streamer writes current bitmap state
--   4. when countdown reaches 0, executor sets completion bitmap
--   5. streamer sends completion to hub via streaming channel

local M = {}

M.kb_name = "remote_unit"

---------------------------------------------------------------------------
-- Build the tree
---------------------------------------------------------------------------
function M.build_tree(tb)
  tb:begin_kb(M.kb_name)

  local root = tb:add_node("root",
    "CFL_COLUMN_MAIN", "CFL_COLUMN_INIT", "CFL_COLUMN_TERM",
    "CFL_NULL", {}, true)
  tb:push_parent(root)

  -- Receiver: picks up incoming RPC commands
  tb:add_node("receiver",
    "REMOTE_RECV", "REMOTE_RECV_INIT", nil,
    nil, {}, true)

  -- Executor: simulates physical action with tick countdown
  tb:add_node("executor",
    "REMOTE_EXEC", "REMOTE_EXEC_INIT", "REMOTE_EXEC_TERM",
    "REMOTE_EXEC_CHECK", {}, true)

  -- Streamer: writes bitmap state for streaming channel
  tb:add_node("streamer",
    "REMOTE_STREAM", "REMOTE_STREAM_INIT", nil,
    nil, {}, true)

  tb:pop_parent()
  tb:end_kb()
end

---------------------------------------------------------------------------
-- Simulated action durations (ticks per command type)
---------------------------------------------------------------------------
local action_durations = {
  path_segment      = 3,
  arm_command        = 4,
  rpc_request        = 2,
  drive_command       = 3,
  sensor_command      = 1,
  wait               = 2,
  circumnavigation   = 5,
}

---------------------------------------------------------------------------
-- User functions (simulated — replaced by C on real robot)
---------------------------------------------------------------------------
local defs   = require("ct_definitions")
local common = require("ct_common")

local user_fns = {}
user_fns.main = {}
user_fns.one_shot = {}
user_fns.boolean = {}

-- =========================================================================
-- Receiver: picks up command packets from RPC channel
-- =========================================================================
user_fns.one_shot.REMOTE_RECV_INIT = function(handle, node)
  local node_id = node.label_dict.ltree_name
  common.alloc_node_state(handle, node_id, {})
end

user_fns.main.REMOTE_RECV = function(handle, bool_fn, node, event_id, event_data)
  if event_id ~= defs.CFL_TIMER_EVENT then
    return defs.CFL_CONTINUE
  end

  local bb = handle.blackboard

  if bb.command_pending then
    -- Copy command to shared state for executor
    bb.active_command = bb.incoming_command
    bb.incoming_command = nil
    bb.command_pending = false
    bb.exec_start = true

    -- Clear completion bitmaps when new command arrives
    bb.stream_seg_complete    = false
    bb.stream_action_complete = false
    bb.stream_obstacle        = false
    bb.stream_motor_fault     = false
    bb.stream_action_fault    = false
  end

  return defs.CFL_CONTINUE
end

-- =========================================================================
-- Executor: simulates physical action with tick countdown
-- =========================================================================
user_fns.one_shot.REMOTE_EXEC_INIT = function(handle, node)
  local node_id = node.label_dict.ltree_name
  common.alloc_node_state(handle, node_id, {
    ticks_remaining = 0,
    active = false,
    command_type = nil,
  })
end

user_fns.one_shot.REMOTE_EXEC_TERM = function(handle, node)
  local node_id = node.label_dict.ltree_name
  handle.node_state[node_id] = nil
end

-- Check function: is action complete?
user_fns.boolean.REMOTE_EXEC_CHECK = function(handle, node, event_id, event_data)
  local node_id = node.label_dict.ltree_name
  local ns = common.get_node_state(handle, node_id)
  if not ns then return false end
  return ns.active and ns.ticks_remaining <= 0
end

user_fns.main.REMOTE_EXEC = function(handle, bool_fn, node, event_id, event_data)
  if event_id ~= defs.CFL_TIMER_EVENT then
    return defs.CFL_CONTINUE
  end

  local node_id = node.label_dict.ltree_name
  local ns = common.get_node_state(handle, node_id)
  if not ns then return defs.CFL_HALT end

  local bb = handle.blackboard

  -- Pick up new command
  if bb.exec_start then
    bb.exec_start = false
    local cmd = bb.active_command
    if cmd then
      local cmd_type = cmd.type or "unknown"
      ns.command_type = cmd_type
      ns.ticks_remaining = action_durations[cmd_type] or 2
      ns.active = true
      ns.command = cmd

      -- Simulated action logging
      bb.last_action_started = cmd_type
    end
  end

  -- Tick down active action
  if ns.active then
    ns.ticks_remaining = ns.ticks_remaining - 1

    if ns.ticks_remaining <= 0 then
      -- Action complete — set appropriate bitmap
      local cmd_type = ns.command_type

      if cmd_type == "path_segment" or cmd_type == "circumnavigation" then
        bb.stream_seg_complete = true
      elseif cmd_type == "arm_command" or cmd_type == "rpc_request"
          or cmd_type == "drive_command" or cmd_type == "sensor_command"
          or cmd_type == "wait" then
        bb.stream_action_complete = true
      end

      ns.active = false
      bb.last_action_completed = cmd_type
    end
  end

  return defs.CFL_CONTINUE
end

-- =========================================================================
-- Streamer: writes bitmap state for streaming channel
-- =========================================================================
-- In a real robot, this reads scan tree outputs and packs them
-- into avro bitmap packets. Here it just ensures the bb stream
-- fields are set from simulated sensor state.
user_fns.one_shot.REMOTE_STREAM_INIT = function(handle, node)
  local node_id = node.label_dict.ltree_name
  common.alloc_node_state(handle, node_id, {})
end

user_fns.main.REMOTE_STREAM = function(handle, bool_fn, node, event_id, event_data)
  if event_id ~= defs.CFL_TIMER_EVENT then
    return defs.CFL_CONTINUE
  end

  local bb = handle.blackboard

  -- In a real robot, this would read scan tree layer buffers
  -- and pack them into avro bitmap packets.
  --
  -- Simulated: obstacle and fault injection comes from
  -- bb.sim_obstacle and bb.sim_motor_fault (set by test harness)

  if bb.sim_obstacle then
    bb.stream_obstacle = true
    bb.sim_obstacle = false  -- consumed
  end

  if bb.sim_motor_fault then
    bb.stream_motor_fault = true
    bb.sim_motor_fault = false
  end

  return defs.CFL_CONTINUE
end

-- =========================================================================
-- Column builtins
-- =========================================================================
user_fns.main.CFL_COLUMN_MAIN = function(handle, bool_fn, node, event_id, event_data)
  if event_id == defs.CFL_TIMER_EVENT then
    if not common.any_child_enabled(handle, node.label_dict.ltree_name) then
      return defs.CFL_DISABLE
    end
  end
  return defs.CFL_CONTINUE
end

user_fns.one_shot.CFL_COLUMN_INIT = function(handle, node) end
user_fns.one_shot.CFL_COLUMN_TERM = function(handle, node) end

---------------------------------------------------------------------------
-- Registry
---------------------------------------------------------------------------
M.registry = {
  main     = user_fns.main,
  one_shot = user_fns.one_shot,
  boolean  = user_fns.boolean,
}

return M
