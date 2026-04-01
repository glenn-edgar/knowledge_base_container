-- mission_template.lua -- ChainTree template for mission virtual actions
--
-- Builds a ChainTree KB for mission execution at a board node.
-- Hub-side user functions monitor bitmaps and send action commands.
--
-- Tree structure:
--   root (COLUMN_MAIN, auto_start=true)
--     mission_exec (MISSION_EXEC — walks action packets, monitors completion)
--
-- The sequencer loads mission params onto the blackboard before activating.
-- mission_exec dispatches to the right action sequence based on params.

local M = {}

M.kb_name = "mission_exec"

---------------------------------------------------------------------------
-- Build the tree
---------------------------------------------------------------------------
function M.build_tree(tb)
  tb:begin_kb(M.kb_name)

  local root = tb:add_node("root",
    "CFL_COLUMN_MAIN", "CFL_COLUMN_INIT", "CFL_COLUMN_TERM",
    "CFL_NULL", {}, true)
  tb:push_parent(root)

  tb:add_node("exec",
    "MISSION_EXEC", "MISSION_INIT", "MISSION_TERM",
    "MISSION_MONITOR", {}, true)

  tb:pop_parent()
  tb:end_kb()
end

---------------------------------------------------------------------------
-- Action packet generators (from catalog params)
---------------------------------------------------------------------------
local function generate_arm_packets(params)
  local packets = {}
  packets[#packets + 1] = {
    type    = "arm_command",
    command = "move_to",
    angle   = params.arm_target,
    speed   = params.arm_speed,
  }
  if params.hold_time and params.hold_time > 0 then
    packets[#packets + 1] = {
      type     = "wait",
      duration = params.hold_time,
    }
  end
  if params.arm_return then
    packets[#packets + 1] = {
      type    = "arm_command",
      command = "move_to",
      angle   = params.arm_return,
      speed   = params.arm_speed,
    }
  end
  return packets
end

local function generate_rpc_packets(params)
  local packets = {}
  if params.rpc_open then
    packets[#packets + 1] = {
      type    = "rpc_request",
      service = params.rpc_open,
    }
  end
  if params.drive_through and params.drive_through > 0 then
    packets[#packets + 1] = {
      type     = "drive_command",
      command  = "straight",
      distance = params.drive_through,
    }
  end
  if params.rpc_close then
    packets[#packets + 1] = {
      type    = "rpc_request",
      service = params.rpc_close,
    }
  end
  return packets
end

local function generate_sensor_packets(params)
  local packets = {}
  packets[#packets + 1] = {
    type        = "sensor_command",
    command     = "read",
    port        = params.sensor_port,
    sensor_type = params.sensor_type,
  }
  return packets
end

local function generate_packets(params)
  if params.arm_target then return generate_arm_packets(params) end
  if params.rpc_open   then return generate_rpc_packets(params) end
  if params.sensor_port then return generate_sensor_packets(params) end
  return {}
end

---------------------------------------------------------------------------
-- User functions
---------------------------------------------------------------------------
local defs   = require("ct_definitions")
local common = require("ct_common")

local user_fns = {}
user_fns.main = {}
user_fns.one_shot = {}
user_fns.boolean = {}

-- Init: unpack params from blackboard, generate action packets
user_fns.one_shot.MISSION_INIT = function(handle, node)
  local node_id = node.label_dict.ltree_name
  local ns = common.alloc_node_state(handle, node_id, {})
  local bb = handle.blackboard

  ns.catalog_key      = bb.catalog_key or ""
  ns.board_node       = bb.board_node or ""
  ns.approach_heading = bb.approach_heading or 0
  ns.params           = bb.params or {}
  ns.packets          = generate_packets(ns.params)
  ns.current_packet   = 1
  ns.recovery_count   = 0
  ns.max_recovery     = 2
  ns.done             = false

  -- Send first packet
  if #ns.packets > 0 then
    bb.current_command = ns.packets[1]
    bb.command_sent = true
  end
end

-- Term: cleanup
user_fns.one_shot.MISSION_TERM = function(handle, node)
  local node_id = node.label_dict.ltree_name
  handle.node_state[node_id] = nil
end

-- Monitor: read bitmaps
user_fns.boolean.MISSION_MONITOR = function(handle, node, event_id, event_data)
  local node_id = node.label_dict.ltree_name
  local ns = common.get_node_state(handle, node_id)
  if not ns then return false end

  local bb = handle.blackboard
  ns.action_complete = bb.bitmap_action_complete or false
  ns.action_fault    = bb.bitmap_action_fault or false

  return ns.action_complete or ns.action_fault
end

-- Main: execute mission action sequence
user_fns.main.MISSION_EXEC = function(handle, bool_fn, node, event_id, event_data)
  if event_id ~= defs.CFL_TIMER_EVENT then
    return defs.CFL_CONTINUE
  end

  local node_id = node.label_dict.ltree_name
  local ns = common.get_node_state(handle, node_id)
  if not ns then return defs.CFL_DISABLE end

  local bb = handle.blackboard

  -- No packets: observation-only mission (sensor via scan tree)
  if #ns.packets == 0 then
    bb.mission_status = "success"
    return defs.CFL_DISABLE
  end

  -- Call monitor
  bool_fn(handle, node, event_id, event_data)

  -- Fault: retry or fail
  if ns.action_fault then
    ns.recovery_count = ns.recovery_count + 1
    if ns.recovery_count > ns.max_recovery then
      bb.mission_status = "fault"
      bb.mission_reason = "max_recovery_exceeded"
      return defs.CFL_TERMINATE
    end
    -- Retry current packet
    ns.action_fault = false
    bb.bitmap_action_fault = false
    bb.bitmap_action_complete = false
    bb.current_command = ns.packets[ns.current_packet]
    bb.command_sent = true
    return defs.CFL_HALT
  end

  -- Action complete: advance
  if ns.action_complete then
    bb.bitmap_action_complete = false
    ns.action_complete = false
    ns.current_packet = ns.current_packet + 1

    if ns.current_packet > #ns.packets then
      bb.mission_status = "success"
      return defs.CFL_DISABLE
    end

    -- Send next packet
    bb.current_command = ns.packets[ns.current_packet]
    bb.command_sent = true
  end

  return defs.CFL_HALT
end

-- Column builtins (shared with path_template)
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
