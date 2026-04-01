-- path_template.lua -- ChainTree template for path virtual actions
--
-- Builds a ChainTree KB for path following.
-- Hub-side user functions monitor bitmaps and send segment commands.
--
-- Tree structure:
--   root (COLUMN_MAIN, auto_start=true)
--     seq (SEQUENCE_MAIN)
--       seg_exec (PATH_SEG_EXEC — walks segments, sends avro packets)
--
-- The root receives the initial event JSON packet.
-- seg_exec iterates through segments, sending each to the robot.
-- Bitmaps are monitored via the boolean function each tick.

local builder_mod = require("tree_builder")

local M = {}

-- KB name for path tests
M.kb_name = "path_follow"

---------------------------------------------------------------------------
-- Build the tree
---------------------------------------------------------------------------
function M.build_tree(tb)
  tb:begin_kb(M.kb_name)

  -- Root: column with auto_start
  local root = tb:add_node("root",
    "CFL_COLUMN_MAIN", "CFL_COLUMN_INIT", "CFL_COLUMN_TERM",
    "CFL_NULL", {}, true)
  tb:push_parent(root)

  -- Segment executor: custom main function
  tb:add_node("seg_exec",
    "PATH_SEG_EXEC", "PATH_SEG_INIT", "PATH_SEG_TERM",
    "PATH_SEG_MONITOR", {}, true)

  tb:pop_parent()
  tb:end_kb()
end

---------------------------------------------------------------------------
-- User functions
---------------------------------------------------------------------------
-- These run on the hub. They monitor scan tree bitmaps and
-- send avro command packets to the robot.

local defs = require("ct_definitions")
local common = require("ct_common")

local user_fns = {}

-- Init one-shot: unpack initial event, set up segment state
user_fns.one_shot = {}
user_fns.one_shot.PATH_SEG_INIT = function(handle, node)
  local node_id = node.label_dict.ltree_name
  local ns = common.alloc_node_state(handle, node_id, {})

  -- Initial event data stored on blackboard by sequencer
  local bb = handle.blackboard
  ns.segments      = bb.segments or {}
  ns.current_seg   = 1
  ns.nav_method    = bb.nav_method or "spline_follow"
  ns.speed         = bb.speed or 0
  ns.max_distance  = bb.max_distance or 2000
  ns.from          = bb.from or ""
  ns.to            = bb.to or ""
  ns.seg_complete  = false
  ns.obstacle      = false
  ns.recovery      = false
  ns.recovery_count = 0
  ns.max_recovery  = 2
  ns.done          = false

  -- Send first segment command to robot
  if #ns.segments > 0 then
    local seg = ns.segments[1]
    -- In real implementation: format avro packet and queue
    -- as streaming_data event to remote unit
    handle.blackboard.current_command = {
      type      = "path_segment",
      seg_index = 1,
      from      = seg.from,
      to        = seg.to,
      nav       = seg.nav,
      speed     = seg.speed,
      distance  = seg.distance,
      waypoints = seg.waypoints,
    }
    handle.blackboard.command_sent = true
  end
end

-- Term one-shot: cleanup
user_fns.one_shot.PATH_SEG_TERM = function(handle, node)
  local node_id = node.label_dict.ltree_name
  handle.node_state[node_id] = nil
end

-- Boolean function: monitor bitmaps from scan tree
-- Called each tick by the main function.
-- Reads scan tree bitmap state from blackboard (set by external scan tree).
user_fns.boolean = {}
user_fns.boolean.PATH_SEG_MONITOR = function(handle, node, event_id, event_data)
  local node_id = node.label_dict.ltree_name
  local ns = common.get_node_state(handle, node_id)
  if not ns then return false end

  local bb = handle.blackboard

  -- Read bitmap state (set externally by scan tree)
  ns.seg_complete = bb.bitmap_seg_complete or false
  ns.obstacle     = bb.bitmap_obstacle or false
  ns.motor_fault  = bb.bitmap_motor_fault or false

  -- Return true if action needed (segment done or fault)
  return ns.seg_complete or ns.obstacle or ns.motor_fault
end

-- Main function: path segment executor
-- Manages segment progression, obstacle recovery, completion.
user_fns.main = {}
user_fns.main.PATH_SEG_EXEC = function(handle, bool_fn, node, event_id, event_data)
  if event_id ~= defs.CFL_TIMER_EVENT then
    return defs.CFL_CONTINUE
  end

  local node_id = node.label_dict.ltree_name
  local ns = common.get_node_state(handle, node_id)
  if not ns then return defs.CFL_DISABLE end

  -- Call monitor to update bitmap state
  bool_fn(handle, node, event_id, event_data)

  local bb = handle.blackboard

  -- Motor fault: unrecoverable
  if ns.motor_fault then
    bb.path_status = "fault"
    bb.path_reason = "motor_fault"
    return defs.CFL_TERMINATE
  end

  -- Obstacle: enter recovery
  if ns.obstacle and not ns.recovery then
    ns.recovery_count = ns.recovery_count + 1
    if ns.recovery_count > ns.max_recovery then
      bb.path_status = "fault"
      bb.path_reason = "max_recovery_exceeded"
      return defs.CFL_TERMINATE
    end
    ns.recovery = true
    -- Send circumnavigation command to robot
    bb.current_command = {
      type           = "circumnavigation",
      turn_direction = "left",
      turn_angle     = 90,
      max_distance   = ns.max_distance,
    }
    bb.command_sent = true
    return defs.CFL_HALT
  end

  -- Recovery in progress: wait for path clear
  if ns.recovery then
    if ns.seg_complete then
      -- Path is clear after recovery
      ns.recovery = false
      ns.obstacle = false
      bb.bitmap_seg_complete = false
      bb.bitmap_obstacle = false
      -- Re-send current segment
      local seg = ns.segments[ns.current_seg]
      if seg then
        bb.current_command = {
          type      = "path_segment",
          seg_index = ns.current_seg,
          from      = seg.from,
          to        = seg.to,
          nav       = seg.nav,
          speed     = seg.speed,
          distance  = seg.distance,
          waypoints = seg.waypoints,
        }
        bb.command_sent = true
      end
    end
    return defs.CFL_HALT
  end

  -- Normal: check segment completion
  if ns.seg_complete then
    bb.bitmap_seg_complete = false
    ns.seg_complete = false
    ns.current_seg = ns.current_seg + 1

    if ns.current_seg > #ns.segments then
      -- All segments done
      bb.path_status = "success"
      return defs.CFL_DISABLE
    end

    -- Send next segment
    local seg = ns.segments[ns.current_seg]
    bb.current_command = {
      type      = "path_segment",
      seg_index = ns.current_seg,
      from      = seg.from,
      to        = seg.to,
      nav       = seg.nav,
      speed     = seg.speed,
      distance  = seg.distance,
      waypoints = seg.waypoints,
    }
    bb.command_sent = true
  end

  return defs.CFL_HALT
end

-- Column builtins needed by the tree
user_fns.main.CFL_COLUMN_MAIN = function(handle, bool_fn, node, event_id, event_data)
  if event_id == defs.CFL_TIMER_EVENT then
    if not common.any_child_enabled(handle, node.label_dict.ltree_name) then
      return defs.CFL_DISABLE
    end
  end
  return defs.CFL_CONTINUE
end

user_fns.one_shot.CFL_COLUMN_INIT = function(handle, node)
  -- no-op
end

user_fns.one_shot.CFL_COLUMN_TERM = function(handle, node)
  -- no-op
end

---------------------------------------------------------------------------
-- Registry for ct_loader.register_functions()
---------------------------------------------------------------------------
M.registry = {
  main     = user_fns.main,
  one_shot = user_fns.one_shot,
  boolean  = user_fns.boolean,
}

return M
