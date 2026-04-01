--[[
    action_server.lua -- Top-level mission execution with replan.

    Accepts multi-stop mission commands, plans navigation between stops,
    inserts mission actions at waypoints, runs the sequencer, and handles
    replanning on fault.

    Single-threaded for now. Threading (one thread per robot) is a follow-up.

    Usage:
        local action_server = require("action_server")
        local server = action_server.new({
            db_file  = "surface_ops.db",
            hub_json = "hub_dsl/hub.json",
        })

        local result = server:execute_mission({
            robot_id = "rover_1",
            board    = "landing_zone",
            start    = "lander_pad",
            stops = {
                { node = "mining_zone_b", action = "deliver_part",
                  params = { arm_target = -45, payload_type = 1 } },
                { node = "lander_pad" },
            },
            bookend = true,
        })

        server:close()
]]

local global_planner  = require("global_planner")
local sequencer       = require("sequencer")
local mission_builder = require("mission_builder")

local M = {}
M.__index = M

---------------------------------------------------------------------------
-- Constructor
---------------------------------------------------------------------------
function M.new(opts)
    local self = setmetatable({}, M)

    self.db_file     = opts.db_file     or error("action_server: db_file required")
    self.hub_json    = opts.hub_json    or error("action_server: hub_json required")
    self.nats_server = opts.nats_server or "nats://127.0.0.1:4222"
    self.ltree_path  = opts.ltree_path  or "/usr/local/lib/ltree"
    self.max_replans = opts.max_replans or 3

    return self
end

---------------------------------------------------------------------------
-- Mission execution
---------------------------------------------------------------------------

--- Execute a multi-stop mission with automatic replanning on fault.
-- @param mission_cmd  table: { robot_id, board, start, stops[], bookend }
-- @return result table: { success, final_pose, completed, total, elapsed_ms,
--                         fault, replans, legs }
function M:execute_mission(mission_cmd)
    local robot_id  = mission_cmd.robot_id or error("action_server: robot_id required")
    local board     = mission_cmd.board    or error("action_server: board required")
    local start     = mission_cmd.start    or error("action_server: start required")

    -- Create global planner for this board
    local planner = global_planner.new({
        db_file    = self.db_file,
        board_name = board,
        ltree_path = self.ltree_path,
    })

    -- Create sequencer for this robot
    local seq = sequencer.new({
        robot_id    = robot_id,
        db_file     = self.db_file,
        hub_json    = self.hub_json,
        nats_server = self.nats_server,
        site        = mission_cmd.site,
    })

    -- Build initial route from mission stops
    local route, plan_info = mission_builder.build(mission_cmd, planner)
    if not route then
        seq:shutdown()
        planner:close()
        return {
            success = false,
            fault   = { reason = "planning_failed", detail = plan_info.error },
            replans = 0,
        }
    end

    print(string.format("Mission: %s on %s, %d stops, %d actions (cost=%d)",
        robot_id, board, #mission_cmd.stops, #route, plan_info.total_cost))

    -- Load and execute
    seq:load_route(route)
    local result = seq:run()
    local replans = 0

    -- Replan loop
    while result.needs_replan and replans < self.max_replans do
        replans = replans + 1

        print(string.format("Replan %d: fault at action %d (%s) — %s",
            replans,
            result.fault.action_index,
            result.fault.kb_name,
            result.fault.reason))

        -- Find nearest node to current pose
        local current_node = planner:find_nearest_node(
            result.final_pose.x, result.final_pose.y)

        -- Determine which stops remain
        local remaining_stops = self:_get_remaining_stops(
            mission_cmd, result, plan_info)

        if #remaining_stops == 0 then
            print("  No remaining stops — mission complete despite fault")
            break
        end

        -- Block the edge that caused the fault (if identifiable)
        self:_block_fault_edge(planner, result, plan_info)

        -- Rebuild route for remaining stops
        local new_route, new_info = mission_builder.rebuild(
            remaining_stops, planner, current_node, result.final_pose.heading)

        if not new_route then
            print("  No alternative route — aborting")
            result.success = false
            result.fault.detail = "replan failed: " .. (new_info.error or "unknown")
            break
        end

        print(string.format("  New route: %d actions (cost=%d)",
            #new_route, new_info.total_cost))

        plan_info = new_info
        seq:load_route(new_route)
        result = seq:run()
    end

    -- Finalize
    if result.needs_replan and replans >= self.max_replans then
        result.success = false
        result.fault = result.fault or {}
        result.fault.detail = "max replans exceeded (" .. self.max_replans .. ")"
    end

    -- Record mission-level result if not already done by sequencer
    if result.success or not result.needs_replan then
        seq:finish_mission(result)
    end

    result.replans = replans
    result.legs = plan_info.legs

    -- Shutdown
    seq:shutdown()
    planner:close()

    return result
end

---------------------------------------------------------------------------
-- Replan helpers
---------------------------------------------------------------------------

--- Determine which mission stops remain after a fault.
-- Maps the faulted action_index back to the leg/stop in plan_info.
function M:_get_remaining_stops(mission_cmd, result, plan_info)
    local fault_action = result.fault and result.fault.action_index or math.huge

    local remaining = {}
    for _, leg in ipairs(plan_info.legs) do
        -- If the fault occurred before this leg completed, include it
        if leg.route_end >= fault_action then
            local stop = mission_cmd.stops[leg.index]
            if stop then
                remaining[#remaining + 1] = {
                    node   = stop.node,
                    action = stop.action,
                    params = stop.params,
                }
            end
        end
    end

    return remaining
end

--- Block the edge that likely caused the fault.
-- Heuristic: find the nav action that faulted and derive the edge from its coords.
function M:_block_fault_edge(planner, result, plan_info)
    if not result.fault then return end

    -- Find which leg contains the faulted action
    local fault_idx = result.fault.action_index
    for _, leg in ipairs(plan_info.legs) do
        if fault_idx >= leg.route_start and fault_idx <= leg.route_end then
            -- Block the entire from→to edge for this leg
            if leg.from ~= leg.to then
                planner:mark_blocked(leg.from, leg.to)
                print(string.format("  Blocked edge: %s ↔ %s", leg.from, leg.to))
            end
            return
        end
    end
end

---------------------------------------------------------------------------
-- Accessors
---------------------------------------------------------------------------

function M:close()
    -- No persistent state in single-threaded mode
end

return M
