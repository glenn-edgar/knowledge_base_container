--[[
    mission_builder.lua -- Convert multi-stop mission → flat route.

    Takes a mission command with waypoint stops and actions, calls the
    global planner for navigation between stops, and produces a flat
    route array compatible with sequencer:load_route().

    Usage:
        local mb = require("mission_builder")
        local route, plan_info = mb.build(mission_cmd, planner)

    Mission command format:
        {
            start = "lander_pad",
            stops = {
                { node = "mining_zone_b", action = "deliver_part",
                  params = { arm_target = -45, payload_type = 1 } },
                { node = "lander_pad" },  -- no action at final stop
            },
            bookend = true,  -- wrap with init_check + idle
        }

    Returns:
        route = { {kb_name, params}, ... }  -- flat array for sequencer
        plan_info = {
            legs = { {from, to, path, cost, route_start, route_end}, ... },
            total_cost = number,
            total_actions = number,
        }
]]

local M = {}

--- Build a flat route from a multi-stop mission.
-- @param mission_cmd  table: mission command with start, stops, bookend
-- @param planner      global_planner instance (already loaded with board)
-- @return route       array of {kb_name, params} or nil
-- @return plan_info   leg details for replan, or {error=string}
function M.build(mission_cmd, planner)
    local start = mission_cmd.start or error("mission_builder: start required")
    local stops = mission_cmd.stops or error("mission_builder: stops required")

    if #stops == 0 then
        return nil, { error = "no stops in mission" }
    end

    local route = {}
    local legs = {}
    local total_cost = 0
    local heading = mission_cmd.initial_heading or 0

    -- Optional init_check bookend
    if mission_cmd.bookend then
        route[#route + 1] = { kb_name = "init_check", params = {} }
    end

    local current_node = start

    for i, stop in ipairs(stops) do
        local goal_node = stop.node or error(
            "mission_builder: stop " .. i .. " missing node")

        -- Plan navigation leg (skip if already at destination)
        local leg_route_start = #route + 1
        local leg_path, leg_cost

        if current_node ~= goal_node then
            local nav_route, nav_info = planner:plan(current_node, goal_node, {
                initial_heading = heading,
            })

            if not nav_route then
                return nil, {
                    error = string.format("no path from '%s' to '%s' (leg %d)",
                        current_node, goal_node, i),
                    legs = legs,
                }
            end

            -- Append nav actions to flat route
            for _, action in ipairs(nav_route) do
                route[#route + 1] = action
            end

            -- Track heading from last rotation in nav route
            for j = #nav_route, 1, -1 do
                if nav_route[j].kb_name == "path_rotate" then
                    heading = nav_route[j].params.to_heading
                    break
                end
            end

            leg_path = nav_info.path
            leg_cost = nav_info.cost
            total_cost = total_cost + leg_cost
        else
            leg_path = { current_node }
            leg_cost = 0
        end

        local leg_route_end = #route

        -- Insert mission action at this stop (if any)
        if stop.action then
            route[#route + 1] = {
                kb_name = stop.action,
                params  = stop.params or {},
            }
        end

        -- Record leg info for replan
        legs[#legs + 1] = {
            index       = i,
            from        = current_node,
            to          = goal_node,
            path        = leg_path,
            cost        = leg_cost,
            route_start = leg_route_start,
            route_end   = #route,
            action      = stop.action,
        }

        current_node = goal_node
    end

    -- Optional idle bookend
    if mission_cmd.bookend then
        route[#route + 1] = { kb_name = "idle", params = {} }
    end

    return route, {
        legs          = legs,
        total_cost    = total_cost,
        total_actions = #route,
    }
end

--- Rebuild a route for remaining stops after a fault.
-- Used during replanning: starts from current_node instead of original start.
-- @param remaining_stops  array of stop tables (subset of original mission)
-- @param planner          global_planner instance (may have blocked edges)
-- @param current_node     string: nearest node to fault position
-- @param current_heading  number: robot's current heading
-- @return route, plan_info (same as build)
function M.rebuild(remaining_stops, planner, current_node, current_heading)
    return M.build({
        start           = current_node,
        stops           = remaining_stops,
        initial_heading = current_heading,
        bookend         = false,  -- no bookend on replan (mission already started)
    }, planner)
end

return M
