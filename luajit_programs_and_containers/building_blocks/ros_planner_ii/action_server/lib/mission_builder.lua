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
-- @param mission_cmd      table: mission command with start, stops, bookend
-- @param planner          global_planner instance (already loaded with board)
-- @param operation_types  optional array of operation type strings the robot supports
-- @return route           array of {kb_name, params} or nil
-- @return plan_info       leg details for replan, or {error=string}
function M.build(mission_cmd, planner, operation_types)
    local stops = mission_cmd.stops or error("mission_builder: stops required")
    local start = mission_cmd.start

    if #stops == 0 then
        return nil, { error = "no stops in mission" }
    end

    -- Validate no stops target transit-only nodes
    if planner.is_transit then
        local transit_errors = {}
        for i, stop in ipairs(stops) do
            if planner:is_transit(stop.node) then
                transit_errors[#transit_errors + 1] = string.format(
                    "stop %d: '%s' is a transit node (not a valid mission stop)", i, stop.node)
            end
        end
        if #transit_errors > 0 then
            return nil, {
                error = "transit_node_stops",
                transit_stops = transit_errors,
            }
        end
    end

    -- Build operation_types lookup if provided
    local op_set = nil
    if operation_types and #operation_types > 0 then
        op_set = {}
        for _, name in ipairs(operation_types) do
            op_set[name] = true
        end
    end

    -- Validate stop actions against robot's operation_types.
    -- Action can be explicit (stop.action) or inferred from node type.
    if op_set then
        local unsupported = {}
        for i, stop in ipairs(stops) do
            local action = stop.action
            if not action and planner.get_node_type then
                action = planner:get_node_type(stop.node)
            end
            if action and not op_set[action] then
                unsupported[#unsupported + 1] = string.format(
                    "stop %d: '%s' not supported by robot", i, action)
            end
        end
        if #unsupported > 0 then
            return nil, {
                error = "unsupported_operation",
                unsupported = unsupported,
            }
        end
    end

    local route = {}
    local legs = {}
    local total_cost = 0

    -- Always start with init_check (robot self-test)
    route[#route + 1] = { kb_name = "init_check", params = {} }

    -- Start node: where the robot is now. Required for route planning.
    -- If not provided, assume robot is at the first stop (no navigation to it).
    local current_node = start or stops[1].node

    for i, stop in ipairs(stops) do
        local goal_node = stop.node or error(
            "mission_builder: stop " .. i .. " missing node")

        -- Plan navigation leg (skip if already at destination)
        local leg_route_start = #route + 1
        local leg_path, leg_cost

        if current_node ~= goal_node then
            local nav_route, nav_info = planner:plan(current_node, goal_node)

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

            leg_path = nav_info.path
            leg_cost = nav_info.cost
            total_cost = total_cost + leg_cost
        else
            leg_path = { current_node }
            leg_cost = 0
        end

        local leg_route_end = #route

        -- Determine operation: explicit stop.action, or inferred from node type
        local action = stop.action
        if not action and planner.get_node_type then
            action = planner:get_node_type(goal_node)
        end

        -- Insert operation VN at this stop (if action exists)
        if action then
            -- Merge: node default params < stop.params (stop overrides)
            local data = {}
            if planner.get_node_params then
                local node_params = planner:get_node_params(goal_node)
                if node_params then
                    for k, v in pairs(node_params) do data[k] = v end
                end
            end
            if stop.params then
                for k, v in pairs(stop.params) do data[k] = v end
            end

            route[#route + 1] = {
                kb_name = "operation",
                params  = {
                    operation_type = action,
                    data = data,
                },
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

    -- Always end with idle (robot parks)
    route[#route + 1] = { kb_name = "idle", params = {} }

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
function M.rebuild(remaining_stops, planner, current_node)
    return M.build({
        start   = current_node,
        stops   = remaining_stops,
        bookend = false,  -- no bookend on replan (mission already started)
    }, planner)
end

return M
