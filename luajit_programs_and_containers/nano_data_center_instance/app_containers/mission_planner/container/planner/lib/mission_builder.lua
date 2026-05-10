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
-- @param mission_cmd      table: mission command with start, stops, bookend.
--                         Nav legs are emitted as drive_packet entries
--                         (one packet per polyline edge). Bookends
--                         (init_check, idle) and per-stop operation
--                         entries use the legacy {kb_name, params, energy}
--                         shape — action_server's dispatch branches on
--                         entry.kind so both shapes coexist in one route.
-- @param planner          global_planner instance (already loaded with board)
-- @param operation_types  optional array of operation type strings the robot supports
-- @param energy_rate      optional number: robot's energy per unit distance (default 1.0)
-- @return route           array of {kb_name, ...} bookend/op entries +
--                         {kind="drive_packet", packet, energy} nav entries
-- @return plan_info       leg details for replan, or {error=string}
function M.build(mission_cmd, planner, operation_types, energy_rate)
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
    local total_energy = 0
    energy_rate = energy_rate or 1.0

    -- Drive-packet path needs monotonic packet_ids spanning ALL legs of
    -- the mission (one ack contract per packet, ids must not collide).
    -- Heading flows leg-to-leg so each plan_v2 call picks up where the
    -- previous left off.
    local drive_packet_id = mission_cmd.packet_id_start or 1
    local drive_heading   = mission_cmd.initial_heading or 0
    -- cmd_drive's optional mission_id is uint32 (wire format). The
    -- string mission_cmd.mission_id (JobQueue job.id) belongs to the
    -- durability layer, not the on-wire packet. Only forward when
    -- numeric; otherwise leave nil and let the packet validator accept
    -- the absence of the field.
    local mission_id      = type(mission_cmd.mission_id) == "number"
        and mission_cmd.mission_id or nil

    -- Get operation energy cost from VN defs
    local op_energy_cost = 0
    local init_energy_cost = 0
    local idle_energy_cost = 0
    if planner.vn_defs then
        if planner.vn_defs["operation"] then
            op_energy_cost = planner.vn_defs["operation"].energy_cost or 0
        end
        if planner.vn_defs["init_check"] then
            init_energy_cost = planner.vn_defs["init_check"].energy_cost or 0
        end
        if planner.vn_defs["idle"] then
            idle_energy_cost = planner.vn_defs["idle"].energy_cost or 0
        end
    end

    -- Always start with init_check (robot self-test)
    route[#route + 1] = { kb_name = "init_check", params = {}, energy = init_energy_cost }
    total_energy = total_energy + init_energy_cost

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
            local nav_route, nav_info = planner:plan_v2(current_node, goal_node, {
                energy_rate     = energy_rate,
                initial_heading = drive_heading,
                packet_id_start = drive_packet_id,
                mission_id      = mission_id,
            })

            if not nav_route then
                return nil, {
                    error = string.format("no path from '%s' to '%s' (leg %d)",
                        current_node, goal_node, i),
                    legs = legs,
                }
            end

            -- Append nav entries to flat route. Each entry is
            -- {kind="drive_packet", packet, energy}; bookend / operation
            -- entries below use {kb_name, params, energy}. Both shapes
            -- coexist; action_server's dispatch branches on entry.kind.
            for _, entry in ipairs(nav_route) do
                route[#route + 1] = entry
                total_energy = total_energy + (entry.energy or 0)
            end

            -- Update drive-path cursor: packet_id advances by the number
            -- of edges (one packet per edge); heading advances so the
            -- NEXT leg's first packet picks up correctly.
            drive_packet_id = drive_packet_id + #nav_route
            local last_pkt = nav_route[#nav_route].packet
            local last_seg = last_pkt.segments[#last_pkt.segments]
            if last_seg.kind == "spline" then
                drive_heading = last_seg.end_heading
            end
            -- For non-spline final segments the drive_heading stays as
            -- build_drive_packets stamped it; route_builder.build_v2's
            -- internal chain keeps that consistent across legs.

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
                energy = op_energy_cost,
            }
            total_energy = total_energy + op_energy_cost
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
    route[#route + 1] = { kb_name = "idle", params = {}, energy = idle_energy_cost }
    total_energy = total_energy + idle_energy_cost

    return route, {
        legs          = legs,
        total_cost    = total_cost,
        total_energy  = total_energy,
        total_actions = #route,
    }
end

--- Rebuild a route for remaining stops after a fault.
-- Used during replanning: starts from current_node instead of original start.
-- @param remaining_stops  array of stop tables (subset of original mission)
-- @param planner          global_planner instance (may have blocked edges)
-- @param current_node     string: nearest node to fault position
-- @param current_heading  number: robot's current heading. PRE-EXISTING:
--                         this arg is currently NOT consumed (build()
--                         has no start_heading parameter). Documented
--                         here so a future fix that threads heading
--                         through doesn't have to touch the signature.
-- @return route, plan_info (same as build)
function M.rebuild(remaining_stops, planner, current_node, current_heading)
    return M.build({
        start   = current_node,
        stops   = remaining_stops,
        bookend = false,  -- no bookend on replan (mission already started)
    }, planner)
end

return M
