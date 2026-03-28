-- global_planner.lua -- Global planner for virtual node graph
--
-- Takes the virtual node graph and a mission list, produces an ordered
-- sequence of virtual node transitions (the global plan).
--
-- Two routing modes:
--   1. Inter-mission transit: Dijkstra over board-only edges (no arm shortcuts)
--   2. Intra-mission actions: walk the visit list using all edges
--
-- Output: ordered list of virtual actions for the local planner.

local dijkstra = require("dijkstra")

-- Locate shared modules from chain_tree_luajit
local ct_base = "../../chain_tree_luajit/lua_dsl/"
package.path = package.path
  .. ";" .. ct_base .. "luajit_pipeline/?.lua"
  .. ";" .. ct_base .. "lua_support/?.lua"

local json_util   = require("json_util")
local yaml_dumper = require("debug_yaml_dumper")

local M = {}

---------------------------------------------------------------------------
-- Filter edges to board-only paths (no arm nodes as transit)
---------------------------------------------------------------------------
local function board_only_edges(edges, arm_nodes)
  local filtered = {}
  for _, e in ipairs(edges) do
    local from, to = e[1], e[2]
    if not arm_nodes[from] and not arm_nodes[to] then
      filtered[#filtered + 1] = e
    end
  end
  return filtered
end

---------------------------------------------------------------------------
-- Find the board node associated with a virtual node.
---------------------------------------------------------------------------
local function resolve_board_node(node_name, board_nodes, arm_nodes)
  if board_nodes[node_name] then
    return node_name
  end
  local arm = arm_nodes[node_name]
  if arm and arm.at_board then
    return arm.at_board
  end
  return nil
end

---------------------------------------------------------------------------
-- Topological sort of missions respecting ordering constraints
---------------------------------------------------------------------------
local function topo_sort(missions, constraints)
  local deps = {}
  local all = {}

  for key, m in pairs(missions) do
    if type(m) == "table" and m.name then
      all[#all + 1] = key
      deps[key] = {}
    end
  end

  if constraints then
    for _, c in ipairs(constraints) do
      local before, _, after = c[1], c[2], c[3]
      if not deps[after] then deps[after] = {} end
      deps[after][#deps[after] + 1] = before
    end
  end

  local done = {}
  local order = {}
  local remaining = #all

  while remaining > 0 do
    local found = false
    for _, key in ipairs(all) do
      if not done[key] then
        local ok = true
        for _, d in ipairs(deps[key]) do
          if not done[d] then ok = false; break end
        end
        if ok then
          order[#order + 1] = key
          done[key] = true
          remaining = remaining - 1
          found = true
        end
      end
    end
    if not found then
      error("Circular dependency in mission ordering constraints")
    end
  end

  return order
end

---------------------------------------------------------------------------
-- Append path to route, avoiding duplicate at junction
---------------------------------------------------------------------------
local function append_path(route, path)
  local start_idx = 1
  if #route > 0 and path[1] == route[#route] then
    start_idx = 2
  end
  for i = start_idx, #path do
    route[#route + 1] = path[i]
  end
end

---------------------------------------------------------------------------
-- Default limits per nav method
---------------------------------------------------------------------------
local nav_limits = {
  spline_follow = { max_distance = 2000 },
  line_follow   = { max_distance = 1500 },
  wall_ride     = { max_distance = 800  },
}

---------------------------------------------------------------------------
-- Test if a node is a waypoint (routing-only, not a real virtual node)
---------------------------------------------------------------------------
local function is_waypoint(node_name, vn)
  local bn = vn.board_nodes[node_name]
  return bn and bn.kind == "waypoint"
end

---------------------------------------------------------------------------
-- Collapse raw steps into virtual actions
---------------------------------------------------------------------------
-- Consecutive path steps through waypoints collapse into one path action
-- with segments. Arm and at_node steps pass through unchanged.
local function collapse_to_virtual_actions(plan, vn)
  local raw = plan.steps
  local actions = {}
  local i = 1

  while i <= #raw do
    local s = raw[i]

    if s.edge_type == "path" then
      local origin = s.from
      local segments = {}
      local total_dist = 0
      local primary_nav = s.data and s.data.nav or "spline_follow"
      local primary_speed = s.data and s.data.speed or 0

      while i <= #raw and raw[i].edge_type == "path" do
        local seg = raw[i]
        local seg_nav = seg.data and seg.data.nav or primary_nav
        local seg_dist = seg.weight

        segments[#segments + 1] = {
          from      = seg.from,
          to        = seg.to,
          distance  = seg_dist,
          nav       = seg_nav,
          speed     = seg.data and seg.data.speed or primary_speed,
          waypoints = seg.data and seg.data.waypoints or nil,
        }
        total_dist = total_dist + seg_dist

        if not is_waypoint(seg.to, vn) then
          i = i + 1
          break
        end
        i = i + 1
      end

      local dest = segments[#segments].to
      local limits = nav_limits[primary_nav] or { max_distance = 2000 }

      actions[#actions + 1] = {
        action_type    = "path",
        from           = origin,
        to             = dest,
        nav_method     = primary_nav,
        speed          = primary_speed,
        max_distance   = limits.max_distance,
        total_distance = total_dist,
        segments       = segments,
      }

    elseif s.edge_type == "arm_move" then
      actions[#actions + 1] = {
        action_type = "arm_move",
        from        = s.from,
        to          = s.to,
        speed       = s.data and s.data.speed or 0,
        cost        = s.weight,
      }
      i = i + 1

    elseif s.edge_type == "at_node" then
      actions[#actions + 1] = {
        action_type = "at_node",
        from        = s.from,
        to          = s.to,
        cost        = s.weight,
      }
      i = i + 1

    else
      i = i + 1
    end
  end

  return actions
end

---------------------------------------------------------------------------
-- Serialize plan to output data table
---------------------------------------------------------------------------
local function plan_to_data(plan, vn)
  local actions = collapse_to_virtual_actions(plan, vn)

  for i, a in ipairs(actions) do
    a.step = i
  end

  -- Build virtual node route (only non-waypoint nodes)
  local vn_route = {}
  for _, node in ipairs(plan.route) do
    if not is_waypoint(node, vn) then
      if #vn_route == 0 or vn_route[#vn_route] ~= node then
        vn_route[#vn_route + 1] = node
      end
    end
  end

  return {
    mission_order   = plan.mission_order,
    total_cost      = plan.total_cost,
    virtual_actions = #actions,
    virtual_route   = vn_route,
    actions         = actions,
  }
end

---------------------------------------------------------------------------
-- Plan generation
---------------------------------------------------------------------------
function M.plan(missions, strategy, vn)
  local start_node = strategy.start_node
  local end_node   = strategy.end_node
  local all_edges  = vn.edges
  local b_edges    = board_only_edges(all_edges, vn.arm_nodes)

  local mission_keys = topo_sort(missions, strategy.ordering_constraints)

  local route = {}
  local current_board = start_node
  local total_cost = 0

  for _, key in ipairs(mission_keys) do
    local mission = missions[key]
    local visit = mission.visit

    local target_board = resolve_board_node(visit[1], vn.board_nodes, vn.arm_nodes)
    if not target_board then
      error(string.format("Mission %s: first visit node '%s' has no board anchor",
        key, visit[1]))
    end

    -- Route to mission's board position (board-only Dijkstra)
    if current_board ~= target_board then
      local path, cost = dijkstra.search(b_edges, current_board, target_board)
      if not path then
        error(string.format("No board path from '%s' to '%s' for mission %s",
          current_board, target_board, key))
      end
      total_cost = total_cost + cost
      append_path(route, path)
    end

    -- Walk the mission's visit list (using all edges)
    for i = 1, #visit do
      local node = visit[i]
      local prev = route[#route]
      if prev and prev ~= node then
        local path, cost = dijkstra.search(all_edges, prev, node)
        if not path then
          error(string.format("No path from '%s' to '%s' in mission %s visit list",
            prev, node, key))
        end
        total_cost = total_cost + cost
        append_path(route, path)
      elseif not prev then
        route[#route + 1] = node
      end
    end

    -- Track board position
    local last_board = nil
    for i = #visit, 1, -1 do
      last_board = resolve_board_node(visit[i], vn.board_nodes, vn.arm_nodes)
      if last_board then break end
    end
    current_board = last_board or current_board

    -- If visit ended on arm node, insert board node for next transit
    local last_visit = route[#route]
    if vn.arm_nodes[last_visit] and current_board then
      route[#route + 1] = current_board
    end
  end

  -- Return to end node
  if current_board ~= end_node then
    local path, cost = dijkstra.search(b_edges, current_board, end_node)
    if not path then
      error(string.format("No board path from '%s' to end node '%s'",
        current_board, end_node))
    end
    total_cost = total_cost + cost
    append_path(route, path)
  end

  local steps = dijkstra.annotate_path(route, all_edges)

  return {
    steps         = steps,
    route         = route,
    mission_order = mission_keys,
    total_cost    = total_cost,
  }
end

---------------------------------------------------------------------------
-- Pretty print a plan
---------------------------------------------------------------------------
function M.print_plan(plan, vn)
  local data = plan_to_data(plan, vn)

  print("=== Global Plan ===")
  print(string.format("Mission order: %s", table.concat(data.mission_order, " -> ")))
  print(string.format("Total cost: %d", data.total_cost))
  print(string.format("Virtual actions: %d", data.virtual_actions))
  print()

  io.write("Virtual route: ")
  local col = 15
  for i, node in ipairs(data.virtual_route) do
    local sep = (i < #data.virtual_route) and " -> " or ""
    local chunk = node .. sep
    col = col + #chunk
    if col > 78 then
      io.write("\n               ")
      col = 15 + #chunk
    end
    io.write(chunk)
  end
  print("\n")

  print("Virtual actions:")
  for _, a in ipairs(data.actions) do
    if a.action_type == "path" then
      print(string.format("  %2d. [path    ] %s -> %s  (dist: %d, max: %d)",
        a.step, a.from, a.to, a.total_distance, a.max_distance))
      print(string.format("       nav: %s, speed: %d, segments: %d",
        a.nav_method, a.speed, #a.segments))
      for j, seg in ipairs(a.segments) do
        print(string.format("         %d. %s -> %s (%s, %dmm)",
          j, seg.from, seg.to, seg.nav, seg.distance))
      end
    elseif a.action_type == "arm_move" then
      print(string.format("  %2d. [arm_move] %s -> %s  (cost: %d, speed: %d deg/s)",
        a.step, a.from, a.to, a.cost, a.speed))
    elseif a.action_type == "at_node" then
      print(string.format("  %2d. [at_node ] %s -> %s  (cost: %d)",
        a.step, a.from, a.to, a.cost))
    end
  end
end

---------------------------------------------------------------------------
-- Write plan to JSON file
---------------------------------------------------------------------------
function M.write_json(plan, vn, filepath)
  local data = plan_to_data(plan, vn)
  local json_str = json_util.encode(data)
  local fh, err = io.open(filepath, "w")
  if not fh then
    error(string.format("Cannot open '%s': %s", filepath, err))
  end
  fh:write(json_str)
  fh:write("\n")
  fh:close()
  print(string.format("[global_planner] Wrote JSON: %s (%d bytes)", filepath, #json_str))
end

---------------------------------------------------------------------------
-- Write plan to YAML file
---------------------------------------------------------------------------
function M.write_yaml(plan, vn, filepath)
  local data = plan_to_data(plan, vn)
  yaml_dumper.dump_to_file(data, filepath)
end

return M
