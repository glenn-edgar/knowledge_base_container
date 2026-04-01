-- global_planner.lua -- Global planner for virtual node graph
--
-- Routes through the board graph and emits virtual actions:
--   - Path actions: full spline geometry from the board graph
--   - Mission actions: high-level parameters from the catalog
--
-- The local planner expands each virtual action into hidden node patterns.

local dijkstra    = require("dijkstra")
local json_util   = require("json_util")
local yaml_dumper = require("plan_yaml")

local M = {}

---------------------------------------------------------------------------
-- Default safety limits per nav method
---------------------------------------------------------------------------
local nav_limits = {
  spline_follow = { max_distance = 2000 },
  line_follow   = { max_distance = 1500 },
  wall_ride     = { max_distance = 800  },
}

---------------------------------------------------------------------------
-- Test if a board node is a waypoint
---------------------------------------------------------------------------
local function is_waypoint(node_name, vn)
  local bn = vn.board_nodes[node_name]
  return bn and bn.kind == "waypoint"
end

---------------------------------------------------------------------------
-- Topological sort of catalog entries respecting preconditions
---------------------------------------------------------------------------
local function topo_sort(catalog, ordering)
  local deps = {}
  local all = {}

  -- Use ordering list if provided, otherwise collect all catalog keys
  if ordering then
    for _, key in ipairs(ordering) do
      all[#all + 1] = key
      deps[key] = {}
    end
  else
    for key, _ in pairs(catalog) do
      all[#all + 1] = key
      deps[key] = {}
    end
  end

  -- Build dependency graph from preconditions
  for _, key in ipairs(all) do
    local entry = catalog[key]
    if entry and entry.preconditions then
      for _, pre in ipairs(entry.preconditions) do
        deps[key][#deps[key] + 1] = pre
      end
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
      error("Circular dependency in catalog preconditions")
    end
  end

  return order
end

---------------------------------------------------------------------------
-- Build path action from a Dijkstra route between two board nodes
---------------------------------------------------------------------------
-- Collapses waypoints into segments within a single path action.
local function node_coords(name, vn)
  local bn = vn.board_nodes[name]
  if bn then return { x = bn.x, y = bn.y } end
  return { x = 0, y = 0 }
end

---------------------------------------------------------------------------
-- Heading calculation from two points (degrees, 0=east, CCW positive)
---------------------------------------------------------------------------
local function calc_heading(from, to)
  local dx = to.x - from.x
  local dy = to.y - from.y
  if dx == 0 and dy == 0 then return 0 end
  return math.deg(math.atan2(dy, dx))
end

---------------------------------------------------------------------------
-- Build init_check action (always first)
---------------------------------------------------------------------------
local function build_init_check()
  return {
    action_type = "init_check",
    heading_out = 0,  -- heading after init (facing east by default)
  }
end

---------------------------------------------------------------------------
-- Build path_rotate action
---------------------------------------------------------------------------
local function build_rotate_action(from_heading, to_heading)
  return {
    action_type  = "path_rotate",
    from_heading = from_heading,
    to_heading   = to_heading,
    heading_out  = to_heading,
  }
end

local function build_path_action(route_nodes, edges, vn)
  local segments = {}
  local total_dist = 0
  local primary_nav = nil
  local primary_speed = nil

  for i = 1, #route_nodes - 1 do
    local from = route_nodes[i]
    local to   = route_nodes[i + 1]
    local edge = dijkstra.find_edge(edges, from, to)
    if not edge then
      error(string.format("No edge from '%s' to '%s'", from, to))
    end

    local nav      = edge[4]
    local spd      = edge[5]
    local wpts     = edge[6]
    local dist     = edge[3]
    local nav_data = edge[8]

    if not primary_nav then
      primary_nav = nav
      primary_speed = spd
    end

    local seg = {
      from      = node_coords(from, vn),
      to        = node_coords(to, vn),
      from_name = from,
      to_name   = to,
      distance  = dist,
      nav       = nav,
      speed     = spd,
      waypoints = wpts,
    }
    -- Merge nav_data fields into segment
    if nav_data then
      for k, v in pairs(nav_data) do
        seg[k] = v
      end
    end

    segments[#segments + 1] = seg
    total_dist = total_dist + dist
  end

  local limits = nav_limits[primary_nav] or { max_distance = 2000 }

  -- Calculate entry and exit headings from segment geometry
  local first_seg = segments[1]
  local last_seg  = segments[#segments]
  local heading_in  = calc_heading(first_seg.from, first_seg.to)
  local heading_out = calc_heading(last_seg.from, last_seg.to)

  return {
    action_type    = "path",
    from           = node_coords(route_nodes[1], vn),
    to             = node_coords(route_nodes[#route_nodes], vn),
    from_name      = route_nodes[1],
    to_name        = route_nodes[#route_nodes],
    nav_method     = primary_nav,
    speed          = primary_speed,
    max_distance   = limits.max_distance,
    total_distance = total_dist,
    heading_in     = heading_in,
    heading_out    = heading_out,
    segments       = segments,
  }
end

---------------------------------------------------------------------------
-- Build mission action from catalog entry
---------------------------------------------------------------------------
local function build_mission_action(catalog_key, entry)
  return {
    action_type      = "mission",
    catalog_key      = catalog_key,
    name             = catalog_key,
    board_node       = entry.board_node,
    approach_heading = entry.approach_heading,
    points           = entry.points,
    params           = entry.params,
  }
end

---------------------------------------------------------------------------
-- Split a Dijkstra path at non-waypoint nodes into sub-paths
---------------------------------------------------------------------------
-- Each sub-path starts and ends at a non-waypoint node.
-- Waypoints are interior to sub-paths.
local function split_path_at_destinations(path, vn)
  local sub_paths = {}
  local current = { path[1] }

  for i = 2, #path do
    current[#current + 1] = path[i]
    if not is_waypoint(path[i], vn) then
      sub_paths[#sub_paths + 1] = current
      if i < #path then
        current = { path[i] }
      end
    end
  end

  return sub_paths
end

---------------------------------------------------------------------------
-- Plan generation
---------------------------------------------------------------------------
-- strategy: { missions = {"deliver_part", ...}, start_node, end_node }
-- vn: virtual node descriptor
function M.plan(strategy, vn)
  local start_node = strategy.start_node
  local end_node   = strategy.end_node
  local edges      = vn.edges
  local catalog    = vn.catalog

  -- Order missions respecting preconditions
  local mission_order = topo_sort(catalog, strategy.missions)

  local actions = {}
  local current = start_node
  local total_cost = 0
  local current_heading = 0  -- track robot heading through the plan

  -- Always start with init_check
  actions[#actions + 1] = build_init_check()
  current_heading = 0

  for _, key in ipairs(mission_order) do
    local entry = catalog[key]
    local target = entry.board_node

    -- Route to mission's board node
    if current ~= target then
      local path, cost = dijkstra.search(edges, current, target)
      if not path then
        error(string.format("No path from '%s' to '%s' for %s", current, target, key))
      end
      total_cost = total_cost + cost

      -- Split into sub-paths at non-waypoint nodes
      local sub_paths = split_path_at_destinations(path, vn)
      for _, sp in ipairs(sub_paths) do
        local path_action = build_path_action(sp, edges, vn)

        -- Insert rotate before non-spline paths if heading doesn't match
        if path_action.nav_method ~= "spline_follow" then
          local needed = path_action.heading_in
          if math.abs(needed - current_heading) > 1 then
            actions[#actions + 1] = build_rotate_action(current_heading, needed)
            current_heading = needed
          end
        end

        actions[#actions + 1] = path_action
        current_heading = path_action.heading_out
      end
    end

    -- Insert rotate before mission if approach heading doesn't match
    if entry.approach_heading then
      if math.abs(entry.approach_heading - current_heading) > 1 then
        actions[#actions + 1] = build_rotate_action(current_heading, entry.approach_heading)
        current_heading = entry.approach_heading
      end
    end

    -- Add mission action
    local mission_action = build_mission_action(key, entry)
    mission_action.heading_out = current_heading
    actions[#actions + 1] = mission_action
    current = target
  end

  -- Return to end node
  if current ~= end_node then
    local path, cost = dijkstra.search(edges, current, end_node)
    if not path then
      error(string.format("No path from '%s' to end node '%s'", current, end_node))
    end
    total_cost = total_cost + cost

    local sub_paths = split_path_at_destinations(path, vn)
    for _, sp in ipairs(sub_paths) do
      local path_action = build_path_action(sp, edges, vn)

      if path_action.nav_method ~= "spline_follow" then
        local needed = path_action.heading_in
        if math.abs(needed - current_heading) > 1 then
          actions[#actions + 1] = build_rotate_action(current_heading, needed)
          current_heading = needed
        end
      end

      actions[#actions + 1] = path_action
      current_heading = path_action.heading_out
    end
  end

  -- Number actions
  for i, a in ipairs(actions) do
    a.step = i
  end

  -- Build virtual route (non-waypoint node names only)
  local vn_route = {}
  for _, a in ipairs(actions) do
    if a.action_type == "path" then
      if #vn_route == 0 or vn_route[#vn_route] ~= a.from_name then
        vn_route[#vn_route + 1] = a.from_name
      end
      vn_route[#vn_route + 1] = a.to_name
    elseif a.action_type == "mission" then
      if #vn_route == 0 or vn_route[#vn_route] ~= a.board_node then
        vn_route[#vn_route + 1] = a.board_node
      end
    elseif a.action_type == "init_check" then
      vn_route[#vn_route + 1] = start_node
    end
    -- path_rotate doesn't add to route (same position)
  end

  return {
    actions       = actions,
    virtual_route = vn_route,
    mission_order = mission_order,
    total_cost    = total_cost,
  }
end

---------------------------------------------------------------------------
-- Pretty print
---------------------------------------------------------------------------
function M.print_plan(plan)
  print("=== Global Plan ===")
  print(string.format("Mission order: %s", table.concat(plan.mission_order, " -> ")))
  print(string.format("Total cost: %d", plan.total_cost))
  print(string.format("Virtual actions: %d", #plan.actions))
  print()

  io.write("Virtual route: ")
  local col = 15
  for i, node in ipairs(plan.virtual_route) do
    local sep = (i < #plan.virtual_route) and " -> " or ""
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
  for _, a in ipairs(plan.actions) do
    if a.action_type == "init_check" then
      print(string.format("  %2d. [init   ] preflight check", a.step))

    elseif a.action_type == "path_rotate" then
      print(string.format("  %2d. [rotate ] %.0f -> %.0f deg",
        a.step, a.from_heading, a.to_heading))

    elseif a.action_type == "path" then
      print(string.format("  %2d. [path   ] %s -> %s  (dist: %d, max: %d)",
        a.step, a.from_name, a.to_name, a.total_distance, a.max_distance))
      print(string.format("       nav: %s, speed: %d, segments: %d, heading: %.0f->%.0f",
        a.nav_method, a.speed, #a.segments, a.heading_in, a.heading_out))
      print(string.format("       from: (%d,%d)  to: (%d,%d)",
        a.from.x, a.from.y, a.to.x, a.to.y))
      for j, seg in ipairs(a.segments) do
        local wpt_str = seg.waypoints and string.format(", %d wpts", #seg.waypoints) or ""
        print(string.format("         %d. (%d,%d)->(%d,%d) %s %dmm%s",
          j, seg.from.x, seg.from.y, seg.to.x, seg.to.y,
          seg.nav, seg.distance, wpt_str))
      end

    elseif a.action_type == "mission" then
      print(string.format("  %2d. [mission] %s at %s  (heading: %d, points: %d)",
        a.step, a.name, a.board_node, a.approach_heading, a.points or 0))
      if a.params then
        local parts = {}
        for k, v in pairs(a.params) do
          parts[#parts + 1] = string.format("%s=%s", k, tostring(v))
        end
        table.sort(parts)
        print("       params: " .. table.concat(parts, ", "))
      end
    end
  end
end

---------------------------------------------------------------------------
-- Serialization
---------------------------------------------------------------------------
function M.write_json(plan, filepath)
  local json_str = json_util.encode(plan)
  local fh, err = io.open(filepath, "w")
  if not fh then error(string.format("Cannot open '%s': %s", filepath, err)) end
  fh:write(json_str)
  fh:write("\n")
  fh:close()
  print(string.format("[global_planner] Wrote JSON: %s (%d bytes)", filepath, #json_str))
end

function M.write_yaml(plan, filepath)
  yaml_dumper.dump_to_file(plan, filepath)
end

return M
