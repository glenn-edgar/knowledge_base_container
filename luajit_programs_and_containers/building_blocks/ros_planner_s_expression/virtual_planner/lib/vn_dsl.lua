-- vn_dsl.lua -- Virtual node DSL builder
--
-- Construction API for virtual node graphs. Validates inputs at
-- definition time to ensure consistent data for the planner.
--
-- Usage:
--   local vn = require("vn_dsl")
--   vn.board("workshop_floor", 2400, 1200)
--
--   vn.node("launch",    0,    0,    "base")
--   vn.node("assembly",  1600, 400,  "mission")
--   vn.node("warehouse", 0,    400,  "waypoint")
--
--   vn.path("launch", "assembly", "spline_follow", 150,
--           { {0,0}, {400,10}, {1200,-5}, {1600,400} })
--   vn.path("launch", "warehouse", "line_follow", 120)
--
--   vn.mission("deliver_part", "assembly", 270, 15, {
--     arm_target = -45, arm_speed = 80, payload = "part", arm_return = 0,
--   })
--
--   return vn.build()

local M = {}

-- Internal state
local board_name = nil
local board_dims = nil
local nodes = {}
local edges = {}
local catalog = {}

-- Valid values
local valid_kinds = { base = true, mission = true, waypoint = true }
local valid_nav   = {
  spline_follow = true,
  line_follow   = true,
  wall_ride     = true,
}

---------------------------------------------------------------------------
-- Reset state (for reuse / testing)
---------------------------------------------------------------------------
function M.reset()
  board_name = nil
  board_dims = nil
  nodes = {}
  edges = {}
  catalog = {}
end

---------------------------------------------------------------------------
-- Board declaration
---------------------------------------------------------------------------
function M.board(name, width_mm, height_mm)
  assert(type(name) == "string", "board: name must be a string")
  assert(type(width_mm) == "number" and width_mm > 0, "board: width must be positive number")
  assert(type(height_mm) == "number" and height_mm > 0, "board: height must be positive number")
  board_name = name
  board_dims = { x = width_mm, y = height_mm }
end

---------------------------------------------------------------------------
-- Board node definition
---------------------------------------------------------------------------
function M.node(name, x, y, kind)
  assert(type(name) == "string", "node: name must be a string")
  assert(type(x) == "number", "node: x must be a number")
  assert(type(y) == "number", "node: y must be a number")
  assert(valid_kinds[kind], string.format(
    "node '%s': kind must be base/mission/waypoint, got '%s'", name, tostring(kind)))
  assert(not nodes[name], string.format(
    "node '%s': duplicate node name", name))

  nodes[name] = { x = x, y = y, kind = kind }
end

---------------------------------------------------------------------------
-- Path (board edge) definition
---------------------------------------------------------------------------
-- All paths are bidirectional.
-- Weight is auto-calculated from Euclidean distance if not provided.
-- Waypoints are optional Catmull-Rom control points.
function M.path(from, to, nav_method, speed, waypoints, weight, nav_data)
  assert(nodes[from], string.format("path: unknown node '%s'", from))
  assert(nodes[to],   string.format("path: unknown node '%s'", to))
  assert(from ~= to,  string.format("path: self-loop on '%s'", from))
  assert(valid_nav[nav_method], string.format(
    "path '%s'->'%s': invalid nav method '%s'", from, to, tostring(nav_method)))
  assert(type(speed) == "number" and speed > 0, string.format(
    "path '%s'->'%s': speed must be positive number", from, to))

  -- Validate waypoints if provided
  if waypoints then
    assert(type(waypoints) == "table", string.format(
      "path '%s'->'%s': waypoints must be a table", from, to))
    for i, wp in ipairs(waypoints) do
      assert(type(wp) == "table" and type(wp[1]) == "number" and type(wp[2]) == "number",
        string.format("path '%s'->'%s': waypoint %d must be {x, y}", from, to, i))
    end
  end

  -- Validate nav_data if provided
  if nav_data then
    assert(type(nav_data) == "table", string.format(
      "path '%s'->'%s': nav_data must be a table", from, to))
  end

  -- Auto-calculate weight from Euclidean distance if not provided
  if not weight then
    local dx = nodes[to].x - nodes[from].x
    local dy = nodes[to].y - nodes[from].y
    weight = math.floor(math.sqrt(dx * dx + dy * dy) + 0.5)
  end
  assert(type(weight) == "number" and weight > 0, string.format(
    "path '%s'->'%s': weight must be positive number", from, to))

  -- Check for duplicate edge
  for _, e in ipairs(edges) do
    if (e[1] == from and e[2] == to) or (e[1] == to and e[2] == from) then
      error(string.format("path '%s'->'%s': duplicate edge", from, to))
    end
  end

  -- Edge format: { from, to, weight, nav_method, speed, waypoints, bidir, nav_data }
  edges[#edges + 1] = { from, to, weight, nav_method, speed, waypoints, true, nav_data }
end

---------------------------------------------------------------------------
-- Mission (catalog entry) definition
---------------------------------------------------------------------------
function M.mission(name, board_node, approach_heading, points, params, preconditions)
  assert(type(name) == "string", "mission: name must be a string")
  assert(not catalog[name], string.format(
    "mission '%s': duplicate catalog entry", name))
  assert(nodes[board_node], string.format(
    "mission '%s': unknown board node '%s'", name, tostring(board_node)))
  assert(nodes[board_node].kind == "mission", string.format(
    "mission '%s': board node '%s' is not kind 'mission'", name, board_node))
  assert(type(approach_heading) == "number", string.format(
    "mission '%s': approach_heading must be a number", name))
  assert(type(points) == "number" and points >= 0, string.format(
    "mission '%s': points must be non-negative number", name))
  assert(type(params) == "table", string.format(
    "mission '%s': params must be a table", name))

  -- Validate preconditions reference existing catalog entries
  preconditions = preconditions or {}
  for _, pre in ipairs(preconditions) do
    assert(catalog[pre], string.format(
      "mission '%s': precondition '%s' not yet defined (define it first)", name, pre))
  end

  catalog[name] = {
    type             = "mission",
    board_node       = board_node,
    approach_heading = approach_heading,
    points           = points,
    params           = params,
    preconditions    = preconditions,
  }
end

---------------------------------------------------------------------------
-- Build and validate the final descriptor
---------------------------------------------------------------------------
function M.build()
  assert(board_name, "build: no board declared (call vn.board() first)")

  -- Verify all mission board_nodes are reachable
  -- (at least one edge touches each mission node)
  for name, entry in pairs(catalog) do
    local bn = entry.board_node
    local found = false
    for _, e in ipairs(edges) do
      if e[1] == bn or e[2] == bn then
        found = true
        break
      end
    end
    assert(found, string.format(
      "build: mission '%s' board node '%s' has no connecting edges", name, bn))
  end

  return {
    name        = board_name,
    dimensions  = board_dims,
    board_nodes = nodes,
    edges       = edges,
    catalog     = catalog,
  }
end

---------------------------------------------------------------------------
-- Summary print
---------------------------------------------------------------------------
function M.summary()
  local node_count = 0
  local mission_count = 0
  local waypoint_count = 0
  for _, n in pairs(nodes) do
    node_count = node_count + 1
    if n.kind == "mission" then mission_count = mission_count + 1 end
    if n.kind == "waypoint" then waypoint_count = waypoint_count + 1 end
  end
  local cat_count = 0
  for _ in pairs(catalog) do cat_count = cat_count + 1 end

  print(string.format("Board: %s (%dx%d mm)", board_name or "?",
    board_dims and board_dims.x or 0, board_dims and board_dims.y or 0))
  print(string.format("Nodes: %d (%d mission, %d waypoint)",
    node_count, mission_count, waypoint_count))
  print(string.format("Edges: %d (all bidirectional)", #edges))
  print(string.format("Catalog: %d missions", cat_count))
end

return M
