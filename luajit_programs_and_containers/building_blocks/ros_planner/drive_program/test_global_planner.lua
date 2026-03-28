-- test_global_planner.lua -- Test the global planner
--
-- Loads virtual node graph and missions, runs the global planner,
-- prints the resulting plan.

package.path = package.path .. ";?.lua"

local dijkstra       = require("dijkstra")
local global_planner = require("global_planner")

-- Load DSLs
local vn       = dofile("virtual_node_dsl.lua")
local missions = dofile("mission_dsl.lua")

print("=== Virtual Node Graph ===")

-- Count nodes
local board_count = 0
for _ in pairs(vn.board_nodes) do board_count = board_count + 1 end
local arm_count = 0
for _ in pairs(vn.arm_nodes) do arm_count = arm_count + 1 end
print(string.format("Board nodes: %d", board_count))
print(string.format("Arm nodes:   %d", arm_count))
print(string.format("Edges:       %d", #vn.edges))
print()

-- Test individual Dijkstra searches (board-only)
print("=== Dijkstra Tests (all edges) ===")
local test_pairs = {
  { "launch", "assembly" },
  { "launch", "painting" },
  { "assembly", "shipping" },
  { "inspection", "launch" },
}

for _, pair in ipairs(test_pairs) do
  local path, cost = dijkstra.search(vn.edges, pair[1], pair[2])
  if path then
    print(string.format("  %s -> %s: cost=%d, path=%s",
      pair[1], pair[2], cost, table.concat(path, " -> ")))
  else
    print(string.format("  %s -> %s: NO PATH", pair[1], pair[2]))
  end
end
print()

-- Test arm paths
print("=== Dijkstra Tests (arm transitions) ===")
local arm_pairs = {
  { "assembly", "arm_deliver" },
  { "arm_deliver", "arm_home" },
  { "painting", "arm_paint_down" },
  { "arm_paint_down", "arm_paint_up" },
}

for _, pair in ipairs(arm_pairs) do
  local path, cost = dijkstra.search(vn.edges, pair[1], pair[2])
  if path then
    print(string.format("  %s -> %s: cost=%d, path=%s",
      pair[1], pair[2], cost, table.concat(path, " -> ")))
  else
    print(string.format("  %s -> %s: NO PATH", pair[1], pair[2]))
  end
end
print()

-- Run the global planner
local plan = global_planner.plan(missions, missions.strategy, vn)
global_planner.print_plan(plan, vn)

-- Write output files
print()
global_planner.write_json(plan, vn, "global_plan.json")
global_planner.write_yaml(plan, vn, "global_plan.yaml")
