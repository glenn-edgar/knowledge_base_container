#!/usr/bin/env luajit
-- =============================================================================
-- test_board_dsl_c1.lua -- host-side smoke for Phase 4 C1 of the board DSL.
--
-- Coverage:
--   - point-in-polygon: rectangle + L-shape concavity
--   - happy: minimal board with 4 nodes + 3 edges + capabilities
--   - mutation: bad region (too few points, missing x/y)
--   - mutation: node out of region
--   - mutation: duplicate node name
--   - mutation: duplicate edge (from,to)
--   - mutation: edge self-loop
--   - mutation: edge endpoint refers to undeclared node
--   - mutation: declare_capabilities with duplicate id
--   - mutation: unknown field on add_node / add_edge / new
--   - line-number-aware errors: original declaration line is included
--
-- Usage:   luajit construction/tests/board_dsl/test_board_dsl_c1.lua
-- Exit:    0 = all green; non-zero = at least one failure.
-- =============================================================================

local SCRIPT_DIR = arg[0]:match("(.*/)") or "./"
package.path = SCRIPT_DIR .. "../../scripts/board_dsl/?.lua;" .. package.path

local board_dsl = require("board_dsl")

local pass, fail = 0, 0
local function ok(name, cond, detail)
  if cond then pass = pass + 1; print("  ok  " .. name)
  else fail = fail + 1; print("  FAIL " .. name .. (detail and " -- " .. detail or "")) end
end

local function expect_error(name, fn, want_substring)
  local good, err = pcall(fn)
  if good then
    fail = fail + 1
    print("  FAIL " .. name .. " -- expected error containing " .. want_substring)
    return
  end
  local err_s = tostring(err)
  if not err_s:find(want_substring, 1, true) then
    fail = fail + 1
    print("  FAIL " .. name .. " -- err " .. err_s .. " missing " .. want_substring)
    return
  end
  pass = pass + 1
  print("  ok  " .. name)
end

------------------------------------------------------------------------
print("== point-in-polygon ==")
------------------------------------------------------------------------

local rect = {
  {x = 0, y = 0}, {x = 10, y = 0}, {x = 10, y = 8}, {x = 0, y = 8},
}
ok("rect: (5,4) inside", board_dsl._point_in_polygon(5, 4, rect))
ok("rect: (-1,4) outside", not board_dsl._point_in_polygon(-1, 4, rect))
ok("rect: (5,9) outside", not board_dsl._point_in_polygon(5, 9, rect))
ok("rect: (15,4) outside", not board_dsl._point_in_polygon(15, 4, rect))

-- L-shape: outer rect [0,10]x[0,10] minus [5,10]x[5,10] (top-right notch)
local L = {
  {x = 0, y = 0}, {x = 10, y = 0}, {x = 10, y = 5},
  {x = 5, y = 5}, {x = 5, y = 10}, {x = 0, y = 10},
}
ok("L: (2,2) inside", board_dsl._point_in_polygon(2, 2, L))
ok("L: (8,2) inside", board_dsl._point_in_polygon(8, 2, L))
ok("L: (2,8) inside", board_dsl._point_in_polygon(2, 8, L))
ok("L: (8,8) outside (notch)", not board_dsl._point_in_polygon(8, 8, L))
ok("L: (7,7) outside (notch)", not board_dsl._point_in_polygon(7, 7, L))

------------------------------------------------------------------------
print()
print("== happy: minimal 4-node 3-edge board ==")
------------------------------------------------------------------------

local b = board_dsl.new{
  name = "warehouse_a",
  region = {
    {x = 0, y = 0}, {x = 10, y = 0}, {x = 10, y = 8}, {x = 0, y = 8},
  },
}
b:declare_capabilities{ "recharge", "dock_in", "dock_out" }
b:add_node{ name = "lander_pad", x = 1, y = 1, description = "starting pad" }
b:add_node{ name = "transit_a",  x = 4, y = 4 }
b:add_node{ name = "dock_3",     x = 7, y = 6,
            kb_ref = "system.moon_base.site.alpha.infrastructure.registry.active_node_def.dock_recharge_v1" }
b:add_node{ name = "shipping_b", x = 9, y = 1 }
b:add_edge{ from = "lander_pad", to = "transit_a" }
b:add_edge{ from = "transit_a",  to = "dock_3" }
b:add_edge{ from = "transit_a",  to = "shipping_b" }

local ok_build, out = pcall(b.build, b)
ok("build succeeded", ok_build, ok_build and "" or tostring(out))
if ok_build then
  ok("schema_version = 2",   out.schema_version == 2)
  ok("name preserved",       out.name == "warehouse_a")
  ok("4 nodes",              #out.nodes == 4)
  ok("3 edges",              #out.edges == 3)
  ok("3 capabilities",       #out.capabilities == 3)
  ok("dock_3 has kb_ref",    out.nodes[3].kb_ref ~= nil)
  ok("transit_a no kb_ref",  out.nodes[2].kb_ref == nil)
  ok("region copied (4 pts)", #out.region == 4)
  -- Edges have a `path` field (nil for C1-style edges with no path
  -- = {...} declared); C2 fills in the folded path tree.
  ok("edge[1].path nil (C1-style)", out.edges[1].path == nil)
end

------------------------------------------------------------------------
print()
print("== mutation: region ==")
------------------------------------------------------------------------

expect_error("region too few points", function()
  board_dsl.new{ name = "t", region = { {x=0,y=0}, {x=1,y=0} } }
end, ">= 3 points")

expect_error("region point missing x", function()
  board_dsl.new{ name = "t", region = { {x=0,y=0}, {y=1}, {x=2,y=2} } }
end, "numeric x and y")

expect_error("region point unknown field", function()
  board_dsl.new{ name = "t", region = { {x=0,y=0,z=0}, {x=1,y=0}, {x=2,y=2} } }
end, "unknown field")

expect_error("new() unknown field", function()
  board_dsl.new{ name = "t", region = { {x=0,y=0},{x=1,y=0},{x=0,y=1} },
                  bonus_field = 42 }
end, "unknown field")

------------------------------------------------------------------------
print()
print("== mutation: nodes ==")
------------------------------------------------------------------------

local function fresh_board()
  return board_dsl.new{
    name = "t", region = { {x=0,y=0},{x=10,y=0},{x=10,y=10},{x=0,y=10} } }
end

expect_error("node outside region", function()
  local b = fresh_board()
  b:add_node{ name = "out", x = 100, y = 100 }
end, "outside region polygon")

expect_error("duplicate node name", function()
  local b = fresh_board()
  b:add_node{ name = "n1", x = 1, y = 1 }
  b:add_node{ name = "n1", x = 2, y = 2 }
end, "already declared")

expect_error("node missing name", function()
  local b = fresh_board()
  b:add_node{ x = 1, y = 1 }
end, "name required")

expect_error("node unknown field", function()
  local b = fresh_board()
  b:add_node{ name = "n1", x = 1, y = 1, role = "magic" }
end, "unknown field")

------------------------------------------------------------------------
print()
print("== mutation: edges ==")
------------------------------------------------------------------------

expect_error("edge self-loop", function()
  local b = fresh_board()
  b:add_node{ name = "n1", x = 1, y = 1 }
  b:add_edge{ from = "n1", to = "n1" }
end, "self-loop")

expect_error("duplicate edge", function()
  local b = fresh_board()
  b:add_node{ name = "n1", x = 1, y = 1 }
  b:add_node{ name = "n2", x = 2, y = 2 }
  b:add_edge{ from = "n1", to = "n2" }
  b:add_edge{ from = "n1", to = "n2" }
end, "already declared")

expect_error("edge to undeclared node", function()
  local b = fresh_board()
  b:add_node{ name = "n1", x = 1, y = 1 }
  b:add_edge{ from = "n1", to = "ghost" }
  b:build()
end, "is not a declared node")

expect_error("edge with empty path = {} rejected", function()
  local b = fresh_board()
  b:add_node{ name = "n1", x = 1, y = 1 }
  b:add_node{ name = "n2", x = 2, y = 2 }
  b:add_edge{ from = "n1", to = "n2", path = {} }
end, "is empty -- omit the field")

expect_error("edge with bonus_field rejected", function()
  local b = fresh_board()
  b:add_node{ name = "n1", x = 1, y = 1 }
  b:add_node{ name = "n2", x = 2, y = 2 }
  b:add_edge{ from = "n1", to = "n2", bonus = 1 }
end, "unknown field")

------------------------------------------------------------------------
print()
print("== mutation: capabilities ==")
------------------------------------------------------------------------

expect_error("capability empty string", function()
  local b = fresh_board()
  b:declare_capabilities{ "recharge", "" }
end, "must be non-empty string")

expect_error("duplicate capability", function()
  local b = fresh_board()
  b:declare_capabilities{ "recharge", "recharge" }
end, "already declared")

------------------------------------------------------------------------
print()
print("== line-number-aware error messages ==")
------------------------------------------------------------------------

do
  local good, err = pcall(function()
    local b = fresh_board()
    b:add_node{ name = "n1", x = 1, y = 1 }   -- this is the "first at" line
    b:add_node{ name = "n1", x = 2, y = 2 }   -- this triggers the error
  end)
  ok("dup-node error mentions first declaration", not good and tostring(err):find("first at"),
     not good and tostring(err))
  ok("dup-node error mentions a file:line",
     not good and tostring(err):find("%.lua:%d+"),
     not good and tostring(err))
end

------------------------------------------------------------------------
print()
print(string.format("SUMMARY: %d passed, %d failed", pass, fail))
os.exit(fail > 0 and 1 or 0)
