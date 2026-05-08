#!/usr/bin/env luajit
-- =============================================================================
-- test_board_dsl_c2.lua -- Phase 4 C2 path-tree DSL.
--
-- Coverage:
--   - typed segment constructors: each of the 5 sub-segment kinds
--   - activate{} leaf
--   - add_edge{path = {...}} with constructor-tagged tables
--   - raw-table rejection in path
--   - empty path rejection
--   - fold_path: consecutive sub-segments collapse into ONE drive leaf
--   - fold_path: activate breaks drive sequences correctly
--   - capability union check: undeclared activate.action_id rejected
--   - line-number-aware errors on constructor misuse
--   - constructor strictness: unknown fields rejected per kind
-- =============================================================================

local SCRIPT_DIR = arg[0]:match("(.*/)") or "./"
package.path = SCRIPT_DIR .. "../../scripts/board_dsl/?.lua;" .. package.path

local bd = require("board_dsl")

local pass, fail = 0, 0
local function ok(name, cond, detail)
  if cond then pass = pass + 1; print("  ok  " .. name)
  else fail = fail + 1; print("  FAIL " .. name .. (detail and " -- " .. detail or "")) end
end
local function expect_error(name, fn, want)
  local good, err = pcall(fn)
  if good then fail = fail + 1; print("  FAIL " .. name .. " -- expected error containing " .. want); return end
  local s = tostring(err)
  if not s:find(want, 1, true) then
    fail = fail + 1; print("  FAIL " .. name .. " -- err " .. s .. " missing " .. want); return
  end
  pass = pass + 1; print("  ok  " .. name)
end

local REGION = { {x=0,y=0}, {x=10,y=0}, {x=10,y=10}, {x=0,y=10} }
local function fresh()
  local b = bd.new{ name = "t", region = REGION }
  b:declare_capabilities{ "recharge", "dock_in", "dock_out" }
  b:add_node{ name = "n1", x = 1, y = 1 }
  b:add_node{ name = "n2", x = 5, y = 5,
              kb_ref = "system.x.site.s.infrastructure.registry.active_node_def.dock_v1" }
  return b
end

------------------------------------------------------------------------
print("== typed segment constructors: shape ==")
------------------------------------------------------------------------

do
  local s = bd.straight_line{ end_pos = {x=1, y=2} }
  ok("straight_line tag",   s.__tag == "drive_seg")
  ok("straight_line kind",  s.kind == "straight_line")
  ok("straight_line end_pos", s.end_pos.x == 1 and s.end_pos.y == 2)
end
do
  local s = bd.spline{ end_pos = {x=2, y=3}, end_heading = 1.5,
                       speed = 0.4, direction = "reverse" }
  ok("spline tag", s.__tag == "drive_seg" and s.kind == "spline")
  ok("spline preserves direction", s.direction == "reverse")
end
do
  local s = bd.rotate{ end_heading = 3.14 }
  ok("rotate tag", s.__tag == "drive_seg" and s.kind == "rotate")
end
do
  local s = bd.wall_follow{
    base = { kind = "straight_line", end_pos = {x=4,y=0} },
    offset = 0.3 }
  ok("wall_follow tag", s.__tag == "drive_seg" and s.kind == "wall_follow")
  ok("wall_follow base preserved",
     s.base.kind == "straight_line" and s.base.end_pos.x == 4)
end
do
  local s = bd.line_follow{
    base = { kind = "spline", end_pos = {x=4,y=2}, end_heading = 0 } }
  ok("line_follow with spline base",
     s.__tag == "drive_seg" and s.base.kind == "spline")
end
do
  local s = bd.activate{ action_id = "recharge", params = { target_soc = 0.85 } }
  ok("activate tag", s.__tag == "activate")
  ok("activate action_id", s.action_id == "recharge")
  ok("activate default empty params", s.params.target_soc == 0.85)
end
do
  local s = bd.activate{ action_id = "dock_in" }
  ok("activate omits params -> empty table",
     type(s.params) == "table" and next(s.params) == nil)
end

------------------------------------------------------------------------
print()
print("== constructor strictness: unknown fields rejected ==")
------------------------------------------------------------------------

expect_error("straight_line unknown field", function()
  bd.straight_line{ end_pos = {x=1,y=0}, end_heading = 0 }
end, "unknown field")
expect_error("spline missing end_heading", function()
  bd.spline{ end_pos = {x=1,y=0} }
end, "end_heading required")
expect_error("rotate with end_pos", function()
  bd.rotate{ end_heading = 0, end_pos = {x=1,y=0} }
end, "unknown field")
expect_error("wall_follow nested composite base", function()
  bd.wall_follow{ base = { kind = "wall_follow", end_pos = {x=1,y=0} },
                  offset = 0.3 }
end, "not allowed")
expect_error("line_follow with offset", function()
  bd.line_follow{ base = { kind = "straight_line", end_pos = {x=1,y=0} },
                  offset = 0.3 }
end, "not allowed")
expect_error("activate missing action_id", function()
  bd.activate{ params = {} }
end, "action_id required")
expect_error("bad direction", function()
  bd.straight_line{ end_pos = {x=1,y=0}, direction = "sideways" }
end, "direction must be")

------------------------------------------------------------------------
print()
print("== add_edge.path: raw tables rejected ==")
------------------------------------------------------------------------

expect_error("path with raw table", function()
  local b = fresh()
  b:add_edge{ from = "n1", to = "n2",
              path = { { kind = "straight_line", end_pos = {x=2,y=2} } } }
end, "must come from bd.straight_line")

expect_error("path empty", function()
  local b = fresh()
  b:add_edge{ from = "n1", to = "n2", path = {} }
end, "is empty")

expect_error("path not a table", function()
  local b = fresh()
  b:add_edge{ from = "n1", to = "n2", path = "drive" }
end, "must be a list")

------------------------------------------------------------------------
print()
print("== happy: drive-only path (single multi-segment leaf) ==")
------------------------------------------------------------------------

do
  local b = fresh()
  b:add_edge{
    from = "n1", to = "n2",
    path = {
      bd.straight_line{ end_pos = {x=2, y=2} },
      bd.spline{ end_pos = {x=4, y=4}, end_heading = 0.5 },
      bd.rotate{ end_heading = 1.57 },
    },
  }
  local out = b:build()
  local p = out.edges[1].path
  ok("one drive leaf", #p == 1 and p[1].kind == "drive")
  ok("drive leaf has 3 segments", #p[1].segments == 3)
  ok("first segment straight_line", p[1].segments[1].kind == "straight_line")
  ok("third segment rotate", p[1].segments[3].kind == "rotate")
  ok("declared_at stripped", p[1].segments[1].declared_at == nil)
  ok("__tag stripped", p[1].segments[1].__tag == nil)
end

------------------------------------------------------------------------
print()
print("== happy: drive + activate + drive (3 leaves after fold) ==")
------------------------------------------------------------------------

do
  local b = fresh()
  b:add_edge{
    from = "n1", to = "n2",
    path = {
      bd.straight_line{ end_pos = {x=2, y=2} },
      bd.spline{ end_pos = {x=4, y=4}, end_heading = 0.5 },
      bd.activate{ action_id = "recharge",
                   kb_ref = "system.x.site.s.infrastructure.registry.active_node_def.dock_v1",
                   params = { target_soc = 0.85 } },
      bd.straight_line{ end_pos = {x=6, y=6} },
    },
  }
  local out = b:build()
  local p = out.edges[1].path
  ok("3 leaves", #p == 3)
  ok("leaf 1 = drive", p[1].kind == "drive")
  ok("leaf 1 has 2 segs", #p[1].segments == 2)
  ok("leaf 2 = activate", p[2].kind == "activate")
  ok("leaf 2 action_id", p[2].action_id == "recharge")
  ok("leaf 2 params kept", p[2].params.target_soc == 0.85)
  ok("leaf 3 = drive", p[3].kind == "drive")
  ok("leaf 3 has 1 seg", #p[3].segments == 1)
end

------------------------------------------------------------------------
print()
print("== happy: activate-first then drive ==")
------------------------------------------------------------------------

do
  local b = fresh()
  b:add_edge{
    from = "n1", to = "n2",
    path = {
      bd.activate{ action_id = "dock_in", params = {} },
      bd.straight_line{ end_pos = {x=3, y=3} },
    },
  }
  local out = b:build()
  local p = out.edges[1].path
  ok("activate-first 2 leaves", #p == 2)
  ok("leaf 1 = activate", p[1].kind == "activate")
  ok("leaf 2 = drive (single)", p[2].kind == "drive" and #p[2].segments == 1)
end

------------------------------------------------------------------------
print()
print("== capability union check ==")
------------------------------------------------------------------------

expect_error("activate.action_id not in capabilities", function()
  local b = bd.new{ name = "t", region = REGION }
  b:declare_capabilities{ "recharge" }
  b:add_node{ name = "n1", x = 1, y = 1 }
  b:add_node{ name = "n2", x = 5, y = 5 }
  b:add_edge{
    from = "n1", to = "n2",
    path = { bd.activate{ action_id = "dock_in" } },
  }
  b:build()
end, "not in board capabilities")

------------------------------------------------------------------------
print()
print("== line-number-aware on constructor misuse ==")
------------------------------------------------------------------------

do
  local good, err = pcall(function()
    bd.spline{ end_pos = {x=1,y=0} }   -- missing end_heading
  end)
  ok("spline error mentions file:line",
     not good and tostring(err):find("%.lua:%d+"),
     not good and tostring(err))
end

------------------------------------------------------------------------
print()
print(string.format("SUMMARY: %d passed, %d failed", pass, fail))
os.exit(fail > 0 and 1 or 0)
