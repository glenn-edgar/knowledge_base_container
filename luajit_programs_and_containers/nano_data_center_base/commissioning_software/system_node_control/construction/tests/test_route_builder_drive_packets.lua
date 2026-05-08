#!/usr/bin/env luajit
-- =============================================================================
-- test_route_builder_drive_packets.lua -- Phase 5 C1 smoke for
-- route_builder.build_drive_packets().
--
-- Coverage:
--   - happy: 2-edge route, mixed nav (spline + line) + wall_follow
--   - per-edge ONE cmd_drive_t (vs legacy N-1 packets per polyline)
--   - chained packet_id (monotonic)
--   - chained start_pos heading: each packet's start_pos.heading == previous
--     packet's last segment end_heading
--   - stop_at_end: only set on last packet
--   - sub-segment shapes: spline carries end_heading, wall_follow has
--     base.straight_line + offset
--   - end_heading rule: not-last spline segment heading points TO next
--     polyline point; last spline segment heading is its own direction
--   - validates against command_packets.validate_drive
--   - mutation: empty node_path
--   - mutation: missing edge in graph
--   - mutation: edge.nav = "path_rotate" (unsupported in this builder)
--
-- Usage: luajit construction/tests/test_route_builder_drive_packets.lua
-- =============================================================================

local SCRIPT_DIR = arg[0]:match("(.*/)") or "./"
-- planner lib at: nano_data_center_instance/.../planner/lib/
-- protocol at:   nano_data_center_instance/.../planner/hub_dsl/protocol/
local PLANNER = SCRIPT_DIR
    .. "../../../../../nano_data_center_instance/app_containers/mission_planner/container/planner"
package.path = PLANNER .. "/lib/?.lua;"
            .. PLANNER .. "/hub_dsl/protocol/?.lua;"
            .. package.path

local rb   = require("route_builder")
local cmds = require("command_packets")

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

local function approx(a, b, tol)
  tol = tol or 1e-4
  return math.abs(a - b) < tol
end

------------------------------------------------------------------------
-- fixture graph
------------------------------------------------------------------------
-- Three nodes in a line; two edges:
--   n1 (0,0) -> n2 (10,0) via path_spline with 4-pt polyline
--   n2 (10,0) -> n3 (10,10) via path_line with simple 2-pt polyline
local FIXTURE_GRAPH = {
  nodes = {
    n1 = { x = 0,  y = 0  },
    n2 = { x = 10, y = 0  },
    n3 = { x = 10, y = 10 },
  },
  adj = {
    n1 = { { to = "n2", nav = "path_spline", speed = 0.40,
             path = { 0,0,  3,1,  7,2,  10,0 } } },  -- 3 segments
    n2 = { { to = "n3", nav = "path_line",   speed = 0.25,
             path = { 10,0,  10,10 } } },             -- 1 segment
    n3 = {},
  },
}

------------------------------------------------------------------------
print("== happy: 2-edge route ==")
------------------------------------------------------------------------

do
  local pkts = rb.build_drive_packets({"n1", "n2", "n3"}, FIXTURE_GRAPH,
    { packet_id_start = 100, mission_id = 7, initial_heading = 0,
      default_speed = 0.30 })
  ok("two packets emitted", #pkts == 2)

  local p1 = pkts[1]
  ok("p1 packet_type = TYPE_DRIVE", p1.packet_type == cmds.TYPE_DRIVE)
  ok("p1 packet_id = 100",          p1.packet_id == 100)
  ok("p1 mission_id = 7",           p1.mission_id == 7)
  ok("p1 start_pos.x = 0",          p1.start_pos.x == 0)
  ok("p1 start_pos.heading = 0",    p1.start_pos.heading == 0)
  ok("p1 default_speed = 0.40",     approx(p1.default_speed, 0.40))
  ok("p1 stop_at_end = false (not last edge)", p1.stop_at_end == false)
  ok("p1 has 3 spline segments",    #p1.segments == 3)
  ok("p1 seg[1] kind=spline",       p1.segments[1].kind == "spline")
  ok("p1 seg[1] end_pos = (3,1)",
     p1.segments[1].end_pos.x == 3 and p1.segments[1].end_pos.y == 1)
  ok("p1 seg[1] end_heading != 0 (points toward next pt)",
     not approx(p1.segments[1].end_heading, 0))

  -- Last spline segment's end_heading should equal its own direction
  -- (no "next" point in this polyline).
  local last = p1.segments[#p1.segments]
  -- Last seg goes (7,2) -> (10,0); direction = atan2(-2, 3)
  local expected = math.atan2(0 - 2, 10 - 7)
  ok("p1 last spline seg end_heading = own direction (last-segment rule)",
     approx(last.end_heading, expected),
     string.format("got %s want %s", tostring(last.end_heading),
       tostring(expected)))

  local p2 = pkts[2]
  ok("p2 packet_id = 101 (monotonic)", p2.packet_id == 101)
  ok("p2 start_pos.heading == p1 last end_heading (chained)",
     approx(p2.start_pos.heading, expected),
     string.format("got %s want %s", tostring(p2.start_pos.heading),
       tostring(expected)))
  ok("p2 default_speed = 0.25",     approx(p2.default_speed, 0.25))
  ok("p2 stop_at_end = true (last edge)", p2.stop_at_end == true)
  ok("p2 has 1 straight_line seg",  #p2.segments == 1)
  ok("p2 seg[1] kind=straight_line", p2.segments[1].kind == "straight_line")
end

------------------------------------------------------------------------
print()
print("== validate_drive: every emitted packet passes ==")
------------------------------------------------------------------------

do
  -- The builder calls validate_drive eagerly; if we got this far, packets
  -- are already valid. Exercise an external validate as a sanity check
  -- in case future changes silently bypass eager validation.
  local pkts = rb.build_drive_packets({"n1", "n2"}, FIXTURE_GRAPH)
  for i, p in ipairs(pkts) do
    local good, err = pcall(cmds.validate_drive, p)
    ok(string.format("packet %d validates", i), good,
       good and "" or tostring(err))
  end
end

------------------------------------------------------------------------
print()
print("== wall_follow mapping ==")
------------------------------------------------------------------------

do
  local g = {
    nodes = {
      a = { x = 0, y = 0 },
      b = { x = 5, y = 0 },
    },
    adj = {
      a = { { to = "b", nav = "path_wall", speed = 0.20,
              wall_standoff = -0.3,
              path = { 0,0,  2,0,  5,0 } } },
      b = {},
    },
  }
  local pkts = rb.build_drive_packets({"a", "b"}, g)
  ok("wall_follow: 1 packet, 2 segs", #pkts == 1 and #pkts[1].segments == 2)
  local s1 = pkts[1].segments[1]
  ok("wall_follow seg kind", s1.kind == "wall_follow")
  ok("wall_follow base kind", s1.base.kind == "straight_line")
  ok("wall_follow offset preserved (signed)", s1.offset == -0.3)
end

------------------------------------------------------------------------
print()
print("== mutations ==")
------------------------------------------------------------------------

expect_error("node_path < 2", function()
  rb.build_drive_packets({"n1"}, FIXTURE_GRAPH)
end, ">= 2 nodes")

expect_error("missing edge", function()
  rb.build_drive_packets({"n1", "n3"}, FIXTURE_GRAPH)
end, "no edge from")

expect_error("path_rotate edge unsupported", function()
  local g = {
    nodes = { x = {x=0,y=0}, y = {x=1,y=0} },
    adj = { x = {{ to = "y", nav = "path_rotate", speed = 0.1,
                    path = {0,0, 1,0} }}, y = {} },
  }
  rb.build_drive_packets({"x", "y"}, g)
end, "no sub_kind mapping")

expect_error("invalid polyline (odd-length)", function()
  local g = {
    nodes = { x = {x=0,y=0}, y = {x=1,y=0} },
    adj = { x = {{ to = "y", nav = "path_line", speed = 0.1,
                    path = {0,0, 1} }}, y = {} },
  }
  rb.build_drive_packets({"x", "y"}, g)
end, "invalid polyline")

------------------------------------------------------------------------
print()
print("== legacy build() still works (regression) ==")
------------------------------------------------------------------------

do
  local route = rb.build({"n1", "n2", "n3"}, FIXTURE_GRAPH,
    { energy_rate = 0.5, vn_defs = {} })
  -- 3 segs from n1->n2 polyline + 1 seg from n2->n3 polyline = 4 actions
  ok("legacy build emits 4 actions", #route == 4,
     "got " .. #route)
end

------------------------------------------------------------------------
print()
print(string.format("SUMMARY: %d passed, %d failed", pass, fail))
os.exit(fail > 0 and 1 or 0)
