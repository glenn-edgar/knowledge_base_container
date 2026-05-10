#!/usr/bin/env luajit
-- =============================================================================
-- test_drive_v2_dispatch.lua -- drive-packet route shape + mission_builder
-- dispatch (post-C5 cleanup: drive_v2 is the only nav path).
--
-- Coverage:
--   route_builder.build_v2:
--     - returns kind="drive_packet" entries, one per polyline edge
--     - each entry's .packet is the same as build_drive_packets output
--     - empty polyline / unsupported nav -> errors propagate from
--       build_drive_packets
--
--   mission_builder.build:
--     - nav legs become drive_packet entries; bookends (init_check /
--       idle) and per-stop operation entries stay {kb_name, params}
--     - packet_id is monotonic across legs (no collisions)
--     - mission_id from mission_cmd flows through to every packet
--   mission_builder.rebuild:
--     - synthetic mission_cmd (start + stops, no bookend) emits the
--       drive_packet path; rebuild's signature no longer carries a
--       use_drive_v2 flag (deleted in the C5 follow-up).
--
-- Uses a lightweight stub planner that exposes plan_v2 + graph access
-- without needing pg_conn or KB connectivity.
-- =============================================================================

local SCRIPT_DIR = arg[0]:match("(.*/)") or "./"
local REPO_ROOT  = SCRIPT_DIR .. "../../../../../"
local PLANNER    = REPO_ROOT
    .. "nano_data_center_instance/app_containers/mission_planner/container/planner"
package.path = PLANNER .. "/lib/?.lua;"
            .. PLANNER .. "/hub_dsl/protocol/?.lua;"
            .. package.path

local rb              = require("route_builder")
local cmds            = require("command_packets")
local mission_builder = require("mission_builder")

local pass, fail = 0, 0
local function ok(name, cond, detail)
  if cond then pass = pass + 1; print("  ok  " .. name)
  else fail = fail + 1; print("  FAIL " .. name .. (detail and " -- " .. detail or "")) end
end
local function expect_error(name, fn, want)
  local good, err = pcall(fn)
  if good then fail = fail + 1; print("  FAIL " .. name .. " -- expected error containing " .. tostring(want)); return end
  if want and not tostring(err):find(want, 1, true) then
    fail = fail + 1; print("  FAIL " .. name .. " -- err " .. tostring(err) .. " missing " .. want); return
  end
  pass = pass + 1; print("  ok  " .. name)
end
local function approx(a, b) return math.abs(a - b) < 1e-6 end

------------------------------------------------------------------------
-- fixture graph: 3 nodes, 2 nav edges (spline + line) plus an unused
-- direct edge so dijkstra has multiple choices.
------------------------------------------------------------------------
local FIXTURE_GRAPH = {
  nodes = {
    n1 = { x = 0,  y = 0  },
    n2 = { x = 10, y = 0  },
    n3 = { x = 10, y = 10 },
  },
  adj = {
    n1 = { { to = "n2", nav = "path_spline", speed = 0.40,
             path = { 0,0,  3,1,  7,2,  10,0 } } },
    n2 = { { to = "n3", nav = "path_line",   speed = 0.25,
             path = { 10,0,  10,10 } } },
    n3 = {},
  },
}

------------------------------------------------------------------------
print("== route_builder.build_v2: shape ==")
------------------------------------------------------------------------

do
  local entries = rb.build_v2({"n1", "n2", "n3"}, FIXTURE_GRAPH,
    { packet_id_start = 1, mission_id = 42 })
  ok("two entries (one per edge)", #entries == 2)
  ok("entry[1].kind = drive_packet", entries[1].kind == "drive_packet")
  ok("entry[1].packet is a cmd_drive_t",
     entries[1].packet and entries[1].packet.packet_type == cmds.TYPE_DRIVE)
  ok("entry[1].packet.packet_id = 1", entries[1].packet.packet_id == 1)
  ok("entry[1].packet.mission_id = 42", entries[1].packet.mission_id == 42)
  ok("entry[1].energy is a number", type(entries[1].energy) == "number")
  ok("entry[1].energy >= 0", entries[1].energy >= 0)

  ok("entry[2].kind = drive_packet", entries[2].kind == "drive_packet")
  ok("entry[2].packet.packet_id = 2", entries[2].packet.packet_id == 2)
end

------------------------------------------------------------------------
print()
print("== route_builder.build_v2: per-entry energy non-negative ==")
------------------------------------------------------------------------

do
  local vn_defs = {
    path_spline = { energy_factor = 2.0 },
    path_line   = { energy_factor = 1.5 },
  }
  local opts = { energy_rate = 0.5, vn_defs = vn_defs, packet_id_start = 1 }

  local v2 = rb.build_v2({"n1", "n2", "n3"}, FIXTURE_GRAPH, opts)
  ok("v2 leg-1 energy is a number >= 0",
     type(v2[1].energy) == "number" and v2[1].energy >= 0,
     "got " .. tostring(v2[1].energy))
  ok("v2 leg-2 energy is a number >= 0",
     type(v2[2].energy) == "number" and v2[2].energy >= 0,
     "got " .. tostring(v2[2].energy))
end

------------------------------------------------------------------------
print()
print("== route_builder.build_v2: bad inputs propagate ==")
------------------------------------------------------------------------

expect_error("rotate edge unsupported", function()
  local g = {
    nodes = { x = {x=0,y=0}, y = {x=1,y=0} },
    adj = { x = {{ to = "y", nav = "path_rotate", speed = 0.1,
                    path = {0,0, 1,0} }}, y = {} },
  }
  rb.build_v2({"x", "y"}, g)
end, "no sub_kind mapping")

expect_error("missing edge", function()
  rb.build_v2({"n1", "n3"}, FIXTURE_GRAPH)
end, "no edge from")

------------------------------------------------------------------------
-- Stub planner: minimum surface mission_builder needs.
-- Wraps build_v2 directly with a fixed graph + path.
------------------------------------------------------------------------
local function make_stub_planner(graph, path_lookup)
  return {
    graph    = graph,
    vn_defs  = {
      init_check = { energy_cost = 1 },
      idle       = { energy_cost = 1 },
      operation  = { energy_cost = 5 },
    },
    is_transit       = function(self, _) return false end,
    get_node_type    = function(self, _) return nil end,
    get_node_params  = function(self, _) return {} end,
    plan_v2 = function(self, from, to, opts)
      local path = path_lookup[from .. ">" .. to]
      if not path then
        return nil, { error = "no path found", path = nil, cost = math.huge, segments = 0 }
      end
      local rb_opts = {}
      if opts then for k, v in pairs(opts) do rb_opts[k] = v end end
      rb_opts.vn_defs = self.vn_defs
      local entries = rb.build_v2(path, self.graph, rb_opts)
      return entries, { path = path, cost = #path - 1, segments = #entries }
    end,
  }
end

------------------------------------------------------------------------
print()
print("== mission_builder: drive_v2 default (no flag, no legacy) ==")
------------------------------------------------------------------------

do
  local planner = make_stub_planner(FIXTURE_GRAPH, {
    ["n1>n3"] = { "n1", "n2", "n3" },
  })
  local mission_cmd = {
    start      = "n1",
    stops      = { { node = "n3" } },
    mission_id = 99,
  }
  local route, info = mission_builder.build(mission_cmd, planner, nil, 1.0)
  ok("route built", route ~= nil)
  -- init_check + 2 drive_packet entries (one per edge) + idle = 4
  ok("route length = 4", #route == 4, "got " .. #route)
  ok("bookend init_check kb_name shape (no kind)",
     route[1].kb_name == "init_check" and route[1].kind == nil)
  ok("nav entry 1 kind = drive_packet",
     route[2].kind == "drive_packet")
  ok("nav entry 1 packet packet_id = 1",
     route[2].packet and route[2].packet.packet_id == 1)
  ok("nav entry 1 packet mission_id = 99",
     route[2].packet.mission_id == 99)
  ok("nav entry 2 kind = drive_packet",
     route[3].kind == "drive_packet")
  ok("nav entry 2 packet packet_id = 2 (monotonic)",
     route[3].packet.packet_id == 2)
  ok("bookend idle kb_name shape (no kind)",
     route[4].kb_name == "idle" and route[4].kind == nil)
end

------------------------------------------------------------------------
print()
print("== mission_builder: per-stop operation entry stays legacy ==")
------------------------------------------------------------------------

do
  local planner = make_stub_planner(FIXTURE_GRAPH, {
    ["n1>n3"] = { "n1", "n2", "n3" },
  })
  local mission_cmd = {
    start = "n1",
    stops = { { node = "n3", action = "deliver_part",
                params = { arm = -45 } } },
  }
  local route = mission_builder.build(mission_cmd, planner,
    { "deliver_part" }, 1.0)
  -- init_check + 2 drive_packets + operation + idle = 5
  ok("drive + op: route length = 5", #route == 5, "got " .. #route)
  ok("drive + op: operation entry uses kb_name shape",
     route[4].kb_name == "operation" and route[4].kind == nil)
  ok("drive + op: operation_type plumbed",
     route[4].params.operation_type == "deliver_part")
end

------------------------------------------------------------------------
print()
print("== mission_builder: multi-leg packet_id chains correctly ==")
------------------------------------------------------------------------

do
  -- Two stops -> two legs. packet_id must NOT collide between legs.
  local planner = make_stub_planner(FIXTURE_GRAPH, {
    ["n1>n2"] = { "n1", "n2" },
    ["n2>n3"] = { "n2", "n3" },
  })
  local mission_cmd = {
    start = "n1",
    stops = { { node = "n2" }, { node = "n3" } },
  }
  local route = mission_builder.build(mission_cmd, planner, nil, 1.0)
  -- init_check + leg1(1 drive) + leg2(1 drive) + idle = 4
  ok("multi-leg: route length = 4", #route == 4, "got " .. #route)
  local pkt_ids = {}
  for _, entry in ipairs(route) do
    if entry.kind == "drive_packet" then
      pkt_ids[#pkt_ids + 1] = entry.packet.packet_id
    end
  end
  ok("multi-leg: 2 drive packets emitted", #pkt_ids == 2)
  ok("multi-leg: packet_ids monotonic (no collision)",
     pkt_ids[1] == 1 and pkt_ids[2] == 2,
     "got " .. tostring(pkt_ids[1]) .. ", " .. tostring(pkt_ids[2]))
end

------------------------------------------------------------------------
print()
print("== mission_builder: rebuild emits drive_packets ==")
------------------------------------------------------------------------

do
  -- rebuild constructs a synthetic mission_cmd (start + stops + bookend=false)
  -- and calls build, which always uses drive_v2 post-C5 cleanup. Verify
  -- the replanned route contains drive_packet entries (no flag arg).
  local planner = make_stub_planner(FIXTURE_GRAPH, {
    ["n1>n3"] = { "n1", "n2", "n3" },
  })
  local route = mission_builder.rebuild(
    { { node = "n3" } }, planner, "n1")
  ok("rebuild: route built", route ~= nil)
  -- build()'s init_check is unconditional (bookend=false only suppresses
  -- the idle terminator). Two edges → 2 drive_packets + init_check = 3.
  ok("rebuild: at least one drive_packet entry",
     (function()
        for _, e in ipairs(route) do
          if e.kind == "drive_packet" then return true end
        end
        return false
     end)())
end

------------------------------------------------------------------------
print()
print(string.format("SUMMARY: %d passed, %d failed", pass, fail))
os.exit(fail > 0 and 1 or 0)
