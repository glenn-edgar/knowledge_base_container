--[[
    route_builder.lua -- Convert a node path to a sequencer-compatible route.

    Two coexisting builders during the Phase 5 transition:

    1. M.build()                  legacy single-tenant path. Each board
       (Phase B + earlier)        edge polyline becomes (N-1) per-segment
                                  cmd_path_*_t actions. Removed in Phase 5
                                  C5 once the new path is robot-validated.

    2. M.build_drive_packets()    Phase 5 C1. Each edge polyline becomes
       (Phase 5 onward)           ONE cmd_drive_t with N-1 chained
                                  sub-segments. Per-PACKET completion
                                  semantics; one ACK per edge instead of
                                  N-1 acks per polyline.

    Both functions live here so the existing planner code keeps working
    while the new dispatch path is wired up + integration-tested
    against a real robot. The cut-over is a separate commit (C5)
    behind a feature flag so we never have a window where neither path
    works.

    Energy per action (legacy path):
      Path VNs : per-segment distance * energy_factor * energy_rate
      Fixed VNs (init_check, idle) : energy_cost from VN definition

    Usage (legacy):
        local rb = require("route_builder")
        local route = rb.build({"lander_pad", "habitat_site"}, graph, {
            bookend = true, energy_rate = 0.5, vn_defs = {...}
        })

    Usage (Phase 5):
        local packets = rb.build_drive_packets(
            {"lander_pad", "habitat_site"}, graph, {
              packet_id_start = 1, mission_id = 42, initial_heading = 0,
            })
        -- packets[i] is a cmd_drive_t Lua table for the i-th edge
        -- (validates against command_packets.validate_drive)
]]

local M = {}

---------------------------------------------------------------------------
-- Path distance from flat array [x1,y1, x2,y2, ...]
---------------------------------------------------------------------------

function M.path_distance(flat)
    if not flat or #flat < 4 then return 0 end
    local total = 0
    for i = 1, #flat - 2, 2 do
        local dx = flat[i + 2] - flat[i]
        local dy = flat[i + 3] - flat[i + 1]
        total = total + math.sqrt(dx * dx + dy * dy)
    end
    return total
end

---------------------------------------------------------------------------
-- Edge lookup
---------------------------------------------------------------------------

local function find_edge(adj, from_name, to_name)
    local neighbors = adj[from_name]
    if not neighbors then return nil end
    for _, edge in ipairs(neighbors) do
        if edge.to == to_name then return edge end
    end
    return nil
end

---------------------------------------------------------------------------
-- cmd_drive_t builder (one packet per edge)
---------------------------------------------------------------------------

-- TYPE_DRIVE id from command_packets.lua. Lazy require so the module
-- can still be loaded host-side even if the protocol dir isn't on
-- package.path (e.g. unit tests that don't exercise the new path).
local _cmds
local function cmds_module()
  if not _cmds then _cmds = require("command_packets") end
  return _cmds
end

-- Map legacy edge.nav strings to new sub-segment kinds.
local NAV_TO_SUB_KIND = {
  path_spline = "spline",
  path_line   = "straight_line",
  path_wall   = "wall_follow",
  -- path_rotate intentionally omitted: rotate edges have no polyline,
  -- so they don't fit the per-edge polyline model. Phase 5 supports
  -- rotate via mission_builder inserting bd.rotate{} leaves directly,
  -- not via this builder.
}

-- Compute end_heading for a spline sub-segment ending at (tx, ty).
-- For a polyline pts[1..N*2], the heading at the end of segment j is
-- the direction from (tx, ty) to the NEXT polyline point. For the
-- last segment in the polyline, heading = the segment's own direction
-- (since there is no "next").
local function end_heading_for(pts, j, fx, fy, tx, ty)
  if j + 4 <= #pts then
    return math.atan2(pts[j + 5] - ty, pts[j + 4] - tx)
  end
  -- last segment: own direction
  return math.atan2(ty - fy, tx - fx)
end

-- Build a single sub-segment table. Returns (segment, new_heading_at_end).
local function build_sub_segment(sub_kind, edge, pts, j, fx, fy, tx, ty)
  if sub_kind == "straight_line" then
    return { kind = "straight_line", end_pos = { x = tx, y = ty } },
           math.atan2(ty - fy, tx - fx)

  elseif sub_kind == "spline" then
    local eh = end_heading_for(pts, j, fx, fy, tx, ty)
    return { kind = "spline", end_pos = { x = tx, y = ty },
             end_heading = eh }, eh

  elseif sub_kind == "wall_follow" then
    -- offset: legacy edge.wall_standoff is signed (sign = side of wall).
    -- Default 0 means "pure follow with zero offset" -- which is
    -- effectively a line_follow, but we keep wall_follow + offset=0
    -- semantics until a board explicitly distinguishes the two.
    local offset = edge.wall_standoff or 0
    return { kind = "wall_follow",
             base = { kind = "straight_line", end_pos = { x = tx, y = ty } },
             offset = offset }, math.atan2(ty - fy, tx - fx)

  else
    error(string.format(
      "route_builder: unknown sub_kind %q (mapped from edge.nav=%q)",
      tostring(sub_kind), tostring(edge.nav)))
  end
end

--- Build a list of cmd_drive_t packets, one per edge.
-- @param node_path  array of node name strings (>= 2 elements)
-- @param graph      same shape as M.build's graph arg
-- @param opts       optional table:
--   packet_id_start  uint32; first packet's packet_id (default 1).
--                    Subsequent packets monotonic +1.
--   mission_id       uint32; copied to every packet (optional).
--   initial_heading  float; robot's heading at start of first edge
--                    (default 0).
--   default_speed    float; fallback when an edge has no .speed.
--                    (default 0.30 m/s).
-- @return packets    list of cmd_drive_t tables. Each one PASSES
--                    command_packets.validate_drive(); caller can
--                    immediately encoder.encode_drive() and ship.
function M.build_drive_packets(node_path, graph, opts)
  opts = opts or {}
  if #node_path < 2 then
    error("route_builder.build_drive_packets: node_path must have >= 2 nodes")
  end

  local cmds = cmds_module()
  local packets = {}
  local packet_id = opts.packet_id_start or 1
  local prev_heading = opts.initial_heading or 0
  local fallback_speed = opts.default_speed or 0.30

  for i = 1, #node_path - 1 do
    local from_name = node_path[i]
    local to_name   = node_path[i + 1]

    if not graph.nodes[from_name] then
      error("route_builder: unknown node '" .. from_name .. "'")
    end
    if not graph.nodes[to_name] then
      error("route_builder: unknown node '" .. to_name .. "'")
    end

    local edge = find_edge(graph.adj, from_name, to_name)
    if not edge then
      error(string.format(
        "route_builder: no edge from %q to %q", from_name, to_name))
    end

    local nav = edge.nav or "path_spline"
    local sub_kind = NAV_TO_SUB_KIND[nav]
    if not sub_kind then
      error(string.format(
        "route_builder.build_drive_packets: edge.nav=%q has no sub_kind " ..
        "mapping (path_rotate edges not supported in this builder)", nav))
    end

    local pts = edge.path
    if not pts or #pts < 4 or (#pts % 2) ~= 0 then
      error(string.format(
        "route_builder: edge %s->%s invalid polyline (len=%d)",
        from_name, to_name, pts and #pts or 0))
    end

    local from_node = graph.nodes[from_name]
    local segments = {}
    local heading_at_end = prev_heading
    for j = 1, #pts - 2, 2 do
      local seg, new_heading = build_sub_segment(
        sub_kind, edge, pts, j,
        pts[j], pts[j + 1], pts[j + 2], pts[j + 3])
      segments[#segments + 1] = seg
      heading_at_end = new_heading
    end

    local pkt = {
      packet_type   = cmds.TYPE_DRIVE,
      packet_id     = packet_id,
      mission_id    = opts.mission_id,
      start_pos     = { x = from_node.x, y = from_node.y,
                        heading = prev_heading },
      default_speed = edge.speed or fallback_speed,
      stop_at_end   = (i == #node_path - 1),
      segments      = segments,
    }
    -- Validate eagerly so a malformed packet errors at build time, not
    -- inside the wire encoder where the stack trace is harder.
    cmds.validate_drive(pkt)

    packets[#packets + 1] = pkt
    packet_id = packet_id + 1
    prev_heading = heading_at_end
  end

  return packets
end

---------------------------------------------------------------------------
-- Phase 5 C3b: route entries with kind discriminator
---------------------------------------------------------------------------

-- build_v2 wraps build_drive_packets() output as kind-discriminated
-- route entries that the action_server dispatch loop can consume
-- directly. The entry shape is:
--
--   { kind = "drive_packet", packet = <cmd_drive_t>, energy = N, leg_idx = i }
--
-- Legacy entries (built by M.build) have no `kind` field and carry
-- `kb_name + params`; the dispatch loop treats absent `kind` as legacy.
-- This lets a single mission's route mix legacy bookends (init_check /
-- idle / per-stop operation VNs) with drive-packet nav entries.
--
-- Energy per drive-packet entry is the sum of the legacy per-segment
-- energies for the same edge, computed identically (distance *
-- energy_factor * energy_rate, then floored), so the energy budget
-- check matches between v1 and v2 dispatch.
--
-- @param node_path  same as build_drive_packets
-- @param graph      same as build_drive_packets
-- @param opts       same as build_drive_packets, PLUS:
--   energy_rate     number; default 1.0 (matches M.build)
--   vn_defs         table; per-kb_name energy_factor (from KB)
-- @return entries   array of { kind="drive_packet", packet, energy }
function M.build_v2(node_path, graph, opts)
  opts = opts or {}
  local energy_rate = opts.energy_rate or 1.0
  local vn_defs = opts.vn_defs or {}

  local packets = M.build_drive_packets(node_path, graph, opts)

  -- Compute per-edge energy by summing the polyline-segment costs;
  -- mirrors M.build's per-segment energy formula so the v2 budget
  -- equals the v1 budget for the same path.
  local entries = {}
  for i, pkt in ipairs(packets) do
    local from_name = node_path[i]
    local to_name   = node_path[i + 1]
    local edge      = find_edge(graph.adj, from_name, to_name)
    -- find_edge returned non-nil already (build_drive_packets would
    -- have errored otherwise); pull energy_factor and the polyline.
    local kb_name = edge.nav or "path_spline"
    local def     = vn_defs[kb_name] or {}
    local factor  = def.energy_factor or 1.0
    local pts     = edge.path

    local energy_total = 0
    for j = 1, #pts - 2, 2 do
      local dx = pts[j + 2] - pts[j]
      local dy = pts[j + 3] - pts[j + 1]
      local dist = math.sqrt(dx * dx + dy * dy)
      energy_total = energy_total
        + math.floor(dist * factor * energy_rate + 0.5)
    end

    entries[#entries + 1] = {
      kind   = "drive_packet",
      packet = pkt,
      energy = energy_total,
    }
  end

  return entries
end

return M
