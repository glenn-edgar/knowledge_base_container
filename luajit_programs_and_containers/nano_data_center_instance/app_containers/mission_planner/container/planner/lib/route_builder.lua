--[[
    route_builder.lua -- Convert a node path to a sequencer-compatible route.

    Takes an array of node names and a graph, produces an array of
    { kb_name, params, energy } actions the sequencer can execute directly.

    Each board edge carries a flat polyline path = [x0,y0, x1,y1, ..., xn,yn].
    route_builder explodes the polyline into ONE action per pairwise segment
    so the per-action params shape matches the robot's wire packets:

      cmd_path_spline_t : { from_x, from_y, to_x, to_y, speed, distance,
                            segment_index, total_segments }
      cmd_path_line_t   : { from_x, from_y, to_x, to_y, speed, distance }
      cmd_path_wall_t   : { from_x, from_y, to_x, to_y, speed, distance,
                            wall_standoff }

    Hub_runtime copies every key from params straight onto the JSON wire
    payload, so each emitted action becomes one robot-bound packet.

    Energy per action:
      Path VNs : per-segment distance * energy_factor * energy_rate
      Fixed VNs (init_check, idle) : energy_cost from VN definition

    Usage:
        local rb = require("route_builder")
        local route = rb.build({"lander_pad", "habitat_site"}, graph, {
            bookend = true, energy_rate = 0.5, vn_defs = {...}
        })
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
-- Per-nav-type segment action builder
---------------------------------------------------------------------------

local function build_segment_action(edge, fx, fy, tx, ty, energy_factor,
                                    energy_rate, seg_idx, total_segs)
    local dx = tx - fx
    local dy = ty - fy
    local dist = math.sqrt(dx * dx + dy * dy)
    local energy = math.floor(dist * energy_factor * energy_rate + 0.5)

    local kb_name = edge.nav or "path_spline"
    local params = {
        from_x   = fx,
        from_y   = fy,
        to_x     = tx,
        to_y     = ty,
        speed    = edge.speed,
        distance = dist,
    }

    -- segment_index / total_segments: only path_spline carries them on the
    -- wire (cmd_path_spline_t). Hub_runtime forwards extras verbatim, but
    -- we emit only the fields each packet's C struct actually decodes.
    if kb_name == "path_spline" then
        params.segment_index   = seg_idx
        params.total_segments  = total_segs
    end

    -- wall_standoff: only path_wall carries it. Source from edge.wall_standoff.
    if kb_name == "path_wall" and edge.wall_standoff ~= nil then
        params.wall_standoff = edge.wall_standoff
    end

    return { kb_name = kb_name, params = params, energy = energy }
end

---------------------------------------------------------------------------
-- Route builder
---------------------------------------------------------------------------

--- Build a route from a node path.
-- @param node_path  array of node name strings
-- @param graph      { nodes = {name={x,y}}, adj = {name={{to=,nav=,speed=,weight=,path=,wall_standoff=?}}} }
-- @param opts       optional table:
--   bookend          bool: wrap with init_check + idle (default false)
--   energy_rate      number: robot's energy per unit distance (default 1.0)
--   vn_defs          table: kb_name → { energy_factor, energy_cost } from KB VN defs
-- @return route     array of { kb_name=string, params=table, energy=number }
--
-- Each board edge becomes (N-1) actions where N is the number of points in
-- the edge's polyline. Each action carries the wire-shaped params for one
-- pairwise segment of that polyline.
function M.build(node_path, graph, opts)
    opts = opts or {}
    local energy_rate = opts.energy_rate or 1.0
    local vn_defs = opts.vn_defs or {}
    local route = {}

    -- Optional init_check bookend
    if opts.bookend then
        local def = vn_defs["init_check"] or {}
        route[#route + 1] = {
            kb_name = "init_check",
            params  = {},
            energy  = def.energy_cost or 0,
        }
    end

    -- Walk consecutive node pairs in the planned path
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
            error("route_builder: no edge from '" .. from_name ..
                "' to '" .. to_name .. "'")
        end

        local kb_name = edge.nav or "path_spline"
        local def = vn_defs[kb_name] or {}
        local factor = def.energy_factor or 1.0

        local pts = edge.path
        if not pts or #pts < 4 or (#pts % 2) ~= 0 then
            error(string.format(
                "route_builder: edge %s→%s has invalid polyline (len=%d)",
                from_name, to_name, pts and #pts or 0))
        end

        local total_segs = (#pts / 2) - 1   -- N points → N-1 segments

        -- Emit one action per pairwise segment.
        for j = 1, #pts - 2, 2 do
            local seg_idx = ((j - 1) / 2) + 1
            route[#route + 1] = build_segment_action(
                edge,
                pts[j],     pts[j + 1],
                pts[j + 2], pts[j + 3],
                factor, energy_rate,
                seg_idx, total_segs)
        end
    end

    -- Optional idle bookend
    if opts.bookend then
        local def = vn_defs["idle"] or {}
        route[#route + 1] = {
            kb_name = "idle",
            params  = {},
            energy  = def.energy_cost or 0,
        }
    end

    return route
end

return M
