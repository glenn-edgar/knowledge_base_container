--[[
    route_builder.lua -- Convert a node path to a sequencer-compatible route.

    Takes an array of node names and a graph, produces an array of
    { kb_name, params } actions the sequencer can execute directly.

    Inserts path_rotate actions when heading changes between segments.
    Heading convention: atan2(dy, dx) degrees — 0=east, 90=north.

    Usage:
        local rb = require("route_builder")
        local route = rb.build({"lander_pad", "habitat_site"}, graph, { bookend = true })
]]

local M = {}

---------------------------------------------------------------------------
-- Nav method → KB action name mapping
---------------------------------------------------------------------------
M.NAV_TO_KB = {
    spline_follow = "path_spline",
    line_follow   = "path_line",
    wall_follow   = "path_wall",
}

---------------------------------------------------------------------------
-- Geometry helpers
---------------------------------------------------------------------------

function M.compute_heading(ax, ay, bx, by)
    local dx = bx - ax
    local dy = by - ay
    if dx == 0 and dy == 0 then return 0 end
    local deg = math.deg(math.atan2(dy, dx))
    if deg < 0 then deg = deg + 360 end
    return deg
end

function M.compute_distance(ax, ay, bx, by)
    local dx = bx - ax
    local dy = by - ay
    return math.sqrt(dx * dx + dy * dy)
end

--- Normalized heading difference in [-180, 180].
function M.heading_diff(a, b)
    local d = (b - a) % 360
    if d > 180 then d = d - 360 end
    return d
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
-- Route builder
---------------------------------------------------------------------------

--- Build a route from a node path.
-- @param node_path  array of node name strings
-- @param graph      { nodes = {name={x,y}}, adj = {name={{to=,nav=,speed=,weight=}}} }
-- @param opts       optional table:
--   bookend          bool: wrap with init_check + idle (default false)
--   initial_heading  number: current heading (default 0)
-- @return route     array of { kb_name=string, params=table }
function M.build(node_path, graph, opts)
    opts = opts or {}
    local heading = opts.initial_heading or 0
    local route = {}

    -- Optional init_check bookend
    if opts.bookend then
        route[#route + 1] = { kb_name = "init_check", params = {} }
    end

    -- Walk consecutive pairs
    for i = 1, #node_path - 1 do
        local from_name = node_path[i]
        local to_name   = node_path[i + 1]
        local from_node = graph.nodes[from_name]
        local to_node   = graph.nodes[to_name]

        if not from_node or not to_node then
            error("route_builder: unknown node '" ..
                (from_node and to_name or from_name) .. "'")
        end

        -- Find edge for nav method and speed
        local edge = find_edge(graph.adj, from_name, to_name)
        if not edge then
            error("route_builder: no edge from '" .. from_name ..
                "' to '" .. to_name .. "'")
        end

        -- Compute segment heading
        local seg_heading = M.compute_heading(
            from_node.x, from_node.y, to_node.x, to_node.y)

        -- Insert rotation if heading changed
        if math.abs(M.heading_diff(heading, seg_heading)) > 0.5 then
            route[#route + 1] = {
                kb_name = "path_rotate",
                params = {
                    from_heading = heading,
                    to_heading   = seg_heading,
                },
            }
            heading = seg_heading
        end

        -- Path action
        local kb_name = M.NAV_TO_KB[edge.nav] or "path_spline"
        local distance = M.compute_distance(
            from_node.x, from_node.y, to_node.x, to_node.y)

        local params = {
            from_x   = from_node.x,
            from_y   = from_node.y,
            to_x     = to_node.x,
            to_y     = to_node.y,
            speed    = edge.speed,
            distance = math.floor(distance + 0.5),
        }

        if kb_name == "path_spline" then
            params.segment_index  = 0
            params.total_segments = 1
        end

        route[#route + 1] = { kb_name = kb_name, params = params }
    end

    -- Optional idle bookend
    if opts.bookend then
        route[#route + 1] = { kb_name = "idle", params = {} }
    end

    return route
end

return M
