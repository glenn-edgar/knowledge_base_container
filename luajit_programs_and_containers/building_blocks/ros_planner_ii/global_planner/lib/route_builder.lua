--[[
    route_builder.lua -- Convert a node path to a sequencer-compatible route.

    Takes an array of node names and a graph, produces an array of
    { kb_name, params } actions the sequencer can execute directly.

    The edge nav field IS the kb_name directly (path_spline, path_line).
    Each edge carries a flat path array [x1,y1, x2,y2, ...] built by
    the board DSL. Route params are just {speed, path}.

    Usage:
        local rb = require("route_builder")
        local route = rb.build({"lander_pad", "habitat_site"}, graph, { bookend = true })
]]

local M = {}

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
-- @param graph      { nodes = {name={x,y}}, adj = {name={{to=,nav=,speed=,weight=,path=}}} }
-- @param opts       optional table:
--   bookend          bool: wrap with init_check + idle (default false)
-- @return route     array of { kb_name=string, params=table }
function M.build(node_path, graph, opts)
    opts = opts or {}
    local route = {}

    -- Optional init_check bookend
    if opts.bookend then
        route[#route + 1] = { kb_name = "init_check", params = {} }
    end

    -- Walk consecutive pairs
    for i = 1, #node_path - 1 do
        local from_name = node_path[i]
        local to_name   = node_path[i + 1]

        if not graph.nodes[from_name] then
            error("route_builder: unknown node '" .. from_name .. "'")
        end
        if not graph.nodes[to_name] then
            error("route_builder: unknown node '" .. to_name .. "'")
        end

        -- Find edge for nav method, speed, and path
        local edge = find_edge(graph.adj, from_name, to_name)
        if not edge then
            error("route_builder: no edge from '" .. from_name ..
                "' to '" .. to_name .. "'")
        end

        route[#route + 1] = {
            kb_name = edge.nav or "path_spline",
            params = {
                speed = edge.speed,
                path  = edge.path,
            },
        }
    end

    -- Optional idle bookend
    if opts.bookend then
        route[#route + 1] = { kb_name = "idle", params = {} }
    end

    return route
end

return M
