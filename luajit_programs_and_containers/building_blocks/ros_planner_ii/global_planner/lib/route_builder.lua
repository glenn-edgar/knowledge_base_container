--[[
    route_builder.lua -- Convert a node path to a sequencer-compatible route.

    Takes an array of node names and a graph, produces an array of
    { kb_name, params, energy } actions the sequencer can execute directly.

    The edge nav field IS the kb_name directly (path_spline, path_line).
    Each edge carries a flat path array [x1,y1, x2,y2, ...] built by
    the board DSL. Route params are just {speed, path}.

    Energy per action:
      Path VNs: distance * energy_factor * energy_rate
      Fixed VNs (init_check, idle, operation): energy_cost from VN definition

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
-- Route builder
---------------------------------------------------------------------------

--- Build a route from a node path.
-- @param node_path  array of node name strings
-- @param graph      { nodes = {name={x,y}}, adj = {name={{to=,nav=,speed=,weight=,path=}}} }
-- @param opts       optional table:
--   bookend          bool: wrap with init_check + idle (default false)
--   energy_rate      number: robot's energy per unit distance (default 1.0)
--   vn_defs          table: kb_name → { energy_factor, energy_cost } from KB VN defs
-- @return route     array of { kb_name=string, params=table, energy=number }
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
            params = {},
            energy = def.energy_cost or 0,
        }
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

        local kb_name = edge.nav or "path_spline"
        local def = vn_defs[kb_name] or {}
        local dist = M.path_distance(edge.path)
        local factor = def.energy_factor or 1.0
        local energy = math.floor(dist * factor * energy_rate + 0.5)

        route[#route + 1] = {
            kb_name = kb_name,
            params = {
                speed = edge.speed,
                path  = edge.path,
            },
            energy = energy,
        }
    end

    -- Optional idle bookend
    if opts.bookend then
        local def = vn_defs["idle"] or {}
        route[#route + 1] = {
            kb_name = "idle",
            params = {},
            energy = def.energy_cost or 0,
        }
    end

    return route
end

return M
