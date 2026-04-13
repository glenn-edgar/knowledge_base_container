--[[
    board_builder.lua — Board DSL: build node graphs with validated paths.

    Nodes: { name, x, y, type, params? }
      type = "transit" → routing only, not a mission stop
      type = anything else → operation node (type IS the operation name)
      params = optional table of default operation parameters

    Edges: { from, to, nav, speed, weight, path }
      nav = "path_spline" or "path_line" (direct kb_name)
      path = {} for straight line (auto-interpolated) or {{x=,y=},...} for waypoints
      DSL prepends from-node coords and appends to-node coords automatically.

    Usage:
        local bb = require("board_builder")
        local data = bb.build(nodes, edges, { interpolate_n = 2 })
        kb:add_info_node("boards", "landing_zone", properties, data, description)
]]

local M = {}

--- Interpolate N evenly-spaced points between two coords.
-- Returns array of {x=,y=} tables (excludes endpoints).
function M.interpolate(ax, ay, bx, by, n)
    local pts = {}
    for i = 1, n do
        local t = i / (n + 1)
        pts[#pts + 1] = {
            x = math.floor(ax + (bx - ax) * t + 0.5),
            y = math.floor(ay + (by - ay) * t + 0.5),
        }
    end
    return pts
end

--- Build board data from nodes and edges.
-- Validates structure and resolves paths to flat arrays.
-- @param nodes   array of {name, x, y, type, params?}
-- @param edges   array of {from, to, nav, speed, weight, path}
-- @param opts    optional: { interpolate_n = 2 }
-- @return table  { nodes = [...], edges = [...] } ready for kb:add_info_node
function M.build(nodes, edges, opts)
    opts = opts or {}
    local interp_n = opts.interpolate_n
    if interp_n == nil then interp_n = 2 end

    -- Build node lookup
    local lookup = {}
    for _, n in ipairs(nodes) do
        assert(n.name, "board_builder: node missing 'name'")
        assert(n.x, "board_builder: node '" .. n.name .. "' missing 'x'")
        assert(n.y, "board_builder: node '" .. n.name .. "' missing 'y'")
        assert(n.type, "board_builder: node '" .. n.name .. "' missing 'type'")
        assert(not lookup[n.name],
            "board_builder: duplicate node '" .. n.name .. "'")
        lookup[n.name] = n
    end

    -- Validate and resolve edges
    local seen_edges = {}
    for _, e in ipairs(edges) do
        assert(e.from, "board_builder: edge missing 'from'")
        assert(e.to, "board_builder: edge missing 'to'")
        assert(e.nav, "board_builder: edge " .. e.from .. "→" .. e.to .. " missing 'nav'")
        assert(e.speed, "board_builder: edge " .. e.from .. "→" .. e.to .. " missing 'speed'")
        assert(e.weight, "board_builder: edge " .. e.from .. "→" .. e.to .. " missing 'weight'")
        assert(e.path, "board_builder: edge " .. e.from .. "→" .. e.to .. " missing 'path'")

        local fn = lookup[e.from]
        local tn = lookup[e.to]
        assert(fn, "board_builder: edge references unknown node '" .. e.from .. "'")
        assert(tn, "board_builder: edge references unknown node '" .. e.to .. "'")

        -- Check for duplicate undirected edges
        local lo = e.from < e.to and e.from or e.to
        local hi = e.from < e.to and e.to or e.from
        local ek = lo .. "|" .. hi
        assert(not seen_edges[ek],
            "board_builder: duplicate edge " .. e.from .. " ↔ " .. e.to)
        seen_edges[ek] = true

        -- Resolve path: author's waypoints or auto-interpolate
        local waypoints = e.path
        if #waypoints == 0 and interp_n > 0 then
            waypoints = M.interpolate(fn.x, fn.y, tn.x, tn.y, interp_n)
        end

        -- Build flat array: [from_x, from_y, ...waypoints..., to_x, to_y]
        local flat = { fn.x, fn.y }
        for _, wp in ipairs(waypoints) do
            assert(wp.x and wp.y,
                "board_builder: waypoint in edge " .. e.from .. "→" .. e.to .. " missing x or y")
            flat[#flat + 1] = wp.x
            flat[#flat + 1] = wp.y
        end
        flat[#flat + 1] = tn.x
        flat[#flat + 1] = tn.y
        e.path = flat
    end

    return { nodes = nodes, edges = edges }
end

return M
