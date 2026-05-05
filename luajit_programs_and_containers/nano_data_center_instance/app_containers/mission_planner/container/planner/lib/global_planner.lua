--[[
    global_planner.lua -- Board graph routing with replan support.

    Reads the board graph from SQLite KB, builds a bidirectional adjacency
    structure, runs Dijkstra to find shortest path, and generates a route
    compatible with sequencer:load_route().

    Replanning: mark edges as blocked, re-run Dijkstra from current node.
    Like car navigation — patch around obstruction or full replan.

    Usage:
        local gp = require("global_planner")
        local planner = gp.new({
            db_file    = "surface_ops.db",
            board_name = "landing_zone",
        })

        local route, info = planner:plan("lander_pad", "mining_zone_b",
            { bookend = true })

        -- On fault: replan
        planner:mark_blocked("habitat_site", "charging_station")
        local new_route = planner:replan("habitat_site", "mining_zone_b",
            { initial_heading = 90 })

        planner:close()
]]

local kb_query      = require("kb_query")
local dijkstra      = require("dijkstra")
local route_builder = require("route_builder")

local M = {}
M.__index = M

---------------------------------------------------------------------------
-- Build bidirectional graph from KB board data
---------------------------------------------------------------------------
local function build_graph(board_data)
    local graph = { nodes = {}, adj = {} }

    for _, node in ipairs(board_data.nodes) do
        graph.nodes[node.name] = {
            x = node.x, y = node.y, type = node.type,
            params = node.params,
        }
        graph.adj[node.name] = {}
    end

    for _, edge in ipairs(board_data.edges) do
        -- Forward
        local fwd = graph.adj[edge.from]
        fwd[#fwd + 1] = {
            to     = edge.to,
            nav    = edge.nav,
            speed  = edge.speed,
            weight = edge.weight,
            path   = edge.path,
        }
        -- Reverse: same nav, speed, weight; path is reversed
        local rev_path = nil
        if edge.path then
            rev_path = {}
            for i = #edge.path - 1, 1, -2 do
                rev_path[#rev_path + 1] = edge.path[i]
                rev_path[#rev_path + 1] = edge.path[i + 1]
            end
        end
        local rev = graph.adj[edge.to]
        rev[#rev + 1] = {
            to     = edge.from,
            nav    = edge.nav,
            speed  = edge.speed,
            weight = edge.weight,
            path   = rev_path,
        }
    end

    return graph
end

---------------------------------------------------------------------------
-- Constructor
---------------------------------------------------------------------------
function M.new(opts)
    local self = setmetatable({}, M)

    local db_file    = opts.db_file    or error("global_planner: db_file required")
    local board_name = opts.board_name or error("global_planner: board_name required")
    local ltree_path = opts.ltree_path or "/usr/local/lib/ltree"
    local site       = opts.site

    local q = kb_query.new(db_file, "knowledge_base", ltree_path, site)
    local board_data = q:get_board(board_name)

    -- Load VN definitions for energy computation
    self.vn_defs = {}
    local all_vns = q:get_all_virtual_nodes()
    if all_vns then
        for name, vn in pairs(all_vns) do
            self.vn_defs[name] = {
                energy_factor = vn.energy_factor,
                energy_cost   = vn.energy_cost,
            }
        end
    end

    q:close()

    if not board_data then
        error("global_planner: board '" .. board_name .. "' not found in KB")
    end

    self.graph = build_graph(board_data)
    self.blocked = {}
    self.board_name = board_name

    return self
end

---------------------------------------------------------------------------
-- Planning
---------------------------------------------------------------------------

--- Plan a route from start_node to goal_node.
-- @param start_node  string
-- @param goal_node   string
-- @param opts        optional: { bookend=bool, initial_heading=number }
-- @return route      array for sequencer:load_route(), or nil
-- @return info       { path=array, cost=number, segments=number }
function M:plan(start_node, goal_node, opts)
    if not self.graph.nodes[start_node] then
        return nil, { error = "unknown start node: " .. start_node }
    end
    if not self.graph.nodes[goal_node] then
        return nil, { error = "unknown goal node: " .. goal_node }
    end

    local path, cost = dijkstra.shortest_path(
        self.graph.adj, start_node, goal_node, self.blocked)

    if not path then
        return nil, { path = nil, cost = math.huge, segments = 0,
                      error = "no path found" }
    end

    -- Merge energy config into route_builder opts
    local rb_opts = {}
    if opts then for k, v in pairs(opts) do rb_opts[k] = v end end
    rb_opts.vn_defs = self.vn_defs
    if not rb_opts.energy_rate then rb_opts.energy_rate = 1.0 end

    local route = route_builder.build(path, self.graph, rb_opts)

    -- Sum route energy
    local total_energy = 0
    for _, action in ipairs(route) do
        total_energy = total_energy + (action.energy or 0)
    end

    return route, {
        path     = path,
        cost     = cost,
        segments = #route,
        energy   = total_energy,
    }
end

--- Replan from current_node to goal_node, respecting blocked edges.
-- Semantically identical to plan() but distinct for caller clarity.
function M:replan(current_node, goal_node, opts)
    return self:plan(current_node, goal_node, opts)
end

---------------------------------------------------------------------------
-- Blocked edge management
---------------------------------------------------------------------------

--- Block an edge in both directions.
function M:mark_blocked(from_node, to_node)
    self.blocked[from_node .. "|" .. to_node] = true
    self.blocked[to_node .. "|" .. from_node] = true
end

--- Block an edge in one direction only.
function M:mark_blocked_oneway(from_node, to_node)
    self.blocked[from_node .. "|" .. to_node] = true
end

--- Clear all blocked edges.
function M:clear_blocked()
    self.blocked = {}
end

---------------------------------------------------------------------------
-- Accessors
---------------------------------------------------------------------------

function M:get_graph()
    return self.graph
end

function M:get_blocked()
    return self.blocked
end

--- Check if a node is a transit-only node (not a valid mission stop).
-- @param node_name  string
-- @return bool
function M:is_transit(node_name)
    local node = self.graph.nodes[node_name]
    return node ~= nil and node.type == "transit"
end

--- Get the operation type for a node (its type field, nil for transit).
-- @param node_name  string
-- @return string or nil
function M:get_node_type(node_name)
    local node = self.graph.nodes[node_name]
    if not node or node.type == "transit" then return nil end
    return node.type
end

--- Get default operation params for a node (nil if none).
-- @param node_name  string
-- @return table or nil
function M:get_node_params(node_name)
    local node = self.graph.nodes[node_name]
    return node and node.params
end

--- Find the nearest graph node to a coordinate.
-- Used for mid-edge replanning when the robot is between nodes.
-- @param x  number
-- @param y  number
-- @return node_name string, distance number
function M:find_nearest_node(x, y)
    local best_name, best_dist = nil, math.huge
    for name, node in pairs(self.graph.nodes) do
        local dx = node.x - x
        local dy = node.y - y
        local d = math.sqrt(dx * dx + dy * dy)
        if d < best_dist then
            best_name, best_dist = name, d
        end
    end
    return best_name, best_dist
end

function M:close()
    -- No persistent connections to close (KB was read-only at construction)
end

return M
