--[[
    dijkstra.lua -- Dijkstra shortest path on an adjacency list.

    Pure algorithm, no KB or coordinate dependency.
    Linear-scan extract_min (O(n^2)) — fine for small graphs.

    Usage:
        local dijkstra = require("dijkstra")
        local path, cost = dijkstra.shortest_path(adj, "start", "goal")
        -- path = { "start", "mid", "goal" }
        -- cost = total weight

        -- With blocked edges:
        local blocked = { ["A|B"] = true, ["B|A"] = true }
        local path, cost = dijkstra.shortest_path(adj, "start", "goal", blocked)
]]

local M = {}

--- Find shortest path from start to goal.
-- @param adj      table: { node_name = { {to=string, weight=number, ...}, ... } }
-- @param start    string: start node name
-- @param goal     string: goal node name
-- @param blocked  table or nil: set of "from|to" keys to skip
-- @return path    array of node names (start to goal inclusive), or nil
-- @return cost    total weight, or math.huge if unreachable
function M.shortest_path(adj, start, goal, blocked)
    blocked = blocked or {}

    -- Validate nodes exist
    if not adj[start] then return nil, math.huge end
    if not adj[goal] then return nil, math.huge end

    -- Same node
    if start == goal then return { start }, 0 end

    -- Initialize distances
    local dist = {}
    local prev = {}
    local visited = {}

    for node, _ in pairs(adj) do
        dist[node] = math.huge
    end
    dist[start] = 0

    -- Main loop
    while true do
        -- Extract min: find unvisited node with smallest distance
        local u, u_dist = nil, math.huge
        for node, d in pairs(dist) do
            if not visited[node] and d < u_dist then
                u, u_dist = node, d
            end
        end

        -- No reachable unvisited nodes
        if not u then break end

        -- Reached goal
        if u == goal then break end

        visited[u] = true

        -- Relax neighbors
        for _, edge in ipairs(adj[u]) do
            local v = edge.to
            if not visited[v] then
                -- Check blocked
                local edge_key = u .. "|" .. v
                if not blocked[edge_key] then
                    local alt = dist[u] + edge.weight
                    if alt < dist[v] then
                        dist[v] = alt
                        prev[v] = u
                    end
                end
            end
        end
    end

    -- Reconstruct path
    if dist[goal] == math.huge then
        return nil, math.huge
    end

    local path = {}
    local node = goal
    while node do
        table.insert(path, 1, node)
        node = prev[node]
    end

    return path, dist[goal]
end

return M
