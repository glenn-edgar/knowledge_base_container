-- dijkstra.lua -- Bidirectional Dijkstra over board graph
--
-- Operates on the board graph from virtual_node_dsl.lua.
-- Edge format: { from, to, weight, nav_method, speed, waypoints, bidir }

local M = {}

---------------------------------------------------------------------------
-- Build adjacency list from edge list
---------------------------------------------------------------------------
function M.build_adjacency(edges)
  local adj = {}
  for _, e in ipairs(edges) do
    local from, to, weight = e[1], e[2], e[3]
    local bidir = e[7]
    if not adj[from] then adj[from] = {} end
    adj[from][#adj[from] + 1] = { to = to, weight = weight, edge = e }
    if bidir then
      if not adj[to] then adj[to] = {} end
      adj[to][#adj[to] + 1] = { to = from, weight = weight, edge = e }
    end
  end
  return adj
end

---------------------------------------------------------------------------
-- Min-heap priority queue
---------------------------------------------------------------------------
local function pq_new() return { entries = {} } end

local function pq_push(pq, node, cost)
  local entries = pq.entries
  entries[#entries + 1] = { node = node, cost = cost }
  local i = #entries
  while i > 1 do
    local parent = math.floor(i / 2)
    if entries[parent].cost > entries[i].cost then
      entries[parent], entries[i] = entries[i], entries[parent]
      i = parent
    else
      break
    end
  end
end

local function pq_pop(pq)
  local entries = pq.entries
  if #entries == 0 then return nil end
  local top = entries[1]
  entries[1] = entries[#entries]
  entries[#entries] = nil
  local i = 1
  while true do
    local left = 2 * i
    local right = 2 * i + 1
    local smallest = i
    if left <= #entries and entries[left].cost < entries[smallest].cost then
      smallest = left
    end
    if right <= #entries and entries[right].cost < entries[smallest].cost then
      smallest = right
    end
    if smallest ~= i then
      entries[i], entries[smallest] = entries[smallest], entries[i]
      i = smallest
    else
      break
    end
  end
  return top.node, top.cost
end

local function pq_empty(pq) return #pq.entries == 0 end

---------------------------------------------------------------------------
-- Bidirectional Dijkstra
---------------------------------------------------------------------------
-- Returns: { node_names }, total_cost  or  nil, nil if no path
function M.search(edges, start_node, goal_node)
  if start_node == goal_node then
    return { start_node }, 0
  end

  local adj = M.build_adjacency(edges)

  -- Forward search
  local fwd_dist = { [start_node] = 0 }
  local fwd_prev = {}
  local fwd_visited = {}
  local fwd_pq = pq_new()
  pq_push(fwd_pq, start_node, 0)

  -- Backward search (reverse adjacency built from bidir edges)
  local rev_adj = {}
  for _, e in ipairs(edges) do
    local from, to, weight = e[1], e[2], e[3]
    local bidir = e[7]
    if not rev_adj[to] then rev_adj[to] = {} end
    rev_adj[to][#rev_adj[to] + 1] = { to = from, weight = weight }
    if bidir then
      if not rev_adj[from] then rev_adj[from] = {} end
      rev_adj[from][#rev_adj[from] + 1] = { to = to, weight = weight }
    end
  end

  local rev_dist = { [goal_node] = 0 }
  local rev_prev = {}
  local rev_visited = {}
  local rev_pq = pq_new()
  pq_push(rev_pq, goal_node, 0)

  local best_cost = math.huge
  local meeting_node = nil

  while not pq_empty(fwd_pq) or not pq_empty(rev_pq) do
    -- Forward step
    if not pq_empty(fwd_pq) then
      local node, cost = pq_pop(fwd_pq)
      if not fwd_visited[node] then
        fwd_visited[node] = true
        if rev_dist[node] then
          local total = cost + rev_dist[node]
          if total < best_cost then
            best_cost = total
            meeting_node = node
          end
        end
        if cost > best_cost then break end
        if adj[node] then
          for _, edge in ipairs(adj[node]) do
            local new_cost = cost + edge.weight
            if not fwd_dist[edge.to] or new_cost < fwd_dist[edge.to] then
              fwd_dist[edge.to] = new_cost
              fwd_prev[edge.to] = node
              pq_push(fwd_pq, edge.to, new_cost)
            end
          end
        end
      end
    end

    -- Backward step
    if not pq_empty(rev_pq) then
      local node, cost = pq_pop(rev_pq)
      if not rev_visited[node] then
        rev_visited[node] = true
        if fwd_dist[node] then
          local total = cost + fwd_dist[node]
          if total < best_cost then
            best_cost = total
            meeting_node = node
          end
        end
        if cost > best_cost then break end
        if rev_adj[node] then
          for _, edge in ipairs(rev_adj[node]) do
            local new_cost = cost + edge.weight
            if not rev_dist[edge.to] or new_cost < rev_dist[edge.to] then
              rev_dist[edge.to] = new_cost
              rev_prev[edge.to] = node
              pq_push(rev_pq, edge.to, new_cost)
            end
          end
        end
      end
    end
  end

  if not meeting_node then return nil, nil end

  -- Reconstruct: forward half
  local fwd_path = {}
  local n = meeting_node
  while n do
    fwd_path[#fwd_path + 1] = n
    n = fwd_prev[n]
  end
  local path = {}
  for i = #fwd_path, 1, -1 do
    path[#path + 1] = fwd_path[i]
  end

  -- Backward half
  n = rev_prev[meeting_node]
  while n do
    path[#path + 1] = n
    n = rev_prev[n]
  end

  return path, best_cost
end

---------------------------------------------------------------------------
-- Look up edge data between two adjacent nodes
---------------------------------------------------------------------------
function M.find_edge(edges, from, to)
  for _, e in ipairs(edges) do
    if e[1] == from and e[2] == to then return e end
    if e[7] and e[1] == to and e[2] == from then return e end
  end
  return nil
end

return M
