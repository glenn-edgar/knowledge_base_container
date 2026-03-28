-- dijkstra.lua — Bidirectional Dijkstra over virtual node graph
--
-- Operates on the unified virtual node graph from virtual_node_dsl.lua.
-- Finds shortest path between any two virtual nodes regardless of type
-- (board position, arm configuration, etc.).
--
-- Bidirectional search: forward from start, backward from goal,
-- meet in the middle. Half the search space.
--
-- All edges are bidirectional by default. The edge list in the DSL
-- is explicit per direction so weights can differ.

local M = {}

---------------------------------------------------------------------------
-- Build adjacency list from edge list
---------------------------------------------------------------------------
-- Returns: adj[node_name] = { { to = name, weight = N, edge = edge_data }, ... }
function M.build_adjacency(edges)
  local adj = {}
  for _, e in ipairs(edges) do
    local from, to, weight = e[1], e[2], e[3]
    local edge_type = e[4]
    local data = e[5]
    local bidir = e[6]  -- true if edge is bidirectional
    if not adj[from] then adj[from] = {} end
    adj[from][#adj[from] + 1] = {
      to = to,
      weight = weight,
      edge_type = edge_type,
      data = data,
    }
    if bidir then
      if not adj[to] then adj[to] = {} end
      adj[to][#adj[to] + 1] = {
        to = from,
        weight = weight,
        edge_type = edge_type,
        data = data,
      }
    end
  end
  return adj
end

---------------------------------------------------------------------------
-- Min-heap priority queue (simple array-based)
---------------------------------------------------------------------------
local function pq_new()
  return { entries = {} }
end

local function pq_push(pq, node, cost)
  local entries = pq.entries
  entries[#entries + 1] = { node = node, cost = cost }
  -- Bubble up
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
  -- Bubble down
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

local function pq_empty(pq)
  return #pq.entries == 0
end

---------------------------------------------------------------------------
-- Build reverse adjacency (for backward search)
---------------------------------------------------------------------------
function M.build_reverse_adjacency(edges)
  local adj = {}
  for _, e in ipairs(edges) do
    local from, to, weight = e[1], e[2], e[3]
    local edge_type = e[4]
    local data = e[5]
    local bidir = e[6]
    if not adj[to] then adj[to] = {} end
    adj[to][#adj[to] + 1] = {
      to = from,
      weight = weight,
      edge_type = edge_type,
      data = data,
    }
    if bidir then
      if not adj[from] then adj[from] = {} end
      adj[from][#adj[from] + 1] = {
        to = to,
        weight = weight,
        edge_type = edge_type,
        data = data,
      }
    end
  end
  return adj
end

---------------------------------------------------------------------------
-- Bidirectional Dijkstra
---------------------------------------------------------------------------
-- Returns: { node_names }, total_cost  or  nil, nil if no path
function M.search(edges, start_node, goal_node)
  if start_node == goal_node then
    return { start_node }, 0
  end

  local fwd_adj = M.build_adjacency(edges)
  local rev_adj = M.build_reverse_adjacency(edges)

  -- Forward search state
  local fwd_dist = { [start_node] = 0 }
  local fwd_prev = {}
  local fwd_visited = {}
  local fwd_pq = pq_new()
  pq_push(fwd_pq, start_node, 0)

  -- Backward search state
  local rev_dist = { [goal_node] = 0 }
  local rev_prev = {}
  local rev_visited = {}
  local rev_pq = pq_new()
  pq_push(rev_pq, goal_node, 0)

  local best_cost = math.huge
  local meeting_node = nil

  -- Alternating expansion
  while not pq_empty(fwd_pq) or not pq_empty(rev_pq) do

    -- Forward step
    if not pq_empty(fwd_pq) then
      local node, cost = pq_pop(fwd_pq)
      if not fwd_visited[node] then
        fwd_visited[node] = true
        -- Check if backward search already reached this node
        if rev_dist[node] then
          local total = cost + rev_dist[node]
          if total < best_cost then
            best_cost = total
            meeting_node = node
          end
        end
        -- Early termination: if cost exceeds best known path, stop
        if cost > best_cost then
          break
        end
        -- Expand neighbors
        if fwd_adj[node] then
          for _, edge in ipairs(fwd_adj[node]) do
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
        -- Check if forward search already reached this node
        if fwd_dist[node] then
          local total = cost + fwd_dist[node]
          if total < best_cost then
            best_cost = total
            meeting_node = node
          end
        end
        if cost > best_cost then
          break
        end
        -- Expand neighbors (in reverse)
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

  if not meeting_node then
    return nil, nil
  end

  -- Reconstruct path: forward half (start → meeting)
  local fwd_path = {}
  local n = meeting_node
  while n do
    fwd_path[#fwd_path + 1] = n
    n = fwd_prev[n]
  end
  -- Reverse to get start → meeting order
  local path = {}
  for i = #fwd_path, 1, -1 do
    path[#path + 1] = fwd_path[i]
  end

  -- Backward half (meeting → goal)
  n = rev_prev[meeting_node]
  while n do
    path[#path + 1] = n
    n = rev_prev[n]
  end

  return path, best_cost
end

---------------------------------------------------------------------------
-- Annotate path with edge data
---------------------------------------------------------------------------
-- Takes a path (list of node names) and the edge list,
-- returns list of { from, to, weight, edge_type, data } for each step.
function M.annotate_path(path, edges)
  -- Build lookup: edges_by_pair[from][to] = edge
  local lookup = {}
  for _, e in ipairs(edges) do
    local from, to = e[1], e[2]
    local bidir = e[6]
    local info = {
      weight    = e[3],
      edge_type = e[4],
      data      = e[5],
    }
    if not lookup[from] then lookup[from] = {} end
    lookup[from][to] = info
    if bidir then
      if not lookup[to] then lookup[to] = {} end
      lookup[to][from] = info
    end
  end

  local steps = {}
  for i = 1, #path - 1 do
    local from = path[i]
    local to   = path[i + 1]
    local edge = lookup[from] and lookup[from][to]
    if edge then
      steps[#steps + 1] = {
        from      = from,
        to        = to,
        weight    = edge.weight,
        edge_type = edge.edge_type,
        data      = edge.data,
      }
    else
      error(string.format("No edge from '%s' to '%s'", from, to))
    end
  end
  return steps
end

return M
