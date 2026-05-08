-- =============================================================================
-- board_dsl.lua -- v2 board-DSL skeleton (Phase 4 C1).
--
-- Procedural LuaJIT DSL for authoring navigation boards. Operator
-- writes a board.lua that requires this module, builds up the board,
-- and returns it. compile_board.lua (Phase 4 C4) drives the compile,
-- emits canonical JSON, and hands off to upload_board.lua.
--
-- C1 covers the structural skeleton:
--   region (polygon, immutable, declared at new())
--   nodes  (name, x, y, optional kb_ref / description)
--   edges  (from -> to, no duplicates, no self-loops, both endpoints exist)
--   capabilities (declared union of leaf-action ids)
--
-- Build-time checks (in :build()):
--   - region is a simple polygon with >= 3 points, all coords numeric
--   - every node coord is inside the region polygon (ray-cast PiP)
--   - node names are unique within the board
--   - edge endpoints reference declared nodes; no self-loops; no duplicate (from,to)
--   - capabilities list contains non-empty unique strings
--
-- Path-tree leaves (cmd_drive_t sub-segments + activate-action) come
-- in C2. KB-connected validating compile (kb_ref existence + action_id
-- catalog cross-check + parameter-schema match + active-node action
-- presence) comes in C3.
--
-- API style: single-table named-args (b:add_node{ name=..., x=..., y=... }).
-- Errors are line-number-aware via debug.getinfo: a duplicate node name
-- prints both the conflict line AND the original declaration line so
-- the operator can find the typo without grepping.
-- =============================================================================

local M = {}

------------------------------------------------------------------------
-- helpers
------------------------------------------------------------------------

-- Locate the caller in the user's DSL file. level=2 by default skips
-- this fn frame; pass level+N to skip more wrappers.
local function caller_loc(level)
  level = (level or 2)
  local info = debug.getinfo(level, "Sl")
  if not info then return "?:?" end
  local src = info.short_src or info.source or "?"
  -- Strip a leading "@" that Lua sometimes prepends to source paths.
  if src:sub(1, 1) == "@" then src = src:sub(2) end
  return string.format("%s:%d", src, info.currentline or 0)
end

local function check_string(where, val, name)
  if type(val) ~= "string" or val == "" then
    error(string.format("%s: %s required (non-empty string; got %s)",
      where, name, type(val)), 0)
  end
end

local function check_number(where, val, name)
  if type(val) ~= "number" then
    error(string.format("%s: %s required (number; got %s)",
      where, name, type(val)), 0)
  end
end

------------------------------------------------------------------------
-- point-in-polygon (ray cast)
------------------------------------------------------------------------

-- Returns true if (x, y) is inside the closed polygon `poly` (a list
-- of {x, y} tables). Uses the standard ray-casting test. Boundary
-- points: result is implementation-defined; we accept boundary as
-- inside (the < / <= asymmetry below treats the upper boundary as
-- inside, lower as outside; for board authoring this is irrelevant
-- since coords on the exact boundary should be rare and the operator
-- can nudge by an epsilon).
local function point_in_polygon(x, y, poly)
  local inside = false
  local n = #poly
  local j = n
  for i = 1, n do
    local xi, yi = poly[i].x, poly[i].y
    local xj, yj = poly[j].x, poly[j].y
    if ((yi > y) ~= (yj > y)) and
       (x < (xj - xi) * (y - yi) / (yj - yi) + xi) then
      inside = not inside
    end
    j = i
  end
  return inside
end

------------------------------------------------------------------------
-- region validation
------------------------------------------------------------------------

local function validate_region(region, where)
  if type(region) ~= "table" then
    error(string.format("%s: region required (list of {x,y} points; got %s)",
      where, type(region)), 0)
  end
  if #region < 3 then
    error(string.format("%s: region polygon must have >= 3 points (got %d)",
      where, #region), 0)
  end
  for i, p in ipairs(region) do
    if type(p) ~= "table" then
      error(string.format("%s: region[%d] must be {x,y} table (got %s)",
        where, i, type(p)), 0)
    end
    if type(p.x) ~= "number" or type(p.y) ~= "number" then
      error(string.format("%s: region[%d] requires numeric x and y", where, i), 0)
    end
    for k, _ in pairs(p) do
      if k ~= "x" and k ~= "y" then
        error(string.format("%s: region[%d] unknown field %q",
          where, i, tostring(k)), 0)
      end
    end
  end
end

------------------------------------------------------------------------
-- board object
------------------------------------------------------------------------

local Board = {}
Board.__index = Board

function M.new(opts)
  local where = caller_loc()
  if type(opts) ~= "table" then
    error(string.format("%s: board_dsl.new requires a table (got %s)",
      where, type(opts)), 0)
  end
  check_string(where, opts.name, "name")
  validate_region(opts.region, where)
  for k, _ in pairs(opts) do
    if k ~= "name" and k ~= "region" then
      error(string.format("%s: board_dsl.new unknown field %q",
        where, tostring(k)), 0)
    end
  end

  local self = setmetatable({}, Board)
  self.name        = opts.name
  -- Defensive copy of region so user can't mutate after declaration.
  self.region      = {}
  for i, p in ipairs(opts.region) do
    self.region[i] = { x = p.x, y = p.y }
  end
  self.nodes        = {}    -- list (preserves declaration order)
  self.nodes_by_name = {}    -- name -> node ref
  self.edges        = {}    -- list
  self.edges_seen   = {}    -- "from\tto" -> declared_at, for dup-detect
  self.capabilities = {}    -- list (preserves order)
  self.caps_seen    = {}    -- action_id -> declared_at
  self.declared_at  = where
  return self
end

------------------------------------------------------------------------
-- nodes
------------------------------------------------------------------------

local NODE_ALLOWED = {
  name = true, x = true, y = true,
  kb_ref = true, description = true,
}

function Board:add_node(spec)
  local where = caller_loc()
  if type(spec) ~= "table" then
    error(string.format("%s: add_node requires a table (got %s)",
      where, type(spec)), 0)
  end
  check_string(where, spec.name, "name")
  check_number(where, spec.x, "x")
  check_number(where, spec.y, "y")
  if spec.kb_ref ~= nil then
    check_string(where, spec.kb_ref, "kb_ref")
  end
  if spec.description ~= nil then
    check_string(where, spec.description, "description")
  end
  for k, _ in pairs(spec) do
    if not NODE_ALLOWED[k] then
      error(string.format("%s: add_node unknown field %q",
        where, tostring(k)), 0)
    end
  end

  local existing = self.nodes_by_name[spec.name]
  if existing then
    error(string.format(
      "%s: add_node: name %q already declared (first at %s)",
      where, spec.name, existing.declared_at), 0)
  end

  if not point_in_polygon(spec.x, spec.y, self.region) then
    error(string.format(
      "%s: add_node: %q at (%s, %s) is outside region polygon",
      where, spec.name, tostring(spec.x), tostring(spec.y)), 0)
  end

  local node = {
    name = spec.name, x = spec.x, y = spec.y,
    kb_ref = spec.kb_ref, description = spec.description,
    declared_at = where,
  }
  self.nodes[#self.nodes + 1] = node
  self.nodes_by_name[spec.name] = node
end

------------------------------------------------------------------------
-- edges
------------------------------------------------------------------------

local EDGE_ALLOWED_C1 = {
  from = true, to = true,
  -- C2 will add `path = {...}` here. Keep allowed-set strict so a
  -- mistakenly-included path key in a C1 board fails with a clear
  -- error pointing the operator at C2.
}

function Board:add_edge(spec)
  local where = caller_loc()
  if type(spec) ~= "table" then
    error(string.format("%s: add_edge requires a table (got %s)",
      where, type(spec)), 0)
  end
  check_string(where, spec.from, "from")
  check_string(where, spec.to, "to")
  if spec.from == spec.to then
    error(string.format("%s: add_edge: self-loop on %q not allowed",
      where, spec.from), 0)
  end
  for k, _ in pairs(spec) do
    if not EDGE_ALLOWED_C1[k] then
      error(string.format(
        "%s: add_edge unknown field %q (path = {...} support lands in C2)",
        where, tostring(k)), 0)
    end
  end

  -- Endpoint existence is checked in :build() rather than here, so
  -- forward references work (you can declare the edge before both
  -- endpoint nodes if the operator prefers an "edges first" style).
  -- Duplicate detection runs here because we want to point at BOTH
  -- declaration sites.
  local key = spec.from .. "\t" .. spec.to
  local prior = self.edges_seen[key]
  if prior then
    error(string.format(
      "%s: add_edge: %q -> %q already declared (first at %s)",
      where, spec.from, spec.to, prior), 0)
  end

  local edge = { from = spec.from, to = spec.to, declared_at = where }
  self.edges[#self.edges + 1] = edge
  self.edges_seen[key] = where
end

------------------------------------------------------------------------
-- capabilities
------------------------------------------------------------------------

function Board:declare_capabilities(spec)
  local where = caller_loc()
  if type(spec) ~= "table" then
    error(string.format(
      "%s: declare_capabilities requires a list of action_id strings (got %s)",
      where, type(spec)), 0)
  end
  for i, action_id in ipairs(spec) do
    if type(action_id) ~= "string" or action_id == "" then
      error(string.format(
        "%s: declare_capabilities[%d] must be non-empty string", where, i), 0)
    end
    local prior = self.caps_seen[action_id]
    if prior then
      error(string.format(
        "%s: declare_capabilities: %q already declared (first at %s)",
        where, action_id, prior), 0)
    end
    self.capabilities[#self.capabilities + 1] = action_id
    self.caps_seen[action_id] = where
  end
end

------------------------------------------------------------------------
-- build (validate + emit canonical table)
------------------------------------------------------------------------

-- Returns the canonical board table ready for JSON serialization.
-- Raises on any validation failure.
function Board:build(opts)
  opts = opts or {}
  -- KB-connected validation (4 build-time checks per architectural
  -- memo) lands in C3. C1 only does structural checks.

  -- Edge endpoint existence (deferred from add_edge so forward refs work).
  for _, edge in ipairs(self.edges) do
    if not self.nodes_by_name[edge.from] then
      error(string.format(
        "%s: edge from-endpoint %q is not a declared node",
        edge.declared_at, edge.from), 0)
    end
    if not self.nodes_by_name[edge.to] then
      error(string.format(
        "%s: edge to-endpoint %q is not a declared node",
        edge.declared_at, edge.to), 0)
    end
  end

  return {
    schema_version = 2,
    name           = self.name,
    region         = self.region,
    capabilities   = self.capabilities,
    nodes          = (function()
      local out = {}
      for i, n in ipairs(self.nodes) do
        out[i] = { name = n.name, x = n.x, y = n.y,
                   kb_ref = n.kb_ref, description = n.description }
      end
      return out
    end)(),
    edges          = (function()
      local out = {}
      for i, e in ipairs(self.edges) do
        out[i] = { from = e.from, to = e.to }
      end
      return out
    end)(),
    path_tree      = nil,  -- populated in C2
  }
end

------------------------------------------------------------------------
-- internal helpers exposed for the test harness
------------------------------------------------------------------------

M._point_in_polygon = point_in_polygon

return M
