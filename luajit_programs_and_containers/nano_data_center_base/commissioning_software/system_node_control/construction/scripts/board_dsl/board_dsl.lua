-- =============================================================================
-- board_dsl.lua -- v2 board-DSL (Phase 4 C1+C2).
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
-- C2 covers the path-tree DSL on edges:
--   bd.straight_line / spline / rotate / wall_follow / line_follow
--                                       (cmd_drive_t sub-segments)
--   bd.activate                          (cmd_activate_action_t leaf)
--   add_edge{ ..., path = { <leaf>, ... } }
--   Compile-time grouping: consecutive sub-segments collapse into ONE
--                          drive leaf (per-PACKET completion semantics).
--   Capability union check: every activate{}.action_id must appear in
--                           b:declare_capabilities (or be imported via
--                           b:import_capabilities -- C3, KB-driven).
--
-- C3 lands the KB-connected validating compile: kb_ref existence,
-- action_id catalog cross-check, parameter-schema match, active-node
-- action presence.
--
-- API style: single-table named-args (b:add_node{ name=..., x=..., y=... }).
-- Errors are line-number-aware via debug.getinfo: a duplicate node name
-- prints both the conflict line AND the original declaration line so
-- the operator can find the typo without grepping.
--
-- IMPORTANT: the sub-segment validators below MUST stay in sync with
-- planner/hub_dsl/protocol/command_packets.lua. Both implementations
-- exist because the DSL must validate at compile time (host-side, no
-- planner container required) and the planner must validate at packet
-- emit/decode time. Phase 5 cleanup extracts these into a shared
-- protocol_schemas.lua module that both require.
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
-- typed segment constructors (C2)
------------------------------------------------------------------------
--
-- Each constructor returns a tagged table. The hidden __tag field is
-- how add_edge / build can tell user-supplied raw tables (rejected)
-- from constructor output (accepted). Constructors do per-kind shape
-- validation eagerly so an error points at the constructor call line,
-- not later at b:build().

local DIRECTIONS = { forward = true, reverse = true }

local function check_pos2(where, p, name)
  if type(p) ~= "table" then
    error(string.format("%s: %s required (table {x,y}; got %s)",
      where, name, type(p)), 0)
  end
  if type(p.x) ~= "number" or type(p.y) ~= "number" then
    error(string.format("%s: %s requires numeric x and y", where, name), 0)
  end
  for k, _ in pairs(p) do
    if k ~= "x" and k ~= "y" then
      error(string.format("%s: %s.%s unknown field", where, name,
        tostring(k)), 0)
    end
  end
end

local function check_optional_speed(where, v)
  if v == nil then return end
  if type(v) ~= "number" then
    error(string.format("%s: speed must be number when present (got %s)",
      where, type(v)), 0)
  end
  if v <= 0 then
    error(string.format("%s: speed must be > 0 (got %s)",
      where, tostring(v)), 0)
  end
end

local function check_optional_direction(where, v)
  if v == nil then return end
  if not DIRECTIONS[v] then
    error(string.format(
      "%s: direction must be \"forward\" or \"reverse\" (got %q)",
      where, tostring(v)), 0)
  end
end

local function check_unknown(where, t, allowed, owner)
  for k, _ in pairs(t) do
    if not allowed[k] then
      error(string.format("%s: %s unknown field %q",
        where, owner, tostring(k)), 0)
    end
  end
end

local STRAIGHT_LINE_ALLOWED = { end_pos = true, speed = true, direction = true }
function M.straight_line(spec)
  local where = caller_loc()
  if type(spec) ~= "table" then
    error(string.format("%s: straight_line requires a table (got %s)",
      where, type(spec)), 0)
  end
  check_pos2(where, spec.end_pos, "end_pos")
  check_optional_speed(where, spec.speed)
  check_optional_direction(where, spec.direction)
  check_unknown(where, spec, STRAIGHT_LINE_ALLOWED, "straight_line")
  return { __tag = "drive_seg", kind = "straight_line",
           end_pos = spec.end_pos, speed = spec.speed,
           direction = spec.direction, declared_at = where }
end

local SPLINE_ALLOWED = { end_pos = true, end_heading = true,
                         speed = true, direction = true }
function M.spline(spec)
  local where = caller_loc()
  if type(spec) ~= "table" then
    error(string.format("%s: spline requires a table (got %s)",
      where, type(spec)), 0)
  end
  check_pos2(where, spec.end_pos, "end_pos")
  if type(spec.end_heading) ~= "number" then
    error(string.format("%s: spline.end_heading required (number; got %s)",
      where, type(spec.end_heading)), 0)
  end
  check_optional_speed(where, spec.speed)
  check_optional_direction(where, spec.direction)
  check_unknown(where, spec, SPLINE_ALLOWED, "spline")
  return { __tag = "drive_seg", kind = "spline",
           end_pos = spec.end_pos, end_heading = spec.end_heading,
           speed = spec.speed, direction = spec.direction,
           declared_at = where }
end

local ROTATE_ALLOWED = { end_heading = true, speed = true }
function M.rotate(spec)
  local where = caller_loc()
  if type(spec) ~= "table" then
    error(string.format("%s: rotate requires a table (got %s)",
      where, type(spec)), 0)
  end
  if type(spec.end_heading) ~= "number" then
    error(string.format("%s: rotate.end_heading required (number; got %s)",
      where, type(spec.end_heading)), 0)
  end
  check_optional_speed(where, spec.speed)
  check_unknown(where, spec, ROTATE_ALLOWED, "rotate")
  return { __tag = "drive_seg", kind = "rotate",
           end_heading = spec.end_heading, speed = spec.speed,
           declared_at = where }
end

-- Composite base validator: only straight_line or spline allowed; no
-- nested composites; no speed/direction at base level (those live on
-- the outer composite).
local COMPOSITE_BASE_KINDS = { straight_line = true, spline = true }
local BASE_LINE_ALLOWED   = { kind = true, end_pos = true }
local BASE_SPLINE_ALLOWED = { kind = true, end_pos = true, end_heading = true }

local function validate_base(where, base)
  if type(base) ~= "table" then
    error(string.format("%s: base required (table; got %s)",
      where, type(base)), 0)
  end
  -- Bases ARE plain tables (not constructor output) -- they're inline
  -- to keep the syntax compact. Strict whitelist enforces shape.
  local k = base.kind
  if type(k) ~= "string" or k == "" then
    error(string.format("%s: base.kind required (non-empty string)", where), 0)
  end
  if not COMPOSITE_BASE_KINDS[k] then
    error(string.format(
      "%s: base.kind=%q not allowed (must be straight_line or spline)",
      where, k), 0)
  end
  check_pos2(where, base.end_pos, "base.end_pos")
  if k == "spline" then
    if type(base.end_heading) ~= "number" then
      error(string.format(
        "%s: base.end_heading required when kind=spline", where), 0)
    end
    check_unknown(where, base, BASE_SPLINE_ALLOWED, "base")
  else
    if base.end_heading ~= nil then
      error(string.format(
        "%s: base.end_heading not allowed when kind=%s", where, k), 0)
    end
    check_unknown(where, base, BASE_LINE_ALLOWED, "base")
  end
end

local WALL_FOLLOW_ALLOWED = { base = true, offset = true,
                              speed = true, direction = true }
function M.wall_follow(spec)
  local where = caller_loc()
  if type(spec) ~= "table" then
    error(string.format("%s: wall_follow requires a table (got %s)",
      where, type(spec)), 0)
  end
  validate_base(where, spec.base)
  if type(spec.offset) ~= "number" then
    error(string.format("%s: wall_follow.offset required (number; got %s)",
      where, type(spec.offset)), 0)
  end
  check_optional_speed(where, spec.speed)
  check_optional_direction(where, spec.direction)
  check_unknown(where, spec, WALL_FOLLOW_ALLOWED, "wall_follow")
  return { __tag = "drive_seg", kind = "wall_follow",
           base = spec.base, offset = spec.offset,
           speed = spec.speed, direction = spec.direction,
           declared_at = where }
end

local LINE_FOLLOW_ALLOWED = { base = true, speed = true, direction = true }
function M.line_follow(spec)
  local where = caller_loc()
  if type(spec) ~= "table" then
    error(string.format("%s: line_follow requires a table (got %s)",
      where, type(spec)), 0)
  end
  validate_base(where, spec.base)
  if spec.offset ~= nil then
    error(string.format(
      "%s: line_follow.offset not allowed (use wall_follow if you want offset)",
      where), 0)
  end
  check_optional_speed(where, spec.speed)
  check_optional_direction(where, spec.direction)
  check_unknown(where, spec, LINE_FOLLOW_ALLOWED, "line_follow")
  return { __tag = "drive_seg", kind = "line_follow",
           base = spec.base, speed = spec.speed,
           direction = spec.direction, declared_at = where }
end

local ACTIVATE_ALLOWED = { action_id = true, kb_ref = true, params = true }
function M.activate(spec)
  local where = caller_loc()
  if type(spec) ~= "table" then
    error(string.format("%s: activate requires a table (got %s)",
      where, type(spec)), 0)
  end
  check_string(where, spec.action_id, "action_id")
  if spec.kb_ref ~= nil then
    check_string(where, spec.kb_ref, "kb_ref")
  end
  if spec.params ~= nil and type(spec.params) ~= "table" then
    error(string.format(
      "%s: activate.params must be table when present (got %s)",
      where, type(spec.params)), 0)
  end
  check_unknown(where, spec, ACTIVATE_ALLOWED, "activate")
  return { __tag = "activate", action_id = spec.action_id,
           kb_ref = spec.kb_ref, params = spec.params or {},
           declared_at = where }
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

local EDGE_ALLOWED = { from = true, to = true, path = true }

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
    if not EDGE_ALLOWED[k] then
      error(string.format("%s: add_edge unknown field %q",
        where, tostring(k)), 0)
    end
  end

  -- path validation: must be a list of constructor outputs (tagged
  -- tables). Raw tables rejected -- this catches "user forgot the
  -- bd. prefix" typos at the first occurrence.
  if spec.path ~= nil then
    if type(spec.path) ~= "table" then
      error(string.format(
        "%s: add_edge.path must be a list (got %s)",
        where, type(spec.path)), 0)
    end
    if #spec.path == 0 then
      error(string.format(
        "%s: add_edge.path is empty -- omit the field if no path needed",
        where), 0)
    end
    for i, leaf in ipairs(spec.path) do
      if type(leaf) ~= "table" or
         (leaf.__tag ~= "drive_seg" and leaf.__tag ~= "activate") then
        error(string.format(
          "%s: add_edge.path[%d] must come from bd.straight_line / spline / " ..
          "rotate / wall_follow / line_follow / activate (got raw table or %s)",
          where, i, type(leaf)), 0)
      end
    end
  end

  local key = spec.from .. "\t" .. spec.to
  local prior = self.edges_seen[key]
  if prior then
    error(string.format(
      "%s: add_edge: %q -> %q already declared (first at %s)",
      where, spec.from, spec.to, prior), 0)
  end

  local edge = { from = spec.from, to = spec.to,
                 path = spec.path, declared_at = where }
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

-- Group consecutive drive_seg entries on a path into a single drive
-- leaf (per-PACKET completion semantics). Activate leaves break drive
-- sequences. Result: an ordered list of leaves where each leaf is
-- either { kind = "drive", segments = {...} } or
--        { kind = "activate", action_id = "...", ... }.
local function fold_path(edge_path)
  if not edge_path then return nil end
  local out = {}
  local pending = nil  -- in-progress drive leaf
  for _, leaf in ipairs(edge_path) do
    if leaf.__tag == "drive_seg" then
      if not pending then
        pending = { kind = "drive", segments = {} }
        out[#out + 1] = pending
      end
      -- Strip the __tag and declared_at metadata; emit the canonical
      -- segment shape (matches command_packets.lua sub-segment shape).
      local seg = { kind = leaf.kind }
      for k, v in pairs(leaf) do
        if k ~= "__tag" and k ~= "declared_at" and k ~= "kind" then
          seg[k] = v
        end
      end
      pending.segments[#pending.segments + 1] = seg
    else  -- activate leaf
      pending = nil  -- close any open drive leaf
      out[#out + 1] = {
        kind = "activate",
        action_id = leaf.action_id,
        kb_ref    = leaf.kb_ref,
        params    = leaf.params,
      }
    end
  end
  return out
end

-- Returns the canonical board table ready for JSON serialization.
-- Raises on any validation failure.
function Board:build(opts)
  opts = opts or {}
  -- KB-connected validation (4 build-time checks per architectural
  -- memo) lands in C3. Structural + capability-union here.

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

  -- Capability union check: every action_id used in any activate leaf
  -- must appear in declared (or imported -- C3) capabilities.
  for _, edge in ipairs(self.edges) do
    if edge.path then
      for i, leaf in ipairs(edge.path) do
        if leaf.__tag == "activate" then
          if not self.caps_seen[leaf.action_id] then
            error(string.format(
              "%s: activate{ action_id=%q } not in board capabilities " ..
              "(use b:declare_capabilities or b:import_capabilities)",
              leaf.declared_at, leaf.action_id), 0)
          end
        end
      end
    end
  end

  -- Build canonical edge representation with folded path tree.
  local out_edges = {}
  for i, e in ipairs(self.edges) do
    out_edges[i] = { from = e.from, to = e.to,
                     path = fold_path(e.path) }
  end

  local out_nodes = {}
  for i, n in ipairs(self.nodes) do
    out_nodes[i] = { name = n.name, x = n.x, y = n.y,
                     kb_ref = n.kb_ref, description = n.description }
  end

  return {
    schema_version = 2,
    name           = self.name,
    region         = self.region,
    capabilities   = self.capabilities,
    nodes          = out_nodes,
    edges          = out_edges,
  }
end

------------------------------------------------------------------------
-- internal helpers exposed for the test harness
------------------------------------------------------------------------

M._point_in_polygon = point_in_polygon

return M
