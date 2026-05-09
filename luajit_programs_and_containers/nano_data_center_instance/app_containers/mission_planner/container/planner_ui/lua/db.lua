-- planner_ui :: db helpers (Phase 5b C2).
--
-- Thin pgmoon-over-cosocket layer for the read API. Mirrors the
-- dcs_console/admin/shell_helpers.pg_connect pattern. Cannot reuse
-- planner/lib/kb_query.lua because that module uses the LuaJIT FFI
-- pg driver, which is incompatible with OpenResty's cosocket model
-- (the FFI driver blocks the worker; cosocket needs nginx-aware I/O).
--
-- Three SQL helpers, all single-tenant for now (return ALL rows
-- regardless of planner_namespace). Multi-tenant filter consumer
-- logic lands with Phase 7 once the KB schema decision for
-- partitioning is made.
--
-- Error contract: every helper returns (rows, nil) on success or
-- (nil, err_string) on failure. Callers are expected to render
-- errors as JSON envelopes.

local pgmoon = require("pgmoon")

local M = {}

-- Connect using env vars. socket_type="nginx" engages cosocket; the
-- worker stays available for other requests during pg I/O.
function M.connect()
  local pg = pgmoon.new({
    host        = os.getenv("PG_HOST") or "127.0.0.1",
    port        = tonumber(os.getenv("PG_PORT") or "5432"),
    database    = os.getenv("PG_DB")       or "knowledge_base",
    user        = os.getenv("PG_USER")     or "gedgar",
    password    = os.getenv("PG_PASSWORD") or "",
    socket_type = "nginx",
  })
  local ok, err = pg:connect()
  if not ok then return nil, "pg connect: " .. tostring(err) end
  return pg
end

-- Boards live at  system.<system>.site.<site>.boards.<name>
-- as fs_node rows. Build the namespace prefix from env vars so the
-- handler doesn't have to.
local function boards_namespace()
  local sys  = os.getenv("APP_SYSTEM") or ""
  local site = os.getenv("APP_SITE")   or ""
  if sys == "" or site == "" then
    return nil, "APP_SYSTEM / APP_SITE not set"
  end
  return string.format("system.%s.site.%s.boards", sys, site)
end
M.boards_namespace = boards_namespace

------------------------------------------------------------------------
-- Boards
------------------------------------------------------------------------

-- List boards at this site. Returns array of {name, sha256_hex,
-- updated_at, size}. sha256 is hex-encoded for JSON friendliness.
--
-- @param pg     pgmoon connection
-- @return rows  array (possibly empty), or nil + err
function M.list_boards(pg)
  local ns, ns_err = boards_namespace()
  if not ns then return nil, ns_err end
  -- ltree subtree query: match all nodes under ns. The board name is
  -- the LAST label of the path; ltree's `subltree(path, nlabels-1, nlabels)`
  -- extracts it. Encode sha256 as hex for JSON return.
  local sql = string.format([[
    SELECT subltree(n.path, nlevel(n.path) - 1, nlevel(n.path))::text AS name,
           encode(b.sha256, 'hex') AS sha256_hex,
           n.updated_at::text AS updated_at,
           b.size
      FROM knowledge_base_fs_node n
 LEFT JOIN knowledge_base_fs_blob b ON b.sha256 = n.sha256
     WHERE n.path <@ %s::ltree
       AND n.kind = 'file'
     ORDER BY n.path
  ]], pg:escape_literal(ns))
  local rs, err = pg:query(sql)
  if not rs then return nil, "list_boards: " .. tostring(err) end
  return rs
end

-- Fetch one board's content as a string (the JSON the planner reads).
-- Returns string + sha256_hex on success, or nil + err.
--
-- @param pg     pgmoon connection
-- @param name   board name (last path segment, no dots/slashes)
-- @return content_str, sha256_hex   or  nil, err
function M.get_board(pg, name)
  if type(name) ~= "string" or name == "" then
    return nil, "get_board: name required"
  end
  -- Reject characters that would break the ltree path (only [a-zA-Z0-9_]
  -- is valid in an ltree label). This also rejects path-traversal
  -- attempts (..//, slashes).
  if not name:match("^[%w_]+$") then
    return nil, "get_board: invalid name (must match [%w_]+)"
  end
  local ns, ns_err = boards_namespace()
  if not ns then return nil, ns_err end
  local path = ns .. "." .. name
  local sql = string.format([[
    SELECT b.content,
           encode(b.sha256, 'hex') AS sha256_hex,
           b.size
      FROM knowledge_base_fs_node n
 LEFT JOIN knowledge_base_fs_blob b ON b.sha256 = n.sha256
     WHERE n.path = %s::ltree
  ]], pg:escape_literal(path))
  local rs, err = pg:query(sql)
  if not rs then return nil, "get_board: " .. tostring(err) end
  if not rs[1] or not rs[1].content then
    return nil, "board not found: " .. name
  end
  -- pgmoon returns bytea as a Lua string already; no decode needed.
  return rs[1].content, rs[1].sha256_hex
end

------------------------------------------------------------------------
-- Active nodes
------------------------------------------------------------------------

-- List active-node definitions visible at this site. Returns
-- {path, name, data} per row. data is the JSON-decoded payload from
-- knowledge_base.data (which carries the active_node_def fields like
-- kb_ref, action_id, params).
--
-- Active node defs live in knowledge_base (NOT file_store); they are
-- regular ltree-keyed rows with label = 'active_node_def' under
--   system.<sys>.site.<site>.infrastructure.registry.active_node_def.<name>
--
-- @param pg     pgmoon connection
-- @return rows  array (possibly empty), or nil + err
function M.list_active_nodes(pg)
  local sys  = os.getenv("APP_SYSTEM") or ""
  local site = os.getenv("APP_SITE")   or ""
  if sys == "" or site == "" then
    return nil, "APP_SYSTEM / APP_SITE not set"
  end
  local ns = string.format(
    "system.%s.site.%s.infrastructure.registry.active_node_def",
    sys, site)
  local sql = string.format([[
    SELECT path::text AS path,
           subltree(path, nlevel(path) - 1, nlevel(path))::text AS name,
           data::text AS data
      FROM knowledge_base
     WHERE path <@ %s::ltree
       AND label = 'active_node_def'
     ORDER BY path
  ]], pg:escape_literal(ns))
  local rs, err = pg:query(sql)
  if not rs then return nil, "list_active_nodes: " .. tostring(err) end
  return rs
end

return M
