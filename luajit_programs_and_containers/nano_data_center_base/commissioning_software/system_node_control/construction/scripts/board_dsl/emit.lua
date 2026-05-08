-- =============================================================================
-- emit.lua -- canonical JSON serialization for compiled boards.
--
-- Pretty-printed by default (indent=true) so file-store revisions show
-- meaningful diffs. dkjson key ordering is hash-bucket-based and not
-- stable across runs of the same input; to keep board JSONs reviewable
-- in version control we explicitly sort top-level + per-row keys via a
-- custom keyorder per object kind. Sub-segment internals stay
-- dkjson-default since their key sets are uniform per-kind and small.
-- =============================================================================

local dkjson = require("dkjson")

local M = {}

local KEYORDER_BOARD = {
  "schema_version", "name", "region", "capabilities", "nodes", "edges",
}
local KEYORDER_NODE = { "name", "x", "y", "kb_ref", "description" }
local KEYORDER_EDGE = { "from", "to", "path" }

-- Walk the canonical board and stamp __jsonorder on each table so
-- dkjson emits keys in stable order. Modifies in place; returns the
-- same table.
local function stamp_keyorder(board)
  board.__jsonorder = KEYORDER_BOARD
  for _, n in ipairs(board.nodes or {}) do
    n.__jsonorder = KEYORDER_NODE
  end
  for _, e in ipairs(board.edges or {}) do
    e.__jsonorder = KEYORDER_EDGE
  end
  return board
end

-- The dkjson __jsonorder convention: an entry called "__jsonorder" is
-- consumed by dkjson and dropped from the output. This works in dkjson
-- 2.x; older versions ignore the hint and just emit hash order.
function M.to_json(board, opts)
  opts = opts or {}
  stamp_keyorder(board)
  local state = {
    indent  = (opts.pretty ~= false),
  }
  local out, err = dkjson.encode(board, state)
  if not out then
    error("emit.to_json: dkjson.encode failed: " .. tostring(err), 0)
  end
  return out
end

function M.write_file(board, path, opts)
  local s = M.to_json(board, opts)
  local f, ferr = io.open(path, "wb")
  if not f then
    error(string.format("emit.write_file: cannot open %s: %s",
      path, tostring(ferr)), 0)
  end
  f:write(s)
  -- Trailing newline for POSIX text-file convention + nicer diffs.
  if s:sub(-1) ~= "\n" then f:write("\n") end
  f:close()
end

return M
