-- planner_ui :: GET /api/active_nodes (Phase 5b C2).
--
-- Returns array of {path, name, data} for every active_node_def at
-- this site. data is the parsed JSON payload (kb_ref, action_id,
-- params, etc.). Used by the SVG renderer (5b C3+) to look up
-- icons / metadata for active nodes referenced by board edges.

local api   = require("api")
local db    = require("db")
local cjson = require("cjson.safe")

api.with_pg(function(pg)
  local rows, err = db.list_active_nodes(pg)
  if not rows then return nil, err end
  -- Decode the data JSON column per-row; pg returns it as a string.
  -- Skip rows whose data is unparseable (log via ngx warn; don't fail
  -- the whole request).
  local out = {}
  for _, row in ipairs(rows) do
    local decoded
    if row.data and row.data ~= "" then
      decoded = cjson.decode(row.data)
    end
    out[#out + 1] = {
      path = row.path,
      name = row.name,
      data = decoded or {},
    }
  end
  return { active_nodes = out, count = #out }
end)
