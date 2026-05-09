-- planner_ui :: GET /api/boards (Phase 5b C2).
--
-- Returns array of {name, sha256_hex, updated_at, size} for every
-- board at this site. Single-tenant scoping today; multi-tenant
-- filter (only boards owned by this planner_namespace) lands with
-- Phase 7's KB schema decision.

local api = require("api")
local db  = require("db")

api.with_pg(function(pg)
  local rows, err = db.list_boards(pg)
  if not rows then return nil, err end
  return { boards = rows, count = #rows }
end)
