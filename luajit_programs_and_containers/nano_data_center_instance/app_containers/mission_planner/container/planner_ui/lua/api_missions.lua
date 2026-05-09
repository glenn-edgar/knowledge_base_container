-- planner_ui :: GET /api/missions (Phase 5b C6).
--
-- Mission dashboard: reads action_server's published summary key and
-- returns the per-robot mission rows plus the registered_robots list.
-- Polled by map_render.js every 2s into #status-region.

local api    = require("api")
local status = require("status")

local payload, err = status.list_missions()
if not payload then api.fail(500, err) end
api.ok(payload)
