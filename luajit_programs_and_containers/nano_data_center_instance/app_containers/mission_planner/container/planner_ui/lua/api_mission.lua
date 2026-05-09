-- planner_ui :: GET /api/mission/<robot_id> (Phase 5b C6).
--
-- Detailed status for one robot. Returns:
--   200 { status: {...} | null, result: {...} | null }
--   400 invalid robot_id
--   404 no mission ever ran for this robot
--   500 KV read / decode failure

local api    = require("api")
local status = require("status")

-- The PCRE in the location regex captures into ngx.var[1], but we
-- defense-in-depth check via Lua too -- mirroring api_board.lua.
local robot_id = ngx.var[1] or ""

local detail, err = status.get_mission(robot_id)
if not detail then
  -- Distinguish input errors (400) from infra errors (500). The
  -- module returns specific strings for input vs ks/decode failure.
  local s = 500
  if err == "robot_id required" or err == "invalid robot_id" then
    s = 400
  end
  api.fail(s, err)
end

if not detail.status and not detail.result then
  api.fail(404, "no mission state for robot: " .. robot_id)
end

api.ok(detail)
