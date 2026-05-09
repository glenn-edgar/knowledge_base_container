-- planner_ui :: health endpoint (Phase 5b C1).
--
-- Liveness probe consumed by node_control's HTTP watchdog. Stays
-- intentionally cheap: no pg / NATS contact. Future health tiers (DB
-- connectivity, NATS reachability) belong in a separate /readiness
-- endpoint where false negatives don't trigger an immediate restart.

local render = require("render")
local ctx    = render.context()

ngx.header.content_type = "application/json"
ngx.say(string.format(
  '{"status":"ok","slot":"planner_ui","container":"%s",' ..
  '"planner_namespace":"%s","ts":%d}',
  ctx.container_name, ctx.planner_namespace, ngx.time()))
