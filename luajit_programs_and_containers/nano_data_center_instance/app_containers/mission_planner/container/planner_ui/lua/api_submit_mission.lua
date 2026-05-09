-- planner_ui :: POST /api/submit_mission (Phase 5b C5).
--
-- Reads a JSON body from the launcher UI, validates it, and submits
-- the mission to NATS JobQueue via direct FFI (lib/nats_job_queue).
-- Same container as the planner worker, so the .so resolves through
-- the system loader -- no LD_LIBRARY_PATH gymnastics needed.
--
-- Body shape (from map_render.js):
--   {"robot_id": "rover_1",
--    "board":    "landing_zone",
--    "source":   "lander_pad",
--    "target":   "habitat_site"}
--
-- Response shape:
--   200 {"job_id": "...", "queue": "<site>.action_server.missions"}
--   400 {"error": "...", "status": 400}   -- validation / bad JSON
--   500 {"error": "...", "status": 500}   -- FFI / NATS / encode failure

local cjson  = require("cjson.safe")
local api    = require("api")
local submit = require("submit")

if ngx.req.get_method() ~= "POST" then
  api.fail(405, "method not allowed (POST only)")
end

ngx.req.read_body()
local body = ngx.req.get_body_data()
if not body or body == "" then
  api.fail(400, "empty body")
end

local input, jerr = cjson.decode(body)
if not input then
  api.fail(400, "invalid JSON: " .. tostring(jerr))
end

local ok, vmsg = submit.validate(input)
if not ok then
  api.fail(400, vmsg)
end

local job_id, err = submit.do_submit(input)
if not job_id then
  api.fail(500, err)
end

api.ok({
  job_id = job_id,
  queue  = submit.queue_name(os.getenv("APP_SITE") or ""),
})
