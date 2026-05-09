-- planner_ui :: shared API helpers (Phase 5b C2).
--
-- Tiny helpers shared across api_*.lua handlers: JSON envelope
-- responses, pg connection lifecycle. Keeping the per-handler files
-- short by factoring out the boilerplate.

local cjson = require("cjson.safe")
local db    = require("db")

local M = {}

-- Return JSON 200 with the payload, then close the connection.
function M.ok(payload)
  ngx.header.content_type = "application/json"
  ngx.status = 200
  ngx.say(cjson.encode(payload))
end

-- Return JSON error with a status code + message. Used for 404 / 500
-- / 503; htmx fragments can swap a clean error widget on these.
function M.fail(status, msg)
  ngx.header.content_type = "application/json"
  ngx.status = status
  ngx.say(cjson.encode({ error = msg, status = status }))
  ngx.exit(status)
end

-- Run fn(pg) with a fresh pgmoon connection; convert pg-connect failure
-- to 503. fn returns (payload, err); on err returns 500.
function M.with_pg(fn)
  local pg, cerr = db.connect()
  if not pg then
    return M.fail(503, "pg unavailable: " .. tostring(cerr))
  end
  local payload, err = fn(pg)
  -- Best-effort keepalive; ignore close errors.
  pcall(function() pg:keepalive() end)
  if not payload then
    return M.fail(500, tostring(err))
  end
  return M.ok(payload)
end

return M
