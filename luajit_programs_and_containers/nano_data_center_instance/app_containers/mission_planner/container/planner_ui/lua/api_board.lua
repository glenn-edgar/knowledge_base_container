-- planner_ui :: GET /api/board/:name (Phase 5b C2).
--
-- Returns the compiled board JSON content as the response body. The
-- file_store stores it as a JSON blob; we return it verbatim with
-- Content-Type application/json so a browser fetch can JSON.parse()
-- it directly. Sets X-Board-SHA256 header for cache-busting / drift
-- detection on the client side.

local api = require("api")
local db  = require("db")

local name = ngx.var[1]
if not name or name == "" then
  api.fail(400, "missing board name in path")
  return
end

local pg, cerr = db.connect()
if not pg then
  api.fail(503, "pg unavailable: " .. tostring(cerr))
  return
end

local content, sha_or_err = db.get_board(pg, name)
pcall(function() pg:keepalive() end)

if not content then
  -- get_board returns nil + err; "board not found" -> 404, others -> 500.
  if tostring(sha_or_err):find("board not found", 1, true) then
    api.fail(404, sha_or_err)
  else
    api.fail(500, sha_or_err)
  end
  return
end

ngx.header.content_type = "application/json"
ngx.header["X-Board-SHA256"] = sha_or_err
ngx.status = 200
ngx.print(content)
