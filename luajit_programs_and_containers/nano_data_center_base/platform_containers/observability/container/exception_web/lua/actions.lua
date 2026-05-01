-- actions.lua -- POST /action handler for ack / shelve / clear / unshelve.
--
-- Form inputs:
--   op         "ack" | "shelve" | "clear" | "unshelve"
--   path       ltree of the SYS_EXCEPTION header
--   duration_s (shelve only) seconds to shelve; 0 = suppress indefinitely
--   operator   optional; defaults to "ops"
--   comment    optional operator note
--
-- On success: 302 redirect back to the Referer (typically the detail page
-- the form was submitted from, so refresh shows the new state).

local h = require("helpers")

if ngx.var.request_method ~= "POST" then
  ngx.status = 405
  ngx.say("method not allowed")
  return
end

local args = h.read_post_args()
local op   = args.op
local path = args.path
local operator = args.operator or "ops"
local comment  = args.comment

if not op or not path then
  ngx.status = 400
  ngx.say("missing op or path")
  return
end

local pg, err = h.pg_connect()
if not pg then
  ngx.status = 500
  ngx.say("pg connect: " .. tostring(err))
  return
end

local ok = true
if op == "ack" then
  h.ack(pg, path, operator, comment)
elseif op == "clear" then
  h.clear(pg, path)
elseif op == "shelve" then
  local dur = tonumber(args.duration_s) or 0
  h.shelve(pg, path, dur, operator, comment or
    string.format("operator shelve (%ds)", dur))
elseif op == "unshelve" then
  h.unshelve(pg, path, operator)
else
  pg:keepalive(60000, 8)
  ngx.status = 400
  ngx.say("unknown op: " .. tostring(op))
  return
end

pg:keepalive(60000, 8)

-- 303 See Other: redirect back to detail page (or the Referer if same host).
local referer = ngx.var.http_referer or ("/detail?path=" .. h.urlencode(path))
ngx.redirect(referer, 303)
