-- actions.lua -- POST /action handler for KB_RULE enable / disable /
-- shelve / unshelve.
--
-- Form inputs:
--   op         "enable" | "disable" | "shelve" | "unshelve"
--   path       ltree of the KB_RULE
--   duration_s (shelve only) seconds to shelve; 0 = indefinite

local h = require("helpers")

if ngx.var.request_method ~= "POST" then
  ngx.status = 405; ngx.say("method not allowed"); return
end

local args = h.read_post_args()
local op   = args.op
local path = args.path

if not op or not path then
  ngx.status = 400; ngx.say("missing op or path"); return
end

local pg, err = h.pg_connect()
if not pg then
  ngx.status = 500; ngx.say("pg connect: " .. tostring(err)); return
end

if op == "enable" then
  h.rule_set_enabled(pg, path, true)
elseif op == "disable" then
  h.rule_set_enabled(pg, path, false)
elseif op == "shelve" then
  h.rule_shelve(pg, path, tonumber(args.duration_s) or 300)
elseif op == "unshelve" then
  h.rule_unshelve(pg, path)
else
  pg:keepalive(60000, 8)
  ngx.status = 400; ngx.say("unknown op: " .. tostring(op)); return
end

pg:keepalive(60000, 8)

local referer = ngx.var.http_referer or "/rules"
ngx.redirect(referer, 303)
