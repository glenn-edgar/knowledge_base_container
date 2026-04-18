-- route.lua -- access_by_lua handler for /ui/<container>/<slot>/<rest>.
--
-- Reads the route table (populated by poller.lua) from shared_dict
-- ui_registry and resolves $upstream_addr for the proxy_pass. Short-
-- circuits with:
--   503   cold start (poller hasn't completed a cycle yet) -- design #5a
--   404   unknown route; body = landing page listing known routes (#6a)

local cjson   = require("cjson.safe")
local landing = require("landing")

local ui_registry = ngx.shared.ui_registry

if ui_registry:get("_ready") ~= "1" then
  ngx.status = 503
  ngx.header["Content-Type"] = "text/plain"
  ngx.header["Retry-After"]  = "5"
  ngx.say("Gateway initializing; registry not yet loaded.")
  return ngx.exit(503)
end

local blob = ui_registry:get("_blob")
local tab  = blob and cjson.decode(blob) or nil
local routes = (tab and tab.routes) or {}

local container = ngx.var.container
local slot      = ngx.var.slot
local key       = container .. "/" .. slot

local entry = routes[key]
if not entry then
  -- Landing page rendered with 404 status. Self-recovering on typos.
  landing.render(404)
  return ngx.exit(404)
end

ngx.var.gw_upstream = entry.upstream
-- proxy_pass takes over after access phase.
