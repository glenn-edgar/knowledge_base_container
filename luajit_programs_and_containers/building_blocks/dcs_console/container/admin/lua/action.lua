-- action.lua -- POST /action/<path> dispatcher.
--
-- Unlike fragment.lua / sse.lua (GET), this is a mutation surface.
-- Requires:
--   * POST method
--   * X-Operator header (shell.js injects)
-- Each action module exposes an execute() function with its own body/
-- header validation. Response is HTML the caller's htmx swaps in
-- place (e.g., empty body + hx-swap="delete" to remove a row).

local method = ngx.req.get_method()
if method ~= "POST" then
  ngx.status = 405
  ngx.header["Allow"] = "POST"
  ngx.say("method not allowed")
  return
end

local path = ngx.var[1] or ""

local ROUTES = {
  ["exception/ack"]              = "actions.exception_ack",
  ["exception/clear"]            = "actions.exception_clear",
  ["setpoint/update"]            = "actions.setpoint_update",
  ["container/maintenance-start"]  = "actions.container_maintenance_start",
  ["container/maintenance-end"]    = "actions.container_maintenance_end",
  ["container/maintenance-extend"] = "actions.container_maintenance_extend",
}

local mod_name = ROUTES[path]
if not mod_name then
  ngx.status = 404
  ngx.say("unknown action: " .. path)
  return
end

local ok, mod = pcall(require, mod_name)
if not ok or not mod or not mod.execute then
  ngx.status = 500
  ngx.say("action handler load error: " .. tostring(mod))
  return
end

mod.execute()
