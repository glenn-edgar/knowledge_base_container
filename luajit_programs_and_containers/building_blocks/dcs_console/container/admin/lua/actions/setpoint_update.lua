-- actions/setpoint_update.lua -- POST /action/setpoint/update.
--
-- Body: form-encoded { name, value }.
-- Header: X-Operator.
--
-- Validates against setpoints_catalog, writes the new value via
-- shell_helpers.write_setpoint, appends an audit row, and returns
-- the refreshed <tr> HTML for hx-swap="outerHTML" so the caller's
-- row updates in place without a full page reload.

local sh      = require("shell_helpers")
local catalog = require("setpoints_catalog")
local view    = require("views.system_setpoints")

local M = {}

function M.execute()
  local operator = ngx.req.get_headers()["X-Operator"] or ""
  if operator == "" then
    ngx.status = 400
    ngx.say("missing X-Operator header")
    return
  end
  ngx.req.read_body()
  local args = ngx.req.get_post_args() or {}
  local name = args.name
  local raw  = args.value
  if not name or name == "" then
    ngx.status = 400
    ngx.say("missing name")
    return
  end
  local spec = catalog.by_name[name]
  if not spec then
    ngx.status = 400
    ngx.say("unknown setpoint")
    return
  end
  local ok_v, v_or_err = catalog.validate(name, raw)
  if not ok_v then
    ngx.status = 400
    ngx.say("invalid value: " .. tostring(v_or_err))
    return
  end

  local pg, err = sh.pg_connect()
  if not pg then
    ngx.status = 500
    ngx.say("pg unreachable: " .. tostring(err))
    return
  end
  sh.ensure_audit_log_table(pg)
  local ok_w, werr = sh.write_setpoint(pg, name, v_or_err)
  sh.audit_log_append(pg, operator, "setpoint_update",
                      name, tostring(v_or_err),
                      ok_w and "ok" or ("error: " .. tostring(werr)))

  if not ok_w then
    pg:disconnect()
    ngx.status = 500
    ngx.say("write failed: " .. tostring(werr))
    return
  end

  -- Return the refreshed row so the caller's hx-swap="outerHTML"
  -- drops the new state right in place.
  local info = sh.read_setpoint(pg, name) or { current = v_or_err, default = nil }
  pg:disconnect()

  ngx.header["Content-Type"] = "text/html; charset=utf-8"
  ngx.status = 200
  ngx.print(view.build_row(spec, info))
end

return M
