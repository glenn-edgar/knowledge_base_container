-- actions/container_maintenance_start.lua -- POST /action/container/
-- maintenance-start. Writes maintenance_until = now() +
-- unmonitor_lease_default_s, audits, and returns the refreshed
-- container_status view body (via hx-target="#shell-content").

local sh   = require("shell_helpers")
local view = require("views.container_status")

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
  if not name or name == "" then
    ngx.status = 400
    ngx.say("missing name")
    return
  end

  local pg, err = sh.pg_connect()
  if not pg then
    ngx.status = 500
    ngx.say("pg unreachable: " .. tostring(err))
    return
  end
  sh.ensure_audit_log_table(pg)

  local c = sh.get_container(pg, name)
  if not c or not c.cpu_id then
    sh.audit_log_append(pg, operator, "maintenance_start", name, "",
                         "error: no such container in CONTAINER_REGISTRY")
    pg:disconnect()
    ngx.status = 404
    ngx.say("container not in registry")
    return
  end

  local lease    = sh.maintenance_lease_default(pg)
  local until_ts = os.time() + lease
  local ok, werr = sh.write_maintenance_until(pg, c.cpu_id, name, until_ts)
  sh.audit_log_append(pg, operator, "maintenance_start", name,
                       tostring(lease) .. "s",
                       ok and ("ok until=" .. until_ts)
                          or ("error: " .. tostring(werr)))
  pg:disconnect()

  if not ok then
    ngx.status = 500
    ngx.say("write failed: " .. tostring(werr))
    return
  end

  -- Return the refreshed view body + shell:context so the header
  -- (title, badge, status_url, stream_url) resettles too.
  local html, ctx = view.build_body(name)
  ngx.header["Content-Type"] = "text/html; charset=utf-8"
  sh.set_context(ctx)
  ngx.say(html)
end

return M
