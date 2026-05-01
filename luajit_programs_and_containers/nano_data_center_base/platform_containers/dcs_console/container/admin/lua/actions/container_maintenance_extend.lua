-- actions/container_maintenance_extend.lua -- POST /action/container/
-- maintenance-extend. Same as start: pushes maintenance_until to
-- now() + unmonitor_lease_default_s. "Extend" semantic = restart the
-- lease clock from now, NOT "add lease to current expiry" (simpler +
-- bounded even if operator taps repeatedly).
--
-- Separate action name so the audit log distinguishes intentional
-- extensions from fresh starts; otherwise the SQL is identical.

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

  -- cpu_id lookup may need to fall back (container deregistered).
  local c = sh.get_container(pg, name)
  local cpu_id = c and c.cpu_id
  if not cpu_id then
    local rs = pg:query(string.format([[
      SELECT subpath(path, -3, 1)::text AS cpu_id
      FROM knowledge_base
      WHERE label = 'container' AND name = '%s'
      LIMIT 1
    ]], name:gsub("'", "''")))
    if rs and rs[1] and rs[1].cpu_id then
      cpu_id = rs[1].cpu_id
    end
  end
  if not cpu_id then
    sh.audit_log_append(pg, operator, "maintenance_extend", name, "",
                         "error: cpu_id unresolvable")
    pg:disconnect()
    ngx.status = 404
    ngx.say("cannot resolve cpu for container")
    return
  end

  local lease    = sh.maintenance_lease_default(pg)
  local until_ts = os.time() + lease
  local ok, werr = sh.write_maintenance_until(pg, cpu_id, name, until_ts)
  sh.audit_log_append(pg, operator, "maintenance_extend", name,
                       tostring(lease) .. "s",
                       ok and ("ok until=" .. until_ts)
                          or ("error: " .. tostring(werr)))
  pg:disconnect()

  if not ok then
    ngx.status = 500
    ngx.say("write failed: " .. tostring(werr))
    return
  end

  local html, ctx = view.build_body(name)
  ngx.header["Content-Type"] = "text/html; charset=utf-8"
  sh.set_context(ctx)
  ngx.say(html)
end

return M
