-- actions/container_maintenance_end.lua -- POST /action/container/
-- maintenance-end. Writes maintenance_until = 0 so node_control's
-- next tick sees the transition and docker-runs the container back.
-- Audits and returns the refreshed view.

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

  -- The container may have been deregistered while in maintenance;
  -- fall back to scanning the KB to find its cpu_id.
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
    sh.audit_log_append(pg, operator, "maintenance_end", name, "",
                         "error: cpu_id unresolvable")
    pg:disconnect()
    ngx.status = 404
    ngx.say("cannot resolve cpu for container")
    return
  end

  local ok, werr = sh.write_maintenance_until(pg, cpu_id, name, 0)
  sh.audit_log_append(pg, operator, "maintenance_end", name, "",
                       ok and "ok" or ("error: " .. tostring(werr)))
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
