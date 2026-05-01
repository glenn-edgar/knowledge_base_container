-- actions/container_restart.lua -- POST /action/container/restart.
-- Reuses the maintenance-transition machinery for an immediate
-- cycle: write maintenance_until = now() + 1. Node_control's next
-- tick (<=5s) sees the entry transition and stops the container;
-- the tick after that sees the lease has expired and re-runs it.
-- Total cycle: 5-15s, audit-logged as a single "restart" action.

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
  if sh.is_protected_container(c) then
    sh.audit_log_append(pg, operator, "restart", name, "",
                         "error: protected container (" .. (c.definition or "?") .. ")")
    pg:disconnect()
    ngx.status = 403
    ngx.say("refused: " .. name .. " hosts the gateway + admin UI; " ..
            "a restart cycles both offline. Use docker CLI if needed.")
    return
  end
  local cpu_id = c and c.cpu_id
  if not cpu_id then
    local rs = pg:query(string.format([[
      SELECT subpath(path, -3, 1)::text AS cpu_id
      FROM knowledge_base
      WHERE label = 'container' AND name = '%s'
      LIMIT 1
    ]], name:gsub("'", "''")))
    if rs and rs[1] and rs[1].cpu_id then cpu_id = rs[1].cpu_id end
  end
  if not cpu_id then
    sh.audit_log_append(pg, operator, "restart", name, "",
                         "error: cpu_id unresolvable")
    pg:disconnect()
    ngx.status = 404
    ngx.say("cannot resolve cpu for container")
    return
  end

  -- Lease of 1s: node_control stops on tick N, lease has expired by
  -- tick N+1, node_control runs a fresh docker run from spec.
  local until_ts = os.time() + 1
  local ok, werr = sh.write_maintenance_until(pg, cpu_id, name, until_ts)
  sh.audit_log_append(pg, operator, "restart", name, "",
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
