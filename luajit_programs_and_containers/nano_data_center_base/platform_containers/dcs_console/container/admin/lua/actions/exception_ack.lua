-- actions/exception_ack.lua -- POST /action/exception/ack.
--
-- Body: form-encoded { path = "system.site...SYS_EXCEPTION.<name>", note? }
-- Header: X-Operator (injected by shell.js on every htmx request).
--
-- Success: 200 with empty body + hx-swap="delete" on the caller's row
-- (caller set hx-target=closest tr).
-- Audit row appended regardless of outcome so the attempt is tracked.

local sh = require("shell_helpers")

local M = {}

function M.execute()
  local operator = ngx.req.get_headers()["X-Operator"] or ""
  if operator == "" then
    ngx.status = 400
    ngx.say("missing X-Operator header (is your shell.js loaded?)")
    return
  end
  ngx.req.read_body()
  local args = ngx.req.get_post_args() or {}
  local path = args.path
  local note = args.note or ""
  if not path or path == "" then
    ngx.status = 400
    ngx.say("missing path")
    return
  end

  local pg, err = sh.pg_connect()
  if not pg then
    ngx.status = 500
    ngx.say("pg unreachable: " .. tostring(err))
    return
  end
  sh.ensure_audit_log_table(pg)
  local ok, derr = sh.ack_exception(pg, path, operator, note)
  sh.audit_log_append(pg, operator, "ack", path, note,
                       ok and "ok" or ("error: " .. tostring(derr)))
  pg:disconnect()

  if not ok then
    ngx.status = 500
    ngx.say("ack failed: " .. tostring(derr))
    return
  end

  -- Empty body; caller's hx-swap="delete" removes the row.
  ngx.status = 200
  ngx.header["Content-Type"] = "text/html; charset=utf-8"
  ngx.print("")
end

return M
