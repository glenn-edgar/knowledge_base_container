-- actions/exception_clear.lua -- POST /action/exception/clear.
--
-- Same contract as exception_ack plus an optional `note` form field.
-- Flips status=false on the exception (moves it to History).

local sh = require("shell_helpers")

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
  local ok, derr = sh.clear_exception(pg, path, operator, note)
  sh.audit_log_append(pg, operator, "clear", path, note,
                       ok and "ok" or ("error: " .. tostring(derr)))
  pg:disconnect()

  if not ok then
    ngx.status = 500
    ngx.say("clear failed: " .. tostring(derr))
    return
  end

  ngx.status = 200
  ngx.header["Content-Type"] = "text/html; charset=utf-8"
  ngx.print("")
end

return M
