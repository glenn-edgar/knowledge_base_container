-- sse_views/infra_service.lua -- live infra probe refresh while the
-- status popup is open. Reuses views.infra_service.build_body so the
-- popup mirrors the main area. Tick every 5s; probes take up to 2s
-- apiece, no point hammering the services.

local view = require("views.infra_service")

local M = {}

function M.stream(service)
  ngx.header["Content-Type"]      = "text/event-stream"
  ngx.header["Cache-Control"]     = "no-cache"
  ngx.header["X-Accel-Buffering"] = "no"

  local function emit_once()
    local html = view.build_body(service)
    local oneline = html:gsub("[\r\n]+", " ")
    ngx.print("event: update\ndata: ", oneline, "\n\n")
    return ngx.flush(true)
  end

  emit_once()
  while true do
    ngx.sleep(5)
    local ok = emit_once()
    if not ok then break end
  end
end

return M
