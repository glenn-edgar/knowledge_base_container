-- sse_views/exception_active.lua -- live refresh of the active
-- exception list while its status popup is open. Reuses
-- views.exception_active.build_body() so the popup mirrors the main
-- area (including ack/clear buttons wired to the same POST endpoints).

local view = require("views.exception_active")

local M = {}

function M.stream()
  ngx.header["Content-Type"]      = "text/event-stream"
  ngx.header["Cache-Control"]     = "no-cache"
  ngx.header["X-Accel-Buffering"] = "no"

  local function emit_once()
    local html = view.build_body()
    local oneline = html:gsub("[\r\n]+", " ")
    ngx.print("event: update\ndata: ", oneline, "\n\n")
    return ngx.flush(true)
  end

  emit_once()
  while true do
    ngx.sleep(3)
    local ok = emit_once()
    if not ok then break end
  end
end

return M
