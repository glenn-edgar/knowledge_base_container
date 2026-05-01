-- sse_views/cpu_summary.lua -- live refresh of CPU Summary while the
-- status popup is open. Reuses views.cpu_summary.build_body() so the
-- popup mirrors the main-area rendering.

local view = require("views.cpu_summary")

local M = {}

function M.stream(cpu_id)
  ngx.header["Content-Type"]      = "text/event-stream"
  ngx.header["Cache-Control"]     = "no-cache"
  ngx.header["X-Accel-Buffering"] = "no"

  local function emit_once()
    local html = view.build_body(cpu_id)
    local oneline = html:gsub("[\r\n]+", " ")
    ngx.print("event: update\ndata: ", oneline, "\n\n")
    return ngx.flush(true)
  end

  emit_once()
  while true do
    ngx.sleep(2)
    local ok = emit_once()
    if not ok then break end
  end
end

return M
