-- sse_views/container_status.lua -- live refresh of the container
-- detail view while the status popup is open. Reuses
-- views.container_status.build_body() so popup content mirrors the
-- main area exactly.

local view = require("views.container_status")

local M = {}

function M.stream(name)
  ngx.header["Content-Type"]      = "text/event-stream"
  ngx.header["Cache-Control"]     = "no-cache"
  ngx.header["X-Accel-Buffering"] = "no"

  local function emit_once()
    local html = view.build_body(name)
    local oneline = html:gsub("[\r\n]+", " ")
    ngx.print("event: update\ndata: ", oneline, "\n\n")
    return ngx.flush(true)
  end

  emit_once()
  while true do
    ngx.sleep(3)                        -- container state changes rarely
    local ok = emit_once()
    if not ok then break end
  end
end

return M
