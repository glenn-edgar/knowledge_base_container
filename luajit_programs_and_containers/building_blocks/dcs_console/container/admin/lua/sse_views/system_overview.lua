-- sse_views/system_overview.lua -- live stream for the status popup
-- when the current view is system/overview. Emits a full re-render of
-- the overview body every 2s while the popup is open.
--
-- The popup body subscribes via htmx-ext-sse; the shell tears down the
-- EventSource (and thus this coroutine) when the popup closes or the
-- operator navigates away.

local sh   = require("shell_helpers")
local view = require("views.system_overview")

local M = {}

function M.stream()
  ngx.header["Content-Type"]      = "text/event-stream"
  ngx.header["Cache-Control"]     = "no-cache"
  ngx.header["X-Accel-Buffering"] = "no"

  -- Emit an initial event immediately so the popup's "Waiting for first
  -- event..." placeholder disappears on open.
  local function emit_once()
    local html = view.build_body()
    -- SSE data lines must not contain raw newlines; flatten the HTML.
    local oneline = html:gsub("[\r\n]+", " ")
    ngx.print("event: update\ndata: ", oneline, "\n\n")
    local ok = ngx.flush(true)
    return ok
  end

  emit_once()

  while true do
    ngx.sleep(2)
    local ok = emit_once()
    if not ok then break end
  end
end

return M
