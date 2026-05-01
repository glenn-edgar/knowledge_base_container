-- sse_views/placeholder.lua -- stub SSE stream for views that opt in to
-- streaming but haven't had their real stream built yet. Emits a single
-- "hello" event, then holds the connection open with 15s comment
-- keepalives until the client disconnects.

local M = {}

function M.stream(path)
  ngx.header["Content-Type"]      = "text/event-stream"
  ngx.header["Cache-Control"]     = "no-cache"
  ngx.header["X-Accel-Buffering"] = "no"

  ngx.print(string.format(
    "event: update\n" ..
    "data: <div><strong>%s</strong> &mdash; SSE stub</div>" ..
    "<div class=\"empty\">Phase 1 stream placeholder." ..
    " Real per-view pushes arrive in a later session.</div>\n\n",
    path))
  ngx.flush(true)

  while true do
    ngx.sleep(15)
    local ok = ngx.print(": keepalive\n\n")
    if not ok then break end
    ngx.flush(true)
  end
end

return M
