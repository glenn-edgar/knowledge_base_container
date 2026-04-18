-- status.lua -- /status/<path> dispatcher. Phase 2 keeps the
-- placeholder for every view since the main system/overview uses the
-- SSE stream; phase 3+ adds one-shot status HTML per view.

local path = ngx.var[1] or "unknown"
ngx.header["Content-Type"] = "text/html; charset=utf-8"
ngx.say(string.format(
  "<div><strong>%s</strong></div>\n" ..
  "<div class=\"empty\">Static placeholder status for this view." ..
  " Real content in a later session.</div>",
  path))
