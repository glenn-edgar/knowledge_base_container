-- views/placeholder.lua -- fallback renderer for menu branches that
-- haven't been built yet. Returns a short "coming soon" body plus a
-- minimal shell:context (title derived from path; status endpoints
-- pointing at the matching placeholder stream + one-shot). Keeps the
-- nav plumbing exercised while phases 3-6 fill in real content.

local sh = require("shell_helpers")

local M = {}

function M.render(path)
  -- Derive a breadcrumb-ish title: "system / ready_bits" -> "system · ready_bits".
  local segs = {}
  for s in path:gmatch("[^/]+") do segs[#segs + 1] = s end
  -- Plain ASCII separator: non-ASCII bytes in HTTP header values get
  -- re-interpreted as Latin-1 by the browser (mojibake). Keep titles
  -- in pure ASCII for the `HX-Trigger-After-Settle` payload.
  local title = (#segs > 0) and table.concat(segs, " / ") or "DCS admin"

  -- Every placeholder view opts in to a one-shot status; a few also opt
  -- in to SSE so the stream plumbing stays exercised until real views
  -- replace them.
  local ctx = {
    title      = title,
    status_url = "status/" .. path,
  }
  if path == "exception/active"
     or path:match("^cpu/[^/]+/summary$") then
    ctx.status_stream_url = "sse/" .. path
  end

  -- Active-exception count propagates to the alarm badge on every view.
  -- We open a short pg connection just for this one query. If pg is
  -- unreachable, the badge simply stays hidden.
  local pg = sh.pg_connect()
  if pg then
    local n = sh.active_exception_count(pg)
    pg:disconnect()
    if n > 0 then ctx.badge = tostring(n) end
  end

  ngx.header["Content-Type"] = "text/html; charset=utf-8"
  sh.set_context(ctx)

  ngx.say(string.format(
    "<h2>%s</h2>\n" ..
    "<p class=\"placeholder\">Placeholder for <code>/fragment/%s</code>" ..
    " &mdash; this menu branch lands in a later phase.</p>",
    sh.escape(title), sh.escape(path)))
end

return M
