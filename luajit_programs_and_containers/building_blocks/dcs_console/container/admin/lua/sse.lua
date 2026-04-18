-- sse.lua -- /sse/<path> dispatcher.
--
-- Real per-view streams replace the placeholder one at a time.

local path = ngx.var[1] or ""

local ROUTES = {
  ["system/overview"] = "sse_views.system_overview",
}

local mod_name = ROUTES[path]
local ok, stream_mod
if mod_name then
  ok, stream_mod = pcall(require, mod_name)
end

if ok and stream_mod and stream_mod.stream then
  stream_mod.stream()
  return
end

local placeholder = require("sse_views.placeholder")
placeholder.stream(path)
