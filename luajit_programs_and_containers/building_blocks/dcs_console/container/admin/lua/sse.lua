-- sse.lua -- /sse/<path> dispatcher.
--
-- Static routes match a ROUTES table; dynamic routes (cpu/<id>/<aspect>)
-- match a regex and pass the captured id into the stream's stream()
-- function. Unmatched paths fall back to sse_views/placeholder.

local path = ngx.var[1] or ""

local STATIC_ROUTES = {
  ["system/overview"]  = "sse_views.system_overview",
  ["exception/active"] = "sse_views.exception_active",
}

local mod_name = STATIC_ROUTES[path]
local ok, stream_mod

if mod_name then
  ok, stream_mod = pcall(require, mod_name)
end

-- Dynamic: cpu/<id>/<aspect>.
if not (ok and stream_mod) then
  local cpu_id, aspect = path:match("^cpu/([^/]+)/([^/]+)$")
  if cpu_id and aspect then
    local sok, smod = pcall(require, "sse_views.cpu_" .. aspect)
    if sok and smod and smod.stream then
      smod.stream(cpu_id)
      return
    end
  end
end

-- Dynamic: container/<name>/<aspect>.
if not (ok and stream_mod) then
  local name, aspect = path:match("^container/([^/]+)/([^/]+)$")
  if name and aspect then
    local sok, smod = pcall(require, "sse_views.container_" .. aspect)
    if sok and smod and smod.stream then
      smod.stream(name)
      return
    end
  end
end

if ok and stream_mod and stream_mod.stream then
  stream_mod.stream()
  return
end

local placeholder = require("sse_views.placeholder")
placeholder.stream(path)
