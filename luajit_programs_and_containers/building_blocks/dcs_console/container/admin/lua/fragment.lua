-- fragment.lua -- /fragment/<path> dispatcher.
--
-- Routes to views/<module> by path. Static routes go through ROUTES;
-- dynamic routes (cpu/<id>/<aspect>, container/<name>/<aspect>) match
-- a regex and pass the captured id/aspect into the view's render().
-- Unmatched paths fall back to views/placeholder.

local path = ngx.var[1] or ""

-- 1. Static routes.
local STATIC_ROUTES = {
  ["system/overview"]         = "views.system_overview",
  ["exception/active"]        = "views.exception_active",
  ["exception/acknowledged"]  = "views.exception_acknowledged",
  ["exception/history"]       = "views.exception_history",
}

local mod_name = STATIC_ROUTES[path]
local ok, view, arg

if mod_name then
  ok, view = pcall(require, mod_name)
end

-- 2. Dynamic: cpu/<id>/<aspect>. One view module per aspect, cpu_id
-- passed as an argument to render().
if not (ok and view) then
  local cpu_id, aspect = path:match("^cpu/([^/]+)/([^/]+)$")
  if cpu_id and aspect then
    local cpu_mod = "views.cpu_" .. aspect
    local cok, cview = pcall(require, cpu_mod)
    if cok and cview and cview.render then
      cview.render(cpu_id)
      return
    end
  end
end

-- 3. Dynamic: container/<name>/<aspect>. Same shape as cpu routes.
if not (ok and view) then
  local name, aspect = path:match("^container/([^/]+)/([^/]+)$")
  if name and aspect then
    local con_mod = "views.container_" .. aspect
    local cok, cview = pcall(require, con_mod)
    if cok and cview and cview.render then
      cview.render(name)
      return
    end
  end
end

-- 3. Matched static view: invoke its render().
if ok and view and view.render then
  view.render()
  return
end

-- 4. Fallback: placeholder with path-derived title.
local placeholder = require("views.placeholder")
placeholder.render(path)
