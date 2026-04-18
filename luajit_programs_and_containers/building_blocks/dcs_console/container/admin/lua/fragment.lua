-- fragment.lua -- /fragment/<path> dispatcher.
--
-- Maps path -> views/<module>. Real renderers land here one at a time;
-- anything not yet mapped falls back to views/placeholder.

local path = ngx.var[1] or ""

-- Path -> view-module name. Keep alphabetised for easy scanning.
local ROUTES = {
  ["system/overview"] = "views.system_overview",
}

local mod_name = ROUTES[path]
local ok, view
if mod_name then
  ok, view = pcall(require, mod_name)
end

if ok and view and view.render then
  view.render()
  return
end

-- Fallback: placeholder with path-derived title.
local placeholder = require("views.placeholder")
placeholder.render(path)
