#!/usr/bin/env luajit
-- =============================================================================
-- test_planner_ui_chassis.lua -- Phase 5b C1 acceptance.
--
-- Coverage:
--   render.lua (pure Lua, host-testable):
--     - html_escape: nil -> ""; basic HTML metacharacters
--     - context: pulls env vars; falls back to "(unset)" for missing
--       OR empty values
--
--   shell_page.lua + health.lua (call ngx.*; can't fully execute
--   host-side):
--     - parse-checked via loadfile(); chunk loads without syntax error
--     - documents the deferred validation: end-to-end behavior is
--       verified by the cluster smoke (hit /health and /, observe
--       200 + valid HTML / JSON)
--
-- Required env at run: none (tests manipulate env via ffi setenv).
-- =============================================================================

local SCRIPT_DIR = arg[0]:match("(.*/)") or "./"
local REPO_ROOT  = SCRIPT_DIR .. "../../../../../"
local PUI        = REPO_ROOT
    .. "nano_data_center_instance/app_containers/mission_planner/container/planner_ui"
package.path = PUI .. "/lua/?.lua;" .. package.path

local pass, fail = 0, 0
local function ok(name, cond, detail)
  if cond then pass = pass + 1; print("  ok  " .. name)
  else fail = fail + 1; print("  FAIL " .. name .. (detail and " -- " .. detail or "")) end
end

------------------------------------------------------------------------
-- env manipulation helpers (Lua has no unsetenv; use ffi).
------------------------------------------------------------------------
local ffi = require("ffi")
pcall(ffi.cdef, [[
  int setenv(const char *name, const char *value, int overwrite);
  int unsetenv(const char *name);
]])
local function clear_env(name) pcall(ffi.C.unsetenv, name) end
local function set_env(name, value) pcall(ffi.C.setenv, name, value, 1) end
local function clear_all()
  for _, n in ipairs({"CONTAINER_NAME", "PLANNER_NAMESPACE",
                      "APP_SITE", "APP_SYSTEM"}) do
    clear_env(n)
  end
end

------------------------------------------------------------------------
print("== render.html_escape ==")
------------------------------------------------------------------------

do
  local render = require("render")
  ok("nil -> empty string",         render.html_escape(nil) == "")
  ok("plain string passes through", render.html_escape("hello") == "hello")
  ok("ampersand escaped",
     render.html_escape("a&b") == "a&amp;b")
  ok("less-than escaped",
     render.html_escape("<script>") == "&lt;script&gt;")
  ok("double-quote escaped",
     render.html_escape('say "hi"') == "say &quot;hi&quot;")
  ok("single-quote escaped",
     render.html_escape("it's") == "it&#39;s")
  ok("number coerced to string",
     render.html_escape(42) == "42")
  -- Order matters: ampersand must be escaped FIRST or it will eat
  -- subsequent &lt; / &gt; substitutions.
  ok("escape order: '&<' -> '&amp;&lt;' (not &amp;lt;)",
     render.html_escape("&<") == "&amp;&lt;")
end

------------------------------------------------------------------------
print()
print("== render.context ==")
------------------------------------------------------------------------

do
  local render = require("render")
  -- Cached require + module-level state? render.lua's context() is
  -- a function so each call re-reads env. Verify by clearing then
  -- setting.
  clear_all()
  local c1 = render.context()
  ok("missing CONTAINER_NAME -> '(unset)'",
     c1.container_name == "(unset)",
     "got " .. tostring(c1.container_name))
  ok("missing PLANNER_NAMESPACE -> '(unset)'",
     c1.planner_namespace == "(unset)")
  ok("missing APP_SITE -> '(unset)'",   c1.site == "(unset)")
  ok("missing APP_SYSTEM -> '(unset)'", c1.system == "(unset)")

  set_env("CONTAINER_NAME",    "mission_planner_01")
  set_env("PLANNER_NAMESPACE", "surface_ops")
  set_env("APP_SITE",          "moonbase.alpha.surface_ops")
  set_env("APP_SYSTEM",        "ros_planner_ii")
  local c2 = render.context()
  ok("env -> CONTAINER_NAME picked up",
     c2.container_name == "mission_planner_01")
  ok("env -> PLANNER_NAMESPACE picked up",
     c2.planner_namespace == "surface_ops")
  ok("env -> APP_SITE picked up",
     c2.site == "moonbase.alpha.surface_ops")
  ok("env -> APP_SYSTEM picked up",
     c2.system == "ros_planner_ii")

  -- Empty string treated as fallback (matches conf default behavior:
  -- env var declared but never set yields "")
  set_env("PLANNER_NAMESPACE", "")
  local c3 = render.context()
  ok("empty PLANNER_NAMESPACE -> '(unset)' fallback",
     c3.planner_namespace == "(unset)")

  clear_all()
end

------------------------------------------------------------------------
print()
print("== shell_page.lua + health.lua: parse-load (no execute) ==")
------------------------------------------------------------------------

-- These handlers reference ngx.* at runtime; we can't execute them
-- host-side without an ngx stub. loadfile catches syntax errors; full
-- behavior is verified by the cluster smoke (curl / and /health).

do
  local SHELL = PUI .. "/lua/shell_page.lua"
  local HEALTH = PUI .. "/lua/health.lua"
  local chunk, err = loadfile(SHELL)
  ok("shell_page.lua parses cleanly",
     chunk ~= nil, err and tostring(err) or "")
  chunk, err = loadfile(HEALTH)
  ok("health.lua parses cleanly",
     chunk ~= nil, err and tostring(err) or "")
end

------------------------------------------------------------------------
print()
print(string.format("SUMMARY: %d passed, %d failed", pass, fail))
os.exit(fail > 0 and 1 or 0)
