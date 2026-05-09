#!/usr/bin/env luajit
-- =============================================================================
-- test_planner_ui_renderer.lua -- Phase 5b C3 / C4 / C5 / C6 acceptance
-- for the SVG L1 renderer + L2 drill-down + popup + launcher + status
-- overlay (vanilla JS).
--
-- The JS runs in a browser; we can't exercise the SVG output host-side
-- without a JS engine. This test verifies:
--   - asset files exist and are non-empty
--   - the JS contains the expected exported behavior surface (function
--     names + API endpoint strings + state shape) so a refactor that
--     drops a hook is caught
--   - the CSS contains the expected selectors so styling regressions
--     are caught at the file level
--   - shell_page.lua references both assets and the launcher elements
--
-- Browser-side behavior verified by the cluster smoke (load planner_ui
-- in a browser, observe board picker, pick board, see SVG render,
-- click "Pick source & target", click two nodes, type robot, submit).
-- =============================================================================

local SCRIPT_DIR = arg[0]:match("(.*/)") or "./"
local REPO_ROOT  = SCRIPT_DIR .. "../../../../../"
local PUI        = REPO_ROOT
    .. "nano_data_center_instance/app_containers/mission_planner/container/planner_ui"

local pass, fail = 0, 0
local function ok(name, cond, detail)
  if cond then pass = pass + 1; print("  ok  " .. name)
  else fail = fail + 1; print("  FAIL " .. name .. (detail and " -- " .. detail or "")) end
end

local function read_file(path)
  local f, err = io.open(path, "rb")
  if not f then return nil, err end
  local content = f:read("*a")
  f:close()
  return content
end

------------------------------------------------------------------------
print("== assets exist + non-empty ==")
------------------------------------------------------------------------

local css_content = read_file(PUI .. "/assets/planner_ui.css")
ok("planner_ui.css exists",       css_content ~= nil)
ok("planner_ui.css non-empty",    css_content and #css_content > 100)

local js_content = read_file(PUI .. "/assets/map_render.js")
ok("map_render.js exists",        js_content ~= nil)
ok("map_render.js non-empty",     js_content and #js_content > 500)

------------------------------------------------------------------------
print()
print("== map_render.js: behavior surface ==")
------------------------------------------------------------------------

if js_content then
  -- API endpoints referenced
  ok("references /api/boards",
     js_content:find("/api/boards", 1, true) ~= nil)
  ok("references /api/board/<name>",
     js_content:find('/api/board/" + encodeURIComponent', 1, true) ~= nil)

  -- Expected function/identifier surface (C3 baseline + C4 additions).
  -- These names are what a maintainer would grep for first; renaming
  -- requires a deliberate cross-file update.
  for _, fn in ipairs({
    "loadBoards", "loadBoard", "renderBoard", "renderRegion",
    "renderEdges", "renderNodes", "buildPicker", "bboxOfBoard", "init",
    -- C4 additions
    "hermitePoints", "renderSegment", "renderL2",
    "showNodePopup", "closePopup", "popupOpen",
    -- C5 launcher additions
    "pickNode", "setLauncherMode", "clearLauncherSelection",
    "submitMission", "wireLauncher",
    "refreshLauncherSelectionDisplay", "setLauncherHint",
    "setLauncherToast",
    -- C6 status overlay additions
    "pollStatus", "renderMissionCards", "showMissionDetail",
    "startStatusPolling", "stopStatusPolling", "relativeTimeStr",
  }) do
    ok("function " .. fn .. " present",
       js_content:find("function " .. fn, 1, true) ~= nil
       or js_content:find(fn .. " = function", 1, true) ~= nil
       or js_content:find(fn .. ":", 1, true) ~= nil
       or js_content:find(fn .. "%(", 1) ~= nil)
  end

  -- C4: leaf-kind colors mirror visualizer.py
  for _, kind in ipairs({
    "straight_line", "spline", "rotate",
    "wall_follow", "line_follow", "activate",
  }) do
    ok("LEAF_COLORS includes " .. kind,
       js_content:find(kind .. ":", 1, true) ~= nil)
  end

  -- C4: Esc key handler + state machine markers
  ok('Esc handler present (key === "Escape")',
     js_content:find('"Escape"', 1, true) ~= nil)
  ok("state.currentView referenced",
     js_content:find("currentView", 1, true) ~= nil)
  ok("state.currentEdgeIdx referenced",
     js_content:find("currentEdgeIdx", 1, true) ~= nil)

  -- C5: launcher state shape
  ok("launcher.mode in state",
     js_content:find("launcher.mode", 1, true) ~= nil
     or js_content:find("mode: false", 1, true) ~= nil)
  ok("launcher.pickRole in state",
     js_content:find("pickRole", 1, true) ~= nil)
  ok("launcher.source / launcher.target tracked",
     js_content:find("launcher.source", 1, true) ~= nil and
     js_content:find("launcher.target", 1, true) ~= nil)

  -- C5: state.currentBoardName threaded through picker change
  ok("state.currentBoardName threaded",
     js_content:find("currentBoardName", 1, true) ~= nil)

  -- C5: POST /api/submit_mission target endpoint
  ok("POSTs to /api/submit_mission",
     js_content:find("/api/submit_mission", 1, true) ~= nil)
  ok('uses fetch method "POST"',
     js_content:find('method: "POST"', 1, true) ~= nil
     or js_content:find("method:'POST'", 1, true) ~= nil)
  ok("Content-Type: application/json sent",
     js_content:find("application/json", 1, true) ~= nil)

  -- C5: source/target highlight class plumbing
  ok("node-source class applied",
     js_content:find("node-source", 1, true) ~= nil)
  ok("node-target class applied",
     js_content:find("node-target", 1, true) ~= nil)

  -- C5: Esc handler also exits launcher mode
  ok("Esc exits launcher mode (setLauncherMode(false))",
     js_content:find("setLauncherMode(false)", 1, true) ~= nil)

  -- C6: status polling shape
  ok("STATUS_POLL_MS constant defined",
     js_content:find("STATUS_POLL_MS", 1, true) ~= nil)
  ok("STATUS_POLL_MS = 2000 (2s cadence)",
     js_content:find("STATUS_POLL_MS = 2000", 1, true) ~= nil)
  ok("statusState in-flight guard",
     js_content:find("statusState", 1, true) ~= nil and
     js_content:find("inFlight", 1, true) ~= nil)
  ok("polls /api/missions",
     js_content:find("/api/missions", 1, true) ~= nil)
  ok("fetches /api/mission/<robot> on detail",
     js_content:find('/api/mission/" + encodeURIComponent', 1, true)
     ~= nil)
  ok("visibilitychange handler pauses polling",
     js_content:find("visibilitychange", 1, true) ~= nil and
     js_content:find("document.hidden", 1, true) ~= nil)
  ok("setInterval used for polling",
     js_content:find("setInterval", 1, true) ~= nil)
  ok("clearInterval on stop",
     js_content:find("clearInterval", 1, true) ~= nil)
  ok("startStatusPolling called from init",
     js_content:find("startStatusPolling()", 1, true) ~= nil)

  -- SVG element types referenced (the renderer creates these)
  for _, elt in ipairs({ '"polygon"', '"line"', '"circle"',
                          '"rect"', '"text"', '"svg"', '"g"' }) do
    ok("SVG element " .. elt .. " referenced",
       js_content:find(elt, 1, true) ~= nil)
  end

  -- Y-flip transform present (world Y up vs SVG Y down)
  ok("Y-flip transform present",
     js_content:find("scale(1,-1)", 1, true) ~= nil)
end

------------------------------------------------------------------------
print()
print("== planner_ui.css: selector surface ==")
------------------------------------------------------------------------

if css_content then
  -- L1-required selectors
  for _, sel in ipairs({
    "#map-svg", ".region", ".edge", ".node-passive", ".node-active",
    ".node-label", ".board-picker", ".error", ".loading",
  }) do
    ok("selector " .. sel .. " present",
       css_content:find(sel, 1, true) ~= nil)
  end

  -- C4: leaf-kind classes (one per kind)
  for _, kind in ipairs({
    "straight_line", "spline", "rotate",
    "wall_follow", "line_follow", "activate",
  }) do
    ok("CSS class .leaf-" .. kind .. " present",
       css_content:find(".leaf-" .. kind, 1, true) ~= nil)
  end

  -- C4: L2 + popup selectors
  for _, sel in ipairs({
    ".l2-bar", ".back-button", ".l2-title", ".l2-endpoint",
    ".popup-overlay", ".popup", ".popup-close", ".edge-hit",
  }) do
    ok("selector " .. sel .. " present",
       css_content:find(sel, 1, true) ~= nil)
  end

  -- C5: launcher selectors. Some elements (robot-input, launcher-hint,
  -- launcher-toast) are styled by class / descendant selectors rather
  -- than by ID -- check the selectors that actually appear.
  for _, sel in ipairs({
    "#launcher-bar", "#launcher-mode-btn", "#submit-mission-btn",
    ".launcher-row", ".launcher-hint", ".launcher-selection",
    ".launcher-toast", ".launcher-toast.success",
    ".launcher-toast.error", ".node-source", ".node-target",
  }) do
    ok("selector " .. sel .. " present",
       css_content:find(sel, 1, true) ~= nil)
  end
  ok("launcher-mode-active body cursor cue present",
     css_content:find("launcher-mode-active", 1, true) ~= nil)

  -- C6: mission status selectors
  for _, sel in ipairs({
    ".status-card", ".status-card-head", ".status-state",
    ".status-board", ".status-meta", ".status-empty",
    ".mission-detail", ".status-state-active",
    ".status-state-failed", ".status-state-complete",
  }) do
    ok("selector " .. sel .. " present",
       css_content:find(sel, 1, true) ~= nil)
  end

  -- Color variables defined for the theme
  for _, v in ipairs({ "--bg", "--text", "--accent", "--passive",
                        "--active", "--edge", "--region",
                        -- C5
                        "--source", "--target" }) do
    ok("CSS var " .. v .. " defined",
       css_content:find(v .. ":", 1, true) ~= nil)
  end
end

------------------------------------------------------------------------
print()
print("== shell_page.lua: references new assets ==")
------------------------------------------------------------------------

local shell = read_file(PUI .. "/lua/shell_page.lua")
ok("shell_page.lua exists", shell ~= nil)
if shell then
  ok("shell links /assets/planner_ui.css",
     shell:find("/assets/planner_ui.css", 1, true) ~= nil)
  ok("shell loads /assets/map_render.js",
     shell:find("/assets/map_render.js", 1, true) ~= nil)
  ok("shell has #map-region container (renderer target)",
     shell:find('id="map-region"', 1, true) ~= nil)
  ok("shell has #status-region container",
     shell:find('id="status-region"', 1, true) ~= nil)

  -- C5: launcher elements injected by shell_page.lua
  ok("shell has #launcher-bar (C5)",
     shell:find('id="launcher%-bar"') ~= nil)
  ok("shell has #robot-input (C5)",
     shell:find('id="robot%-input"') ~= nil)
  ok("shell has #launcher-mode-btn (C5)",
     shell:find('id="launcher%-mode%-btn"') ~= nil)
  ok("shell has #submit-mission-btn (C5)",
     shell:find('id="submit%-mission%-btn"') ~= nil)
  ok("submit button starts disabled",
     shell:find('disabled', 1, true) ~= nil)

  -- Should NOT have a giant inline <style> block any more (CSS
  -- moved to external file). Heuristic: <style> presence + > 200
  -- chars between <style> and </style>.
  local style_open = shell:find("<style", 1, true)
  if style_open then
    local style_close = shell:find("</style>", style_open, true)
    local style_len = style_close and (style_close - style_open) or 0
    ok("inline <style> block removed (or short)",
       style_len < 200,
       "inline style block is " .. style_len .. " chars")
  else
    ok("no inline <style> block (good)", true)
  end

  -- Lua file still parses
  local chunk, err = loadfile(PUI .. "/lua/shell_page.lua")
  ok("shell_page.lua parses cleanly",
     chunk ~= nil, err and tostring(err) or "")
end

------------------------------------------------------------------------
print()
print(string.format("SUMMARY: %d passed, %d failed", pass, fail))
os.exit(fail > 0 and 1 or 0)
