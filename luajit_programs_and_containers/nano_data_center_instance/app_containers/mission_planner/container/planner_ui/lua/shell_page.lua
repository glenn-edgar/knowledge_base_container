-- planner_ui :: shell page (Phase 5b C3, extended C5).
--
-- Server-rendered shell. C1 had inline placeholder content; C3 hooks
-- the SVG L1 renderer (assets/map_render.js) which fetches /api/boards
-- on load, populates a board picker in the header, and renders into
-- #map-region on selection. C5 adds the launcher bar (robot input +
-- "Pick source & target" mode toggle + submit button) so an operator
-- can queue a mission via direct FFI to NATS JobQueue.
--
-- htmx is intentionally NOT loaded yet -- vanilla JS is sufficient
-- for L1 rendering and the launcher click-state machine. htmx returns
-- in 5b C6 (mission status overlay polling).

local render = require("render")
local ctx    = render.context()
local h      = render.html_escape

ngx.header.content_type = "text/html; charset=utf-8"
ngx.say(string.format([[<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>planner_ui :: %s</title>
<link rel="stylesheet" href="/assets/planner_ui.css">
</head>
<body>
<header>
  <h1>mission_planner :: planner_ui</h1>
  <span class="id">tenant <strong>%s</strong> &middot;
       container <strong>%s</strong> &middot;
       site <strong>%s</strong></span>
  <!-- board picker is appended here by map_render.js once /api/boards
       has loaded -->
</header>
<main>
  <section id="map-region">
    <p class="loading">loading board list...</p>
  </section>
  <aside id="launcher-bar">
    <h2>Launch Mission</h2>
    <div class="launcher-row">
      <label for="robot-input">robot:</label>
      <input id="robot-input" type="text" placeholder="rover_1"
             autocomplete="off" spellcheck="false">
    </div>
    <button id="launcher-mode-btn" type="button">
      Pick source &amp; target
    </button>
    <p id="launcher-hint" class="launcher-hint">
      enter robot id, then click "Pick source &amp; target" and tap
      two nodes on the map.
    </p>
    <div id="launcher-selection" class="launcher-selection">
      <span class="src">source: <strong id="launcher-source">&mdash;</strong></span>
      <span class="dst">target: <strong id="launcher-target">&mdash;</strong></span>
    </div>
    <button id="submit-mission-btn" type="button" disabled>
      Submit mission
    </button>
    <div id="launcher-toast" class="launcher-toast" role="status"></div>
  </aside>
  <aside id="status-region">
    <h2>Mission Status</h2>
    <p class="placeholder">live mission status lands in 5b C6
       (poll action_server NATS keys; htmx swap on update).</p>
  </aside>
</main>
<script src="/assets/map_render.js" defer></script>
</body>
</html>
]], h(ctx.planner_namespace), h(ctx.planner_namespace),
    h(ctx.container_name), h(ctx.site)))
