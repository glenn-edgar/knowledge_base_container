-- planner_ui :: shell page (Phase 5b C3).
--
-- Server-rendered shell. C1 had inline placeholder content; C3 hooks
-- the SVG L1 renderer (assets/map_render.js) which fetches /api/boards
-- on load, populates a board picker in the header, and renders into
-- #map-region on selection.
--
-- htmx is intentionally NOT loaded yet -- vanilla JS is sufficient
-- for L1 rendering. htmx returns in 5b C4 (drill-down fragment swaps)
-- and 5b C6 (mission status overlay polling).

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
