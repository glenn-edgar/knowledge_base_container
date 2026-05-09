-- planner_ui :: shell page (Phase 5b C1).
--
-- Server-rendered shell. Placeholder content; the SVG map renderer
-- lands in 5b C3 (location swap inside #map-region) and the mission
-- status overlay lands in 5b C6 (#status-region).
--
-- Layout follows the dcs_console pattern: header with identity, two
-- empty regions sized for their future contents, htmx loaded so later
-- fragments can swap into the regions without a full reload.

local render = require("render")
local ctx    = render.context()
local h      = render.html_escape

ngx.header.content_type = "text/html; charset=utf-8"
ngx.say(string.format([[<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>planner_ui :: %s</title>
<script src="/assets/htmx.min.js" defer></script>
<style>
:root { color-scheme: dark; }
body { margin: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI",
       monospace; background: #111; color: #ddd; }
header { background: #1a1a1a; border-bottom: 1px solid #333;
         padding: 0.6em 1em; display: flex; gap: 2em; align-items: baseline; }
header h1 { margin: 0; font-size: 1.05em; color: #fff; font-weight: 600; }
header .id { color: #888; font-size: 0.9em; }
header .id strong { color: #ccc; font-weight: 500; }
main { display: grid; grid-template-columns: 1fr 320px; gap: 1px;
       background: #222; min-height: calc(100vh - 3em); }
#map-region, #status-region { background: #181818; padding: 1em;
                              overflow: auto; }
#map-region .placeholder, #status-region .placeholder {
    color: #666; font-style: italic; }
#status-region h2 { font-size: 0.95em; color: #aaa; margin: 0 0 0.5em 0;
                    font-weight: 500; text-transform: uppercase;
                    letter-spacing: 0.05em; }
</style>
</head>
<body>
<header>
  <h1>mission_planner :: planner_ui</h1>
  <span class="id">tenant <strong>%s</strong> &middot;
       container <strong>%s</strong> &middot;
       site <strong>%s</strong></span>
</header>
<main>
  <section id="map-region">
    <p class="placeholder">map renderer lands in 5b C3
       (boards loaded from file_store, scoped to planner_namespace).</p>
  </section>
  <aside id="status-region">
    <h2>Mission Status</h2>
    <p class="placeholder">live mission status lands in 5b C6
       (poll action_server NATS keys; htmx swap on update).</p>
  </aside>
</main>
</body>
</html>
]], h(ctx.planner_namespace), h(ctx.planner_namespace),
    h(ctx.container_name), h(ctx.site)))
