-- landing.lua -- render the gateway UI surface.
--
-- Two entry points:
--   M.render_tabs() -- served at GET /. Tab-managed workspace: sidebar
--                      catalog of registered UIs on the left, tab strip +
--                      iframe stack on the right. Clicking a sidebar item
--                      opens (or focuses if already open) a tab. Tabs +
--                      active index persist in localStorage across reload.
--   M.render(status) -- plain catalog page used as the 404 body from
--                       route.lua when an unknown route is hit. Unchanged
--                       from the initial implementation.
--
-- Both read the registered-UI list from shared_dict ui_registry (populated
-- by poller.lua). Cold-start (registry empty) is handled: the tabs UI
-- still renders -- sidebar just shows "no UIs registered".

local cjson       = require("cjson.safe")
local ui_registry = ngx.shared.ui_registry

local M = {}

local function html_escape(s)
  if s == nil then return "" end
  return (tostring(s)
    :gsub("&", "&amp;")
    :gsub("<", "&lt;")
    :gsub(">", "&gt;")
    :gsub('"', "&quot;")
    :gsub("'", "&#39;"))
end

local function fetch_menu()
  if ui_registry:get("_ready") ~= "1" then return {} end
  local blob = ui_registry:get("_blob")
  local tab  = blob and cjson.decode(blob)
  return (tab and tab.menu) or {}
end

---------------------------------------------------------------------------
-- Plain catalog (used as 404 body). Unchanged from v1.
---------------------------------------------------------------------------

local function render_rows(menu)
  if #menu == 0 then
    return "<p>No registered UIs.</p>\n"
  end
  local out = { "<table><thead><tr><th>Container</th><th>Slot</th>",
                "<th>External</th><th>Description</th></tr></thead><tbody>\n" }
  for _, e in ipairs(menu) do
    local href = "/ui/" .. e.container .. "/" .. e.slot .. "/"
    out[#out + 1] = string.format(
      '<tr><td><a href="%s">%s</a></td><td>%s</td><td>%d</td><td>%s</td></tr>\n',
      html_escape(href), html_escape(e.container), html_escape(e.slot),
      tonumber(e.external) or 0, html_escape(e.description))
  end
  out[#out + 1] = "</tbody></table>\n"
  return table.concat(out)
end

function M.render(status)
  status = status or 200
  local menu = fetch_menu()

  ngx.status = status
  ngx.header["Content-Type"] = "text/html; charset=utf-8"
  ngx.header["Cache-Control"] = "no-store"

  local title = (status == 404) and "Route not found" or "DCS gateway"
  ngx.say("<!doctype html>")
  ngx.say('<meta charset="utf-8">')
  ngx.say("<title>", html_escape(title), "</title>")
  ngx.say([[<style>
body{font-family:system-ui,monospace;background:#111;color:#ddd;
     padding:2em;max-width:60em;margin:auto}
h1{color:#fff}
code,a{color:#7fbfff}
table{border-collapse:collapse;width:100%;margin-top:1em}
th,td{border-bottom:1px solid #333;padding:0.4em 0.7em;text-align:left}
th{color:#888;font-weight:normal;font-size:0.9em}
a{text-decoration:none}
a:hover{text-decoration:underline}
.hdr{color:#e55}
</style>]])
  if status == 404 then
    ngx.say('<h1 class="hdr">404 &mdash; route not found</h1>')
    ngx.say("<p>Known routes:</p>")
  else
    ngx.say("<h1>DCS gateway</h1>")
    ngx.say("<p>Registered UIs:</p>")
  end
  ngx.print(render_rows(menu))
end

---------------------------------------------------------------------------
-- Tab-managed workspace (served at /).
---------------------------------------------------------------------------

local TAB_UI_CSS = [[
*, *::before, *::after { box-sizing: border-box; }
html, body { height: 100%; margin: 0; font-family: system-ui, sans-serif;
             background: #111; color: #ddd; }
body { display: flex; flex-direction: column; }

/* ---- workspace (full width now; sidebar is an overlay drawer) ---- */
#workspace { flex: 1; display: flex; flex-direction: column; min-width: 0; }

/* ---- tab strip with hamburger at the start --------------------- */
#tabs { display: flex; background: #181818; border-bottom: 1px solid #333;
        min-height: 2.4em; overflow-x: auto; align-items: stretch; }
#menu-btn { background: none; border: 0; color: #ddd; font-size: 1.3em;
            padding: 0 0.7em; cursor: pointer; line-height: 1;
            border-right: 1px solid #333; flex-shrink: 0; }
#menu-btn:hover { background: #222; }

.tab { display: inline-flex; align-items: center; padding: 0 0.35em 0 0.9em;
       border-right: 1px solid #333; background: #1a1a1a; color: #aaa;
       cursor: pointer; gap: 0.3em; user-select: none; white-space: nowrap;
       font-size: 0.9em; }
.tab:hover { background: #222; }
.tab.active { background: #2a2a2a; color: #fff; }
.tab .close { display: inline-block; width: 1.5em; height: 1.5em;
              line-height: 1.5em; text-align: center; border-radius: 3px;
              color: #777; }
.tab .close:hover { background: #444; color: #fff; }

/* ---- sidebar drawer (mobile: full-width; desktop: 20em) -------- */
#sidebar { position: fixed; top: 0; left: 0; bottom: 0; width: 100%;
           max-width: 22em; background: #1a1a1a;
           border-right: 1px solid #333;
           display: flex; flex-direction: column; overflow: hidden;
           transform: translateX(-100%); transition: transform 0.18s ease-out;
           z-index: 30; }
#sidebar.open { transform: translateX(0); }
#sidebar header { display: flex; align-items: center; justify-content: space-between;
                  padding: 0.9em 1em; border-bottom: 1px solid #333;
                  color: #fff; font-weight: 600; letter-spacing: 0.02em; }
#sidebar header small { color: #666; font-weight: 400; }
#sidebar-close { background: none; border: 0; color: #aaa; font-size: 1.3em;
                 cursor: pointer; padding: 0 0.4em; }
#sidebar-close:hover { color: #fff; }

.catalog { list-style: none; margin: 0; padding: 0; overflow-y: auto; flex: 1; }
.catalog .empty { padding: 1em; color: #666; font-size: 0.9em; }
.catalog li { padding: 0.6em 1em; border-bottom: 1px solid #222;
              cursor: pointer; line-height: 1.3; }
.catalog li:hover { background: #222; }
.catalog li.open { background: #1f2a33; }
.catalog li .c { color: #fff; font-weight: 500; }
.catalog li .s { color: #7fbfff; font-size: 0.9em; }
.catalog li .e { color: #666; font-size: 0.78em; margin-left: 0.4em; }
.catalog li .d { color: #888; font-size: 0.8em; margin-top: 0.15em; }

#scrim { position: fixed; inset: 0; background: rgba(0,0,0,0.5);
         z-index: 25; opacity: 0; pointer-events: none;
         transition: opacity 0.18s; }
#scrim.open { opacity: 1; pointer-events: auto; }

/* ---- frames + placeholder ------------------------------------- */
#frames { flex: 1; position: relative; overflow: hidden; }
iframe.frame { position: absolute; inset: 0; width: 100%; height: 100%;
               border: 0; display: none; background: #fff; }
iframe.frame.active { display: block; }

#placeholder { position: absolute; inset: 0; display: flex;
               flex-direction: column; align-items: center;
               justify-content: center; color: #666; text-align: center;
               padding: 1em; }
#placeholder.hidden { display: none; }
#placeholder h2 { font-weight: normal; color: #aaa; margin: 0 0 0.4em; }
#placeholder p { margin: 0; font-size: 0.9em; }

@media (min-width: 700px) {
  #sidebar { width: 20em; }
}
]]

local TAB_UI_JS = [[
(function () {
  const STORAGE_KEY = 'dcs_gateway_tabs_v1';

  function loadState() {
    try {
      const raw = localStorage.getItem(STORAGE_KEY);
      if (raw) {
        const s = JSON.parse(raw);
        if (s && Array.isArray(s.tabs)) {
          s.nextId = s.nextId || (s.tabs.reduce((a, t) => Math.max(a, t.id), 0) + 1);
          return s;
        }
      }
    } catch (e) {}
    return { tabs: [], activeId: null, nextId: 1 };
  }

  const state = loadState();

  function save() {
    try { localStorage.setItem(STORAGE_KEY, JSON.stringify(state)); } catch (e) {}
  }

  const tabsEl     = document.getElementById('tabs');
  const framesEl   = document.getElementById('frames');
  const placeEl    = document.getElementById('placeholder');
  const catalogEl  = document.getElementById('catalog');
  const sidebarEl  = document.getElementById('sidebar');
  const scrimEl    = document.getElementById('scrim');
  const menuBtn    = document.getElementById('menu-btn');
  const closeBtn   = document.getElementById('sidebar-close');

  function openDrawer()  { sidebarEl.classList.add('open');  scrimEl.classList.add('open'); }
  function closeDrawer() { sidebarEl.classList.remove('open'); scrimEl.classList.remove('open'); }

  menuBtn.addEventListener('click', openDrawer);
  closeBtn.addEventListener('click', closeDrawer);
  scrimEl.addEventListener('click', closeDrawer);
  document.addEventListener('keydown', function (e) {
    if (e.key === 'Escape' && sidebarEl.classList.contains('open')) closeDrawer();
  });

  function render() {
    // Clear just the .tab children; keep the hamburger button element
    // (and any other non-tab siblings) intact.
    Array.prototype.forEach.call(tabsEl.querySelectorAll('.tab'), function (t) { t.remove(); });
    state.tabs.forEach(function (t) {
      const btn = document.createElement('span');
      btn.className = 'tab' + (t.id === state.activeId ? ' active' : '');
      btn.dataset.id = t.id;
      const label = document.createElement('span');
      label.textContent = t.title;
      btn.appendChild(label);
      const close = document.createElement('span');
      close.className = 'close';
      close.textContent = '\u00d7';
      close.title = 'Close tab';
      close.addEventListener('click', function (e) {
        e.stopPropagation();
        closeTab(t.id);
      });
      btn.appendChild(close);
      btn.addEventListener('click', function () { activate(t.id); });
      tabsEl.appendChild(btn);
    });

    // Iframes (create missing, drop stale, toggle active).
    const openIds = {};
    state.tabs.forEach(function (t) {
      openIds[t.id] = true;
      let f = document.getElementById('frame-' + t.id);
      if (!f) {
        f = document.createElement('iframe');
        f.className = 'frame';
        f.id = 'frame-' + t.id;
        f.src = '/ui/' + t.route + '/';
        framesEl.appendChild(f);
      }
      f.classList.toggle('active', t.id === state.activeId);
    });
    Array.prototype.forEach.call(framesEl.querySelectorAll('iframe'), function (f) {
      const id = parseInt(f.id.replace('frame-', ''), 10);
      if (!openIds[id]) f.remove();
    });

    // Placeholder.
    placeEl.classList.toggle('hidden', state.tabs.length > 0);

    // Sidebar highlight for open routes.
    Array.prototype.forEach.call(catalogEl.querySelectorAll('li'), function (li) {
      const r = li.dataset.route;
      const isOpen = state.tabs.some(function (t) { return t.route === r; });
      li.classList.toggle('open', isOpen);
    });
  }

  function openOrFocus(route, title) {
    const existing = state.tabs.find(function (t) { return t.route === route; });
    if (existing) {
      state.activeId = existing.id;
    } else {
      const id = state.nextId++;
      state.tabs.push({ id: id, route: route, title: title || route });
      state.activeId = id;
    }
    save(); render();
  }

  function closeTab(id) {
    const idx = state.tabs.findIndex(function (t) { return t.id === id; });
    if (idx < 0) return;
    state.tabs.splice(idx, 1);
    if (state.activeId === id) {
      const next = state.tabs[idx - 1] || state.tabs[idx] || null;
      state.activeId = next ? next.id : null;
    }
    save(); render();
  }

  function activate(id) {
    state.activeId = id;
    save(); render();
  }

  // Bind sidebar clicks. Closing the drawer after a selection is the
  // usual mobile pattern -- on desktop the drawer overlays content and
  // staying open is noisy.
  Array.prototype.forEach.call(catalogEl.querySelectorAll('li'), function (li) {
    li.addEventListener('click', function () {
      openOrFocus(li.dataset.route, li.dataset.title || li.dataset.route);
      closeDrawer();
    });
  });

  // If the previously-active tab was closed some other way, pick the first.
  if (state.activeId && !state.tabs.some(function (t) { return t.id === state.activeId; })) {
    state.activeId = state.tabs.length ? state.tabs[0].id : null;
  }

  render();
})();
]]

function M.render_tabs()
  local menu = fetch_menu()

  ngx.status = 200
  ngx.header["Content-Type"] = "text/html; charset=utf-8"
  ngx.header["Cache-Control"] = "no-store"

  ngx.say("<!doctype html>")
  ngx.say('<html lang="en"><head><meta charset="utf-8">')
  ngx.say("<title>DCS gateway</title>")
  ngx.say("<style>", TAB_UI_CSS, "</style>")
  ngx.say("</head><body>")

  -- Sidebar drawer (hidden off-screen until hamburger clicked).
  ngx.say('<aside id="sidebar">')
  ngx.say('<header>')
  ngx.say('<span>DCS gateway <small>&middot; ', tostring(#menu),
          ' UI', (#menu == 1) and "" or "s", '</small></span>')
  ngx.say('<button id="sidebar-close" type="button" aria-label="Close menu">&times;</button>')
  ngx.say('</header>')
  ngx.say('<ul class="catalog" id="catalog">')
  if #menu == 0 then
    ngx.say('<li class="empty">No UIs registered yet.</li>')
  else
    for _, e in ipairs(menu) do
      local route = e.container .. "/" .. e.slot
      local title = e.container .. " &middot; " .. e.slot
      ngx.say(string.format(
        '<li data-route="%s" data-title="%s">' ..
          '<span class="c">%s</span> ' ..
          '<span class="s">%s</span>' ..
          '<span class="e">:%d</span>' ..
          '<div class="d">%s</div>' ..
        '</li>',
        html_escape(route), html_escape(title),
        html_escape(e.container), html_escape(e.slot),
        tonumber(e.external) or 0, html_escape(e.description)))
    end
  end
  ngx.say("</ul></aside>")

  -- Scrim behind the drawer.
  ngx.say('<div id="scrim"></div>')

  -- Workspace: tab strip (with hamburger button) + frames.
  ngx.say('<section id="workspace">')
  ngx.say('<nav id="tabs">')
  ngx.say('<button id="menu-btn" type="button" aria-label="Menu">&#9776;</button>')
  ngx.say('</nav>')
  ngx.say('<div id="frames">')
  ngx.say('<div id="placeholder">')
  ngx.say('<h2>No tab open.</h2>')
  ngx.say('<p>Tap the menu (&#9776;) and pick a UI.</p>')
  ngx.say('</div></div></section>')

  ngx.say("<script>", TAB_UI_JS, "</script>")
  ngx.say("</body></html>")
end

return M
