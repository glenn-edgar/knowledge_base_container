-- render.lua -- shared page-frame renderer for exception_web views.
--
-- Every view calls render.page(title, active_tab, body_html) which emits:
--   * <head> with CSS + meta
--   * nav strip (tabs for the 5 views)
--   * <main> containing body_html
--
-- Pure server-render; no JS dependency. Browser refresh model — no live
-- polling, no SSE. Click a nav tab = full page load.

local h = require("helpers")

local M = {}

local CSS = [[
<style>
  :root {
    --bg: #0b0d10;
    --fg: #ddd;
    --muted: #888;
    --panel: #161a1f;
    --panel-border: #2a323c;
    --accent: #6af;
    --pri-1: #f44;
    --pri-2: #f90;
    --pri-3: #fc4;
    --pri-4: #8a8;
    --st-unack-active: #f44;
    --st-ack-active: #f90;
    --st-rtn-unack: #fc4;
    --st-shelved: #8af;
    --st-normal: #6b6;
  }
  * { box-sizing: border-box; }
  body {
    margin: 0; padding: 0;
    font: 14px/1.4 ui-monospace, "SF Mono", Menlo, Consolas, monospace;
    background: var(--bg); color: var(--fg);
  }
  header.bar {
    background: #12161c; border-bottom: 1px solid var(--panel-border);
    padding: 0.5em 1em; display: flex; align-items: center; gap: 1em;
  }
  header.bar h1 {
    margin: 0; font-size: 1em; font-weight: 600; color: var(--accent);
  }
  nav.tabs {
    display: flex; gap: 0.5em; margin-left: auto;
  }
  nav.tabs a {
    color: var(--muted); text-decoration: none;
    padding: 0.3em 0.8em; border: 1px solid transparent;
    border-radius: 4px;
  }
  nav.tabs a:hover { color: var(--fg); }
  nav.tabs a.active {
    color: var(--fg); border-color: var(--panel-border);
    background: var(--panel);
  }
  main {
    max-width: 1400px; margin: 0 auto; padding: 1.5em 1em;
  }
  h2 { color: var(--accent); margin-top: 0; font-size: 1.1em; }
  h3 { color: #8c8; margin-bottom: 0.3em; font-size: 0.95em; }
  .panel {
    background: var(--panel); border: 1px solid var(--panel-border);
    border-radius: 4px; padding: 1em; margin-bottom: 1em;
  }
  .grid-4 {
    display: grid; grid-template-columns: repeat(4, 1fr); gap: 0.8em;
  }
  .pri-box {
    padding: 0.8em; border-radius: 4px; text-align: center;
    background: #0f1317; border: 1px solid var(--panel-border);
  }
  .pri-box .n {
    font-size: 1.8em; font-weight: 600; display: block;
  }
  .pri-box .label { color: var(--muted); font-size: 0.85em; }
  .pri-1 .n, .pri-1 { border-color: var(--pri-1); color: var(--pri-1); }
  .pri-2 .n, .pri-2 { border-color: var(--pri-2); color: var(--pri-2); }
  .pri-3 .n, .pri-3 { border-color: var(--pri-3); color: var(--pri-3); }
  .pri-4 .n, .pri-4 { border-color: var(--pri-4); color: var(--pri-4); }
  table {
    width: 100%; border-collapse: collapse; font-size: 0.92em;
  }
  table th, table td {
    padding: 0.4em 0.6em; text-align: left;
    border-bottom: 1px solid var(--panel-border);
  }
  table th { color: var(--muted); font-weight: 500; }
  table tr:hover { background: #12161c; }
  .state-badge {
    display: inline-block; padding: 2px 8px; border-radius: 3px;
    font-size: 0.8em; font-weight: 600;
    background: #0f1317; border: 1px solid;
  }
  .st-unack-active { color: var(--st-unack-active); border-color: var(--st-unack-active); }
  .st-ack-active   { color: var(--st-ack-active);   border-color: var(--st-ack-active); }
  .st-rtn-unack    { color: var(--st-rtn-unack);    border-color: var(--st-rtn-unack); }
  .st-shelved      { color: var(--st-shelved);      border-color: var(--st-shelved); }
  .st-normal       { color: var(--st-normal);       border-color: var(--st-normal); }
  .empty {
    color: var(--muted); font-style: italic; padding: 2em; text-align: center;
  }
  .err { color: #f88; }
  .footer-note {
    color: var(--muted); font-size: 0.8em; margin-top: 2em; text-align: center;
  }
  a { color: var(--accent); }
</style>
]]

local TABS = {
  { id = "overview", label = "Site Overview", path = "/overview" },
  { id = "active",   label = "Active Alarms", path = "/alarms"   },
  { id = "journal",  label = "Alarm Journal", path = "/journal"  },
  { id = "shelved",  label = "Shelved",       path = "/shelved"  },
}

local function render_tabs(active_id)
  local parts = { '<nav class="tabs">' }
  for _, t in ipairs(TABS) do
    local cls = (t.id == active_id) and ' class="active"' or ""
    parts[#parts + 1] = string.format('<a href="%s"%s>%s</a>',
      h.mk_url(t.path), cls, t.label)
  end
  parts[#parts + 1] = "</nav>"
  return table.concat(parts, "")
end

--- Render a full HTML page with the shared frame.
--- @param title  string  shown in the header bar + <title>
--- @param tab    string  active nav tab id ("overview" | "active" | ...)
--- @param body   string  page body HTML (already escaped)
function M.page(title, tab, body)
  ngx.header["Content-Type"]  = "text/html; charset=utf-8"
  ngx.header["Cache-Control"] = "no-store"
  ngx.say('<!doctype html>')
  ngx.say('<html lang="en"><head>')
  ngx.say('<meta charset="utf-8">')
  ngx.say('<meta name="viewport" content="width=device-width, initial-scale=1">')
  ngx.say(string.format('<title>observability :: %s</title>', title))
  ngx.say(CSS)
  -- Expose the gateway prefix to inline JS (e.g. uPlot fetch URLs).
  -- "" when the app is hit directly. JS-encoded as a string literal.
  ngx.say(string.format(
    '<script>window.GATEWAY_PREFIX=%q;</script>', h.gateway_prefix()))
  ngx.say('</head><body>')
  ngx.say('<header class="bar">')
  ngx.say('<h1>observability / exceptions</h1>')
  ngx.say(render_tabs(tab))
  ngx.say('</header>')
  ngx.say('<main>')
  ngx.say(body)
  ngx.say('<div class="footer-note">SCADA-style alarm ops · refresh browser for latest state</div>')
  ngx.say('</main></body></html>')
end

--- Error fallback: render a visible error to the page.
function M.error_page(title, tab, msg)
  M.page(title, tab, string.format(
    '<div class="panel"><p class="err">error: %s</p></div>', msg or "(no message)"))
end

return M
