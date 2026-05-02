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
  { id = "tree",     label = "Tree",          path = "/tree"     },
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

---------------------------------------------------------------------------
-- Tree renderer (Phase B Layer O)
--
-- Recursive <details>/<summary> — pure CSS, no JS, browser-native expand/
-- collapse. Leaves link to /detail?path=<full_path> so the existing alarm
-- detail view opens for each SYS_EXCEPTION.
---------------------------------------------------------------------------

local TREE_CSS = [[
<style>
  .tree { font: 13px/1.5 ui-monospace, "SF Mono", Menlo, Consolas, monospace;
          padding-left: 0.4em; }
  .tree details { margin: 0; padding: 0; }
  .tree summary {
    list-style: none; cursor: pointer; padding: 0.15em 0.4em;
    border-radius: 3px; user-select: none;
  }
  .tree summary::-webkit-details-marker { display: none; }
  .tree summary:hover { background: #12161c; }
  .tree summary::before {
    content: "▸"; display: inline-block; width: 1em; color: var(--muted);
  }
  .tree details[open] > summary::before { content: "▾"; }
  .tree .node-name  { color: var(--fg); }
  .tree .node-count { color: var(--muted); margin-left: 0.5em; font-size: 0.85em; }
  .tree .node-active { color: var(--st-unack-active); margin-left: 0.4em;
                       font-size: 0.8em; font-weight: 700; }
  .tree .leaf {
    display: block; padding: 0.15em 0.4em 0.15em 1.4em;
    border-radius: 3px; color: var(--accent); text-decoration: none;
  }
  .tree .leaf:hover { background: #12161c; text-decoration: underline; }
  .tree .leaf .leaf-meta { color: var(--muted); margin-left: 0.6em; font-size: 0.8em; }
  .tree ul { list-style: none; margin: 0; padding-left: 1.2em; }
  .tree li { margin: 0; }
</style>
]]

local function sorted_child_keys(children)
  local keys = {}
  for k in pairs(children or {}) do keys[#keys + 1] = k end
  table.sort(keys)
  return keys
end

function M.render_tree_node(node, leaf_url_fn, open_top)
  local h_ = require("helpers")
  local parts = {}
  local function emit(s) parts[#parts + 1] = s end

  if node.is_leaf then
    local state_cls = "st-" .. (node.state or "normal")
    emit(string.format(
      '<a class="leaf" href="%s"><span>%s</span>' ..
      '<span class="state-badge %s">%s</span>' ..
      '<span class="leaf-meta">priority %d</span></a>',
      h_.escape(leaf_url_fn(node.path)),
      h_.escape(node.name),
      state_cls, h_.escape(node.state or "normal"),
      node.priority or 4))
    return table.concat(parts, "")
  end

  emit(open_top and '<details open>' or '<details>')
  local active_badge = ""
  if (node.active_count or 0) > 0 then
    active_badge = string.format('<span class="node-active">%d active</span>',
      node.active_count)
  end
  emit(string.format(
    '<summary><span class="node-name">%s</span>' ..
    '<span class="node-count">(%d)</span>%s</summary>',
    h_.escape(node.name or ""),
    node.leaf_count or 0,
    active_badge))
  emit('<ul>')
  for _, k in ipairs(sorted_child_keys(node.children)) do
    emit('<li>')
    emit(M.render_tree_node(node.children[k], leaf_url_fn, false))
    emit('</li>')
  end
  emit('</ul>')
  emit('</details>')
  return table.concat(parts, "")
end

function M.render_tree(root, leaf_url_fn)
  local parts = { TREE_CSS, '<div class="tree">' }
  if not root.children or next(root.children) == nil then
    parts[#parts + 1] = '<p class="empty">no entries.</p></div>'
    return table.concat(parts, "")
  end
  for _, k in ipairs(sorted_child_keys(root.children)) do
    parts[#parts + 1] = M.render_tree_node(root.children[k], leaf_url_fn, true)
  end
  parts[#parts + 1] = '</div>'
  return table.concat(parts, "")
end

return M
