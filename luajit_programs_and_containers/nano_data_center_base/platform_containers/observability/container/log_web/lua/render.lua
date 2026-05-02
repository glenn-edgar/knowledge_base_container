-- log_web/lua/render.lua -- shared page frame for log views.

local h = require("helpers")

local M = {}

local CSS = [[
<style>
  :root {
    --bg: #0b0d10; --fg: #ddd; --muted: #888;
    --panel: #161a1f; --panel-border: #2a323c;
    --accent: #6af; --ok: #6b6; --warn: #fc4; --err: #f44;
    --kind-op: #6af; --kind-arch: #8a8; --kind-diag: #888;
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
  header.bar h1 { margin:0; font-size:1em; font-weight:600; color: var(--accent); }
  nav.tabs { display: flex; gap: 0.5em; margin-left: auto; }
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
  main { max-width: 1400px; margin: 0 auto; padding: 1.5em 1em; }
  h2 { color: var(--accent); margin-top: 0; font-size: 1.1em; }
  h3 { color: #8c8; margin-bottom: 0.3em; font-size: 0.95em; }
  .panel {
    background: var(--panel); border: 1px solid var(--panel-border);
    border-radius: 4px; padding: 1em; margin-bottom: 1em;
  }
  .two-col { display: grid; grid-template-columns: 1fr 280px; gap: 1em; }
  table { width: 100%; border-collapse: collapse; font-size: 0.92em; }
  table th, table td {
    padding: 0.4em 0.6em; text-align: left;
    border-bottom: 1px solid var(--panel-border);
  }
  table th { color: var(--muted); font-weight: 500; }
  table tr:hover { background: #12161c; }
  .kind-badge {
    display: inline-block; padding: 2px 8px; border-radius: 3px;
    font-size: 0.8em; font-weight: 600; border: 1px solid;
  }
  .kind-operational { color: var(--kind-op);   border-color: var(--kind-op); }
  .kind-archival    { color: var(--kind-arch); border-color: var(--kind-arch); }
  .kind-diagnostic  { color: var(--kind-diag); border-color: var(--kind-diag); }
  .empty { color: var(--muted); font-style: italic; padding: 2em; text-align: center; }
  .err { color: var(--err); }
  .kv { display: grid; grid-template-columns: auto 1fr; gap: 0.3em 1em; font-size: 0.9em; }
  .kv dt { color: var(--muted); }
  .kv dd { margin: 0; color: var(--fg); }
  .footer-note { color: var(--muted); font-size: 0.8em; margin-top: 2em; text-align: center; }
  a { color: var(--accent); }
  .time-range { display: flex; gap: 0.3em; margin-bottom: 0.8em; }
  .time-range a {
    padding: 0.25em 0.7em; border: 1px solid var(--panel-border);
    border-radius: 3px; text-decoration: none; color: var(--muted);
    font-size: 0.85em;
  }
  .time-range a.active {
    color: var(--fg); border-color: var(--accent); background: #0f1a22;
  }
  /* uPlot overrides for our dark theme */
  .uplot .u-title { color: var(--fg); }
  .uplot .u-legend { color: var(--muted); font-size: 0.85em; }
</style>
]]

local TABS = {
  { id = "tree",      label = "Tree",             path = "/tree" },
  { id = "live",      label = "Live Operational", path = "/live" },
  { id = "detail",    label = "Log Detail",       path = "/detail" },
  { id = "archival",  label = "Archival",         path = "/archival" },
  { id = "rules",     label = "Rule Inventory",   path = "/rules" },
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

--- Render a full page. If `include_uplot` is truthy, vendors uPlot
--- assets from /static/ (available in the log_web container).
function M.page(title, tab, body, include_uplot)
  ngx.header["Content-Type"]  = "text/html; charset=utf-8"
  ngx.header["Cache-Control"] = "no-store"
  ngx.say('<!doctype html>')
  ngx.say('<html lang="en"><head>')
  ngx.say('<meta charset="utf-8">')
  ngx.say('<meta name="viewport" content="width=device-width, initial-scale=1">')
  ngx.say(string.format('<title>observability :: %s</title>', title))
  ngx.say(CSS)
  if include_uplot then
    ngx.say(string.format(
      '<link rel="stylesheet" href="%s">', h.mk_url("/static/uPlot.min.css")))
    ngx.say(string.format(
      '<script src="%s"></script>', h.mk_url("/static/uPlot.iife.min.js")))
  end
  -- Expose the gateway prefix to inline JS (e.g. uPlot fetch URLs).
  ngx.say(string.format(
    '<script>window.GATEWAY_PREFIX=%q;</script>', h.gateway_prefix()))
  ngx.say('</head><body>')
  ngx.say('<header class="bar">')
  ngx.say('<h1>observability / logs</h1>')
  ngx.say(render_tabs(tab))
  ngx.say('</header>')
  ngx.say('<main>')
  ngx.say(body)
  ngx.say('<div class="footer-note">strip charts via uPlot · refresh browser for fresh data</div>')
  ngx.say('</main></body></html>')
end

function M.error_page(title, tab, msg)
  M.page(title, tab, string.format(
    '<div class="panel"><p class="err">error: %s</p></div>', msg or "(nil)"))
end

---------------------------------------------------------------------------
-- Tree renderer (Phase B Layer O)
--
-- Recursive <details>/<summary> — pure CSS, no JS, browser-native expand/
-- collapse. Leaves link to /detail?path=<full_path> so the existing strip-
-- chart view opens for each KB_LOG.
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
    transition: transform 0.1s;
  }
  .tree details[open] > summary::before { content: "▾"; }
  .tree .node-name  { color: var(--fg); }
  .tree .node-count { color: var(--muted); margin-left: 0.5em; font-size: 0.85em; }
  .tree .leaf {
    display: block; padding: 0.15em 0.4em 0.15em 1.4em;
    border-radius: 3px; color: var(--accent); text-decoration: none;
  }
  .tree .leaf:hover { background: #12161c; text-decoration: underline; }
  .tree .leaf .leaf-meta { color: var(--muted); margin-left: 0.6em; font-size: 0.8em; }
  .tree .kind-badge {
    display: inline-block; padding: 0 5px; margin-left: 0.4em;
    border-radius: 3px; font-size: 0.7em; font-weight: 600; border: 1px solid;
  }
  .tree ul { list-style: none; margin: 0; padding-left: 1.2em; }
  .tree li { margin: 0; }
</style>
]]

M.tree_css = TREE_CSS

local function sorted_child_keys(children)
  local keys = {}
  for k in pairs(children or {}) do keys[#keys + 1] = k end
  table.sort(keys)
  return keys
end

--- Render one tree node (and all descendants).
--- node: { name, children?, is_leaf?, path?, kind?, sample_count?, leaf_count? }
--- leaf_url_fn: function(path) -> string  (called for each leaf's href)
--- open_top: if true, the top-level <details> is rendered open.
function M.render_tree_node(node, leaf_url_fn, open_top)
  local h_ = require("helpers")
  local parts = {}
  local function emit(s) parts[#parts + 1] = s end

  if node.is_leaf then
    local kind = node.kind or "diagnostic"
    local kind_class = "kind-" .. kind
    emit(string.format(
      '<a class="leaf" href="%s"><span>%s</span>' ..
      '<span class="kind-badge %s">%s</span>' ..
      '<span class="leaf-meta">%d sample%s</span></a>',
      h_.escape(leaf_url_fn(node.path)),
      h_.escape(node.name),
      kind_class, h_.escape(kind),
      node.sample_count or 0,
      (node.sample_count or 0) == 1 and "" or "s"))
    return table.concat(parts, "")
  end

  -- Internal node: <details><summary>name (count)</summary><ul>...</ul></details>
  emit(open_top and '<details open>' or '<details>')
  emit(string.format(
    '<summary><span class="node-name">%s</span>' ..
    '<span class="node-count">(%d)</span></summary>',
    h_.escape(node.name or ""),
    node.leaf_count or 0))
  emit('<ul>')
  for _, k in ipairs(sorted_child_keys(node.children)) do
    local child = node.children[k]
    emit('<li>')
    emit(M.render_tree_node(child, leaf_url_fn, false))
    emit('</li>')
  end
  emit('</ul>')
  emit('</details>')
  return table.concat(parts, "")
end

--- Render the whole tree starting from root. Skips root summary; renders
--- each top-level child of root open-by-default.
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
