-- tree.lua -- view: namespace tree of KB_LOG paths.
--
-- Builds a nested tree from the KB ltree namespace and renders it as
-- collapsible <details>/<summary> nodes. Each leaf is a KB_LOG; clicking
-- a leaf opens the existing strip-chart at /detail?path=...
--
-- Phase B Layer O: discoverability for app-container logs landing under
-- system.<sys>.site.<s>.cpu.<id>.container.<c>.KB_LOG.<sample>.

local h = require("helpers")
local r = require("render")

local pg, err = h.pg_connect()
if not pg then r.error_page("Tree", "tree", h.escape(err or "(nil)")); return end

local root = h.build_kb_log_tree(pg)
pg:keepalive(60000, 8)

local function leaf_url(path)
  return h.mk_url("/detail?path=" .. h.urlencode(path))
end

local body = {}
local function emit(s) body[#body + 1] = s end

emit('<div class="panel">')
emit(string.format('<h2>KB_LOG namespace tree (%d logs)</h2>', root.leaf_count))
emit('<p style="color:#888;font-size:0.85em;margin-top:0">')
emit('Each leaf is a KB_LOG; click to open its strip chart. Counts are total raw samples in the KB stream table.</p>')
emit(r.render_tree(root, leaf_url))
emit('</div>')

r.page("Tree", "tree", table.concat(body, ""))
