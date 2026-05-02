-- tree.lua -- view: namespace tree of SYS_EXCEPTION paths.
--
-- Builds a nested tree from the KB ltree namespace and renders it as
-- collapsible <details>/<summary> nodes. Each leaf is a SYS_EXCEPTION;
-- clicking a leaf opens the existing alarm detail at /detail?path=...
--
-- Internal nodes show "(N active)" when any descendant exception is
-- currently unacked-active so operators can scan for hotspots without
-- drilling in.
--
-- Phase B Layer O.

local h = require("helpers")
local r = require("render")

local pg, err = h.pg_connect()
if not pg then r.error_page("Tree", "tree", h.escape(err or "(nil)")); return end

local root = h.build_sys_exception_tree(pg)
pg:keepalive(60000, 8)

local function leaf_url(path)
  return h.mk_url("/detail?path=" .. h.urlencode(path))
end

local body = {}
local function emit(s) body[#body + 1] = s end

emit('<div class="panel">')
emit(string.format('<h2>SYS_EXCEPTION namespace tree (%d exceptions)</h2>',
  root.leaf_count))
emit('<p style="color:#888;font-size:0.85em;margin-top:0">')
emit('Each leaf is a SYS_EXCEPTION; click to open its detail. ')
emit('Internal nodes badge "N active" when descendants have unacked alarms.</p>')
emit(r.render_tree(root, leaf_url))
emit('</div>')

r.page("Tree", "tree", table.concat(body, ""))
