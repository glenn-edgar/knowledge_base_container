-- archival.lua -- view 8: Archival Browser.
--
-- Like view 6 but filtered to kind='archival' -- these are long-range
-- historical signals (daily values like ETO, rainfall, cumulative counters)
-- rather than live process metrics. Rendered as a list with summary;
-- click through to the detail chart for long-range (30d/90d/1y) zoom.

local h = require("helpers")
local r = require("render")

local pg, err = h.pg_connect()
if not pg then r.error_page("Archival", "archival", h.escape(err or "(nil)")); return end

local rows = h.list_logs_with_summary(pg, "archival")
pg:keepalive(60000, 8)

local parts = {}
local function emit(s) parts[#parts + 1] = s end

emit(string.format('<div class="panel"><h2>Archival logs (%d)</h2>', #rows))
emit('<p style="color:#888;font-size:0.85em;margin-top:0">')
emit('Long-range historical signals; time-resolution is per-day. Examples: ETO, ')
emit('daily rainfall, cumulative counters.</p>')

if #rows == 0 then
  emit('<p class="empty">No <code>kind="archival"</code> KB_LOGs declared in this KB.<br>')
  emit('<small>Archival logs are declared via <code>kb:add_log(name, {kind="archival", ...})</code>. ')
  emit('They skip tier-1 / tier-2 rollups and keep tier-3 (1day) only.</small></p>')
else
  emit('<table><thead><tr>')
  emit('<th>Log</th><th>Scope</th><th>Unit</th><th>Last value</th><th>Last sample</th><th>Samples total</th>')
  emit('</tr></thead><tbody>')
  for _, row in ipairs(rows) do
    local props = row.props or {}
    local name  = row.path:match("KB_LOG%.(.+)$") or row.path
    local last_ts = tonumber(row.last_sample_ts) or 0
    emit(string.format(
      '<tr><td><a href="/detail?path=%s&window_s=2592000">%s</a></td>' ..
      '<td>%s</td><td><code>%s</code></td><td>%s</td>' ..
      '<td title="%s">%s</td><td>%s</td></tr>',
      h.urlencode(row.path), h.escape(name),
      h.escape(h.short_log_path(row.path):gsub(" / " .. name .. "$", "") or ""),
      h.escape(props.unit or ""),
      h.escape(row.last_value ~= "" and tostring(row.last_value) or "—"),
      h.escape(h.fmt_ts(last_ts)),
      h.escape(h.fmt_age(last_ts)),
      tostring(row.sample_count_total or 0)))
  end
  emit('</tbody></table>')
end
emit('</div>')

r.page("Archival Browser", "archival", table.concat(parts, "\n"))
