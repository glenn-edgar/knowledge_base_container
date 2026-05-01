-- rules.lua -- view 9: Rule Inventory.
--
-- Flat table of every KB_RULE across the site, sorted by fire_count desc.
-- Per row: inline enable/disable + shelve buttons (POST /action).
-- Click target_exception -> exception_web detail view.

local h = require("helpers")
local r = require("render")

local pg, err = h.pg_connect()
if not pg then r.error_page("Rule Inventory", "rules", h.escape(err or "(nil)")); return end

local rows = h.list_all_rules(pg)
pg:keepalive(60000, 8)

local parts = {}
local function emit(s) parts[#parts + 1] = s end

-- Count enabled / suppressed / fires
local n_enabled, n_suppressed, n_fired_ever = 0, 0, 0
for _, row in ipairs(rows) do
  if row.enabled == "true"     then n_enabled = n_enabled + 1 end
  if row.suppressed == "true"  then n_suppressed = n_suppressed + 1 end
  if (tonumber(row.fire_count) or 0) > 0 then n_fired_ever = n_fired_ever + 1 end
end

emit(string.format(
  '<div class="panel"><h2>Rule inventory (%d total)</h2>', #rows))
emit(string.format(
  '<p style="color:#888;font-size:0.9em;margin:0">' ..
  'enabled: <strong>%d</strong> · suppressed: <strong>%d</strong> · ' ..
  'has fired: <strong>%d</strong></p></div>',
  n_enabled, n_suppressed, n_fired_ever))

-- Filter bar (client-side via query param; simple)
emit('<div class="panel"><form method="GET" style="margin:0;display:flex;gap:0.5em;align-items:center">')
emit('<label>Kind: <select name="kind">')
for _, k in ipairs({"", "threshold", "slope_trend", "sample_gap", "z_score",
                    "rate_of_change", "envelope_drift", "cusum"}) do
  emit(string.format('<option value="%s"%s>%s</option>',
    k, k == (ngx.req.get_uri_args().kind or "") and ' selected' or "",
    k == "" and "(all)" or k))
end
emit('</select></label>')
emit('<label>Fired at least: <input type="number" name="min_fires" value="' ..
     h.escape(ngx.req.get_uri_args().min_fires or "0") .. '" style="width:6em"></label>')
emit('<button type="submit" style="padding:0.25em 0.8em">Filter</button>')
emit('</form></div>')

local filter_kind = ngx.req.get_uri_args().kind or ""
local min_fires = tonumber(ngx.req.get_uri_args().min_fires) or 0

emit('<div class="panel">')

local shown = 0
emit('<table><thead><tr>')
emit('<th>Rule</th><th>Log</th><th>Kind</th><th>Target exception</th>')
emit('<th>Fires</th><th>Last fired</th>')
emit('<th>Enabled</th><th>Suppressed</th><th style="text-align:right">Actions</th>')
emit('</tr></thead><tbody>')

for _, row in ipairs(rows) do
  local props = row.props or {}
  local kind  = props.kind or ""
  if (filter_kind == "" or kind == filter_kind)
     and (tonumber(row.fire_count) or 0) >= min_fires then
    shown = shown + 1
    local rule_name = row.path:match("KB_RULE%.(.+)$") or row.path
    -- Parent log path: strip trailing .KB_RULE.<name>
    local log_path = row.path:gsub("%.KB_RULE%.[^%.]+$", "")
    local log_name = log_path:match("KB_LOG%.(.+)$") or log_path

    local is_suppressed = (row.suppressed == "true" or row.suppressed == true)
    local is_enabled    = (row.enabled == "true"    or row.enabled == true)

    emit(string.format(
      '<tr>' ..
      '<td><code>%s</code></td>' ..
      '<td><a href="' .. h.mk_url("/detail") .. '?path=%s">%s</a></td>' ..
      '<td>%s</td>' ..
      '<td><code>%s</code></td>' ..
      '<td>%d</td>' ..
      '<td title="%s">%s</td>' ..
      '<td>%s</td><td>%s</td>' ..
      '<td style="text-align:right">%s</td>' ..
      '</tr>',
      h.escape(rule_name),
      h.urlencode(log_path), h.escape(log_name),
      h.escape(kind),
      h.escape(props.target_exception or ""),
      tonumber(row.fire_count) or 0,
      h.escape(h.fmt_ts(row.last_fired_ts)),
      h.escape(h.fmt_age(row.last_fired_ts)),
      is_enabled and "✓" or "✗",
      is_suppressed and "✓" or "—",
      -- action forms: toggle enabled; shelve / unshelve
      string.format(
        '<form method="POST" action="' .. h.mk_url("/action") .. '" style="display:inline">' ..
        '<input type="hidden" name="op" value="%s">' ..
        '<input type="hidden" name="path" value="%s">' ..
        '<button type="submit" style="padding:0.2em 0.6em;background:#345;color:#fff;border:none;border-radius:3px;cursor:pointer;font-size:0.85em">%s</button>' ..
        '</form> ' ..
        (is_suppressed
          and ('<form method="POST" action="' .. h.mk_url("/action") .. '" style="display:inline">' ..
               '<input type="hidden" name="op" value="unshelve">' ..
               '<input type="hidden" name="path" value="' .. h.escape(row.path) .. '">' ..
               '<button type="submit" style="padding:0.2em 0.6em;background:#6b6;color:#fff;border:none;border-radius:3px;cursor:pointer;font-size:0.85em">Unsup</button>' ..
               '</form>')
          or ('<form method="POST" action="' .. h.mk_url("/action") .. '" style="display:inline">' ..
              '<input type="hidden" name="op" value="shelve">' ..
              '<input type="hidden" name="path" value="' .. h.escape(row.path) .. '">' ..
              '<input type="hidden" name="duration_s" value="300">' ..
              '<button type="submit" style="padding:0.2em 0.6em;background:#468;color:#fff;border:none;border-radius:3px;cursor:pointer;font-size:0.85em">Shelve 5m</button>' ..
              '</form>')),
        is_enabled and "disable" or "enable",
        h.escape(row.path),
        is_enabled and "Disable" or "Enable"
      )
    ))
  end
end

emit('</tbody></table>')
if shown == 0 then
  emit('<p class="empty">No rules match the filter.</p>')
else
  emit(string.format(
    '<p style="color:#888;font-size:0.85em;margin-top:0.8em">' ..
    '%d of %d rules shown. Enable/Disable toggles in-place; Shelve 5m / Unsuppress round-trip via POST /action.</p>',
    shown, #rows))
end
emit('</div>')

r.page("Rule Inventory", "rules", table.concat(parts, "\n"))
