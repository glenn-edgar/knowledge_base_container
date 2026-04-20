-- alarm_detail.lua -- view 3: one SYS_EXCEPTION's full state.
--
-- URL: /detail?path=<ltree>
-- Shows:
--   * Header props (priority, type, instance, description, response_procedure)
--   * All state children (state, timestamps, counters, flap_rate, last_error)
--   * Signatures jsonb (signature-deduped unique errors with first/last/count)
--   * Action buttons: Ack, Clear, Shelve 5m / 1h / 1d, Unshelve

local h = require("helpers")
local r = require("render")

local args = ngx.req.get_uri_args() or {}
local exc_path = args.path

if not exc_path or exc_path == "" then
  r.error_page("Alarm Detail", "active",
    "missing ?path=<ltree> query argument")
  return
end

local pg, err = h.pg_connect()
if not pg then
  r.error_page("Alarm Detail", "active",
    "pg connect: " .. h.escape(err or "(nil)"))
  return
end

local props = h.read_props(pg, exc_path)
if not props then
  pg:keepalive(60000, 8)
  r.error_page("Alarm Detail", "active",
    "no SYS_EXCEPTION at path: " .. h.escape(exc_path))
  return
end

-- Read all state fields in parallel-looking code (each is one SELECT; at
-- ~1-2ms per query on localhost pg, total <30ms)
local state               = h.read_status(pg, exc_path, "state") or "NORMAL"
local last_raised_ts      = tonumber(h.read_status(pg, exc_path, "last_raised_ts"))      or 0
local last_rtn_ts         = tonumber(h.read_status(pg, exc_path, "last_rtn_ts"))         or 0
local last_ack_ts         = tonumber(h.read_status(pg, exc_path, "last_ack_ts"))         or 0
local last_ack_by         = h.read_status(pg, exc_path, "last_ack_by") or ""
local last_shelve_ts      = tonumber(h.read_status(pg, exc_path, "last_shelve_ts"))      or 0
local last_shelve_by      = h.read_status(pg, exc_path, "last_shelve_by") or ""
local shelve_until        = tonumber(h.read_status(pg, exc_path, "shelve_until"))        or 0
local last_error          = h.read_status(pg, exc_path, "last_error") or ""
local last_trigger_value  = h.read_status(pg, exc_path, "last_trigger_value") or ""
local last_limit_value    = h.read_status(pg, exc_path, "last_limit_value") or ""
local last_source_path    = h.read_status(pg, exc_path, "last_source_path") or ""
local last_comment        = h.read_status(pg, exc_path, "last_comment") or ""
local hit_count           = tonumber(h.read_status(pg, exc_path, "hit_count"))           or 0
local flap_rate           = tonumber(h.read_status(pg, exc_path, "flap_rate_5min"))      or 0
local signatures          = h.read_signatures(pg, exc_path)

pg:keepalive(60000, 8)

---------------------------------------------------------------------------
-- Render
---------------------------------------------------------------------------

local priority = tonumber(props.priority) or 3
local parts = {}
local function emit(s) parts[#parts + 1] = s end

-- Header
emit(string.format(
  '<div class="panel"><h2>%s <span class="state-badge %s">%s</span></h2>',
  h.escape(exc_path:match("SYS_EXCEPTION%.(.+)$") or exc_path),
  h.state_class(state), h.escape(state)))

emit('<table style="max-width:900px">')
emit(string.format('<tr><th style="width:180px">Path</th><td><code>%s</code></td></tr>',
  h.escape(exc_path)))
emit(string.format('<tr><th>Priority</th><td><span class="state-badge %s">P%d &middot; %s</span></td></tr>',
  h.pri_class(priority), priority, h.pri_name(priority)))
emit(string.format('<tr><th>Type</th><td>%s</td></tr>', h.escape(props.type or "")))
emit(string.format('<tr><th>Instance</th><td>%s</td></tr>', h.escape(props.instance or "")))
emit(string.format('<tr><th>Description</th><td>%s</td></tr>', h.escape(props.description or "")))
if props.response_procedure and props.response_procedure ~= "" then
  emit(string.format('<tr><th>Runbook</th><td><code>%s</code></td></tr>',
    h.escape(props.response_procedure)))
end
emit('</table></div>')

-- Timestamps + counters
emit('<div class="panel"><h3>Runtime state</h3>')
emit('<table style="max-width:900px">')
local function row(th, td, tooltip)
  emit(string.format(
    '<tr><th style="width:220px">%s</th><td%s>%s</td></tr>',
    th, tooltip and (' title="' .. h.escape(tooltip) .. '"') or '', td))
end
row("State",               '<span class="state-badge ' .. h.state_class(state) .. '">' .. h.escape(state) .. '</span>')
row("Hit count",           tostring(hit_count))
row("Flap rate (5-min)",   (flap_rate > 0.001) and string.format("%.4f /s", flap_rate) or "—")
row("Last raised",         h.fmt_age(last_raised_ts), h.fmt_ts(last_raised_ts))
row("Last returned normal",h.fmt_age(last_rtn_ts),    h.fmt_ts(last_rtn_ts))
row("Last acknowledged",   h.fmt_age(last_ack_ts) .. (last_ack_by ~= "" and (" &middot; by " .. h.escape(last_ack_by)) or ""),
                           h.fmt_ts(last_ack_ts))
row("Last shelved",        h.fmt_age(last_shelve_ts) .. (last_shelve_by ~= "" and (" &middot; by " .. h.escape(last_shelve_by)) or ""),
                           h.fmt_ts(last_shelve_ts))
if shelve_until > 0 then
  local age = shelve_until - os.time()
  local lbl = (age > 0) and (string.format("in %ds", age)) or "expired"
  row("Shelve expires",    lbl, h.fmt_ts(shelve_until))
end
if last_comment ~= "" then row("Last comment", h.escape(last_comment)) end
emit('</table></div>')

-- Last error payload
if last_error ~= "" or last_trigger_value ~= "" or last_source_path ~= "" then
  emit('<div class="panel"><h3>Last raise payload</h3>')
  emit('<table style="max-width:900px">')
  if last_error ~= "" then
    emit('<tr><th style="width:220px">last_error</th><td><pre style="margin:0;white-space:pre-wrap">' ..
         h.escape(last_error) .. '</pre></td></tr>')
  end
  if last_trigger_value ~= "" then
    emit('<tr><th>last_trigger_value</th><td><code>' .. h.escape(last_trigger_value) .. '</code></td></tr>')
  end
  if last_limit_value ~= "" then
    emit('<tr><th>last_limit_value</th><td><code>' .. h.escape(last_limit_value) .. '</code></td></tr>')
  end
  if last_source_path ~= "" then
    emit('<tr><th>last_source_path</th><td><code>' .. h.escape(last_source_path) .. '</code></td></tr>')
  end
  emit('</table></div>')
end

-- Signatures (dedup-by-content summary)
emit(string.format('<div class="panel"><h3>Signature summary (%d unique)</h3>', #signatures))
if #signatures == 0 then
  emit('<p class="empty">No signatures recorded. This exception has never been raised through the SCADA API.</p>')
else
  emit('<table><thead><tr>')
  emit('<th>First seen</th><th>Last seen</th><th>Count</th><th>Source</th><th>Error</th>')
  emit('</tr></thead><tbody>')
  for _, sig in ipairs(signatures) do
    emit(string.format(
      '<tr><td title="%s">%s</td><td title="%s">%s</td><td>%d</td><td><code>%s</code></td>' ..
      '<td><pre style="margin:0;white-space:pre-wrap">%s</pre></td></tr>',
      h.escape(h.fmt_ts(sig.first_occurrence_ts)),
      h.escape(h.fmt_age(sig.first_occurrence_ts)),
      h.escape(h.fmt_ts(sig.last_occurrence_ts)),
      h.escape(h.fmt_age(sig.last_occurrence_ts)),
      tonumber(sig.occurrence_count) or 0,
      h.escape(sig.source_path or ""),
      h.escape((sig.error or ""):sub(1, 300))))
  end
  emit('</tbody></table>')
end
emit('</div>')

-- Action buttons. Each form POSTs /action with op+path+optional inputs.
-- Handler writes back via helpers.<op> then 302s to the referer (usually
-- this detail page, so refresh reflects the new state).
emit('<div class="panel"><h3>Actions</h3>')
local function form(op, label, extras)
  emit(string.format(
    '<form method="POST" action="/action" style="display:inline-block;margin-right:0.5em">' ..
    '<input type="hidden" name="op" value="%s">' ..
    '<input type="hidden" name="path" value="%s">%s' ..
    '<button type="submit" style="padding:0.4em 1em;background:%s;color:#fff;border:none;border-radius:3px;cursor:pointer">%s</button>' ..
    '</form>',
    op, h.escape(exc_path),
    extras or "",
    (op == "clear" and "#6b6") or (op == "unshelve" and "#6b6") or "#46a",
    label))
end
form("ack",      "Ack")
form("shelve",   "Shelve 5 min",  '<input type="hidden" name="duration_s" value="300">')
form("shelve",   "Shelve 1 h",    '<input type="hidden" name="duration_s" value="3600">')
form("shelve",   "Shelve 1 d",    '<input type="hidden" name="duration_s" value="86400">')
form("clear",    "Clear")
if state == "SHELVED" then
  form("unshelve", "Unshelve")
end
emit('<p style="color:#888;font-size:0.9em;margin-top:1em">')
emit('Operator defaults to <code>ops</code>. All actions write back to ')
emit('<code>knowledge_base_status</code> + state children; refresh shows new state.</p>')
emit('</div>')

-- Back link
emit('<p><a href="/alarms">&larr; Back to Active Alarms</a></p>')

r.page("Alarm Detail", "active", table.concat(parts, "\n"))
