-- alarm_journal.lua -- view 4: synthesized state-transition log.
--
-- We don't have a dedicated journal stream yet (deferred). Synthesize
-- from the 4 timestamp columns per exception:
--   last_raised_ts  -> "RAISED"
--   last_rtn_ts     -> "RTN_NORMAL"
--   last_ack_ts     -> "ACK"
--   last_shelve_ts  -> "SHELVE"
-- Merge all non-zero timestamps into one list, sort newest-first, show
-- as a single chronological feed. Last 50 events site-wide.

local h = require("helpers")
local r = require("render")

local pg, err = h.pg_connect()
if not pg then
  r.error_page("Alarm Journal", "journal",
    "pg connect: " .. h.escape(err or "(nil)"))
  return
end

-- Pull all 4 timestamps per SYS_EXCEPTION in one query; synthesize
-- events client-side. Much cheaper than a UNION with 4 subselects.
local JOURNAL_SQL = [[
  SELECT k.path::text AS path,
         COALESCE((k.properties->>'priority')::int, 3) AS priority,
         COALESCE(k.properties->>'description', '') AS description,
         COALESCE((s_raised.data->>'value')::bigint, 0) AS last_raised_ts,
         COALESCE((s_rtn.data->>'value')::bigint, 0)    AS last_rtn_ts,
         COALESCE((s_ack.data->>'value')::bigint, 0)    AS last_ack_ts,
         COALESCE(s_ack_by.data->>'value', '')          AS last_ack_by,
         COALESCE((s_shv.data->>'value')::bigint, 0)    AS last_shelve_ts,
         COALESCE(s_shv_by.data->>'value', '')          AS last_shelve_by,
         COALESCE(s_err.data->>'value', '')             AS last_error
    FROM knowledge_base k
    LEFT JOIN knowledge_base_status s_raised
      ON s_raised.path = (k.path::text || '.KB_STATUS_FIELD.last_raised_ts')::ltree
    LEFT JOIN knowledge_base_status s_rtn
      ON s_rtn.path = (k.path::text || '.KB_STATUS_FIELD.last_rtn_ts')::ltree
    LEFT JOIN knowledge_base_status s_ack
      ON s_ack.path = (k.path::text || '.KB_STATUS_FIELD.last_ack_ts')::ltree
    LEFT JOIN knowledge_base_status s_ack_by
      ON s_ack_by.path = (k.path::text || '.KB_STATUS_FIELD.last_ack_by')::ltree
    LEFT JOIN knowledge_base_status s_shv
      ON s_shv.path = (k.path::text || '.KB_STATUS_FIELD.last_shelve_ts')::ltree
    LEFT JOIN knowledge_base_status s_shv_by
      ON s_shv_by.path = (k.path::text || '.KB_STATUS_FIELD.last_shelve_by')::ltree
    LEFT JOIN knowledge_base_status s_err
      ON s_err.path = (k.path::text || '.KB_STATUS_FIELD.last_error')::ltree
   WHERE k.label = 'SYS_EXCEPTION'
]]

local rows, qerr = pg:query(JOURNAL_SQL)
pg:keepalive(60000, 8)

if not rows then
  r.error_page("Alarm Journal", "journal",
    "query: " .. h.escape(tostring(qerr)))
  return
end

-- Flatten into event list
local events = {}
for _, row in ipairs(rows) do
  local function push(kind, ts, actor, extra)
    ts = tonumber(ts) or 0
    if ts > 0 then
      events[#events + 1] = {
        ts = ts, kind = kind,
        path = row.path,
        priority = row.priority,
        description = row.description,
        actor = actor,
        extra = extra,
      }
    end
  end
  push("RAISED", row.last_raised_ts, nil, row.last_error)
  push("RTN_NORMAL", row.last_rtn_ts, nil, nil)
  push("ACK", row.last_ack_ts, row.last_ack_by, nil)
  push("SHELVE", row.last_shelve_ts, row.last_shelve_by, nil)
end

table.sort(events, function(a, b) return a.ts > b.ts end)

-- Cap at 50 most recent
while #events > 50 do table.remove(events) end

---------------------------------------------------------------------------
-- Render
---------------------------------------------------------------------------

local parts = {}
local function emit(s) parts[#parts + 1] = s end

emit(string.format(
  '<div class="panel"><h2>Alarm Journal (last %d events)</h2>', #events))

emit('<p style="color:#888;font-size:0.9em;margin-top:0">')
emit('Synthesized from per-exception <code>last_*_ts</code> fields. ')
emit('A proper append-only journal stream is deferred — this shows the ')
emit('SINGLE most-recent occurrence of each transition type per exception.</p>')

if #events == 0 then
  emit('<p class="empty">No alarm transitions have ever occurred.</p>')
else
  emit('<table><thead><tr>')
  emit('<th style="width:170px">When</th><th>Kind</th><th>Pri</th>')
  emit('<th>Scope / Name</th><th>Actor</th><th>Detail</th>')
  emit('</tr></thead><tbody>')

  local KIND_COLOR = {
    RAISED     = "#f44",
    RTN_NORMAL = "#6b6",
    ACK        = "#f90",
    SHELVE     = "#8af",
  }

  for _, ev in ipairs(events) do
    local pri = tonumber(ev.priority) or 3
    local col = KIND_COLOR[ev.kind] or "#888"
    local detail_html = ""
    if ev.kind == "RAISED" and ev.extra and ev.extra ~= "" then
      detail_html = string.format(
        '<pre style="margin:0;white-space:pre-wrap;max-width:500px">%s</pre>',
        h.escape(ev.extra:sub(1, 150)))
    elseif ev.description ~= "" then
      detail_html = h.escape(ev.description)
    end
    emit(string.format(
      '<tr><td title="%s">%s</td>' ..
      '<td><span class="state-badge" style="border-color:%s;color:%s">%s</span></td>' ..
      '<td><span class="state-badge %s">P%d</span></td>' ..
      '<td><a href="/detail?path=%s"><code>%s</code></a></td>' ..
      '<td>%s</td><td>%s</td></tr>',
      h.escape(h.fmt_ts(ev.ts)), h.escape(h.fmt_age(ev.ts)),
      col, col, h.escape(ev.kind),
      h.pri_class(pri), pri,
      h.urlencode(ev.path), h.escape(h.short_path(ev.path)),
      h.escape(ev.actor or "—"),
      detail_html))
  end
  emit('</tbody></table>')
end
emit('</div>')

r.page("Alarm Journal", "journal", table.concat(parts, "\n"))
