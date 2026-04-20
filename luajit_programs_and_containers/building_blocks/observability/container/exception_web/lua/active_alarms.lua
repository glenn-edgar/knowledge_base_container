-- active_alarms.lua -- view 2: flat table of non-NORMAL SYS_EXCEPTIONs.
--
-- Columns: Pri, State, Path, Type, Description, Hits, Last raised, Last error.
-- Sorted: priority asc (P1 first), then last_raised_ts desc.
-- Click row -> /detail/<path> (Phase 8b; link placeholder for now).

local h = require("helpers")
local r = require("render")

local pg, err = h.pg_connect()
if not pg then
  r.error_page("Active Alarms", "active",
    "pg connect failed: " .. h.escape(err or "(nil)"))
  return
end

---------------------------------------------------------------------------
-- Query: SYS_EXCEPTIONs with state != 'NORMAL'
---------------------------------------------------------------------------

local ACTIVE_SQL = [[
  SELECT k.path::text AS path,
         COALESCE((k.properties->>'priority')::int, 3) AS priority,
         COALESCE(k.properties->>'type', '') AS type,
         COALESCE(k.properties->>'description', '') AS description,
         COALESCE(s_state.data->>'value', 'NORMAL') AS state,
         COALESCE((s_hits.data->>'value')::bigint, 0) AS hit_count,
         COALESCE((s_raised.data->>'value')::bigint, 0) AS last_raised_ts,
         COALESCE(s_err.data->>'value', '') AS last_error,
         COALESCE((s_shelve.data->>'value')::bigint, 0) AS shelve_until,
         COALESCE((s_flap.data->>'value')::float, 0) AS flap_rate_5min
    FROM knowledge_base k
    JOIN knowledge_base_status s_state
      ON s_state.path = (k.path::text || '.KB_STATUS_FIELD.state')::ltree
    LEFT JOIN knowledge_base_status s_hits
      ON s_hits.path = (k.path::text || '.KB_STATUS_FIELD.hit_count')::ltree
    LEFT JOIN knowledge_base_status s_raised
      ON s_raised.path = (k.path::text || '.KB_STATUS_FIELD.last_raised_ts')::ltree
    LEFT JOIN knowledge_base_status s_err
      ON s_err.path = (k.path::text || '.KB_STATUS_FIELD.last_error')::ltree
    LEFT JOIN knowledge_base_status s_shelve
      ON s_shelve.path = (k.path::text || '.KB_STATUS_FIELD.shelve_until')::ltree
    LEFT JOIN knowledge_base_status s_flap
      ON s_flap.path = (k.path::text || '.KB_STATUS_FIELD.flap_rate_5min')::ltree
   WHERE k.label = 'SYS_EXCEPTION'
     AND COALESCE(s_state.data->>'value', 'NORMAL') <> 'NORMAL'
   ORDER BY priority ASC, last_raised_ts DESC
]]

local rows, qerr = pg:query(ACTIVE_SQL)
pg:keepalive(60000, 8)

if not rows then
  r.error_page("Active Alarms", "active",
    "query failed: " .. h.escape(tostring(qerr)))
  return
end

---------------------------------------------------------------------------
-- Render
---------------------------------------------------------------------------

local parts = {}
local function emit(s) parts[#parts + 1] = s end

emit(string.format(
  '<div class="panel"><h2>Active alarms (%d)</h2>', #rows))

if #rows == 0 then
  emit('<p class="empty">No alarms currently in non-NORMAL state.<br>')
  emit('<small>All SYS_EXCEPTIONs are quiescent. ')
  emit('This is the expected resting state when no problems exist, ')
  emit('or when sample writers are not yet wired (current state).</small></p>')
else
  emit('<table><thead><tr>')
  emit('<th>Pri</th><th>State</th><th>Scope / Name</th><th>Type</th>')
  emit('<th>Description</th><th>Hits</th><th>Flap/s</th>')
  emit('<th>Last raised</th><th>Last error</th>')
  emit('</tr></thead><tbody>')

  for _, row in ipairs(rows) do
    local pri   = tonumber(row.priority) or 3
    local state = row.state or "NORMAL"
    local flap  = tonumber(row.flap_rate_5min) or 0
    local flap_disp = (flap > 0.001) and string.format("%.3f", flap) or "—"

    emit(string.format(
      '<tr>' ..
      '<td><span class="state-badge %s">P%d</span></td>' ..
      '<td><span class="state-badge %s">%s</span></td>' ..
      '<td><a href="/detail?path=%s"><code>%s</code></a></td>' ..
      '<td>%s</td><td>%s</td>' ..
      '<td>%s</td><td>%s</td>' ..
      '<td title="%s">%s</td><td>%s</td>' ..
      '</tr>',
      h.pri_class(pri), pri,
      h.state_class(state), h.escape(state),
      h.urlencode(row.path), h.escape(h.short_path(row.path)),
      h.escape(row.type or ""),
      h.escape(row.description or ""),
      h.escape(tostring(row.hit_count or 0)),
      flap_disp,
      h.escape(h.fmt_ts(row.last_raised_ts)),
      h.escape(h.fmt_age(row.last_raised_ts)),
      h.escape((row.last_error or ""):sub(1, 100))))
  end

  emit('</tbody></table>')
end
emit('</div>')

-- Hint panel
emit('<div class="panel"><h3>Filtering / ack / shelve</h3>')
emit('<p>Phase 8a: read-only flat table. ')
emit('Phase 8b adds: click row &rarr; drill to Alarm Detail (signatures, ' ..
     'history, ack+shelve controls); filter by scope/priority; inline ' ..
     'sort controls.</p></div>')

r.page("Active Alarms", "active", table.concat(parts, "\n"))
