-- site_overview.lua -- view 1 (homepage).
--
-- Top: priority counts (4 boxes) of non-NORMAL alarms across the site.
-- Middle: recent 10 alarm events by last_raised_ts desc.
-- Bottom: deferred panels (ready_bits grid, cluster sync state) noted
--         as Phase 8b follow-up.
--
-- Pattern: open pg, run queries, close pg, render.

local h = require("helpers")
local r = require("render")

local pg, err = h.pg_connect()
if not pg then
  r.error_page("Site Overview", "overview",
    "pg connect failed: " .. h.escape(err or "(nil)"))
  return
end

---------------------------------------------------------------------------
-- Priority counts of non-NORMAL alarms (state != 'NORMAL')
---------------------------------------------------------------------------

local COUNT_SQL = [[
  SELECT COALESCE((k.properties->>'priority')::int, 3) AS priority,
         COUNT(*)::int AS n
    FROM knowledge_base k
    JOIN knowledge_base_status s
      ON s.path = (k.path::text || '.KB_STATUS_FIELD.state')::ltree
   WHERE k.label = 'SYS_EXCEPTION'
     AND (s.data->>'value') IS NOT NULL
     AND (s.data->>'value') <> 'NORMAL'
   GROUP BY priority
]]

local counts = { [1] = 0, [2] = 0, [3] = 0, [4] = 0 }
local rows = pg:query(COUNT_SQL) or {}
for _, row in ipairs(rows) do
  counts[tonumber(row.priority) or 3] = tonumber(row.n) or 0
end
local total_nonnormal = counts[1] + counts[2] + counts[3] + counts[4]

---------------------------------------------------------------------------
-- Recent events (10 most-recently-raised alarms, any state)
---------------------------------------------------------------------------

local RECENT_SQL = [[
  SELECT k.path::text AS path,
         COALESCE((k.properties->>'priority')::int, 3) AS priority,
         COALESCE(k.properties->>'description', '') AS description,
         s_state.data->>'value' AS state,
         COALESCE((s_raised.data->>'value')::bigint, 0) AS last_raised_ts,
         COALESCE(s_err.data->>'value', '') AS last_error
    FROM knowledge_base k
    LEFT JOIN knowledge_base_status s_state
      ON s_state.path = (k.path::text || '.KB_STATUS_FIELD.state')::ltree
    LEFT JOIN knowledge_base_status s_raised
      ON s_raised.path = (k.path::text || '.KB_STATUS_FIELD.last_raised_ts')::ltree
    LEFT JOIN knowledge_base_status s_err
      ON s_err.path = (k.path::text || '.KB_STATUS_FIELD.last_error')::ltree
   WHERE k.label = 'SYS_EXCEPTION'
     AND COALESCE((s_raised.data->>'value')::bigint, 0) > 0
   ORDER BY last_raised_ts DESC
   LIMIT 10
]]

local recent = pg:query(RECENT_SQL) or {}

---------------------------------------------------------------------------
-- Site totals (for bottom strip)
---------------------------------------------------------------------------

local TOTAL_SQL =
  "SELECT COUNT(*)::int AS n FROM knowledge_base WHERE label = 'SYS_EXCEPTION'"
local total_declared = 0
local t = pg:query(TOTAL_SQL)
if t and t[1] then total_declared = tonumber(t[1].n) or 0 end

pg:keepalive(60000, 8)

---------------------------------------------------------------------------
-- Render
---------------------------------------------------------------------------

local parts = {}
local function emit(s) parts[#parts + 1] = s end

-- Priority counts panel
emit('<div class="panel"><h2>Active alarms by priority</h2>')
emit('<div class="grid-4">')
for pri = 1, 4 do
  emit(string.format(
    '<div class="pri-box %s"><span class="n">%d</span><span class="label">Pri %d · %s</span></div>',
    h.pri_class(pri), counts[pri], pri, h.pri_name(pri)))
end
emit('</div>')
if total_nonnormal == 0 then
  emit('<p class="empty">No active alarms. All SYS_EXCEPTIONs in NORMAL state.</p>')
end
emit('</div>')

-- Recent events panel
emit('<div class="panel"><h2>Recent alarm events</h2>')
if #recent == 0 then
  emit('<p class="empty">No alarm has ever been raised in this KB.</p>')
else
  emit('<table><thead><tr>')
  emit('<th>Pri</th><th>State</th><th>Path</th><th>Description</th>')
  emit('<th>Last raised</th><th>Last error</th>')
  emit('</tr></thead><tbody>')
  for _, row in ipairs(recent) do
    local pri = tonumber(row.priority) or 3
    local state = row.state or "NORMAL"
    emit(string.format(
      '<tr><td><span class="state-badge %s">P%d</span></td>' ..
      '<td><span class="state-badge %s">%s</span></td>' ..
      '<td><a href="/detail?path=%s">%s</a></td>' ..
      '<td>%s</td><td title="%s">%s</td><td>%s</td></tr>',
      h.pri_class(pri), pri,
      h.state_class(state), h.escape(state),
      h.urlencode(row.path), h.escape(h.short_path(row.path)),
      h.escape(row.description or ""),
      h.escape(h.fmt_ts(row.last_raised_ts)),
      h.escape(h.fmt_age(row.last_raised_ts)),
      h.escape((row.last_error or ""):sub(1, 80))))
  end
  emit('</tbody></table>')
end
emit('</div>')

-- Summary strip
emit('<div class="panel"><h3>Site summary</h3>')
emit(string.format(
  '<p>Declared exceptions: <strong>%d</strong> &nbsp;·&nbsp; ' ..
  'Non-NORMAL: <strong>%d</strong> &nbsp;·&nbsp; ' ..
  'Ready bits / cluster state panels: <em>Phase 8b</em></p>',
  total_declared, total_nonnormal))
emit('</div>')

r.page("Site Overview", "overview", table.concat(parts, "\n"))
