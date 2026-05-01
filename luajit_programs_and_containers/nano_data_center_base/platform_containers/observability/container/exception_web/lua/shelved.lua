-- shelved.lua -- view 5: exceptions currently in SHELVED state.
--
-- Shows table with unshelve button per row + shelve-expiry countdown.

local h = require("helpers")
local r = require("render")

local pg, err = h.pg_connect()
if not pg then
  r.error_page("Shelved Alarms", "shelved",
    "pg connect: " .. h.escape(err or "(nil)"))
  return
end

local SHELVED_SQL = [[
  SELECT k.path::text AS path,
         COALESCE((k.properties->>'priority')::int, 3) AS priority,
         COALESCE(k.properties->>'description', '') AS description,
         COALESCE((s_shelve_ts.data->>'value')::bigint, 0) AS last_shelve_ts,
         COALESCE(s_shelve_by.data->>'value', '') AS last_shelve_by,
         COALESCE((s_until.data->>'value')::bigint, 0) AS shelve_until,
         COALESCE(s_comment.data->>'value', '') AS last_comment
    FROM knowledge_base k
    JOIN knowledge_base_status s_state
      ON s_state.path = (k.path::text || '.KB_STATUS_FIELD.state')::ltree
    LEFT JOIN knowledge_base_status s_shelve_ts
      ON s_shelve_ts.path = (k.path::text || '.KB_STATUS_FIELD.last_shelve_ts')::ltree
    LEFT JOIN knowledge_base_status s_shelve_by
      ON s_shelve_by.path = (k.path::text || '.KB_STATUS_FIELD.last_shelve_by')::ltree
    LEFT JOIN knowledge_base_status s_until
      ON s_until.path = (k.path::text || '.KB_STATUS_FIELD.shelve_until')::ltree
    LEFT JOIN knowledge_base_status s_comment
      ON s_comment.path = (k.path::text || '.KB_STATUS_FIELD.last_comment')::ltree
   WHERE k.label = 'SYS_EXCEPTION'
     AND s_state.data->>'value' = 'SHELVED'
   ORDER BY priority ASC, last_shelve_ts DESC
]]

local rows, qerr = pg:query(SHELVED_SQL)
pg:keepalive(60000, 8)

if not rows then
  r.error_page("Shelved Alarms", "shelved",
    "query: " .. h.escape(tostring(qerr)))
  return
end

local now = os.time()
local parts = {}
local function emit(s) parts[#parts + 1] = s end

emit(string.format('<div class="panel"><h2>Shelved alarms (%d)</h2>', #rows))

if #rows == 0 then
  emit('<p class="empty">No alarms currently shelved.</p>')
else
  emit('<table><thead><tr>')
  emit('<th>Pri</th><th>Scope / Name</th><th>Description</th>')
  emit('<th>Shelved at</th><th>By</th><th>Expires</th><th>Reason</th>')
  emit('<th style="text-align:right">Action</th>')
  emit('</tr></thead><tbody>')
  for _, row in ipairs(rows) do
    local pri = tonumber(row.priority) or 3
    local until_ = tonumber(row.shelve_until) or 0
    local expiry_txt
    if until_ == 0 then
      expiry_txt = '<em>no lease (manual)</em>'
    else
      local age = until_ - now
      if age > 0 then
        expiry_txt = h.fmt_age(until_):gsub("ago", "from now")
                     .. " <small>(" .. h.fmt_ts(until_) .. ")</small>"
      else
        expiry_txt = '<span style="color:#fc4">expired</span>'
      end
    end
    emit(string.format(
      '<tr><td><span class="state-badge %s">P%d</span></td>' ..
      '<td><code>%s</code></td><td>%s</td>' ..
      '<td title="%s">%s</td><td>%s</td><td>%s</td><td>%s</td>' ..
      '<td style="text-align:right">' ..
      '<form method="POST" action="' .. h.mk_url("/action") .. '" style="display:inline">' ..
      '<input type="hidden" name="op" value="unshelve">' ..
      '<input type="hidden" name="path" value="%s">' ..
      '<button type="submit" style="padding:0.25em 0.8em;background:#6b6;color:#fff;border:none;border-radius:3px;cursor:pointer">Unshelve</button>' ..
      '</form></td></tr>',
      h.pri_class(pri), pri,
      h.escape(h.short_path(row.path)),
      h.escape(row.description or ""),
      h.escape(h.fmt_ts(row.last_shelve_ts)),
      h.escape(h.fmt_age(row.last_shelve_ts)),
      h.escape(row.last_shelve_by or ""),
      expiry_txt,
      h.escape((row.last_comment or ""):sub(1, 80)),
      h.escape(row.path)))
  end
  emit('</tbody></table>')
end
emit('</div>')

emit('<div class="panel"><h3>About shelving</h3>')
emit('<p>Shelving silences an alarm without marking its condition resolved. ')
emit('Used during planned maintenance or to rationalize alarm storms. ')
emit('<code>shelve_until=0</code> means no lease — operator must unshelve manually.')
emit('Otherwise the <code>exception_analyzer</code> auto-unshelves when the ')
emit('lease expires.</p></div>')

r.page("Shelved Alarms", "shelved", table.concat(parts, "\n"))
