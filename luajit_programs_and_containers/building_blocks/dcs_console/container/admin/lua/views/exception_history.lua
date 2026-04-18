-- views/exception_history.lua -- Cleared SYS_EXCEPTION rows (read-only
-- retrospective). Shows who cleared, when, and the operator note.

local sh = require("shell_helpers")

local M = {}

local function row_html(e)
  local raised_html  = e.ts         and sh.time_el(e.ts, 86400)         or '<span class="empty">?</span>'
  local cleared_html = e.cleared_at and sh.time_el(e.cleared_at, 86400) or '<span class="empty">?</span>'
  local msg = e.last_error or e.description or ""
  if #msg > 120 then msg = msg:sub(1, 120) .. "&hellip;" end
  return string.format([[
<tr>
<td style="padding:0.4em 0.6em;border-bottom:1px solid #222;font-size:0.9em;vertical-align:top">%s</td>
<td style="padding:0.4em 0.6em;border-bottom:1px solid #222;vertical-align:top">%s</td>
<td style="padding:0.4em 0.6em;border-bottom:1px solid #222;vertical-align:top"><strong>%s</strong><div style="color:#888;font-size:0.85em">%s</div></td>
<td style="padding:0.4em 0.6em;border-bottom:1px solid #222;vertical-align:top;font-size:0.9em">%s</td>
<td style="padding:0.4em 0.6em;border-bottom:1px solid #222;vertical-align:top;font-size:0.85em">%s<div style="color:#888">%s</div></td>
<td style="padding:0.4em 0.6em;border-bottom:1px solid #222;vertical-align:top;font-size:0.85em;color:#bbb">%s</td>
</tr>
]],
    raised_html,
    sh.escape(e.cpu_id or "?"),
    sh.escape(e.name or ""), sh.escape(e.agent_instance or ""),
    sh.escape(msg),
    sh.escape(e.cleared_by or ""), cleared_html,
    sh.escape(e.note or ""))
end

function M.render()
  ngx.header["Content-Type"] = "text/html; charset=utf-8"
  local ctx = {
    title      = "exceptions / history",
    status_url = "status/exception/history",
  }

  local pg, err = sh.pg_connect()
  if not pg then
    sh.set_context(ctx)
    ngx.say('<h2>Exception history</h2><p class="placeholder">pg unreachable: ' ..
            sh.escape(err or "") .. '</p>')
    return
  end
  local list = sh.list_exceptions(pg, "history") or {}
  local exc_count = sh.active_exception_count(pg)
  pg:disconnect()

  if exc_count > 0 then ctx.badge = tostring(exc_count) end
  sh.set_context(ctx)

  local parts = { '<h2>Exception history</h2>' }
  if #list == 0 then
    parts[#parts + 1] =
      '<p class="placeholder">No cleared exceptions recorded.</p>'
  else
    parts[#parts + 1] = '<p style="color:#888;font-size:0.9em">' ..
      'Most-recently-cleared first. Capped at 200 rows.</p>'
    parts[#parts + 1] = '<table style="width:100%;border-collapse:collapse">'
    parts[#parts + 1] = '<thead><tr style="color:#888;font-size:0.88em;text-align:left">' ..
      '<th style="padding:0.35em 0.6em;border-bottom:1px solid #333">Raised</th>' ..
      '<th style="padding:0.35em 0.6em;border-bottom:1px solid #333">CPU</th>' ..
      '<th style="padding:0.35em 0.6em;border-bottom:1px solid #333">Exception / Agent</th>' ..
      '<th style="padding:0.35em 0.6em;border-bottom:1px solid #333">Message</th>' ..
      '<th style="padding:0.35em 0.6em;border-bottom:1px solid #333">Cleared</th>' ..
      '<th style="padding:0.35em 0.6em;border-bottom:1px solid #333">Note</th>' ..
      '</tr></thead><tbody>'
    for _, e in ipairs(list) do parts[#parts + 1] = row_html(e) end
    parts[#parts + 1] = '</tbody></table>'
  end
  parts[#parts + 1] = '<footer class="last-event">Source: ' ..
    '<code>knowledge_base_status</code> rows with status=false joined with ' ..
    'label=SYS_EXCEPTION.</footer>'
  ngx.say(table.concat(parts))
end

return M
