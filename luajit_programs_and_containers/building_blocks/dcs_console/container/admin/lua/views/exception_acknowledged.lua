-- views/exception_acknowledged.lua -- Acknowledged but not cleared.
-- Operator said "I'm on it" but the underlying fault is still raised;
-- the alarm badge stops rising but the row persists. Clear moves to
-- history.

local sh = require("shell_helpers")

local M = {}

local function row_html(e)
  local when_html   = e.ts     and sh.time_el(e.ts, 3600)     or '<span class="empty">?</span>'
  local acked_html  = e.ack_at and sh.time_el(e.ack_at, 3600) or '<span class="empty">?</span>'
  local msg = e.last_error or e.description or ""
  if #msg > 120 then msg = msg:sub(1, 120) .. "&hellip;" end

  local clear_block = string.format([[
<details class="inline-confirm" style="display:inline-block">
  <summary style="cursor:pointer;color:#fc6;padding:0.2em 0.5em;border:1px solid #553;border-radius:3px;list-style:none">clear</summary>
  <form hx-post="action/exception/clear" hx-target="closest tr" hx-swap="delete"
        style="display:inline-flex;gap:0.3em;margin-left:0.4em;align-items:center">
    <input type="hidden" name="path" value="%s">
    <input type="text" name="note" placeholder="reason (optional)"
           style="background:#222;border:1px solid #444;color:#ddd;padding:0.2em 0.4em;font-size:0.85em;width:12em">
    <button type="submit" style="background:#511;color:#fcc;border:1px solid #744;padding:0.2em 0.6em;border-radius:3px;cursor:pointer">confirm clear</button>
  </form>
</details>
]], sh.escape(e.path))

  return string.format([[
<tr>
<td style="padding:0.4em 0.6em;border-bottom:1px solid #222;font-size:0.9em;vertical-align:top">%s</td>
<td style="padding:0.4em 0.6em;border-bottom:1px solid #222;vertical-align:top"><a href="#view=fragment/cpu/%s/summary" style="color:#7fbfff">%s</a></td>
<td style="padding:0.4em 0.6em;border-bottom:1px solid #222;vertical-align:top"><strong>%s</strong><div style="color:#888;font-size:0.85em">%s</div></td>
<td style="padding:0.4em 0.6em;border-bottom:1px solid #222;vertical-align:top;font-size:0.9em">%s</td>
<td style="padding:0.4em 0.6em;border-bottom:1px solid #222;vertical-align:top;font-size:0.85em">%s<div style="color:#888">%s</div></td>
<td style="padding:0.4em 0.6em;border-bottom:1px solid #222;vertical-align:top;white-space:nowrap">%s</td>
</tr>
]],
    when_html,
    sh.escape(e.cpu_id or "?"), sh.escape(e.cpu_id or "?"),
    sh.escape(e.name or ""), sh.escape(e.agent_instance or ""),
    sh.escape(msg),
    sh.escape(e.ack_by or ""), acked_html,
    clear_block)
end

function M.render()
  ngx.header["Content-Type"] = "text/html; charset=utf-8"
  local ctx = {
    title      = "exceptions / acknowledged",
    status_url = "status/exception/acknowledged",
  }

  local pg, err = sh.pg_connect()
  if not pg then
    sh.set_context(ctx)
    ngx.say('<h2>Acknowledged exceptions</h2><p class="placeholder">pg unreachable: ' ..
            sh.escape(err or "") .. '</p>')
    return
  end
  local list = sh.list_exceptions(pg, "acknowledged") or {}
  local exc_count = sh.active_exception_count(pg)
  pg:disconnect()

  if exc_count > 0 then ctx.badge = tostring(exc_count) end
  sh.set_context(ctx)

  local parts = { '<h2>Acknowledged exceptions</h2>' }
  if #list == 0 then
    parts[#parts + 1] =
      '<p class="placeholder">Nothing acknowledged. ' ..
      'Either there are no faults, or the operator hasn\'t ack\'d yet.</p>'
  else
    parts[#parts + 1] = '<p style="color:#888;font-size:0.9em">' ..
      tostring(#list) .. ' acknowledged (still raised in ready_bits).</p>'
    parts[#parts + 1] = '<table style="width:100%;border-collapse:collapse">'
    parts[#parts + 1] = '<thead><tr style="color:#888;font-size:0.88em;text-align:left">' ..
      '<th style="padding:0.35em 0.6em;border-bottom:1px solid #333">Raised</th>' ..
      '<th style="padding:0.35em 0.6em;border-bottom:1px solid #333">CPU</th>' ..
      '<th style="padding:0.35em 0.6em;border-bottom:1px solid #333">Exception / Agent</th>' ..
      '<th style="padding:0.35em 0.6em;border-bottom:1px solid #333">Message</th>' ..
      '<th style="padding:0.35em 0.6em;border-bottom:1px solid #333">Ack</th>' ..
      '<th style="padding:0.35em 0.6em;border-bottom:1px solid #333">Actions</th>' ..
      '</tr></thead><tbody>'
    for _, e in ipairs(list) do parts[#parts + 1] = row_html(e) end
    parts[#parts + 1] = '</tbody></table>'
  end
  parts[#parts + 1] = '<footer class="last-event">Snapshot ' ..
    sh.time_el(os.time(), 60) .. '</footer>'
  ngx.say(table.concat(parts))
end

return M
