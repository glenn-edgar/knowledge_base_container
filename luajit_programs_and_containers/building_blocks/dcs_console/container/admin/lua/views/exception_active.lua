-- views/exception_active.lua -- Active (unacknowledged, still-raised)
-- SYS_EXCEPTION rows. Each row has two mutation buttons:
--   [ack]   -> POST /action/exception/ack  (row removes on success)
--   [clear] -> <details>/<summary> inline confirm with optional note
--              -> POST /action/exception/clear  (row removes on success)
--
-- Build body + ctx split out so sse_views/exception_active.lua can
-- re-render on each 3s tick and keep the list live-current.

local sh = require("shell_helpers")

local M = {}

-- Per-row HTML. The <tr> is the htmx target for both mutations.
local function row_html(e)
  local cpu   = e.cpu_id or "?"
  local when_html = e.ts and sh.time_el(e.ts, 3600)
                         or '<span class="empty">unknown</span>'
  local msg = e.last_error or e.description or ""
  if #msg > 120 then msg = msg:sub(1, 120) .. "&hellip;" end

  -- Clear button pattern: <details> native-expand with an inline form.
  -- Click summary -> form appears with optional note + confirm. No
  -- accidental single-click clears. No JS needed.
  local path_attr = sh.escape(e.path)
  local clear_block = string.format([[
<details class="inline-confirm" style="display:inline-block">
  <summary style="cursor:pointer;color:#fc6;padding:0.2em 0.5em;border:1px solid #553;border-radius:3px;list-style:none">clear</summary>
  <form hx-post="action/exception/clear" hx-target="closest tr" hx-swap="delete"
        style="display:inline-flex;gap:0.3em;margin-left:0.4em;align-items:center">
    <input type="hidden" name="path" value="%s">
    <input type="text"   name="note" placeholder="reason (optional)"
           style="background:#222;border:1px solid #444;color:#ddd;padding:0.2em 0.4em;font-size:0.85em;width:12em">
    <button type="submit" style="background:#511;color:#fcc;border:1px solid #744;padding:0.2em 0.6em;border-radius:3px;cursor:pointer">confirm clear</button>
  </form>
</details>
]], path_attr)

  local ack_btn = string.format([[
<button hx-post="action/exception/ack" hx-target="closest tr" hx-swap="delete"
        hx-vals='{"path":"%s"}'
        style="color:#8bf;background:#223;border:1px solid #446;padding:0.2em 0.6em;border-radius:3px;cursor:pointer;margin-right:0.4em">ack</button>
]], path_attr)

  return string.format([[
<tr>
<td style="padding:0.4em 0.6em;border-bottom:1px solid #222;font-size:0.9em;vertical-align:top">%s</td>
<td style="padding:0.4em 0.6em;border-bottom:1px solid #222;vertical-align:top"><a href="#view=fragment/cpu/%s/summary" style="color:#7fbfff">%s</a></td>
<td style="padding:0.4em 0.6em;border-bottom:1px solid #222;vertical-align:top"><strong>%s</strong><div style="color:#888;font-size:0.85em">%s</div></td>
<td style="padding:0.4em 0.6em;border-bottom:1px solid #222;vertical-align:top;font-size:0.9em">%s</td>
<td style="padding:0.4em 0.6em;border-bottom:1px solid #222;vertical-align:top;white-space:nowrap">%s%s</td>
</tr>
]],
    when_html,
    sh.escape(cpu), sh.escape(cpu),
    sh.escape(e.name or ""), sh.escape(e.agent_instance or ""),
    sh.escape(msg),
    ack_btn, clear_block)
end

function M.build_body()
  local pg, err = sh.pg_connect()
  if not pg then
    return string.format(
      '<h2>Active exceptions</h2><p class="placeholder">pg unreachable: %s</p>',
      sh.escape(err or "")),
      { title = "exceptions / active" }
  end
  local list = sh.list_exceptions(pg, "active") or {}
  local exc_count = sh.active_exception_count(pg)
  pg:disconnect()

  local parts = {
    '<h2>Active exceptions</h2>',
    '<p>KB parent: ' .. sh.kb_path_span() ..
      ' (rows under <code>SYS_EXCEPTION</code>)</p>',
  }
  if #list == 0 then
    parts[#parts + 1] =
      '<p class="placeholder">No active exceptions. ' ..
      'Site is quiet.</p>'
  else
    parts[#parts + 1] = '<p style="color:#888;font-size:0.9em">' ..
      tostring(#list) .. ' active &middot; ' ..
      'ack silences the alarm badge; clear moves to history.</p>'
    parts[#parts + 1] = '<table style="width:100%;border-collapse:collapse">'
    parts[#parts + 1] = '<thead><tr style="color:#888;font-size:0.88em;text-align:left">' ..
      '<th style="padding:0.35em 0.6em;border-bottom:1px solid #333">When</th>' ..
      '<th style="padding:0.35em 0.6em;border-bottom:1px solid #333">CPU</th>' ..
      '<th style="padding:0.35em 0.6em;border-bottom:1px solid #333">Exception / Agent</th>' ..
      '<th style="padding:0.35em 0.6em;border-bottom:1px solid #333">Message</th>' ..
      '<th style="padding:0.35em 0.6em;border-bottom:1px solid #333">Actions</th>' ..
      '</tr></thead><tbody>'
    for _, e in ipairs(list) do parts[#parts + 1] = row_html(e) end
    parts[#parts + 1] = '</tbody></table>'
  end

  parts[#parts + 1] = '<footer class="last-event">Snapshot ' ..
    sh.time_el(os.time(), 60) ..
    '. Open status popup (<span aria-hidden="true">&#9432;</span>) for a live 3s feed.</footer>'

  local ctx = {
    title             = "exceptions / active",
    status_url        = "status/exception/active",
    status_stream_url = "sse/exception/active",
  }
  if exc_count > 0 then ctx.badge = tostring(exc_count) end
  return table.concat(parts), ctx
end

function M.render()
  local html, ctx = M.build_body()
  ngx.header["Content-Type"] = "text/html; charset=utf-8"
  sh.set_context(ctx)
  ngx.say(html)
end

return M
