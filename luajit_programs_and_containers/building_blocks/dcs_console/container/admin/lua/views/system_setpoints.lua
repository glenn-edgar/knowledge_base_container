-- views/system_setpoints.lua -- Operator-tunable setpoints view.
-- Each row shows: label, description, current value (or "default"
-- when runtime row is empty), default, and an <details>/<summary>
-- inline edit form that POSTs to /action/setpoint/update.
--
-- build_row() split out so the action handler can return an updated
-- row HTML and the client's hx-swap="outerHTML" slots the new state
-- in without a full page refresh.

local sh      = require("shell_helpers")
local catalog = require("setpoints_catalog")

local M = {}

-- Build the <tr> for one setpoint. `info` is {current, default, description}.
function M.build_row(spec, info)
  local current_str
  if info.current ~= nil then
    current_str = string.format(
      '<strong>%d</strong> <span style="color:#888">%s</span>',
      info.current, sh.escape(spec.unit or ""))
  else
    current_str = string.format(
      '<span style="color:#888">default &middot; </span>' ..
      '<strong>%d</strong> <span style="color:#888">%s</span>',
      tonumber(info.default) or 0, sh.escape(spec.unit or ""))
  end

  -- Edit form inside a <details>. Native expand/collapse; no JS.
  local default_attr = tonumber(info.default) or 0
  local placeholder  = tostring(info.current or info.default or "")
  local edit_form = string.format([[
<details class="inline-edit" style="display:inline-block">
  <summary style="cursor:pointer;color:#7fbfff;padding:0.2em 0.5em;border:1px solid #345;border-radius:3px;list-style:none">edit</summary>
  <form hx-post="action/setpoint/update"
        hx-target="closest tr" hx-swap="outerHTML"
        style="display:inline-flex;gap:0.3em;margin-left:0.4em;align-items:center">
    <input type="hidden" name="name"  value="%s">
    <input type="number" name="value" value="%s" min="%d" max="%d" step="1" required
           style="background:#222;border:1px solid #444;color:#ddd;padding:0.2em 0.4em;font-size:0.9em;width:6em">
    <button type="submit"
            style="background:#133;color:#bff;border:1px solid #466;padding:0.2em 0.6em;border-radius:3px;cursor:pointer">save</button>
  </form>
</details>
]],
    sh.escape(spec.name),
    sh.escape(placeholder),
    spec.min or 0, spec.max or 999999999)

  return string.format([[
<tr id="setpoint-%s">
<td style="padding:0.5em 0.6em;border-bottom:1px solid #222;vertical-align:top">
  <strong>%s</strong>
  <div style="color:#888;font-size:0.85em;margin-top:0.2em">%s</div>
  <div style="color:#666;font-size:0.78em;margin-top:0.25em">%s</div>
</td>
<td style="padding:0.5em 0.6em;border-bottom:1px solid #222;vertical-align:top;white-space:nowrap">%s</td>
<td style="padding:0.5em 0.6em;border-bottom:1px solid #222;vertical-align:top;color:#888;font-size:0.9em">%d %s</td>
<td style="padding:0.5em 0.6em;border-bottom:1px solid #222;vertical-align:top;white-space:nowrap">%s</td>
</tr>
]],
    sh.escape(spec.name),
    sh.escape(spec.label),
    sh.escape(spec.description),
    sh.kb_path_span("KB_STATUS_FIELD", spec.name),
    current_str,
    default_attr, sh.escape(spec.unit or ""),
    edit_form)
end

function M.render()
  ngx.header["Content-Type"] = "text/html; charset=utf-8"
  local ctx = {
    title      = "system / setpoints",
    status_url = "status/system/setpoints",
  }

  local pg, err = sh.pg_connect()
  if not pg then
    sh.set_context(ctx)
    ngx.say('<h2>Setpoints</h2><p class="placeholder">pg unreachable: ' ..
            sh.escape(err or "") .. '</p>')
    return
  end

  local rows = {}
  for _, spec in ipairs(catalog.list) do
    local info, ierr = sh.read_setpoint(pg, spec.name)
    if info then
      rows[#rows + 1] = M.build_row(spec, info)
    else
      rows[#rows + 1] = string.format(
        '<tr><td colspan="4" style="padding:0.6em;color:#f88">%s: %s</td></tr>',
        sh.escape(spec.name), sh.escape(ierr or "read failed"))
    end
  end
  local exc_count = sh.active_exception_count(pg)
  pg:disconnect()

  if exc_count > 0 then ctx.badge = tostring(exc_count) end
  sh.set_context(ctx)

  local parts = {
    '<h2>Setpoints</h2>',
    '<p>Operator-tunable site-level values. Changes apply on the ' ..
    'next consumer tick (e.g., the gateway picks up a new poll ' ..
    'interval within ~one old interval).</p>',
    '<table style="width:100%;border-collapse:collapse">',
    '<thead><tr style="color:#888;font-size:0.88em;text-align:left">' ..
      '<th style="padding:0.4em 0.6em;border-bottom:1px solid #333">Setpoint</th>' ..
      '<th style="padding:0.4em 0.6em;border-bottom:1px solid #333">Current</th>' ..
      '<th style="padding:0.4em 0.6em;border-bottom:1px solid #333">Default</th>' ..
      '<th style="padding:0.4em 0.6em;border-bottom:1px solid #333">Edit</th>' ..
    '</tr></thead><tbody>',
  }
  for _, r in ipairs(rows) do parts[#parts + 1] = r end
  parts[#parts + 1] = '</tbody></table>'
  parts[#parts + 1] =
    '<footer class="last-event">Every edit is written to the audit log ' ..
    'with the operator identity and value. Invalid values (outside the ' ..
    'catalog\'s min/max) are rejected before write.</footer>'

  ngx.say(table.concat(parts))
end

return M
