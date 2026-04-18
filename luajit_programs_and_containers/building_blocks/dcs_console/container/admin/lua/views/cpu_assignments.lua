-- views/cpu_assignments.lua -- Assignments leaf. What containers are
-- supposed to run on this CPU, derived from CONTAINER_REGISTRY.
-- Phase 4 will extend each row with drill-down into its status detail.

local sh = require("shell_helpers")

local M = {}

function M.render(cpu_id)
  ngx.header["Content-Type"] = "text/html; charset=utf-8"

  local ctx = {
    title      = cpu_id .. " / assignments",
    status_url = "status/cpu/" .. cpu_id .. "/assignments",
  }

  local pg, err = sh.pg_connect()
  if not pg then
    sh.set_context(ctx)
    ngx.say(string.format(
      '<h2>%s assignments</h2><p class="placeholder">pg unreachable: %s</p>',
      sh.escape(cpu_id), sh.escape(err or "")))
    return
  end

  local me        = sh.get_cpu(pg, cpu_id)
  local list      = sh.containers_on(pg, cpu_id) or {}
  local exc_count = sh.active_exception_count(pg)
  pg:disconnect()

  local hostname = (me and me.hostname) or "(no hostname)"
  ctx.title = hostname .. " / " .. cpu_id .. " / assignments"

  if exc_count > 0 then ctx.badge = tostring(exc_count) end
  sh.set_context(ctx)

  local parts = {
    string.format(
      '<h2>%s <span style="color:#888;font-weight:normal">(%s) assignments</span></h2>',
      sh.escape(hostname), sh.escape(cpu_id)),
    '<p>Registry parent path: ' ..
      sh.kb_path_span("cpu", cpu_id, "CONTAINER_REGISTRY") ..
      '</p>',
  }

  if #list == 0 then
    parts[#parts + 1] =
      '<p class="placeholder">No containers assigned to this CPU.</p>'
  else
    parts[#parts + 1] = '<table style="width:100%;border-collapse:collapse">'
    parts[#parts + 1] = '<thead><tr style="color:#888;font-size:0.88em;text-align:left">' ..
      '<th style="padding:0.4em 0.6em;border-bottom:1px solid #333">Name</th>' ..
      '<th style="padding:0.4em 0.6em;border-bottom:1px solid #333">Definition</th>' ..
      '<th style="padding:0.4em 0.6em;border-bottom:1px solid #333">Registered</th>' ..
      '<th style="padding:0.4em 0.6em;border-bottom:1px solid #333">Image</th>' ..
      '</tr></thead><tbody>'
    for _, c in ipairs(list) do
      local reg_cell = c.registered_at
        and sh.time_el(c.registered_at, 3600)
        or  '<span class="empty">pending</span>'
      parts[#parts + 1] = string.format(
        '<tr>' ..
        '<td style="padding:0.4em 0.6em;border-bottom:1px solid #222">' ..
          '<a href="#view=fragment/container/%s/status" style="color:#7fbfff">%s</a></td>' ..
        '<td style="padding:0.4em 0.6em;border-bottom:1px solid #222">%s</td>' ..
        '<td style="padding:0.4em 0.6em;border-bottom:1px solid #222">%s</td>' ..
        '<td style="padding:0.4em 0.6em;border-bottom:1px solid #222;font-size:0.85em;color:#aaa">%s</td>' ..
        '</tr>',
        sh.escape(c.name), sh.escape(c.name),
        sh.escape(c.definition or ""),
        reg_cell,
        sh.escape(c.image or ""))
    end
    parts[#parts + 1] = '</tbody></table>'
  end

  parts[#parts + 1] = '<footer class="last-event">' ..
    tostring(#list) .. ' container' .. ((#list == 1) and '' or 's') ..
    ' on ' .. sh.escape(hostname) .. ' (' .. sh.escape(cpu_id) .. ')' ..
    ' &middot; snapshot ' .. sh.time_el(os.time(), 120) ..
    '</footer>'

  ngx.say(table.concat(parts))
end

return M
