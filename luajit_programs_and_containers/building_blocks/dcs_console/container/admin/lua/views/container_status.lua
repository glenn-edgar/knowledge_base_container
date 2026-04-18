-- views/container_status.lua -- Per-container detail. Driven entirely
-- from the CONTAINER_REGISTRY row (node_control writes / deletes on
-- container lifecycle). "Running" is inferred from "registered":
-- node_control removes the row when it stops the container, and re-
-- writes it on restart, so presence-of-row is a strong (but not
-- definitive) liveness proxy without needing docker access from the
-- admin pod.
--
-- Phase 5+ will add real events ring, restart count, and mutation
-- buttons (stop-for-maintenance / start / restart).

local sh = require("shell_helpers")

local M = {}

local STALE_REGISTERED_S = 3600   -- registry rows older than 1h are still fine

-- Render one port record row.
local function port_row(p)
  return string.format(
    '<tr>' ..
    '<td style="padding:0.35em 0.6em;border-bottom:1px solid #222">%s</td>' ..
    '<td style="padding:0.35em 0.6em;border-bottom:1px solid #222">%s</td>' ..
    '<td style="padding:0.35em 0.6em;border-bottom:1px solid #222">%s</td>' ..
    '<td style="padding:0.35em 0.6em;border-bottom:1px solid #222">%s</td>' ..
    '<td style="padding:0.35em 0.6em;border-bottom:1px solid #222">%s</td>' ..
    '<td style="padding:0.35em 0.6em;border-bottom:1px solid #222">%s</td>' ..
    '</tr>',
    sh.escape(p.slot      or ""),
    sh.escape(tostring(p.external or "")),
    sh.escape(tostring(p.internal or "")),
    sh.escape(p.protocol  or ""),
    sh.escape(p.purpose   or ""),
    sh.escape(p.description or ""))
end

-- Build body + context. Separated so the SSE stream can reuse.
function M.build_body(name)
  if not name or name == "" then
    return '<h2>Container</h2><p class="placeholder">Missing name.</p>',
           { title = "container / (invalid)" }
  end

  local pg, err = sh.pg_connect()
  if not pg then
    return string.format(
      '<h2>container %s</h2><p class="placeholder">pg unreachable: %s</p>',
      sh.escape(name), sh.escape(err or "")),
      { title = "container / " .. name }
  end

  local c         = sh.get_container(pg, name)
  local exc_count = sh.active_exception_count(pg)
  local host_cpu
  if c and c.cpu_id then host_cpu = sh.get_cpu(pg, c.cpu_id) end
  pg:disconnect()

  if not c then
    return string.format(
      '<h2>container %s</h2>' ..
      '<p class="placeholder">Not present in CONTAINER_REGISTRY.</p>' ..
      '<p style="color:#666;font-size:0.9em">The container is either stopped, ' ..
      'not yet started, or has been deregistered. Check <code>docker ps</code> ' ..
      'on its assigned CPU or open the CPU\'s Assignments view.</p>',
      sh.escape(name)),
      { title = "container / " .. name .. " / not found" }
  end

  -- "Running" inference: a row exists; node_control removes it on stop.
  -- We render an ok-pill but caveat the wording in the footer.
  local running_pill = sh.pill("ok", "REGISTERED")

  local hostname = (host_cpu and host_cpu.hostname) or "(unknown)"
  local cpu_display = string.format("%s (%s)", hostname, c.cpu_id or "?")

  local parts = {
    string.format(
      '<h2>%s <span style="color:#888;font-weight:normal">[%s]</span></h2>',
      sh.escape(name), sh.escape(c.definition or "?")),
    '<dl class="status-list">',
    '<dt>KB path</dt><dd>',
      sh.kb_path_span("cpu", c.cpu_id or "?", "CONTAINER_REGISTRY", name), '</dd>',
    '<dt>Status</dt><dd>',          running_pill, '</dd>',
    '<dt>Assigned CPU</dt><dd>',    sh.escape(cpu_display),
      ' <a href="#view=fragment/cpu/', sh.escape(c.cpu_id or ""),
      '/summary" style="color:#7fbfff">(view)</a></dd>',
    '<dt>Definition</dt><dd>',      sh.escape(c.definition or ""), '</dd>',
    '<dt>Category</dt><dd>',        sh.escape(c.category or ""), '</dd>',
    '<dt>Image</dt><dd><code style="font-size:0.9em">',
      sh.escape(c.image or ""), '</code></dd>',
    '<dt>Description</dt><dd>',     sh.escape(c.description or ""), '</dd>',
    '<dt>Registered</dt><dd>',
      c.registered_at and sh.time_el(c.registered_at, STALE_REGISTERED_S)
                      or '<span class="empty">pending</span>',
    '</dd>',
    '</dl>',
  }

  -- Ports table.
  if c.ports and #c.ports > 0 then
    table.insert(parts, '<h3 style="color:#fff;font-weight:500;margin-top:1.4em">Ports</h3>')
    table.insert(parts, '<table style="width:100%;border-collapse:collapse">')
    table.insert(parts, '<thead><tr style="color:#888;font-size:0.88em;text-align:left">' ..
      '<th style="padding:0.35em 0.6em;border-bottom:1px solid #333">Slot</th>' ..
      '<th style="padding:0.35em 0.6em;border-bottom:1px solid #333">External</th>' ..
      '<th style="padding:0.35em 0.6em;border-bottom:1px solid #333">Internal</th>' ..
      '<th style="padding:0.35em 0.6em;border-bottom:1px solid #333">Proto</th>' ..
      '<th style="padding:0.35em 0.6em;border-bottom:1px solid #333">Purpose</th>' ..
      '<th style="padding:0.35em 0.6em;border-bottom:1px solid #333">Description</th>' ..
      '</tr></thead><tbody>')
    for _, p in ipairs(c.ports) do
      table.insert(parts, port_row(p))
    end
    table.insert(parts, '</tbody></table>')
  end

  -- Events placeholder -- phase 5+ will stream real lifecycle events.
  table.insert(parts,
    '<h3 style="color:#fff;font-weight:500;margin-top:1.4em">Lifecycle events</h3>')
  table.insert(parts,
    '<p class="placeholder">No events ring written yet. Phase 5 will populate ' ..
    'from the per-container <code>KB_STREAM_FIELD.events</code> stream at ' ..
    sh.kb_path_span("cpu", c.cpu_id or "?", "container", name,
                    "KB_STREAM_FIELD", "events") ..
    '.</p>')

  -- Footer.
  table.insert(parts,
    '<footer class="last-event">Source: <code>CONTAINER_REGISTRY</code> row written by ' ..
    'node_control at REGISTER time. Snapshot ' ..
    sh.time_el(os.time(), 60) ..
    '. "REGISTERED" means node_control currently considers this container ' ..
    'alive; for definitive liveness, run <code>docker ps</code> on the assigned CPU.</footer>')

  local ctx = {
    title             = "container / " .. name,
    status_url        = "status/container/" .. name .. "/status",
    status_stream_url = "sse/container/" .. name .. "/status",
  }
  if exc_count > 0 then ctx.badge = tostring(exc_count) end
  return table.concat(parts), ctx
end

function M.render(name)
  local html, ctx = M.build_body(name)
  ngx.header["Content-Type"] = "text/html; charset=utf-8"
  sh.set_context(ctx)
  ngx.say(html)
end

return M
