-- views/cpu_summary.lua -- Per-CPU Summary leaf. One-glance answer to
-- "what is this CPU doing?": role, ready/sync bit state, heartbeat
-- freshness, assignment count, derived operational flag.
--
-- Dispatched from fragment.lua with a cpu_id argument parsed out of
-- the URL. The matching SSE stream (sse_views/cpu_summary.lua) reuses
-- this module's build_body() to emit live refreshes while the status
-- popup is open.

local sh = require("shell_helpers")

local M = {}

local HEARTBEAT_STALE_S = 15   -- heartbeat cadence is ~5s

function M.build_body(cpu_id)
  if not cpu_id or cpu_id == "" then
    return '<h2>CPU</h2><p class="placeholder">Missing cpu_id.</p>',
           { title = "CPU / (invalid)" }
  end

  local pg, err = sh.pg_connect()
  if not pg then
    return string.format(
      '<h2>CPU %s</h2><p class="placeholder">pg unreachable: %s</p>',
      sh.escape(cpu_id), sh.escape(err or "")),
      { title = "CPU / " .. cpu_id }
  end

  -- Pull everything in a single pg session.
  local cpus       = sh.list_cpus(pg) or {}
  local me
  for _, c in ipairs(cpus) do if c.cpu_id == cpu_id then me = c break end end
  local ready_bits = sh.site_bit_mask(pg, "ready_bits")
  local sync_bits  = sh.site_bit_mask(pg, "cluster_sync_bits")
  local hb_epoch   = sh.read_cpu_heartbeat_epoch(pg, cpu_id)
  local assigns    = sh.containers_on(pg, cpu_id) or {}
  local exc_count  = sh.active_exception_count(pg)
  pg:disconnect()

  if not me then
    return string.format(
      '<h2>CPU %s</h2><p class="placeholder">No such CPU in topology.</p>',
      sh.escape(cpu_id)),
      { title = "CPU / " .. cpu_id .. " / not found" }
  end

  -- Bit helpers.
  local function bit_set(mask, idx)
    if not mask then return nil end
    return (math.floor(mask / (2 ^ idx)) % 2) == 1
  end
  local ready_on = bit_set(ready_bits, me.bit_index)
  local sync_on  = bit_set(sync_bits,  me.bit_index)

  -- Heartbeat pill.
  local hb_pill, hb_time_html
  if hb_epoch then
    local age = os.time() - hb_epoch
    if age <= HEARTBEAT_STALE_S then
      hb_pill = sh.pill("ok", "FRESH")
    elseif age <= HEARTBEAT_STALE_S * 3 then
      hb_pill = sh.pill("warn", "LAGGING")
    else
      hb_pill = sh.pill("fail", "STALE")
    end
    hb_time_html = sh.time_el(hb_epoch, HEARTBEAT_STALE_S)
  else
    hb_pill = sh.pill("unknown", "NEVER")
    hb_time_html = '<span class="empty">never written</span>'
  end

  -- Operational flag: ready bit set AND heartbeat fresh.
  local operational = sh.cpu_is_operational(ready_bits, me.bit_index,
                                            hb_epoch, HEARTBEAT_STALE_S)
  local op_pill
  if operational == true  then op_pill = sh.pill("ok", "OPERATIONAL")
  elseif operational == false then op_pill = sh.pill("fail", "DOWN")
  else op_pill = sh.pill("unknown", "?") end

  local role_pill = me.is_master and sh.pill("ok", "MASTER")
                                  or sh.pill("ok", "SLAVE")
  local ready_pill = (ready_on == true)  and sh.pill("ok",   "SET")
                   or (ready_on == false) and sh.pill("fail", "UNSET")
                   or sh.pill("unknown", "?")
  local sync_pill  = (sync_on  == true)  and sh.pill("ok",   "SET")
                   or (sync_on  == false) and sh.pill("fail", "UNSET")
                   or sh.pill("unknown", "?")

  local hostname = me.hostname or "(no hostname)"
  local parts = {
    string.format('<h2>%s <span style="color:#888;font-weight:normal">(%s)</span></h2>',
                  sh.escape(hostname), sh.escape(cpu_id)),
    '<dl class="status-list">',
    '<dt>KB path</dt><dd>',           sh.kb_path_span("cpu", cpu_id), '</dd>',
    '<dt>Hostname</dt><dd>',          sh.escape(hostname), '</dd>',
    '<dt>cpu_id</dt><dd>',            sh.escape(cpu_id), '</dd>',
    '<dt>Role</dt><dd>',              role_pill, '</dd>',
    '<dt>bit_index</dt><dd>',         tostring(me.bit_index), '</dd>',
    '<dt>Operational</dt><dd>',       op_pill, '</dd>',
    '<dt>Ready bit</dt><dd>',         ready_pill, '</dd>',
    '<dt>Sync bit</dt><dd>',          sync_pill, '</dd>',
    '<dt>Heartbeat</dt><dd>',         hb_pill, ' &middot; last ', hb_time_html, '</dd>',
    '<dt>Assignments</dt><dd>',       tostring(#assigns),
      (#assigns > 0) and (' <a href="#view=fragment/cpu/' .. cpu_id .. '/assignments" style="color:#7fbfff">(view)</a>') or '',
    '</dd>',
    '<dt>Updated</dt><dd>',           sh.time_el(os.time(), 30), '</dd>',
    '</dl>',
    '<footer class="last-event">',
      'Heartbeat stale threshold: ', tostring(HEARTBEAT_STALE_S), 's. ',
      'Click <span aria-hidden="true">&#9432;</span> for the live status stream.',
    '</footer>',
  }
  local html = table.concat(parts)

  local ctx = {
    title             = hostname .. " / " .. cpu_id,
    status_url        = "status/cpu/" .. cpu_id .. "/summary",
    status_stream_url = "sse/cpu/" .. cpu_id .. "/summary",
  }
  if exc_count > 0 then ctx.badge = tostring(exc_count) end
  return html, ctx
end

function M.render(cpu_id)
  local html, ctx = M.build_body(cpu_id)
  ngx.header["Content-Type"] = "text/html; charset=utf-8"
  sh.set_context(ctx)
  ngx.say(html)
end

return M
