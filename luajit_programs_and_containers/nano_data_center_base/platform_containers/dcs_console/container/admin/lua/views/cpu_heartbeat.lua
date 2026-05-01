-- views/cpu_heartbeat.lua -- Heartbeat leaf. More detail than Summary:
-- the wall-clock heartbeat value with its age ticking, the staleness
-- threshold, and a nudge if it has never been written (pre-handoff).
--
-- Future: extend with the last N heartbeat samples so operators can
-- see cadence jitter. Today's heartbeat isn't streamed through a
-- ring, so v1 shows only the latest.

local sh = require("shell_helpers")

local M = {}

local HEARTBEAT_STALE_S = 15

function M.render(cpu_id)
  ngx.header["Content-Type"] = "text/html; charset=utf-8"

  local ctx = {
    title      = cpu_id .. " / heartbeat",
    status_url = "status/cpu/" .. cpu_id .. "/heartbeat",
  }

  local pg, err = sh.pg_connect()
  if not pg then
    sh.set_context(ctx)
    ngx.say(string.format(
      '<h2>%s heartbeat</h2><p class="placeholder">pg unreachable: %s</p>',
      sh.escape(cpu_id), sh.escape(err or "")))
    return
  end

  local me        = sh.get_cpu(pg, cpu_id)
  local hb_epoch  = sh.read_cpu_heartbeat_epoch(pg, cpu_id)
  local exc_count = sh.active_exception_count(pg)
  pg:disconnect()

  local hostname = (me and me.hostname) or "(no hostname)"
  ctx.title = hostname .. " / " .. cpu_id .. " / heartbeat"

  if exc_count > 0 then ctx.badge = tostring(exc_count) end
  sh.set_context(ctx)

  local parts = {
    string.format(
      '<h2>%s <span style="color:#888;font-weight:normal">(%s) heartbeat</span></h2>',
      sh.escape(hostname), sh.escape(cpu_id))
  }

  if not hb_epoch then
    parts[#parts + 1] =
      '<p class="placeholder">Heartbeat not yet written.' ..
      ' This is normal if the CPU\'s node_control has not reached monitor' ..
      ' state yet (refresh once DCS is fully up).</p>'
  else
    local age = os.time() - hb_epoch
    local pill
    if age <= HEARTBEAT_STALE_S        then pill = sh.pill("ok",   "FRESH")
    elseif age <= HEARTBEAT_STALE_S*3 then pill = sh.pill("warn", "LAGGING")
    else                                    pill = sh.pill("fail", "STALE") end
    parts[#parts + 1] = '<dl class="status-list">'
    parts[#parts + 1] = '<dt>KB path</dt><dd>' ..
                        sh.kb_path_span("cpu", cpu_id, "KB_BIT_MASK", "heartbeat") ..
                        '</dd>'
    parts[#parts + 1] = '<dt>Status</dt><dd>' .. pill .. '</dd>'
    parts[#parts + 1] = '<dt>Last beat</dt><dd>' ..
                        sh.time_el(hb_epoch, HEARTBEAT_STALE_S) ..
                        ' (' .. os.date("!%Y-%m-%dT%H:%M:%SZ", hb_epoch) ..
                        ')</dd>'
    parts[#parts + 1] = '<dt>Stale threshold</dt><dd>' ..
                        tostring(HEARTBEAT_STALE_S) .. 's</dd>'
    parts[#parts + 1] = '<dt>Expected cadence</dt><dd>5s (system_control monitor loop)</dd>'
    parts[#parts + 1] = '</dl>'
  end

  parts[#parts + 1] = '<footer class="last-event">Source: ' ..
    '<code>bit_mask_table</code> (bit_mask column holds Unix epoch ns).</footer>'

  ngx.say(table.concat(parts))
end

return M
