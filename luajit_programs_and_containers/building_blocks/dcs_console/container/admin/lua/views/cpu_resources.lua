-- views/cpu_resources.lua -- Resources leaf. Shows the most recent
-- samples from node_monitor's `monitor.samples` stream for this CPU.
-- Sampler cadence is 60s for host/proc/container rows and 300s for
-- trend snapshots; Docker Desktop/WSL2 does not expose container
-- cgroups, so in dev the container-kind samples will often be
-- absent (see project_dcs_resource_monitor memo).

local sh    = require("shell_helpers")
local cjson = require("cjson.safe")

local M = {}

local SAMPLES_SHOWN = 8

function M.render(cpu_id)
  ngx.header["Content-Type"] = "text/html; charset=utf-8"

  local ctx = {
    title      = cpu_id .. " / resources",
    status_url = "status/cpu/" .. cpu_id .. "/resources",
  }

  local pg, err = sh.pg_connect()
  if not pg then
    sh.set_context(ctx)
    ngx.say(string.format(
      '<h2>%s resources</h2><p class="placeholder">pg unreachable: %s</p>',
      sh.escape(cpu_id), sh.escape(err or "")))
    return
  end

  local me = sh.get_cpu(pg, cpu_id)

  -- Grab last N samples from the stream table, newest first.
  local path = string.format(
    "system.site.%s.cpu.%s.monitor.samples.KB_STREAM_FIELD.samples",
    os.getenv("APP_SITE") or "moonbase.alpha.dcs", cpu_id)
  local rs, qerr = pg:query(string.format([[
    SELECT recorded_at, data
    FROM knowledge_base_stream
    WHERE path = '%s'::ltree
    ORDER BY recorded_at DESC
    LIMIT %d
  ]], path:gsub("'", "''"), SAMPLES_SHOWN))
  local exc_count = sh.active_exception_count(pg)
  pg:disconnect()

  if exc_count > 0 then ctx.badge = tostring(exc_count) end
  local hostname = (me and me.hostname) or "(no hostname)"
  ctx.title = hostname .. " / " .. cpu_id .. " / resources"
  sh.set_context(ctx)

  local parts = {
    string.format(
      '<h2>%s <span style="color:#888;font-weight:normal">(%s) resources</span></h2>',
      sh.escape(hostname), sh.escape(cpu_id)),
    '<p>Stream path: ' ..
      sh.kb_path_span("cpu", cpu_id, "monitor", "samples",
                      "KB_STREAM_FIELD", "samples") ..
      '</p>',
  }

  if qerr then
    parts[#parts + 1] = '<p class="placeholder">stream query failed: ' ..
                        sh.escape(qerr) .. '</p>'
  elseif not rs or #rs == 0 then
    parts[#parts + 1] =
      '<p class="placeholder">No resource samples recorded yet.</p>' ..
      '<p style="color:#666;font-size:0.9em">node_monitor samples host/proc at 60s cadence and ' ..
      'trends at 300s. Containers may be absent in Docker Desktop/WSL2 ' ..
      'dev (no cgroup access). First row typically appears within 60s of node_control setup.</p>'
  else
    parts[#parts + 1] =
      '<p style="color:#888;font-size:0.9em">Last ' .. tostring(#rs) ..
      ' sample' .. ((#rs == 1) and '' or 's') .. ', newest first.</p>'
    parts[#parts + 1] = '<table style="width:100%;border-collapse:collapse">'
    parts[#parts + 1] = '<thead><tr style="color:#888;font-size:0.88em;text-align:left">' ..
      '<th style="padding:0.4em 0.6em;border-bottom:1px solid #333">When</th>' ..
      '<th style="padding:0.4em 0.6em;border-bottom:1px solid #333">Kind</th>' ..
      '<th style="padding:0.4em 0.6em;border-bottom:1px solid #333">Payload</th>' ..
      '</tr></thead><tbody>'
    for _, r in ipairs(rs) do
      -- recorded_at is a timestamptz string like "2026-04-18 19:01:26.323+00".
      -- We extract just the "YYYY-MM-DDTHH:MM:SSZ" part for our <time> tag.
      -- For dev-speed and not parsing timestamptz: use now() - N as a
      -- coarse proxy (later: compute properly via pg's to_char).
      local ts_str = tostring(r.recorded_at or "")
      local kind = "?"
      local data = r.data
      if type(data) == "string" then data = cjson.decode(data) or {} end
      if type(data) == "table" and data.kind then kind = data.kind end
      local pretty = cjson.encode(data) or ""
      if #pretty > 200 then pretty = pretty:sub(1, 200) .. "&hellip;" end
      parts[#parts + 1] = string.format(
        '<tr>' ..
        '<td style="padding:0.4em 0.6em;border-bottom:1px solid #222;white-space:nowrap;font-size:0.85em">%s</td>' ..
        '<td style="padding:0.4em 0.6em;border-bottom:1px solid #222">%s</td>' ..
        '<td style="padding:0.4em 0.6em;border-bottom:1px solid #222;font-family:monospace;font-size:0.82em;color:#aaa">%s</td>' ..
        '</tr>',
        sh.escape(ts_str), sh.escape(kind), sh.escape(pretty))
    end
    parts[#parts + 1] = '</tbody></table>'
  end

  parts[#parts + 1] = '<footer class="last-event">Source: ' ..
    '<code>knowledge_base_stream</code> (see stream path above).</footer>'

  ngx.say(table.concat(parts))
end

return M
