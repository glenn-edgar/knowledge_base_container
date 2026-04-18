-- views/infra_service.lua -- One module for the whole Infra menu.
-- Dispatched from fragment.lua via infra/<service>; the service name
-- picks the probe and the per-service metadata. Uniform rendering:
--
--   h2   name
--   dl   KB path, kind, managed_by, probed host:port
--        probe result: traffic-light pill + latency
--   footer  source note
--
-- build_body(service) split out so sse_views/infra_service.lua can
-- reuse the exact rendering on each 3s live tick.

local sh = require("shell_helpers")

local M = {}

-- Per-service probe recipe. port is the infra's published host port;
-- host is always host.docker.internal (cached by shell_helpers). pg
-- uses a deeper check via pgmoon (connect + SELECT 1) since we
-- already have the machinery; the others are plain TCP SYN+RST.
local SERVICES = {
  postgres = {
    container_name = "pg-vector",
    port           = 5432,
    probe          = function()
      local pg, err = sh.pg_connect()
      local t0 = ngx.now()
      if not pg then
        return false, "connect: " .. tostring(err),
               math.floor((ngx.now() - t0) * 1000 + 0.5)
      end
      local rs, qerr = pg:query("SELECT 1 AS ok")
      local ms = math.floor((ngx.now() - t0) * 1000 + 0.5)
      pg:disconnect()
      if not rs then return false, "query: " .. tostring(qerr), ms end
      return true, nil, ms
    end,
    probe_label = "pgmoon connect + SELECT 1",
  },
  nats = {
    container_name = "nats-js-ram",
    port           = 4222,
    probe          = function() return sh.probe_infra(4222, 2000) end,
    probe_label    = "TCP SYN to 4222",
  },
  mosquitto = {
    container_name = "mosquitto-ram-ws_main",
    port           = 1883,
    probe          = function() return sh.probe_infra(1883, 2000) end,
    probe_label    = "TCP SYN to 1883",
  },
  kv_bridge = {
    container_name = "kv-bridge",
    port           = 8080,
    probe          = function() return sh.probe_infra(8080, 2000) end,
    probe_label    = "TCP SYN to 8080",
  },
}

function M.build_body(service)
  local spec = SERVICES[service]
  if not spec then
    return string.format(
      '<h2>infra / %s</h2><p class="placeholder">Unknown service.</p>',
      sh.escape(service or "")),
      { title = "infra / " .. (service or "?") }
  end

  -- Probe first (may take up to 2s; admin's tick-budget is generous).
  local reachable, perr, ms = spec.probe()

  -- Meta from pg (best-effort; probe is the real signal).
  local pg = sh.pg_connect()
  local meta, exc_count
  if pg then
    meta      = sh.get_infra_container(pg, spec.container_name)
    exc_count = sh.active_exception_count(pg)
    pg:disconnect()
  end

  -- "connection refused" means the host routed the probe but nothing
  -- is listening on that port -- most commonly the container exists
  -- but its port isn't published to the host (common on dev laptops
  -- where install_infra.sh created the container without -p). That's
  -- distinct from a REAL outage (timeout, DNS fail, etc), so render
  -- it as WARN with a diagnostic hint rather than a full FAIL.
  local refused = perr and tostring(perr):lower():find("connection refused")
  local probe_pill, err_html
  if reachable then
    probe_pill = sh.pill("ok", "REACHABLE")
    err_html   = ""
  elseif refused then
    probe_pill = sh.pill("warn", "PORT NOT PUBLISHED")
    err_html = string.format(
      '<div style="color:#fc6;font-size:0.85em;margin-top:0.3em">' ..
      '%s. The container may still be up on its docker network but not ' ..
      'reachable from the host. Check <code>docker port %s</code>; fix by ' ..
      'recreating with <code>-p %d:%d</code> (laptop provisioning) or by ' ..
      'adding admin to the container\'s docker network.</div>',
      sh.escape(perr or "refused"),
      sh.escape(spec.container_name),
      spec.port, spec.port)
  else
    probe_pill = sh.pill("fail", "UNREACHABLE")
    err_html = string.format(
      '<div style="color:#f88;font-size:0.85em;margin-top:0.3em">%s</div>',
      sh.escape(perr or ""))
  end

  local latency_html = string.format('<span style="color:#aaa">%dms</span>', ms or 0)

  local path_span
  if meta and meta.cpu_id then
    path_span = sh.kb_path_span("cpu", meta.cpu_id, "container", spec.container_name)
  else
    path_span = '<span class="empty">not in KB</span>'
  end

  local parts = {
    string.format('<h2>infra / %s <span style="color:#888;font-weight:normal">[%s]</span></h2>',
                  sh.escape(service), sh.escape(spec.container_name)),
    '<dl class="status-list">',
    '<dt>KB path</dt><dd>', path_span, '</dd>',
    '<dt>Container</dt><dd>', sh.escape(spec.container_name), '</dd>',
  }
  if meta then
    table.insert(parts, '<dt>Kind</dt><dd>'       .. sh.escape(meta.kind or "")       .. '</dd>')
    table.insert(parts, '<dt>Managed by</dt><dd>' .. sh.escape(meta.managed_by or "") .. '</dd>')
    table.insert(parts, '<dt>On CPU</dt><dd>'     .. sh.escape(meta.cpu_id or "")     .. '</dd>')
  end
  table.insert(parts, '<dt>Probe</dt><dd>' .. sh.escape(spec.probe_label) ..
               ' &rarr; host.docker.internal:' .. tostring(spec.port) .. '</dd>')
  table.insert(parts, '<dt>Reachable</dt><dd>' .. probe_pill .. ' &middot; ' ..
               latency_html .. err_html .. '</dd>')
  table.insert(parts, '<dt>Probed</dt><dd>' .. sh.time_el(os.time(), 10) .. '</dd>')
  table.insert(parts, '</dl>')
  table.insert(parts,
    '<footer class="last-event">Infra containers are pre-placed by laptop ' ..
    'provisioning scripts; DCS only start/stops them. A REACHABLE result ' ..
    'means the service is listening on its port right now -- not a deep ' ..
    'healthcheck. For pgmoon the probe is a real SELECT 1; others are TCP SYN.</footer>')

  local ctx = {
    title             = "infra / " .. service,
    status_url        = "status/infra/" .. service,
    status_stream_url = "sse/infra/" .. service,
  }
  if exc_count and exc_count > 0 then ctx.badge = tostring(exc_count) end
  return table.concat(parts), ctx
end

function M.render(service)
  local html, ctx = M.build_body(service)
  ngx.header["Content-Type"] = "text/html; charset=utf-8"
  sh.set_context(ctx)
  ngx.say(html)
end

return M
