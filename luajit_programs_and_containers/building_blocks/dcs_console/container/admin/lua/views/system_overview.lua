-- views/system_overview.lua -- Phase 2 System/Overview fragment.
--
-- Passive-monitoring view (~90% of operator time): traffic-light
-- dashboard answering "is anything wrong?" at a glance. Every numeric
-- pulls from pg; stale timestamps render via the shell's client-side
-- ticker.
--
-- Exposes:
--   M.render()      -- one-shot fragment for GET /fragment/system/overview
--   M.build_body()  -- returns {html, ctx} so sse/system_overview can
--                      reuse the exact same rendering on each tick.

local sh = require("shell_helpers")

local M = {}

-- Build the HTML body + shell:context payload for the overview. Used
-- both by the one-shot fragment handler and by the SSE stream so a
-- popup-open viewer sees the same data that just painted the main area.
function M.build_body()
  local pg, err = sh.pg_connect()
  local html, ctx
  if not pg then
    html = string.format(
      '<h2>System Overview</h2>\n' ..
      '<p class="placeholder">pg unreachable: %s</p>',
      sh.escape(err or ""))
    -- Plain ASCII separator in header values (see placeholder.lua).
    ctx = { title = "System / Overview" }
    return html, ctx
  end

  -- Pull everything we need in compact SQL. We could batch into a
  -- single round-trip via a CTE; for phase 2, clarity over micro-opt.
  local system_ready = sh.site_status_value(pg, "system_ready")
  local cluster_go   = sh.site_status_value(pg, "cluster_go")
  local ready_bits   = sh.site_bit_mask(pg, "ready_bits")
  local sync_bits    = sh.site_bit_mask(pg, "cluster_sync_bits")
  local expected     = sh.expected_cpu_count(pg)
  local exc_count    = sh.active_exception_count(pg)
  pg:disconnect()

  local expected_mask
  if expected and expected < 64 then
    expected_mask = (2 ^ expected) - 1
  end

  -- Traffic-light helpers.
  local function bits_pill(actual)
    if not actual or not expected_mask then
      return sh.pill("unknown", "?")
    end
    if actual == expected_mask then
      return sh.pill("ok",   string.format("0x%x / 0x%x", actual, expected_mask))
    elseif actual == 0 then
      return sh.pill("fail", string.format("0x%x / 0x%x", actual, expected_mask))
    else
      return sh.pill("warn", string.format("0x%x / 0x%x", actual, expected_mask))
    end
  end

  local function bool_pill(value, ok_label, fail_label)
    if value == 1 then return sh.pill("ok",   ok_label)   end
    if value == 0 then return sh.pill("fail", fail_label) end
    return sh.pill("unknown", "?")
  end

  -- Count of ready bits set (popcount) for "N / M" display.
  local ready_count, expected_count = 0, expected or 0
  if ready_bits then
    local v = ready_bits
    while v > 0 do
      if v % 2 == 1 then ready_count = ready_count + 1 end
      v = math.floor(v / 2)
    end
  end
  local cpu_pill
  if expected_count == 0 then
    cpu_pill = sh.pill("unknown", "?")
  elseif ready_count == expected_count then
    cpu_pill = sh.pill("ok",   string.format("%d / %d", ready_count, expected_count))
  elseif ready_count == 0 then
    cpu_pill = sh.pill("fail", string.format("%d / %d", ready_count, expected_count))
  else
    cpu_pill = sh.pill("warn", string.format("%d / %d", ready_count, expected_count))
  end

  -- Exception count: 0 green, >0 warn (fail reserved for site-level
  -- outages). Click-through to the Exceptions/Active view.
  local exc_pill
  if exc_count == 0 then
    exc_pill = sh.pill("ok",   "0")
  else
    exc_pill = string.format(
      '%s <a href="#view=fragment/exception/active" style="color:#7fbfff">(view)</a>',
      sh.pill("warn", tostring(exc_count)))
  end

  -- Build the HTML body.
  local parts = {
    '<h2>System Overview</h2>',
    '<dl class="status-list">',
    '<dt>KB path</dt><dd>',        sh.kb_path_span(), '</dd>',
    '<dt>System ready</dt><dd>',   bool_pill(system_ready, "GREEN", "RED"), '</dd>',
    '<dt>Cluster go</dt><dd>',     bool_pill(cluster_go,   "GO",    "WAIT"), '</dd>',
    '<dt>CPUs ready</dt><dd>',     cpu_pill, '</dd>',
    '<dt>Ready bits</dt><dd>',     bits_pill(ready_bits), '</dd>',
    '<dt>Sync bits</dt><dd>',      bits_pill(sync_bits),  '</dd>',
    '<dt>Active exceptions</dt><dd>', exc_pill, '</dd>',
    '<dt>Updated</dt><dd>',        sh.time_el(os.time(), 30), '</dd>',
    '</dl>',
    '<footer class="last-event">Snapshot taken at ',
      sh.time_el(os.time(), 120),
    '. Open the status popup (<span aria-hidden="true">&#9432;</span>) for a live feed.</footer>',
  }
  html = table.concat(parts, "\n")

  ctx = {
    title             = "System / Overview",
    status_url        = "status/system/overview",
    status_stream_url = "sse/system/overview",
  }
  if exc_count > 0 then ctx.badge = tostring(exc_count) end
  return html, ctx
end

function M.render()
  local html, ctx = M.build_body()
  ngx.header["Content-Type"] = "text/html; charset=utf-8"
  sh.set_context(ctx)
  ngx.say(html)
end

return M
