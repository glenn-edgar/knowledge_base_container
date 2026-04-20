-- log_detail.lua -- view 7: single-log strip chart + rules.
--
-- URL: /detail?path=<KB_LOG ltree>[&window_s=N]
--
-- Layout:
--   top   -- header (log name, kind, unit, description, expected_hz)
--   left  -- strip chart via uPlot (raw samples OR rollup buckets depending
--            on window_s). time-range selector above the chart.
--   right -- live_stats snapshot (welford mean, stddev, MA, slope, cusum)
--   bottom- rules panel (kind, cooldown, fire_count, last_fired, enabled)

local cjson = require("cjson.safe")
local h = require("helpers")
local r = require("render")

local args    = ngx.req.get_uri_args() or {}
local path    = args.path
local window_s_req = tonumber(args.window_s)

if not path or path == "" then
  -- Index: list all KB_LOGs, link to each for selection.
  local pg, err = h.pg_connect()
  if not pg then r.error_page("Log Detail", "detail", h.escape(err or "(nil)")); return end
  local rows = h.list_logs(pg)
  pg:keepalive(60000, 8)

  local parts = {}
  parts[#parts + 1] = '<div class="panel"><h2>Select a log to inspect</h2>'
  if #rows == 0 then
    parts[#parts + 1] = '<p class="empty">No KB_LOG declared in the KB.</p>'
  else
    parts[#parts + 1] = '<table><thead><tr><th>Log</th><th>Kind</th><th>Unit</th><th>Description</th></tr></thead><tbody>'
    for _, lg in ipairs(rows) do
      local p = lg.properties or {}
      parts[#parts + 1] = string.format(
        '<tr><td><a href="/detail?path=%s">%s</a></td>' ..
        '<td><span class="kind-badge kind-%s">%s</span></td>' ..
        '<td><code>%s</code></td><td>%s</td></tr>',
        h.urlencode(lg.path), h.escape(h.short_log_path(lg.path)),
        h.escape(p.kind or "operational"), h.escape(p.kind or "operational"),
        h.escape(p.unit or ""), h.escape(p.description or ""))
    end
    parts[#parts + 1] = '</tbody></table>'
  end
  parts[#parts + 1] = '</div>'
  r.page("Log Detail", "detail", table.concat(parts, "\n"))
  return
end

---------------------------------------------------------------------------
-- Specific log selected
---------------------------------------------------------------------------

local pg, err = h.pg_connect()
if not pg then r.error_page("Log Detail", "detail", h.escape(err or "(nil)")); return end

local props = h.read_log_props(pg, path)
if not props then
  pg:keepalive(60000, 8)
  r.error_page("Log Detail", "detail", "no KB_LOG at path: " .. h.escape(path))
  return
end

local kind             = props.kind or "operational"
local default_window_s = tonumber(props.default_window_s) or 300
local window_s         = window_s_req or default_window_s
local expected_hz      = tonumber(props.expected_hz) or 1.0
local sample_cap       = tonumber(props.sample_cap) or 512

local live = h.read_live_stats(pg, path)
local rules = h.read_rules_for_log(pg, path)

-- Decide tier based on window_s + kind.
local raw_ring_covers_s = sample_cap / math.max(expected_hz, 0.001)
local xs, ys = {}, {}
local tier_used = "none"
local chart_data_rows = 0

if window_s <= raw_ring_covers_s and kind ~= "archival" then
  tier_used = "tier-0 (raw)"
  local rows = h.read_raw_samples(pg, path, math.floor(sample_cap))
  -- Samples returned newest-first; reverse for chronological.
  for i = #rows, 1, -1 do
    local r_ = rows[i]
    local d = r_.data or {}
    local ts_sample = tonumber(d.ts) or tonumber(r_.rec_epoch) or 0
    local val = tonumber(d.value)
    if ts_sample > 0 and val ~= nil then
      -- Only include samples within the requested window
      if (os.time() - ts_sample) <= window_s then
        xs[#xs + 1] = ts_sample
        ys[#ys + 1] = val
      end
    end
  end
  chart_data_rows = #xs
elseif window_s <= 86400 then
  tier_used = "tier-1 (1min buckets)"
  local rows = h.read_rollups(pg, path, "1min", window_s)
  for _, row in ipairs(rows) do
    local c = tonumber(row.count) or 0
    local s = tonumber(row.sum) or 0
    if c > 0 then
      xs[#xs + 1] = row.bucket_epoch
      ys[#ys + 1] = s / c   -- mean
    end
  end
  chart_data_rows = #xs
elseif window_s <= 7 * 86400 then
  tier_used = "tier-2 (1hour buckets)"
  local rows = h.read_rollups(pg, path, "1hour", window_s)
  for _, row in ipairs(rows) do
    local c = tonumber(row.count) or 0
    local s = tonumber(row.sum) or 0
    if c > 0 then
      xs[#xs + 1] = row.bucket_epoch
      ys[#ys + 1] = s / c
    end
  end
  chart_data_rows = #xs
else
  tier_used = "tier-3 (1day buckets)"
  local rows = h.read_rollups(pg, path, "1day", window_s)
  for _, row in ipairs(rows) do
    local c = tonumber(row.count) or 0
    local s = tonumber(row.sum) or 0
    if c > 0 then
      xs[#xs + 1] = row.bucket_epoch
      ys[#ys + 1] = s / c
    end
  end
  chart_data_rows = #xs
end

pg:keepalive(60000, 8)

---------------------------------------------------------------------------
-- Time range options (filtered by kind)
---------------------------------------------------------------------------

local ranges
if kind == "archival" then
  ranges = { { "30d", 30*86400 }, { "90d", 90*86400 }, { "1y", 365*86400 } }
elseif kind == "diagnostic" then
  ranges = { { "5m", 300 }, { "30m", 1800 } }
else
  ranges = { { "1m", 60 }, { "5m", 300 }, { "30m", 1800 },
             { "1h", 3600 }, { "24h", 86400 }, { "7d", 7*86400 }, { "30d", 30*86400 } }
end

---------------------------------------------------------------------------
-- Render
---------------------------------------------------------------------------

local name = path:match("KB_LOG%.(.+)$") or path
local parts = {}
local function emit(s) parts[#parts + 1] = s end

-- Header panel
emit(string.format(
  '<div class="panel"><h2>%s <span class="kind-badge kind-%s">%s</span></h2>',
  h.escape(name), h.escape(kind), h.escape(kind)))
emit('<dl class="kv" style="grid-template-columns: 150px 1fr;">')
emit(string.format('<dt>Path</dt><dd><code>%s</code></dd>', h.escape(path)))
emit(string.format('<dt>Description</dt><dd>%s</dd>', h.escape(props.description or "")))
emit(string.format('<dt>Unit</dt><dd><code>%s</code></dd>', h.escape(props.unit or "")))
emit(string.format('<dt>Sample cap</dt><dd>%s</dd>', tostring(sample_cap)))
emit(string.format('<dt>Expected Hz</dt><dd>%.3f</dd>', expected_hz))
emit(string.format('<dt>MA windows</dt><dd>short %ss · long %ss</dd>',
  tostring(props.ma_short_s or 60), tostring(props.ma_long_s or 900)))
emit(string.format('<dt>Auto-health</dt><dd>%s</dd>',
  (props.auto_health == false or props.auto_health == "false") and "OFF" or "ON"))
emit('</dl></div>')

-- Two-col: chart | live_stats
emit('<div class="two-col">')

-- LEFT: chart
emit('<div class="panel"><h3>Strip chart</h3>')
emit('<div class="time-range">')
for _, rng in ipairs(ranges) do
  local lbl, ws = rng[1], rng[2]
  local cls = (ws == window_s) and ' class="active"' or ""
  emit(string.format('<a href="/detail?path=%s&window_s=%d"%s>%s</a>',
    h.urlencode(path), ws, cls, h.escape(lbl)))
end
emit('</div>')
emit(string.format(
  '<p style="color:#888;font-size:0.85em;margin:0 0 0.5em">' ..
  'source: %s · %d points · window: %ds</p>',
  h.escape(tier_used), chart_data_rows, window_s))
emit('<div id="chart" style="width:100%;height:360px;"></div>')
emit('</div>')

-- RIGHT: live_stats snapshot
emit('<div class="panel"><h3>Live stats</h3>')
emit('<dl class="kv" style="grid-template-columns: 130px 1fr;">')
local w = live.welford or {}
local function k(label, val)
  emit(string.format('<dt>%s</dt><dd>%s</dd>', label, h.escape(tostring(val))))
end
k("welford.n",      tonumber(w.n) or 0)
k("welford.mean",   w.mean and string.format("%.4g", tonumber(w.mean) or 0) or "—")
k("welford.stddev", w.m2 and w.n and (w.n > 1) and string.format("%.4g", math.sqrt(w.m2/(w.n-1))) or "—")
k("ma_short",       live.ma_short and string.format("%.4g", tonumber(live.ma_short) or 0) or "—")
k("ma_long",        live.ma_long  and string.format("%.4g", tonumber(live.ma_long) or 0) or "—")
k("ewma",           live.ewma     and string.format("%.4g", tonumber(live.ewma) or 0) or "—")
local env = live.envelope or {}
k("envelope.max",    env.max and string.format("%.4g", tonumber(env.max) or 0) or "—")
k("envelope.min",    env.min and string.format("%.4g", tonumber(env.min) or 0) or "—")
k("envelope.midpoint", env.midpoint and string.format("%.4g", tonumber(env.midpoint) or 0) or "—")
local slope = live.slope or {}
k("slope.value",    slope.value and string.format("%.4g /s", tonumber(slope.value) or 0) or "—")
local cusum = live.cusum or {}
k("cusum.pos",      cusum.pos and string.format("%.4g", tonumber(cusum.pos) or 0) or "—")
k("cusum.neg",      cusum.neg and string.format("%.4g", tonumber(cusum.neg) or 0) or "—")
k("dv_dt",          live.dv_dt and string.format("%.4g", tonumber(live.dv_dt) or 0) or "—")
k("last_update",    h.fmt_age(live.last_update_ts))
emit('</dl></div>')
emit('</div>')  -- /two-col

-- Rules panel
emit(string.format('<div class="panel"><h3>Rules (%d)</h3>', #rules))
if #rules == 0 then
  emit('<p class="empty">No rules attached to this log.</p>')
else
  emit('<table><thead><tr>')
  emit('<th>Rule</th><th>Kind</th><th>Params</th><th>Target</th>')
  emit('<th>Fires</th><th>Last fired</th><th>Enabled</th><th>Suppressed</th>')
  emit('</tr></thead><tbody>')
  for _, rule in ipairs(rules) do
    local p = rule.props or {}
    local rule_name = rule.path:match("KB_RULE%.(.+)$") or rule.path
    local params = {}
    if p.value ~= nil then params[#params+1] = tostring(p.op or "") .. " " .. tostring(p.value) end
    if p.z_threshold then params[#params+1] = "z>=" .. tostring(p.z_threshold) end
    if p.slope_threshold then params[#params+1] = "slope>=" .. tostring(p.slope_threshold) end
    if p.gap_s then params[#params+1] = "gap>=" .. tostring(p.gap_s) .. "s" end
    if p.rate_count then params[#params+1] = p.rate_count .. "/" .. (p.rate_window_s or "") .. "s" end
    if p.abs_threshold then params[#params+1] = "|dV/dt|>=" .. tostring(p.abs_threshold) end
    if p.cusum_threshold then params[#params+1] = "cusum>=" .. tostring(p.cusum_threshold) end
    local enabled_txt = (rule.enabled == "true" or rule.enabled == true) and "✓" or "✗"
    local suppr_txt   = (rule.suppressed == "true" or rule.suppressed == true) and "✓" or "—"
    emit(string.format(
      '<tr><td><code>%s</code></td><td>%s</td><td><small>%s</small></td>' ..
      '<td><code>%s</code></td><td>%d</td>' ..
      '<td title="%s">%s</td><td>%s</td><td>%s</td></tr>',
      h.escape(rule_name),
      h.escape(p.kind or ""),
      h.escape(table.concat(params, " · ")),
      h.escape(p.target_exception or ""),
      tonumber(rule.fire_count) or 0,
      h.escape(h.fmt_ts(rule.last_fired_ts)),
      h.escape(h.fmt_age(rule.last_fired_ts)),
      enabled_txt, suppr_txt))
  end
  emit('</tbody></table>')
end
emit('<p style="color:#888;font-size:0.85em;margin-top:0.8em">')
emit('Phase 8d will add inline enable/suppress toggle buttons (POST /action).')
emit('</p></div>')

-- uPlot script (inline data, client renders on load)
emit('<script>')
emit('window.__CHART_XS = [' .. table.concat(xs, ",") .. '];')
emit('window.__CHART_YS = [' .. table.concat(ys, ",") .. '];')
emit(string.format('window.__CHART_LABEL = %s;', cjson.encode(name)))
emit(string.format('window.__CHART_UNIT = %s;',  cjson.encode(props.unit or "")))
emit([[
(function () {
  var xs = window.__CHART_XS || [];
  var ys = window.__CHART_YS || [];
  var el = document.getElementById("chart");
  if (!el) return;
  if (xs.length === 0) {
    el.innerHTML = '<div style="padding:3em;text-align:center;color:#888;font-style:italic">No samples in this window. Writers not wired yet? Check that kb_log.push_sample is being called.</div>';
    return;
  }
  var rect = el.getBoundingClientRect();
  var opts = {
    width:  Math.max(rect.width, 600),
    height: 360,
    title:  window.__CHART_LABEL + " (" + window.__CHART_UNIT + ")",
    series: [
      {},
      { label: window.__CHART_LABEL,
        stroke: "#6af", width: 1.5, points: { show: xs.length < 200 } }
    ],
    axes: [
      { stroke: "#888", grid: { stroke: "#2a323c" } },
      { stroke: "#888", grid: { stroke: "#2a323c" } }
    ]
  };
  new uPlot(opts, [xs, ys], el);
})();
]])
emit('</script>')

r.page("Log Detail · " .. name, "detail", table.concat(parts, "\n"), true)
