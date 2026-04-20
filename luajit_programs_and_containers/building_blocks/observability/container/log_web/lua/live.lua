-- live.lua -- view 6: Live Operational.
--
-- Grid of all kind='operational' logs, one card each. For an at-a-glance
-- fleet view. Each card shows: name, scope, last_value + unit, last_sample
-- age, ma_short, slope arrow, sample count. Click card → /detail.
--
-- v1: no mini-charts embedded. Rendering 60+ uPlot instances on page load
-- is noticeable overhead; charts live in view 7 (Log Detail).

local h = require("helpers")
local r = require("render")

local pg, err = h.pg_connect()
if not pg then r.error_page("Live Operational", "live", h.escape(err or "(nil)")); return end

local rows = h.list_logs_with_summary(pg, "operational")
pg:keepalive(60000, 8)

---------------------------------------------------------------------------
-- Render
---------------------------------------------------------------------------

local parts = {}
local function emit(s) parts[#parts + 1] = s end

emit(string.format('<div class="panel"><h2>Live operational (%d logs)</h2>', #rows))
emit('<p style="color:#888;font-size:0.85em;margin-top:0">')
emit('Current value + MA from <code>live_stats</code>; click a card for the strip chart.</p>')

if #rows == 0 then
  emit('<p class="empty">No operational-kind KB_LOGs declared.</p>')
else
  -- CSS grid of cards. ~4 columns on wide screens.
  emit('<style>')
  emit([[
    .ops-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(280px, 1fr)); gap: 0.6em; }
    .ops-card { background: #0f1317; border: 1px solid var(--panel-border);
                 border-radius: 4px; padding: 0.7em 0.9em; text-decoration: none; color: inherit; }
    .ops-card:hover { border-color: var(--accent); }
    .ops-card .title { font-weight: 600; color: var(--fg); font-size: 0.92em; margin-bottom: 0.2em; }
    .ops-card .scope { color: var(--muted); font-size: 0.78em; margin-bottom: 0.5em; }
    .ops-card .value { font-size: 1.5em; color: var(--accent); font-weight: 700; }
    .ops-card .unit  { font-size: 0.7em; color: var(--muted); margin-left: 0.3em; }
    .ops-card .meta  { margin-top: 0.5em; font-size: 0.75em; color: var(--muted); display: flex; justify-content: space-between; }
    .ops-card.stale  { border-left: 3px solid var(--warn); }
    .ops-card.dead   { border-left: 3px solid var(--err); opacity: 0.75; }
    .ops-card .slope-up   { color: var(--ok); }
    .ops-card .slope-down { color: var(--err); }
  ]])
  emit('</style>')

  emit('<div class="ops-grid">')
  for _, row in ipairs(rows) do
    local props = row.props or {}
    local unit = props.unit or ""
    local live = row.live_stats or {}
    local name = row.path:match("KB_LOG%.(.+)$") or row.path
    local last_ts = tonumber(row.last_sample_ts) or 0
    local value = row.last_value or ""
    local slope = tonumber((live.slope or {}).value) or 0
    local ma_short = tonumber(live.ma_short) or 0
    local ma_long  = tonumber(live.ma_long)  or 0

    local age = (last_ts > 0) and (os.time() - last_ts) or 999999
    local class = "ops-card"
    if last_ts == 0 then
      class = class .. " dead"
    elseif age > 120 then
      class = class .. " stale"
    end

    local slope_arrow = ""
    if math.abs(slope) > 0.001 then
      slope_arrow = slope > 0
        and string.format(' <span class="slope-up">&uarr; %.3g/s</span>', slope)
        or  string.format(' <span class="slope-down">&darr; %.3g/s</span>', slope)
    end

    emit(string.format(
      '<a class="%s" href="' .. h.mk_url("/detail") .. '?path=%s">' ..
      '<div class="title">%s</div>' ..
      '<div class="scope">%s</div>' ..
      '<div class="value">%s<span class="unit">%s</span></div>' ..
      '<div class="meta">' ..
      '<span>MA %.3g / %.3g</span>' ..
      '<span>%s%s</span>' ..
      '</div></a>',
      class, h.urlencode(row.path),
      h.escape(name),
      h.escape(h.short_log_path(row.path):gsub(" / " .. name .. "$", "") or ""),
      (value ~= "") and h.escape(tostring(value)) or "—",
      h.escape(unit),
      ma_short, ma_long,
      h.escape(h.fmt_age(last_ts)), slope_arrow))
  end
  emit('</div>')
end
emit('</div>')

r.page("Live Operational", "live", table.concat(parts, "\n"))
