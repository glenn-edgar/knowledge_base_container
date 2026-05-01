-- views/shell_page.lua -- Server-renders the full admin shell HTML
-- with a pg-driven tree. Invoked directly via content_by_lua_file on
-- GET /, so this file executes at load time (no module exports).
--
-- Every full-page load re-queries pg for the current CPU roster +
-- CONTAINER_REGISTRY state. Everything after page-load is htmx
-- fragment-swapping, so this is exactly one short-lived pg
-- connection per visit. Refresh the page to pick up new containers.
--
-- On pg failure: renders the shell anyway with a stub tree + keeps
-- the content-area placeholder. Operator can still navigate; the
-- first fragment fetch will surface a more useful error.

local sh = require("shell_helpers")

local function leaf(frag, label)
  return string.format(
    '<li><a href="#" data-fragment="%s">%s</a></li>',
    sh.escape(frag), sh.escape(label))
end

local function branch(summary, children_html, open)
  return string.format(
    '<li><details%s><summary>%s</summary><ul>%s</ul></details></li>',
    open and " open" or "", sh.escape(summary), children_html)
end

local function system_branch()
  return branch("System",
    leaf("fragment/system/overview",   "Overview") ..
    leaf("fragment/system/ready_bits", "Ready bits") ..
    leaf("fragment/system/setpoints",  "Setpoints"),
    true)
end

local function cpus_branch(cpus)
  if not cpus or #cpus == 0 then
    return branch("CPUs",
      '<li class="empty" style="color:#666;padding:0.4em 1em;">No CPUs registered.</li>',
      false)
  end
  local parts = {}
  for _, c in ipairs(cpus) do
    -- Primary label: hostname (role) [cpu_id]. Hostname is the
    -- operator-meaningful name on real hardware; cpu_id is kept in
    -- brackets so dev deployments where multiple CPUs share a host
    -- (e.g. localhost) stay navigable.
    local hostname = c.hostname or "(no hostname)"
    local role     = c.role     or (c.is_master and "master" or "slave")
    local label    = string.format("%s (%s) [%s]", hostname, role, c.cpu_id)
    local children = leaf("fragment/cpu/" .. c.cpu_id .. "/summary",     "Summary") ..
                     leaf("fragment/cpu/" .. c.cpu_id .. "/heartbeat",   "Heartbeat") ..
                     leaf("fragment/cpu/" .. c.cpu_id .. "/assignments", "Assignments") ..
                     leaf("fragment/cpu/" .. c.cpu_id .. "/resources",   "Resources")
    parts[#parts + 1] = branch(label, children, false)
  end
  return branch("CPUs", table.concat(parts), false)
end

local function containers_branch(containers, cpus)
  if not containers or #containers == 0 then
    return branch("Containers",
      '<li class="empty" style="color:#666;padding:0.4em 1em;">No containers registered.</li>',
      false)
  end
  -- Build {cpu_id -> hostname} from the cpus list so container groups
  -- match the CPUs-branch labelling (hostname primary, cpu_id in brackets).
  local host_of = {}
  for _, c in ipairs(cpus or {}) do host_of[c.cpu_id] = c.hostname end

  local by_cpu = {}
  for _, c in ipairs(containers) do
    local cpu = c.cpu_id or "(unknown)"
    by_cpu[cpu] = by_cpu[cpu] or {}
    table.insert(by_cpu[cpu], c)
  end
  local cpu_keys = {}
  for k in pairs(by_cpu) do cpu_keys[#cpu_keys + 1] = k end
  table.sort(cpu_keys)
  local parts = {}
  for _, cpu in ipairs(cpu_keys) do
    local inner = {}
    for _, c in ipairs(by_cpu[cpu]) do
      if c.registered then
        inner[#inner + 1] = leaf("fragment/container/" .. c.name .. "/status", c.name)
      else
        -- In maintenance (deregistered but still placed). Show it
        -- dimmed with a small indicator so operator can click through
        -- to the maintenance panel and bring it back.
        inner[#inner + 1] = string.format(
          '<li><a href="#" data-fragment="fragment/container/%s/status" ' ..
          'style="color:#888" title="paused for maintenance">' ..
          '%s <span style="color:#fc6;font-size:0.8em">&#128274;</span></a></li>',
          sh.escape(c.name), sh.escape(c.name))
      end
    end
    local hostname = host_of[cpu] or "(no hostname)"
    local group_label = string.format("%s [%s]", hostname, cpu)
    parts[#parts + 1] = branch(group_label, table.concat(inner), false)
  end
  return branch("Containers", table.concat(parts), false)
end

local function exceptions_branch()
  return branch("Exceptions",
    leaf("fragment/exception/active",       "Active") ..
    leaf("fragment/exception/acknowledged", "Acknowledged") ..
    leaf("fragment/exception/history",      "History"),
    false)
end

local function infra_branch()
  return branch("Infra",
    leaf("fragment/infra/postgres",  "postgres") ..
    leaf("fragment/infra/nats",      "nats") ..
    leaf("fragment/infra/mosquitto", "mosquitto") ..
    leaf("fragment/infra/kv_bridge", "kv_bridge"),
    false)
end

-- --------------- render ---------------

local pg = sh.pg_connect()
local cpus, containers
if pg then
  cpus       = sh.list_cpus(pg) or {}
  containers = sh.list_containers(pg) or {}
  pg:disconnect()
end

ngx.header["Content-Type"]  = "text/html; charset=utf-8"
ngx.header["Cache-Control"] = "no-store"

ngx.say([[<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="dcs-default-view" content="fragment/system/overview">
<title>DCS admin</title>
<link rel="stylesheet" href="assets/dcs_shell/shell.css">
<script src="assets/htmx/htmx.min.js" defer></script>
<script src="assets/htmx/sse.js" defer></script>
</head>
<body>
<header id="shell-header">
  <button id="menu-btn" type="button" aria-label="Menu">&#9776;</button>
  <h1 id="shell-title">DCS admin</h1>
  <span id="operator-name" title="Operator identity"></span>
  <button id="refresh-btn" type="button" aria-label="Refresh current view" title="Refresh current view">&#8635;</button>
  <button id="alarm-btn" type="button" aria-label="Toggle audio alarm">&#128277;</button>
  <button id="status-btn" type="button" aria-label="Status" disabled style="opacity:0.4">
    &#9432;<span id="status-badge" hidden></span>
  </button>
</header>
<div id="shell-scrim"></div>
<nav id="shell-drawer" aria-label="Navigation">
<ul class="tree">]])

ngx.say(system_branch())
ngx.say(cpus_branch(cpus))
ngx.say(containers_branch(containers, cpus))
ngx.say(exceptions_branch())
ngx.say(infra_branch())

ngx.say([[</ul>
</nav>
<main id="shell-content">
  <p class="placeholder">Open the menu (&#9776;) and pick a view.</p>
</main>
<dialog id="status-popup">
  <div id="status-popup-header">
    <span id="status-popup-title">Status</span>
    <button id="status-popup-close" type="button" aria-label="Close">&times;</button>
  </div>
  <div id="status-body"></div>
</dialog>
<script src="assets/dcs_shell/shell.js" defer></script>
</body>
</html>]])
