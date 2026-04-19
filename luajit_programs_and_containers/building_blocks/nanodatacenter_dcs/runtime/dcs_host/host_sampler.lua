-- =============================================================================
-- host_sampler.lua -- Lightweight readers for /proc, /sys, and cgroup files.
--
-- Returns plain Lua tables; no pg or chain-tree dependencies. The
-- node_monitor user functions wrap these and write the results to pg.
--
-- Design notes:
--   * All reads are single-file io.open + read("*a"). Cost is microseconds
--     per file. Suitable to call from a chain-tree user function without
--     blowing the tick budget.
--   * Stateless; deltas (CPU jiffies, diskstats, net) require the caller
--     to remember the previous sample and subtract.
--   * Cgroup paths are v2 (modern systemd + Docker on cgroupv2 hosts).
--     v1 path discovery is left for later; flag at boot if v1 detected.
-- =============================================================================

local M = {}

---------------------------------------------------------------------------
-- read(path) -> string|nil
---------------------------------------------------------------------------
local function read(path)
  local f = io.open(path, "r")
  if not f then return nil end
  local s = f:read("*a")
  f:close()
  return s
end

M.read = read

---------------------------------------------------------------------------
-- /proc/loadavg -> { load1, load5, load15, runnable, total, last_pid }
---------------------------------------------------------------------------
function M.loadavg()
  local s = read("/proc/loadavg")
  if not s then return nil end
  local l1, l5, l15, runr, lp = s:match(
    "([%d%.]+)%s+([%d%.]+)%s+([%d%.]+)%s+(%d+)/(%d+)%s+(%d+)")
  return {
    load1   = tonumber(l1)   or 0,
    load5   = tonumber(l5)   or 0,
    load15  = tonumber(l15)  or 0,
    runnable = tonumber(runr) or 0,
  }
end

---------------------------------------------------------------------------
-- /proc/meminfo -> { mem_total_kb, mem_available_kb, slab_kb, dirty_kb,
--                    swap_total_kb, swap_free_kb, ... } (kB units)
---------------------------------------------------------------------------
function M.meminfo()
  local s = read("/proc/meminfo")
  if not s then return nil end
  local out = {}
  for k, v in s:gmatch("([%w_%(%)]+):%s+(%d+)") do
    out[k] = tonumber(v)
  end
  return {
    mem_total_kb     = out.MemTotal     or 0,
    mem_available_kb = out.MemAvailable or 0,
    mem_free_kb      = out.MemFree      or 0,
    cached_kb        = out.Cached       or 0,
    buffers_kb       = out.Buffers      or 0,
    slab_kb          = out.Slab         or 0,
    dirty_kb         = out.Dirty        or 0,
    swap_total_kb    = out.SwapTotal    or 0,
    swap_free_kb     = out.SwapFree     or 0,
  }
end

---------------------------------------------------------------------------
-- /proc/stat aggregate cpu line
-- Returns { user, nice, system, idle, iowait, irq, softirq, steal, total }
-- in jiffies (cumulative since boot). Caller subtracts to get a delta.
---------------------------------------------------------------------------
function M.cpu_stat()
  local s = read("/proc/stat")
  if not s then return nil end
  local cpu = s:match("cpu%s+([^\n]+)")
  if not cpu then return nil end
  local fields = {}
  for n in cpu:gmatch("(%d+)") do fields[#fields + 1] = tonumber(n) end
  local function f(i) return fields[i] or 0 end
  local total = 0
  for i = 1, #fields do total = total + fields[i] end
  return {
    user    = f(1),
    nice    = f(2),
    system  = f(3),
    idle    = f(4),
    iowait  = f(5),
    irq     = f(6),
    softirq = f(7),
    steal   = f(8),
    total   = total,
  }
end

---------------------------------------------------------------------------
-- Compute % busy from two cpu_stat() snapshots taken Δt apart.
-- Returns { cpu_pct, iowait_pct } both 0..100.
---------------------------------------------------------------------------
function M.cpu_pct(prev, curr)
  if not prev or not curr then return nil end
  local d_total = curr.total - prev.total
  if d_total <= 0 then return { cpu_pct = 0, iowait_pct = 0 } end
  local d_idle   = (curr.idle - prev.idle) + (curr.iowait - prev.iowait)
  local d_iowait = curr.iowait - prev.iowait
  return {
    cpu_pct    = 100 * (d_total - d_idle) / d_total,
    iowait_pct = 100 * d_iowait / d_total,
  }
end

---------------------------------------------------------------------------
-- /proc/<pid>/status -> { rss_kb, vmsize_kb, threads }
---------------------------------------------------------------------------
function M.proc_status(pid)
  local s = read("/proc/" .. tostring(pid) .. "/status")
  if not s then return nil end
  return {
    rss_kb    = tonumber(s:match("VmRSS:%s+(%d+)"))   or 0,
    vmsize_kb = tonumber(s:match("VmSize:%s+(%d+)"))  or 0,
    threads   = tonumber(s:match("Threads:%s+(%d+)")) or 0,
  }
end

---------------------------------------------------------------------------
-- /proc/<pid>/stat -> { utime, stime, cpu_jiffies }
-- (cumulative; caller subtracts)
---------------------------------------------------------------------------
function M.proc_stat(pid)
  local s = read("/proc/" .. tostring(pid) .. "/stat")
  if not s then return nil end
  -- Skip past comm field which can contain spaces/parens.
  local rest = s:match("%)%s+(.+)$")
  if not rest then return nil end
  local fields = {}
  for tok in rest:gmatch("(%S+)") do fields[#fields + 1] = tok end
  -- field 14 (utime) and 15 (stime) — but rest started after field 2
  -- (comm), so utime is index 12, stime is index 13.
  local utime = tonumber(fields[12]) or 0
  local stime = tonumber(fields[13]) or 0
  return {
    utime        = utime,
    stime        = stime,
    cpu_jiffies  = utime + stime,
  }
end

---------------------------------------------------------------------------
-- /proc/<pid>/io -> { read_bytes, write_bytes, rchar, wchar }
---------------------------------------------------------------------------
function M.proc_io(pid)
  local s = read("/proc/" .. tostring(pid) .. "/io")
  if not s then return nil end
  return {
    read_bytes  = tonumber(s:match("read_bytes:%s+(%d+)"))  or 0,
    write_bytes = tonumber(s:match("write_bytes:%s+(%d+)")) or 0,
    rchar       = tonumber(s:match("rchar:%s+(%d+)"))       or 0,
    wchar       = tonumber(s:match("wchar:%s+(%d+)"))       or 0,
  }
end

---------------------------------------------------------------------------
-- /proc/diskstats -> { [dev_name] = { reads, read_sectors, writes,
--                                     write_sectors, ios_in_progress } }
-- All cumulative since boot. Caller deltas.
-- Filters out loop and ram devices; keeps anything that looks like a
-- real disk (sd*, nvme*, mmcblk*, vd*, xvd*).
---------------------------------------------------------------------------
local function _is_real_disk(name)
  return name:match("^sd[a-z]+$")
      or name:match("^nvme%d+n%d+$")
      or name:match("^mmcblk%d+$")
      or name:match("^vd[a-z]+$")
      or name:match("^xvd[a-z]+$")
end

function M.diskstats()
  local s = read("/proc/diskstats")
  if not s then return nil end
  local out = {}
  for line in s:gmatch("[^\n]+") do
    -- Format: maj min name reads merges read_sectors read_ms writes
    --         merges write_sectors write_ms ios_in_progress io_ms ...
    local fields = {}
    for tok in line:gmatch("(%S+)") do fields[#fields + 1] = tok end
    local name = fields[3]
    if name and _is_real_disk(name) then
      out[name] = {
        reads          = tonumber(fields[4])  or 0,
        read_sectors   = tonumber(fields[6])  or 0,
        writes         = tonumber(fields[8])  or 0,
        write_sectors  = tonumber(fields[10]) or 0,
        ios_in_flight  = tonumber(fields[12]) or 0,
      }
    end
  end
  return out
end

---------------------------------------------------------------------------
-- statvfs-style disk free for a given mount path. Implemented via
-- `df --output=source,size,used,avail -B1 <path>` (cheap) so we don't
-- need an FFI binding to statvfs(). Returns
--   { source, size_bytes, used_bytes, avail_bytes, used_pct }
-- or nil on failure.
---------------------------------------------------------------------------
function M.df(path)
  local p = io.popen(string.format(
    "df --output=source,size,used,avail -B1 %q 2>/dev/null", path))
  if not p then return nil end
  local hdr = p:read("*l")  -- discard header
  local row = p:read("*l")
  p:close()
  if not row then return nil end
  local source, size, used, avail = row:match(
    "(%S+)%s+(%d+)%s+(%d+)%s+(%d+)")
  if not size then return nil end
  size  = tonumber(size)
  used  = tonumber(used)
  avail = tonumber(avail)
  return {
    source      = source,
    size_bytes  = size,
    used_bytes  = used,
    avail_bytes = avail,
    used_pct    = (size > 0) and (100 * used / size) or 0,
  }
end

---------------------------------------------------------------------------
-- /proc/net/dev -> { [iface] = { rx_bytes, rx_packets, rx_errors,
--                                tx_bytes, tx_packets, tx_errors } }
-- Cumulative; caller deltas. Skips lo and docker bridges by default.
---------------------------------------------------------------------------
local function _skip_iface(name)
  return name == "lo"
      or name:match("^docker")
      or name:match("^br%-")
      or name:match("^veth")
end

function M.net_dev()
  local s = read("/proc/net/dev")
  if not s then return nil end
  local out = {}
  -- Skip the two header lines.
  for line in s:gmatch("[^\n]+") do
    local iface, rest = line:match("^%s*([%w%-_%.@]+):%s*(.+)$")
    if iface and not _skip_iface(iface) then
      local f = {}
      for tok in rest:gmatch("(%d+)") do f[#f + 1] = tonumber(tok) end
      out[iface] = {
        rx_bytes   = f[1] or 0,
        rx_packets = f[2] or 0,
        rx_errors  = f[3] or 0,
        tx_bytes   = f[9] or 0,
        tx_packets = f[10] or 0,
        tx_errors  = f[11] or 0,
      }
    end
  end
  return out
end

---------------------------------------------------------------------------
-- CPU temperature. Walks /sys/class/thermal/thermal_zone* and returns
-- the highest reading in millidegrees C. nil if no zones found.
---------------------------------------------------------------------------
function M.cpu_temperature_mC()
  local max
  for i = 0, 15 do
    local t = read(string.format("/sys/class/thermal/thermal_zone%d/temp", i))
    if not t then break end
    local n = tonumber(t)
    if n and (not max or n > max) then max = n end
  end
  return max
end

---------------------------------------------------------------------------
-- Cgroup path discovery for a docker container by name.
--
-- Strategy: ask docker for the container's host pid, then read
-- /proc/<pid>/cgroup to learn the cgroup path the kernel actually
-- assigned. Works across docker cgroup drivers (systemd vs cgroupfs)
-- and across cgroup v1 vs v2 hosts; the cgroup line in /proc tells you.
--
-- Returns full /sys/fs/cgroup/<...> dir or nil.
---------------------------------------------------------------------------
function M.cgroup_path(container_name)
  -- Single docker inspect for the container's host pid.
  local p = io.popen(string.format(
    "docker inspect --format '{{.State.Pid}}' %q 2>/dev/null",
    container_name))
  if not p then return nil end
  local pid_s = p:read("*l")
  p:close()
  local pid = tonumber(pid_s or "")
  if not pid or pid <= 0 then return nil end

  -- Read the cgroup line from /proc/<pid>/cgroup. Format:
  --   v2:  0::/system.slice/docker-<id>.scope
  --   v1:  12:memory:/docker/<id>     (one line per controller)
  local cg = read("/proc/" .. pid .. "/cgroup")
  if not cg then return nil end

  -- Prefer the unified v2 line (starts with "0::").
  local v2 = cg:match("0::(%S+)")
  if v2 then
    local path = "/sys/fs/cgroup" .. v2
    if read(path .. "/memory.current") then return path end
  end

  -- Fall back: v1 memory controller path.
  local v1 = cg:match(":memory:(%S+)")
  if v1 then
    local path = "/sys/fs/cgroup/memory" .. v1
    if read(path .. "/memory.usage_in_bytes") then return path end
  end

  return nil
end

---------------------------------------------------------------------------
-- Sample one container's cgroup. Path is what cgroup_path returned.
-- Returns { mem_current_bytes, cpu_usec, io_rbytes, io_wbytes }.
-- All cumulative from cgroup birth — caller deltas as needed.
---------------------------------------------------------------------------
function M.cgroup_sample(path)
  if not path then return nil end

  -- v2 first
  local mem = tonumber(read(path .. "/memory.current") or "")
  local cpu_usec
  local cs = read(path .. "/cpu.stat")
  if cs then
    cpu_usec = tonumber(cs:match("usage_usec%s+(%d+)"))
  end
  local rbytes, wbytes = 0, 0
  local io_stat = read(path .. "/io.stat")
  if io_stat then
    for line in io_stat:gmatch("[^\n]+") do
      rbytes = rbytes + (tonumber(line:match("rbytes=(%d+)") or "0") or 0)
      wbytes = wbytes + (tonumber(line:match("wbytes=(%d+)") or "0") or 0)
    end
  end

  -- v1 fallback if v2 files weren't there
  if not mem then
    mem = tonumber(read(path .. "/memory.usage_in_bytes") or "")
  end
  if not cpu_usec then
    -- v1: cpuacct.usage in nanoseconds; convert to usec
    local ns = tonumber(read(path .. "/cpuacct.usage") or "")
    if ns then cpu_usec = math.floor(ns / 1000) end
  end

  return {
    mem_current_bytes = mem      or 0,
    cpu_usec          = cpu_usec or 0,
    io_rbytes         = rbytes,
    io_wbytes         = wbytes,
  }
end

---------------------------------------------------------------------------
-- Welford running mean/variance update.
-- state = { n, mean, m2 } (m2 = sum of squared deltas; variance = m2/(n-1))
-- Returns updated state in place.
---------------------------------------------------------------------------
function M.welford_update(state, x)
  state.n    = (state.n or 0) + 1
  local d    = x - (state.mean or 0)
  state.mean = (state.mean or 0) + d / state.n
  state.m2   = (state.m2 or 0) + d * (x - state.mean)
  return state
end

function M.welford_variance(state)
  if not state or (state.n or 0) < 2 then return 0 end
  return state.m2 / (state.n - 1)
end

---------------------------------------------------------------------------
-- Linear regression slope of points (x_i, y_i). Returns slope per
-- unit of x. Used for trend / leak detection: x = seconds since
-- window start, y = metric value, slope = units per second.
---------------------------------------------------------------------------
function M.linreg_slope(points)
  local n = #points
  if n < 2 then return 0 end
  local sx, sy, sxx, sxy = 0, 0, 0, 0
  for i = 1, n do
    local x, y = points[i][1], points[i][2]
    sx  = sx  + x
    sy  = sy  + y
    sxx = sxx + x * x
    sxy = sxy + x * y
  end
  local denom = n * sxx - sx * sx
  if denom == 0 then return 0 end
  return (n * sxy - sx * sy) / denom
end

return M
