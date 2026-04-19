-- =============================================================================
-- bit_mask_helpers.lua -- Direct SQL helpers for `bit_mask_table`.
--
-- The bit_mask_table schema is `(node_id PRIMARY KEY, bit_mask BIGINT)`.
-- node_id is derived from a path by flattening (dots -> underscores,
-- lowercased) -- matches construct_bit_mask_store.lua's convention.
--
-- Two distinct uses we wrap here:
--   * Per-CPU heartbeat: `bit_mask` column holds a 64-bit nanosecond
--     timestamp. Repurposed-as-raw-value, not a flag register.
--   * Site-level ready_bits: bit per CPU; bit_index assigned in topology.
--
-- All operations take a DBI conn (autocommit on) and return (ok, err).
-- =============================================================================

local M = {}
local ptime = require("posix_time")

---------------------------------------------------------------------------
-- helpers
---------------------------------------------------------------------------

local function flatten_path(path_str)
  -- "system.site.X.cpu.Y.KB_BIT_MASK.heartbeat" -> "system_site_x_cpu_y_..."
  return (path_str:gsub("%.", "_")):lower()
end

local function exec(conn, sql)
  local sth, err = conn:prepare(sql)
  if not sth then return nil, "prepare: " .. tostring(err) .. "  sql=" .. sql end
  local ok, eerr = sth:execute()
  if not ok then return nil, "execute: " .. tostring(eerr) .. "  sql=" .. sql end
  sth:close()
  return true
end

local function query_one(conn, sql)
  local sth, err = conn:prepare(sql)
  if not sth then return nil, "prepare: " .. tostring(err) end
  local ok, eerr = sth:execute()
  if not ok then sth:close(); return nil, "execute: " .. tostring(eerr) end
  local row = sth:fetch(true)
  sth:close()
  return row
end

local function escape(s) return tostring(s):gsub("'", "''") end

---------------------------------------------------------------------------
-- now() in nanoseconds, WALL-CLOCK Unix epoch (not monotonic). Heartbeat
-- timestamps stored in bit_mask_table are read by other processes
-- (admin UI, master-side aggregators), which have their own monotonic
-- clocks starting at their process start -- so a monotonic value is
-- meaningless cross-process. Wall-clock is the only value that round-
-- trips cleanly through pg. Seconds precision is sufficient for the
-- 5-to-15s heartbeat cadence.
---------------------------------------------------------------------------

function M.now_ns()
  return os.time() * 1000000000
end

---------------------------------------------------------------------------
-- node_id derivation
---------------------------------------------------------------------------

function M.heartbeat_node_id(site, cpu_id)
  -- matches construct: "system.site.<S>.cpu.<id>.KB_BIT_MASK.heartbeat"
  return flatten_path(string.format(
    "system.site.%s.cpu.%s.KB_BIT_MASK.heartbeat", site, cpu_id))
end

function M.ready_bits_node_id(site)
  -- matches construct: "system.site.<S>.KB_BIT_MASK.ready_bits"
  return flatten_path(string.format(
    "system.site.%s.KB_BIT_MASK.ready_bits", site))
end

function M.cluster_sync_bits_node_id(site)
  -- matches construct: "system.site.<S>.KB_BIT_MASK.cluster_sync_bits"
  -- Used by sync_control KB: each CPU sets its own bit once it has
  -- verified all 4 infra services are reachable. Master waits for
  -- the full mask before flipping cluster_go.
  return flatten_path(string.format(
    "system.site.%s.KB_BIT_MASK.cluster_sync_bits", site))
end

---------------------------------------------------------------------------
-- heartbeat: 64-bit timestamp in bit_mask column
---------------------------------------------------------------------------

function M.write_heartbeat_ts(conn, site, cpu_id, ts_ns)
  ts_ns = ts_ns or M.now_ns()
  local node_id = M.heartbeat_node_id(site, cpu_id)
  return exec(conn, string.format(
    "UPDATE bit_mask_table SET bit_mask = %d WHERE node_id = '%s'",
    ts_ns, escape(node_id)))
end

function M.read_heartbeat_ts(conn, site, cpu_id)
  local node_id = M.heartbeat_node_id(site, cpu_id)
  local row, err = query_one(conn, string.format(
    "SELECT bit_mask FROM bit_mask_table WHERE node_id = '%s'",
    escape(node_id)))
  if not row then return nil, err or "row not found" end
  return tonumber(row.bit_mask)
end

---------------------------------------------------------------------------
-- ready_bits: site-level mask, per-CPU bit ownership
---------------------------------------------------------------------------

function M.read_ready_bits(conn, site)
  local node_id = M.ready_bits_node_id(site)
  local row, err = query_one(conn, string.format(
    "SELECT bit_mask FROM bit_mask_table WHERE node_id = '%s'",
    escape(node_id)))
  if not row then return nil, err or "row not found" end
  return tonumber(row.bit_mask)
end

-- atomic OR / AND-NOT to avoid read-modify-write races
function M.set_ready_bit(conn, site, bit_index)
  local node_id = M.ready_bits_node_id(site)
  return exec(conn, string.format(
    "UPDATE bit_mask_table SET bit_mask = bit_mask | (1::bigint << %d) " ..
    "WHERE node_id = '%s'",
    bit_index, escape(node_id)))
end

function M.clear_ready_bit(conn, site, bit_index)
  local node_id = M.ready_bits_node_id(site)
  return exec(conn, string.format(
    "UPDATE bit_mask_table SET bit_mask = bit_mask & ~((1::bigint << %d)) " ..
    "WHERE node_id = '%s'",
    bit_index, escape(node_id)))
end

function M.expected_full_mask(cpu_count)
  -- (1 << N) - 1 in 64-bit; safe for N up to 63
  if cpu_count >= 64 then error("cpu_count >= 64 unsupported") end
  return (2 ^ cpu_count) - 1
end

---------------------------------------------------------------------------
-- cluster_sync_bits: parallel to ready_bits but written DURING sync_control
-- (before operational phase). Master flips cluster_go once this mask is
-- full. Cleared explicitly at teardown so a subsequent restart re-enters
-- sync cleanly.
---------------------------------------------------------------------------

function M.read_cluster_sync_bits(conn, site)
  local node_id = M.cluster_sync_bits_node_id(site)
  local row, err = query_one(conn, string.format(
    "SELECT bit_mask FROM bit_mask_table WHERE node_id = '%s'",
    escape(node_id)))
  if not row then return nil, err or "row not found" end
  return tonumber(row.bit_mask)
end

function M.set_cluster_sync_bit(conn, site, bit_index)
  local node_id = M.cluster_sync_bits_node_id(site)
  return exec(conn, string.format(
    "UPDATE bit_mask_table SET bit_mask = bit_mask | (1::bigint << %d) " ..
    "WHERE node_id = '%s'",
    bit_index, escape(node_id)))
end

function M.clear_cluster_sync_bit(conn, site, bit_index)
  local node_id = M.cluster_sync_bits_node_id(site)
  return exec(conn, string.format(
    "UPDATE bit_mask_table SET bit_mask = bit_mask & ~((1::bigint << %d)) " ..
    "WHERE node_id = '%s'",
    bit_index, escape(node_id)))
end

-- Master only: zero the full mask at teardown so a restart re-enters sync
-- cleanly. Safe to call from any CPU; only master does.
function M.clear_all_cluster_sync_bits(conn, site)
  local node_id = M.cluster_sync_bits_node_id(site)
  return exec(conn, string.format(
    "UPDATE bit_mask_table SET bit_mask = 0 WHERE node_id = '%s'",
    escape(node_id)))
end

return M
