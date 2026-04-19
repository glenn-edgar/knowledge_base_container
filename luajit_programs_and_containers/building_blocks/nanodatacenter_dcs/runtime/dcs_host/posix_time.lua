-- =============================================================================
-- posix_time.lua -- CLOCK_MONOTONIC + nanosleep helpers via FFI.
--
-- Drift-free pacing for the host-process tick loop. Avoids forking
-- `sh -c sleep` on every iteration and lets us set tick intervals
-- precisely from ctx.settings.tick_interval_s.
--
-- API:
--   M.now_sec()            -> number      monotonic seconds (double)
--   M.sleep_for(sec)       -> nil         sleep for `sec` seconds
--   M.sleep_until(deadline)-> nil         sleep until M.now_sec() >= deadline
--
-- Uses CLOCK_MONOTONIC (id=1) so wall-clock adjustments don't perturb us.
-- Handles EINTR by looping nanosleep with the remaining timespec.
-- =============================================================================

local ffi = require("ffi")

pcall(ffi.cdef, [[
  typedef struct { long tv_sec; long tv_nsec; } dcs_timespec_t;
  int clock_gettime(int clk_id, dcs_timespec_t *tp);
  int nanosleep(const dcs_timespec_t *req, dcs_timespec_t *rem);
]])

local C = ffi.C
local CLOCK_MONOTONIC = 1

local M = {}

local ts_now = ffi.new("dcs_timespec_t")

function M.now_sec()
  C.clock_gettime(CLOCK_MONOTONIC, ts_now)
  return tonumber(ts_now.tv_sec) + tonumber(ts_now.tv_nsec) * 1e-9
end

local req = ffi.new("dcs_timespec_t")
local rem = ffi.new("dcs_timespec_t")

function M.sleep_for(sec)
  if not sec or sec <= 0 then return end
  local whole = math.floor(sec)
  req.tv_sec  = whole
  req.tv_nsec = math.floor((sec - whole) * 1e9)
  while C.nanosleep(req, rem) ~= 0 do
    -- EINTR: continue with remaining
    req.tv_sec, req.tv_nsec = rem.tv_sec, rem.tv_nsec
  end
end

function M.sleep_until(deadline_sec)
  local remaining = deadline_sec - M.now_sec()
  if remaining > 0 then M.sleep_for(remaining) end
end

return M
