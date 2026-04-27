-- ct_comm.lua
-- Thin internal helper used by the chain-tree comm subtree (comm_tx /
-- comm_rx columns). Not exported via require() from app columns —
-- those post COMM_SUBMIT events and asm_wait_for_event on COMM_RESPONSE.
--
-- 8 functions, no internal state. When the dongle path lands, this is
-- the *only* Lua-side file that changes (transport_inproc → transport_uart
-- happens entirely below comm_ffi). Same handle layout, same signatures.

local ffi      = require("ffi")
local bit      = require("bit")
local comm_ffi = require("comm_ffi")

local C = comm_ffi.C
local M = {}

-- One reusable err_box per call (created on first use, then recycled)
-- avoids allocation in the hot submit path.
local err_box = ffi.new("comm_result_t[1]")

-- Reusable poll buffer; max 32 events per call (= COMM_HANDLES_MAX).
local poll_buf_max = comm_ffi.HANDLES_MAX
local poll_buf     = ffi.new("comm_event_t[?]", poll_buf_max)

-- Reusable scratch event for claim().
local claim_buf = ffi.new("comm_event_t[1]")

------------------------------------------------------------------------
-- submit(mcu, cmd, payload, payload_len) -> handle, err
-- payload may be nil when payload_len is 0.
function M.submit(mcu, cmd, payload, payload_len)
    payload_len = payload_len or 0
    err_box[0]  = 0
    local h = C.comm_submit(mcu, cmd,
                            payload or nil,
                            payload_len,
                            err_box)
    return h, err_box[0]
end

-- broadcast(cmd, responder, payload, payload_len, bus_id) -> handle, err
function M.broadcast(cmd, responder, payload, payload_len, bus_id)
    payload_len = payload_len or 0
    err_box[0]  = 0
    local h = C.comm_submit_broadcast(bus_id or 0, cmd, responder,
                                      payload or nil,
                                      payload_len,
                                      err_box)
    return h, err_box[0]
end

-- poll(max) -> events_table_or_nil, err
-- Tick libcomm and return up to `max` newly-completed events as a
-- 1-indexed Lua table of comm_event_t cdata. Each call returns each
-- terminal slot at most once (libcomm tracks a per-slot surfaced flag).
function M.poll(max)
    max = math.min(max or poll_buf_max, poll_buf_max)
    local n = C.comm_poll(poll_buf, max)
    if n < 0 then return nil, n end
    local t = {}
    for i = 0, n - 1 do
        t[#t + 1] = poll_buf[i]
    end
    return t
end

-- claim(handle) -> event_or_nil, err
-- Consume a terminal slot. Returns the event cdata (copied out of
-- libcomm's slot, so the caller can hold it past the next poll).
function M.claim(handle)
    local rc = C.comm_claim(handle, claim_buf)
    if rc ~= 0 and rc ~= comm_ffi.RESULT.ERR_NAK then
        return nil, rc
    end
    -- Copy out so the caller can keep it past the next slot recycle.
    local e = ffi.new("comm_event_t")
    ffi.copy(e, claim_buf[0], ffi.sizeof("comm_event_t"))
    return e, rc
end

function M.cancel(handle)
    return C.comm_cancel(handle)
end

-- Handle layout helpers: low 5 bits = slot, high 27 bits = generation.
function M.handle_slot(h)
    return tonumber(bit.band(h, 0x1F))
end

function M.handle_gen(h)
    return tonumber(bit.rshift(h, 5))
end

function M.now_ms()
    return tonumber(C.comm_now_ms())
end

return M
