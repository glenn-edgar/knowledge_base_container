--[[
    transport.lua -- Generic hub↔remote transport interface.

    Abstracts the communication channel between hub and remote.
    Same API for ringbuffer pipes (simulation) or serial/network (hardware).

    Two channels:
      rpc:    hub → remote  (command packets)
      stream: remote → hub  (bitmap/sensor updates)

    Each channel is a simple send/recv of JSON strings.

    Usage:
      -- Hub side:
      local tx = transport.hub_side(pipe)
      tx:send_rpc('{"type":"path_segment","speed":150}')
      local stream_msg = tx:recv_stream()

      -- Remote side:
      local tx = transport.remote_side(pipe)
      local cmd = tx:recv_rpc()
      tx:send_stream('{"seg_complete":true}')

    Backend implementations:
      transport.ringbuf_pipe(pipe)  -- uses vmrt pipe (SPSC ringbuffers)
      transport.loopback()          -- in-process queues (testing)
      -- future: transport.serial(port, baud)
      -- future: transport.mqtt(broker, topic)
]]

local M = {}

---------------------------------------------------------------------------
-- Hub-side transport handle
---------------------------------------------------------------------------
-- send_rpc: hub → remote (writes to rpc channel)
-- recv_stream: remote → hub (reads from stream channel)

local HubTransport = {}
HubTransport.__index = HubTransport

function HubTransport:send_rpc(msg)
    return self._send_rpc(msg)
end

function HubTransport:recv_stream()
    return self._recv_stream()
end

---------------------------------------------------------------------------
-- Remote-side transport handle
---------------------------------------------------------------------------
-- recv_rpc: hub → remote (reads from rpc channel)
-- send_stream: remote → hub (writes to stream channel)

local RemoteTransport = {}
RemoteTransport.__index = RemoteTransport

function RemoteTransport:recv_rpc()
    return self._recv_rpc()
end

function RemoteTransport:send_stream(msg)
    return self._send_stream(msg)
end

---------------------------------------------------------------------------
-- Backend: ringbuffer pipe (vmrt SPSC)
---------------------------------------------------------------------------
-- Uses a vmrt_pipe_t with:
--   to_hub   = stream channel (remote writes, hub reads)
--   from_hub = rpc channel    (hub writes, remote reads)
--
-- NOTE: pipe direction naming (to_hub/from_hub) is from the main→hub
-- perspective. For hub↔remote, we reuse the same struct but assign:
--   from_hub → rpc (hub writes commands here)
--   to_hub   → stream (remote writes updates here)

function M.ringbuf_backend(pipe, lib)
    local read_buf_size = 8192
    local read_buf = require("ffi").new("uint8_t[?]", read_buf_size)

    local function send(rb, msg)
        return lib.vmrt_ringbuf_write(rb, msg, #msg) == 0
    end

    local function recv(rb)
        local len = lib.vmrt_ringbuf_read(rb, nil, 0)
        if len <= 0 then return nil end
        if len > read_buf_size then
            read_buf = require("ffi").new("uint8_t[?]", len)
            read_buf_size = len
        end
        local got = lib.vmrt_ringbuf_read(rb, read_buf, read_buf_size)
        if got <= 0 then return nil end
        return require("ffi").string(read_buf, got)
    end

    -- rpc: hub writes to from_hub, remote reads from from_hub
    -- stream: remote writes to to_hub, hub reads from to_hub
    local rpc_rb    = pipe.from_hub
    local stream_rb = pipe.to_hub

    return {
        send_rpc     = function(msg) return send(rpc_rb, msg) end,
        recv_rpc     = function()    return recv(rpc_rb) end,
        send_stream  = function(msg) return send(stream_rb, msg) end,
        recv_stream  = function()    return recv(stream_rb) end,
    }
end

function M.hub_side_ringbuf(pipe, lib)
    local backend = M.ringbuf_backend(pipe, lib)
    return setmetatable({
        _send_rpc    = backend.send_rpc,
        _recv_stream = backend.recv_stream,
    }, HubTransport)
end

function M.remote_side_ringbuf(pipe, lib)
    local backend = M.ringbuf_backend(pipe, lib)
    return setmetatable({
        _recv_rpc    = backend.recv_rpc,
        _send_stream = backend.send_stream,
    }, RemoteTransport)
end

---------------------------------------------------------------------------
-- Backend: loopback (in-process queues, for single-VM testing)
---------------------------------------------------------------------------

function M.loopback()
    local rpc_queue = {}
    local stream_queue = {}

    local hub = setmetatable({
        _send_rpc = function(msg)
            rpc_queue[#rpc_queue + 1] = msg
            return true
        end,
        _recv_stream = function()
            if #stream_queue == 0 then return nil end
            return table.remove(stream_queue, 1)
        end,
    }, HubTransport)

    local remote = setmetatable({
        _recv_rpc = function()
            if #rpc_queue == 0 then return nil end
            return table.remove(rpc_queue, 1)
        end,
        _send_stream = function(msg)
            stream_queue[#stream_queue + 1] = msg
            return true
        end,
    }, RemoteTransport)

    return hub, remote
end

return M
