--[[
    transport.lua -- Generic hub↔remote transport interface.

    Same API regardless of backend:
      - socket (unix domain, TCP, future: BLE)
      - loopback (in-process queues for testing)

    Two channels:
      rpc:    hub → remote  (command packets)
      stream: remote → hub  (bitmap/sensor updates)

    Hub side:   send_rpc(msg), recv_stream() -> msg or nil
    Remote side: recv_rpc() -> msg or nil, send_stream(msg)
]]

local M = {}

---------------------------------------------------------------------------
-- Hub-side transport
---------------------------------------------------------------------------

local HubTransport = {}
HubTransport.__index = HubTransport

function HubTransport:send_rpc(msg)
    return self._send_rpc(msg)
end

function HubTransport:recv_stream()
    return self._recv_stream()
end

---------------------------------------------------------------------------
-- Remote-side transport
---------------------------------------------------------------------------

local RemoteTransport = {}
RemoteTransport.__index = RemoteTransport

function RemoteTransport:recv_rpc()
    return self._recv_rpc()
end

function RemoteTransport:send_stream(msg)
    return self._send_stream(msg)
end

---------------------------------------------------------------------------
-- Backend: socket (unix domain / TCP)
---------------------------------------------------------------------------

-- Hub side: rpc_sock is the write end, stream_sock is the read end
function M.hub_side_socket(rpc_sock, stream_sock, vmrt)
    return setmetatable({
        _send_rpc = function(msg)
            return vmrt.send(rpc_sock, msg)
        end,
        _recv_stream = function()
            return vmrt.recv_nb(stream_sock)
        end,
    }, HubTransport)
end

-- Remote side: rpc_sock is the read end, stream_sock is the write end
function M.remote_side_socket(rpc_sock, stream_sock, vmrt)
    return setmetatable({
        _recv_rpc = function()
            return vmrt.recv_nb(rpc_sock)
        end,
        _send_stream = function(msg)
            return vmrt.send(stream_sock, msg)
        end,
    }, RemoteTransport)
end

---------------------------------------------------------------------------
-- Backend: loopback (in-process queues)
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
