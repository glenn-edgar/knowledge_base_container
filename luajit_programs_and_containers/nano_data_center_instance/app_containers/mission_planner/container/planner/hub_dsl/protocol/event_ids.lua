--[[
    event_ids.lua -- Event ID constants for NATS↔ChainTree message mapping.

    Each message type from the job queue maps to a unique event_id.
    The job queue monitor reads the "type" field from the JSON payload,
    looks up the event_id, and injects the event into the ChainTree
    event queue with the parsed payload as event_data.

    Event ID ranges:
      100-199: Hub inbound (stream events from robot)
      200-299: Robot inbound (rpc commands from hub)
      300-399: System events
]]

local M = {}

---------------------------------------------------------------------------
-- Hub inbound: stream events from robot → hub ChainTree
---------------------------------------------------------------------------

M.HUB_STREAM_ACK         = 101   -- command acknowledged
M.HUB_STREAM_BITMAP      = 102   -- bitmap state update (per-KB bitmask fields)
M.HUB_STREAM_ODOMETRY    = 103   -- odometry update
M.HUB_STREAM_SENSOR      = 104   -- sensor reading
M.HUB_STREAM_KB_DONE     = 105   -- robot reports KB action complete
M.HUB_STREAM_FAULT       = 106   -- robot reports fault
M.HUB_STREAM_DELTA_POSE  = 107   -- delta position since KB start
M.HUB_STREAM_HEARTBEAT   = 108   -- periodic heartbeat with delta pose from remote

---------------------------------------------------------------------------
-- Robot inbound: rpc commands from hub → robot ChainTree
---------------------------------------------------------------------------

M.ROBOT_RPC_COMMAND       = 201   -- command packet (any virtual node type)
M.ROBOT_RPC_SHUTDOWN      = 202   -- shutdown request

---------------------------------------------------------------------------
-- System events
---------------------------------------------------------------------------

M.SYSTEM_SHUTDOWN         = 301
M.SYSTEM_HEARTBEAT        = 302

---------------------------------------------------------------------------
-- Message type string → event_id mapping
---------------------------------------------------------------------------

-- Hub side: maps "type" field from stream queue messages
M.hub_inbound = {
    ack         = M.HUB_STREAM_ACK,
    bitmap      = M.HUB_STREAM_BITMAP,
    odometry    = M.HUB_STREAM_ODOMETRY,
    sensor      = M.HUB_STREAM_SENSOR,
    kb_done     = M.HUB_STREAM_KB_DONE,
    fault       = M.HUB_STREAM_FAULT,
    delta_pose  = M.HUB_STREAM_DELTA_POSE,
    heartbeat   = M.HUB_STREAM_HEARTBEAT,
}

-- Robot side: maps "type" field from rpc queue messages
-- (commands don't have a "type" field — they have "packet_type")
M.robot_inbound_by_packet_type = {
    [255] = M.ROBOT_RPC_SHUTDOWN,  -- TYPE_SHUTDOWN from command_packets
}
-- All other packet_types map to ROBOT_RPC_COMMAND
M.ROBOT_RPC_COMMAND_DEFAULT = M.ROBOT_RPC_COMMAND

---------------------------------------------------------------------------
-- Event name lookup (for debug output)
---------------------------------------------------------------------------

M.names = {
    [M.HUB_STREAM_ACK]        = "HUB_STREAM_ACK",
    [M.HUB_STREAM_BITMAP]     = "HUB_STREAM_BITMAP",
    [M.HUB_STREAM_ODOMETRY]   = "HUB_STREAM_ODOMETRY",
    [M.HUB_STREAM_SENSOR]     = "HUB_STREAM_SENSOR",
    [M.HUB_STREAM_KB_DONE]    = "HUB_STREAM_KB_DONE",
    [M.HUB_STREAM_FAULT]      = "HUB_STREAM_FAULT",
    [M.HUB_STREAM_DELTA_POSE] = "HUB_STREAM_DELTA_POSE",
    [M.HUB_STREAM_HEARTBEAT]  = "HUB_STREAM_HEARTBEAT",
    [M.ROBOT_RPC_COMMAND]     = "ROBOT_RPC_COMMAND",
    [M.ROBOT_RPC_SHUTDOWN]    = "ROBOT_RPC_SHUTDOWN",
    [M.SYSTEM_SHUTDOWN]       = "SYSTEM_SHUTDOWN",
    [M.SYSTEM_HEARTBEAT]      = "SYSTEM_HEARTBEAT",
}

return M
