--[[
    queue_monitor.lua -- Job queue monitor for ChainTree event injection.

    Sits between NATS job queues and the ChainTree runtime.
    Each tick:
      1. Drains inbound queue — claims all available jobs
      2. Maps each payload to an event_id based on message type
      3. Injects events into ChainTree event queue with parsed payload as event_data
      4. Checks outbound: if KB wrote a command, submits to outbound queue

    Works for both hub and robot — just configure the mapping direction.

    Usage:
      local monitor = queue_monitor.new({
          handle    = ct_handle,        -- ChainTree runtime handle
          transport = nats_tx,          -- nats_transport hub_side or remote_side
          direction = "hub" or "robot", -- determines event mapping
          target_node = "kb.xxx.root",  -- which node receives events (or nil for active KB root)
      })

      -- In tick loop:
      monitor:tick()
]]

local json_util = require("json_util")
local event_ids = require("event_ids")
local cmd_packets = require("command_packets")

local M = {}

function M.new(opts)
    local mon = {
        handle    = opts.handle,
        transport = opts.transport,
        direction = opts.direction or "hub",
        target_node = opts.target_node,  -- nil = broadcast to all active KB roots
    }
    return setmetatable(mon, { __index = M })
end

---------------------------------------------------------------------------
-- Drain inbound queue and inject events
---------------------------------------------------------------------------

function M.drain_inbound(self)
    local handle = self.handle
    local tx = self.transport
    local max_per_tick = 10  -- prevent runaway drain

    for _ = 1, max_per_tick do
        local payload_str

        if self.direction == "hub" then
            payload_str = tx:recv_stream()
        else
            payload_str = tx:recv_rpc()
        end

        if not payload_str then break end

        -- Parse JSON
        local ok, payload = pcall(json_util.decode, payload_str)
        if not ok or not payload then break end

        -- Map to event_id
        local event_id = self:map_event_id(payload)
        if not event_id then break end

        -- Inject event into ChainTree event queue
        -- Target: specific node, or broadcast to all active KB roots
        if self.target_node then
            table.insert(handle.event_queue, {
                node_id    = self.target_node,
                event_id   = event_id,
                event_data = payload,
            })
        else
            -- Broadcast to all active KB roots
            for kb_name, _ in pairs(handle.active_tests) do
                local kb = handle.kb_table[kb_name]
                if kb and kb.root_node then
                    table.insert(handle.event_queue, {
                        node_id    = kb.root_node,
                        event_id   = event_id,
                        event_data = payload,
                    })
                end
            end
        end
    end
end

---------------------------------------------------------------------------
-- Map payload to event_id
---------------------------------------------------------------------------

function M.map_event_id(self, payload)
    if self.direction == "hub" then
        -- Hub inbound: stream messages have a "type" field
        local msg_type = payload.type
        if msg_type and event_ids.hub_inbound[msg_type] then
            return event_ids.hub_inbound[msg_type]
        end
        return nil
    else
        -- Robot inbound: commands have a "packet_type" field
        local pkt_type = payload.packet_type
        if pkt_type then
            local mapped = event_ids.robot_inbound_by_packet_type[pkt_type]
            if mapped then return mapped end
            return event_ids.ROBOT_RPC_COMMAND_DEFAULT
        end
        return nil
    end
end

---------------------------------------------------------------------------
-- Check outbound: KB command_packet on blackboard → submit to queue
---------------------------------------------------------------------------

function M.flush_outbound(self)
    local handle = self.handle
    local tx = self.transport
    local bb = handle.blackboard

    if self.direction == "hub" then
        -- Hub outbound: current command packet → rpc queue
        local cmd_json = bb.command_packet_json
        if cmd_json then
            tx:send_rpc(cmd_json)
            bb.command_packet = nil
            bb.command_packet_size = 0
            bb.command_packet_json = nil
        end

        -- Hub outbound: next command (double buffer) → rpc queue
        local next_json = bb.next_command_json
        if next_json then
            tx:send_rpc(next_json)
            bb.next_command_json = nil
        end
    else
        -- Robot outbound: stream responses already sent by user functions
    end
end

---------------------------------------------------------------------------
-- Tick: drain inbound + flush outbound
---------------------------------------------------------------------------

function M.tick(self)
    self:drain_inbound()
    self:flush_outbound()
end

return M
