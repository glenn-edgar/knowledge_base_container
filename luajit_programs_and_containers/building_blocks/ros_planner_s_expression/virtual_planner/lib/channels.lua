-- channels.lua -- RPC and streaming channel simulation
--
-- Connects hub and remote blackboards.
-- In a real system these are avro packets over BLE/UART/MQTT.
-- Here they are direct table copies between two runtime handles.
--
-- RPC channel (hub -> remote):
--   Hub writes command to hub_bb.current_command + hub_bb.command_sent
--   Channel copies to remote_bb.incoming_command + remote_bb.command_pending
--
-- Streaming channel (remote -> hub):
--   Remote writes bitmap state to remote_bb.stream_*
--   Channel copies to hub_bb.bitmap_*

local M = {}

function M.new(hub_handle, remote_handle)
  local ch = {
    hub_handle    = hub_handle,
    remote_handle = remote_handle,
    -- Stats
    rpc_count     = 0,
    stream_count  = 0,
  }
  return setmetatable(ch, { __index = M })
end

---------------------------------------------------------------------------
-- RPC channel: hub -> remote
---------------------------------------------------------------------------
-- Transfers command packets from hub blackboard to remote blackboard.
function M.transfer_rpc(self)
  local hub_bb    = self.hub_handle.blackboard
  local remote_bb = self.remote_handle.blackboard

  if hub_bb.command_sent then
    -- Deep copy command to remote side
    remote_bb.incoming_command = {}
    if hub_bb.current_command then
      for k, v in pairs(hub_bb.current_command) do
        if type(v) == "table" then
          local copy = {}
          for ik, iv in pairs(v) do copy[ik] = iv end
          remote_bb.incoming_command[k] = copy
        else
          remote_bb.incoming_command[k] = v
        end
      end
    end
    remote_bb.command_pending = true
    hub_bb.command_sent = false
    self.rpc_count = self.rpc_count + 1
  end
end

---------------------------------------------------------------------------
-- Streaming channel: remote -> hub
---------------------------------------------------------------------------
-- Transfers bitmap state from remote blackboard to hub blackboard.
function M.transfer_stream(self)
  local hub_bb    = self.hub_handle.blackboard
  local remote_bb = self.remote_handle.blackboard

  -- Map remote stream fields to hub bitmap fields
  local mappings = {
    { "stream_seg_complete",    "bitmap_seg_complete"    },
    { "stream_obstacle",        "bitmap_obstacle"        },
    { "stream_motor_fault",     "bitmap_motor_fault"     },
    { "stream_action_complete", "bitmap_action_complete" },
    { "stream_action_fault",    "bitmap_action_fault"    },
  }

  local changed = false
  for _, map in ipairs(mappings) do
    local remote_key = map[1]
    local hub_key    = map[2]
    local val = remote_bb[remote_key]
    if val ~= nil then
      hub_bb[hub_key] = val
      changed = true
    end
  end

  if changed then
    self.stream_count = self.stream_count + 1
  end
end

---------------------------------------------------------------------------
-- Transfer both directions (call once per tick)
---------------------------------------------------------------------------
function M.tick(self)
  self:transfer_rpc()
  self:transfer_stream()
end

---------------------------------------------------------------------------
-- Summary
---------------------------------------------------------------------------
function M.print_stats(self)
  print(string.format("Channels: %d RPC packets, %d stream updates",
    self.rpc_count, self.stream_count))
end

return M
