# Hub Runtime State Machine

Replaced the ChainTree-based hub with a simple state machine. Every VN follows the same cycle.

## States

```
idle → send_command → wait_ack → active → done
                         |                  |
                    ack_timeout         kb_done_timeout
                         |                  |
                         +---> error <------+
```

## Timeouts

| Timeout | Duration | Trigger |
|---------|----------|---------|
| ACK | 5 seconds | No ACK after command sent |
| KB_DONE | 10 seconds | No KB_DONE after ACK received |

Heartbeats from the robot reset the KB_DONE timeout (robot is still alive and working).

## API

```lua
local hub_runtime = require("hub_runtime")
local hub_rt = hub_runtime.new({
    robot_id  = "rover_1",
    db_file   = "surface_ops.db",   -- VN definitions loaded from KB
    site      = site,               -- from KB infrastructure query
    transport = mqtt_transport,     -- injected MQTT transport
    mqtt_hub  = mqtt_hub,           -- shared MQTT client for polling
})

-- Execute one action
bb.current_test_json = json_util.encode(action_params)
hub_rt:activate_kb("path_spline")

while not hub_rt:kb_is_complete("path_spline") do
    hub_rt:tick()
end

hub_rt:deactivate_kb("path_spline")
local pose = hub_rt:get_global_pose()
```

## VN Definitions from KB

At startup, hub_runtime loads all VN definitions:

```lua
kb_query:get_all_virtual_nodes()
-- Returns: { path_spline = { packet_type_id=2, json_schema={...}, bitmask={...} }, ... }
```

No plugin files, no hub.json, no build step. Adding a VN to the KB is all that's needed.
