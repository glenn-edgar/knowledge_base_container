# MQTT Robot

Self-contained robot at `ros_planner_ii_mqtt_robot/`. Base for containerization.

## Files

| File | Role |
|------|------|
| mqtt_robot_main.lua | Production entry point with link_client |
| remote_mqtt_ct.lua | Test harness with link_client |
| mqtt_robot_config.lua | Config loader, MQTT status publisher |
| remote_dsl.lua | ChainTree DSL (controller + 12 workers) |
| remote_user_functions.lua | All VN worker implementations |
| capabilities.lua | Lunar rover VN set (12 capabilities) |
| build.sh | Compile remote.json from DSL |

## Startup

```bash
luajit mqtt_robot_main.lua config.json
```

Config JSON:
```json
{
    "robot_id": "rover_1",
    "site": "moonbase.alpha.surface_ops",
    "mqtt_host": "localhost",
    "mqtt_port": 1883,
    "robot_class": "lunar_rover",
    "remote_json": "remote.json",
    "energy_max": 10000,
    "energy_infinite": false,
    "wire_format": "json",
    "capabilities": ["init_check", "path_spline", "path_line", ...]
}
```

## Tick Loop

```
while not shutdown:
    link_client:tick()          -- announce, monitor planner heartbeats
    if link_client:is_live():
        queue_monitor:tick()    -- drain RPC commands
        chaintree:tick()        -- execute workers
    publish bitmask/heartbeat
    update energy
    usleep(1ms)
```

## Link Protocol Integration

- `link_client` announces on startup, waits for planner registration
- Mission processing gated on `is_live()`
- On planner loss: abort mission (deactivate workers, reset blackboard)
- On shutdown: `link_client:shutdown()` sends disconnect

## Worker Architecture

Controller (always active): dispatch, watchdog, heartbeat, completion
Workers (one per VN): activated by controller, execute action, report done

Currently simulated (tick countdowns). Next step: real hardware drivers.
