# Action Server

Coroutine-based mission scheduler. Runs N missions concurrently in one LuaJIT process.

## Modes

- **Server mode** (`serve({drain_nats=true})`) — persistent, drains NATS JobQueue for missions
- **Direct mode** (`execute_mission(cmd)`) — single mission, synchronous, for tests

## Server Loop

```
while running:
    link_manager:tick()           -- process robot registrations
    drain_nats_queue()            -- claim new missions
    resume all active coroutines  -- cooperative multitasking
    publish_summary()             -- on state changes
    sleep
```

Each mission runs in its own coroutine: plan → validate → execute → publish result.

## Link Integration

- Creates `link_manager` in constructor (MQTT-first mode)
- Routes link messages from `mqtt_hub_transport` to link_manager
- `on_link_exception` cancels active mission for that robot
- Robot capabilities from `link_manager:get_capabilities()` (announced, not KB static)

## Fleet Summary

Published to NATS KV on every mission state change:

```json
{
    "active_missions": 2,
    "missions": {
        "rover_1": { "state": "active", "board": "landing_zone" },
        "rover_2": { "state": "done", "board": "landing_zone" }
    },
    "registered_robots": ["rover_1", "rover_2", "arm_1"],
    "timestamp": "2026-04-06T..."
}
```

Key: `{site}.action_server.summary`

## Mission Log

Rolling log of last 50 completed missions (NATS KV with history=50):

```json
{
    "robot_id": "rover_1",
    "board": "landing_zone",
    "success": true,
    "completed": 18,
    "total": 18,
    "elapsed_ms": 1234,
    "timestamp": "2026-04-06T..."
}
```

Bucket: `{site_bucket}_mission_log`
Key: `{site}.action_server.mission_log`
