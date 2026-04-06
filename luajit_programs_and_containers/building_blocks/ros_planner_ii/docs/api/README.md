# ROS Planner II — External Interface Guide

This document describes how external systems (fleet managers, dashboards, mission schedulers, telemetry collectors) interact with the ROS Planner. All communication uses NATS.

## Prerequisites

- NATS server with JetStream enabled
- Site bucket names use underscores: `moonbase_alpha_surface_ops_action_server`

## 1. Submit a Mission

**Protocol:** NATS JobQueue

**Queue subject:** `{site}.action_server.missions`

```json
{
    "robot_id": "rover_1",
    "board": "landing_zone",
    "start": "lander_pad",
    "stops": [
        { "node": "mining_zone_a", "action": "deliver_part",
          "params": { "arm_target": -45, "arm_speed": 80, "payload_type": 1 } },
        { "node": "charging_station", "action": "recharge",
          "params": { "target_energy": 0 } },
        { "node": "lander_pad" }
    ],
    "bookend": true
}
```

**Fields:**

| Field | Required | Description |
|-------|----------|-------------|
| robot_id | Yes | Target robot (must be registered/live) |
| board | Yes | Board graph name from KB |
| start | Yes | Starting node on the board |
| stops | Yes | Array of destination nodes |
| stops[].node | Yes | Waypoint name on the board |
| stops[].action | No | Virtual node to execute at this stop. Omit for pass-through waypoint |
| stops[].params | No | Parameters for the action (VN-specific) |
| bookend | No | If true, prepends init_check and appends idle |

**Rejection reasons:**

| Reason | Description |
|--------|-------------|
| unsupported_capabilities | Robot doesn't support a required virtual node |
| insufficient_energy | Route cost exceeds robot energy |
| planning_failed | No path found between stops |
| robot already active | Robot has an in-progress mission |

## 2. Poll Mission Status

**Protocol:** NATS KeyStore (get)

**Bucket:** `{site_bucket}_action_server`

### Per-Robot Status (during execution)

**Key:** `{site}.action_server.{robot_id}.status`

```json
{
    "state": "planning",
    "robot_id": "rover_1",
    "timestamp": "2026-04-06T18:30:00Z"
}
```

States: `planning`, `executing`, `complete`, `failed`, `cancelled`

### Per-Robot Result (after completion)

**Key:** `{site}.action_server.{robot_id}.result`

```json
{
    "success": true,
    "completed": 18,
    "total": 18,
    "elapsed_ms": 1234,
    "replans": 0,
    "fault": null,
    "final_pose": { "x": 0, "y": 0, "heading": 270, "arm_angle": -135 }
}
```

On failure, `fault` contains:

```json
{
    "reason": "timeout",
    "action_index": 5,
    "kb_name": "path_wall"
}
```

Fault reasons: `ack_timeout`, `kb_done_timeout`, `kb_done_failed`, `planning_failed`, `insufficient_energy`, `link_exception`, `abort_requested`

## 3. Fleet Summary (Real-Time)

**Protocol:** NATS KeyStore (get or watch)

**Bucket:** `{site_bucket}_action_server`

**Key:** `{site}.action_server.summary`

```json
{
    "active_missions": 2,
    "missions": {
        "rover_1": { "state": "active", "board": "landing_zone" },
        "rover_2": { "state": "done", "board": "landing_zone" }
    },
    "registered_robots": ["rover_1", "rover_2", "arm_1"],
    "timestamp": "2026-04-06T18:30:00Z"
}
```

Updated on every mission state change (start, complete, cancel, error). Use NATS KV watch for push notifications.

`registered_robots` lists all robots currently registered via link protocol (live state).

## 4. Mission Log (Last N Missions)

**Protocol:** NATS KeyStore (history)

**Bucket:** `{site_bucket}_mission_log` (history=50)

**Key:** `{site}.action_server.mission_log`

```json
{
    "robot_id": "rover_1",
    "board": "landing_zone",
    "success": true,
    "completed": 18,
    "total": 18,
    "elapsed_ms": 1234,
    "fault": null,
    "timestamp": "2026-04-06T18:30:00Z"
}
```

Each completed mission (success or failure) appends one entry. NATS KV retains the last 50 values. Read with `history()` to get the rolling log.

## 5. Telemetry Stream (Per-Mission Events)

**Protocol:** NATS JetStream (subscribe or replay)

**Subject:** `{site}.robots.{robot_id}.stream.telemetry`

Events are published in order during mission execution:

| Event | Fields |
|-------|--------|
| mission_start | robot_id, board, route_length, timestamp |
| action_start | action_index, kb_name, pose, timestamp |
| heartbeat | action_index, kb_name, pose, timestamp |
| action_complete | action_index, kb_name, pose, timestamp |
| action_failed | action_index, kb_name, reason, timestamp |
| mission_complete | success, completed, total, elapsed_ms, final_pose |

Subscribe with `deliver_last_per_subject` for current state, or replay from sequence for historical data.

## 6. Robot Status

**Protocol:** NATS KeyStore (get or watch)

**Bucket:** `{site_bucket}_robot_status`

### Link State

**Key:** `{site}.robots.{robot_id}.status.link`

```json
{
    "link_state": "live",
    "robot_id": "rover_1",
    "transport": "mqtt",
    "wire_format": "json",
    "heartbeat_seq": 42,
    "heartbeat_at": "2026-04-06T18:30:00Z",
    "registered_at": "2026-04-06T18:00:00Z",
    "energy_remaining": 8500
}
```

Link states: `offline`, `registering`, `live`, `stale`

### Energy

**Key:** `{site}.robots.{robot_id}.status.energy`

```json
{
    "energy_max": 10000,
    "energy_remaining": 8500,
    "robot_id": "rover_1",
    "timestamp": "2026-04-06T18:30:00Z"
}
```

### Bitmask (Active Action Status)

**Key:** `{site}.robots.{robot_id}.status.bitmask`

```json
{
    "kb_name": "path_spline",
    "raw": 3,
    "fields": { "heartbeat": true, "seg_complete": true },
    "robot_id": "rover_1",
    "timestamp": "2026-04-06T18:30:00Z"
}
```

## 7. KB Data (Read-Only)

**Protocol:** NATS KeyStore (get)

**Bucket:** `kb_export`

The planner exports all KB data to NATS KV at startup. External systems can read board graphs, virtual node definitions, and robot configurations without SQLite access.

| Key Pattern | Content |
|-------------|---------|
| `{site}.boards.{name}` | Board graph (nodes, edges) |
| `{site}.robots.{id}.connection` | Robot connection info |
| `{site}.robots.{id}.capabilities` | Virtual node list |
| `{site}.virtual_nodes.{name}` | VN definition (schema, bitmask, pose_fields) |

## Namespace Convention

All keys use lowercase dot-separated paths:

```
moonbase.alpha.surface_ops.action_server.rover_1.status
moonbase.alpha.surface_ops.robots.rover_1.status.energy
moonbase.alpha.surface_ops.boards.landing_zone
```

Bucket names use underscores:

```
moonbase_alpha_surface_ops_action_server
moonbase_alpha_surface_ops_mission_log
moonbase_alpha_surface_ops_robot_status
kb_export
```

## Example: Fleet Manager Integration

```python
# Pseudocode — any NATS client library

# Watch fleet summary for real-time updates
watcher = kv.watch("moonbase.alpha.surface_ops.action_server.summary")
for update in watcher:
    summary = json.loads(update.value)
    print(f"Active: {summary['active_missions']}, Robots: {summary['registered_robots']}")

# Read mission log
entries = kv.history("moonbase.alpha.surface_ops.action_server.mission_log")
for entry in entries:
    mission = json.loads(entry.value)
    print(f"{mission['timestamp']} {mission['robot_id']} {mission['board']} success={mission['success']}")

# Submit a mission
jq.publish("moonbase.alpha.surface_ops.action_server.missions", json.dumps({
    "robot_id": "rover_1",
    "board": "landing_zone",
    "start": "lander_pad",
    "stops": [{"node": "mining_zone_a", "action": "deliver_part", "params": {"arm_target": -45}}],
    "bookend": True
}))

# Poll for result
result = kv.get("moonbase.alpha.surface_ops.action_server.rover_1.result")
```
