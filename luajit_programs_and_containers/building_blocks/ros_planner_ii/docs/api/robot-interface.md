# Robot Interface Requirements

This document specifies the protocol a robot must implement to work with the ROS Planner. The robot communicates exclusively over MQTT. Any language or platform that supports MQTT pub/sub can implement a compatible robot.

## Transport

- **Protocol:** MQTT v3.1.1 or v5.0
- **QoS:** 1 (at least once) for all messages
- **Wire format:** JSON (default) or CBOR (declared during registration)

## MQTT Topics

The robot uses four topics. `{site_path}` is the site name with dots replaced by slashes (e.g. `moonbase/alpha/surface_ops`).

| Topic | Direction | Purpose |
|-------|-----------|---------|
| `{site_path}/robots/{robot_id}/link` | Robot → Planner | Link protocol messages |
| `{site_path}/robots/{robot_id}/planner/#` | Planner → Robot | Link protocol responses (subscribe with wildcard) |
| `{site_path}/robots/{robot_id}/rpc` | Planner → Robot | RPC commands (subscribe) |
| `{site_path}/robots/{robot_id}/stream_bus` | Robot → Planner | Stream responses (ACK, heartbeat, KB_DONE) |

## Phase 1: Registration (Required)

The robot must complete the three-way handshake before receiving missions.

### Step 1: Announce

Publish to `{site_path}/robots/{robot_id}/link` every 2 seconds until acknowledged:

```json
{
    "type": "link_announce",
    "robot_id": "rover_1",
    "seq": 1,
    "energy_remaining": 10000,
    "ts": "2026-04-06T18:00:00Z"
}
```

### Step 2: Receive Bridge ACK

Subscribe to `{site_path}/robots/{robot_id}/planner/#`. Wait for:

```json
{
    "type": "link_bridge_ack",
    "robot_id": "rover_1",
    "ack_seq": 1,
    "seq": 0,
    "ts": "2026-04-06T18:00:01Z"
}
```

Save `seq` — this is the planner's heartbeat sequence for restart detection.

### Step 3: Send Confirm

Publish to `{site_path}/robots/{robot_id}/link`:

```json
{
    "type": "link_confirm",
    "robot_id": "rover_1",
    "wire_format": "json",
    "capabilities": [
        "init_check", "path_spline", "path_line", "path_wall",
        "path_rotate", "deliver_part", "paint_sample", "load_shipping",
        "pass_gate", "inspection_scan", "recharge", "idle"
    ],
    "energy_max": 10000,
    "energy_remaining": 10000,
    "ts": "2026-04-06T18:00:01Z"
}
```

**capabilities** — array of virtual node names this robot supports. The planner validates missions against this list. Only include VNs the robot can actually execute.

**wire_format** — `"json"` or `"cbor"`. Applies to RPC commands and stream responses only. Link protocol messages are always JSON.

After sending confirm, the robot is **live** and will receive RPC commands.

## Phase 2: Keepalive (Required)

### Robot Heartbeat

Publish to `{site_path}/robots/{robot_id}/link` every 2 seconds while live:

```json
{
    "type": "link_heartbeat",
    "robot_id": "rover_1",
    "energy_remaining": 9500,
    "ts": "2026-04-06T18:00:10Z"
}
```

If the planner misses 3 consecutive heartbeats (6 seconds), the robot is marked **stale**. After 15 more seconds, **offline**. Active mission is cancelled.

### Monitor Planner Heartbeat

The planner sends heartbeats every 3 seconds on `{site_path}/robots/{robot_id}/planner/heartbeat`:

```json
{
    "type": "link_bridge_heartbeat",
    "bridge_id": "planner",
    "seq": 5,
    "ts": "2026-04-06T18:00:12Z"
}
```

**Robot must monitor these.** If 3 consecutive planner heartbeats are missed (9 seconds):

1. Abort any active action
2. Return to safe state
3. Go back to Phase 1 (re-announce)

**Planner restart detection:** If `seq` decreases (e.g. was 50, now 1), the planner restarted. Treat the same as missed heartbeats — abort and re-announce.

## Phase 3: Command Execution (Per Action)

### Receive RPC Command

Subscribe to `{site_path}/robots/{robot_id}/rpc`. Commands arrive as:

```json
{
    "packet_type": 2,
    "test_id": 5,
    "seq": 42,
    "next_test": 6,
    "from_x": 0,
    "from_y": 0,
    "to_x": 800,
    "to_y": 0,
    "speed": 150,
    "distance": 800
}
```

**packet_type** identifies the virtual node. Parameter fields are VN-specific (defined in the KB json_schema).

| packet_type | VN Name | Category |
|-------------|---------|----------|
| 1 | init_check | System |
| 2 | path_spline | Navigation |
| 3 | path_line | Navigation |
| 4 | path_wall | Navigation |
| 5 | path_rotate | Navigation |
| 6 | deliver_part | Task |
| 7 | paint_sample | Task |
| 8 | load_shipping | Task |
| 9 | pass_gate | Task |
| 10 | inspection_scan | Task |
| 11 | idle | System |
| 12 | recharge | System |
| 255 | shutdown | System (terminate robot) |

### Send ACK (immediately)

Publish to `{site_path}/robots/{robot_id}/stream_bus`:

```json
{
    "type": "ack",
    "seq": 42,
    "test_id": 5,
    "status": "ok"
}
```

**Send ACK within 5 seconds** of receiving the command. The planner will timeout and fault if no ACK.

### Send Heartbeats (periodic, during execution)

Publish to `{site_path}/robots/{robot_id}/stream_bus` periodically during action execution:

```json
{
    "type": "heartbeat",
    "phase": "periodic",
    "test_id": 5,
    "delta_x": 400,
    "delta_y": 0,
    "delta_z": 0,
    "delta_heading": 0,
    "delta_arm_angle": 0,
    "global_x": 400,
    "global_y": 0,
    "global_z": 0,
    "global_heading": 0,
    "global_arm_angle": 0,
    "watchdog_ticks": 12,
    "worker": "worker_path_spline"
}
```

Heartbeats reset the planner's KB_DONE timeout. **Send at least every 5 seconds** during long actions to prevent timeout.

### Send KB_DONE (on completion)

Publish to `{site_path}/robots/{robot_id}/stream_bus` when the action finishes:

```json
{
    "type": "kb_done",
    "test_id": 5,
    "success": true,
    "delta_x": 800,
    "delta_y": 0,
    "delta_z": 0,
    "delta_heading": 0,
    "delta_arm_angle": 0,
    "fault_reason": null,
    "energy_remaining": 9500,
    "energy_max": 10000
}
```

**Send KB_DONE within 10 seconds** of ACK (or last heartbeat). The planner timeouts if missing.

**delta fields** — the change in pose this action produced. The planner adds these to the global pose. Navigation VNs report position changes. Task VNs may report arm_angle changes. System VNs (init_check, idle) report zeros.

**success** — `true` if the action completed normally. `false` triggers fault handling on the planner (possible replan).

**fault_reason** — string describing the failure (e.g. `"obstacle_detected"`, `"motor_fault"`, `"sensor_timeout"`). Only included when `success = false`.

## Phase 4: Shutdown

### Clean Disconnect (robot-initiated)

Publish to `{site_path}/robots/{robot_id}/link`:

```json
{
    "type": "link_disconnect",
    "robot_id": "rover_1",
    "reason": "robot_shutdown",
    "energy_remaining": 8000,
    "ts": "2026-04-06T19:00:00Z"
}
```

### Planner Disconnect

The planner may send a disconnect on `{site_path}/robots/{robot_id}/planner/disconnect`:

```json
{
    "type": "link_bridge_disconnect",
    "bridge_id": "planner",
    "reason": "shutdown",
    "ts": "2026-04-06T19:00:00Z"
}
```

On receiving this: abort action, go to Phase 1 (re-announce when planner returns).

### Shutdown Command

`packet_type = 255` in an RPC command means terminate. ACK it and exit gracefully.

## Timing Summary

| Event | Timeout | Consequence |
|-------|---------|-------------|
| Robot heartbeat | 6s (3 missed) | Planner marks robot stale |
| Stale to offline | 15s | Mission cancelled, robot deregistered |
| ACK after command | 5s | Action faulted, possible replan |
| KB_DONE after ACK | 10s (reset by heartbeats) | Action faulted, possible replan |
| Planner heartbeat | 9s (3 missed) | Robot aborts, re-announces |
| Announce interval | 2s | Robot re-sends until acknowledged |
| Registration timeout | 10s | Planner gives up, robot stays announcing |

## Minimal Implementation Checklist

1. Connect MQTT, subscribe to `rpc` and `planner/#` topics
2. Send `link_announce` every 2s
3. On `link_bridge_ack`: send `link_confirm` with capabilities
4. Send `link_heartbeat` every 2s while live
5. Monitor planner heartbeats (abort + re-announce on loss)
6. On RPC command: send ACK immediately
7. Execute action, send periodic heartbeats
8. Send KB_DONE with delta pose and success/failure
9. Handle shutdown command (packet_type 255)
10. Send `link_disconnect` on clean exit

## CBOR Wire Format

If `wire_format = "cbor"` in link_confirm:

- RPC commands arrive as CBOR-encoded bytes on the `rpc` topic
- Stream responses (ACK, heartbeat, KB_DONE) must be CBOR-encoded on `stream_bus`
- Link protocol messages (`link` and `planner/#` topics) remain JSON always

The CBOR payload is the same JSON structure encoded as CBOR. No schema changes.
