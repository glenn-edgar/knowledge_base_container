# Command Protocol

Per-action execution cycle between hub and robot.

## Handshake per Virtual Node

```
Hub (planner)                    Robot
    |                              |
    |-- RPC command (JSON) ------->|  packet_type, test_id, seq, params
    |                              |
    |<-- ACK ---------------------|  seq, test_id, status="ok"
    |                              |
    |<-- heartbeat (periodic) ----|  delta pose, watchdog ticks
    |<-- heartbeat ---------------|
    |                              |
    |<-- KB_DONE -----------------|  success, delta pose, energy, fault_reason
    |                              |
    Hub applies delta pose
    Hub moves to next action
```

## Hub State Machine

```
idle → send_command → wait_ack (5s timeout) → active → wait_kb_done (10s timeout) → done
                          |                                    |
                          +--- ack_timeout → error             +--- kb_done_timeout → error
```

Heartbeats reset the KB_DONE timeout (robot is still alive).

## Message Formats

### RPC Command (hub → robot)
```json
{
    "packet_type": 2,
    "test_id": 5,
    "seq": 42,
    "next_test": 6,
    "from_x": 0, "from_y": 0,
    "to_x": 800, "to_y": 0,
    "speed": 150, "distance": 800
}
```

### ACK (robot → hub)
```json
{ "type": "ack", "seq": 42, "test_id": 5, "status": "ok" }
```

### Heartbeat (robot → hub)
```json
{
    "type": "heartbeat", "phase": "periodic", "test_id": 5,
    "delta_x": 400, "delta_y": 0, "delta_heading": 0,
    "global_x": 400, "global_y": 0, "global_heading": 0,
    "watchdog_ticks": 12, "worker": "worker_path_spline"
}
```

### KB_DONE (robot → hub)
```json
{
    "type": "kb_done", "test_id": 5, "success": true,
    "delta_x": 800, "delta_y": 0, "delta_heading": 0,
    "delta_arm_angle": 0,
    "energy_remaining": 8500, "energy_max": 10000
}
```

### Shutdown (hub → robot)
```json
{ "packet_type": 255, "seq": 99 }
```
