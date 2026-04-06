# Mission Lifecycle

## Submission

Client submits a mission JSON to NATS JobQueue:

```json
{
    "robot_id": "rover_1",
    "board": "landing_zone",
    "start": "lander_pad",
    "stops": [
        { "node": "mining_zone_a", "action": "deliver_part",
          "params": { "arm_target": -45, "arm_speed": 80, "payload_type": 1 } },
        { "node": "lander_pad" }
    ],
    "bookend": true
}
```

Queue: `{site}.action_server.missions`

## Planning Phase

1. **Capability check** — robot's announced capabilities (from link_confirm) validated against mission stop actions and navigation VNs
2. **Energy check** — total route cost vs robot energy_remaining
3. **Route generation** — Dijkstra shortest path, route_builder converts edges to VNs, auto-inserts path_rotate
4. **Bookend** — optionally adds init_check (start) and idle (end)

Rejection reasons: `unsupported_capabilities`, `insufficient_energy`, `planning_failed`

## Execution Phase

Sequencer iterates route actions. For each:

1. Hub runtime sends command → waits ACK → waits KB_DONE
2. Applies delta pose to global coordinates
3. Publishes telemetry (action_start, heartbeat, action_complete)

On fault: sequencer can replan (up to 3 attempts). Replanning re-runs Dijkstra from current position with failed edge blocked.

## Completion

Mission result published to NATS KV:

```
{site}.action_server.{robot_id}.status → { state: "complete" }
{site}.action_server.{robot_id}.result → { success, completed, total, elapsed_ms, final_pose }
{site}.action_server.summary          → { active_missions, registered_robots }
{site}.action_server.mission_log      → { robot_id, board, success, ... } (history=50)
```

## Cancellation

Missions are cancelled on link exception (robot reboot, disconnect, stale timeout). The action server:

1. Sets coroutine state to "cancelled"
2. Publishes failure status and result to NATS KV
3. Updates fleet summary
4. Appends to mission log

The robot is responsible for aborting its own execution on planner loss.
