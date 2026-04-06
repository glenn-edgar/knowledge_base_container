# NATS API

## Mission Submission

**Queue:** `{site}.action_server.missions` (NATS JobQueue)

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

Stops without `action` are waypoints (robot passes through). `bookend: true` adds init_check and idle.

## Mission Status (KeyStore)

**Bucket:** `{site_bucket}_action_server`

| Key | Content |
|-----|---------|
| `{site}.action_server.{robot_id}.status` | `{ state, robot_id, timestamp, error? }` |
| `{site}.action_server.{robot_id}.result` | `{ success, completed, total, elapsed_ms, replans, fault?, final_pose }` |
| `{site}.action_server.summary` | `{ active_missions, missions: {id: {state,board}}, registered_robots: [...] }` |

## Mission Log (KeyStore, history=50)

**Bucket:** `{site_bucket}_mission_log`

| Key | Content |
|-----|---------|
| `{site}.action_server.mission_log` | `{ robot_id, board, success, completed, total, elapsed_ms, fault?, timestamp }` |

Read last N missions: NATS KV `history()` on this key.

## Telemetry Stream (JetStream)

**Subject:** `{site}.robots.{robot_id}.stream.telemetry`

Events: mission_start, action_start, heartbeat, action_complete, action_failed, mission_complete

## Robot Status (KeyStore, via kv-bridge)

**Bucket:** `{site_bucket}_robot_status`

| Key | Content |
|-----|---------|
| `{site}.robots.{robot_id}.status.bitmask` | `{ kb_name, raw, fields, robot_id, timestamp }` |
| `{site}.robots.{robot_id}.status.energy` | `{ energy_max, energy_remaining, robot_id, timestamp }` |
| `{site}.robots.{robot_id}.status.link` | `{ link_state, wire_format, heartbeat_seq, registered_at, energy_remaining }` |

## KB Export (KeyStore)

**Bucket:** `kb_export`

All KB data exported at planner startup. Dashboard/UI can read board graphs, VN definitions, robot configs from NATS KV without SQLite access.
