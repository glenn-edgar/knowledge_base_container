# Telemetry

Mission telemetry published to NATS KeyStore and JetStream.

## NATS KeyStore (latest state)

| Key | Content |
|-----|---------|
| `{site}.action_server.{robot_id}.status` | Current mission state |
| `{site}.action_server.{robot_id}.result` | Final mission result |
| `{site}.action_server.summary` | Fleet overview (active missions, registered robots) |

## NATS JetStream (event stream)

Stream subject: `{site}.robots.{robot_id}.stream.telemetry`

| Event Type | When |
|-----------|------|
| mission_start | Mission begins |
| action_start | Each VN action begins |
| heartbeat | Every 10 ticks during action |
| action_complete | Action succeeded |
| action_failed | Action faulted |
| mission_complete | Mission finished |

## Mission Log (rolling history)

Bucket: `{site_bucket}_mission_log` (history=50)
Key: `{site}.action_server.mission_log`

Each completed mission appends one entry. External consumers read `history()` to get the last 50 missions.

## Robot Status (via kv-bridge)

Published by hub_runtime to MQTT, bridged to NATS KV by kv-bridge container:

| Key | Content |
|-----|---------|
| `{site}.robots.{robot_id}.status.bitmask` | Active KB bitmask + heartbeat bit |
| `{site}.robots.{robot_id}.status.energy` | energy_max, energy_remaining |
| `{site}.robots.{robot_id}.status.link` | Link state, wire_format, registered_at |
