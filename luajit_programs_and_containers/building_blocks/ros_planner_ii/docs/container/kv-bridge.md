# KV Bridge

Go container that bridges MQTT to NATS KV asynchronously.

Location: `third_party_containers/kv_bridge/`

## Purpose

The hub runtime publishes robot status (bitmask, energy) to MQTT topic `kv_bridge/write`. The kv-bridge container subscribes and writes to NATS KV in the background. This keeps all KV writes off the hub tick path (microseconds for MQTT publish, never blocks).

## Message Format

```json
{
    "bucket": "moonbase_alpha_surface_ops_robot_status",
    "key": "moonbase.alpha.surface_ops.robots.rover_1.status.bitmask",
    "value": { "kb_name": "path_spline", "raw": 1, "fields": {...} }
}
```

Delete: `{"bucket": "...", "key": "...", "op": "delete"}`

## Running

```bash
docker start kv-bridge
# or
docker run --name kv-bridge --restart unless-stopped --network host \
  -e MQTT_HOST=localhost -e MQTT_PORT=1883 \
  -e NATS_URL=nats://127.0.0.1:4222 \
  nanodatacenter/kv-bridge:latest
```
