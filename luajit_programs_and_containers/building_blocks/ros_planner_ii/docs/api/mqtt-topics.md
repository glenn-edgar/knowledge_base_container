# MQTT Topics

Site path: `moonbase/alpha/surface_ops` (dots replaced with slashes)

## Robot Transport

| Topic | Direction | Content |
|-------|-----------|---------|
| `{site_path}/robots/{id}/rpc` | Hub → Robot | RPC command (JSON or CBOR) |
| `{site_path}/robots/{id}/stream_bus` | Robot → Hub | ACK, heartbeat, KB_DONE |

## Link Protocol

| Topic | Direction | Content |
|-------|-----------|---------|
| `{site_path}/robots/{id}/link` | Robot → Planner | link_announce, link_heartbeat, link_confirm, link_disconnect |
| `{site_path}/robots/{id}/planner/ack` | Planner → Robot | link_bridge_ack |
| `{site_path}/robots/{id}/planner/heartbeat` | Planner → Robot | link_bridge_heartbeat |
| `{site_path}/robots/{id}/planner/disconnect` | Planner → Robot | link_bridge_disconnect |

## Robot Status (retained)

| Topic | Direction | Content |
|-------|-----------|---------|
| `{site_path}/robots/{id}/status/state` | Robot → Broker | connected, wire_format, started_at |
| `{site_path}/robots/{id}/status/energy` | Robot → Broker | energy_max, energy_remaining |
| `{site_path}/robots/{id}/status/bitmask` | Robot → Broker | active KB bitmask + heartbeat |

## KV Bridge

| Topic | Direction | Content |
|-------|-----------|---------|
| `kv_bridge/write` | Hub → Bridge | `{ bucket, key, value }` — async write to NATS KV |

## Wire Format

- **JSON** (default): plain JSON strings on all topics
- **CBOR**: RPC commands and stream responses encoded as CBOR. Link protocol messages always JSON. Set via `wire_format` in link_confirm.
