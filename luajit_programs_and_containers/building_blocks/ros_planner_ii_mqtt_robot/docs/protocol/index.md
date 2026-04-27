# MQTT protocol

The robot speaks Planner II's link + RPC + stream protocol. Wire format
is JSON in this build (`wire_format = "json"`), but the same code path
supports CBOR via the `wire_format` field.

## Topic layout

For `site = moonbase.alpha.surface_ops`, `robot_id = rover_1`:

| Topic | Direction | Carries |
|---|---|---|
| `moonbase/alpha/surface_ops/robots/rover_1/link`           | robot → planner | `link_announce`, `link_heartbeat` |
| `moonbase/alpha/surface_ops/robots/rover_1/planner/ack`    | planner → robot | `link_bridge_ack` |
| `moonbase/alpha/surface_ops/robots/rover_1/planner/hb`     | planner → robot | `link_bridge_heartbeat` |
| `moonbase/alpha/surface_ops/robots/rover_1/rpc`            | planner → robot | command packets |
| `moonbase/alpha/surface_ops/robots/rover_1/stream_bus`     | robot → planner | `ack`, `heartbeat`, `kb_done` |
| `moonbase/alpha/surface_ops/robots/rover_1/status/bitmask` | robot → planner | 1 Hz bitmask publishes |

The site name's dots are replaced with slashes.

## Layers

- [Link lifecycle](link.md) — announce / ack / heartbeat / planner-lost.
- [Command packets](rpc.md) — RPC schema, `packet_type` table, ack flow.
- [Stream events](stream.md) — heartbeat phases, kb_done shape, bitmask
  status publishes.

## Test scaffolding

`test_mock_planner.lua` is a 120-line stand-in for the planner side: it
subscribes to `link`, sends `link_bridge_ack` on `link_announce`, and
sends a `link_bridge_heartbeat` once a second to keep the robot in
"live" state. `test_random_paths.lua` then publishes random commands to
`rpc` and tallies `ack`/`heartbeat`/`kb_done` events from `stream_bus`.
See [Operations / Testing](../operations/testing.md).
