# Link Protocol (URLP v1)

Robot registration and liveness monitoring. Three-way handshake with heartbeat keepalive.

## States

**Robot:** `offline → announcing → wait_ack → live → planner_lost → offline`

**Planner:** `offline → registering → live → stale → offline`

## Three-Way Handshake

```
Robot                              Planner
  |                                  |
  |-- link_announce ----------------->|  (robot boot/reboot)
  |                                  |-- sends link_bridge_ack
  |<-- link_bridge_ack --------------|
  |                                  |
  |-- link_confirm ----------------->|  (wire_format, capabilities, energy)
  |                                  |-- state → live
  |          LIVE                    |
```

## Messages

### Robot → Planner (MQTT: `{site_path}/robots/{id}/link`)

| Message | When | Fields |
|---------|------|--------|
| link_announce | Boot/reboot | robot_id, seq, energy_remaining |
| link_heartbeat | Every 2s while live | robot_id, energy_remaining |
| link_confirm | After bridge_ack | wire_format, capabilities[], energy_max |
| link_disconnect | Clean shutdown | robot_id, reason, energy_remaining |

### Planner → Robot (MQTT: `{site_path}/robots/{id}/planner/{type}`)

| Message | When | Fields |
|---------|------|--------|
| link_bridge_ack | After announce | ack_seq, seq (planner seq for restart detection) |
| link_bridge_heartbeat | Every 3s | seq (monotonic, reset = planner restart) |
| link_bridge_disconnect | Planner shutdown | reason |

## Keepalive

- `link_heartbeat` is keepalive only. No state transitions. Ignored if robot not live.
- Planner detects stale robot: 3 missed heartbeats (6s) → stale. 15s more → offline.
- Robot detects planner loss: 3 missed planner heartbeats (9s) → planner_lost.

## Re-announce (Link Exception)

If a live robot re-announces (crash/reboot), the planner:

1. Fires `on_link_exception` callback → cancels active mission
2. Deregisters robot (writes offline to KV)
3. Starts fresh handshake

The robot aborts its mission, resets ChainTree state, and re-announces.

## Planner Restart Detection

Planner heartbeats include monotonic `seq`. If the robot sees seq decrease, the planner restarted. Robot aborts mission and re-announces.

## Robot Capabilities

The `link_confirm` message includes the robot's actual capabilities (VN names it supports). The planner uses these — not the KB's static list — when validating missions. The robot is authoritative.
