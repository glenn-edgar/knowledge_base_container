# Link lifecycle

Owned by `link_client` (the shared library at
`building_blocks/knowledge_base/mqtt/link_client.lua`); the robot just
constructs one with its identity + capabilities and ticks it.

## States

```
        +---------+   on connect   +-------------+
        | offline |--------------->| announcing  |
        +---------+                +------+------+
                                          | link_bridge_ack received
                                          v
                                   +-------------+
                                   |    live     |
                                   +------+------+
                                          | hb timeout / disconnect
                                          v
                                   +-------------+
                                   | planner_lost|
                                   +-------------+
```

## `link_announce` (robot → planner)

Published to `…/robots/<id>/link` on connect and on every reconnect.
Contains: `robot_id`, `class_name`, `wire_format`, `capabilities[]`,
`energy_max`, `energy_remaining`.

The mock planner responds with:

```json
{
  "type":      "link_bridge_ack",
  "robot_id":  "rover_1",
  "bridge_id": "planner",
  "ack_seq":   1,
  "seq":       0,
  "ts":        "2026-04-24T..."
}
```

Once `link_bridge_ack` is received, the robot transitions to **live**
and starts processing RPC traffic.

## Heartbeats

Once live, the planner publishes `link_bridge_heartbeat` every second
to `…/planner/hb`. If the robot doesn't see one within its silence
window, it goes to **planner_lost** and calls the
`on_planner_lost(reason)` callback registered by `mqtt_robot_main.lua`.

That callback:

1. Clears every entry in `remote_handle.active_tests` (no more KB ticks).
2. Resets `exec_active`, `exec_start`, `worker_done`, `worker_success`,
   `active_worker`.
3. Calls `ctrl:abort_all()` — flushes Lua worker_queue, drops the
   non-motion gate, calls `hal:abort_path()` to clear the C queue.

After the link comes back, the robot re-runs announce/ack and resumes
from a clean slate. There is no replay — anything the planner wanted
done before the drop has to be re-issued.

## Why the planner-lost path is so aggressive

If we resumed stale commands, the robot could move based on packets the
planner has already given up on (and possibly issued replacements for).
Forced abort guarantees the robot's behaviour after reconnect is fully
governed by the *fresh* command stream.
