# Configuration

Three JSON files per robot.

## `rover_<id>_config.json`

The robot-process config consumed by `mqtt_robot_config.load`.

```json
{
  "robot_id":    "rover_1",
  "site":        "moonbase.alpha.surface_ops",
  "mqtt_host":   "localhost",
  "mqtt_port":   1883,
  "robot_class": "lunar_rover",
  "remote_json": "/abs/path/to/remote.json",
  "energy_max":      10000,
  "energy_infinite": false,
  "wire_format":     "json",
  "capabilities": [
    "init_check", "path_spline", "path_line",
    "path_rotate", "deliver_part", "paint_sample", "load_shipping",
    "pass_gate", "inspection_scan", "recharge", "idle"
  ],
  "speed_factor": 5.0
}
```

| Field | Notes |
|---|---|
| `robot_id` | Used in MQTT topic paths; must be unique per site. |
| `site`     | Dot-separated; converted to slashes for topics. |
| `remote_json` | Absolute path to the compiled ChainTree `remote.json`. |
| `energy_max` / `energy_remaining` | Joule-equivalent budget; persisted to the slot store every 30 sim s. |
| `energy_infinite` | If true, controller never debits — useful for manual smoke runs. |
| `wire_format` | `"json"` or `"cbor"`. Today only JSON is wired up end-to-end. |
| `capabilities` | Reported in `link_announce`; planner uses it to decide what packets to send. **`path_wall` is intentionally absent** — the worker is a stub. |
| `speed_factor` | Wall-clock pacing knob (also overridable via `SPEED_FACTOR` env). 5x is the test-suite default. |

## `physics_config.json` and `sim_map.json`

Loaded from the same directory as the robot config when the HAL
constructs. See [physics/config.md](../physics/config.md) and
[physics/sim-map.md](../physics/sim-map.md) for the field-by-field map.

## What the planner sees

After link establishment the planner has:

- `robot_id`, `class_name`, `wire_format` — for routing
- `capabilities[]` — what packet types this robot can handle
- `energy_remaining`, `energy_max` — budget for planning

The planner does *not* see the physics_config or sim_map — those are
internal to the robot. (A real planner running in production would have
its own world model.)
