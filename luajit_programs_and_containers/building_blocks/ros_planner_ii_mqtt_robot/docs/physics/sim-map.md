# `sim_map.json`

Passive fixtures the robot can interact with. The robot "is at" a station
when its pose is within the station's `tolerance_m / tolerance_h_deg` of
the station's `dock_pose`.

## Schema

```json
{ "stations": [ { "id": "...", "kind": "...", "dock_pose": [x,y,h], ... } ] }
```

| Kind | Extra fields | Used by |
|---|---|---|
| `charger`          | `charge_rate_w` | `phys_begin_charge` to integrate `rate_w * dt`. |
| `load_dock`        | `payload.id`, `payload.mass_kg`, `pickup_arm_angle_deg` | `phys_begin_grip` snaps `payload_mass` from station when at it. |
| `paint_fixture`    | `paint_arm_angle_deg` | Stored as `param1` (radians); informational for arm-cycle workers. |
| `assembly_fixture` | `deliver_arm_angle_deg` | Stored as `param1` (radians); informational. |

`tolerance_m` (default 0.25) and `tolerance_h_deg` (default 20) define the
docking window.

## Default world

The checked-in `sim_map.json` defines four stations:

| id | kind | dock pose (x, y, h) | extra |
|---|---|---|---|
| `recharger_1`   | charger          | (8, 0, 0)        | 4 kW |
| `shipping_1`    | load_dock        | (6, 6, π/2)      | box_5kg, arm at 75° |
| `paint_stand_1` | paint_fixture    | (-4, 3, π)       | arm at 90° |
| `assembly_1`    | assembly_fixture | (6, -6, -π/2)    | arm at 110° |

These are the only fixtures present today. Adding a new station is just
an entry in this JSON; the physics_ffi loader will pass it through to C
via `phys_add_station`.
