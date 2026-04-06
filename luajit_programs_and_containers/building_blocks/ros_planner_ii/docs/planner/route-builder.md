# Route Builder

Converts a Dijkstra path into a sequence of virtual node actions.

## Edge to VN Translation

Each graph edge has a `nav` field specifying the navigation method:

| nav | VN kb_name |
|-----|-----------|
| spline_follow | path_spline |
| line_follow | path_line |
| wall_follow | path_wall |

## Auto-Inserted Actions

- **path_rotate** — inserted when heading changes between consecutive edges
- **init_check** — prepended when `bookend = true`
- **idle** — appended when `bookend = true`

## Heading Calculation

Heading is computed from edge geometry (atan2 of direction vector). If consecutive edges differ in heading by > 0.5 degrees, a path_rotate action is inserted with `from_heading` and `to_heading` parameters.

## Route Format

```lua
{
    { kb_name = "init_check", params = {} },
    { kb_name = "path_spline", params = { from_x=0, from_y=0, to_x=800, to_y=0, speed=150, distance=800 } },
    { kb_name = "path_rotate", params = { from_heading=0, to_heading=90 } },
    { kb_name = "deliver_part", params = { arm_target=-45, arm_speed=80, payload_type=1 } },
    { kb_name = "idle", params = {} },
}
```
