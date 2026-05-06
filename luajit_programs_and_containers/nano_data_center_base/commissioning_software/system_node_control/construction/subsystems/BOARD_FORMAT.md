# Board file format

Navigation boards describe the virtual-node graph the mission planner
routes over. They are stored content-addressable in the file_store
(`system.<sys>.site.<S>.boards.<name>` → sha256 → JSON blob) and read at
mission start by `kb_query:get_active_board()` then walked by
`global_planner` and `route_builder`.

This document is the contract for any **offline production tool** that
generates board files. Hand-authored fixtures (`build_landing_zone.lua`
and friends) follow the same contract.

---

## Top-level shape

```json
{
  "schema_version": 1,
  "metadata":  { ... },
  "ui":        { ... },
  "nodes":     [ ... ],
  "edges":     [ ... ],
  "markers":   [ ]
}
```

| Field             | v1 status | Notes |
|-------------------|-----------|-------|
| `schema_version`  | required  | Integer. `1` today. Readers branch on this; an upload with a higher version against an older reader fails loudly. |
| `metadata`        | required  | Authoring + coordinate-system context. See below. |
| `ui`              | optional  | Display registry consumed by the map-rendering web UI. Planner ignores. |
| `nodes`           | required  | The graph vertices. |
| `edges`           | required  | The graph edges. Each edge is one polyline between two nodes. |
| `markers`         | optional, **v2 reserved** | Walls, beacons, painted lines, and other terrain features. Empty array (or omitted) in v1. |

Strict JSON (RFC 8259). No trailing commas, no comments syntax. Use the
`description` and `_comment` fields below for human notes.

---

## metadata

```json
{
  "name":        "landing_zone",
  "description": "Landing zone virtual node graph for surface ops",
  "author":      "...",
  "created_at":  "2026-05-06T18:00:00Z",
  "coordinate_system": {
    "frame":     "local_enu",
    "units":     "mm",
    "origin":    { "lat": -89.9000, "lon": 0.0000, "alt_m": -120 },
    "heading_offset_deg": 0
  },
  "bounds": { "x_min": 0, "x_max": 2400, "y_min": -100, "y_max": 2400 }
}
```

| Field                               | Required | Notes |
|-------------------------------------|----------|-------|
| `name`                              | yes      | Must equal the upload `--name` argument. |
| `description`                       | no       | Human-readable summary; map UI may show in sidebar. |
| `author`, `created_at`              | no       | Authoring trail. `created_at` should be ISO-8601 UTC if present. |
| `coordinate_system.frame`           | yes      | Currently only `"local_enu"` is supported. The planner's distance math is Euclidean — this is correct only for local ENU. |
| `coordinate_system.units`           | yes      | `"mm"` or `"m"`. The reference fixtures use `mm` (so x=800 means 0.8 m). Pick one and stick to it for the whole board. |
| `coordinate_system.origin`          | optional | GPS anchor for offline tools that input lat/lon waypoints. **The robot does not see GPS** — the offline tool converts to local x/y on emit. |
| `coordinate_system.heading_offset_deg` | optional | Rotation between the local x-axis and true north. Default 0 (x-axis = east). |
| `bounds`                            | optional | Bounding box. Used by the map UI for default zoom; not authoritative for planning. |

### Why local ENU and not GPS

`route_builder.path_distance` walks the polyline and computes
`sqrt(dx^2 + dy^2)` segment by segment, then `cmd_path_*_t` packets
carry `from_x, from_y, to_x, to_y, distance` as `float` (single
precision, in the same `units` as the board). At GPS scales (degrees,
or meters from a non-local origin) Euclidean math is wrong and float
precision blows up. **Boards must be authored in a local Cartesian
frame.** The offline tool is the place to do GPS → local conversion.

---

## ui

The map-rendering web server reads this registry to pick icons, colors,
and labels. The planner does not read it.

```json
{
  "node_styles": {
    "base":            { "icon": "rocket",     "color": "#888", "label": "Base" },
    "transit":         { "icon": "dot",        "color": "#aaa", "size": "small" },
    "recharge":        { "icon": "battery",    "color": "#0a0" },
    "deliver_part":    { "icon": "package",    "color": "#06c" },
    "load_shipping":   { "icon": "crate",      "color": "#06c" },
    "paint_sample":    { "icon": "paintbrush", "color": "#c0c" },
    "inspection_scan": { "icon": "magnifier",  "color": "#c60" },
    "pass_gate":       { "icon": "doorway",    "color": "#888" }
  },
  "edge_styles": {
    "path_line":   { "color": "#666", "stroke": "solid",  "width": 2 },
    "path_spline": { "color": "#69f", "stroke": "solid",  "width": 2 },
    "path_wall":   { "color": "#a00", "stroke": "dashed", "width": 2 }
  }
}
```

- `node_styles` is keyed by node `type`. Every type used in `nodes[]`
  should have an entry here so the map UI never falls through to a
  default-unknown icon.
- A node may override per-instance via its own `ui` field (see Node).
- `edge_styles` is keyed by edge `nav` (`"path_line"`, `"path_spline"`,
  `"path_wall"`, future `"path_line_follow"`, etc.).
- Unknown fields are allowed and forwarded by the UI verbatim. This is
  where future per-icon size, layer, or animation config lives.

---

## Node

```json
{
  "name":        "construction_bay",
  "x":           800,
  "y":           1600,
  "type":        "load_shipping",
  "params":      { "arm_target": 30, "payload_type": 2 },
  "description": "Operator-staged module deposit point; arm extends to 30°",
  "ui":          { "label": "Construction Bay", "icon": "crate" }
}
```

| Field         | Required | Notes |
|---------------|----------|-------|
| `name`        | yes      | Globally unique within the board. Used as the graph key. |
| `x`, `y`      | yes      | Numbers in `metadata.coordinate_system.units`. |
| `type`        | yes      | `"transit"` = routing only, never a mission stop. Anything else IS the operation name and must appear in the robot's `operation_types` capability list. See **Allowed types** below. |
| `params`      | optional | Default operation parameters; merged into the operation VN's `data` blob at `mission_builder` time. Example: `{"arm_target": 30, "payload_type": 2}`. |
| `description` | optional | Human note; map UI may show as tooltip. |
| `ui`          | optional | Per-instance icon/label override (rarely needed; the registry in top-level `ui.node_styles[type]` covers most cases). |

### Allowed types (v1)

The robot's `capabilities.lua` enumerates what operation types it can
execute. As of 2026-05-06 the lunar_rover supports:

```
"base", "transit", "recharge",
"deliver_part", "paint_sample", "load_shipping",
"pass_gate", "inspection_scan"
```

`mission_builder` validates every non-transit stop's type against this
list. A board with a stop type not in the list will fail validation
when the first mission tries to plan to it.

When the robot capability list grows, add the new type to
`ui.node_styles` so the map UI has an icon for it.

---

## Edge (v1)

```json
{
  "from":        "lander_pad",
  "to":          "habitat_site",
  "nav":         "path_spline",
  "speed":       150,
  "weight":      800,
  "path":        [0, 0, 267, 0, 533, 0, 800, 0],
  "description": "Open-terrain spline along the western spine"
}
```

| Field         | Required | Notes |
|---------------|----------|-------|
| `from`, `to`  | yes      | Both must match a `name` in `nodes[]`. **Undirected**: the planner builds the reverse direction automatically by reversing the polyline. Author each edge once. |
| `nav`         | yes      | Direct kb_name. `"path_spline"` (Cartesian curve), `"path_line"` (painted-line follow), `"path_wall"` (wall ride). See **Nav-to-wire mapping** below. |
| `speed`       | yes      | Number in cm/s (or whatever units the robot's `cmd_*_t.speed` field uses; matches the v2 fixtures). |
| `weight`      | yes      | Dijkstra cost. Vary by terrain difficulty. Single weight per edge regardless of polyline length. |
| `path`        | yes      | **Flat** array of pairwise coordinates: `[x0, y0, x1, y1, ..., xn, yn]`. Must start at `nodes[from].{x,y}` and end at `nodes[to].{x,y}`. Length is always even and ≥ 4. |
| `description` | optional | Human note; map UI may show on hover. |

### `nav` extras

For `nav = "path_wall"` the edge may carry a `wall_standoff` field
(number, same units as `x/y`) that is forwarded into `cmd_path_wall_t`
on the wire. Default standoff is robot-firmware-defined.

### Path encoding gotcha

The polyline is **flat**, not an array of `{x, y}` objects:

```json
"path": [0, 0, 267, 0, 533, 0, 800, 0]      ✓
"path": [{"x":0,"y":0}, {"x":267,"y":0}]    ✗ — silently produces wrong distances
```

`global_planner.build_graph` reverses paths by walking
`for i = #path - 1, 1, -2 do`; that loop assumes the flat layout.
`route_builder.path_distance` walks pairs `(i, i+1) → (i+2, i+3)` for
the same reason. An object-of-objects path will silently yield zero
distance and broken reverses.

---

## Edge (v2 reserved — sub-paths for terrain)

In v2, an edge between two nodes may decompose into a sequence of
typed legs. **There is still ONE edge per node-pair.** The decomposition
is internal to that single edge.

```json
{
  "from": "habitat_site", "to": "junction_north",
  "weight": 400,
  "subpaths": [
    {
      "nav": "path_spline",
      "points": [[800,0],[900,40],[1100,30],[1200,0]],
      "speed": 150,
      "description": "open-terrain curve into the junction"
    },
    {
      "nav": "path_wall",
      "points": [[1200,0],[1280,-40]],
      "speed": 80,
      "wall_standoff": 200,
      "wall_marker_ref": "perimeter_wall_n",
      "description": "wall-ride along the north perimeter"
    },
    {
      "nav": "path_line",
      "points": [[1280,-40],[1300,-50]],
      "speed": 100,
      "line_marker_ref": "painted_lane_a",
      "description": "follow painted lane into the bay"
    }
  ],
  "description": "Composite traversal: open curve → wall ride → painted-line approach"
}
```

**Invariants:**

1. Sequential continuity: `subpath[i].points[last] == subpath[i+1].points[0]`.
2. Endpoint anchoring: `subpaths[0].points[0] == nodes[from].{x,y}` and `subpaths[N-1].points[last] == nodes[to].{x,y}`.
3. Per-subpath `nav`, `speed`, and nav-specific params (`wall_standoff`, `line_marker_ref`, etc.) override edge defaults.
4. `weight` stays at edge level — single dijkstra cost across all legs.
5. Marker refs (`wall_marker_ref`, `line_marker_ref`) point into the top-level `markers[]` table by `id`.
6. `points` is `[[x,y],[x,y]]` (array-of-pairs, **not** the v1 flat array). Object-of-objects allowed because v2 readers will deserialize differently anyway.

`schema_version` is bumped to `2` when the file uses sub-paths. A v1
reader sees `schema_version: 2` and refuses the file (the guard in
`kb_query.lua` is the loud-fail point).

`route_builder` for v2 walks each subpath in turn, emitting one wire
action per pairwise segment (same pattern as v1's polyline, just
applied per-leg).

---

## Marker (v2 reserved)

```json
{
  "id":          "perimeter_wall_n",
  "type":        "wall",
  "geometry":    { "kind": "polyline", "points": [[0,2400],[2400,2400]] },
  "ui":          { "color": "#a00", "stroke": "dashed" },
  "description": "..."
}
```

| Field         | Required | Notes |
|---------------|----------|-------|
| `id`          | yes      | Globally unique. Edges/sub-paths reference markers by this id. |
| `type`        | yes      | `"wall"`, `"beacon"`, `"painted_line"`, or future. |
| `geometry`    | yes      | `{kind, points}` for polyline; `{kind, x, y}` for point markers. Coordinates in board units. |
| `ui`          | optional | Per-marker color/stroke override. |
| `description` | optional | Human note. |

In v1 the `markers` array is empty (or omitted). The planner ignores it
either way.

---

## Nav-to-wire mapping

Conceptual sub-path types map cleanly to the existing robot wire types:

| Conceptual                 | `nav` value     | Wire packet (`cmd_*_t`) | Wire-extra params           |
|----------------------------|-----------------|-------------------------|-----------------------------|
| drive_straight (Cartesian) | `path_spline`   | `cmd_path_spline_t`     | — (degenerate 2-point spline) |
| drive_spline               | `path_spline`   | `cmd_path_spline_t`     | —                           |
| follow_wall                | `path_wall`     | `cmd_path_wall_t`       | `wall_standoff`             |
| follow_line (painted)      | `path_line`     | `cmd_path_line_t`       | (v2: `line_marker_ref`)     |

All path-class wire packets share `{from_x, from_y, to_x, to_y, speed,
distance}` plus the path-specific extras above and the segment
bookkeeping (`segment_index`, `total_segments` on `path_spline`).

`route_builder` is responsible for exploding the polyline into one
wire packet per pairwise segment with `distance` precomputed and
segment indices set. The board format is **wire-decoupled**: it carries
the full polyline and lets the planner do the explosion.

---

## Comments

JSON has no `//`. Two conventions cover the human-notes case; both are
ignored by every reader and parser:

| Field         | Where                                                                       | Audience                                   |
|---------------|-----------------------------------------------------------------------------|--------------------------------------------|
| `description` | `metadata`, every node, every edge, every subpath, every marker             | Map UI may render as tooltip / sidebar text |
| `_comment`    | Anywhere (free-form sibling field). Single string or array of strings.      | Offline-tool internal notes. Nothing in this codebase reads it. |

Authoring tools (yours and ours) treat `description` as the official
field; `_comment` is a courtesy for one-off scribbles that should not
end up in the UI.

---

## Storage and the upload tool

Boards live in the file_store under `system.<sys>.site.<S>.boards.<name>`
with `writer = "commissioning_only"`. The runtime planner is read-only
via `kb_query:get_active_board()`.

To upload a new revision:

```bash
upload_board.lua --system <sys> --site <S> --name <board> --file <path.json>
```

The tool reads the file, parses JSON, computes sha256, INSERTs a
`knowledge_base_fs_blob` row (no-op on hash collision — content is
dedup'd), UPSERTs the `knowledge_base_fs_node` pointer, and writes an
audit row at `<board_path>.KB_STATUS_FIELD.last_upload`. All in one
transaction.

Old hashes are retained in `fs_blob` (content-addressable history).
Mid-mission revision uses policy (1) drain-then-flip: in-flight
missions complete on the hash they captured at start. The wiring point
for future policy (3) replan-in-place is the drift hook in
`sequencer.lua`.

---

## Validation

### Offline tool — JSON Schema

`construction/subsystems/boards/board.schema.json` is a draft-07 schema
covering v1 strictly. Run before upload:

```bash
ajv validate -s board.schema.json -d landing_zone.json
```

(Or any other JSON Schema validator. The schema marks v2-reserved
fields permissively so a v2 file does not lint-fail in v1 tooling — the
runtime guard catches it instead.)

### Runtime — schema_version guard

`kb_query.lua:get_active_board()` reads `schema_version`. If it is
present and not `1`, the call fails loudly with the offending version
in the error message. This is the line of defense against a v2 board
being read by a v1 planner.

### Build-time — none yet

There is no Lua-side schema validation today. If malformed boards
start landing in production, add assertions to
`build_landing_zone.lua` (or whatever generator the offline tool ships)
and to `kb_query:get_active_board()`. The asymmetry is intentional:
schema-on-read keeps the runtime fast, and the offline tool already has
JSON Schema validation.

---

## Forward-evolution summary

| Future change                         | Breaks v1 reader? | Mechanism |
|---------------------------------------|-------------------|-----------|
| Add walls / beacons / painted lines   | No                | Populate `markers[]` (already reserved) |
| Per-edge sub-paths for terrain        | **Yes**           | Bump `schema_version` to 2; readers branch |
| New node types                        | No (provided robot lists them in `operation_types`) | Extend `ui.node_styles` registry |
| New `nav` type (e.g. `path_straight`) | No                | Add to `ui.edge_styles` + robot wire support |
| Lat/lon-native nodes                  | **Yes**           | Bump `schema_version`; current x/y stays canonical for v1 |

`schema_version: 1` is the future fork point. Every additive change
that does not break the v1 reader can land without bumping. Anything
that changes how `nodes[]` or `edges[]` are interpreted MUST bump.
