# Map Storage in the ltree Knowledge Base
## Option 4: Map as Authority, PDDL Predicates as Projections
### With In-Memory SQLite for Planning Performance

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [In-Memory SQLite Strategy](#2-in-memory-sqlite-strategy)
   - 2.1 [Why In-Memory for Planning](#21-why-in-memory-for-planning)
   - 2.2 [Dual-Database Pattern](#22-dual-database-pattern)
   - 2.3 [Sync Strategy: Disk to Memory](#23-sync-strategy-disk-to-memory)
   - 2.4 [Write-Back Strategy: Memory to Disk](#24-write-back-strategy-memory-to-disk)
3. [Map Schema in the KB](#3-map-schema-in-the-kb)
   - 3.1 [Path Conventions](#31-path-conventions)
   - 3.2 [Waypoint Table](#32-waypoint-table)
   - 3.3 [Topology Edge Table](#33-topology-edge-table)
   - 3.4 [Zone Hierarchy](#34-zone-hierarchy)
   - 3.5 [Passability State](#35-passability-state)
4. [Map Loading](#4-map-loading)
   - 4.1 [YAML Definition File (Canonical Format)](#41-yaml-definition-file-canonical-format)
   - 4.2 [Loading from YAML in Python](#42-loading-from-yaml-in-python)
   - 4.3 [Loading from YAML in LuaJIT](#43-loading-from-yaml-in-luajit)
   - 4.4 [Loading from ROS map_server](#44-loading-from-ros-map_server)
5. [The Projection Function](#5-the-projection-function)
   - 5.1 [What Projection Does](#51-what-projection-does)
   - 5.2 [SQL Implementation](#52-sql-implementation)
   - 5.3 [LuaJIT Implementation](#53-luajit-implementation)
   - 5.4 [Passability Filter](#54-passability-filter)
   - 5.5 [When to Run Projection](#55-when-to-run-projection)
6. [The Mission Controller Loop](#6-the-mission-controller-loop)
7. [Distance Matrix Pre-computation](#7-distance-matrix-pre-computation)
8. [Dynamic Passability Updates](#8-dynamic-passability-updates)
9. [Worked Example](#9-worked-example)
   - 9.1 [Map Definition File](#91-map-definition-file)
   - 9.2 [DB Setup and Map Load](#92-db-setup-and-map-load)
   - 9.3 [Projection Run](#93-projection-run)
   - 9.4 [Inspect What Planner Sees](#94-inspect-what-planner-sees)
10. [Query Reference](#10-query-reference)

---

## 1. Architecture Overview

The core principle of Option 4 is a clean separation between three layers:

```
┌─────────────────────────────────────────────────────────────┐
│  LAYER 1: MAP AUTHORITY  (planner.map.*)                    │
│                                                             │
│  Written once from a definition file.                       │
│  Never modified during robot operation.                     │
│  Contains: waypoints, topology edges, zone hierarchy,       │
│            pre-computed distance matrix, pose data.         │
│                                                             │
│  Source of truth for ALL spatial knowledge.                 │
└──────────────────────────┬──────────────────────────────────┘
                           │
                    project_map()
                    (runs before each plan request)
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│  LAYER 2: PASSABILITY FILTER  (planner.problem.state.*)     │
│                                                             │
│  path_blocked.corridor.kitchen — dynamic, sensor-driven     │
│  door_locked.kitchen            — dynamic, event-driven     │
│                                                             │
│  Projection reads this layer and SKIPS blocked edges.       │
└──────────────────────────┬──────────────────────────────────┘
                           │
                    project_map() applies filter
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│  LAYER 3: PDDL PROJECTION  (planner.problem.state.*)        │
│                                                             │
│  connected.entrance.corridor   — derived from topology      │
│  connected.corridor.kitchen    — derived from topology      │
│  (= (distance entrance corridor) 5.0) — from distance table │
│                                                             │
│  This is a CACHE, rebuilt by projection before each plan.   │
│  The planner reads ONLY this layer.                         │
│  ChainTree nodes read ONLY layer 1 for poses/Nav2 goals.    │
└─────────────────────────────────────────────────────────────┘
```

**The single rule:** only `project_map()` writes to `planner.problem.state.connected.*` and `planner.problem.functions.distance.*`. Everything else in the system is either reading from layer 1 or writing dynamic state facts (battery, cargo, robot_at) into layer 2/3.

---

## 2. In-Memory SQLite Strategy

### 2.1 Why In-Memory for Planning

The planning cycle has a tight inner loop:

```
project_map() → assemble_domain_pddl() → assemble_problem_pddl() → invoke_solver → store_plan()
```

Every one of these steps reads from the KB. If the KB is on disk, each read goes through the OS page cache. For a typical planning KB with a 20-node map and 50 predicates this is fast enough. But for larger maps, repeated replanning under tight timing, or embedded targets with slow flash storage, in-memory operation is the right choice.

SQLite supports a true in-memory database via the special filename `:memory:`. The entire database — schema, indexes, all rows — lives in RAM. No disk I/O at all during planning.

### 2.2 Dual-Database Pattern

The recommended pattern is two databases running simultaneously:

```
disk_db  (/var/data/robot_kb.sqlite)  ← persistent, survives restart
mem_db   (:memory:)                   ← planning scratchpad, fast

          load_to_memory(disk_db → mem_db)
               at startup
                    │
                    ▼
          mem_db used for ALL planning operations:
            - project_map()
            - assemble_pddl()
            - store_plan()
            - update world state
                    │
                    │  commit_to_disk(mem_db → disk_db)
                    │  periodically or on significant state changes
                    ▼
          disk_db updated with state changes:
            - robot_at
            - battery_level
            - completed plan steps
```

The map layer (`planner.map.*`) is read-only after load — it never needs write-back. Only dynamic state changes (predicates, function values, plan execution status) need periodic commit to disk.

### 2.3 Sync Strategy: Disk to Memory

SQLite's built-in backup API is the cleanest way to load disk → memory:

```lua
-- planner_memdb.lua
-- In-memory SQLite management

local sqlite3 = require('lsqlite3')  -- or your FFI binding

local M = {}

-- Open disk DB and load into memory
-- Returns the in-memory db handle
function M.load_to_memory(disk_path)
  local disk_db = sqlite3.open(disk_path)
  local mem_db  = sqlite3.open(':memory:')

  -- SQLite backup API: copies disk_db → mem_db atomically
  local backup = disk_db:backup('main', mem_db, 'main')
  while true do
    local rc = backup:step(100)  -- copy 100 pages per step
    if rc == sqlite3.DONE then break end
    if rc ~= sqlite3.OK and rc ~= sqlite3.BUSY and rc ~= sqlite3.LOCKED then
      error('Backup failed: ' .. tostring(rc))
    end
  end
  backup:finish()
  disk_db:close()

  -- Enable WAL on in-memory (no-op but harmless)
  mem_db:exec('PRAGMA journal_mode=MEMORY')
  mem_db:exec('PRAGMA synchronous=OFF')
  mem_db:exec('PRAGMA foreign_keys=ON')

  return mem_db
end

-- Alternative: open disk DB, attach memory DB, use SQL COPY
-- Useful when you need both open simultaneously
function M.load_with_attach(disk_path)
  local db = sqlite3.open(':memory:')

  -- Attach the disk DB
  db:exec(string.format("ATTACH DATABASE '%s' AS disk", disk_path))

  -- Copy all tables
  db:exec([[
    INSERT INTO knowledge_base
    SELECT * FROM disk.knowledge_base;

    INSERT INTO predicate_state
    SELECT * FROM disk.predicate_state;
  ]])

  db:exec("DETACH DATABASE disk")
  return db
end

return M
```

**Python equivalent using the standard library:**

```python
import sqlite3

def load_to_memory(disk_path: str) -> sqlite3.Connection:
    """Load disk SQLite DB into a memory DB using iterdump/executescript."""
    disk_conn = sqlite3.connect(disk_path)
    mem_conn  = sqlite3.connect(':memory:')

    # iterdump() generates the full SQL to recreate the database
    script = '\n'.join(disk_conn.iterdump())
    mem_conn.executescript(script)
    disk_conn.close()
    return mem_conn

# For large databases, use the backup API (Python 3.7+)
def load_to_memory_backup(disk_path: str) -> sqlite3.Connection:
    disk_conn = sqlite3.connect(disk_path)
    mem_conn  = sqlite3.connect(':memory:')
    disk_conn.backup(mem_conn)   # uses SQLite backup API natively
    disk_conn.close()
    return mem_conn
```

### 2.4 Write-Back Strategy: Memory to Disk

Three events should trigger a write-back from memory to disk:

1. **After each action completes** — robot_at changes, battery changes, plan step status changes
2. **After each replan** — new plan is stored
3. **On clean shutdown** — final state snapshot

```lua
-- Write memory DB back to disk
function M.commit_to_disk(mem_db, disk_path)
  -- Open (or overwrite) disk file
  local disk_db = sqlite3.open(disk_path)

  local backup = mem_db:backup('main', disk_db, 'main')
  while true do
    local rc = backup:step(100)
    if rc == sqlite3.DONE then break end
    if rc ~= sqlite3.OK then
      error('Write-back failed: ' .. tostring(rc))
    end
  end
  backup:finish()
  disk_db:close()
end

-- Selective write-back: only flush changed rows
-- More efficient for high-frequency state updates
function M.flush_state_changes(mem_db, disk_path)
  local disk_db = sqlite3.open(disk_path)
  disk_db:exec('BEGIN')

  -- Only sync dynamic state, NOT the map (it never changes)
  local sql = [[
    SELECT label, name, properties, data, path
    FROM knowledge_base
    WHERE path LIKE 'planner.problem.%'
       OR path LIKE 'planner.plan.%'
       OR path LIKE 'planner.exec.%'
  ]]

  -- Delete stale rows from disk, reinsert from memory
  disk_db:exec([[
    DELETE FROM knowledge_base
    WHERE path LIKE 'planner.problem.%'
       OR path LIKE 'planner.plan.%'
       OR path LIKE 'planner.exec.%'
  ]])

  local stmt_w = disk_db:prepare([[
    INSERT INTO knowledge_base (label, name, properties, data, path)
    VALUES (?, ?, ?, ?, ?)
  ]])

  for row in mem_db:nrows(sql) do
    stmt_w:bind_values(row.label, row.name, row.properties, row.data, row.path)
    stmt_w:step()
    stmt_w:reset()
  end
  stmt_w:finalize()

  -- Sync predicate_state
  disk_db:exec('DELETE FROM predicate_state')
  local stmt_ps = disk_db:prepare(
    'INSERT INTO predicate_state (path,pred_name,arg0,arg1,arg2) VALUES (?,?,?,?,?)')
  for row in mem_db:nrows('SELECT path,pred_name,arg0,arg1,arg2 FROM predicate_state') do
    stmt_ps:bind_values(row.path,row.pred_name,row.arg0,row.arg1,row.arg2)
    stmt_ps:step()
    stmt_ps:reset()
  end
  stmt_ps:finalize()

  disk_db:exec('COMMIT')
  disk_db:close()
end
```

**Startup sequence summary:**

```lua
-- robot_startup.lua
local memdb_mgr = require('planner_memdb')
local map_loader = require('planner_map_loader')

-- 1. Load disk KB into memory
local db = memdb_mgr.load_to_memory('/var/data/robot_kb.sqlite')

-- 2. If map not yet loaded, load it now
if not kb_path_exists(db, 'planner.map') then
  map_loader.load_from_yaml(db, '/etc/robot/map.yaml')
end

-- 3. Hand db handle to everything else
-- All planning, state management, and execution monitoring
-- operates on the in-memory db from this point forward

-- At shutdown or after each action:
memdb_mgr.flush_state_changes(db, '/var/data/robot_kb.sqlite')
```

---

## 3. Map Schema in the KB

### 3.1 Path Conventions

```
planner.map
planner.map.meta                              -- map metadata
planner.map.zones                             -- zone hierarchy root
planner.map.zones.<zone>                      -- zone node
planner.map.zones.<zone>.<subzone>            -- nested zone
planner.map.waypoints                         -- all waypoints
planner.map.waypoints.<wp_name>               -- single waypoint (pose + zone)
planner.map.topology                          -- directed edge graph
planner.map.topology.<from>.<to>              -- directed edge from → to
planner.map.distances                         -- pre-computed distance matrix
planner.map.distances.<from>.<to>             -- shortest path distance
```

### 3.2 Waypoint Table

Each waypoint is a named pose in the map frame:

```sql
-- Waypoint: entrance
INSERT INTO knowledge_base (label, name, properties, data, path)
VALUES (
  'entrance',
  'Entrance Waypoint',
  '{"kind":"waypoint"}',
  '{
    "x": 0.5,
    "y": 1.2,
    "theta": 0.0,
    "frame_id": "map",
    "zone": "lobby",
    "nav2_tolerance": 0.15,
    "description": "Main entrance door"
  }',
  'planner.map.waypoints.entrance'
);

-- Waypoint: corridor_junction_1
INSERT INTO knowledge_base (label, name, properties, data, path)
VALUES (
  'corridor_junction_1',
  'Corridor Junction 1',
  '{"kind":"waypoint"}',
  '{
    "x": 3.1,
    "y": 1.2,
    "theta": 0.0,
    "frame_id": "map",
    "zone": "corridor",
    "nav2_tolerance": 0.20
  }',
  'planner.map.waypoints.corridor_junction_1'
);
```

**What goes in `data`:**

| Field | Type | Purpose |
|---|---|---|
| `x`, `y` | float | Position in map frame (metres) |
| `theta` | float | Heading at waypoint (radians) |
| `frame_id` | string | ROS TF frame (`"map"`) |
| `zone` | string | Zone this waypoint belongs to |
| `nav2_tolerance` | float | Position tolerance for Nav2 arrive check |
| `description` | string | Human-readable label for debugging |

### 3.3 Topology Edge Table

Edges define which waypoints the planner can navigate between. They are directed — add both directions for undirected traversal.

```sql
-- Edge: entrance → corridor_junction_1
INSERT INTO knowledge_base (label, name, properties, data, path)
VALUES (
  'corridor_junction_1',
  'entrance → corridor_junction_1',
  '{"kind":"topology_edge"}',
  '{
    "traversable": true,
    "base_cost": 5.0,
    "speed_limit": 0.5,
    "requires_clearance": false,
    "door": null,
    "notes": ""
  }',
  'planner.map.topology.entrance.corridor_junction_1'
);

-- Reverse edge (if bidirectional)
INSERT INTO knowledge_base (label, name, properties, data, path)
VALUES (
  'entrance',
  'corridor_junction_1 → entrance',
  '{"kind":"topology_edge"}',
  '{
    "traversable": true,
    "base_cost": 5.0,
    "speed_limit": 0.5,
    "requires_clearance": false,
    "door": null
  }',
  'planner.map.topology.corridor_junction_1.entrance'
);

-- Edge with a door (passability is conditional)
INSERT INTO knowledge_base (label, name, properties, data, path)
VALUES (
  'kitchen',
  'corridor → kitchen (door)',
  '{"kind":"topology_edge"}',
  '{
    "traversable": true,
    "base_cost": 8.0,
    "speed_limit": 0.3,
    "requires_clearance": false,
    "door": "kitchen_door",
    "door_normally_open": true
  }',
  'planner.map.topology.corridor.kitchen'
);
```

**What goes in `data`:**

| Field | Type | Purpose |
|---|---|---|
| `traversable` | bool | Default traversability (can be overridden by passability state) |
| `base_cost` | float | Distance/cost in seconds or metres |
| `speed_limit` | float | Max robot speed on this edge (m/s) |
| `requires_clearance` | bool | Edge needs explicit clearance signal before use |
| `door` | string\|null | Named door that must be open for this edge |
| `door_normally_open` | bool | If true, only blocked when door event fires |

### 3.4 Zone Hierarchy

Zones organize waypoints into logical areas. The planner can reason about zones; Nav2 uses individual waypoints.

```sql
-- Zone root
INSERT INTO knowledge_base (label, name, properties, data, path)
VALUES ('zones', 'Zone Hierarchy', '{"kind":"zones_root"}', '{}',
        'planner.map.zones');

-- Top-level zones
INSERT INTO knowledge_base (label, name, properties, data, path)
VALUES ('lobby', 'Lobby Area', '{"kind":"zone"}',
        '{"restricted":false,"speed_limit":0.5,"waypoints":["entrance","lobby_center"]}',
        'planner.map.zones.lobby');

INSERT INTO knowledge_base (label, name, properties, data, path)
VALUES ('warehouse', 'Warehouse', '{"kind":"zone"}',
        '{"restricted":false,"speed_limit":1.0,"waypoints":["warehouse_entry","storage_a","storage_b","loading_dock"]}',
        'planner.map.zones.warehouse');

-- Nested zone (warehouse has a restricted charging area)
INSERT INTO knowledge_base (label, name, properties, data, path)
VALUES ('charging', 'Charging Area', '{"kind":"zone"}',
        '{"restricted":true,"speed_limit":0.2,"waypoints":["charging_bay_1","charging_bay_2"]}',
        'planner.map.zones.warehouse.charging');
```

**Using the zone hierarchy in the planner:**

Zone names become PDDL objects of type `zone`. Actions like `escort_to_zone` or `patrol_zone` can reference them. The projection step can also add zone-level connectivity (if all waypoints in a zone are connected, the zone itself is treated as a PDDL location).

### 3.5 Passability State

Passability facts live in `planner.problem.state.*` — they are dynamic world state, not map data. The naming convention makes the association clear:

```sql
-- Block an edge (e.g., obstacle detected on corridor→kitchen)
INSERT OR IGNORE INTO knowledge_base (label, name, properties, data, path)
VALUES ('kitchen', '(path_blocked corridor kitchen)',
        '{"kind":"state_fact"}', '{}',
        'planner.problem.state.path_blocked.corridor.kitchen');
INSERT OR IGNORE INTO predicate_state (path, pred_name, arg0, arg1)
VALUES ('planner.problem.state.path_blocked.corridor.kitchen',
        'path_blocked', 'corridor', 'kitchen');

-- Clear it when obstacle is gone
DELETE FROM knowledge_base
WHERE path = 'planner.problem.state.path_blocked.corridor.kitchen';

-- Lock a door
INSERT OR IGNORE INTO knowledge_base (label, name, properties, data, path)
VALUES ('kitchen_door', '(door_locked kitchen_door)',
        '{"kind":"state_fact"}', '{}',
        'planner.problem.state.door_locked.kitchen_door');
```

The projection function (Section 5) checks for `path_blocked` and `door_locked` before emitting a `connected` predicate.

---

## 4. Map Loading

### 4.1 YAML Definition File (Canonical Format)

The recommended source format is a hand-authored YAML file. It is human-readable, version-controllable, and independent of ROS.

```yaml
# map.yaml
meta:
  name: "warehouse_floor_1"
  frame_id: "map"
  version: "2024-03-01"
  description: "Warehouse ground floor, 20 waypoints"

zones:
  lobby:
    description: "Entrance lobby"
    restricted: false
    speed_limit: 0.5
  corridor:
    description: "Main corridor"
    restricted: false
    speed_limit: 0.8
  warehouse:
    description: "Warehouse floor"
    restricted: false
    speed_limit: 1.0
    subzones:
      charging:
        description: "Charging bays"
        restricted: true
        speed_limit: 0.2

waypoints:
  entrance:
    x: 0.5
    y: 1.2
    theta: 0.0
    zone: lobby
    nav2_tolerance: 0.15
    description: "Main entrance"

  corridor_j1:
    x: 3.1
    y: 1.2
    theta: 0.0
    zone: corridor
    nav2_tolerance: 0.20

  corridor_j2:
    x: 6.4
    y: 1.2
    theta: 0.0
    zone: corridor
    nav2_tolerance: 0.20

  kitchen:
    x: 6.4
    y: 4.5
    theta: 1.5708
    zone: kitchen
    nav2_tolerance: 0.15

  charging_bay_1:
    x: 12.0
    y: 0.8
    theta: 3.1416
    zone: warehouse.charging
    nav2_tolerance: 0.05
    description: "Charging bay 1 — dock precisely"

# Directed edges.
# Both directions listed explicitly for clarity.
# base_cost is in seconds (for PDDL duration expressions).
topology:
  # Lobby ↔ Corridor
  - from: entrance
    to: corridor_j1
    base_cost: 5.0
    speed_limit: 0.5
    door: null

  - from: corridor_j1
    to: entrance
    base_cost: 5.0
    speed_limit: 0.5
    door: null

  # Corridor internal
  - from: corridor_j1
    to: corridor_j2
    base_cost: 4.0
    speed_limit: 0.8

  - from: corridor_j2
    to: corridor_j1
    base_cost: 4.0
    speed_limit: 0.8

  # Corridor → Kitchen (has a door)
  - from: corridor_j2
    to: kitchen
    base_cost: 8.0
    speed_limit: 0.3
    door: kitchen_door
    door_normally_open: true

  - from: kitchen
    to: corridor_j2
    base_cost: 8.0
    speed_limit: 0.3
    door: kitchen_door
    door_normally_open: true

  # Corridor → Charging
  - from: corridor_j2
    to: charging_bay_1
    base_cost: 6.0
    speed_limit: 0.5

  - from: charging_bay_1
    to: corridor_j2
    base_cost: 6.0
    speed_limit: 0.5

# Named doors referenced in topology edges
doors:
  kitchen_door:
    waypoint_near: corridor_j2
    normally_open: true
    description: "Swing door to kitchen"
```

### 4.2 Loading from YAML in Python

```python
# planner_map_loader.py
import yaml
import json
import sqlite3
from typing import Dict, Any

class MapLoader:
    """Loads a map YAML file into the ltree knowledge base."""

    def __init__(self, db_conn: sqlite3.Connection):
        self.db = db_conn
        self.cursor = db_conn.cursor()

    def load_yaml(self, yaml_path: str):
        with open(yaml_path) as f:
            map_def = yaml.safe_load(f)

        self.db.execute('BEGIN')
        try:
            self._clear_map()
            self._load_meta(map_def['meta'])
            self._load_zones(map_def.get('zones', {}))
            self._load_waypoints(map_def['waypoints'])
            self._load_topology(map_def['topology'])
            self._load_doors(map_def.get('doors', {}))
            self._compute_distances()
            self.db.execute('COMMIT')
        except Exception as e:
            self.db.execute('ROLLBACK')
            raise

    def _clear_map(self):
        """Remove all existing map data."""
        self.cursor.execute(
            "DELETE FROM knowledge_base WHERE path LIKE 'planner.map.%'")

    def _insert(self, label, name, kind, data, path):
        self.cursor.execute('''
            INSERT OR REPLACE INTO knowledge_base
            (label, name, properties, data, path)
            VALUES (?, ?, ?, ?, ?)
        ''', (label, name,
              json.dumps({'kind': kind}),
              json.dumps(data),
              path))

    def _load_meta(self, meta: Dict):
        self._insert('map', 'Map Root', 'map_root', meta, 'planner.map')

    def _load_zones(self, zones: Dict, parent_path='planner.map.zones'):
        """Recursively load zone hierarchy."""
        for zone_name, zone_data in zones.items():
            path = parent_path + '.' + zone_name
            subzones = zone_data.pop('subzones', {})
            self._insert(zone_name, zone_data.get('description', zone_name),
                        'zone', zone_data, path)
            if subzones:
                self._load_zones(subzones, path)

    def _load_waypoints(self, waypoints: Dict):
        for wp_name, wp_data in waypoints.items():
            path = 'planner.map.waypoints.' + wp_name
            self._insert(wp_name,
                        wp_data.get('description', wp_name),
                        'waypoint', wp_data, path)

    def _load_topology(self, edges: list):
        for edge in edges:
            frm  = edge['from']
            to   = edge['to']
            path = f'planner.map.topology.{frm}.{to}'
            data = {
                'traversable':       True,
                'base_cost':         edge.get('base_cost', 5.0),
                'speed_limit':       edge.get('speed_limit', 0.5),
                'requires_clearance':edge.get('requires_clearance', False),
                'door':              edge.get('door'),
                'door_normally_open':edge.get('door_normally_open', True),
            }
            self._insert(to,
                        f'{frm} → {to}',
                        'topology_edge', data, path)

    def _load_doors(self, doors: Dict):
        for door_name, door_data in doors.items():
            path = 'planner.map.doors.' + door_name
            self._insert(door_name,
                        door_data.get('description', door_name),
                        'door', door_data, path)

    def _compute_distances(self):
        """
        Pre-compute shortest-path distance matrix using Floyd-Warshall.
        Stores results in planner.map.distances.<from>.<to>
        """
        # Read all edges
        self.cursor.execute('''
            SELECT
              substr(path, length('planner.map.topology.')+1,
                     instr(substr(path, length('planner.map.topology.')+1),'.')-1) AS frm,
              label AS to_node,
              json_extract(data,'$.base_cost') AS cost
            FROM knowledge_base
            WHERE path LIKE 'planner.map.topology.%.%'
              AND json_extract(properties,'$.kind') = 'topology_edge'
        ''')
        edges = self.cursor.fetchall()

        # Get all node names
        nodes = set()
        adj   = {}
        INF   = float('inf')
        for frm, to, cost in edges:
            nodes.add(frm)
            nodes.add(to)
            if frm not in adj:
                adj[frm] = {}
            adj[frm][to] = cost

        nodes = list(nodes)
        n = len(nodes)
        idx = {name: i for i, name in enumerate(nodes)}

        # Floyd-Warshall distance matrix
        dist = [[INF] * n for _ in range(n)]
        for i in range(n):
            dist[i][i] = 0.0
        for frm, to, cost in edges:
            dist[idx[frm]][idx[to]] = float(cost)
        for k in range(n):
            for i in range(n):
                for j in range(n):
                    if dist[i][k] + dist[k][j] < dist[i][j]:
                        dist[i][j] = dist[i][k] + dist[k][j]

        # Store results
        self.cursor.execute(
            "DELETE FROM knowledge_base WHERE path LIKE 'planner.map.distances.%'")
        for i, frm in enumerate(nodes):
            for j, to in enumerate(nodes):
                if i != j and dist[i][j] < INF:
                    path = f'planner.map.distances.{frm}.{to}'
                    self._insert(to,
                                f'dist({frm},{to})',
                                'distance',
                                {'value': dist[i][j], 'from': frm, 'to': to},
                                path)
```

### 4.3 Loading from YAML in LuaJIT

```lua
-- planner_map_loader.lua
-- Requires: lyaml or tinyyaml for YAML parsing, dkjson for JSON

local json  = require('dkjson')
-- If no native YAML parser, use Python to pre-convert yaml→json,
-- or ship the map as JSON directly.

local M = {}

-- Assumes map_data is already a Lua table (from JSON or YAML parse)
function M.load_map(db, map_data)
  local function ins(label, name, kind, data, path)
    local sql = [[
      INSERT OR REPLACE INTO knowledge_base
      (label, name, properties, data, path)
      VALUES (?, ?, ?, ?, ?)
    ]]
    local stmt = db:prepare(sql)
    stmt:bind_values(label, name,
      json.encode({kind=kind}), json.encode(data), path)
    stmt:step()
    stmt:finalize()
  end

  -- Clear existing map
  db:exec("DELETE FROM knowledge_base WHERE path LIKE 'planner.map.%'")

  -- Meta
  ins('map', 'Map Root', 'map_root', map_data.meta, 'planner.map')

  -- Waypoints
  for wp_name, wp in pairs(map_data.waypoints) do
    ins(wp_name, wp.description or wp_name, 'waypoint', wp,
        'planner.map.waypoints.' .. wp_name)
  end

  -- Topology edges
  for _, edge in ipairs(map_data.topology) do
    local path = 'planner.map.topology.' .. edge.from .. '.' .. edge.to
    ins(edge.to, edge.from .. ' → ' .. edge.to, 'topology_edge', {
      traversable        = true,
      base_cost          = edge.base_cost or 5.0,
      speed_limit        = edge.speed_limit or 0.5,
      requires_clearance = edge.requires_clearance or false,
      door               = edge.door,
      door_normally_open = edge.door_normally_open ~= false,
    }, path)
  end

  -- Doors
  for door_name, door in pairs(map_data.doors or {}) do
    ins(door_name, door.description or door_name, 'door', door,
        'planner.map.doors.' .. door_name)
  end

  -- Pre-compute distances
  M.compute_distances(db)
end

function M.compute_distances(db)
  -- Read edges
  local edges = {}
  local nodes = {}
  local node_set = {}

  local sql = [[
    SELECT
      substr(path, length('planner.map.topology.')+1,
             instr(substr(path, length('planner.map.topology.')+1),'.')-1),
      label,
      json_extract(data,'$.base_cost')
    FROM knowledge_base
    WHERE path LIKE 'planner.map.topology.%.%'
  ]]
  for frm, to, cost in db:urows(sql) do
    table.insert(edges, {frm=frm, to=to, cost=tonumber(cost)})
    if not node_set[frm] then
      node_set[frm] = true
      table.insert(nodes, frm)
    end
    if not node_set[to] then
      node_set[to] = true
      table.insert(nodes, to)
    end
  end

  local n   = #nodes
  local idx = {}
  for i, name in ipairs(nodes) do idx[name] = i end

  -- Initialize distance matrix
  local INF  = math.huge
  local dist = {}
  for i = 1, n do
    dist[i] = {}
    for j = 1, n do
      dist[i][j] = (i == j) and 0.0 or INF
    end
  end
  for _, e in ipairs(edges) do
    dist[idx[e.frm]][idx[e.to]] = e.cost
  end

  -- Floyd-Warshall
  for k = 1, n do
    for i = 1, n do
      for j = 1, n do
        if dist[i][k] + dist[k][j] < dist[i][j] then
          dist[i][j] = dist[i][k] + dist[k][j]
        end
      end
    end
  end

  -- Store results
  db:exec("DELETE FROM knowledge_base WHERE path LIKE 'planner.map.distances.%'")
  local stmt = db:prepare([[
    INSERT INTO knowledge_base (label, name, properties, data, path)
    VALUES (?, ?, ?, ?, ?)
  ]])
  for i, frm in ipairs(nodes) do
    for j, to in ipairs(nodes) do
      if i ~= j and dist[i][j] < INF then
        local path = 'planner.map.distances.' .. frm .. '.' .. to
        stmt:bind_values(
          to,
          'dist(' .. frm .. ',' .. to .. ')',
          json.encode({kind='distance'}),
          json.encode({value=dist[i][j], from=frm, to=to}),
          path)
        stmt:step()
        stmt:reset()
      end
    end
  end
  stmt:finalize()
end

return M
```

### 4.4 Loading from ROS map_server

For ROS-integrated deployments, the semantic layer (waypoint names and connectivity) must be defined separately from the occupancy grid. The recommended approach:

1. Keep the occupancy grid in ROS (`map_server`) for Nav2 navigation
2. Define the semantic topology in your YAML file
3. Populate x/y coordinates by querying the TF tree or a separate semantic map node

```python
# ros_map_loader.py — bridge between ROS and the KB map
import rclpy
from geometry_msgs.msg import PoseStamped
from nav2_simple_commander.robot_navigator import BasicNavigator

class ROSMapLoader:
    def __init__(self, db_conn, yaml_path):
        self.db = db_conn
        self.loader = MapLoader(db_conn)

    def load_with_ros_poses(self, yaml_path: str):
        """Load map from YAML, then validate poses are reachable via Nav2."""
        self.loader.load_yaml(yaml_path)
        # Optionally: query /tf for pose validation, update waypoints with
        # current TF-based poses if needed
        # self._update_poses_from_tf()

    def get_nav2_pose(self, waypoint_name: str) -> PoseStamped:
        """
        Retrieve the Nav2-compatible PoseStamped for a waypoint.
        Called by action performers — uses map layer directly, not problem state.
        """
        import json
        self.db.cursor.execute(
            "SELECT data FROM knowledge_base WHERE path=?",
            (f'planner.map.waypoints.{waypoint_name}',))
        row = self.db.cursor.fetchone()
        if not row:
            raise ValueError(f"Waypoint '{waypoint_name}' not found in map")
        d = json.loads(row[0])

        pose = PoseStamped()
        pose.header.frame_id = d.get('frame_id', 'map')
        pose.pose.position.x = d['x']
        pose.pose.position.y = d['y']
        # Convert theta to quaternion
        import math
        pose.pose.orientation.z = math.sin(d['theta'] / 2.0)
        pose.pose.orientation.w = math.cos(d['theta'] / 2.0)
        return pose
```

---

## 5. The Projection Function

### 5.1 What Projection Does

`project_map(db)` is a pure transformation:

```
planner.map.topology.*          → planner.problem.state.connected.*
planner.map.distances.*         → planner.problem.functions.distance.*
planner.problem.state.path_blocked.*  (passability filter — removes blocked edges)
planner.problem.state.door_locked.*   (door filter — removes edges with locked doors)
```

It is the only function in the system that writes to `planner.problem.state.connected.*` and `planner.problem.functions.distance.*`. No other code touches these paths. If the planner sees unexpected connectivity, there is exactly one place to look.

### 5.2 SQL Implementation

```sql
-- Step 1: Clear projected connected predicates
DELETE FROM knowledge_base
WHERE path LIKE 'planner.problem.state.connected.%';

DELETE FROM predicate_state
WHERE pred_name = 'connected';

-- Step 2: Insert connected predicates for all traversable,
--         unblocked edges with no locked door
INSERT INTO knowledge_base (label, name, properties, data, path)
SELECT
  t.label AS label,
  '(connected ' || frm_name || ' ' || t.label || ')' AS name,
  '{"kind":"state_fact"}' AS properties,
  '{}' AS data,
  'planner.problem.state.connected.' || frm_name || '.' || t.label AS path
FROM knowledge_base t
-- Extract from-node name from path
JOIN (
  SELECT
    path,
    substr(path, length('planner.map.topology.')+1,
           instr(substr(path, length('planner.map.topology.')+1), '.')-1) AS frm_name
  FROM knowledge_base
  WHERE path LIKE 'planner.map.topology.%.%'
) frm ON frm.path = t.path
WHERE
  -- Edge must be marked traversable
  json_extract(t.data, '$.traversable') = 1

  -- Edge must not be path_blocked
  AND NOT EXISTS (
    SELECT 1 FROM predicate_state ps
    WHERE ps.pred_name = 'path_blocked'
      AND ps.arg0 = frm_name
      AND ps.arg1 = t.label
  )

  -- If edge has a door, that door must not be locked
  AND (
    json_extract(t.data, '$.door') IS NULL
    OR NOT EXISTS (
      SELECT 1 FROM predicate_state ps
      WHERE ps.pred_name = 'door_locked'
        AND ps.arg0 = json_extract(t.data, '$.door')
    )
  );

-- Step 3: Rebuild predicate_state index for connected
INSERT INTO predicate_state (path, pred_name, arg0, arg1)
SELECT path,
       'connected',
       substr(path, length('planner.problem.state.connected.')+1,
              instr(substr(path, length('planner.problem.state.connected.')+1),'.')-1),
       substr(path, length('planner.problem.state.connected.')+1 +
              instr(substr(path, length('planner.problem.state.connected.')+1),'.')),
       NULL
FROM knowledge_base
WHERE path LIKE 'planner.problem.state.connected.%';

-- Step 4: Clear and rebuild distance function values
DELETE FROM knowledge_base
WHERE path LIKE 'planner.problem.functions.distance.%';

INSERT INTO knowledge_base (label, name, properties, data, path)
SELECT
  label,
  '(distance ' || frm_name || ' ' || label || ')',
  '{"kind":"function_value"}',
  json_object('value', json_extract(d.data,'$.value')),
  'planner.problem.functions.distance.' || frm_name || '.' || label
FROM knowledge_base d
JOIN (
  SELECT path,
    substr(path, length('planner.map.distances.')+1,
           instr(substr(path, length('planner.map.distances.')+1),'.')-1) AS frm_name
  FROM knowledge_base
  WHERE path LIKE 'planner.map.distances.%.%'
) frm ON frm.path = d.path
-- Only project distances for connected pairs
-- (no point giving the planner a distance for a blocked edge)
WHERE EXISTS (
  SELECT 1 FROM predicate_state ps
  WHERE ps.pred_name = 'connected'
    AND ps.arg0 = frm_name
    AND ps.arg1 = d.label
);
```

### 5.3 LuaJIT Implementation

```lua
-- planner_projection.lua

local json = require('dkjson')
local M    = {}

function M.project_map(db)
  -- Single transaction for atomicity
  db:exec('BEGIN')

  -- Clear existing projections
  db:exec([[
    DELETE FROM knowledge_base
    WHERE path LIKE 'planner.problem.state.connected.%'
  ]])
  db:exec([[
    DELETE FROM predicate_state WHERE pred_name = 'connected'
  ]])
  db:exec([[
    DELETE FROM knowledge_base
    WHERE path LIKE 'planner.problem.functions.distance.%'
  ]])

  -- Read all topology edges
  local edge_sql = [[
    SELECT
      substr(path, length('planner.map.topology.')+1,
             instr(substr(path, length('planner.map.topology.')+1),'.')-1) AS frm,
      label AS to_node,
      json_extract(data,'$.traversable')        AS traversable,
      json_extract(data,'$.door')                AS door,
      json_extract(data,'$.door_normally_open')  AS door_normally_open
    FROM knowledge_base
    WHERE path LIKE 'planner.map.topology.%.%'
      AND json_extract(properties,'$.kind') = 'topology_edge'
  ]]

  -- Prepare insert statements
  local ins_kb = db:prepare([[
    INSERT OR IGNORE INTO knowledge_base
    (label, name, properties, data, path)
    VALUES (?, ?, '{"kind":"state_fact"}', '{}', ?)
  ]])
  local ins_ps = db:prepare([[
    INSERT OR IGNORE INTO predicate_state
    (path, pred_name, arg0, arg1)
    VALUES (?, 'connected', ?, ?)
  ]])

  -- Project each edge through the passability filter
  for frm, to, traversable, door, door_normally_open in db:urows(edge_sql) do
    -- Skip non-traversable edges
    if traversable ~= 0 and traversable ~= false then

      -- Check path_blocked
      local blocked = false
      local chk = db:prepare([[
        SELECT 1 FROM predicate_state
        WHERE pred_name='path_blocked' AND arg0=? AND arg1=? LIMIT 1
      ]])
      chk:bind_values(frm, to)
      blocked = chk:step() ~= nil
      chk:finalize()

      -- Check door_locked (if this edge has a door)
      local door_locked = false
      if door and door ~= '' then
        local dlk = db:prepare([[
          SELECT 1 FROM predicate_state
          WHERE pred_name='door_locked' AND arg0=? LIMIT 1
        ]])
        dlk:bind_values(door)
        door_locked = dlk:step() ~= nil
        dlk:finalize()
      end

      -- Emit connected predicate if not blocked
      if not blocked and not door_locked then
        local path = 'planner.problem.state.connected.' .. frm .. '.' .. to
        ins_kb:bind_values(to, '(connected ' .. frm .. ' ' .. to .. ')', path)
        ins_kb:step()
        ins_kb:reset()

        ins_ps:bind_values(path, frm, to)
        ins_ps:step()
        ins_ps:reset()
      end
    end
  end

  ins_kb:finalize()
  ins_ps:finalize()

  -- Project distances (only for connected pairs)
  local dist_sql = [[
    SELECT
      substr(path, length('planner.map.distances.')+1,
             instr(substr(path, length('planner.map.distances.')+1),'.')-1) AS frm,
      label AS to_node,
      json_extract(data,'$.value') AS dist_val
    FROM knowledge_base
    WHERE path LIKE 'planner.map.distances.%.%'
  ]]

  local ins_dist = db:prepare([[
    INSERT INTO knowledge_base (label, name, properties, data, path)
    VALUES (?, ?, '{"kind":"function_value"}', ?, ?)
  ]])

  for frm, to, dist_val in db:urows(dist_sql) do
    -- Only project if the pair is currently connected
    local conn_chk = db:prepare([[
      SELECT 1 FROM predicate_state
      WHERE pred_name='connected' AND arg0=? AND arg1=? LIMIT 1
    ]])
    conn_chk:bind_values(frm, to)
    local is_connected = conn_chk:step() ~= nil
    conn_chk:finalize()

    if is_connected then
      local path = 'planner.problem.functions.distance.' .. frm .. '.' .. to
      ins_dist:bind_values(
        to,
        '(distance ' .. frm .. ' ' .. to .. ')',
        json.encode({value = tonumber(dist_val)}),
        path)
      ins_dist:step()
      ins_dist:reset()
    end
  end
  ins_dist:finalize()

  db:exec('COMMIT')

  -- Report how many edges were projected
  local count = 0
  for row in db:nrows("SELECT COUNT(*) AS n FROM predicate_state WHERE pred_name='connected'") do
    count = row.n
  end
  return count  -- number of connected edges now visible to planner
end

return M
```

### 5.4 Passability Filter

The filter inside `project_map` enforces these rules in order:

| Condition | Check | Effect |
|---|---|---|
| Edge `traversable=false` | topology data | Skip — always blocked |
| `(path_blocked from to)` asserted | predicate_state | Skip this edge |
| Edge has `door` AND `(door_locked door_name)` asserted | predicate_state | Skip this edge |
| Edge has `door` AND `door_normally_open=true` AND door NOT locked | no path_blocked | Include |
| Edge has `requires_clearance=true` AND `(clearance_given from to)` NOT asserted | predicate_state | Skip |

Only edges passing all filters become `(connected from to)` predicates visible to the PDDL planner.

### 5.5 When to Run Projection

```lua
-- The projection must run before every plan request.
-- It is the first step in the planning pipeline, not an optional preprocessing step.

-- ALWAYS call: project_map(db) → assemble_pddl → invoke_solver

-- NEVER call the planner on stale projected state.

-- In addition, projection should re-run when passability changes
-- during plan execution (obstacle detected mid-run), which signals
-- the replan_requested flag.
```

**Three triggers:**

1. **Before every plan request** — mandatory, unconditional
2. **When `path_blocked` changes** — assert a `replan_requested` flag and break the current execution loop; the loop will re-project and replan
3. **After map reload** — if the physical map changes (not expected at runtime, but supported)

---

## 6. The Mission Controller Loop

This is the complete loop showing where projection fits:

```lua
-- mission_controller.lua
local projection = require('planner_projection')
local assembler  = require('planner_pddl_assembler')
local plan_kb    = require('planner_kb_plan')
local problem    = require('planner_kb_problem')
local memdb      = require('planner_memdb')

local function mission_loop(db, solver_path, disk_path)

  while not goal_achieved(db) do

    -- ── STEP 1: PROJECT MAP → PDDL predicates ─────────────────────────
    -- This is the ONLY step that populates connected.* and distance.*
    local edge_count = projection.project_map(db)
    print(string.format('[plan] projected %d edges into PDDL state', edge_count))

    -- ── STEP 2: ASSEMBLE PDDL FILES ────────────────────────────────────
    local domain_pddl  = assembler.assemble_domain(db)
    local problem_pddl = assembler.assemble_problem(db)
    write_file('/tmp/domain.pddl',  domain_pddl)
    write_file('/tmp/problem.pddl', problem_pddl)

    -- ── STEP 3: RUN SOLVER ─────────────────────────────────────────────
    local ok = os.execute(solver_path ..
      ' /tmp/domain.pddl /tmp/problem.pddl > /tmp/plan.pddl 2>&1')
    if not ok then
      -- Goal may be unreachable — passability may have cut off all paths
      print('[plan] solver failed — checking map connectivity...')
      local isolated = find_isolated_waypoints(db)
      if #isolated > 0 then
        print('[plan] isolated waypoints: ' .. table.concat(isolated, ', '))
      end
      error('No plan found. Check passability state and goal reachability.')
    end

    -- ── STEP 4: STORE PLAN ─────────────────────────────────────────────
    local steps = parse_plan_file('/tmp/plan.pddl')
    plan_kb.store_plan(db, steps, {solver=solver_path})

    -- ── STEP 5: EXECUTE PLAN ───────────────────────────────────────────
    for _, step in ipairs(steps) do

      -- Mark executing
      plan_kb.update_step_status(db, step._path, 'executing',
        {started_at = os.date('!%Y-%m-%dT%H:%M:%SZ')})

      -- Call action performer (blocks until complete or failure)
      local success, reason = execute_action(step.action, step.args, step.duration)

      if success then
        -- Apply action effects to world state
        apply_action_effects(db, step.action, step.args)
        plan_kb.update_step_status(db, step._path, 'success')

        -- Flush state changes to disk
        memdb.flush_state_changes(db, disk_path)

      else
        -- Action failed
        plan_kb.update_step_status(db, step._path, 'failure',
          {failure_reason = reason})

        -- Signal replan
        problem.set_replan_flag(db, 'action_failed', reason)
      end

      -- Check for replan request (from obstacle detection or action failure)
      if problem.replan_requested(db) then
        problem.clear_replan_flag(db)
        print('[exec] replanning triggered: ' .. problem.replan_reason(db))
        break  -- Exit execution loop, go back to projection + plan
      end

    end -- step loop

  end -- mission loop

  print('[mission] goal achieved.')
  memdb.commit_to_disk(db, disk_path)
end
```

---

## 7. Distance Matrix Pre-computation

The Floyd-Warshall computation runs once at map load time. Key properties:

- **O(N³) time, O(N²) space** — for N=20 waypoints: 8,000 ops, 400 rows, ~10ms
- **Stored in KB** — persistent across robot restarts (no recompute needed unless map changes)
- **Only connected pairs projected** — the projection step skips distances for blocked edges, so the planner's duration expressions always match its connectivity

```sql
-- Verify the distance matrix is sane after loading
SELECT
  frm.frm_name AS from_wp,
  d.label      AS to_wp,
  json_extract(d.data,'$.value') AS dist_seconds
FROM knowledge_base d
JOIN (
  SELECT path,
    substr(path, length('planner.map.distances.')+1,
           instr(substr(path, length('planner.map.distances.')+1),'.')-1) AS frm_name
  FROM knowledge_base
  WHERE path LIKE 'planner.map.distances.%.%'
) frm ON frm.path = d.path
ORDER BY dist_seconds DESC
LIMIT 10;
-- Shows the 10 longest routes, useful for sanity-checking

-- Check for waypoints with no outgoing edges (sinks — robot can reach but never leave)
SELECT wp.label AS waypoint
FROM knowledge_base wp
WHERE json_extract(wp.properties,'$.kind') = 'waypoint'
  AND NOT EXISTS (
    SELECT 1 FROM knowledge_base t
    WHERE t.path LIKE 'planner.map.topology.' || wp.label || '.%'
  );
```

---

## 8. Dynamic Passability Updates

Sensor nodes (obstacle detectors, door sensors, area monitors) update the passability layer. They write only to `planner.problem.state.path_blocked.*` and `planner.problem.state.door_locked.*` — never to the map layer.

```lua
-- obstacle_sensor_node.lua
-- Called when a laser scan detects a blocked corridor

local problem = require('planner_kb_problem')

local function on_obstacle_detected(db, from_wp, to_wp)
  -- Assert passability block
  problem.assert_predicate(db, 'path_blocked', from_wp, to_wp)
  -- If bidirectional, block the reverse too
  problem.assert_predicate(db, 'path_blocked', to_wp, from_wp)

  -- Signal replan (projection will exclude this edge next cycle)
  problem.set_replan_flag(db,
    'obstacle_detected',
    string.format('path blocked: %s → %s', from_wp, to_wp))

  print(string.format('[sensor] path_blocked: %s ↔ %s', from_wp, to_wp))
end

local function on_obstacle_cleared(db, from_wp, to_wp)
  problem.retract_predicate(db, 'path_blocked', from_wp, to_wp)
  problem.retract_predicate(db, 'path_blocked', to_wp, from_wp)

  -- May want to replan if a previously blocked shorter route is now available
  -- (optional — only replan if it would produce a better plan)
  print(string.format('[sensor] path cleared: %s ↔ %s', from_wp, to_wp))
end

local function on_door_locked(db, door_name)
  problem.assert_predicate(db, 'door_locked', door_name)
  problem.set_replan_flag(db, 'door_locked', door_name)
end

local function on_door_unlocked(db, door_name)
  problem.retract_predicate(db, 'door_locked', door_name)
end
```

**What projection sees after `on_obstacle_detected(db, 'corridor_j2', 'kitchen')`:**

```
Before:  planner.problem.state.path_blocked.*  — empty
         planner.problem.state.connected.corridor_j2.kitchen  — exists (projected)

After assert:
         planner.problem.state.path_blocked.corridor_j2.kitchen  — EXISTS

Next projection:
         planner.problem.state.connected.corridor_j2.kitchen  — DELETED
         planner.problem.functions.distance.corridor_j2.kitchen  — DELETED
         (planner can no longer route through this edge)
```

---

## 9. Worked Example

### 9.1 Map Definition File

```yaml
# /etc/robot/warehouse.yaml
meta:
  name: "warehouse_floor_1"
  frame_id: "map"

waypoints:
  entrance:   {x: 0.5,  y: 1.2,  theta: 0.0, zone: lobby}
  corridor:   {x: 3.1,  y: 1.2,  theta: 0.0, zone: corridor}
  kitchen:    {x: 6.4,  y: 4.5,  theta: 1.57, zone: kitchen}
  charging:   {x: 10.0, y: 0.8,  theta: 3.14, zone: warehouse}

topology:
  - {from: entrance, to: corridor,  base_cost: 5.0}
  - {from: corridor, to: entrance,  base_cost: 5.0}
  - {from: corridor, to: kitchen,   base_cost: 8.0, door: kitchen_door}
  - {from: kitchen,  to: corridor,  base_cost: 8.0, door: kitchen_door}
  - {from: corridor, to: charging,  base_cost: 6.0}
  - {from: charging, to: corridor,  base_cost: 6.0}

doors:
  kitchen_door: {normally_open: true}
```

### 9.2 DB Setup and Map Load

```python
import sqlite3
from planner_map_loader import MapLoader
from planner_memdb import load_to_memory_backup

# First run: build disk DB and load map
disk_conn = sqlite3.connect('/var/data/robot_kb.sqlite')
# ... create tables, load domain, set up problem ...
loader = MapLoader(disk_conn)
loader.load_yaml('/etc/robot/warehouse.yaml')
disk_conn.commit()
disk_conn.close()

# Every subsequent run: load disk into memory for fast operations
mem_conn = load_to_memory_backup('/var/data/robot_kb.sqlite')
# mem_conn is now the active DB for all planning operations
```

### 9.3 Projection Run

```lua
local proj = require('planner_projection')

-- Initial projection (no obstacles, no locked doors)
local n = proj.project_map(db)
print(n)   -- 6 (all 6 directed edges projected)

-- Check what the planner sees
for row in db:nrows([[
  SELECT arg0, arg1 FROM predicate_state
  WHERE pred_name='connected' ORDER BY arg0
]]) do
  print(string.format('(connected %s %s)', row.arg0, row.arg1))
end
-- (connected charging corridor)
-- (connected corridor charging)
-- (connected corridor entrance)
-- (connected corridor kitchen)
-- (connected entrance corridor)
-- (connected kitchen corridor)

-- Now block corridor → kitchen (obstacle)
local problem = require('planner_kb_problem')
problem.assert_predicate(db, 'path_blocked', 'corridor', 'kitchen')

-- Re-project
n = proj.project_map(db)
print(n)  -- 5 (one edge removed)

-- Check planner's view after block
-- (connected corridor kitchen) is now ABSENT
-- planner will route via a different path or declare goal unreachable
```

### 9.4 Inspect What Planner Sees

```sql
-- Current connectivity (after projection)
SELECT arg0 AS from_wp, arg1 AS to_wp
FROM predicate_state
WHERE pred_name = 'connected'
ORDER BY arg0;

-- Current distances (only for connected pairs)
SELECT
  substr(path, length('planner.problem.functions.distance.')+1,
         instr(substr(path, length('planner.problem.functions.distance.')+1),'.')-1) AS frm,
  label AS to_wp,
  json_extract(data,'$.value') AS dist_secs
FROM knowledge_base
WHERE path LIKE 'planner.problem.functions.distance.%'
ORDER BY frm;

-- Currently blocked paths
SELECT arg0 AS from_wp, arg1 AS to_wp
FROM predicate_state
WHERE pred_name = 'path_blocked';

-- Currently locked doors
SELECT arg0 AS door_name
FROM predicate_state
WHERE pred_name = 'door_locked';

-- Waypoint pose for Nav2 (read directly from map layer — NOT problem state)
SELECT
  json_extract(data,'$.x')          AS x,
  json_extract(data,'$.y')          AS y,
  json_extract(data,'$.theta')      AS theta,
  json_extract(data,'$.frame_id')   AS frame_id,
  json_extract(data,'$.nav2_tolerance') AS tolerance
FROM knowledge_base
WHERE path = 'planner.map.waypoints.kitchen';
```

---

## 10. Query Reference

```sql
-- MAP LAYER (read-only after load)

-- All waypoints
SELECT label, json_extract(data,'$.x') AS x, json_extract(data,'$.y') AS y,
       json_extract(data,'$.zone') AS zone
FROM knowledge_base
WHERE path LIKE 'planner.map.waypoints.%';

-- All topology edges
SELECT
  substr(path, length('planner.map.topology.')+1,
         instr(substr(path, length('planner.map.topology.')+1),'.')-1) AS from_wp,
  label AS to_wp,
  json_extract(data,'$.base_cost') AS cost,
  json_extract(data,'$.door') AS door
FROM knowledge_base
WHERE path LIKE 'planner.map.topology.%.%';

-- Shortest path distance from 'entrance' to anywhere
SELECT label AS to_wp, json_extract(data,'$.value') AS dist_seconds
FROM knowledge_base
WHERE path LIKE 'planner.map.distances.entrance.%'
ORDER BY dist_seconds;

-- PASSABILITY LAYER

-- What is currently blocked?
SELECT arg0, arg1 FROM predicate_state WHERE pred_name='path_blocked';
SELECT arg0 FROM predicate_state WHERE pred_name='door_locked';

-- PROJECTION RESULT (what planner sees)

-- Connected edges visible to planner
SELECT arg0, arg1 FROM predicate_state WHERE pred_name='connected';

-- Distance values in planner problem state
SELECT
  substr(path, length('planner.problem.functions.distance.')+1,
         instr(substr(path, length('planner.problem.functions.distance.')+1),'.')-1) AS frm,
  label AS to_wp,
  json_extract(data,'$.value') AS dist
FROM knowledge_base
WHERE path LIKE 'planner.problem.functions.distance.%';

-- Consistency check: every connected pair should have a distance
SELECT ps.arg0, ps.arg1
FROM predicate_state ps
WHERE ps.pred_name = 'connected'
  AND NOT EXISTS (
    SELECT 1 FROM knowledge_base kb
    WHERE kb.path = 'planner.problem.functions.distance.' || ps.arg0 || '.' || ps.arg1
  );
-- This should return 0 rows. If not, distances and connectivity are out of sync.
```
