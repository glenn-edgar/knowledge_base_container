# CONTINUE.md — Drive Program Architecture for ChainTree

## Context

Glenn Edgar has been developing the ChainTree framework for ~20 years. This document captures an architectural discussion about a **Drive Program** — a unified planning/execution model for robotics that eliminates the traditional three-layer planner stack (task planner / global planner / local planner).

The architecture is proven. Glenn built and deployed a version of this on **32K flash** in the 2000s, encoding a topological map of five Southern California counties (LA, Orange, Riverside, San Bernardino, Ventura) with bidirectional Dijkstra search.

## Core Architecture: The Drive Program

### The Key Insight

The plan **is** the behavior tree. The drive program generates a behavior tree from a topological map, then tracks execution of that tree. Recovery is always local — patch the smallest failing subtree, not a full replan.

### How It Works

1. **Global Map** — a static topological graph stored in a knowledge base. Named nodes with connections, weights, and properties. Not a costmap grid. Not PDDL.
2. **Drive Program** — takes start + goal, runs bidirectional Dijkstra over the map, emits a behavior tree representing the route. Each tree node corresponds to a segment or place in the map.
3. **Execution** — the drive program tracks progress through its own tree. Each node has status: pending, running, succeeded, failed.
4. **Local Recovery** — when a node fails, recovery happens at that node's level. Like GPS rerouting: miss a turn, the GPS doesn't recalculate from home — it patches you back to the next segment. Only if failure bubbles all the way up do you get a full replan.

### Why This Eliminates the Three-Layer Problem

Traditional ROS architecture:
- Layer 1: Task Planner (PDDL, symbolic) — what to do
- Layer 2: Global Planner (A*/Dijkstra on costmap) — path through known space
- Layer 3: Local Planner/Controller (DWA/TEB at 10-20Hz) — velocity commands

These layers have hard seams. Failures don't propagate cleanly. Each layer treats the one below as a black box.

The Drive Program replaces all three with **one tree at different granularity levels**:
- Task-level nodes (go to room, pick object)
- Navigation-level nodes (traverse hallway, pass through door)
- Control-level nodes (follow wall, align to door)

Recovery is scoped to the smallest subtree that can handle it. Same mechanism everywhere in the tree.

### The Map Structure (from the 2000s implementation)

Three levels of data, one structure:

- **Nodes** — the graph for Dijkstra. Named places/intersections with connections.
- **Paths** — connect two nodes. Carry constant weight for Dijkstra. Contain geometry.
- **Line Segments** — piecewise linear approximations of actual road/path shape. Execution-level data.

Dijkstra never touches line segments. It only sees nodes and weights. Once you have a route (sequence of paths), you pull out line segments for the current path and drive them.

### Bidirectional Dijkstra

- Forward search expands from current position
- Backward search expands from goal
- They meet in the middle — half the search space, half the memory
- Same algorithm serves both initial planning and local recovery (just smaller search scope for recovery)
- On 32K flash this wasn't an optimization, it was what made it possible

### Manipulator Arms as Pseudo Maps

The same architecture applies to robot arms. Joint space is a map:

- Nodes are **named configurations** (home, pre-grasp, grasp, safe-transit, drop-off)
- Paths between them are validated motions with joint-space line segments
- Bidirectional Dijkstra routes through configuration nodes
- Recovery is the same: path blocked → reroute through alternate configuration

```
/arm
  /home
    joints: [0, -90, 0, 0, 0]
    connected: [pre_grasp, safe_transit]
  /safe_transit
    joints: [45, -60, 30, 0, 0]
    connected: [home, pre_grasp, drop_off]
  /pre_grasp
    connected: [safe_transit, grasp]
    /path_to_grasp
      segments: [interpolated joint waypoints]
```

Navigation and manipulation share the same representation, algorithm, and execution model. The drive program doesn't care whether it's routing through intersections or joint configurations.

## Implementation Stack

### Knowledge Base: SQLite3

Glenn has an existing **SQLite3 C filter** that serves as the bridge between the KB and application code. pgRouting/Neo4j are too heavy. The architecture keeps Dijkstra in application code and uses the database for storage/queries only.

Schema:

```sql
CREATE TABLE nodes (
  path TEXT PRIMARY KEY,
  properties JSON,
  status TEXT DEFAULT 'clear'
);

CREATE TABLE connections (
  source TEXT,
  target TEXT,
  weight INTEGER,
  status TEXT DEFAULT 'clear'
);

CREATE INDEX idx_conn_source ON connections(source);
CREATE INDEX idx_conn_target ON connections(target);
```

Hierarchy queries use LIKE:
```sql
SELECT * FROM nodes WHERE path LIKE 'socal/la/%';
```

Neighbor queries for Dijkstra:
```sql
SELECT target, weight FROM connections
WHERE source = ? AND status = 'clear';
```

### Data Flow

```
SQLite3 DB file (the map / knowledge base)
    ↓
SQLite3 C filter (reads nodes, connections, status)
    ↓
Bidirectional Dijkstra (pure C, queries neighbors through filter)
    ↓
S-expression behavior tree (plan IS the executable)
    ↓
CFL runtime (walks the tree, writes status back through filter)
```

### Hardware Tier Scaling

Same architecture, same code, different database size:

| Tier | Hardware | DB Size | Notes |
|------|----------|---------|-------|
| Micro | ARM Cortex-M | KB-range | Small SQLite DB in flash |
| Mid | ESP32 | MB-range | Bigger maps, same code |
| Edge | Raspberry Pi / SPIKE Prime | MB-range | Full arena maps |
| Server | Linux + PostgreSQL | GB-range | Swap filter to PG bindings |

The SQLite3 C filter is the only thing that changes between tiers (swap to PostgreSQL bindings on server). Drive program, Dijkstra, CFL runtime — identical everywhere. This follows the ArenaEnv tier specialization pattern already built in Zig.

### Relationship to Existing ChainTree Components

- **CFL Runtime** (`cfl_image_loader.h/.c`) — executes the behavior tree
- **S-expression Engine** — compiles DSL into executable tree nodes
- **SQLite3 C Filter** — existing code, serves as KB interface
- **Binary Image System** (`.ctb` format) — the drive program output could be a `.ctb` image
- **ltree KB on PostgreSQL** — server-tier version of the same architecture
- **NATS/MQTT** — distributed status updates for multi-robot or remote monitoring

## What Has NOT Been Done in Robotics

The academic robotics community has the pieces separately:

- **PRM (Probabilistic Roadmaps)** — builds a graph of configurations, but uses random sampling instead of meaningful named poses, has no knowledge base, no behavior tree output, no local recovery
- **TAMP (Task and Motion Planning)** — tries to unify task and motion layers, but still treats them as separate processes with bridging logic
- **Nav2 Behavior Trees** — orchestrates planners but is a thin layer over separate processes, not a unified plan-is-the-executable model

**Nobody has combined**: topological configuration graph + bidirectional Dijkstra + behavior tree as plan output + knowledge base for live state + local recovery by subtree patching.

The constraint of 32K flash drove the better architecture. The field fragmented into specialties (symbolic planning, motion planning, reactive control) and is now struggling to reconnect them through TAMP. The drive program never separated them in the first place.

## Next Steps

Potential work items (prioritize based on current needs):

1. **Drive Program C module** — bidirectional Dijkstra using SQLite3 C filter, outputs S-expression behavior tree
2. **Schema refinement** — nodes table, connections table, line segments storage, properties for both navigation and manipulation maps
3. **CFL integration** — drive program emitting `.ctb` images or direct S-expression trees that the CFL runtime executes
4. **Recovery logic** — local subtree patching when a path is blocked, querying neighborhood through the filter
5. **Test map** — encode a small test environment (e.g., SPIKE Prime arena or a simple room layout) as a SQLite DB
6. **Manipulation pseudo-map** — named arm configurations as nodes, validated motions as paths, same Dijkstra search
7. **LuaJIT version** — drive program in LuaJIT using existing SQLite/lsqlite3 bindings for rapid prototyping
8. **PostgreSQL version** — swap filter to PG bindings for server-tier deployment, same drive program logic

## Key Design Principles

- The plan is the behavior tree. No separate plan representation and execution representation.
- Recovery is always local. Patch the smallest failing subtree.
- One algorithm (bidirectional Dijkstra) serves both initial planning and recovery.
- The map is a topological graph, not a grid. Nodes have meaning.
- The knowledge base is live — execution reads and writes status.
- Same architecture from Cortex-M to server. Only the database layer changes.
- No heavyweight dependencies. Dijkstra is 50-100 lines of C.

## Repository Context

- **glenn-edgar/knowledge_base_container** — ChainTree and related projects (MIT license)
- **glenn-edgar/knowledge_base** — ltree knowledge base project (MIT license)
- CFL runtime, S-expression engine, binary image system, SQLite3 C filter are existing components

