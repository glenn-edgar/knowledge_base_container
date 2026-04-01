# Session Summary — 2026-03-24
## Planner Architecture Analysis and Next-Generation Design

---

## What We Did

Reorganized the ros_planner directory structure, then conducted a deep architectural analysis comparing our planner against ROS planners. This led to a next-generation planner design: a two-tier KB-driven behavior tree architecture that addresses the gaps while preserving transparency.

### Directory Reorganization

```
ros_planner/
├── mkdocs.yml
├── docs/                              # 8 design documents
├── lua_dsl/                           # general purpose compiler + libs
│   ├── compile_mission.lua            # accepts path args now
│   └── spline.lua
└── lego_robots/
    └── lua_jit_dsl/                   # LuaJIT DSLs for lego robots
        ├── equipment_dsl.lua          # shared robot hardware
        ├── map_dsl.lua                # shared field layout
        └── missions/
            └── first_mission/         # per-mission directory
                ├── s_build.sh         # ./s_build.sh [--all]
                ├── mission_dsl.lua
                └── (output files)
```

Compiler updated to accept explicit paths: `luajit compile_mission.lua <equipment> <map> <mission> <output_dir> [flags]`. Each mission has its own `s_build.sh` that wires the paths.

---

## Gap Analysis: Our Planner vs ROS

### What ROS does that we cannot (currently)

| Capability | ROS (nav2 / PlanSys2) | Our Planner |
|---|---|---|
| Runtime replanning | POPF/Fast Downward replan on failure | Fixed compiled sequences |
| Dynamic obstacle avoidance | DWA/TEB local planner at 20Hz | Static splines, compile-time paths |
| Localization | AMCL particle filter, SLAM | Dead reckoning only (IMU + odometry) |
| Occupancy grid planning | A*/Dijkstra on dense costmap | Sparse 24-node waypoint graph |
| Sensor fusion | EKF fuses IMU, odometry, visual | Raw sensor reads |
| Precondition reasoning | PDDL solver searches action space | Preconditions stored but not queried |
| Reactive fallback | BT fallback nodes try alternatives | Guards abort/pause only |
| Multi-robot | ROS2 DDS, namespaced topics | Single robot |
| Continuous replanning | nav2 replans every N seconds | Path fixed at compile time |

### Why the DSL approach was chosen over raw s-expressions/XML

**Gains:** Domain vocabulary, compile-time validation (slot/jig checking), compile-time optimization (Dijkstra, spline fitting), separation of concerns (equipment/map/mission), multi-target portability, readable git diffs.

**Costs:** Runtime flexibility (tree is compiled and fixed), no direct engine access from DSL, tooling dependency (compiler is mandatory), abstraction leaks (DSL can't express every case), two mental models (DSL vs node indices), language lock-in (Lua tables).

**Key tradeoff:** DSL trades runtime power for compile-time safety and readability. Right trade for well-understood domains (FLL). Wrong trade for dynamic environments — unless we add the mechanisms below.

### Why LuaJIT over Python for DSLs

The original DSLs were in Python but pip dependency management became untenable. LuaJIT provides: single binary (500KB, zero deps), native C FFI (`ffi.cdef` + `ffi.C`), 5-10ms startup, 10MB total footprint on Pi Zero 2 W, perfect reproducibility (copy .lua files, done). Python's only advantage is ecosystem (numpy/scipy) which the DSL doesn't need.

---

## Next-Generation Architecture Design

### ChainTree Hidden Nodes Solve Runtime Flexibility

The "compiled and fixed" limitation is solved by ChainTree's hidden (controlled/dead) node mechanism. The tree structure is compiled and validated, but dormant subtrees can be activated at runtime via AVRC typed packets.

```
supervisor
├── active: run_1_sequence
│   ├── nav_to_m6
│   ├── do_m6_push_lever          ← primary
│   ├── [hidden] do_m6_retry_left  ← alternative, dormant
│   ├── [hidden] do_m6_retry_right ← alternative, dormant
│   ├── [hidden] do_m6_skip        ← fallback, dormant
│   └── ...
├── [hidden] run_1_recovery        ← exception handler subtree
└── [hidden] run_1_return_home     ← timeout handler subtree
```

The planner doesn't rewrite the tree. It activates hidden nodes when conditions change:
- Primary fails → supervisor catches exception → activates retry
- Retry fails → activates skip, planner moves on
- Time interval expires → activates return_home

**What ChainTree provides beyond standard BTs:**
- **Hidden nodes**: pre-compiled alternatives, validated at compile time, activated at runtime
- **AVRC typed packets**: structured planner-to-subtree communication with FNV-1a schema hashes
- **Supervisor + exception unwinding**: nested failures unwind correctly through the tree
- **Time intervals**: temporal planning built into the tick model, not external logic

### Two-Tier Planner Architecture

The planner is the outer loop. ChainTree runs are short-lived workers.

```
┌─────────────────────────────────────────────────┐
│  PLANNER (high-level behavior tree)             │
│                                                 │
│  KB queries → select next task → dispatch       │
│  monitor completion/exception → update KB       │
│  check goals → replan if needed                 │
│                                                 │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐     │
│  │ task BT  │  │ task BT  │  │ task BT  │     │
│  │ node     │  │ node     │  │ node     │     │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘     │
│       │              │              │           │
├───────┼──────────────┼──────────────┼───────────┤
        │              │              │
   ┌────▼─────┐  ┌────▼─────┐  ┌────▼─────┐
   │ shallow  │  │ shallow  │  │ shallow  │
   │ ChainTree│  │ ChainTree│  │ ChainTree│
   │ nav to   │  │ push     │  │ nav back │
   │ mission  │  │ lever    │  │ to home  │
   └──────────┘  └──────────┘  └──────────┘
```

**Planner level** (long-lived):
- Persists across entire match/shift
- Reads KB for world state, task status, time remaining
- Decides *what* to do next via precondition queries
- Dispatches shallow ChainTree runs for *how* to do it
- Handles exceptions — retry, skip, replan
- Updates KB with effects after success

**ChainTree level** (short-lived, disposable):
- One nav leg, one mission action, one recovery — 5-20 nodes
- Has safety guards (bump, tilt, battery)
- Returns success/exception to planner
- No KB awareness — just executes its slot sequence

**Planner pseudocode:**
```lua
while time_remaining() > reserve do
    local task = kb:query_next_applicable_task()
    if not task then break end

    local nav_tree = compile_nav(task.approach_waypoint)
    local result = chaintree.run(nav_tree)

    if result == EXCEPTION then
        kb:record_nav_failure(task)
        continue  -- planner decides: retry? skip? go home?
    end

    local action_tree = compile_action(task, current_jig)
    result = chaintree.run(action_tree)

    if result == SUCCESS then
        kb:apply_effects(task)
    else
        kb:record_action_failure(task)
    end
end
chaintree.run(compile_nav("home"))
```

### KB-Driven Planning Is Not a Black Box

Classical planners (PDDL solvers) are opaque — a C++ search algorithm produces an action sequence with no explanation. Our approach:

```
Planner BT node: "select next task"

    SELECT path, num_value
    FROM kb
    WHERE path LIKE 'plan.precond.%.requires.%'
    AND ... (preconditions match current state)
    ORDER BY points_per_second DESC

    The query IS the planning.
    The KB IS the world model.
    The result IS the decision.
```

Every decision is a SQL query against visible data. Every execution path is a tree structure you can draw. Nothing is hidden in a solver.

The tree gives you: transparent control flow, exception handling, tick-based reactivity.
The KB gives you: goal reasoning, state projection, temporal reasoning, inspectability.

### ROS Architectural Weakness We Avoid

In ROS, PlanSys2 (task planner), BehaviorTree.CPP (executor), nav2 (navigation), and MoveIt (manipulation) are separate subsystems with separate world models:
- Nav2 has its costmap
- PlanSys2 has its PDDL predicates
- MoveIt has its collision scene
- The behavior tree has its blackboard

Four representations of reality that can contradict each other. When nav fails, PlanSys2 only hears "failed" — not *why*.

Our architecture: **one KB, one tree engine, one world model.** Navigation leaves, mission actions, and safety guards live in the same tree. When nav fails, the exception propagates through the same supervisor that handles mission failures. The KB updates in one place. The planner's next query sees updated state immediately.

---

## Deployment Tiers

### FLL Competition (single hub)
Everything on the SPIKE Prime. Planner, shallow trees, KB — all in MicroPython. 77K flash, 32K heap.

### Local Swarm (zone controller + robots)

```
Zone controller (Pi Zero 2 W)
│  SQLite in-memory KB (:memory:, load/save at start/end)
│  LuaJIT planner BT + KB Manager
│  AVRC dispatch over BLE/WiFi/Thread
│
├── Robot A (C ChainTree runtime, shallow trees)
├── Robot B
└── Robot C
```

SQLite in-memory eliminates disk I/O. 0.01-0.05ms query latency. 50+ precondition queries per tick. Load from disk at startup, periodic checkpoints, save on shutdown.

### Factory (PostgreSQL + zone controllers)

```
PostgreSQL + PostGIS (factory floor)
├── Global world model, production schedule
├── PostGIS spatial queries across facility
├── LISTEN/NOTIFY to zone controllers
│
└── Zone controllers (SQLite local swarm)
    ├── Local planner, local task dispatch
    ├── Offline-capable (keeps working if DB unreachable)
    └── Robots (ChainTree runtime)
```

| Tier | Engine | KB | Decides |
|---|---|---|---|
| Factory | Production scheduler | PostgreSQL + PostGIS | What to make, where to send it |
| Zone | Planner BT | SQLite in-memory + ltree | Which robot, which task, when |
| Robot | Shallow ChainTree | In-memory blackboard | How to execute one nav/action |

KB Manager code is identical at PostgreSQL and SQLite tiers — same ltree queries, same table schemas. Only the connection string and spatial queries differ.

---

## Existing Building Blocks

### Already built and tested:
- **KnowledgeBaseManager** (`knowledge_base/sqlite3/construct_kb/`) — ltree queries, status/job/stream/RPC/bit-mask tables
- **Mission compiler** (`ros_planner/lua_dsl/`) — Dijkstra, splines, jig validation, 4 output artifacts
- **S-expression engine** — se_state_machine, se_verify, se_cond, se_sequence, all control flow
- **ChainTree runtime** — supervisor, exception handling, hidden nodes, AVRC packets
- **SPIKE DSL helpers** (`se_lego_spike.lua`) — 50+ leaf functions, guards, composites
- **Spline library** — Catmull-Rom fit, Bezier evaluation, differential wheel speed calculation

### The bridge that's missing:
A runtime executor that connects the compiler output to the KB Manager:

1. Load compiler output into KB Manager schema (not flat path/value table)
2. Precondition query engine — ltree pattern matching against current world state
3. Effect application — KB CRUD after mission success
4. Goal satisfaction check — query KB to see if we're done
5. Plan execution tracking — job tables for dispatch, execution_log for history
6. Replan trigger — ChainTree hidden node activation on failure

This bridge belongs in `lua_dsl/` as the general-purpose runtime planner. ~300-500 lines of LuaJIT.

---

## What's Next — 2026-03-25

### Suggested plan (in priority order):

1. **Build the planner runtime bridge** — Connect KB Manager to the compiler output. This is the core missing piece. Load compiled plan into KB Manager schema, implement the planner loop (query → dispatch → result → update → next).

2. **Refactor compiler for two-tier output** — Instead of one giant state machine per run, emit a library of shallow trees (nav_trees/, action_trees/) plus a planner script that sequences them. The planner BT queries KB and dispatches individual trees.

3. **Add hidden node / retry support to DSL** — `{ op = "supervised", primary = {...}, retry = {...}, fallback = {...} }` in mission templates. Compiler emits primary active + alternatives hidden. ChainTree supervisor wires the exception paths.

4. **Integrate KB Manager tables** — Map equipment status to KB status tables, mission dispatch to job tables, AVRC communication to RPC tables, guard/completion flags to bit mask tables.

5. **Test on FLL mission** — Run the two-tier planner against the first_mission DSL. Verify it produces the same 120 points but now with retry/skip capability on failure.
