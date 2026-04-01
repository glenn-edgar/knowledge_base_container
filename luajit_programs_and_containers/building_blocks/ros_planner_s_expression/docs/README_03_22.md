# Session Summary — 2026-03-22
## Planner Architecture Design for ChainTree + SPIKE Prime

---

## What We Did

Reviewed the Sonnet-generated ROS planner documentation (6 markdown files + continue.md) and performed critical analysis. Then iteratively designed the actual planner architecture through discussion, arriving at a complete top-level design.

### Files Reviewed

| File | Verdict |
|------|---------|
| `README_background.md` | Good philosophical analysis of STRIPS/BDI/BT traditions. Over-academic, no engineering guidance. |
| `README_integrated_planner.md` | **Wrong file.** Contains Pybricks motor/sensor API reference, not a planner doc. Sonnet misnamed or overwrote it. |
| `README_PlanSys2_input_output.md` | Thorough PDDL pipeline walkthrough. Documents PlanSys2 well but describes an architecture we're not using. |
| `README_knowledge_base_input_storeage.md` | Good ltree path conventions for PDDL data. Has implementation bugs (backup API inversion, SQL injection, broken sync triggers). |
| `README_knowledge_base_map_integration.md` | Good three-layer map architecture. Dual-database pattern is right but implementation has destructive flush bug. |
| `README_knowledge_base_gaps_filled.md` | Schema init, type resolver, PDDL assembler, effect engine. SQL injection in type_resolver.lua. |
| `continue.md` | Lego SPIKE DSL continuation notes. Useful context. |

### Key Critique of Sonnet Docs

The Sonnet output produced a thorough **data storage layer** and strong **philosophical framing** but the actual planning system is absent. The docs describe storing PDDL artifacts and invoking external solvers (POPF, Fast Downward) that are heavyweight C++ programs unsuitable for the target deployment. There is no planner implementation.

### Files Created

| File | Content |
|------|---------|
| `README_planner_spike_interface.md` | Complete specification of the planner-to-target interface: transport abstraction, protocol spec, command vocabulary, status stream, target-side MicroPython architecture, host-side integration. |

---

## Architecture Decisions Made

### 1. No External PDDL Solver

The planner does not generate PDDL text or invoke POPF/Fast Downward. Instead, the planner queries the SQLite KB directly for applicable actions (preconditions match current state) and performs shallow forward search (1-3 steps). The KB is the planner. PDDL is unnecessary overhead.

### 2. ChainTree Dead Nodes Are the Plan Library

Each possible action is a pre-built controlled (dead) node in the ChainTree. The planner activates dead nodes in sequence. Controlled nodes communicate via typed AVRC packets (request_port / response_port with FNV-1a schema hashes). The planner doesn't generate new behavior — it selects and activates existing dormant nodes.

### 3. ChainTree Runtime as RPC Server

The ChainTree runtime on the host acts as an RPC server. The planner sends commands to ChainTree, which dispatches to the target over the transport layer. Initially BLE, with Thread and WiFi as future options.

### 4. MicroPython Target Is a Dumb Actuator

The SPIKE Prime hub runs MicroPython (Pybricks). It receives RPC commands, executes motor/sensor calls, and returns results. All planning intelligence lives on the host. The hub doesn't need to know about goals, plans, or world state.

### 5. Outbound Status Stream

The SPIKE hub pushes sensor/motor state to the host at 10Hz over BLE notify (Nordic UART TX characteristic), independent of command traffic. The host blackboard is continuously updated. The planner reads fresh sensor state from the blackboard with zero BLE latency. Commands are the only thing requiring round-trip RPC.

### 6. Single-Leaf-Per-Global-Tick Execution Model

On the target, the behavior tree runs one leaf node per global tick. Once a leaf starts, it enters a 10ms monitor tick loop polling for completion or fault. No concurrent leaf execution. Deterministic, correct for MicroPython's cooperative multitasking.

Key patterns:
- **Centralized `guard_check()`** called on every 10ms monitor tick. Safety logic is not duplicated per leaf.
- **`try/finally` cleanup** in every leaf guarantees hardware is safe on exit.
- **Three coroutines** via Pybricks `multitask`: status_stream (always), command_listener (always), tree_executor (sequential leaves).

### 7. Transport-Agnostic Protocol

All messages are newline-delimited JSON with single-character prefix:
- `C:` — command (host → target), carries sequence number
- `R:` — response (target → host), echoes sequence number
- `S:` — status stream (target → host, unsolicited, 10Hz)

Idempotent commands via sequence numbers handle BLE/UDP unreliability.

### 8. Virtual Function Architecture

ChainTree and s-expression engines are language-neutral. Leaf nodes are virtual function slots — function pointers resolved at link time. The same tree image (binary node table + slot indices) runs on any target by swapping the binding table:
- BLE RPC stubs (host sends JSON command, waits for response)
- C direct calls (bare metal target)
- MicroPython direct calls (tree runs on hub natively)

### 9. Four DSLs

| DSL | Defines | Changes When |
|-----|---------|-------------|
| **Equipment & I/O** | Hardware config: motors, sensors, ports, guard thresholds. Implicitly defines virtual function slots and status stream fields. | Robot is rebuilt |
| **Map** | Waypoints, topology edges, zones, distances. Spatial knowledge separate from hardware. | Operating area changes |
| **Mission** | Goals, action sequences (explicit steps or precondition/effect schemas for planning). Compiles to s-expression tree image. | Task changes |
| **Target I/O** | Binding table: maps virtual function slots to RPC commands per target. Merged with Equipment in practice. | New hardware type added |

The mission DSL IS s-expression composition — same `se_sequence`, `se_verify`, `se_cond` primitives. The leaf functions are virtual slots, not concrete implementations. Equipment and Target I/O DSLs define what those slots are and how they're bound.

### 10. KB Is the Planner — Language Independent

The knowledge base (SQLite + ltree) has been ported to Go, Python, C#, and LuaJIT. The planner is KB queries + tree dispatch. The four DSLs compile to portable artifacts:
- `robot_kb.sqlite` — all domain knowledge, equipment, map, mission, world state
- Tree image (binary) — node table, virtual function slot indices
- Binding table (JSON/KB rows) — slot-to-RPC command mapping

Take these three files to any host with KB bindings. Plug in transport. The planner works. ~300-500 lines of language-specific code per platform.

LuaJIT chosen first because it runs on Pi Zero 2 W (512MB RAM, ARM Cortex-A53). Entire planner stack under 10MB.

### 11. PostgreSQL + PostGIS Option

For larger deployments (building-scale, multi-robot), PostgreSQL provides:
- Native ltree (no extension to maintain)
- PostGIS spatial queries (nearest waypoint, path-obstacle intersection, zone containment)
- Multi-user access for coordinated planning

SQLite is the baseline. Map DSL stores coordinates in data fields so PostGIS migration can promote them to geometry columns later.

---

## What's Next — 2026-03-23

**Set up a LEGO robot competition problem and design the DSL to specify it.**

This means:
1. Define a concrete competition scenario (course layout, objectives, scoring)
2. Write the Equipment DSL for a specific SPIKE Prime robot configuration
3. Write the Map DSL for the competition course
4. Write the Mission DSL with explicit step sequences (Option B — no automatic planning yet)
5. Validate that the DSL captures everything the planner needs

This forms the base for more advanced work: automatic planning from precondition/effect schemas, multi-target coordination, PostGIS spatial reasoning.
