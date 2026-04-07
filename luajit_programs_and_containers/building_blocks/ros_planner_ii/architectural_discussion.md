# Architectural Discussion: Nano Datacenter Edge Robotics

A comparison of the ROS Planner II nano datacenter architecture against NASA's Artemis-era lunar robotics software stack and major commercial fleet robotics systems, and the design rationale for our approach.

## Context

NASA's planned lunar robotic operations under Artemis use a modular, hybrid, standards-based ecosystem built on **core Flight System (cFS)** for flight software and **Space ROS** for robotics. It emphasizes certifiability (DO-178C), onboard autonomy (for Earth-Moon comms delays), and interoperability across rovers, construction bots, ISRU systems, and swarms. Communication rides on the **LunaNet** DTN backbone.

Our architecture takes a fundamentally different approach: **put a datacenter at the operations site and treat robots as edge clients.**

---

## The Two Philosophies

| | NASA (probe mentality) | Nano Datacenter (factory-floor mentality) |
|---|---|---|
| Core assumption | Comms are unreliable, each robot is on its own | Local infrastructure is reliable, robots are clients |
| Robot intelligence | Push autonomy onboard because comms fail | Keep robots simple because the hub is local |
| Fault tolerance | Onboard FDIR, radiation hardening, formal verification | Restart containers, replan missions, link protocol handles disconnection |
| Scaling model | Each robot is a self-contained mission | Each robot is a client; add robots, same infrastructure |
| New capability | New cFS app, qualification, integration | KB entry + worker function |
| Cost driver | Per-robot flight software | Per-site infrastructure (amortized across fleet) |
| Hardware | $200K rad-hardened processors, 10W each | $80-150 ARM SBCs or $20-150 ASIL-D automotive silicon |

NASA's approach treats every robot like a deep-space probe that needs its own flight software stack. Our approach treats the operations site like a factory floor — which is what a permanent base actually is.

---

## Architectural Alignment

Despite the philosophical difference, the two architectures are structurally parallel:

| NASA Artemis | ROS Planner II |
|---|---|
| cFS table management + ground databases | SQLite KB — single source of truth |
| ROS 2 action servers (perception, nav, manipulation) | Virtual nodes — 12 atomic action types, data-driven from KB |
| BehaviorTree.CPP in ROS 2 | ChainTree DSL — controller + 12 workers |
| cFS software bus | MQTT (robot transport) + NATS (infrastructure) |
| ROS 2 DDS middleware | NATS JetStream + KV (telemetry, state, persistence) |
| Ground-side planning (VIPER model) | Planner container with Dijkstra + route builder |
| Per-robot flight qualification | Link protocol capability announcement — robot is authoritative |
| Gazebo simulation | Same codebase, swap worker functions (sim → hardware) |

Both architectures are layered, modular, KB-driven, capability-validated, and simulation-first. The structural similarity is not coincidental — these are the right patterns for multi-robot mission planning regardless of deployment environment.

---

## The Nano Datacenter

Our system is not "hobby scale with MQTT on a LAN." It is a containerized edge datacenter running the full infrastructure stack at the operations site:

| Service | Role | Container |
|---------|------|-----------|
| NATS (JetStream + KV) | Message bus, state store, telemetry streams, job queue | nats-js-ram |
| Postgres | Durable persistent state | postgres |
| MQTT (Mosquitto) | Robot command/response transport | mosquitto-ram-ws |
| KV Bridge | Async MQTT-to-NATS KV writer (off the hot path) | kv-bridge |
| OpenResty | API gateway, federated UI | openresty |
| Planner | Per-domain mission planning and execution | ros-planner |
| SQLite | Knowledge base (bind-mounted, read-only) | host filesystem |

This entire stack runs on a **Pi 5 with 16GB RAM at 8 watts**. The nano datacenter fits in the palm of your hand. It draws less power than a single NASA RAD750 flight computer.

### Infrastructure Comparison

| Function | NASA Artemis | Nano Datacenter |
|----------|-------------|-----------------|
| Message persistence | DTN Bundle Protocol store-and-forward | NATS JetStream (replay, persist, history) |
| State distribution | Ground databases + uplink tables | NATS KV + Postgres |
| Service discovery | LunaNet PNT + manual config | KB infrastructure query (SQLite to runtime) |
| Telemetry collection | DSN downlink + ground processing | NATS JetStream streams + KV-bridge |
| API gateway | Mission control interfaces | OpenResty |
| Multi-domain support | Per-mission ground segment | Per-domain SQLite DB, same container image |

---

## Certified Silicon Is Commercial Off-the-Shelf

The assumption that space robotics requires expensive radiation-hardened processors is outdated. The automotive industry solved safety-certified silicon at scale:

| Vendor | Chip | Features | Certification |
|--------|------|----------|---------------|
| TI TDA4 / Jacinto | Cortex-A72 + R5F | ECC RAM, flash parity, lockstep cores | ISO 26262 ASIL-D |
| NXP S32G / i.MX 9 | Cortex-A55/A72 + M7 | ECC, safety island, secure boot | ISO 26262 ASIL-D |
| Renesas R-Car H4 | Cortex-A76 + R52 | Lockstep, ECC, built-in self-test | ISO 26262 ASIL-D |
| Infineon AURIX | TriCore lockstep | Full ECC, parity, safety MPU | ISO 26262 ASIL-D |

ASIL-D is automotive's highest safety integrity level, comparable to DO-178C Level A. These chips have hardware-level fault detection — ECC on all RAM, parity on flash, lockstep cores that vote on every cycle. They cost $20-150 and run Linux containers.

SpaceX proved this approach: Falcon 9 and Dragon run on redundant x86 Linux clusters, voting across processor strings for fault tolerance instead of relying on rad-hardened single-board computers. Commercial off-the-shelf hardware with redundancy beats expensive custom silicon.

The Mars Helicopter (Ingenuity) proved ARM can fly on another planet — it runs a Qualcomm Snapdragon 801.

---

## Tiered Autonomy via Container Deployment

The planner is a Docker container. A container runs anywhere arm64 Linux runs. This enables hierarchical autonomy without architecture changes:

```
Tier 1: Site Hub (Pi 5 / TI TDA4 in shielded enclosure)
  +-- Full nano datacenter: NATS, Postgres, MQTT, OpenResty
  +-- Fleet-wide planning, telemetry, mission log
  +-- Earth uplink (when available)

Tier 2: Commander Robot (TI TDA4 / NXP S32G on rover)
  +-- Local planner container (same image as site hub)
  +-- Local NATS + MQTT (lightweight, on-robot)
  +-- Controls slave robots via same link protocol
  +-- Operates autonomously if site hub is unreachable

Tier 3: Slave Robots (ESP32 / Pi Zero 2 / AURIX)
  +-- MQTT client only
  +-- Receives commands from Tier 2 commander
  +-- Same protocol: announce, confirm, RPC, ACK, KB_DONE
```

The commander robot runs the **exact same planner container** with its own KB scoped to its work cell. It plans locally, dispatches to slaves, reports aggregate results up to the site hub. If the site hub goes down, the commander keeps operating.

There is no single point of failure because the same container can run at multiple tiers. The planner is not a centralized service — it is a deployable unit. Put it wherever you need planning authority.

---

## The Earth-Moon Link Is Out of Scope

The Earth-Moon communications relay is an external subsystem, not part of the surface operations architecture. This is the same approach used by:

- **Mars Helicopter (Ingenuity)** — ran autonomy locally, talked to Perseverance over short-range radio. Perseverance handled the Earth relay. The helicopter did not know or care about the 4-24 minute delay to Earth.
- **NASA orbiters (MRO, MAVEN)** — the VM-based command systems treat the deep-space link as an external service.

```
Our scope                           Not our scope
------------------------------      ------------------

Surface Ops Site                    Earth Link
+-------------------------+        +-----------------+
| Nano Datacenter         |        | DSN / Relay     |
|  NATS <-> Postgres      |--NATS->| DTN gateway     |
|  MQTT <-> Robots        |  link  | Earth ground    |
|  Planner containers     |<-NATS--| segment         |
+-------------------------+        +-----------------+
```

The nano datacenter exposes NATS subjects. An external gateway subscribes to telemetry streams and publishes commands. Whether that gateway talks to Earth via DTN, laser relay, orbital store-and-forward, or direct radio is its problem. The surface system sees it as another NATS client.

---

## LuaJIT: C-Native Scripting for Real-Time Systems

LuaJIT is not competing with Python. It is a **C scripting layer** — the same pattern used by game engines (WoW, Roblox, CryEngine, Defold) for decades.

### LuaJIT vs Python for Edge Robotics

| Metric | LuaJIT | Python (CPython) | Factor |
|--------|--------|-------------------|--------|
| Startup time | ~2ms | ~30-50ms | 15-25x |
| Memory footprint | ~1-2MB | ~15-30MB | 15x |
| FFI call overhead | ~2-5ns (no binding code) | ~100-500ns (ctypes/cffi) | 50-100x |
| Execution speed | Near C (JIT compiled) | 50-100x slower (interpreted) | 50-100x |
| GC pauses | Incremental, microseconds | Stop-the-world, milliseconds | 1000x |
| Cross-compile for ARM | Trivial | Full toolchain + pip ecosystem | -- |

### The FFI Advantage

All I/O libraries — libnats, libmqtt_pubsub, liblua_cbor, ltree — are C libraries called directly from LuaJIT FFI with zero binding code:

```lua
-- Declare the C function, call it. Done.
ffi.cdef[[ int nats_publish(const char *subject, const char *data, int len); ]]
local rc = lib.nats_publish("topic", payload, #payload)
```

Python requires ctypes boilerplate, C extension modules with `PyObject*` ceremony, or cffi wrappers. Every boundary crossing fights the GIL. LuaJIT FFI calls C functions at the same cost as C calling C — the JIT compiler inlines FFI calls into native machine code.

### Why It Matters for This Architecture

- **Hub runtime tick loop** — microsecond granularity polling. Python's GC pauses introduce millisecond jitter. LuaJIT's incremental GC does not stall.
- **ChainTree execution** — 137 behavior tree nodes ticking every cycle. LuaJIT JIT-compiles the hot paths.
- **Container footprint** — Planner container is 150MB. Python equivalent with dependencies would be 400-600MB.
- **Startup** — Bootstrap queries SQLite, builds runtime KB, exports to NATS KV, starts planner. Sub-second in LuaJIT.
- **Deterministic timeouts** — ACK timeout 5s, KB_DONE timeout 10s. Python's GIL + GC can introduce 10-50ms pauses that cascade into false timeouts.
- **Same language at every tier** — Site hub, commander robot, slave robot ChainTree — all LuaJIT calling C via FFI. One language, one runtime, zero impedance mismatch.

---

## Zig for Safety-Critical and Hardware Functions

For safety-related functions, hardware drivers, and non-scripting code on certified ARM silicon, we use **Zig** instead of Rust or C.

### The Language Stack

| Layer | Language | Role |
|-------|----------|------|
| Mission planning, coordination | LuaJIT | Scripting, behavior trees, KB queries |
| Protocol handling, telemetry | LuaJIT | Rapid iteration, FFI to infrastructure |
| Hardware drivers, sensor fusion | Zig | Memory safe, no GC, direct hardware access |
| Safety functions, FDIR, watchdogs | Zig | Deterministic, auditable, no hidden control flow |
| Infrastructure (NATS, Postgres, MQTT) | Existing C/Go | Battle-tested, containerized |

### Why Zig Over Rust

**C interop is native, not adversarial.** Zig calls C headers directly with `@cImport`. Zig exports C ABI functions with `export fn`. No `unsafe` blocks, no `extern "C"`, no `#[no_mangle]`, no bindgen. LuaJIT FFI declarations map 1:1 to Zig exports. Zero friction at the boundary.

Rust's C FFI is wrapped in `unsafe` because the language philosophically distrusts C. For a system where LuaJIT calls C libraries on every tick, that ceremony is constant drag.

**Arenas match the allocation pattern.** The hub runtime ticks at fixed intervals. Each tick allocates working state, processes messages, publishes results. Arena allocation means: allocate everything for this tick from one arena, process, reset — one operation, zero fragmentation, zero GC. The tick loop has a natural allocation lifetime. Arenas express that directly.

Rust's ownership model solves a different problem — long-lived objects with complex sharing. Safety-critical robotics code has simple lifetimes: per-tick, per-command, per-mission. Arenas are the right tool.

**No hidden control flow.** Zig has no hidden allocations, no operator overloading, no implicit conversions, no exceptions, no destructors running at scope exit. For safety-critical code on certified silicon, you need to read the code and know exactly what the machine does. Rust's `Drop` traits, `Deref` coercions, and `?` operator hide control flow that matters in safety contexts.

**Comptime replaces macro complexity.** Zig's compile-time execution is plain Zig code. Rust's proc macros are a separate language with a separate toolchain. For generating packet type dispatch tables:

```zig
const handlers = comptime blk: {
    var table: [256]HandlerFn = .{null_handler} ** 256;
    table[1] = init_check;
    table[2] = path_spline;
    // ... 12 VN types
    break :blk table;
};
```

**Cross-compilation is trivial.** `zig build -Dtarget=aarch64-linux-gnu` — done. Zig ships its own linker and libc. Build `.so` files for TI TDA4 on your dev machine. Same artifact strategy as the current prebuilt `.so` approach but reproducible from source.

**Zig is the better C, not the better C++.** Rust replaces C++. Zig replaces C. This architecture uses C libraries with a scripting layer — we need a better C, not a systems language with a runtime and a package ecosystem.

### How Zig Integrates

```
LuaJIT (planner container, robot ChainTree)
    |
    |  FFI calls -- same declarations as today
    v
Zig .so libraries (replace current C .so files)
    |
    +-- nats_client.zig       -- replaces libnats.so
    +-- mqtt_client.zig       -- replaces libmqtt_pubsub.so
    +-- cbor_codec.zig        -- replaces liblua_cbor.so
    +-- safety_monitor.zig    -- watchdog, e-stop, fault isolation
    +-- motor_driver.zig      -- PWM, encoder, PID
    +-- sensor_hub.zig        -- IMU, distance, color, force
    +-- power_manager.zig     -- battery curves, thermal, charging
        |
        |  Direct hardware access
        v
    TI TDA4 / NXP S32G peripherals
```

LuaJIT does not know or care that the `.so` changed from C to Zig. The FFI declarations stay the same. The ABI is identical.

---

## The Game Engine Parallel

SpaceX hires game programmers. The reason is architectural — game engines and real-time robotics systems share the same patterns:

| Game Engine Pattern | ROS Planner II |
|---|---|
| Entity-Component-System (ECS) | Virtual nodes — atomic actions composed into missions |
| Behavior trees (Unreal, Unity) | ChainTree DSL — controller + workers |
| Game loop (fixed timestep tick) | Hub runtime tick, robot tick, action server loop |
| Scripting layer over C engine | LuaJIT FFI over C/Zig libraries |
| Asset hot-reload / data-driven design | KB-driven config — change the DB, not the code |
| Client-server with authoritative server | Planner is authoritative, robots are clients |
| Netcode state reconciliation | Delta pose — robot reports deltas, planner accumulates |
| Multiple players on one server tick | Coroutine-based N concurrent missions |
| Frame-time budget | Energy budget per mission |

Game programmers think in **ticks, state machines, message passing, and fixed budgets**. They write C with a scripting layer on top. They optimize for deterministic real-time behavior. They handle thousands of entities with cooperative scheduling.

The entire ROS Planner II system — nano datacenter as game server cluster, NATS as message bus, planner as session manager, robots as game clients running behavior tree AI, delta pose as netcode reconciliation — is a multiplayer game server with robot clients.

NASA hires aerospace engineers who learn to code. SpaceX hires game programmers who learn aerospace. This architecture is built for the second kind of person.

---

---

## Comparison with Commercial Fleet Robotics Systems

### The Commercial Landscape

The major commercial multi-robot systems fall into three architectural camps:

| Camp | Systems | Planning | Robot Intelligence |
|------|---------|----------|--------------------|
| Fully centralized | Amazon Robotics (Kiva) | Server plans all paths (MAPF) | Simple — follow waypoints on grid |
| Hybrid (central task, onboard nav) | Locus, Fetch/Zebra, MiR, OTTO, Boston Dynamics | Server assigns tasks, robot navigates | Full onboard SLAM, obstacle avoidance |
| Onboard-heavy | Waymo, John Deere, DJI | Cloud for logistics only | Full autonomy onboard |

Our architecture aligns most closely with the **Amazon Robotics / Kiva model** — centralized planning with simple robot clients — but achieves it without constraining the physical environment.

### System-by-System Comparison

#### Amazon Robotics (Kiva) — Closest Match

Amazon is the most architecturally similar commercial system to ours:

| Aspect | Amazon Robotics | ROS Planner II |
|--------|----------------|----------------|
| Planning | Central server, Dijkstra/MAPF on discrete graph | Central planner container, Dijkstra on board graph |
| Robot role | Follow pre-computed waypoint sequences | Execute VN commands from planner |
| Localization | QR codes on warehouse floor | Known graph coordinates, delta pose from workers |
| Communication | Proprietary Wi-Fi | MQTT (standard, any client) |
| Fleet scale | 800+ robots per warehouse | N robots per site, same infrastructure |
| Onboard compute | ARM embedded controller | ESP32 / Pi Zero 2 / TI TDA4 |
| World model | Discrete grid (QR-code tiles) | Waypoint graph (KB-defined boards) |
| Adding capabilities | Software update to central planner + robot firmware | KB entry + worker function |

**Key difference:** Amazon constrains the physical environment (grid floor with QR codes) to make centralized planning tractable. We achieve the same centralized model by operating on a known waypoint graph. Both approaches avoid the complexity of continuous motion planning by discretizing the world.

**Key advantage we hold:** Amazon's system is proprietary end-to-end. Our system uses standard protocols (MQTT, NATS) and open infrastructure. A new robot implementation needs only an MQTT client and 10 message types.

#### Warehouse AMRs (Locus, Fetch/Zebra, MiR, OTTO) — Different Split

These systems give robots significantly more intelligence than we do:

| Aspect | Warehouse AMRs | ROS Planner II |
|--------|---------------|----------------|
| Planning split | Server assigns task + goal; robot plans path | Server plans full route; robot executes commands |
| Onboard stack | Full ROS nav stack (SLAM, AMCL, move_base) | ChainTree behavior tree, no onboard planning |
| Onboard compute | x86 or Nvidia Jetson (heavy) | ESP32 to Pi Zero 2 (light) |
| Footprint | Ubuntu + ROS = 2-4GB on robot | LuaJIT + ChainTree = 10-20MB on robot |
| Traffic management | Zone locking, priority queues | Planner owns full route, no contention protocol yet |
| Protocol | ROS topics internally, REST/MQTT to fleet server | MQTT everywhere, NATS for infrastructure |
| Adding capabilities | ROS package + integration testing | KB entry + worker function |
| Runtime | C++ nav stack, Python orchestration | LuaJIT scripting, C/Zig drivers via FFI |

**Their advantage:** Onboard navigation handles unknown obstacles, dynamic environments, and sensor-based replanning. A MiR robot can navigate around a person standing in the aisle. Our robots follow pre-planned routes.

**Our advantage:** Their robots are expensive computers running Ubuntu + ROS. Our robots are cheap microcontrollers running a behavior tree. Their fleet server is a web application with REST APIs bolted on. Our fleet infrastructure is a full datacenter (NATS, Postgres, MQTT) that handles persistence, telemetry, streaming, and job queuing natively.

**Our advantage 2:** Their "add a capability" story is: write a ROS package in C++, integrate with the nav stack, test across the fleet, deploy firmware update. Ours is: add a row to the KB, add a worker function in Lua. Zero planner changes.

#### Boston Dynamics (Spot + Orbit) — Heavyweight Onboard

| Aspect | Spot + Orbit | ROS Planner II |
|--------|-------------|----------------|
| Onboard intelligence | MPC locomotion, GraphNav visual navigation, full perception | Execute VN commands, delta pose reporting |
| Fleet manager | Orbit (on-prem server, Docker-based) | Planner container (same — on-prem, Docker) |
| Mission definition | Autowalk recording or SDK behavior trees | KB-defined routes via Dijkstra |
| API | gRPC + protobuf (Spot SDK) | MQTT + JSON/CBOR |
| Navigation | Topological visual graph (recorded routes) | Waypoint graph (KB-defined boards) |
| Robot cost | $75,000+ | $10-150 (SBC/microcontroller) |
| Target environment | Unstructured (industrial sites, construction) | Structured (known waypoint graphs) |

**Their advantage:** Spot operates in genuinely unstructured environments — stairs, rubble, outdoor terrain. Our system requires a known waypoint graph.

**Our advantage:** Orbit manages Spots for inspection routes in known facilities. At that point, the environment is structured and mapped. Our architecture serves the same use case (planned missions across known sites) at 1/500th the robot hardware cost. And our planner deploys at multiple tiers — Spot cannot run Orbit on itself.

**Architectural note:** Spot's GraphNav (topological visual navigation along recorded routes) is conceptually similar to our board graph, just richer. Both systems navigate by following a graph of known waypoints. The difference is sensor-based localization vs coordinate-based.

#### Industrial Arms (ABB, FANUC, KUKA) — Different Problem

| Aspect | Industrial Arms | ROS Planner II |
|--------|----------------|----------------|
| Controller | Dedicated real-time controller per robot (VxWorks/QNX) | Shared planner container for fleet |
| Coordination | PLC as master sequencer (EtherNet/IP, PROFINET) | Planner as master sequencer (MQTT, NATS) |
| Programming | Proprietary languages (RAPID, KAREL, KRL) | LuaJIT + KB definitions |
| Servo rate | 1-8 kHz | Hub tick rate (milliseconds, not microseconds) |
| Multi-robot | Controller-to-controller sync, max 4 (ABB MultiMove) | Coroutine-based N concurrent missions |
| Adding capability | New program in vendor language, teach pendant | KB entry + worker function |

**Not directly comparable** — these are fixed-base manipulators with hard real-time servo loops. But the coordination pattern is relevant: a PLC sequences actions across multiple arms, similar to how our action server sequences missions across multiple robots.

**Our advantage:** Industrial robots are vendor-locked. ABB programs don't run on FANUC. Our system is protocol-defined — any MQTT client implementing 10 message types is a compatible robot, regardless of vendor, language, or hardware.

#### Waymo / Autonomous Vehicles — Onboard Everything

| Aspect | Waymo | ROS Planner II |
|--------|-------|----------------|
| Onboard compute | Custom high-performance (TPU/GPU), multi-board redundant | ESP32 to TI TDA4 |
| Planning | Full onboard: route + behavior + trajectory optimization | Full server-side: Dijkstra + route builder |
| Cloud role | Fleet dispatch, ML training, HD map updates, monitoring | Everything — planning, execution, telemetry, state |
| Safety model | Must drive safely with zero cloud connectivity | Must have planner connectivity to operate |
| Perception | LiDAR + camera + radar fusion, ML models | None (known graph) |
| Cost per robot | $100,000+ compute hardware | $10-150 |

**Not directly comparable** — autonomous vehicles must operate in open-world environments with human safety at stake. The onboard-heavy architecture is a regulatory and safety requirement, not a choice.

**Architectural lesson:** Waymo's cloud does fleet logistics (dispatching, positioning, ride matching). Our NATS job queue + action server does the same thing. The difference is what runs onboard — they pushed everything safety-critical to the robot. We pushed everything planning-critical to the hub. Different problems, same layering.

#### DJI FlightHub — Closest Commercial Fleet Pattern

| Aspect | DJI FlightHub | ROS Planner II |
|--------|---------------|----------------|
| Fleet management | Cloud-hosted web app | On-site nano datacenter |
| Mission model | Upload waypoint mission to drone, execute autonomously | Plan route from KB, push VN commands to robot |
| Communication | Proprietary radio (OcuSync) + cellular backhaul | MQTT (standard) |
| Onboard | Proprietary RTOS on custom SoC | ChainTree on LuaJIT |
| Automated ops | DJI Dock (drone-in-a-box, auto charge/launch) | Recharge VN at charging_station node |
| Multi-robot coord | None — each drone independent | Full fleet coordination via action server |

**Our advantage:** DJI FlightHub has no real-time multi-drone coordination. Each drone operates independently. Our action server runs N concurrent missions with coroutine scheduling and shared fleet state. DJI requires cloud connectivity; our system runs entirely at the edge.

#### John Deere — Agricultural Pattern

| Aspect | John Deere | ROS Planner II |
|--------|-----------|----------------|
| Navigation | RTK-GPS centimeter positioning on pre-planned field paths | Known waypoint graph with delta pose tracking |
| Planning | Coverage patterns defined in cloud Operations Center | Routes defined by Dijkstra over board graph |
| Onboard | Nvidia GPU for ML (See & Spray), ARM for vehicle control | ESP32 to TI TDA4, ChainTree workers |
| Communication | Cellular + satellite (JDLink telematics) | MQTT (local network) |
| Fleet coord | Per-machine (cloud assigns machines to fields) | Per-fleet (action server coordinates robots) |

**Architectural similarity:** Both operate in structured, pre-mapped environments. Both pre-plan paths (field coverage patterns / Dijkstra routes). Both execute onboard with position tracking. The difference is Deere's machines operate independently; our robots are coordinated by a central planner.

### Cross-Cutting Comparison

| Capability | Amazon | AMRs | Spot | Industrial | Waymo | DJI | Deere | **Ours** |
|---|---|---|---|---|---|---|---|---|
| Central planning | Yes | Task only | Schedule | PLC | No | Schedule | Cloud plan | **Yes** |
| Onboard nav | No (grid) | Full SLAM | GraphNav | Servo only | Full | Waypoint | RTK-GPS | **Tiered (graph to SLAM)** |
| Fleet coordination | MAPF | Zone lock | Orbit | PLC sync | Dispatch | None | None | **Coroutines** |
| Standard protocol | No | REST/MQTT | gRPC | EtherNet/IP | No | No | ISOBUS | **MQTT/NATS** |
| Data-driven config | Limited | ROS params | SDK | Teach pendant | HD maps | Mission upload | Operations Center | **Full (KB)** |
| Add capability | Firmware | ROS package | SDK app | Vendor program | ML model | Payload SDK | Implement | **KB + worker** |
| Open robot protocol | No | Partial | Yes (SDK) | No | No | Partial (SDK) | No | **Yes (10 msgs)** |
| Edge infrastructure | Proprietary | Fleet server | Orbit server | PLC network | Cloud | Cloud | Cloud | **Full datacenter** |
| Robot hardware cost | Low ($K) | Medium ($10K+) | High ($75K+) | High ($50K+) | Very high | Medium ($5K+) | Very high | **Very low ($10-150)** |

### Where We Win

**1. Lowest robot hardware cost in the industry.**
No commercial fleet system operates with $10-150 robot hardware. Amazon's Kiva units are the cheapest commercial fleet robots and they cost thousands. Our robot is an MQTT client on an ESP32 or Pi Zero 2 running a behavior tree.

**2. Most open robot interface.**
10 MQTT message types. Any language, any platform that supports MQTT can implement a compatible robot. No vendor SDK, no proprietary protocol, no license fees. The robot-interface.md document is the complete specification.

**3. Full edge infrastructure, not a fleet server.**
Commercial systems run either a "fleet server" (web app with REST API) or cloud services. We run a **complete datacenter** — message bus with persistence and streaming, relational database, KV store, API gateway — at the edge in 8 watts. This is architecturally richer than any commercial fleet management system.

**4. Fastest capability extension.**
Adding a new action type across the entire industry requires firmware updates, ROS packages, SDK integrations, or vendor-specific programming. We add a KB entry and a worker function. Zero planner changes, zero hub changes, zero container rebuild.

**5. Domain-portable across verticals.**
Swap the SQLite DB and the same container image runs a warehouse, a lunar surface operation, or an agricultural field. No commercial system offers this — they are all vertical-specific.

**6. Tiered deployment with the same image.**
No commercial fleet system can run its fleet manager on the robot. Our planner container runs at any tier — site hub, commander robot, or cloud. This enables hierarchical autonomy that no commercial system supports.

### Where We Lose (Maturity Gaps, Not Architectural Gaps)

**1. No proven scale beyond testing.**
Amazon runs 800+ robots per warehouse. MiR and OTTO run 100+. Our system has been tested with simulated robots, not with a large physical fleet. The architecture should scale (coroutines are lightweight, NATS handles high throughput), but it is unproven at fleet scale.

**2. No commercial ecosystem.**
Commercial systems have vendor support, spare parts, integration partners, training programs, and regulatory certifications. Our system is a development-stage architecture.

**3. No formal flight software qualification yet.**
The silicon has ASIL-D certification. The container runtime and application code do not have flight heritage or certification artifacts. This is a process gap — the architecture does not prevent qualification.

**4. Integration testing requires live infrastructure.**
Core mission tests need running NATS, MQTT, and KV-bridge containers. Inherent to testing real message flow.

### Previously Identified Gaps — Now Resolved

| Original Objection | Resolution |
|---|---|
| No onboard navigation | Tiered robot architecture. SLAM runs on capable robots (TI TDA4, NXP S32G with hardware vision accelerators). Simple robots (ESP32, Pi Zero 2) follow pre-planned routes on known graphs. Both tiers use the same protocol. |
| No perception pipeline | Perception is VN workers on capable robots. Sensor data (visual odometry, obstacle detection, terrain mapping) feeds back through existing KB_DONE and heartbeat messages. Zig drivers on automotive-grade DSPs/accelerators. Not a separate subsystem — it is workers reporting richer data through the existing protocol. |
| No traffic management | Corridor access managed via virtual nodes with API access to external systems. The VN extension point supports gate/lock/reservation patterns. External corridor management systems coordinate access; the planner integrates via VN workers. |
| Single point of failure | Tiered deployment — same planner container runs at site hub, on commander robot, or both. Commander operates autonomously if site hub is unreachable. |
| No onboard autonomy | Commander robot runs full planner container locally with its own KB. Controls slave robots via same link protocol. |
| No delay tolerance | Earth-Moon link is out of scope — handled as external subsystem, same as Ingenuity and NASA orbiter VMs. |
| No radiation hardening | ASIL-D certified automotive silicon (TI, NXP, Renesas) with hardware ECC, lockstep cores, parity on flash. $20-150, not $200K. |
| Energy model too simple | Planning-grade model works for routing. Real battery management is a Zig subsystem feeding energy_remaining into the existing link protocol. Build when needed, no architecture change. |
| LuaJIT ecosystem small | LuaJIT FFI calls any C/Zig library directly. The ecosystem is C's ecosystem. Developer pool is game programmers and embedded systems engineers who already know this pattern. |

### Architectural Position

Our architecture occupies a unique position in the commercial landscape:

- **Amazon Robotics' centralized planning model** — but with standard protocols and without requiring a constrained grid environment
- **Industrial PLC coordination pattern** — but with richer infrastructure (persistent messaging, streaming telemetry, relational database)
- **DJI/Deere pre-planned mission pattern** — but with real-time fleet coordination and edge-local infrastructure
- **None of the onboard-heavy approach** of warehouse AMRs, Spot, or Waymo — by design, because we bet on local infrastructure over onboard intelligence

The closest commercial analogy is **Amazon Robotics reimagined with open standards, deployable edge infrastructure, and data-driven extensibility** — with tiered robot capabilities from simple microcontrollers to perception-capable automotive-grade processors, all using the same protocol.

---

## Advantages

**1. C-native scripting with zero binding overhead.**
LuaJIT FFI calls C/Zig at C speed. The entire I/O layer — NATS, MQTT, CBOR, SQLite — is native libraries with 3-line FFI declarations. No marshaling, no GIL, no binding generators.

**2. Palm-of-hand datacenter at the edge.**
Full infrastructure stack — NATS, Postgres, MQTT, OpenResty, planner containers — runs on a Pi 5 at 8 watts. Same software that would fill a rack room, deployed at the operations site in a shielded enclosure the size of a fist.

**3. Containerized planning deploys at any tier.**
Same planner image runs on site hub, on commander robot, or on Earth-side ops center. Tiered autonomy is a deployment decision, not an architecture change.

**4. Certified silicon is commercial off-the-shelf.**
TI TDA4, NXP S32G, Renesas R-Car — ASIL-D certified ARM processors with hardware ECC and lockstep cores. $20-150 each. Certification comes from the chip vendor.

**5. Zero-code extensibility from the KB.**
New virtual node = KB definition + robot worker function. No planner changes, no hub changes, no container rebuild. The data drives the system.

**6. Simple robots are reliable robots.**
Robots are MQTT clients with a behavior tree and 10 message types. Complexity lives in the recoverable, restartable hub — not on the robot.

**7. Fleet scales at zero marginal software cost.**
Add a robot: KB entry + link_announce. Same planner, same infrastructure, same container image.

**8. Game-engine architecture attracts the right developers.**
Tick loops, behavior trees, coroutines, state machines, C engine with scripting layer. Game programmers recognize this instantly and are productive immediately.

**9. Deterministic and explainable planning.**
Dijkstra is provable, auditable, fast. Every routing decision traces to edge weights in the KB. No ML black boxes, no training data dependencies.

**10. Composed from battle-tested infrastructure.**
NATS, Postgres, Mosquitto, OpenResty — millions of production deployments. We compose proven systems, we do not invent middleware.

**11. Recoverable by design.**
Containers restart. Robots detect planner restart via seq reset, re-announce, resume. NATS persists state. Postgres survives power cycles. Designed for restart, not for never-fail.

**12. Domain-portable.**
Swap the SQLite DB, get a different domain. Same container image. No per-mission ground segments.

**13. Zig for safety-critical code.**
Memory safe, no GC, no hidden control flow, arena allocation matching tick lifetimes, native C ABI for seamless LuaJIT FFI integration, trivial cross-compilation to ARM targets, auditable for certification.

---

## Remaining Disadvantages

Four disadvantages remain. All are **maturity gaps**, not architectural gaps.

**1. No proven scale beyond testing.**
The architecture should scale — coroutines are lightweight, NATS handles high throughput — but it is unproven with a large physical fleet.

**2. No commercial ecosystem.**
No vendor support, integration partners, training programs, or regulatory certifications. Development-stage architecture.

**3. No formal flight software qualification yet.**
The silicon has ASIL-D certification. The container runtime and application code do not have flight heritage or certification artifacts. This is a process gap. Zig's no-hidden-control-flow, no-undefined-behavior design is more auditable than C for certification purposes.

**4. Integration testing requires live infrastructure.**
Core mission tests need running NATS, MQTT, and KV-bridge containers. Unit tests (79 assertions for link protocol) run standalone. Inherent to testing real message flow.

---

## Resolution of All Technical Objections

Every technical disadvantage originally identified has been resolved within the existing architecture:

| Original Objection | Resolution | Architecture Change Required |
|---|---|---|
| Single point of failure | Tiered deployment — same container at any tier | None |
| No onboard autonomy | Commander robot runs local planner container | None — deployment decision |
| No delay tolerance | Earth link is out of scope (Ingenuity precedent) | None — external subsystem |
| No radiation hardening | ASIL-D automotive silicon ($20-150) | None — hardware selection |
| No perception pipeline | VN workers on capable robots, existing protocol | None — add workers |
| No onboard navigation | SLAM on TDA4/S32G tier robots | None — add workers |
| No traffic management | VN with API access, external corridor management | None — add VN type |
| Energy model too simple | Zig power manager feeds link protocol | None — add subsystem |
| LuaJIT ecosystem small | FFI calls any C/Zig library directly | None |
| Developer pool | Game programmers and embedded engineers | None |

Zero architectural changes. The container-based, tiered, KB-driven, VN-extensible design absorbs every capability addition through its existing extension points.

---

## Net Assessment

The architecture has **zero structural disadvantages**. What remains is the gap between a tested design and a deployed product: fleet-scale proof, commercial ecosystem, qualification paperwork, and integration test infrastructure.

Against NASA's Artemis stack, we offer the same structural patterns (layered, modular, KB-driven, capability-validated) at a fraction of the cost and complexity, optimized for permanent base operations rather than probe missions.

Against commercial fleet systems, we match Amazon Robotics' centralized planning model with open standards and edge infrastructure, support tiered robot capabilities from $10 microcontrollers to perception-capable automotive processors, and offer the fastest capability extension path in the industry — all running on a palm-of-hand datacenter at 8 watts.

As Artemis moves toward permanent lunar bases, NASA's architecture will likely converge toward site-local services, simpler robots, and centralized planning with local infrastructure. The ARMADAS work — voxel construction with simple grid robots and centralized coordination — is already heading in this direction. Our architecture is built for where permanent base operations are going, not for where single-probe missions have been.
