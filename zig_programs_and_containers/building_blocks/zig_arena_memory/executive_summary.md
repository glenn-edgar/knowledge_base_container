# ArenaEnv: A Self-Contained Environment Primitive for Distributed Control Systems

## Executive Summary

ArenaEnv is a novel software architectural pattern that combines five well-established concepts into a unified primitive: **arena memory allocation**, **symbol table environments**, **transactional access control**, **atomic reader tracking**, and **comptime tier specialization**. The result is a self-contained, transferable environment object suitable for deployment across hardware tiers ranging from 32KB ARM Cortex-M microcontrollers to multi-gigabyte servers — using the same source code.

---

## The Problem

Modern distributed control systems span radically different hardware tiers. Conventional software patterns address safety, efficiency, and portability as separate concerns, forcing engineers to maintain multiple codebases or accept compromises at each tier. Existing approaches suffer from:

- **Scattered memory** — individual heap allocations cause fragmentation, unpredictable timing, and high overhead on resource-constrained devices
- **Weak ownership semantics** — no clear mechanism for transferring entire execution environments between subsystems
- **No unified safety model** — safety guarantees that work on servers break down on bare-metal embedded targets
- **Thread model mismatch** — locking primitives designed for servers are unusable on single-core microcontrollers
- **Use-after-free in concurrent reads** — languages without borrow checkers (C, Zig) cannot statically prevent freeing memory while readers hold live pointers

---

## The Solution

ArenaEnv packages an entire execution environment — memory allocator, symbol table, data, and access policy — into a single transferable object. Key properties:

**Self-Containment** — The arena owns everything: hash table buckets, symbol keys, values, and string data. All internal pointers are intra-arena. There are no external heap references.

**O(1) Environment Transfer** — Moving an environment between subsystems is a single pointer swap regardless of how many symbols it contains. The entire namespace, with all its data, moves atomically.

**Transactional Access** — All reads and writes occur inside explicit transactions. Write transactions hold a mutex for exclusive access. Read transactions either acquire the mutex or increment an atomic reader count, depending on configuration. There is no path to the data that bypasses the transaction boundary.

**Atomic Reader Tracking** — When concurrent reads are enabled, an atomic reader count replaces the mutex for read transactions. The `deinit` operation spins until the reader count reaches zero before freeing memory. The increment-before-live-check ordering guarantees that no reader can hold a pointer into freed memory. This closes the use-after-free gap that exists in lock-free read designs without reader tracking.

**Comptime Tier Specialization** — A single configuration struct selects thread safety, locking strategy, allocator type, capacity hints, and string interning at compile time. On embedded builds, the mutex, reader count, and interning logic do not exist in the binary — they are `void` types with zero size and zero cost.

**Allocator Type Parameterization** — The backing allocator type is a comptime parameter, not a runtime interface. This allows concrete allocator pointer types on embedded targets (avoiding the cost of the `std.mem.Allocator` fat pointer) while preserving the standard interface as the default.

**Atomic Deallocation** — Freeing an environment is one operation (after reader drain). No per-symbol deallocation, no reference counting traversal, no garbage collection pause.

---

## Comparison to Alternatives

| Property | Rust + HashMap | C++ RAII | ArenaEnv (Zig) |
|---|---|---|---|
| Memory layout | Scattered heap | Scattered heap | Contiguous arena block |
| Transfer cost | O(1) struct move | O(1) struct move | O(1) pointer swap |
| Free cost | O(n) individual frees | O(n) destructors | O(1) single dealloc |
| Safety model | Compile-time proof | Convention | Design constraint + runtime |
| Concurrent read safety | Compile-time (`RwLock`) | Convention (`shared_mutex`) | Atomic reader count |
| Use-after-free prevention | Compile-time (borrow checker) | Convention | Atomic reader count + mutex serialization |
| Embedded portable | Breaks at freestanding | Possible, complex | Same source, all tiers |
| Tick determinism | Not guaranteed | Not guaranteed | Guaranteed post-init |
| Data race prevention | Compile-time | Convention | Architectural constraint |
| Allocator flexibility | trait-based | template | Comptime type parameter |

---

## The Safety Protocol

The atomic reader count protocol deserves specific attention, as it solves a problem that Zig (and C) cannot solve statically:

```
Thread A (reader):              Thread B (owner, deinit):
  reader_count.fetchAdd(1)        mutex.lock()
  if (!live) {                    live = false
    reader_count.fetchSub(1)      mutex.unlock()
    return error                  while (reader_count.load() != 0)
  }                                 spinLoopHint()       // waits for A
  // ... safe reads ...           arena.deinit()          // safe
  reader_count.fetchSub(1)
```

The critical invariant is ordering: the reader count is incremented BEFORE checking the `live` flag. This guarantees mutual exclusion between "reader holds live pointer" and "arena memory is freed" without requiring the reader to acquire the mutex. The cost is one atomic increment and one atomic decrement per read transaction — negligible compared to mutex acquisition.

---

## Prior Art and Novelty

The individual components are well-established: arena allocators (Tofte and Talpin, 1994), Lisp environment models (SICP, 1986), database transaction semantics (Jim Gray, 1970s), policy-based compile-time configuration (Alexandrescu, 2001), and atomic reference counting for safe memory reclamation (RCU, epoch-based reclamation). The specific synthesis — arena-owned symbol table with transactional access, atomic reader tracking for use-after-free prevention, ownership transfer as environment move, allocator type parameterization, and comptime hardware tier specialization — does not have a direct precedent in the published literature.

---

## Strategic Value

ArenaEnv is the foundational memory and namespace primitive for the ChainTree distributed control architecture. It enables:

- A single codebase spanning microcontrollers to servers
- Deterministic behavior tree tick timing with zero allocation post-initialization
- Safe environment handoff between ChainTree subsystems
- Safe concurrent reads without mutex contention on server-tier deployments
- A clean separation between the construction phase (owner writes) and the runtime phase (concurrent reads)

The pattern is suitable for publication as a systems architecture contribution and as an open-source library under the existing ChainTree MIT license framework.

---

*Glenn Edgar — Onyx Engineering*
*ChainTree Distributed Control Architecture*