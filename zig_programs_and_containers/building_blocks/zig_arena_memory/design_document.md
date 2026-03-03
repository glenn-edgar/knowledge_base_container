# ArenaEnv: Detailed Design Document

## 1. Overview

ArenaEnv is a configurable, self-contained environment primitive implemented in Zig 0.13.0. It combines an arena bump allocator, a string-keyed hash table symbol table, transactional mutex-protected access, an atomic reader count for safe concurrent reads, and comptime hardware tier specialization into a single coherent abstraction. The environment is the unit of allocation, the unit of ownership, and the unit of transfer.

This document describes the architecture, design decisions, data layout, safety model, and deployment strategy across ChainTree hardware tiers.

---

## 2. Motivation

### 2.1 The Tier Problem

ChainTree deployments span four hardware tiers:

| Tier | Example Hardware | RAM | Threading |
|---|---|---|---|
| Micro | STM32F0, Cortex-M0 | 32KB | None (bare metal) |
| Small | STM32F4, Cortex-M4 | 512KB | RTOS tasks |
| Mid | Raspberry Pi, i.MX8 | 256MB | POSIX threads |
| Server | x86-64 Linux | 8GB+ | std pthreads |

A control architecture that works cleanly across all four tiers, from a single codebase, has significant engineering value. Conventional approaches require separate memory models per tier.

### 2.2 The Ownership Problem

Behavior trees construct environments during initialization and hand them to runtime consumers. The construction phase (Owner) needs write access. The runtime phase (tree nodes) needs read-only access. There must be a clean mechanism to transfer the environment from the initializer to the runtime without copying data.

### 2.3 The Safety Problem

Distributed control systems access shared state from multiple contexts — RTOS tasks, interrupt handlers, NATS message callbacks, behavior tree ticks. Preventing data races through programmer discipline alone is insufficient. The design must make unsafe access architecturally impossible.

Zig provides no borrow checker and no reference counting. A pointer can be freed while another pointer to the same memory is still live. ArenaEnv must solve this at the design level.

---

## 3. Core Concepts

### 3.1 The Arena as Environment

An arena allocator allocates memory by bumping a pointer forward in a fixed backing block. ArenaEnv uses the arena not just as a memory source but as the **boundary of the environment**. Everything that belongs to the environment lives inside the arena:

- The hash table's internal bucket array
- All symbol key strings (duped into the arena at insertion time)
- All value structs
- All string data inside values (optionally interned)

This means the environment has no external heap references. It is self-contained. All internal pointers are valid as long as the arena backing block exists.

### 3.2 The Symbol Table

The hash table maps `[]const u8` keys to `*Value` pointers. Because both the table's internal storage and the pointed-to values live in the arena, the symbol table and its data form one coherent memory region. Symbol resolution involves no cross-allocation pointer chasing.

### 3.3 Transactions as the Only Access Path

Neither Owner nor ReadTx exposes raw data access methods. All data access occurs through transactions:

- `beginWrite()` — acquires exclusive mutex lock, returns `WriteTx`
- `beginRead()` — acquires shared access (mutex or reader count), returns `ReadTx`
- `commit()` — releases the lock or decrements the reader count

There is no `owner.get()` or `owner.put()` that bypasses the transaction. Data races are prevented by the architecture of the API, not by convention.

### 3.4 The Atomic Reader Count

When `concurrent_reads` is true, read transactions do not acquire the mutex. Instead, they atomically increment a reader count. This allows multiple concurrent readers without mutex contention. The protocol is:

1. `beginRead()` increments `reader_count` with acquire ordering
2. `beginRead()` checks `live` — if false, decrements and returns error
3. The reader performs lookups through `ReadTx.get()` / `ReadTx.contains()`
4. `ReadTx.commit()` decrements `reader_count` with release ordering

The increment-before-check ordering is critical: it guarantees that `deinit`'s `waitForReaders()` spin loop sees any reader that has passed the live check. Without this ordering, a reader could check `live`, see true, then be preempted while `deinit` sets `live = false`, sees `reader_count == 0`, and frees the arena.

When `concurrent_reads` is false (or `threadsafe` is false), the reader count type is `void` — zero size, zero cost, compiles away completely.

### 3.5 Ownership Transfer

Transfer moves the environment from one Owner to another:

```
owner.store → [ arena block: buckets | keys | values | strings ]
                                  ↑
                            new_owner.store

owner.store = null   (original owner is now blind)
```

The arena block does not move. No data is copied. No internal pointers are invalidated. The cost is one pointer assignment.

### 3.6 Allocator Type Parameterization

The backing allocator type is a comptime parameter. This allows:

- `std.mem.Allocator` (default) — the standard fat pointer interface, works with any allocator
- `*std.heap.GeneralPurposeAllocator(.{})` — a concrete allocator pointer, avoids one level of indirection
- Custom allocator pointer types for embedded targets

A `resolveAllocator` helper dispatches at comptime: if the type is `std.mem.Allocator`, return it directly; otherwise call `.allocator()` on it to obtain the standard interface. No runtime dispatch.

---

## 4. Data Layout

### 4.1 Memory Map

```
Backing allocator (GPA, c_allocator, or static buffer)
└── Store struct (allocated from backing)
    ├── mutex: Thread.Mutex  (or void on embedded)
    ├── reader_count: atomic.Value(u32) (or void if not concurrent)
    ├── live: bool
    └── arena: ArenaAllocator
        └── Arena backing block (one contiguous allocation)
            ├── HashMap bucket array
            ├── HashMap metadata
            ├── [key "node_timeout"      ] (duped string)
            ├── [Value{ .int = 1000 }    ] (allocated value)
            ├── [key "handler"           ] (duped string)
            ├── [Value{ .text = ptr }    ] (allocated value)
            └── ["pump_ctrl"             ] (interned string data)
```

The Store struct itself is the only allocation from the backing allocator. Everything else is inside the arena.

### 4.2 The Store Struct

```zig
const ReaderCountType = if (cfg.threadsafe and cfg.concurrent_reads)
    std.atomic.Value(u32)
else
    void;

const Store = struct {
    arena:        std.heap.ArenaAllocator,
    table:        std.StringHashMap(*Value),
    mutex:        if (cfg.threadsafe) std.Thread.Mutex else void,
    reader_count: ReaderCountType,
    live:         bool,
};
```

When `threadsafe = false`, both the mutex and reader count are `void` — zero size, zero cost. When `threadsafe = true` but `concurrent_reads = false`, only the reader count is void; reads acquire the mutex instead.

---

## 5. Configuration

ArenaEnv is parameterized by a comptime configuration struct:

```zig
const EnvConfig = struct {
    Value:            type,                        // The value type stored in the table
    Allocator:        type = std.mem.Allocator,    // Backing allocator type
    threadsafe:       bool = true,                 // Include mutex and/or reader count
    concurrent_reads: bool = true,                 // Use atomic reader count for reads
    initial_capacity: u32  = 64,                   // Hash table pre-allocation hint
    intern_strings:   bool = true,                 // Dupe text fields into arena
};
```

Each configuration produces a **distinct Zig type**. `MicroEnv(V).Owner` and `ServerEnv(V).Owner` are different types — they cannot be mixed accidentally. The compiler enforces tier separation.

### 5.1 Tier Configurations

```zig
// Cortex-M0, 32KB RAM, bare metal
// No mutex, no reader count, no string interning
const MicroEnv = ConfiguredEnvironment(.{
    .Value            = NodeConfig,
    .threadsafe       = false,
    .concurrent_reads = false,
    .initial_capacity = 16,
    .intern_strings   = false,
});

// Cortex-M4, RTOS, 512KB RAM
// Mutex for reads and writes, no concurrent reads
const SmallEnv = ConfiguredEnvironment(.{
    .Value            = NodeConfig,
    .threadsafe       = true,
    .concurrent_reads = false,
    .initial_capacity = 32,
    .intern_strings   = true,
});

// Linux server, full threading
// Mutex for writes, atomic reader count for reads
const ServerEnv = ConfiguredEnvironment(.{
    .Value            = ChainTreeValue,
    .threadsafe       = true,
    .concurrent_reads = true,
    .initial_capacity = 256,
    .intern_strings   = true,
});
```

---

## 6. Safety Model

### 6.1 What the Architecture Prevents

| Hazard | Mechanism |
|---|---|
| Data race on write | Mutex held during all write access |
| Data race on read | Mutex or atomic reader count, depending on config |
| Lock bypass | No access path exists outside transactions |
| Use after transfer | `owner.store` set to null, all methods return error |
| Use after free (writer) | Mutex serializes deinit — cannot destroy while write tx active |
| Use after free (reader) | Atomic reader count — deinit spins until all readers finish |
| Stale read on dead store | `live` flag checked after acquiring lock/reader count |
| Forgotten commit | Idempotent commit + `defer tx.commit()` idiom |

### 6.2 The Reader Count Protocol

The reader count closes what was previously a safety gap in concurrent read mode. The protocol:

```
beginRead:                          deinit:
  acquireReader()  (count → 1)        lock()
  check live                          live = false
  if false:                           unlock()
    releaseReader()                   waitForReaders()  ← spins until count == 0
    return error                      arena.deinit()    ← safe, no readers active
  return ReadTx
    ...
  commit:
    releaseReader()  (count → 0)
```

The key ordering invariant: `acquireReader()` happens BEFORE checking `live`. This means:
- If `beginRead` sees `live = true`, the reader count is already > 0, so `waitForReaders` will block
- If `beginRead` sees `live = false`, it decrements and returns error — no dangling pointer

### 6.3 What Requires Discipline

| Hazard | Required Practice |
|---|---|
| Forgotten `defer tx.commit()` | Code review; idempotent commit makes double-call safe |
| Same-thread double lock | Structural — never begin two write tx on same owner |
| Transfer during active tx | Structural — transfer is single-threaded by design |
| Cross-tier type confusion | Zig type system — distinct types per config |

### 6.4 Comparison to Rust

Rust's borrow checker provides compile-time proof of the properties ArenaEnv achieves at runtime. The practical gap for this use case:

- Rust catches forgotten unlock at compile time (RAII drop); ArenaEnv uses idempotent commit with `defer`
- Rust prevents double-locking at compile time; ArenaEnv deadlocks at runtime
- Rust's `Send`/`Sync` checks thread safety of value types at compile time; ArenaEnv trusts the Value type is safe
- Rust proves no use-after-free at compile time; ArenaEnv prevents it at runtime via atomic reader count and mutex serialization

For a codebase where the value types are simple primitives and the call sites are controlled, the runtime gap is acceptable and the efficiency and portability gains are significant.

---

## 7. The Type Factory

The `ConfiguredEnvironment` function is a **type factory** — a comptime function that returns a struct type:

```zig
fn ConfiguredEnvironment(comptime cfg: EnvConfig) type {
    return struct {
        pub const Owner   = struct { ... };
        pub const WriteTx = struct { ... };
        pub const ReadTx  = struct { ... };
    };
}
```

This is Zig's idiomatic equivalent of a generic class. The returned type contains Owner, WriteTx, and ReadTx as nested types, all sharing access to the same Store definition and configuration constants. There is no runtime dispatch, no vtable, no interface overhead.

---

## 8. Lifecycle

```
init(backing)
    │
    ▼
Owner (write phase)
    │  beginWrite() → WriteTx → commit()   [mutex held]
    │  beginWrite() → WriteTx → abort()    [mutex held]
    │  beginRead()  → ReadTx  → commit()   [reader count or mutex]
    │
    ├─ transfer() ──→ new Owner (original nulled)
    │
    ▼
deinit()
    mutex.lock()
    live = false
    mutex.unlock()
    waitForReaders()   ← spin until atomic count == 0
    arena.deinit()     ← one dealloc, entire environment freed
    backing.destroy()
```

---

## 9. Integration with ChainTree

### 9.1 Initialization Phase

During ChainTree startup, the configurator holds an Owner and populates the environment via write transactions:

```
ChainTree Init
├── Owner.init(backing_allocator)
├── beginWrite() → populate node configs → commit()
├── beginWrite() → populate NATS subjects → commit()
└── transfer() → Runtime Owner
```

### 9.2 Runtime Phase

The Runtime Owner issues read transactions to behavior tree nodes. Each node opens a ReadTx for the lifetime of the tick cycle. The atomic reader count guarantees safe concurrent access from multiple tree nodes ticking in parallel. No allocation occurs during ticking.

### 9.3 Hierarchical Environments

The environment model supports ChainTree's hierarchical namespace by chaining environments:

```
Global Env (server config)
    └── Zone Env (irrigation zone)
            └── Node Env (pump node)
```

Symbol lookup walks the chain. Each level is a distinct arena, a distinct Owner, a distinct transfer boundary.

---

## 10. Future Work

- **Snapshot / rollback** — capture arena state before a write transaction for true abort semantics
- **Persistent backing** — map arena to memory-mapped file for crash recovery
- **Read-write lock** — replace mutex + reader count with a full RwLock for writer-during-readers support
- **Iteration protocol** — safe iterator over symbols inside a read transaction
- **NATS integration** — serialize/deserialize environment over NATS JetStream for distributed state sync
- **Timeout on waitForReaders** — configurable timeout instead of unbounded spin, with error return

---

*Glenn Edgar — Onyx Engineering*
*ChainTree Distributed Control Architecture*
*Document version 2.0*