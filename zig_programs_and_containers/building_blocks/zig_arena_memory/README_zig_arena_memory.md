# ArenaEnv

A self-contained environment primitive for the [ChainTree](https://github.com/glenn-edgar/knowledge_base_container) distributed control architecture. Written in Zig 0.13.0.

ArenaEnv combines an arena allocator, a string-keyed symbol table, transactional access control, an atomic reader count, and comptime hardware tier specialization into a single transferable object. One codebase compiles to bare-metal Cortex-M0 through multi-threaded Linux servers.

## Quick Start

```zig
const arena_env = @import("arena_env.zig");

const Value = union(enum) {
    integer: i64,
    text: []const u8,
};

const Env = arena_env.ServerEnv(Value);

// Initialize
var owner = try Env.Owner.init(allocator);
defer owner.deinit();

// Write
{
    var tx = try owner.beginWrite();
    defer tx.commit();
    try tx.put("timeout_ms", .{ .integer = 5000 });
    try tx.put("node_name", .{ .text = "pump_controller" });
}

// Read
{
    var tx = try owner.beginRead();
    defer tx.commit();
    if (tx.get("timeout_ms")) |val| {
        // val is *const Value
    }
}

// Transfer (O(1), no copy)
var runtime_owner = try owner.transfer();
defer runtime_owner.deinit();
// owner is now inert — all methods return error
```

## Design

All data lives inside one arena allocation: hash table buckets, symbol keys, values, and interned strings. There are no external heap references. Freeing the environment is a single deallocation.

All access goes through transactions. There is no `owner.get()`. The mutex is always held during writes. For reads, the design offers two modes:

- **Serialized reads** (`concurrent_reads = false`) — read transactions acquire the mutex, same as writes
- **Concurrent reads** (`concurrent_reads = true`) — read transactions atomically increment a reader count instead of locking. `deinit` spins until the count reaches zero before freeing memory. This prevents use-after-free without requiring readers to take a lock.

On embedded targets where `threadsafe = false`, the mutex and reader count are `void` types — zero bytes, zero cost, compiled away entirely.

## Tier Presets

| Preset | Threading | Reads | Capacity | String Interning |
|---|---|---|---|---|
| `MicroEnv(V)` | None | Serialized | 16 | Off |
| `SmallEnv(V)` | Mutex | Serialized | 32 | On |
| `ServerEnv(V)` | Mutex + atomic readers | Concurrent | 256 | On |

Or configure directly:

```zig
const Env = arena_env.ConfiguredEnvironment(.{
    .Value            = MyValue,
    .Allocator        = *std.heap.GeneralPurposeAllocator(.{}),
    .threadsafe       = true,
    .concurrent_reads = true,
    .initial_capacity = 128,
    .intern_strings   = true,
});
```

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `Value` | `type` | (required) | The value type stored in the symbol table |
| `Allocator` | `type` | `std.mem.Allocator` | Backing allocator type; may be a concrete pointer type |
| `threadsafe` | `bool` | `true` | Include mutex and reader count |
| `concurrent_reads` | `bool` | `true` | Use atomic reader count instead of mutex for reads |
| `initial_capacity` | `u32` | `64` | Hash table pre-allocation hint |
| `intern_strings` | `bool` | `true` | Dupe `[]const u8` fields into the arena at `put()` time |

## API

### Owner

| Method | Description |
|---|---|
| `init(backing)` | Allocate and initialize a new environment |
| `deinit()` | Free the environment (waits for readers, then one dealloc) |
| `transfer()` | Move ownership to a new Owner (O(1), original becomes inert) |
| `beginWrite()` | Start an exclusive write transaction (acquires mutex) |
| `beginRead()` | Start a read transaction (acquires reader count or mutex) |

### WriteTx

| Method | Description |
|---|---|
| `put(key, value)` | Insert or update a symbol (key and value duped into arena) |
| `get(key)` | Lookup a symbol, returns `?*Value` (mutable) |
| `remove(key)` | Remove a symbol (arena memory not reclaimed) |
| `commit()` | Release the write lock (idempotent) |
| `abort()` | Same as commit (future: snapshot rollback) |
| `count()` | Number of symbols in the table |

### ReadTx

| Method | Description |
|---|---|
| `get(key)` | Lookup a symbol, returns `?*const Value` (immutable) |
| `contains(key)` | Check whether a symbol exists |
| `commit()` | Release the read lock / decrement reader count (idempotent) |
| `count()` | Number of symbols in the table |

## Safety Guarantees

- **No data races** — mutex held during writes; atomic reader count or mutex during reads
- **No use-after-free** — `deinit` waits for all readers to finish before freeing
- **No lock bypass** — all data access goes through transactions
- **No use after transfer** — transferred owner returns `error.OwnershipReleased`
- **No double free** — `owner.store` is null after deinit or transfer
- **No forgotten commits** — `defer tx.commit()` + idempotent commit

## Files

```
arena_env/
├── arena_env.zig       # Type factory, Owner, WriteTx, ReadTx, tier presets
├── types.zig           # EnvConfig struct definition
└── tests.zig           # 15 test cases including concurrent reader safety
```

## Testing

```bash
zig test tests.zig
```

Requires Zig 0.13.0. Tests cover basic operations, ownership transfer, string interning, tagged unions, custom allocator types, and a multi-threaded concurrent reader + deinit race condition test.

## Documentation

- [Implementation Prompt](implementation_prompt.md) — step-by-step build specification
- [Detailed Design](detailed_design.md) — architecture, data layout, safety model
- [Paper](paper.md) — overview for publication / external audience

## License

MIT — part of the ChainTree distributed control architecture.

---

*Glenn Edgar — Onyx Engineering*