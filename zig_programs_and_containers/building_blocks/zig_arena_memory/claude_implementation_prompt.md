# ArenaEnv Implementation Prompt for Claude

## Context

You are implementing **ArenaEnv** — a configurable, self-contained environment primitive in Zig 0.13.0. This is part of the ChainTree distributed control architecture by Glenn Edgar at Onyx Engineering. Read this entire document before writing any code.

---

## What You Are Building

A Zig type factory function `ConfiguredEnvironment(cfg)` that returns a struct type containing:

- `Owner` — holds mutable access, manages lifecycle, issues transactions
- `WriteTx` — exclusive write transaction, holds mutex for its lifetime
- `ReadTx` — read transaction, holds lock or increments atomic reader count depending on config
- An internal `Store` struct never exposed publicly

The environment is self-contained: the arena allocator, hash table buckets, all symbol keys, and all values live inside one contiguous arena backing block. The hash table is a symbol table — string keys map to typed values. Transfer of ownership is O(1) and moves the entire environment.

---

## The Configuration Struct

```zig
const EnvConfig = struct {
    Value:            type,
    Allocator:        type     = std.mem.Allocator,
    threadsafe:       bool     = true,
    concurrent_reads: bool     = true,
    initial_capacity: u32      = 64,
    intern_strings:   bool     = true,
};
```

**Value** — the type stored in the table. May be a struct or tagged union. Caller provides this.

**Allocator** — the type of the backing allocator. Defaults to `std.mem.Allocator` (the fat pointer interface). May be a concrete allocator pointer type such as `*std.heap.GeneralPurposeAllocator(.{})`, in which case the implementation calls `.allocator()` on it to obtain the `std.mem.Allocator` interface. This is resolved at comptime via a `resolveAllocator` helper — no runtime dispatch.

**threadsafe** — if false, the mutex field is `void` (compiles away entirely). All lock/unlock calls are no-ops. Use for bare-metal embedded targets.

**concurrent_reads** — if true and threadsafe is true, read transactions use an atomic reader count instead of acquiring the mutex. `deinit` spins until the reader count reaches zero before freeing the arena. If false, read transactions acquire the mutex like write transactions.

**initial_capacity** — passed to `table.ensureTotalCapacity()` at init time to pre-allocate hash table storage inside the arena.

**intern_strings** — if true and Value contains `[]const u8` fields, dupe those strings into the arena at `put()` time. If false, the caller is responsible for string lifetime.

---

## The Store (Internal, Never Public)

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

**Critical**: The table is initialized with `arena.allocator()` — hash table internal storage lives inside the arena, not on the backing allocator. This is mandatory.

**live** — set to `false` inside the mutex before `arena.deinit()` is called. Any transaction that acquires the mutex (or increments the reader count) after this point sees `live = false` and returns `error.StoreDestroyed`.

**reader_count** — atomic `u32` when `threadsafe and concurrent_reads`, `void` otherwise (zero size, compiles away). Tracks the number of active concurrent read transactions. `deinit` spins on this reaching zero before freeing memory.

Helper methods on Store:
```zig
fn lock(self: *Store) void {
    if (cfg.threadsafe) self.mutex.lock();
}
fn unlock(self: *Store) void {
    if (cfg.threadsafe) self.mutex.unlock();
}
fn acquireReader(self: *Store) void {
    if (cfg.threadsafe and cfg.concurrent_reads) {
        _ = self.reader_count.fetchAdd(1, .acquire);
    }
}
fn releaseReader(self: *Store) void {
    if (cfg.threadsafe and cfg.concurrent_reads) {
        _ = self.reader_count.fetchSub(1, .release);
    }
}
fn waitForReaders(self: *Store) void {
    if (cfg.threadsafe and cfg.concurrent_reads) {
        while (self.reader_count.load(.acquire) != 0) {
            std.atomic.spinLoopHint();
        }
    }
}
```

---

## Allocator Resolution

The `resolveAllocator` helper converts the configured Allocator type to the standard `std.mem.Allocator` interface at comptime:

```zig
fn resolveAllocator(backing: cfg.Allocator) std.mem.Allocator {
    if (cfg.Allocator == std.mem.Allocator) {
        return backing;
    } else {
        return backing.allocator();
    }
}
```

This allows users to pass concrete allocator pointers (e.g. `*GeneralPurposeAllocator(.{})`) or the standard interface interchangeably.

---

## Owner

Fields:
```zig
store:   ?*Store,
backing: cfg.Allocator,
```

### `init(backing: cfg.Allocator) !Owner`

1. Resolve backing to `std.mem.Allocator` via `resolveAllocator(backing)`
2. Allocate `Store` from resolved allocator (not arena — the Store struct itself lives on backing)
3. Init arena with resolved allocator
4. Set `live = true`
5. Init mutex (default init if threadsafe, skip if void)
6. Init `reader_count` to 0 (if threadsafe and concurrent_reads, skip if void)
7. Init table with `arena.allocator()`
8. Call `table.ensureTotalCapacity(cfg.initial_capacity)` to pre-fill arena

### `deinit(self: *Owner) void`

1. If `self.store` is null, return immediately
2. `s.lock()` — block until any in-flight write transaction completes
3. Set `s.live = false`
4. `s.unlock()`
5. `s.waitForReaders()` — spin until all concurrent readers finish
6. `s.arena.deinit()` — frees the entire arena block including table internals, keys, values
7. `resolveAllocator(self.backing).destroy(s)` — frees the Store struct itself
8. Set `self.store = null`

### `transfer(self: *Owner) error{AlreadyTransferred}!Owner`

1. If `self.store` is null, return `error.AlreadyTransferred`
2. Capture `s = self.store`
3. Set `self.store = null`
4. Return `Owner{ .store = s, .backing = self.backing }`

No lock needed — transfer is single-threaded by design.

### `beginWrite(self: Owner) !WriteTx`

1. If `self.store` is null, return `error.OwnershipReleased`
2. `s.lock()`
3. If `!s.live`, unlock and return `error.StoreDestroyed`
4. Return `WriteTx{ .store = s, .committed = false }`

### `beginRead(self: Owner) !ReadTx`

1. If `self.store` is null, return `error.OwnershipReleased`
2. If `cfg.concurrent_reads` is false: `s.lock()` then check `s.live`
3. If `cfg.concurrent_reads` is true:
   a. `s.acquireReader()` — increment atomic count BEFORE checking live
   b. If `!s.live`: `s.releaseReader()` and return `error.StoreDestroyed`
   c. This ordering guarantees `deinit`'s `waitForReaders` sees any reader that passed the live check
4. Return `ReadTx{ .store = s, .released = false }`

---

## WriteTx

Fields:
```zig
store:     *Store,
committed: bool = false,
```

### `put(self: *WriteTx, key: []const u8, value: Value) !void`

1. Get `alloc = self.store.arena.allocator()`
2. `owned_key = try alloc.dupe(u8, key)` — key lives in arena
3. `owned_val = try alloc.create(Value)` — value struct lives in arena
4. If `cfg.intern_strings`: run `internValue` to dupe string fields into arena
5. `owned_val.* = interned_value`
6. `try self.store.table.put(owned_key, owned_val)`

### `get(self: WriteTx, key: []const u8) ?*Value`

Direct table lookup. Returns mutable pointer (write tx holds exclusive lock).

### `remove(self: *WriteTx, key: []const u8) bool`

Uses `self.store.table.fetchRemove(key)`, returns whether an entry was found. Arena memory for the removed entry is not reclaimed — expected for arena allocators.

### `commit(self: *WriteTx) void`

1. If already committed, return (idempotent)
2. Set `self.committed = true`
3. `self.store.unlock()`

### `abort(self: *WriteTx) void`

Identical to commit for now. Future work may add snapshot/restore for true rollback.

### `count(self: WriteTx) u32`

Returns `self.store.table.count()`.

---

## ReadTx

Fields:
```zig
store:    *Store,
released: bool = false,
```

Note: `*Store` (not `*const Store`) because `releaseReader` and `unlock` require mutable access to the store's atomic/mutex fields. Const safety is enforced at the API level — `get()` returns `?*const Value`.

### `get(self: ReadTx, key: []const u8) ?*const Value`

`self.store.table.get(key)` — `*Value` coerces to `*const Value`.

### `contains(self: ReadTx, key: []const u8) bool`

`self.store.table.contains(key)`

### `commit(self: *ReadTx) void`

1. If already released, return (idempotent)
2. Set `self.released = true`
3. If `cfg.concurrent_reads`: `self.store.releaseReader()`
4. If `!cfg.concurrent_reads`: `self.store.unlock()`

### `count(self: ReadTx) u32`

Returns `self.store.table.count()`.

---

## String Interning Helper

When `cfg.intern_strings` is true, `WriteTx.put` interns string fields using comptime reflection. The helper handles three cases:

1. **Struct values** — iterate fields with `inline for`, dupe any `[]const u8` fields
2. **Tagged union values** — switch on the active tag with `inline else`, then:
   - If the payload is `[]const u8`, dupe it directly
   - If the payload is a struct, recurse into its fields
   - Otherwise, return unchanged
3. **Other types** — return unchanged

```zig
fn internValue(alloc: std.mem.Allocator, value: Value) !Value {
    if (!cfg.intern_strings) return value;
    const info = @typeInfo(Value);
    switch (info) {
        .Struct => { ... },   // Note: Zig 0.13 uses .Struct, not .@"struct"
        .Union => |u| { ... },
        else => return value,
    }
}
```

**Zig version note**: Zig 0.13.0 uses PascalCase enum tags (`.Struct`, `.Union`). Zig 0.14-dev uses quoted lowercase (`.@"struct"`, `.@"union"`).

---

## The Type Factory Function

```zig
pub fn ConfiguredEnvironment(comptime cfg: EnvConfig) type {
    comptime {
        if (cfg.initial_capacity == 0) @compileError("initial_capacity must be > 0");
    }

    return struct {
        const Self  = @This();
        pub const Value = cfg.Value;
        const Table = std.StringHashMap(*Value);
        const MutexType = if (cfg.threadsafe) std.Thread.Mutex else void;
        const ReaderCountType = if (cfg.threadsafe and cfg.concurrent_reads)
            std.atomic.Value(u32) else void;

        pub const is_threadsafe = cfg.threadsafe;
        pub const has_concurrent_reads = cfg.concurrent_reads;
        pub const interns_strings = cfg.intern_strings;

        const Store = struct { ... };       // as above
        pub const WriteTx = struct { ... }; // as above
        pub const ReadTx  = struct { ... }; // as above
        pub const Owner   = struct { ... }; // as above
    };
}
```

---

## Mandatory Usage Pattern

The caller MUST use `defer` for transaction commit. Document this as the only correct pattern:

```zig
// Write
{
    var tx = try owner.beginWrite();
    defer tx.commit();
    try tx.put("key", value);
}

// Read
{
    var tx = try owner.beginRead();
    defer tx.commit();
    const val = tx.get("key");
}
```

Commit is idempotent — calling it explicitly and via defer is safe.

---

## Standard Tier Instantiations

```zig
pub fn MicroEnv(comptime V: type) type {
    return ConfiguredEnvironment(.{
        .Value = V, .threadsafe = false,
        .concurrent_reads = false, .initial_capacity = 16, .intern_strings = false,
    });
}

pub fn SmallEnv(comptime V: type) type {
    return ConfiguredEnvironment(.{
        .Value = V, .threadsafe = true,
        .concurrent_reads = false, .initial_capacity = 32, .intern_strings = true,
    });
}

pub fn ServerEnv(comptime V: type) type {
    return ConfiguredEnvironment(.{
        .Value = V, .threadsafe = true,
        .concurrent_reads = true, .initial_capacity = 256, .intern_strings = true,
    });
}
```

---

## Test Cases

1. **Basic put/get** — write values, read back in a read transaction
2. **Transfer** — write, transfer, read from new owner, verify original returns error
3. **Double transfer** — second transfer returns `error.AlreadyTransferred`
4. **Deinit safety** — deinit, verify beginWrite/beginRead return error; double deinit is no-op
5. **MicroEnv config** — verify threadsafe/concurrent_reads/intern_strings are false
6. **WriteTx remove** — insert, verify present, remove, verify absent
7. **Multi-put 100 symbols** — insert 100, verify all retrievable
8. **String interning** — put text field, corrupt original buffer, verify arena copy intact
9. **Tagged union value** — put integer/text/struct variants, verify discriminants and payloads
10. **Tagged union string interning** — put text variant, corrupt original, verify intact
11. **SmallEnv config** — verify threadsafe true, concurrent_reads false, intern_strings true
12. **Custom allocator type** — use `*GeneralPurposeAllocator(.{})` as Allocator parameter
13. **Write tx mutation** — mutate value through pointer returned by WriteTx.get
14. **Transfer then extend** — write on transferred owner, verify both old and new keys
15. **Concurrent readers with deinit** — 4 threads reading while main thread calls deinit; verify no use-after-free via atomic reader count

---

## File Structure

```
arena_env/
├── arena_env.zig       // ConfiguredEnvironment, MicroEnv, SmallEnv, ServerEnv
├── types.zig           // EnvConfig struct definition
└── tests.zig           // All test cases
```

Run tests: `zig test tests.zig` (Zig 0.13.0, Linux)

---

## What NOT to Do

- Do not expose `Store` publicly — it is internal
- Do not add `get()` or `put()` directly on Owner — all access via transactions
- Do not use the backing allocator inside the arena — `Store` struct on backing, everything else in arena
- Do not init the hash table with the backing allocator — it must use `arena.allocator()`
- Do not implement rollback yet — abort is identical to commit for now
- Do not add logging or debug prints — the API should be silent
- Do not use `anyopaque` or runtime interfaces — everything is comptime parameterized
- Do not use `.@"struct"` / `.@"union"` — Zig 0.13 uses `.Struct` / `.Union`

---

## Key Invariants

1. `store.mutex` is always held when data is written
2. `store.live` is always checked before returning a transaction
3. `store.reader_count` is incremented BEFORE checking `live` in concurrent read mode — this guarantees `deinit`'s `waitForReaders` sees any reader that proceeds
4. `store.arena.deinit()` is only called after `live = false` AND `reader_count == 0`
5. All keys and values inserted via `put()` are owned by the arena
6. `owner.store` is null after transfer or deinit — no double-free possible
7. `WriteTx.committed` and `ReadTx.released` are true after commit — idempotent, safe with defer