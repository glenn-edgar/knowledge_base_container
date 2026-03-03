const std = @import("std");
pub const EnvConfig = @import("types.zig").EnvConfig;

/// Type factory: returns a self-contained environment type parameterized by cfg.
/// The returned struct contains Owner, WriteTx, and ReadTx as public nested types.
/// The internal Store is never exposed.
pub fn ConfiguredEnvironment(comptime cfg: EnvConfig) type {
    comptime {
        if (cfg.initial_capacity == 0)
            @compileError("initial_capacity must be > 0");
    }

    return struct {
        const Self = @This();
        pub const Value = cfg.Value;
        const Table = std.StringHashMap(*Value);
        const MutexType = if (cfg.threadsafe) std.Thread.Mutex else void;

        /// Exposed for comptime introspection / tests.
        pub const is_threadsafe = cfg.threadsafe;
        pub const has_concurrent_reads = cfg.concurrent_reads;
        pub const interns_strings = cfg.intern_strings;

        // ----------------------------------------------------------------
        // Store — internal, never exposed
        // ----------------------------------------------------------------
        const ReaderCountType = if (cfg.threadsafe and cfg.concurrent_reads)
            std.atomic.Value(u32)
        else
            void;

        const Store = struct {
            arena: std.heap.ArenaAllocator,
            table: Table,
            mutex: MutexType,
            reader_count: ReaderCountType,
            live: bool,

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
        };

        // ----------------------------------------------------------------
        // Helpers
        // ----------------------------------------------------------------

        /// Convert the configured Allocator type to the std.mem.Allocator interface.
        fn resolveAllocator(backing: cfg.Allocator) std.mem.Allocator {
            if (cfg.Allocator == std.mem.Allocator) {
                return backing;
            } else {
                return backing.allocator();
            }
        }

        /// Comptime string interning: dupe []const u8 fields of Value into the arena.
        fn internValue(alloc: std.mem.Allocator, value: Value) !Value {
            if (!cfg.intern_strings) return value;

            const info = @typeInfo(Value);
            switch (info) {
                .Struct => {
                    var v = value;
                    inline for (std.meta.fields(Value)) |field| {
                        if (field.type == []const u8) {
                            @field(v, field.name) = try alloc.dupe(u8, @field(value, field.name));
                        }
                    }
                    return v;
                },
                .Union => |u| {
                    if (u.tag_type) |_| {
                        switch (value) {
                            inline else => |payload, tag| {
                                if (@TypeOf(payload) == []const u8) {
                                    return @unionInit(Value, @tagName(tag), try alloc.dupe(u8, payload));
                                }
                                const PayloadType = @TypeOf(payload);
                                if (@typeInfo(PayloadType) == .Struct) {
                                    var v = payload;
                                    inline for (std.meta.fields(PayloadType)) |field| {
                                        if (field.type == []const u8) {
                                            @field(v, field.name) = try alloc.dupe(u8, @field(payload, field.name));
                                        }
                                    }
                                    return @unionInit(Value, @tagName(tag), v);
                                }
                                return value;
                            },
                        }
                    } else {
                        return value;
                    }
                },
                else => return value,
            }
        }

        // ----------------------------------------------------------------
        // WriteTx — exclusive write transaction, holds mutex
        // ----------------------------------------------------------------
        pub const WriteTx = struct {
            store: *Store,
            committed: bool = false,

            /// Insert or update a symbol. Key and value are duped into the arena.
            pub fn put(self: *WriteTx, key: []const u8, value: Value) !void {
                const alloc = self.store.arena.allocator();
                const owned_key = try alloc.dupe(u8, key);
                const owned_val = try alloc.create(Value);
                owned_val.* = try internValue(alloc, value);
                try self.store.table.put(owned_key, owned_val);
            }

            /// Lookup a symbol. Returns mutable pointer (write tx holds exclusive lock).
            pub fn get(self: WriteTx, key: []const u8) ?*Value {
                return self.store.table.get(key);
            }

            /// Remove a symbol. Arena memory is not reclaimed (expected for arena allocators).
            pub fn remove(self: *WriteTx, key: []const u8) bool {
                return self.store.table.fetchRemove(key) != null;
            }

            /// Release the write lock. Idempotent — safe to call twice (e.g. explicit + defer).
            pub fn commit(self: *WriteTx) void {
                if (self.committed) return;
                self.committed = true;
                self.store.unlock();
            }

            /// Identical to commit for now. Future: snapshot/restore for true rollback.
            pub fn abort(self: *WriteTx) void {
                self.commit();
            }

            /// Returns the count of symbols in the table.
            pub fn count(self: WriteTx) u32 {
                return self.store.table.count();
            }
        };

        // ----------------------------------------------------------------
        // ReadTx — read transaction, const access only
        // ----------------------------------------------------------------
        pub const ReadTx = struct {
            store: *Store, // mutable pointer needed for unlock; API enforces const access
            released: bool = false,

            /// Lookup a symbol. Returns const pointer — mutation structurally impossible.
            pub fn get(self: ReadTx, key: []const u8) ?*const Value {
                return self.store.table.get(key);
            }

            /// Check whether a symbol exists.
            pub fn contains(self: ReadTx, key: []const u8) bool {
                return self.store.table.contains(key);
            }

            /// Release the read lock (if held). Idempotent.
            pub fn commit(self: *ReadTx) void {
                if (self.released) return;
                self.released = true;
                if (cfg.concurrent_reads) {
                    self.store.releaseReader();
                } else {
                    self.store.unlock();
                }
            }

            /// Returns the count of symbols in the table.
            pub fn count(self: ReadTx) u32 {
                return self.store.table.count();
            }
        };

        // ----------------------------------------------------------------
        // Owner — lifecycle manager, issues transactions
        // ----------------------------------------------------------------
        pub const Owner = struct {
            store: ?*Store,
            backing: cfg.Allocator,

            /// Allocate and initialize a new environment.
            pub fn init(backing: cfg.Allocator) !Owner {
                const alloc = resolveAllocator(backing);

                // Store struct lives on the backing allocator
                const s = try alloc.create(Store);

                // Arena is backed by the same allocator
                s.arena = std.heap.ArenaAllocator.init(alloc);
                s.live = true;

                // Mutex: default-init if threadsafe, void otherwise
                if (cfg.threadsafe) {
                    s.mutex = .{};
                }

                // Reader count: atomic init if concurrent_reads, void otherwise
                if (cfg.threadsafe and cfg.concurrent_reads) {
                    s.reader_count = std.atomic.Value(u32).init(0);
                }

                // Hash table uses the arena allocator — buckets live inside the arena
                s.table = Table.init(s.arena.allocator());
                try s.table.ensureTotalCapacity(cfg.initial_capacity);

                return .{ .store = s, .backing = backing };
            }

            /// Destroy the environment. Blocks until in-flight transactions complete.
            /// After deinit, all methods return error.OwnershipReleased.
            pub fn deinit(self: *Owner) void {
                const s = self.store orelse return;

                // Block until any in-flight write transaction completes
                s.lock();
                s.live = false;
                s.unlock();

                // Wait for all concurrent readers to finish
                s.waitForReaders();

                // One dealloc frees the entire arena: table internals, keys, values, strings
                s.arena.deinit();

                // Free the Store struct itself from the backing allocator
                const alloc = resolveAllocator(self.backing);
                alloc.destroy(s);
                self.store = null;
            }

            /// Transfer ownership. O(1) — one pointer swap, no data copy.
            /// The original Owner becomes inert (store = null).
            pub fn transfer(self: *Owner) error{AlreadyTransferred}!Owner {
                const s = self.store orelse return error.AlreadyTransferred;
                self.store = null;
                return .{ .store = s, .backing = self.backing };
            }

            /// Begin an exclusive write transaction. Acquires the mutex.
            /// Caller MUST use: defer tx.commit();
            pub fn beginWrite(self: Owner) !WriteTx {
                const s = self.store orelse return error.OwnershipReleased;
                s.lock();
                if (!s.live) {
                    s.unlock();
                    return error.StoreDestroyed;
                }
                return .{ .store = s, .committed = false };
            }

            /// Begin a read transaction.
            /// If concurrent_reads is false, acquires the mutex.
            /// If concurrent_reads is true, no lock (safe post-write-phase).
            /// Caller MUST use: defer tx.commit();
            pub fn beginRead(self: Owner) !ReadTx {
                const s = self.store orelse return error.OwnershipReleased;
                if (!cfg.concurrent_reads) {
                    s.lock();
                    if (!s.live) {
                        s.unlock();
                        return error.StoreDestroyed;
                    }
                } else {
                    // Increment reader count first, then check live.
                    // If live is false, decrement and return error.
                    // This ensures deinit's waitForReaders sees us if we proceed.
                    s.acquireReader();
                    if (!s.live) {
                        s.releaseReader();
                        return error.StoreDestroyed;
                    }
                }
                return .{ .store = s, .released = false };
            }
        };
    };
}

// ====================================================================
// Standard tier instantiations
// ====================================================================

/// Cortex-M0 / bare metal: no mutex, no string interning, minimal capacity.
pub fn MicroEnv(comptime V: type) type {
    return ConfiguredEnvironment(.{
        .Value = V,
        .threadsafe = false,
        .concurrent_reads = false,
        .initial_capacity = 16,
        .intern_strings = false,
    });
}

/// Cortex-M4 / RTOS: mutex, string interning, small capacity.
pub fn SmallEnv(comptime V: type) type {
    return ConfiguredEnvironment(.{
        .Value = V,
        .threadsafe = true,
        .concurrent_reads = false,
        .initial_capacity = 32,
        .intern_strings = true,
    });
}

/// Linux server: full threading, concurrent reads, large capacity.
pub fn ServerEnv(comptime V: type) type {
    return ConfiguredEnvironment(.{
        .Value = V,
        .threadsafe = true,
        .concurrent_reads = true,
        .initial_capacity = 256,
        .intern_strings = true,
    });
}