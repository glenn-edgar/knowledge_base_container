const std = @import("std");
const c = @import("c_api.zig");
const status = @import("status.zig");
pub const Error = status.Error;

// ----------------------------------------------------------------
//  Configuration
// ----------------------------------------------------------------

pub const Config = struct {
    server: [:0]const u8 = "nats://127.0.0.1:4222",
    bucket: [:0]const u8 = "keystore",
    description: [:0]const u8 = "NATS JetStream KeyStore",
    client_name: [:0]const u8 = "keystore-client",
    create_bucket: bool = true,
    history: i32 = 1,
    ttl_seconds: i64 = 0,
    max_reconnect: i32 = 3,
    reconnect_delay_s: f64 = 1.0,

    /// Convert to the C struct expected by ks_create().
    fn toCConfig(self: Config) c.KeyStoreConfig {
        var cfg: c.KeyStoreConfig = undefined;
        c.ks_config_defaults(&cfg);
        cfg.server = self.server.ptr;
        cfg.bucket = self.bucket.ptr;
        cfg.description = self.description.ptr;
        cfg.client_name = self.client_name.ptr;
        cfg.create_bucket = self.create_bucket;
        cfg.history = self.history;
        cfg.ttl_seconds = self.ttl_seconds;
        cfg.max_reconnect = self.max_reconnect;
        cfg.reconnect_delay_s = self.reconnect_delay_s;
        return cfg;
    }
};

// ----------------------------------------------------------------
//  KeyStore
// ----------------------------------------------------------------

pub const KeyStore = struct {
    handle: *c.KeyStore,

    const Self = @This();

    /// Create a new KeyStore (does NOT connect yet).
    pub fn init(cfg: Config) Error!Self {
        var handle: ?*c.KeyStore = null;
        var cc = cfg.toCConfig();
        try status.check(c.ks_create(&handle, &cc));
        return Self{ .handle = handle.? };
    }

    /// Destroy the KeyStore and free all resources.
    pub fn deinit(self: *Self) void {
        c.ks_destroy(self.handle);
        self.handle = undefined;
    }

    // ----------------------------------------------------------
    //  Connection
    // ----------------------------------------------------------

    pub fn connect(self: *Self) Error!void {
        try status.check(c.ks_connect(self.handle));
    }

    pub fn disconnect(self: *Self) Error!void {
        try status.check(c.ks_disconnect(self.handle));
    }

    pub fn isConnected(self: *const Self) bool {
        return c.ks_is_connected(self.handle);
    }

    pub fn lastNatsStatus(self: *const Self) c.NatsStatus {
        return c.ks_last_nats_status(self.handle);
    }

    // ----------------------------------------------------------
    //  Put
    // ----------------------------------------------------------

    /// Store a string value.  Returns the revision number.
    pub fn put(self: *Self, key: [:0]const u8, value: [:0]const u8) Error!u64 {
        var rev: u64 = 0;
        try status.check(c.ks_put(self.handle, key.ptr, value.ptr, &rev));
        return rev;
    }

    /// Store a string value, ignoring the revision.
    pub fn putNoRev(self: *Self, key: [:0]const u8, value: [:0]const u8) Error!void {
        try status.check(c.ks_put(self.handle, key.ptr, value.ptr, null));
    }

    // ----------------------------------------------------------
    //  Get
    // ----------------------------------------------------------

    /// Retrieve a value as a Zig-owned slice.
    /// Caller must free the returned slice with `std.c.free()` or
    /// use `getAlloc` for a std.mem.Allocator-managed result.
    pub fn getRaw(self: *Self, key: [:0]const u8) Error![:0]u8 {
        var ptr: ?[*:0]u8 = null;
        try status.check(c.ks_get(self.handle, key.ptr, @ptrCast(&ptr)));
        if (ptr) |p| {
            return std.mem.span(p);
        }
        return Error.NotFound;
    }

    /// Retrieve a value, copying it into a Zig allocator-managed buffer.
    pub fn getAlloc(self: *Self, allocator: std.mem.Allocator, key: [:0]const u8) (Error || std.mem.Allocator.Error)![]u8 {
        const raw = try self.getRaw(key);
        defer std.c.free(@ptrCast(@constCast(raw.ptr)));
        return try allocator.dupe(u8, raw);
    }

    /// Free a raw slice returned by `getRaw`.
    pub fn freeRaw(slice: [:0]u8) void {
        std.c.free(@ptrCast(@constCast(slice.ptr)));
    }

    /// Retrieve raw bytes.
    pub fn getBytes(self: *Self, key: [:0]const u8) Error![]u8 {
        var data: ?*anyopaque = null;
        var len: usize = 0;
        try status.check(c.ks_get_bytes(self.handle, key.ptr, &data, &len));
        if (data) |d| {
            const ptr: [*]u8 = @ptrCast(@alignCast(d));
            return ptr[0..len];
        }
        return Error.NotFound;
    }

    /// Free raw bytes returned by `getBytes`.
    pub fn freeBytes(buf: []u8) void {
        std.c.free(@ptrCast(buf.ptr));
    }

    // ----------------------------------------------------------
    //  Delete / Exists
    // ----------------------------------------------------------

    pub fn delete(self: *Self, key: [:0]const u8) Error!void {
        try status.check(c.ks_delete(self.handle, key.ptr));
    }

    pub fn exists(self: *Self, key: [:0]const u8) Error!bool {
        var ex: bool = false;
        try status.check(c.ks_exists(self.handle, key.ptr, &ex));
        return ex;
    }

    // ----------------------------------------------------------
    //  Key listing
    // ----------------------------------------------------------

    /// Result of a key listing.  Call `deinit()` when done.
    pub const KeyList = struct {
        keys: [][:0]const u8,  // Zig fat-pointer slices (allocated)
        raw_keys: ?[*]?[*:0]u8, // original C char** (freed by ks_free_keys)
        count: usize,

        pub fn deinit(self: *KeyList) void {
            // Free the Zig slice array we allocated
            if (self.keys.len > 0) {
                std.heap.c_allocator.free(self.keys);
            }
            // Free the C key strings
            if (self.raw_keys) |rk| {
                c.ks_free_keys(@ptrCast(rk), self.count);
            }
            self.keys = &.{};
            self.raw_keys = null;
        }

        pub fn slice(self: *const KeyList) [][:0]const u8 {
            return self.keys;
        }
    };

    /// List keys matching an optional glob pattern.
    /// Pass `null` for all keys.
    pub fn keys(self: *Self, pattern: ?[:0]const u8) Error!KeyList {
        var raw_keys: ?[*]?[*:0]u8 = null;
        var count: usize = 0;
        const pat_ptr: ?[*:0]const u8 = if (pattern) |p| p.ptr else null;

        try status.check(c.ks_keys(self.handle, pat_ptr, @ptrCast(&raw_keys), &count));

        if (raw_keys == null or count == 0) {
            return KeyList{
                .keys = &.{},
                .raw_keys = raw_keys,
                .count = 0,
            };
        }

        const rk = raw_keys.?;

        // Allocate a Zig array of fat-pointer slices, one per C string.
        // C char* is a thin pointer; Zig [:0]const u8 is ptr+len (fat),
        // so we must build them individually via std.mem.span().
        const zig_keys = std.heap.c_allocator.alloc([:0]const u8, count) catch
            return Error.OutOfMemory;

        for (0..count) |i| {
            if (rk[i]) |ptr| {
                zig_keys[i] = std.mem.span(ptr);
            } else {
                zig_keys[i] = "";
            }
        }

        return KeyList{
            .keys = zig_keys,
            .raw_keys = rk,
            .count = count,
        };
    }

    // ----------------------------------------------------------
    //  Atomic counters
    // ----------------------------------------------------------

    pub fn increment(self: *Self, key: [:0]const u8, delta: i64) Error!i64 {
        var new_val: i64 = 0;
        try status.check(c.ks_increment(self.handle, key.ptr, delta, &new_val));
        return new_val;
    }

    pub fn decrement(self: *Self, key: [:0]const u8, delta: i64) Error!i64 {
        var new_val: i64 = 0;
        try status.check(c.ks_decrement(self.handle, key.ptr, delta, &new_val));
        return new_val;
    }

    // ----------------------------------------------------------
    //  Sync wrappers (connect → op → disconnect)
    // ----------------------------------------------------------

    pub fn putSync(self: *Self, key: [:0]const u8, value: [:0]const u8) Error!u64 {
        var rev: u64 = 0;
        try status.check(c.ks_put_sync(self.handle, key.ptr, value.ptr, &rev));
        return rev;
    }

    pub fn getSync(self: *Self, key: [:0]const u8) Error![:0]u8 {
        var ptr: ?[*:0]u8 = null;
        try status.check(c.ks_get_sync(self.handle, key.ptr, @ptrCast(&ptr)));
        if (ptr) |p| {
            return std.mem.span(p);
        }
        return Error.NotFound;
    }

    pub fn deleteSync(self: *Self, key: [:0]const u8) Error!void {
        try status.check(c.ks_delete_sync(self.handle, key.ptr));
    }

    pub fn existsSync(self: *Self, key: [:0]const u8) Error!bool {
        var ex: bool = false;
        try status.check(c.ks_exists_sync(self.handle, key.ptr, &ex));
        return ex;
    }

    pub fn incrementSync(self: *Self, key: [:0]const u8, delta: i64) Error!i64 {
        var new_val: i64 = 0;
        try status.check(c.ks_increment_sync(self.handle, key.ptr, delta, &new_val));
        return new_val;
    }

    pub fn decrementSync(self: *Self, key: [:0]const u8, delta: i64) Error!i64 {
        var new_val: i64 = 0;
        try status.check(c.ks_decrement_sync(self.handle, key.ptr, delta, &new_val));
        return new_val;
    }
};