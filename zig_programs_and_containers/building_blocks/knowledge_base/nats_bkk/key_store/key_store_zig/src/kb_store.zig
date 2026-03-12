const std = @import("std");
const c = @import("c_api.zig");
const status = @import("status.zig");
const key_store = @import("key_store.zig");
pub const Error = status.Error;

// ----------------------------------------------------------------
//  KbEntry – result of a KB get
// ----------------------------------------------------------------

pub const KbEntry = struct {
    label_json: [:0]u8,
    node_json: [:0]u8,
    raw: c.KbEntry,

    const Self = @This();

    pub fn deinit(self: *Self) void {
        c.kb_entry_free(&self.raw);
        self.label_json = undefined;
        self.node_json = undefined;
    }

    /// Copy both JSON strings into a Zig allocator.
    pub fn cloneAlloc(self: *const Self, allocator: std.mem.Allocator) (Error || std.mem.Allocator.Error)!struct { label: []u8, node: []u8 } {
        return .{
            .label = try allocator.dupe(u8, self.label_json),
            .node = try allocator.dupe(u8, self.node_json),
        };
    }
};

// ----------------------------------------------------------------
//  KbStats
// ----------------------------------------------------------------

pub const KbStats = struct {
    total_kb_keys: usize,
    total_topics: usize,
    all_keys_count: usize,
    topic_names: [][:0]const u8,
    topic_counts: []usize,
    raw: c.KbStats,

    const Self = @This();

    pub fn deinit(self: *Self) void {
        // Free the Zig-allocated names array (strings are owned by C)
        if (self.topic_names.len > 0) {
            std.heap.c_allocator.free(self.topic_names);
        }
        c.kb_stats_free(&self.raw);
        self.topic_names = &.{};
        self.topic_counts = &.{};
    }
};

// ----------------------------------------------------------------
//  KbStore
// ----------------------------------------------------------------

pub const KbStore = struct {
    handle: *c.KbStore,

    const Self = @This();

    /// Create a KbStore.  Internally creates its own KeyStore.
    pub fn init(
        server: [:0]const u8,
        bucket: [:0]const u8,
        description: ?[:0]const u8,
    ) Error!Self {
        var handle: ?*c.KbStore = null;
        const desc_ptr: ?[*:0]const u8 = if (description) |d| d.ptr else null;
        try status.check(c.kb_create(&handle, server.ptr, bucket.ptr, desc_ptr));
        return Self{ .handle = handle.? };
    }

    pub fn deinit(self: *Self) void {
        c.kb_destroy(self.handle);
        self.handle = undefined;
    }

    /// Access the underlying KeyStore for connect/disconnect.
    pub fn getKeyStore(self: *Self) key_store.KeyStore {
        const raw = c.kb_get_keystore(self.handle);
        return key_store.KeyStore{ .handle = raw.? };
    }

    // ----------------------------------------------------------
    //  Validation helpers
    // ----------------------------------------------------------

    pub fn validateTopic(topic: [:0]const u8) Error!void {
        try status.check(c.kb_validate_topic(topic.ptr));
    }

    pub fn validateLabelName(name: [:0]const u8) Error!void {
        try status.check(c.kb_validate_label_name(name.ptr));
    }

    pub fn validateNodeName(name: [:0]const u8) Error!void {
        try status.check(c.kb_validate_node_name(name.ptr));
    }

    pub fn validateKeyFormat(kb_key: [:0]const u8) bool {
        return c.kb_validate_key_format(kb_key.ptr);
    }

    // ----------------------------------------------------------
    //  Store
    // ----------------------------------------------------------

    pub const StoreResult = struct {
        key: [:0]u8,

        pub fn deinit(self: *StoreResult) void {
            std.c.free(@ptrCast(@constCast(self.key.ptr)));
            self.key = undefined;
        }
    };

    /// Store a KB entry.
    ///
    /// When `composite` is true the returned key is the full
    /// `base_topic.label_name.node_name`; otherwise it is a copy
    /// of `base_topic`.
    pub fn store(
        self: *Self,
        base_topic: [:0]const u8,
        label_name: [:0]const u8,
        node_name: [:0]const u8,
        label_json: [:0]const u8,
        node_json: [:0]const u8,
        composite: bool,
    ) Error!StoreResult {
        var out_key: ?[*:0]u8 = null;
        try status.check(c.kb_store(
            self.handle,
            base_topic.ptr,
            label_name.ptr,
            node_name.ptr,
            label_json.ptr,
            node_json.ptr,
            composite,
            @ptrCast(&out_key),
        ));
        if (out_key) |k| {
            return StoreResult{ .key = std.mem.span(k) };
        }
        return Error.OutOfMemory;
    }

    /// Store without returning a key.
    pub fn storeNoKey(
        self: *Self,
        base_topic: [:0]const u8,
        label_name: [:0]const u8,
        node_name: [:0]const u8,
        label_json: [:0]const u8,
        node_json: [:0]const u8,
    ) Error!void {
        try status.check(c.kb_store(
            self.handle,
            base_topic.ptr,
            label_name.ptr,
            node_name.ptr,
            label_json.ptr,
            node_json.ptr,
            true,
            null,
        ));
    }

    // ----------------------------------------------------------
    //  Get
    // ----------------------------------------------------------

    pub fn get(self: *Self, kb_key: [:0]const u8) Error!KbEntry {
        var raw: c.KbEntry = undefined;
        try status.check(c.kb_get(self.handle, kb_key.ptr, &raw));
        return KbEntry{
            .label_json = std.mem.span(@as([*:0]u8, @ptrCast(raw.label_json))),
            .node_json = std.mem.span(@as([*:0]u8, @ptrCast(raw.node_json))),
            .raw = raw,
        };
    }

    // ----------------------------------------------------------
    //  Delete
    // ----------------------------------------------------------

    pub fn delete(self: *Self, kb_key: [:0]const u8) Error!void {
        try status.check(c.kb_delete(self.handle, kb_key.ptr));
    }

    // ----------------------------------------------------------
    //  Pop key (remove last 2 segments)
    // ----------------------------------------------------------

    pub fn popKey(kb_key: [:0]const u8) Error![:0]u8 {
        var out: ?[*:0]u8 = null;
        try status.check(c.kb_pop_key(kb_key.ptr, @ptrCast(&out)));
        if (out) |o| {
            return std.mem.span(o);
        }
        return Error.OutOfMemory;
    }

    /// Free a string returned by popKey.
    pub fn freePopKey(s: [:0]u8) void {
        std.c.free(@ptrCast(@constCast(s.ptr)));
    }

    // ----------------------------------------------------------
    //  List keys
    // ----------------------------------------------------------

    pub fn listKeys(self: *Self, base_topic: ?[:0]const u8) Error!key_store.KeyStore.KeyList {
        var raw_keys: ?[*]?[*:0]u8 = null;
        var count: usize = 0;
        const topic_ptr: ?[*:0]const u8 = if (base_topic) |t| t.ptr else null;

        try status.check(c.kb_list_keys(self.handle, topic_ptr, @ptrCast(&raw_keys), &count));

        if (raw_keys == null or count == 0) {
            return key_store.KeyStore.KeyList{
                .keys = &.{},
                .raw_keys = raw_keys,
                .count = 0,
            };
        }

        const rk = raw_keys.?;
        const zig_keys = std.heap.c_allocator.alloc([:0]const u8, count) catch
            return Error.OutOfMemory;
        for (0..count) |i| {
            if (rk[i]) |ptr| {
                zig_keys[i] = std.mem.span(ptr);
            } else {
                zig_keys[i] = "";
            }
        }

        return key_store.KeyStore.KeyList{
            .keys = zig_keys,
            .raw_keys = rk,
            .count = count,
        };
    }

    // ----------------------------------------------------------
    //  Stats
    // ----------------------------------------------------------

    pub fn getStats(self: *Self) Error!KbStats {
        var raw: c.KbStats = undefined;
        try status.check(c.kb_get_stats(self.handle, &raw));

        const counts: [*]usize = @ptrCast(raw.topic_counts);
        const n = raw.topic_array_len;

        // Build Zig fat-pointer slices from C thin char* pointers
        const names = std.heap.c_allocator.alloc([:0]const u8, n) catch
            return Error.OutOfMemory;
        const raw_names: [*]?[*:0]u8 = @ptrCast(raw.topic_names);
        for (0..n) |i| {
            if (raw_names[i]) |ptr| {
                names[i] = std.mem.span(ptr);
            } else {
                names[i] = "";
            }
        }

        return KbStats{
            .total_kb_keys = raw.total_kb_keys,
            .total_topics = raw.total_topics,
            .all_keys_count = raw.all_keys_count,
            .topic_names = names,
            .topic_counts = counts[0..n],
            .raw = raw,
        };
    }

    // ----------------------------------------------------------
    //  Sync wrappers
    // ----------------------------------------------------------

    pub fn storeSync(
        self: *Self,
        base_topic: [:0]const u8,
        label_name: [:0]const u8,
        node_name: [:0]const u8,
        label_json: [:0]const u8,
        node_json: [:0]const u8,
        composite: bool,
    ) Error!StoreResult {
        var out_key: ?[*:0]u8 = null;
        try status.check(c.kb_store_sync(
            self.handle,
            base_topic.ptr,
            label_name.ptr,
            node_name.ptr,
            label_json.ptr,
            node_json.ptr,
            composite,
            @ptrCast(&out_key),
        ));
        if (out_key) |k| {
            return StoreResult{ .key = std.mem.span(k) };
        }
        return Error.OutOfMemory;
    }

    pub fn getSync(self: *Self, kb_key: [:0]const u8) Error!KbEntry {
        var raw: c.KbEntry = undefined;
        try status.check(c.kb_get_sync(self.handle, kb_key.ptr, &raw));
        return KbEntry{
            .label_json = std.mem.span(@as([*:0]u8, @ptrCast(raw.label_json))),
            .node_json = std.mem.span(@as([*:0]u8, @ptrCast(raw.node_json))),
            .raw = raw,
        };
    }

    pub fn deleteSync(self: *Self, kb_key: [:0]const u8) Error!void {
        try status.check(c.kb_delete_sync(self.handle, kb_key.ptr));
    }

    pub fn listKeysSync(self: *Self, base_topic: ?[:0]const u8) Error!key_store.KeyStore.KeyList {
        var raw_keys: ?[*]?[*:0]u8 = null;
        var count: usize = 0;
        const topic_ptr: ?[*:0]const u8 = if (base_topic) |t| t.ptr else null;

        try status.check(c.kb_list_keys_sync(self.handle, topic_ptr, @ptrCast(&raw_keys), &count));

        if (raw_keys == null or count == 0) {
            return key_store.KeyStore.KeyList{
                .keys = &.{},
                .raw_keys = raw_keys,
                .count = 0,
            };
        }

        const rk = raw_keys.?;
        const zig_keys = std.heap.c_allocator.alloc([:0]const u8, count) catch
            return Error.OutOfMemory;
        for (0..count) |i| {
            if (rk[i]) |ptr| {
                zig_keys[i] = std.mem.span(ptr);
            } else {
                zig_keys[i] = "";
            }
        }

        return key_store.KeyStore.KeyList{
            .keys = zig_keys,
            .raw_keys = rk,
            .count = count,
        };
    }

    pub fn getStatsSync(self: *Self) Error!KbStats {
        var raw: c.KbStats = undefined;
        try status.check(c.kb_get_stats_sync(self.handle, &raw));

        const counts: [*]usize = @ptrCast(raw.topic_counts);
        const n = raw.topic_array_len;

        const names = std.heap.c_allocator.alloc([:0]const u8, n) catch
            return Error.OutOfMemory;
        const raw_names: [*]?[*:0]u8 = @ptrCast(raw.topic_names);
        for (0..n) |i| {
            if (raw_names[i]) |ptr| {
                names[i] = std.mem.span(ptr);
            } else {
                names[i] = "";
            }
        }

        return KbStats{
            .total_kb_keys = raw.total_kb_keys,
            .total_topics = raw.total_topics,
            .all_keys_count = raw.all_keys_count,
            .topic_names = names,
            .topic_counts = counts[0..n],
            .raw = raw,
        };
    }
};