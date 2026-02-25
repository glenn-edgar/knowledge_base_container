//! kv_store_writer.zig - Writes retained key/value messages to an MQTT broker.
//!
//! Translated from C KVStoreWriter.
//! Requires: libmosquitto (apt install libmosquitto-dev)

const std = @import("std");
const log = std.log.scoped(.kv_writer);

const c = @cImport({
    @cInclude("mosquitto.h");
});

// ──────────────────────────────────────────────────────────────────────
//  Constants
// ──────────────────────────────────────────────────────────────────────

pub const MAX_TOPIC_LEN = 256;
pub const MAX_VALUE_LEN = 4096;
pub const MAX_PENDING = 128;
pub const MAX_BATCH = 64;

// ──────────────────────────────────────────────────────────────────────
//  Types
// ──────────────────────────────────────────────────────────────────────

pub const Config = struct {
    host: [:0]const u8 = "localhost",
    port: u16 = 1883,
    client_id: [:0]const u8 = "kv-writer",
    keepalive: u16 = 60,
    username: ?[:0]const u8 = null,
    password: ?[:0]const u8 = null,
    clean_session: bool = true,
};

pub const Qos = enum(c_int) {
    at_most_once = 0,
    at_least_once = 1,
    exactly_once = 2,
};

const PendingPublish = struct {
    mid: c_int = 0,
    completed: bool = false,
    success: bool = false,
};

pub const Error = error{
    MosquittoNew,
    ConnectFailed,
    LoopStartFailed,
    ConnectTimeout,
    NotConnected,
    PublishFailed,
    TooManyPending,
    PublishTimeout,
};

// ──────────────────────────────────────────────────────────────────────
//  Writer
// ──────────────────────────────────────────────────────────────────────

pub const Writer = struct {
    mosq: *c.struct_mosquitto = undefined,
    cfg: Config,

    // Publish tracking
    pending: [MAX_PENDING]PendingPublish = [_]PendingPublish{.{}} ** MAX_PENDING,
    pending_count: usize = 0,
    pending_mutex: std.Thread.Mutex = .{},
    pending_cond: std.Thread.Condition = .{},

    // Connection state
    connected: bool = false,
    running: bool = false,
    connect_done: bool = false,
    state_mutex: std.Thread.Mutex = .{},
    connect_cond: std.Thread.Condition = .{},

    const Self = @This();

    // ── Pending helpers (caller must hold pending_mutex) ─────────────

    fn pendingAdd(self: *Self, mid: c_int) ?usize {
        if (self.pending_count >= MAX_PENDING) return null;
        const idx = self.pending_count;
        self.pending[idx] = .{ .mid = mid, .completed = false, .success = false };
        self.pending_count += 1;
        return idx;
    }

    fn pendingFind(self: *Self, mid: c_int) ?usize {
        for (0..self.pending_count) |i| {
            if (self.pending[i].mid == mid) return i;
        }
        return null;
    }

    fn pendingRemove(self: *Self, idx: usize) void {
        if (idx >= self.pending_count) return;
        self.pending[idx] = self.pending[self.pending_count - 1];
        self.pending_count -= 1;
    }

    /// Wait for a specific mid to complete. Caller must hold pending_mutex.
    fn pendingWait(self: *Self, mid: c_int, timeout_ns: u64) bool {
        const deadline_ns = @as(u64, @intCast(std.time.nanoTimestamp())) + timeout_ns;
        while (true) {
            const idx = self.pendingFind(mid) orelse return false;
            if (self.pending[idx].completed) return true;

            const now_ns = @as(u64, @intCast(std.time.nanoTimestamp()));
            if (now_ns >= deadline_ns) return false;
            const remaining = deadline_ns - now_ns;

            self.pending_cond.timedWait(&self.pending_mutex, remaining) catch return false;
        }
    }

    // ── Public API ───────────────────────────────────────────────────

    pub fn init(cfg: Config) Error!Self {
        var self = Self{ .cfg = cfg };

        const mosq = c.mosquitto_new(cfg.client_id.ptr, cfg.clean_session, null);
        if (mosq == null) {
            log.err("[Error] mosquitto_new failed", .{});
            return Error.MosquittoNew;
        }
        self.mosq = mosq.?;

        if (cfg.username) |user| {
            const pw: ?[*:0]const u8 = if (cfg.password) |p| p.ptr else null;
            _ = c.mosquitto_username_pw_set(self.mosq, user.ptr, pw);
        }

        c.mosquitto_connect_callback_set(self.mosq, onConnectCb);
        c.mosquitto_disconnect_callback_set(self.mosq, onDisconnectCb);
        c.mosquitto_publish_callback_set(self.mosq, onPublishCb);

        return self;
    }

    pub fn connect(self: *Self, timeout_ms: u64) Error!void {
        c.mosquitto_user_data_set(self.mosq, @ptrCast(self));

        self.state_mutex.lock();
        self.connect_done = false;
        self.running = true;
        self.state_mutex.unlock();

        const rc_conn = c.mosquitto_connect(
            self.mosq,
            self.cfg.host.ptr,
            @intCast(self.cfg.port),
            @intCast(self.cfg.keepalive),
        );
        if (rc_conn != c.MOSQ_ERR_SUCCESS) {
            log.err("[Error] Connection failed: {s}", .{c.mosquitto_strerror(rc_conn)});
            self.running = false;
            return Error.ConnectFailed;
        }

        const rc_loop = c.mosquitto_loop_start(self.mosq);
        if (rc_loop != c.MOSQ_ERR_SUCCESS) {
            log.err("[Error] loop_start failed: {s}", .{c.mosquitto_strerror(rc_loop)});
            self.running = false;
            return Error.LoopStartFailed;
        }

        // Wait for CONNACK
        self.state_mutex.lock();
        defer self.state_mutex.unlock();

        if (!self.connect_done) {
            self.connect_cond.timedWait(&self.state_mutex, timeout_ms * std.time.ns_per_ms) catch {};
        }

        if (!self.connected) {
            log.err("[Error] Connection timeout or refused ({s}:{d})", .{ self.cfg.host, self.cfg.port });
            self.running = false;
            _ = c.mosquitto_loop_stop(self.mosq, true);
            return Error.ConnectTimeout;
        }

        log.info("[Connected] Successfully connected to {s}:{d}", .{ self.cfg.host, self.cfg.port });
    }

    pub fn disconnect(self: *Self) void {
        self.running = false;
        _ = c.mosquitto_disconnect(self.mosq);
        _ = c.mosquitto_loop_stop(self.mosq, false);
        self.state_mutex.lock();
        defer self.state_mutex.unlock();
        self.connected = false;
    }

    pub fn deinit(self: *Self) void {
        if (self.running or self.connected) {
            self.disconnect();
        }
        c.mosquitto_destroy(self.mosq);
    }

    /// Write a single key/value pair. Returns true on success.
    pub fn writeSingle(
        self: *Self,
        topic: [:0]const u8,
        value: ?[]const u8,
        qos: Qos,
        retain: bool,
        timeout_ms: u64,
    ) bool {
        if (!self.connected) {
            log.err("[Error] Not connected. Call connect() first.", .{});
            return false;
        }

        const payload = value orelse "";
        var mid: c_int = 0;
        const rc = c.mosquitto_publish(
            self.mosq,
            &mid,
            topic.ptr,
            @intCast(payload.len),
            payload.ptr,
            @intFromEnum(qos),
            retain,
        );
        if (rc != c.MOSQ_ERR_SUCCESS) {
            log.err("[Error] Failed to queue message for {s}: {s}", .{ topic, c.mosquitto_strerror(rc) });
            return false;
        }

        const timeout_ns = timeout_ms * std.time.ns_per_ms;

        self.pending_mutex.lock();
        defer self.pending_mutex.unlock();

        if (self.pendingAdd(mid) == null) {
            log.err("[Error] Too many pending publishes", .{});
            return false;
        }

        const completed = self.pendingWait(mid, timeout_ns);
        var success = false;

        if (completed) {
            if (self.pendingFind(mid)) |idx| {
                success = self.pending[idx].success;
                self.pendingRemove(idx);
            }
        } else {
            log.err("[Timeout] Publish timeout for {s}", .{topic});
            if (self.pendingFind(mid)) |idx| {
                self.pendingRemove(idx);
            }
        }

        if (success) {
            log.info("[Written] {s} => {s}", .{ topic, payload });
        }

        return success;
    }

    /// Write multiple key/value pairs. Returns count of successes.
    pub fn writeBatch(
        self: *Self,
        topics: []const [:0]const u8,
        values: []const ?[]const u8,
        qos: Qos,
        retain: bool,
        timeout_ms: u64,
    ) usize {
        if (!self.connected) {
            log.err("[Error] Not connected. Call connect() first.", .{});
            return 0;
        }

        const count = @min(topics.len, MAX_BATCH);
        const timeout_ns = timeout_ms * std.time.ns_per_ms;

        var mids: [MAX_BATCH]c_int = [_]c_int{0} ** MAX_BATCH;
        var queued: [MAX_BATCH]bool = [_]bool{false} ** MAX_BATCH;

        // Phase 1: publish all messages and register mids
        self.pending_mutex.lock();
        for (0..count) |i| {
            const payload = values[i] orelse "";
            const rc = c.mosquitto_publish(
                self.mosq,
                &mids[i],
                topics[i].ptr,
                @intCast(payload.len),
                payload.ptr,
                @intFromEnum(qos),
                retain,
            );
            if (rc != c.MOSQ_ERR_SUCCESS) {
                log.err("[Error] Failed to queue {s}: {s}", .{ topics[i], c.mosquitto_strerror(rc) });
                continue;
            }
            if (self.pendingAdd(mids[i]) == null) {
                log.err("[Error] Too many pending publishes", .{});
                continue;
            }
            queued[i] = true;
        }
        self.pending_mutex.unlock();

        // Phase 2: wait for each queued message
        var success_count: usize = 0;

        self.pending_mutex.lock();
        defer self.pending_mutex.unlock();

        for (0..count) |i| {
            if (!queued[i]) continue;

            const completed = self.pendingWait(mids[i], timeout_ns);
            const idx = self.pendingFind(mids[i]);

            if (completed) {
                if (idx) |j| {
                    if (self.pending[j].success) {
                        success_count += 1;
                        log.info("[Batch Written] {s}", .{topics[i]});
                    }
                }
            } else {
                log.err("[Batch Timeout] {s}", .{topics[i]});
            }

            if (idx) |j| self.pendingRemove(j);
        }

        return success_count;
    }

    /// Delete a single key by publishing an empty retained message.
    pub fn deleteSingle(self: *Self, topic: [:0]const u8, timeout_ms: u64) bool {
        if (!self.connected) {
            log.err("[Error] Not connected. Call connect() first.", .{});
            return false;
        }

        var mid: c_int = 0;
        const rc = c.mosquitto_publish(self.mosq, &mid, topic.ptr, 0, "", 1, true);
        if (rc != c.MOSQ_ERR_SUCCESS) {
            log.err("[Error] Failed to delete {s}: {s}", .{ topic, c.mosquitto_strerror(rc) });
            return false;
        }

        const timeout_ns = timeout_ms * std.time.ns_per_ms;

        self.pending_mutex.lock();
        defer self.pending_mutex.unlock();

        if (self.pendingAdd(mid) == null) {
            log.err("[Error] Too many pending publishes", .{});
            return false;
        }

        const completed = self.pendingWait(mid, timeout_ns);
        var success = false;

        if (completed) {
            if (self.pendingFind(mid)) |idx| {
                success = self.pending[idx].success;
                self.pendingRemove(idx);
            }
        } else {
            log.err("[Timeout] Delete timeout for {s}", .{topic});
            if (self.pendingFind(mid)) |idx| {
                self.pendingRemove(idx);
            }
        }

        if (success) {
            log.info("[Deleted] {s}", .{topic});
        }

        return success;
    }

    /// Delete multiple keys. Returns count of successes.
    pub fn deleteBatch(
        self: *Self,
        topics: []const [:0]const u8,
        timeout_ms: u64,
    ) usize {
        if (!self.connected) {
            log.err("[Error] Not connected. Call connect() first.", .{});
            return 0;
        }

        const count = @min(topics.len, MAX_BATCH);
        const timeout_ns = timeout_ms * std.time.ns_per_ms;

        var mids: [MAX_BATCH]c_int = [_]c_int{0} ** MAX_BATCH;
        var queued: [MAX_BATCH]bool = [_]bool{false} ** MAX_BATCH;

        // Phase 1: publish all empty retained messages
        self.pending_mutex.lock();
        for (0..count) |i| {
            const rc = c.mosquitto_publish(self.mosq, &mids[i], topics[i].ptr, 0, "", 1, true);
            if (rc != c.MOSQ_ERR_SUCCESS) {
                log.err("[Error] Failed to queue delete for {s}: {s}", .{ topics[i], c.mosquitto_strerror(rc) });
                continue;
            }
            if (self.pendingAdd(mids[i]) == null) {
                log.err("[Error] Too many pending publishes", .{});
                continue;
            }
            queued[i] = true;
        }
        self.pending_mutex.unlock();

        // Phase 2: wait for each
        var success_count: usize = 0;

        self.pending_mutex.lock();
        defer self.pending_mutex.unlock();

        for (0..count) |i| {
            if (!queued[i]) continue;

            const completed = self.pendingWait(mids[i], timeout_ns);
            const idx = self.pendingFind(mids[i]);

            if (completed) {
                if (idx) |j| {
                    if (self.pending[j].success) {
                        success_count += 1;
                        log.info("[Batch Deleted] {s}", .{topics[i]});
                    }
                }
            } else {
                log.err("[Delete Timeout] {s}", .{topics[i]});
            }

            if (idx) |j| self.pendingRemove(j);
        }

        return success_count;
    }

    /// Update a key's value (convenience: writeSingle with retain=true).
    pub fn updateSingle(self: *Self, topic: [:0]const u8, value: []const u8, qos: Qos, timeout_ms: u64) bool {
        return self.writeSingle(topic, value, qos, true, timeout_ms);
    }

    pub fn isConnected(self: *Self) bool {
        self.state_mutex.lock();
        defer self.state_mutex.unlock();
        return self.connected;
    }

    // ── Mosquitto C callbacks ────────────────────────────────────────

    fn onConnectCb(_: ?*c.struct_mosquitto, userdata: ?*anyopaque, rc: c_int) callconv(.C) void {
        const self: *Self = @ptrCast(@alignCast(userdata));
        self.state_mutex.lock();
        defer self.state_mutex.unlock();
        self.connected = (rc == 0);
        self.connect_done = true;
        self.connect_cond.signal();
    }

    fn onDisconnectCb(_: ?*c.struct_mosquitto, userdata: ?*anyopaque, _: c_int) callconv(.C) void {
        const self: *Self = @ptrCast(@alignCast(userdata));
        self.state_mutex.lock();
        defer self.state_mutex.unlock();
        self.connected = false;
    }

    fn onPublishCb(_: ?*c.struct_mosquitto, userdata: ?*anyopaque, mid: c_int) callconv(.C) void {
        const self: *Self = @ptrCast(@alignCast(userdata));
        self.pending_mutex.lock();
        defer self.pending_mutex.unlock();
        if (self.pendingFind(mid)) |idx| {
            self.pending[idx].completed = true;
            self.pending[idx].success = true;
        }
        self.pending_cond.broadcast();
    }
};

// ──────────────────────────────────────────────────────────────────────
//  Tests
// ──────────────────────────────────────────────────────────────────────

test "Writer Config defaults" {
    const cfg = Config{};
    try std.testing.expectEqualStrings("localhost", cfg.host);
    try std.testing.expectEqual(@as(u16, 1883), cfg.port);
    try std.testing.expectEqualStrings("kv-writer", cfg.client_id);
    try std.testing.expect(cfg.clean_session);
}