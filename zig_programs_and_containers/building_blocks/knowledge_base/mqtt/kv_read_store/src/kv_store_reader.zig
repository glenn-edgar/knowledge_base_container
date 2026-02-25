//! kv_store_reader.zig - Reads retained key/value messages from an MQTT broker.
//!
//! Translated from C KVStoreReader.
//! Requires: libmosquitto (apt install libmosquitto-dev)

const std = @import("std");
const log = std.log.scoped(.kv_reader);

const c = @cImport({
    @cInclude("mosquitto.h");
});

// ──────────────────────────────────────────────────────────────────────
//  Constants
// ──────────────────────────────────────────────────────────────────────

pub const MAX_TOPIC_LEN = 256;
pub const MAX_VALUE_LEN = 4096;
pub const MAX_ENTRIES = 256;
pub const MAX_SENTINELS = 8;

// ──────────────────────────────────────────────────────────────────────
//  Types
// ──────────────────────────────────────────────────────────────────────

pub const Config = struct {
    host: [:0]const u8 = "localhost",
    port: u16 = 1883,
    client_id: [:0]const u8 = "kv-reader",
    keepalive: u16 = 60,
    username: ?[:0]const u8 = null,
    password: ?[:0]const u8 = null,
    clean_session: bool = true,
};

pub const Entry = struct {
    topic: [MAX_TOPIC_LEN]u8 = [_]u8{0} ** MAX_TOPIC_LEN,
    value: [MAX_VALUE_LEN]u8 = [_]u8{0} ** MAX_VALUE_LEN,
    active: bool = false,

    /// Return the topic as a slice.
    pub fn topicSlice(self: *const Entry) []const u8 {
        return std.mem.sliceTo(&self.topic, 0);
    }

    /// Return the value as a slice.
    pub fn valueSlice(self: *const Entry) []const u8 {
        return std.mem.sliceTo(&self.value, 0);
    }
};

pub const Error = error{
    MosquittoNew,
    ConnectFailed,
    LoopStartFailed,
    ConnectTimeout,
    NotConnected,
    SubscribeFailed,
};

// ──────────────────────────────────────────────────────────────────────
//  Reader
// ──────────────────────────────────────────────────────────────────────

pub const Reader = struct {
    mosq: *c.struct_mosquitto = undefined,
    cfg: Config,

    // Collected KV entries
    entries: [MAX_ENTRIES]Entry = [_]Entry{.{}} ** MAX_ENTRIES,
    entry_count: usize = 0,
    entries_mutex: std.Thread.Mutex = .{},

    // Sentinel coordination
    sentinels: [MAX_SENTINELS][MAX_TOPIC_LEN]u8 = [_][MAX_TOPIC_LEN]u8{[_]u8{0} ** MAX_TOPIC_LEN} ** MAX_SENTINELS,
    sentinel_count: usize = 0,
    sentinel_fired: bool = false,
    sentinel_mutex: std.Thread.Mutex = .{},
    sentinel_cond: std.Thread.Condition = .{},

    // Subscribe acknowledgement
    sub_acked: bool = false,
    sub_mutex: std.Thread.Mutex = .{},
    sub_cond: std.Thread.Condition = .{},

    // Connection state
    connected: bool = false,
    running: bool = false,
    connect_done: bool = false,
    state_mutex: std.Thread.Mutex = .{},
    connect_cond: std.Thread.Condition = .{},

    const Self = @This();

    // ── Internal helpers ─────────────────────────────────────────────

    /// Check if topic is a sentinel. Caller must hold sentinel_mutex.
    fn isSentinel(self: *Self, topic: []const u8) bool {
        for (0..self.sentinel_count) |i| {
            const sentinel = std.mem.sliceTo(&self.sentinels[i], 0);
            if (std.mem.eql(u8, sentinel, topic)) return true;
        }
        return false;
    }

    /// Store a KV entry. Overwrites if topic exists. Caller must hold entries_mutex.
    fn storeEntry(self: *Self, topic: []const u8, value: []const u8) void {
        // Check for existing — overwrite
        for (0..self.entry_count) |i| {
            if (self.entries[i].active) {
                const existing = std.mem.sliceTo(&self.entries[i].topic, 0);
                if (std.mem.eql(u8, existing, topic)) {
                    const vlen = @min(value.len, MAX_VALUE_LEN - 1);
                    @memcpy(self.entries[i].value[0..vlen], value[0..vlen]);
                    self.entries[i].value[vlen] = 0;
                    return;
                }
            }
        }
        // Append new
        if (self.entry_count < MAX_ENTRIES) {
            var e = &self.entries[self.entry_count];
            const tlen = @min(topic.len, MAX_TOPIC_LEN - 1);
            @memcpy(e.topic[0..tlen], topic[0..tlen]);
            e.topic[tlen] = 0;
            const vlen = @min(value.len, MAX_VALUE_LEN - 1);
            @memcpy(e.value[0..vlen], value[0..vlen]);
            e.value[vlen] = 0;
            e.active = true;
            self.entry_count += 1;
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
        c.mosquitto_message_callback_set(self.mosq, onMessageCb);
        c.mosquitto_subscribe_callback_set(self.mosq, onSubscribeCb);

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

    /// Read retained messages matching a topic pattern.
    /// Subscribes, collects retained messages until sentinel fires or timeout.
    /// Returns number of entries written to out_entries.
    pub fn readPattern(
        self: *Self,
        pattern: [:0]const u8,
        qos: c_int,
        timeout_ms: u64,
        sentinel_topics: ?[]const [:0]const u8,
        wait_for_sentinel: bool,
        out_entries: []Entry,
    ) usize {
        if (!self.connected) {
            log.err("[Error] Not connected. Call connect() first.", .{});
            return 0;
        }

        // Clear previous entries
        self.entries_mutex.lock();
        self.entry_count = 0;
        self.entries_mutex.unlock();

        // Setup sentinels
        self.sentinel_mutex.lock();
        self.sentinel_fired = false;
        self.sentinel_count = 0;
        if (sentinel_topics) |sents| {
            for (sents) |s| {
                if (self.sentinel_count >= MAX_SENTINELS) break;
                const slen = @min(s.len, MAX_TOPIC_LEN - 1);
                @memcpy(self.sentinels[self.sentinel_count][0..slen], s[0..slen]);
                self.sentinels[self.sentinel_count][slen] = 0;
                self.sentinel_count += 1;
            }
        }
        self.sentinel_mutex.unlock();

        // Subscribe to pattern
        self.sub_mutex.lock();
        self.sub_acked = false;
        self.sub_mutex.unlock();

        const rc = c.mosquitto_subscribe(self.mosq, null, pattern.ptr, qos);
        if (rc != c.MOSQ_ERR_SUCCESS) {
            log.err("[Error] Subscribe failed for {s}: {s}", .{ pattern, c.mosquitto_strerror(rc) });
            self.sentinel_mutex.lock();
            self.sentinel_count = 0;
            self.sentinel_mutex.unlock();
            return 0;
        }

        // Wait for SUBACK (best-effort, 2s cap)
        {
            self.sub_mutex.lock();
            defer self.sub_mutex.unlock();
            if (!self.sub_acked) {
                self.sub_cond.timedWait(&self.sub_mutex, 2000 * std.time.ns_per_ms) catch {};
            }
        }

        // Wait for retained messages: sentinel or timeout
        if (wait_for_sentinel and self.sentinel_count > 0) {
            self.sentinel_mutex.lock();
            defer self.sentinel_mutex.unlock();
            if (!self.sentinel_fired) {
                self.sentinel_cond.timedWait(&self.sentinel_mutex, timeout_ms * std.time.ns_per_ms) catch {};
            }
        } else {
            std.time.sleep(timeout_ms * std.time.ns_per_ms);
        }

        // Unsubscribe
        _ = c.mosquitto_unsubscribe(self.mosq, null, pattern.ptr);

        // Copy results, excluding sentinels
        var out_count: usize = 0;
        self.entries_mutex.lock();
        defer self.entries_mutex.unlock();

        for (0..self.entry_count) |i| {
            if (!self.entries[i].active) continue;
            if (out_count >= out_entries.len) break;

            const topic = std.mem.sliceTo(&self.entries[i].topic, 0);

            self.sentinel_mutex.lock();
            const is_sent = self.isSentinel(topic);
            self.sentinel_mutex.unlock();
            if (is_sent) continue;

            out_entries[out_count] = self.entries[i];
            out_count += 1;
        }

        // Clear sentinels
        self.sentinel_mutex.lock();
        self.sentinel_count = 0;
        self.sentinel_mutex.unlock();

        return out_count;
    }

    /// Read a single retained value for an exact topic.
    pub fn readSingle(
        self: *Self,
        topic: [:0]const u8,
        timeout_ms: u64,
        out_value: []u8,
    ) bool {
        var entries: [1]Entry = [_]Entry{.{}};
        const n = self.readPattern(topic, 1, timeout_ms, null, false, &entries);
        if (n > 0) {
            const got_topic = std.mem.sliceTo(&entries[0].topic, 0);
            if (std.mem.eql(u8, got_topic, topic)) {
                const val = std.mem.sliceTo(&entries[0].value, 0);
                const clen = @min(val.len, out_value.len - 1);
                @memcpy(out_value[0..clen], val[0..clen]);
                out_value[clen] = 0;
                return true;
            }
        }
        return false;
    }

    /// Read all retained messages under a base topic (default "#").
    pub fn readAll(
        self: *Self,
        base_topic: ?[:0]const u8,
        timeout_ms: u64,
        sentinel_topics: ?[]const [:0]const u8,
        wait_for_sentinel: bool,
        out_entries: []Entry,
    ) usize {
        const pattern = base_topic orelse "#";
        return self.readPattern(pattern, 1, timeout_ms, sentinel_topics, wait_for_sentinel, out_entries);
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

    fn onMessageCb(_: ?*c.struct_mosquitto, userdata: ?*anyopaque, msg: ?*const c.struct_mosquitto_message) callconv(.C) void {
        const self: *Self = @ptrCast(@alignCast(userdata));
        const m = msg orelse return;

        // Extract topic
        const topic_cstr: [*:0]const u8 = @ptrCast(m.topic orelse return);
        const topic = std.mem.sliceTo(topic_cstr, 0);

        // Extract payload
        var payload_buf: [MAX_VALUE_LEN]u8 = undefined;
        var payload_len: usize = 0;
        if (m.payload != null and m.payloadlen > 0) {
            const pptr: [*]const u8 = @ptrCast(m.payload);
            payload_len = @min(@as(usize, @intCast(m.payloadlen)), MAX_VALUE_LEN - 1);
            @memcpy(payload_buf[0..payload_len], pptr[0..payload_len]);
        }
        payload_buf[payload_len] = 0;
        const payload = payload_buf[0..payload_len];

        // Check sentinel
        self.sentinel_mutex.lock();
        if (self.isSentinel(topic)) {
            self.sentinel_fired = true;
            self.sentinel_cond.signal();
            self.sentinel_mutex.unlock();
            return;
        }
        self.sentinel_mutex.unlock();

        // Normal KV handling
        self.entries_mutex.lock();
        defer self.entries_mutex.unlock();
        if (m.retain) {
            self.storeEntry(topic, payload);
            log.info("[Retained] {s}", .{topic});
        } else {
            log.info("[Non-retained] {s}", .{topic});
        }
    }

    fn onSubscribeCb(_: ?*c.struct_mosquitto, userdata: ?*anyopaque, mid: c_int, _: c_int, _: ?*const c_int) callconv(.C) void {
        const self: *Self = @ptrCast(@alignCast(userdata));
        log.info("[Subscribed] mid={d}", .{mid});
        self.sub_mutex.lock();
        defer self.sub_mutex.unlock();
        self.sub_acked = true;
        self.sub_cond.signal();
    }
};

// ──────────────────────────────────────────────────────────────────────
//  Tests
// ──────────────────────────────────────────────────────────────────────

test "Reader Config defaults" {
    const cfg = Config{};
    try std.testing.expectEqualStrings("localhost", cfg.host);
    try std.testing.expectEqual(@as(u16, 1883), cfg.port);
    try std.testing.expectEqualStrings("kv-reader", cfg.client_id);
    try std.testing.expect(cfg.clean_session);
}

test "Entry sliceTo" {
    var entry = Entry{};
    const src = "test/topic";
    @memcpy(entry.topic[0..src.len], src);
    entry.topic[src.len] = 0;
    entry.active = true;
    try std.testing.expectEqualStrings("test/topic", entry.topicSlice());
}