//! mqtt_queue.zig - MQTT Queue Publisher & Reader Library
//!
//! Reliable queued messaging over MQTT v3.1.1 using persistent sessions.
//! Wraps the Mosquitto C client library (libmosquitto).
//!
//! Features:
//!   - QueuePublisher: publish JSON messages (single + batch) with QoS 1/2
//!   - QueueReader:    persistent-session consumer with offline message queuing
//!
//! Thread safety: each instance is internally synchronised via std.Thread.Mutex.

const std = @import("std");
const log = std.log.scoped(.mqtt_queue);

const c = @cImport({
    @cInclude("mosquitto.h");
});

// ──────────────────────────────────────────────────────────────────────
//  Public types
// ──────────────────────────────────────────────────────────────────────

pub const Config = struct {
    host: [:0]const u8 = "localhost",
    port: u16 = 1883,
    client_id: ?[:0]const u8 = null,
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

/// A received MQTT message.
pub const Message = struct {
    topic: []u8,
    payload: []u8,
    next: ?*Message = null,

    /// Free a single message node (and its owned slices).
    pub fn deinit(self: *Message, allocator: std.mem.Allocator) void {
        allocator.free(self.topic);
        allocator.free(self.payload);
        allocator.destroy(self);
    }
};

/// Free an entire linked list of messages.
pub fn messageFreeList(head: ?*Message, allocator: std.mem.Allocator) void {
    var cur = head;
    while (cur) |node| {
        const next = node.next;
        node.deinit(allocator);
        cur = next;
    }
}

pub const Error = error{
    MosquittoNew,
    ConnectFailed,
    LoopStartFailed,
    ConnectTimeout,
    NotConnected,
    SubscribeFailed,
    SubscribeTimeout,
    PublishFailed,
    OutOfMemory,
};

// ──────────────────────────────────────────────────────────────────────
//  Library-level init / cleanup
// ──────────────────────────────────────────────────────────────────────

/// Call before any other mqtt_queue function (once per process).
pub fn libInit() void {
    _ = c.mosquitto_lib_init();
}

/// Call at program exit.
pub fn libCleanup() void {
    _ = c.mosquitto_lib_cleanup();
}

// ──────────────────────────────────────────────────────────────────────
//  QueuePublisher
// ──────────────────────────────────────────────────────────────────────

pub const Publisher = struct {
    mosq: *c.struct_mosquitto = undefined,
    cfg: Config,
    connected: bool = false,
    mutex: std.Thread.Mutex = .{},
    connect_cond: std.Thread.Condition = .{},

    const Self = @This();

    /// Initialise a publisher. Returns error on failure.
    pub fn init(cfg: Config) Error!Self {
        var self = Self{
            .cfg = cfg,
        };

        const cid: ?[*:0]const u8 = if (cfg.client_id) |id| id.ptr else null;
        const mosq = c.mosquitto_new(cid, cfg.clean_session, null);
        if (mosq == null) {
            log.err("[publisher] mosquitto_new failed", .{});
            return Error.MosquittoNew;
        }
        self.mosq = mosq.?;

        if (cfg.username) |user| {
            const pw: ?[*:0]const u8 = if (cfg.password) |p| p.ptr else null;
            _ = c.mosquitto_username_pw_set(self.mosq, user.ptr, pw);
        }

        c.mosquitto_connect_callback_set(self.mosq, pubOnConnect);
        c.mosquitto_disconnect_callback_set(self.mosq, pubOnDisconnect);

        return self;
    }

    /// Connect to broker. Blocks up to timeout_ms for CONNACK.
    pub fn connect(self: *Self, timeout_ms: u64) Error!void {
        // Set userdata to the caller's stable pointer (not the temporary from init)
        c.mosquitto_user_data_set(self.mosq, @ptrCast(self));

        const rc_conn = c.mosquitto_connect(
            self.mosq,
            self.cfg.host.ptr,
            @intCast(self.cfg.port),
            @intCast(self.cfg.keepalive),
        );
        if (rc_conn != c.MOSQ_ERR_SUCCESS) {
            log.err("[publisher] mosquitto_connect: {s}", .{c.mosquitto_strerror(rc_conn)});
            return Error.ConnectFailed;
        }

        const rc_loop = c.mosquitto_loop_start(self.mosq);
        if (rc_loop != c.MOSQ_ERR_SUCCESS) {
            log.err("[publisher] mosquitto_loop_start: {s}", .{c.mosquitto_strerror(rc_loop)});
            return Error.LoopStartFailed;
        }

        // Wait for CONNACK
        self.mutex.lock();
        defer self.mutex.unlock();

        if (!self.connected) {
            self.connect_cond.timedWait(&self.mutex, timeout_ms * std.time.ns_per_ms) catch {};
        }

        if (!self.connected) {
            log.err("[publisher] connect timeout", .{});
            _ = c.mosquitto_loop_stop(self.mosq, true);
            return Error.ConnectTimeout;
        }
    }

    /// Disconnect from broker.
    pub fn disconnect(self: *Self) void {
        _ = c.mosquitto_disconnect(self.mosq);
        _ = c.mosquitto_loop_stop(self.mosq, false);
        self.mutex.lock();
        defer self.mutex.unlock();
        self.connected = false;
    }

    /// Destroy publisher and free mosquitto resources.
    pub fn deinit(self: *Self) void {
        c.mosquitto_destroy(self.mosq);
    }

    /// Publish a single payload to topic. Returns error on failure.
    pub fn publish(
        self: *Self,
        topic: [:0]const u8,
        payload: []const u8,
        qos: Qos,
        retain: bool,
    ) Error!void {
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
            log.err("[publisher] publish failed: {s}", .{c.mosquitto_strerror(rc)});
            return Error.PublishFailed;
        }
    }

    /// Publish a batch of payloads. Returns the number successfully published.
    pub fn publishBatch(
        self: *Self,
        topic: [:0]const u8,
        payloads: []const []const u8,
        qos: Qos,
        retain: bool,
        delay_between_ms: u64,
    ) usize {
        var ok: usize = 0;
        for (payloads, 0..) |payload, i| {
            if (self.publish(topic, payload, qos, retain)) |_| {
                ok += 1;
                log.info("  [batch] published {d}/{d}", .{ i + 1, payloads.len });
            } else |_| {
                log.err("  [batch] failed {d}/{d}", .{ i + 1, payloads.len });
            }
            if (delay_between_ms > 0 and i < payloads.len - 1) {
                std.time.sleep(delay_between_ms * std.time.ns_per_ms);
            }
        }
        return ok;
    }

    pub fn isConnected(self: *const Self) bool {
        return self.connected;
    }

    // ── Mosquitto C callbacks (called from the network thread) ───────

    fn pubOnConnect(mosq: ?*c.struct_mosquitto, userdata: ?*anyopaque, rc: c_int) callconv(.C) void {
        _ = mosq;
        const self: *Self = @ptrCast(@alignCast(userdata));
        self.mutex.lock();
        defer self.mutex.unlock();
        self.connected = (rc == 0);
        log.info("[publisher] on_connect rc={d}", .{rc});
        self.connect_cond.signal();
    }

    fn pubOnDisconnect(mosq: ?*c.struct_mosquitto, userdata: ?*anyopaque, rc: c_int) callconv(.C) void {
        _ = mosq;
        const self: *Self = @ptrCast(@alignCast(userdata));
        self.mutex.lock();
        defer self.mutex.unlock();
        self.connected = false;
        log.info("[publisher] on_disconnect rc={d}", .{rc});
        self.connect_cond.signal();
    }
};

// ──────────────────────────────────────────────────────────────────────
//  QueueReader
// ──────────────────────────────────────────────────────────────────────

pub const Reader = struct {
    mosq: *c.struct_mosquitto = undefined,
    cfg: Config,
    allocator: std.mem.Allocator,
    connected: bool = false,
    session_present: bool = false,

    // Incoming message buffer (linked list, guarded by mutex)
    msg_head: ?*Message = null,
    msg_tail: ?*Message = null,
    msg_count: usize = 0,

    // Synchronisation
    mutex: std.Thread.Mutex = .{},
    connect_cond: std.Thread.Condition = .{},
    subscribe_cond: std.Thread.Condition = .{},
    subscribe_ack: bool = false,
    subscribe_mid: c_int = 0,

    const Self = @This();

    /// Initialise a reader. The allocator is used for incoming message buffers.
    pub fn init(cfg: Config, allocator: std.mem.Allocator) Error!Self {
        var self = Self{
            .cfg = cfg,
            .allocator = allocator,
        };

        const cid: ?[*:0]const u8 = if (cfg.client_id) |id| id.ptr else null;
        const mosq = c.mosquitto_new(cid, cfg.clean_session, null);
        if (mosq == null) {
            log.err("[reader] mosquitto_new failed", .{});
            return Error.MosquittoNew;
        }
        self.mosq = mosq.?;

        if (cfg.username) |user| {
            const pw: ?[*:0]const u8 = if (cfg.password) |p| p.ptr else null;
            _ = c.mosquitto_username_pw_set(self.mosq, user.ptr, pw);
        }

        c.mosquitto_connect_callback_set(self.mosq, rdrOnConnect);
        c.mosquitto_disconnect_callback_set(self.mosq, rdrOnDisconnect);
        c.mosquitto_message_callback_set(self.mosq, rdrOnMessage);
        c.mosquitto_subscribe_callback_set(self.mosq, rdrOnSubscribe);

        return self;
    }

    /// Connect to broker. Blocks up to timeout_ms for CONNACK.
    pub fn connect(self: *Self, timeout_ms: u64) Error!void {
        // Set userdata to the caller's stable pointer (not the temporary from init)
        c.mosquitto_user_data_set(self.mosq, @ptrCast(self));

        const rc_conn = c.mosquitto_connect(
            self.mosq,
            self.cfg.host.ptr,
            @intCast(self.cfg.port),
            @intCast(self.cfg.keepalive),
        );
        if (rc_conn != c.MOSQ_ERR_SUCCESS) {
            log.err("[reader] mosquitto_connect: {s}", .{c.mosquitto_strerror(rc_conn)});
            return Error.ConnectFailed;
        }

        const rc_loop = c.mosquitto_loop_start(self.mosq);
        if (rc_loop != c.MOSQ_ERR_SUCCESS) {
            log.err("[reader] mosquitto_loop_start: {s}", .{c.mosquitto_strerror(rc_loop)});
            return Error.LoopStartFailed;
        }

        self.mutex.lock();
        defer self.mutex.unlock();

        if (!self.connected) {
            self.connect_cond.timedWait(&self.mutex, timeout_ms * std.time.ns_per_ms) catch {};
        }

        if (!self.connected) {
            log.err("[reader] connect timeout", .{});
            _ = c.mosquitto_loop_stop(self.mosq, true);
            return Error.ConnectTimeout;
        }
    }

    /// Disconnect from broker.
    pub fn disconnect(self: *Self) void {
        _ = c.mosquitto_disconnect(self.mosq);
        _ = c.mosquitto_loop_stop(self.mosq, false);
        self.mutex.lock();
        defer self.mutex.unlock();
        self.connected = false;
    }

    /// Destroy reader and free all resources including buffered messages.
    pub fn deinit(self: *Self) void {
        self.clear();
        c.mosquitto_destroy(self.mosq);
    }

    /// Subscribe to a topic and block until SUBACK or timeout.
    pub fn subscribe(self: *Self, topic: [:0]const u8, qos: Qos, timeout_ms: u64) Error!void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (!self.connected) {
            log.err("[reader] subscribe: not connected", .{});
            return Error.NotConnected;
        }

        self.subscribe_ack = false;
        var mid: c_int = 0;
        const rc = c.mosquitto_subscribe(self.mosq, &mid, topic.ptr, @intFromEnum(qos));
        if (rc != c.MOSQ_ERR_SUCCESS) {
            log.err("[reader] subscribe failed: {s}", .{c.mosquitto_strerror(rc)});
            return Error.SubscribeFailed;
        }
        self.subscribe_mid = mid;

        // Wait for SUBACK
        while (!self.subscribe_ack) {
            self.subscribe_cond.timedWait(&self.mutex, timeout_ms * std.time.ns_per_ms) catch {
                log.err("[reader] subscribe timeout waiting for SUBACK", .{});
                return Error.SubscribeTimeout;
            };
        }
    }

    /// Collect messages for up to timeout_ms milliseconds.
    /// If the session was already present, skips re-subscribing.
    /// Returns a linked list of messages (caller must free with messageFreeList).
    pub fn readQueue(
        self: *Self,
        topic: [:0]const u8,
        qos: Qos,
        timeout_ms: u64,
        out_count: ?*usize,
    ) Error!?*Message {
        // Clear any old messages
        self.clear();

        // Subscribe if this isn't a resumed persistent session
        if (!self.session_present) {
            try self.subscribe(topic, qos, 2000);
        } else {
            log.info("[reader] using existing subscription from persistent session", .{});
        }

        // Collect messages for the specified timeout
        std.time.sleep(timeout_ms * std.time.ns_per_ms);

        // Detach the message list
        self.mutex.lock();
        defer self.mutex.unlock();

        const head = self.msg_head;
        const count = self.msg_count;
        self.msg_head = null;
        self.msg_tail = null;
        self.msg_count = 0;

        if (out_count) |cnt| {
            cnt.* = count;
        }
        return head;
    }

    /// Clear any buffered messages.
    pub fn clear(self: *Self) void {
        self.mutex.lock();
        const head = self.msg_head;
        self.msg_head = null;
        self.msg_tail = null;
        self.msg_count = 0;
        self.mutex.unlock();
        messageFreeList(head, self.allocator);
    }

    pub fn isConnected(self: *const Self) bool {
        return self.connected;
    }

    // ── Mosquitto C callbacks ────────────────────────────────────────

    fn rdrOnConnect(mosq: ?*c.struct_mosquitto, userdata: ?*anyopaque, rc: c_int) callconv(.C) void {
        _ = mosq;
        const self: *Self = @ptrCast(@alignCast(userdata));
        self.mutex.lock();
        defer self.mutex.unlock();
        self.connected = (rc == 0);
        log.info("[reader] on_connect rc={d}", .{rc});
        self.connect_cond.signal();
    }

    fn rdrOnDisconnect(mosq: ?*c.struct_mosquitto, userdata: ?*anyopaque, rc: c_int) callconv(.C) void {
        _ = mosq;
        const self: *Self = @ptrCast(@alignCast(userdata));
        self.mutex.lock();
        defer self.mutex.unlock();
        self.connected = false;
        log.info("[reader] on_disconnect rc={d}", .{rc});
        self.connect_cond.signal();
    }

    fn rdrOnMessage(mosq: ?*c.struct_mosquitto, userdata: ?*anyopaque, msg: ?*const c.struct_mosquitto_message) callconv(.C) void {
        _ = mosq;
        const self: *Self = @ptrCast(@alignCast(userdata));
        const m = msg orelse return;

        // Allocate a new Message node
        const node = self.allocator.create(Message) catch return;
        const topic_ptr: [*]const u8 = @ptrCast(m.topic orelse return);
        const topic_len = std.mem.len(@as([*:0]const u8, @ptrCast(m.topic orelse return)));
        node.topic = self.allocator.alloc(u8, topic_len) catch {
            self.allocator.destroy(node);
            return;
        };
        @memcpy(node.topic, topic_ptr[0..topic_len]);

        const payload_ptr: [*]const u8 = @ptrCast(m.payload orelse {
            self.allocator.free(node.topic);
            self.allocator.destroy(node);
            return;
        });
        const payload_len: usize = @intCast(m.payloadlen);
        node.payload = self.allocator.alloc(u8, payload_len) catch {
            self.allocator.free(node.topic);
            self.allocator.destroy(node);
            return;
        };
        @memcpy(node.payload, payload_ptr[0..payload_len]);
        node.next = null;

        // Append to linked list
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.msg_tail) |tail| {
            tail.next = node;
        } else {
            self.msg_head = node;
        }
        self.msg_tail = node;
        self.msg_count += 1;
    }

    fn rdrOnSubscribe(
        mosq: ?*c.struct_mosquitto,
        userdata: ?*anyopaque,
        mid: c_int,
        _: c_int,
        _: ?*const c_int,
    ) callconv(.C) void {
        _ = mosq;
        const self: *Self = @ptrCast(@alignCast(userdata));
        log.info("[reader] on_subscribe mid={d}", .{mid});
        self.mutex.lock();
        defer self.mutex.unlock();
        if (mid == self.subscribe_mid) {
            self.subscribe_ack = true;
            self.subscribe_cond.signal();
        }
    }
};

// ──────────────────────────────────────────────────────────────────────
//  Tests
// ──────────────────────────────────────────────────────────────────────

test "Config defaults" {
    const cfg = Config{};
    try std.testing.expectEqualStrings("localhost", cfg.host);
    try std.testing.expectEqual(@as(u16, 1883), cfg.port);
    try std.testing.expect(cfg.clean_session);
    try std.testing.expectEqual(@as(?[:0]const u8, null), cfg.client_id);
}

test "messageFreeList handles null" {
    messageFreeList(null, std.testing.allocator);
}

test "Message allocate and free" {
    const allocator = std.testing.allocator;

    const node = try allocator.create(Message);
    node.topic = try allocator.dupe(u8, "test/topic");
    node.payload = try allocator.dupe(u8, "{\"hello\":1}");
    node.next = null;

    messageFreeList(node, allocator);
}