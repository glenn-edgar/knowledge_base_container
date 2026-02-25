//! mqtt_pubsub.zig - JSON-pubsub 2.0 over MQTT
//!
//! Server listens on  pubsub/{service}/request/+
//! Client publishes to pubsub/{service}/request/{client_id}
//! Server replies to   pubsub/{service}/response/{client_id}
//! Client subscribes   pubsub/{service}/response/{client_id}
//!
//! Replaces cJSON with std.json — no C JSON dependency needed.
//! Requires: libmosquitto (apt install libmosquitto-dev)

const std = @import("std");
const log = std.log.scoped(.mqtt_pubsub);

const c = @cImport({
    @cInclude("mosquitto.h");
});

// ──────────────────────────────────────────────────────────────────────
//  JSON-pubsub error codes
// ──────────────────────────────────────────────────────────────────────

pub const JSONPUBSUB_PARSE_ERROR = -32700;
pub const JSONPUBSUB_INVALID_REQUEST = -32600;
pub const JSONPUBSUB_METHOD_NOT_FOUND = -32601;
pub const JSONPUBSUB_INVALID_PARAMS = -32602;
pub const JSONPUBSUB_INTERNAL_ERROR = -32603;

// ──────────────────────────────────────────────────────────────────────
//  Configuration
// ──────────────────────────────────────────────────────────────────────

pub const Config = struct {
    host: [:0]const u8 = "localhost",
    port: u16 = 1883,
    client_id: ?[:0]const u8 = null,
    service_name: [:0]const u8 = "pubsub_service",
    keepalive: u16 = 60,
    username: ?[:0]const u8 = null,
    password: ?[:0]const u8 = null,
    qos: c_int = 1,
};

pub const Error = error{
    MosquittoNew,
    ConnectFailed,
    LoopStartFailed,
    ConnectTimeout,
    NotConnected,
    SubscribeFailed,
    PublishFailed,
    OutOfMemory,
    Timeout,
    PubsubError,
};

// ──────────────────────────────────────────────────────────────────────
//  Method handler type
// ──────────────────────────────────────────────────────────────────────

/// Server-side method handler.
/// Return a JSON string allocated with the provided allocator, or null for error.
pub const MethodFn = *const fn (
    allocator: std.mem.Allocator,
    params_json: ?[]const u8,
    userdata: ?*anyopaque,
) ?[]const u8;

// ──────────────────────────────────────────────────────────────────────
//  Async callback type
// ──────────────────────────────────────────────────────────────────────

/// Async call callback.  Exactly one of error_json / result_json is non-null.
/// The callee must NOT free the slices — they are owned by the async machinery.
pub const AsyncCallback = *const fn (
    error_json: ?[]const u8,
    result_json: ?[]const u8,
    userdata: ?*anyopaque,
) void;

// ──────────────────────────────────────────────────────────────────────
//  Helpers
// ──────────────────────────────────────────────────────────────────────

fn autoClientId(allocator: std.mem.Allocator, prefix: []const u8) ![:0]const u8 {
    var rng = std.rand.DefaultPrng.init(@as(u64, @truncate(@as(u128, @bitCast(std.time.nanoTimestamp())))));
    const suffix: u32 = @truncate(rng.next());
    return try std.fmt.allocPrintZ(allocator, "{s}_{x:0>8}", .{ prefix, suffix });
}

fn extractCallerId(topic: []const u8) ?[]const u8 {
    var slash_count: usize = 0;
    for (topic, 0..) |ch, i| {
        if (ch == '/') {
            slash_count += 1;
            if (slash_count == 3) {
                if (i + 1 < topic.len) return topic[i + 1 ..];
                return null;
            }
        }
    }
    return null;
}

fn jsonGetString(value: std.json.Value, key: []const u8) ?[]const u8 {
    if (value != .object) return null;
    const v = value.object.get(key) orelse return null;
    return switch (v) {
        .string => |s| s,
        else => null,
    };
}

fn jsonSerializeField(allocator: std.mem.Allocator, value: std.json.Value, key: []const u8) ?[]const u8 {
    if (value != .object) return null;
    const field = value.object.get(key) orelse return null;
    var buf = std.ArrayList(u8).init(allocator);
    std.json.stringify(field, .{}, buf.writer()) catch return null;
    return buf.toOwnedSlice() catch null;
}

// ──────────────────────────────────────────────────────────────────────
//  Pubsub Server
// ──────────────────────────────────────────────────────────────────────

const MethodEntry = struct {
    name: []const u8,
    handler: MethodFn,
    userdata: ?*anyopaque,
    next: ?*MethodEntry,
};

pub const Server = struct {
    mosq: *c.struct_mosquitto = undefined,
    cfg: Config,
    allocator: std.mem.Allocator,

    client_id: [:0]const u8 = "",
    request_topic: [:0]const u8 = "",
    response_topic_base: [:0]const u8 = "",

    methods: ?*MethodEntry = null,

    connected: bool = false,
    subscribed: bool = false,
    stop_flag: bool = false,

    mutex: std.Thread.Mutex = .{},
    connect_cond: std.Thread.Condition = .{},
    subscribe_cond: std.Thread.Condition = .{},

    const Self = @This();

    pub fn init(cfg: Config, allocator: std.mem.Allocator) Error!Self {
        var self = Self{
            .cfg = cfg,
            .allocator = allocator,
        };

        self.client_id = if (cfg.client_id) |id|
            allocator.dupeZ(u8, id) catch return Error.OutOfMemory
        else
            autoClientId(allocator, "pubsub_server") catch return Error.OutOfMemory;

        self.request_topic = std.fmt.allocPrintZ(allocator, "pubsub/{s}/request/+", .{cfg.service_name}) catch return Error.OutOfMemory;
        self.response_topic_base = std.fmt.allocPrintZ(allocator, "pubsub/{s}/response", .{cfg.service_name}) catch return Error.OutOfMemory;

        const mosq = c.mosquitto_new(self.client_id.ptr, true, null);
        if (mosq == null) {
            log.err("[pubsub_server] mosquitto_new failed", .{});
            return Error.MosquittoNew;
        }
        self.mosq = mosq.?;

        if (cfg.username) |user| {
            const pw: ?[*:0]const u8 = if (cfg.password) |p| p.ptr else null;
            _ = c.mosquitto_username_pw_set(self.mosq, user.ptr, pw);
        }

        c.mosquitto_connect_callback_set(self.mosq, srvOnConnect);
        c.mosquitto_disconnect_callback_set(self.mosq, srvOnDisconnect);
        c.mosquitto_subscribe_callback_set(self.mosq, srvOnSubscribe);
        c.mosquitto_message_callback_set(self.mosq, srvOnMessage);

        return self;
    }

    pub fn register(self: *Self, name: [:0]const u8, handler: MethodFn, userdata: ?*anyopaque) void {
        const entry = self.allocator.create(MethodEntry) catch return;
        entry.* = .{
            .name = self.allocator.dupe(u8, name) catch return,
            .handler = handler,
            .userdata = userdata,
            .next = null,
        };

        self.mutex.lock();
        entry.next = self.methods;
        self.methods = entry;
        self.mutex.unlock();

        log.info("[pubsub_server] registered method: {s}", .{name});
    }

    pub fn start(self: *Self, wait_for_suback: bool, sub_timeout_ms: u64) Error!void {
        // Set userdata now — self pointer is stable (caller holds *Self)
        c.mosquitto_user_data_set(self.mosq, @ptrCast(self));

        const rc_conn = c.mosquitto_connect(
            self.mosq,
            self.cfg.host.ptr,
            @intCast(self.cfg.port),
            @intCast(self.cfg.keepalive),
        );
        if (rc_conn != c.MOSQ_ERR_SUCCESS) {
            log.err("[pubsub_server] connect: {s}", .{c.mosquitto_strerror(rc_conn)});
            return Error.ConnectFailed;
        }

        const rc_loop = c.mosquitto_loop_start(self.mosq);
        if (rc_loop != c.MOSQ_ERR_SUCCESS) {
            log.err("[pubsub_server] loop_start: {s}", .{c.mosquitto_strerror(rc_loop)});
            return Error.LoopStartFailed;
        }

        {
            self.mutex.lock();
            defer self.mutex.unlock();
            if (!self.connected) {
                self.connect_cond.timedWait(&self.mutex, 5000 * std.time.ns_per_ms) catch {};
            }
            if (!self.connected) {
                log.err("[pubsub_server] connect timeout", .{});
                return Error.ConnectTimeout;
            }
        }

        if (wait_for_suback) {
            self.mutex.lock();
            defer self.mutex.unlock();
            if (!self.subscribed) {
                self.subscribe_cond.timedWait(&self.mutex, sub_timeout_ms * std.time.ns_per_ms) catch {};
            }
            if (!self.subscribed) {
                log.warn("[pubsub_server] warning: SUBACK not received yet", .{});
            }
        }

        log.info("[pubsub_server] started (service={s})", .{self.cfg.service_name});
    }

    pub fn wait(self: *Self) void {
        while (!self.stop_flag) {
            std.time.sleep(250 * std.time.ns_per_ms);
        }
    }

    pub fn stop(self: *Self) void {
        log.info("[pubsub_server] stopping...", .{});
        self.stop_flag = true;
        _ = c.mosquitto_disconnect(self.mosq);
        _ = c.mosquitto_loop_stop(self.mosq, false);
        self.mutex.lock();
        defer self.mutex.unlock();
        self.connected = false;
    }

    pub fn deinit(self: *Self) void {
        var entry = self.methods;
        while (entry) |e| {
            const next = e.next;
            self.allocator.free(e.name);
            self.allocator.destroy(e);
            entry = next;
        }
        self.methods = null;

        self.allocator.free(self.client_id);
        self.allocator.free(self.request_topic);
        self.allocator.free(self.response_topic_base);

        c.mosquitto_destroy(self.mosq);
    }

    // ── Server request processing ────────────────────────────────────

    fn processRequest(self: *Self, topic: []const u8, payload: []const u8) void {
        const caller_id = extractCallerId(topic) orelse return;

        var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer arena.deinit();
        const arena_alloc = arena.allocator();

        const parsed = std.json.parseFromSlice(std.json.Value, arena_alloc, payload, .{}) catch {
            const err_resp = std.fmt.allocPrintZ(self.allocator, "{{\"jsonpubsub\":\"2.0\",\"id\":null,\"error\":{{\"code\":{d},\"message\":\"Parse error\"}}}}", .{JSONPUBSUB_PARSE_ERROR}) catch return;
            defer self.allocator.free(err_resp);
            self.publishResponse(caller_id, err_resp);
            return;
        };
        const root = parsed.value;

        const id_json = jsonSerializeField(arena_alloc, root, "id") orelse "null";

        const version = jsonGetString(root, "jsonpubsub");
        const method_name = jsonGetString(root, "method");

        if (version == null or !std.mem.eql(u8, version.?, "2.0") or method_name == null) {
            const err_resp = std.fmt.allocPrint(self.allocator, "{{\"jsonpubsub\":\"2.0\",\"id\":{s},\"error\":{{\"code\":{d},\"message\":\"Invalid Request\"}}}}", .{ id_json, JSONPUBSUB_INVALID_REQUEST }) catch return;
            defer self.allocator.free(err_resp);
            self.publishResponse(caller_id, err_resp);
            return;
        }

        const params_json = jsonSerializeField(arena_alloc, root, "params");

        var handler: ?MethodFn = null;
        var userdata: ?*anyopaque = null;
        {
            self.mutex.lock();
            defer self.mutex.unlock();
            var me = self.methods;
            while (me) |e| {
                if (std.mem.eql(u8, e.name, method_name.?)) {
                    handler = e.handler;
                    userdata = e.userdata;
                    break;
                }
                me = e.next;
            }
        }

        if (handler == null) {
            const err_resp = std.fmt.allocPrint(self.allocator, "{{\"jsonpubsub\":\"2.0\",\"id\":{s},\"error\":{{\"code\":{d},\"message\":\"Method '{s}' not found\"}}}}", .{ id_json, JSONPUBSUB_METHOD_NOT_FOUND, method_name.? }) catch return;
            defer self.allocator.free(err_resp);
            self.publishResponse(caller_id, err_resp);
            return;
        }

        const result_json = handler.?(self.allocator, params_json, userdata);
        defer if (result_json) |r| self.allocator.free(r);

        if (result_json) |result| {
            const resp = std.fmt.allocPrint(self.allocator, "{{\"jsonpubsub\":\"2.0\",\"id\":{s},\"result\":{s}}}", .{ id_json, result }) catch return;
            defer self.allocator.free(resp);
            self.publishResponse(caller_id, resp);
        } else {
            const err_resp = std.fmt.allocPrint(self.allocator, "{{\"jsonpubsub\":\"2.0\",\"id\":{s},\"error\":{{\"code\":{d},\"message\":\"Method returned null\"}}}}", .{ id_json, JSONPUBSUB_INTERNAL_ERROR }) catch return;
            defer self.allocator.free(err_resp);
            self.publishResponse(caller_id, err_resp);
        }
    }

    fn publishResponse(self: *Self, caller_id: []const u8, payload: []const u8) void {
        const resp_topic = std.fmt.allocPrintZ(self.allocator, "{s}/{s}", .{ self.response_topic_base, caller_id }) catch return;
        defer self.allocator.free(resp_topic);
        _ = c.mosquitto_publish(
            self.mosq,
            null,
            resp_topic.ptr,
            @intCast(payload.len),
            payload.ptr,
            self.cfg.qos,
            false,
        );
    }

    // ── Mosquitto C callbacks ────────────────────────────────────────

    fn srvOnConnect(_: ?*c.struct_mosquitto, userdata: ?*anyopaque, rc: c_int) callconv(.C) void {
        const self: *Self = @ptrCast(@alignCast(userdata));
        self.mutex.lock();
        self.connected = (rc == 0);
        log.info("[pubsub_server] on_connect rc={d}", .{rc});

        if (self.connected) {
            _ = c.mosquitto_subscribe(self.mosq, null, self.request_topic.ptr, self.cfg.qos);
        }
        self.connect_cond.signal();
        self.mutex.unlock();
    }

    fn srvOnDisconnect(_: ?*c.struct_mosquitto, userdata: ?*anyopaque, _: c_int) callconv(.C) void {
        const self: *Self = @ptrCast(@alignCast(userdata));
        self.mutex.lock();
        defer self.mutex.unlock();
        self.connected = false;
        self.connect_cond.signal();
    }

    fn srvOnSubscribe(_: ?*c.struct_mosquitto, userdata: ?*anyopaque, mid: c_int, _: c_int, _: ?*const c_int) callconv(.C) void {
        const self: *Self = @ptrCast(@alignCast(userdata));
        log.info("[pubsub_server] on_subscribe mid={d}", .{mid});
        self.mutex.lock();
        defer self.mutex.unlock();
        self.subscribed = true;
        self.subscribe_cond.signal();
    }

    fn srvOnMessage(_: ?*c.struct_mosquitto, userdata: ?*anyopaque, msg: ?*const c.struct_mosquitto_message) callconv(.C) void {
        const self: *Self = @ptrCast(@alignCast(userdata));
        const m = msg orelse return;

        const topic_cstr: [*:0]const u8 = @ptrCast(m.topic orelse return);
        const topic = std.mem.sliceTo(topic_cstr, 0);

        if (m.payload == null or m.payloadlen <= 0) return;
        const payload_ptr: [*]const u8 = @ptrCast(m.payload);
        const payload = payload_ptr[0..@intCast(m.payloadlen)];

        // MUST copy — mosquitto frees its buffers when this callback returns
        const topic_copy = self.allocator.dupe(u8, topic) catch return;
        const payload_copy = self.allocator.dupe(u8, payload) catch {
            self.allocator.free(topic_copy);
            return;
        };

        const thread = std.Thread.spawn(.{}, processThread, .{ self, topic_copy, payload_copy }) catch {
            log.err("[pubsub_server] failed to spawn request thread", .{});
            self.allocator.free(topic_copy);
            self.allocator.free(payload_copy);
            return;
        };
        thread.detach();
    }

    fn processThread(self: *Self, topic: []const u8, payload: []const u8) void {
        defer self.allocator.free(topic);
        defer self.allocator.free(payload);
        self.processRequest(topic, payload);
    }
};

// ──────────────────────────────────────────────────────────────────────
//  Pubsub Client
// ──────────────────────────────────────────────────────────────────────

const PendingRequest = struct {
    id: []const u8,
    response: ?[]const u8 = null,
    done: bool = false,
    cond: std.Thread.Condition = .{},
    next: ?*PendingRequest = null,
};

/// Result of a pubsub call.
pub const CallResult = struct {
    /// JSON string of the "result" field (caller must free with allocator).
    result: ?[]const u8 = null,
    /// JSON string of the "error" field (caller must free with allocator).
    err: ?[]const u8 = null,

    pub fn deinit(self: *CallResult, allocator: std.mem.Allocator) void {
        if (self.result) |r| allocator.free(r);
        if (self.err) |e| allocator.free(e);
    }
};

pub const Client = struct {
    mosq: *c.struct_mosquitto = undefined,
    cfg: Config,
    allocator: std.mem.Allocator,

    client_id: [:0]const u8 = "",
    request_topic: [:0]const u8 = "",
    response_topic: [:0]const u8 = "",

    pending_head: ?*PendingRequest = null,
    request_counter: u32 = 0,

    connected: bool = false,
    subscribed: bool = false,
    default_timeout_ms: u64 = 30000,

    mutex: std.Thread.Mutex = .{},
    connect_cond: std.Thread.Condition = .{},
    subscribe_cond: std.Thread.Condition = .{},

    const Self = @This();

    pub fn init(cfg: Config, allocator: std.mem.Allocator, default_timeout_ms: u64) Error!Self {
        var self = Self{
            .cfg = cfg,
            .allocator = allocator,
            .default_timeout_ms = if (default_timeout_ms > 0) default_timeout_ms else 30000,
        };

        self.client_id = if (cfg.client_id) |id|
            allocator.dupeZ(u8, id) catch return Error.OutOfMemory
        else
            autoClientId(allocator, "pubsub_client") catch return Error.OutOfMemory;

        self.request_topic = std.fmt.allocPrintZ(allocator, "pubsub/{s}/request/{s}", .{ cfg.service_name, self.client_id }) catch return Error.OutOfMemory;
        self.response_topic = std.fmt.allocPrintZ(allocator, "pubsub/{s}/response/{s}", .{ cfg.service_name, self.client_id }) catch return Error.OutOfMemory;

        const mosq = c.mosquitto_new(self.client_id.ptr, true, null);
        if (mosq == null) {
            log.err("[pubsub_client] mosquitto_new failed", .{});
            return Error.MosquittoNew;
        }
        self.mosq = mosq.?;

        if (cfg.username) |user| {
            const pw: ?[*:0]const u8 = if (cfg.password) |p| p.ptr else null;
            _ = c.mosquitto_username_pw_set(self.mosq, user.ptr, pw);
        }

        c.mosquitto_connect_callback_set(self.mosq, cliOnConnect);
        c.mosquitto_disconnect_callback_set(self.mosq, cliOnDisconnect);
        c.mosquitto_subscribe_callback_set(self.mosq, cliOnSubscribe);
        c.mosquitto_message_callback_set(self.mosq, cliOnMessage);

        return self;
    }

    pub fn connect(self: *Self, timeout_ms: u64) Error!void {
        // Set userdata now — self pointer is stable (caller holds *Self)
        c.mosquitto_user_data_set(self.mosq, @ptrCast(self));

        const rc_conn = c.mosquitto_connect(
            self.mosq,
            self.cfg.host.ptr,
            @intCast(self.cfg.port),
            @intCast(self.cfg.keepalive),
        );
        if (rc_conn != c.MOSQ_ERR_SUCCESS) {
            log.err("[pubsub_client] connect: {s}", .{c.mosquitto_strerror(rc_conn)});
            return Error.ConnectFailed;
        }

        const rc_loop = c.mosquitto_loop_start(self.mosq);
        if (rc_loop != c.MOSQ_ERR_SUCCESS) {
            log.err("[pubsub_client] loop_start: {s}", .{c.mosquitto_strerror(rc_loop)});
            return Error.LoopStartFailed;
        }

        {
            self.mutex.lock();
            defer self.mutex.unlock();
            if (!self.connected) {
                self.connect_cond.timedWait(&self.mutex, timeout_ms * std.time.ns_per_ms) catch {};
            }
            if (!self.connected) {
                log.err("[pubsub_client] connect timeout", .{});
                _ = c.mosquitto_loop_stop(self.mosq, true);
                return Error.ConnectTimeout;
            }
        }

        {
            self.mutex.lock();
            defer self.mutex.unlock();
            if (!self.subscribed) {
                self.subscribe_cond.timedWait(&self.mutex, timeout_ms * std.time.ns_per_ms) catch {};
            }
            if (!self.subscribed) {
                log.warn("[pubsub_client] warning: SUBACK not received yet", .{});
            }
        }
    }

    pub fn disconnect(self: *Self) void {
        log.info("[pubsub_client] disconnecting...", .{});
        _ = c.mosquitto_disconnect(self.mosq);
        _ = c.mosquitto_loop_stop(self.mosq, false);
        self.mutex.lock();
        defer self.mutex.unlock();
        self.connected = false;
    }

    pub fn deinit(self: *Self) void {
        var pending = self.pending_head;
        while (pending) |p| {
            const next = p.next;
            self.allocator.free(p.id);
            if (p.response) |r| self.allocator.free(r);
            self.allocator.destroy(p);
            pending = next;
        }
        self.pending_head = null;

        self.allocator.free(self.client_id);
        self.allocator.free(self.request_topic);
        self.allocator.free(self.response_topic);

        c.mosquitto_destroy(self.mosq);
    }

    /// Synchronous pubsub call.
    pub fn call(
        self: *Self,
        method: [:0]const u8,
        params_json: ?[]const u8,
        timeout_ms: u64,
    ) Error!CallResult {
        if (!self.connected) {
            log.err("[pubsub_client] call: not connected", .{});
            return Error.NotConnected;
        }

        const tout = if (timeout_ms > 0) timeout_ms else self.default_timeout_ms;

        var req_id: [:0]const u8 = undefined;
        {
            self.mutex.lock();
            self.request_counter += 1;
            req_id = std.fmt.allocPrintZ(self.allocator, "{s}_{d}", .{ self.client_id, self.request_counter }) catch return Error.OutOfMemory;
            self.mutex.unlock();
        }

        const payload = if (params_json) |params|
            std.fmt.allocPrint(self.allocator, "{{\"jsonpubsub\":\"2.0\",\"method\":\"{s}\",\"id\":\"{s}\",\"params\":{s}}}", .{ method, req_id, params }) catch return Error.OutOfMemory
        else
            std.fmt.allocPrint(self.allocator, "{{\"jsonpubsub\":\"2.0\",\"method\":\"{s}\",\"id\":\"{s}\"}}", .{ method, req_id }) catch return Error.OutOfMemory;
        defer self.allocator.free(payload);

        const pend = self.allocator.create(PendingRequest) catch return Error.OutOfMemory;
        pend.* = .{ .id = req_id, .done = false };

        {
            self.mutex.lock();
            pend.next = self.pending_head;
            self.pending_head = pend;
            self.mutex.unlock();
        }

        const rc = c.mosquitto_publish(
            self.mosq,
            null,
            self.request_topic.ptr,
            @intCast(payload.len),
            payload.ptr,
            self.cfg.qos,
            false,
        );
        if (rc != c.MOSQ_ERR_SUCCESS) {
            log.err("[pubsub_client] publish failed: {s}", .{c.mosquitto_strerror(rc)});
            self.removePending(pend);
            self.allocator.free(req_id);
            self.allocator.destroy(pend);
            return Error.PublishFailed;
        }

        {
            self.mutex.lock();
            defer self.mutex.unlock();

            const deadline_ns = @as(u64, @intCast(std.time.nanoTimestamp())) + tout * std.time.ns_per_ms;

            while (!pend.done) {
                const now_ns = @as(u64, @intCast(std.time.nanoTimestamp()));
                if (now_ns >= deadline_ns) break;
                const remaining = deadline_ns - now_ns;
                pend.cond.timedWait(&self.mutex, remaining) catch break;
            }

            self.unlinkPendingLocked(pend);
        }

        if (!pend.done) {
            log.err("[pubsub_client] call '{s}' timed out", .{method});
            self.allocator.free(req_id);
            if (pend.response) |r| self.allocator.free(r);
            self.allocator.destroy(pend);
            return Error.Timeout;
        }

        const response_json = pend.response orelse {
            self.allocator.free(req_id);
            self.allocator.destroy(pend);
            return Error.PubsubError;
        };
        defer self.allocator.free(response_json);
        self.allocator.free(req_id);
        self.allocator.destroy(pend);

        return self.parseResponse(response_json);
    }

    /// Asynchronous pubsub call — fires a request, invokes callback from
    /// a background thread when the response arrives (or on timeout).
    pub fn callAsync(
        self: *Self,
        method: [:0]const u8,
        params_json: ?[]const u8,
        timeout_ms: u64,
        callback: AsyncCallback,
        cb_userdata: ?*anyopaque,
    ) void {
        // Copy args for the detached thread
        const method_copy = self.allocator.dupeZ(u8, method) catch return;
        const params_copy: ?[]const u8 = if (params_json) |p|
            self.allocator.dupe(u8, p) catch {
                self.allocator.free(method_copy);
                return;
            }
        else
            null;

        const ctx = self.allocator.create(AsyncCtx) catch {
            self.allocator.free(method_copy);
            if (params_copy) |pc| self.allocator.free(pc);
            return;
        };
        ctx.* = .{
            .client = self,
            .method = method_copy,
            .params = params_copy,
            .timeout_ms = timeout_ms,
            .callback = callback,
            .cb_userdata = cb_userdata,
        };

        const thread = std.Thread.spawn(.{}, asyncThread, .{ctx}) catch {
            log.err("[pubsub_client] failed to spawn async thread", .{});
            self.allocator.free(method_copy);
            if (params_copy) |pc| self.allocator.free(pc);
            self.allocator.destroy(ctx);
            return;
        };
        thread.detach();
    }

    // ── Async internals ──────────────────────────────────────────────

    const AsyncCtx = struct {
        client: *Self,
        method: [:0]const u8,
        params: ?[]const u8,
        timeout_ms: u64,
        callback: AsyncCallback,
        cb_userdata: ?*anyopaque,
    };

    fn asyncThread(ctx: *AsyncCtx) void {
        const cli = ctx.client;
        const allocator = cli.allocator;

        defer {
            allocator.free(ctx.method);
            if (ctx.params) |p| allocator.free(p);
            allocator.destroy(ctx);
        }

        var result = cli.call(ctx.method, ctx.params, ctx.timeout_ms) catch {
            // Transport / timeout error
            const terr = "{\"code\":-1,\"message\":\"Transport/timeout error\"}";
            ctx.callback(terr, null, ctx.cb_userdata);
            return;
        };
        defer result.deinit(allocator);

        if (result.err) |err_json| {
            ctx.callback(err_json, null, ctx.cb_userdata);
        } else if (result.result) |res_json| {
            ctx.callback(null, res_json, ctx.cb_userdata);
        } else {
            const terr = "{\"code\":-1,\"message\":\"Empty response\"}";
            ctx.callback(terr, null, ctx.cb_userdata);
        }
    }

    // ── Response parsing ─────────────────────────────────────────────

    fn parseResponse(self: *Self, response_json: []const u8) Error!CallResult {
        var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer arena.deinit();
        const arena_alloc = arena.allocator();

        const parsed = std.json.parseFromSlice(std.json.Value, arena_alloc, response_json, .{}) catch return Error.PubsubError;

        var result = CallResult{};

        if (jsonSerializeField(arena_alloc, parsed.value, "error")) |err_json| {
            result.err = self.allocator.dupe(u8, err_json) catch return Error.OutOfMemory;
        }

        if (jsonSerializeField(arena_alloc, parsed.value, "result")) |res_json| {
            result.result = self.allocator.dupe(u8, res_json) catch return Error.OutOfMemory;
        }

        if (result.err != null) {
            if (result.result) |r| {
                self.allocator.free(r);
                result.result = null;
            }
        }

        return result;
    }

    // ── Pending list management ──────────────────────────────────────

    fn removePending(self: *Self, pend: *PendingRequest) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.unlinkPendingLocked(pend);
    }

    fn unlinkPendingLocked(self: *Self, pend: *PendingRequest) void {
        var pp: *?*PendingRequest = &self.pending_head;
        while (pp.*) |p| {
            if (p == pend) {
                pp.* = p.next;
                return;
            }
            pp = &p.next;
        }
    }

    // ── Mosquitto C callbacks ────────────────────────────────────────

    fn cliOnConnect(_: ?*c.struct_mosquitto, userdata: ?*anyopaque, rc: c_int) callconv(.C) void {
        const self: *Self = @ptrCast(@alignCast(userdata));
        self.mutex.lock();
        self.connected = (rc == 0);
        log.info("[pubsub_client] on_connect rc={d}", .{rc});

        if (self.connected) {
            _ = c.mosquitto_subscribe(self.mosq, null, self.response_topic.ptr, self.cfg.qos);
        }
        self.connect_cond.signal();
        self.mutex.unlock();
    }

    fn cliOnDisconnect(_: ?*c.struct_mosquitto, userdata: ?*anyopaque, _: c_int) callconv(.C) void {
        const self: *Self = @ptrCast(@alignCast(userdata));
        self.mutex.lock();
        defer self.mutex.unlock();
        self.connected = false;
        self.connect_cond.signal();
    }

    fn cliOnSubscribe(_: ?*c.struct_mosquitto, userdata: ?*anyopaque, mid: c_int, _: c_int, _: ?*const c_int) callconv(.C) void {
        const self: *Self = @ptrCast(@alignCast(userdata));
        log.info("[pubsub_client] on_subscribe mid={d}", .{mid});
        self.mutex.lock();
        defer self.mutex.unlock();
        self.subscribed = true;
        self.subscribe_cond.signal();
    }

    fn cliOnMessage(_: ?*c.struct_mosquitto, userdata: ?*anyopaque, msg: ?*const c.struct_mosquitto_message) callconv(.C) void {
        const self: *Self = @ptrCast(@alignCast(userdata));
        const m = msg orelse return;

        if (m.payload == null or m.payloadlen <= 0) return;
        const payload_ptr: [*]const u8 = @ptrCast(m.payload);
        const payload = payload_ptr[0..@intCast(m.payloadlen)];

        var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer arena.deinit();
        const arena_alloc = arena.allocator();

        const parsed = std.json.parseFromSlice(std.json.Value, arena_alloc, payload, .{}) catch return;
        const id_str = jsonGetString(parsed.value, "id") orelse return;

        self.mutex.lock();
        defer self.mutex.unlock();

        var pend = self.pending_head;
        while (pend) |p| {
            if (std.mem.eql(u8, p.id, id_str)) {
                p.response = self.allocator.dupe(u8, payload) catch return;
                p.done = true;
                p.cond.signal();
                return;
            }
            pend = p.next;
        }
    }
};

// ──────────────────────────────────────────────────────────────────────
//  Library init / cleanup
// ──────────────────────────────────────────────────────────────────────

pub fn libInit() void {
    _ = c.mosquitto_lib_init();
}

pub fn libCleanup() void {
    _ = c.mosquitto_lib_cleanup();
}

// ──────────────────────────────────────────────────────────────────────
//  Tests
// ──────────────────────────────────────────────────────────────────────

test "Config defaults" {
    const cfg = Config{};
    try std.testing.expectEqualStrings("localhost", cfg.host);
    try std.testing.expectEqual(@as(u16, 1883), cfg.port);
    try std.testing.expectEqualStrings("pubsub_service", cfg.service_name);
    try std.testing.expectEqual(@as(c_int, 1), cfg.qos);
}

test "extractCallerId" {
    try std.testing.expectEqualStrings("my-client", extractCallerId("pubsub/svc/request/my-client").?);
    try std.testing.expectEqualStrings("abc_123", extractCallerId("pubsub/test/request/abc_123").?);
    try std.testing.expect(extractCallerId("pubsub/svc/request/") == null);
    try std.testing.expect(extractCallerId("pubsub/svc") == null);
}

test "autoClientId" {
    const id = try autoClientId(std.testing.allocator, "test");
    defer std.testing.allocator.free(id);
    try std.testing.expect(std.mem.startsWith(u8, id, "test_"));
}