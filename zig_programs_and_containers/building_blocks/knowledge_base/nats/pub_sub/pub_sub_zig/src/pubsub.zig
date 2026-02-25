const std = @import("std");
const c = @import("c_api.zig");
const status = @import("status.zig");
pub const Error = status.Error;

// ----------------------------------------------------------------
//  Configuration
// ----------------------------------------------------------------

pub const Config = struct {
    server: [:0]const u8 = "nats://127.0.0.1:4222",
    namespace: [:0]const u8 = "default",
    client_name: ?[:0]const u8 = null,

    fn toCConfig(self: Config) c.PubSubConfig {
        return .{
            .server = self.server.ptr,
            .namespace_ = self.namespace.ptr,
            .client_name = if (self.client_name) |n| n.ptr else null,
        };
    }
};

// ----------------------------------------------------------------
//  Message (delivered to callbacks)
// ----------------------------------------------------------------

pub const Message = struct {
    subject: []const u8,
    original_subject: []const u8,
    data: []const u8,
    reply_to: ?[]const u8,

    /// Get the data as a string (same bytes, just typed).
    pub fn dataStr(self: *const Message) []const u8 {
        return self.data;
    }
};

// ----------------------------------------------------------------
//  Stats
// ----------------------------------------------------------------

pub const Stats = struct {
    msgs_published: i64,
    msgs_received: i64,
    active_subscriptions: i32,
};

// ----------------------------------------------------------------
//  Callback types
// ----------------------------------------------------------------

/// Zig-friendly callback signature.
pub const MessageCallback = *const fn (msg: *const Message, user_data: ?*anyopaque) void;

/// Internal context to bridge C callback → Zig callback.
const CallbackContext = struct {
    zig_cb: MessageCallback,
    user_data: ?*anyopaque,
};

/// C-ABI trampoline: converts C PubSubMsg → Zig Message and calls the user's callback.
fn cCallbackTrampoline(raw_msg: *const c.PubSubMsg, closure: ?*anyopaque) callconv(.C) void {
    const ctx: *CallbackContext = @ptrCast(@alignCast(closure orelse return));

    const msg = Message{
        .subject = if (raw_msg.subject) |s| std.mem.span(s) else "",
        .original_subject = if (raw_msg.original_subject) |s| std.mem.span(s) else "",
        .data = if (raw_msg.data) |d|
            d[0..@as(usize, @intCast(if (raw_msg.data_len > 0) raw_msg.data_len else 0))]
        else
            "",
        .reply_to = if (raw_msg.reply_to) |r| std.mem.span(r) else null,
    };

    ctx.zig_cb(&msg, ctx.user_data);
}

// ----------------------------------------------------------------
//  Subscription handle
// ----------------------------------------------------------------

pub const Subscription = struct {
    handle: *c.PubSubSub,
    ctx: *CallbackContext,

    pub fn subject(self: *const Subscription) []const u8 {
        const ptr = c.pubsub_sub_subject(self.handle);
        if (ptr) |p| {
            return std.mem.span(p);
        }
        return "";
    }
};

// ----------------------------------------------------------------
//  PubSub
// ----------------------------------------------------------------

pub const PubSub = struct {
    handle: *c.PubSub,
    /// Track callback contexts so we can free them.
    contexts: std.ArrayList(*CallbackContext),

    const Self = @This();

    /// Create a PubSub client (does NOT connect).
    pub fn init(cfg: Config) Error!Self {
        var handle: ?*c.PubSub = null;
        var cc = cfg.toCConfig();
        try status.check(c.pubsub_create(&handle, &cc));
        return Self{
            .handle = handle.?,
            .contexts = std.ArrayList(*CallbackContext).init(std.heap.c_allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        // Free all callback contexts
        for (self.contexts.items) |ctx| {
            std.heap.c_allocator.destroy(ctx);
        }
        self.contexts.deinit();
        c.pubsub_destroy(self.handle);
        self.handle = undefined;
    }

    // ----------------------------------------------------------
    //  Connection
    // ----------------------------------------------------------

    pub fn connect(self: *Self) Error!void {
        try status.check(c.pubsub_connect(self.handle));
    }

    pub fn disconnect(self: *Self) Error!void {
        try status.check(c.pubsub_disconnect(self.handle));
    }

    pub fn isConnected(self: *const Self) bool {
        return c.pubsub_is_connected(self.handle);
    }

    pub fn getNamespace(self: *const Self) []const u8 {
        const ptr = c.pubsub_namespace(self.handle);
        if (ptr) |p| return std.mem.span(p);
        return "";
    }

    pub fn clientName(self: *const Self) []const u8 {
        const ptr = c.pubsub_client_name(self.handle);
        if (ptr) |p| return std.mem.span(p);
        return "";
    }

    // ----------------------------------------------------------
    //  Publish
    // ----------------------------------------------------------

    /// Publish raw bytes to a subject.
    pub fn publish(self: *Self, subject_name: [:0]const u8, data: []const u8) Error!void {
        try status.check(c.pubsub_publish(
            self.handle,
            subject_name.ptr,
            if (data.len > 0) data.ptr else null,
            @intCast(data.len),
        ));
    }

    /// Publish a string to a subject.
    pub fn publishStr(self: *Self, subject_name: [:0]const u8, str: [:0]const u8) Error!void {
        try status.check(c.pubsub_publish_str(self.handle, subject_name.ptr, str.ptr));
    }

    // ----------------------------------------------------------
    //  Subscribe
    // ----------------------------------------------------------

    /// Subscribe to a subject with namespace prefix.
    pub fn subscribe(
        self: *Self,
        subject_name: [:0]const u8,
        cb: MessageCallback,
        user_data: ?*anyopaque,
        queue: ?[:0]const u8,
    ) Error!Subscription {
        return self.doSubscribe(false, subject_name, cb, user_data, queue);
    }

    /// Subscribe to a raw subject (no namespace prefix).
    pub fn subscribeRaw(
        self: *Self,
        subject_name: [:0]const u8,
        cb: MessageCallback,
        user_data: ?*anyopaque,
        queue: ?[:0]const u8,
    ) Error!Subscription {
        return self.doSubscribe(true, subject_name, cb, user_data, queue);
    }

    fn doSubscribe(
        self: *Self,
        raw: bool,
        subject_name: [:0]const u8,
        cb: MessageCallback,
        user_data: ?*anyopaque,
        queue: ?[:0]const u8,
    ) Error!Subscription {
        // Allocate a callback context that lives until unsubscribe/destroy.
        const ctx = std.heap.c_allocator.create(CallbackContext) catch
            return Error.OutOfMemory;
        ctx.* = .{
            .zig_cb = cb,
            .user_data = user_data,
        };

        var sub_handle: ?*c.PubSubSub = null;
        const queue_ptr: ?[*:0]const u8 = if (queue) |q| q.ptr else null;

        const st = if (raw)
            c.pubsub_subscribe_raw(
                self.handle,
                subject_name.ptr,
                cCallbackTrampoline,
                @ptrCast(ctx),
                queue_ptr,
                &sub_handle,
            )
        else
            c.pubsub_subscribe(
                self.handle,
                subject_name.ptr,
                cCallbackTrampoline,
                @ptrCast(ctx),
                queue_ptr,
                &sub_handle,
            );

        status.check(st) catch |err| {
            std.heap.c_allocator.destroy(ctx);
            return err;
        };

        self.contexts.append(ctx) catch {
            std.heap.c_allocator.destroy(ctx);
            return Error.OutOfMemory;
        };

        return Subscription{
            .handle = sub_handle.?,
            .ctx = ctx,
        };
    }

    /// Unsubscribe and free the subscription.
    pub fn unsubscribe(self: *Self, sub: *Subscription) Error!void {
        try status.check(c.pubsub_unsubscribe(self.handle, sub.handle));

        // Remove and free the callback context
        for (self.contexts.items, 0..) |ctx, i| {
            if (ctx == sub.ctx) {
                _ = self.contexts.swapRemove(i);
                std.heap.c_allocator.destroy(ctx);
                break;
            }
        }
        sub.handle = undefined;
        sub.ctx = undefined;
    }

    /// Auto-unsubscribe after a number of messages.
    pub fn autoUnsubscribe(sub: *const Subscription, max_msgs: i32) Error!void {
        try status.check(c.pubsub_auto_unsubscribe(sub.handle, @intCast(max_msgs)));
    }

    // ----------------------------------------------------------
    //  Request / Reply
    // ----------------------------------------------------------

    /// Send a request and wait for a reply (synchronous).
    /// Returns the reply data. Caller must free with `freeReply`.
    pub fn request(
        self: *Self,
        subject_name: [:0]const u8,
        data: []const u8,
        timeout_sec: f64,
    ) Error!struct { data: [:0]u8, len: i32 } {
        var reply_data: ?[*:0]u8 = null;
        var reply_len: c_int = 0;

        try status.check(c.pubsub_request(
            self.handle,
            subject_name.ptr,
            if (data.len > 0) data.ptr else null,
            @intCast(data.len),
            timeout_sec,
            &reply_data,
            &reply_len,
        ));

        if (reply_data) |rd| {
            return .{
                .data = std.mem.span(rd),
                .len = reply_len,
            };
        }
        return Error.Timeout;
    }

    /// Request with a string, get a string reply.
    pub fn requestStr(
        self: *Self,
        subject_name: [:0]const u8,
        str: [:0]const u8,
        timeout_sec: f64,
    ) Error![:0]u8 {
        var reply_ptr: ?[*:0]u8 = null;
        try status.check(c.pubsub_request_str(
            self.handle,
            subject_name.ptr,
            str.ptr,
            timeout_sec,
            &reply_ptr,
        ));
        if (reply_ptr) |r| {
            return std.mem.span(r);
        }
        return Error.Timeout;
    }

    /// Free a reply returned by `request` or `requestStr`.
    pub fn freeReply(data: anytype) void {
        const ptr = switch (@TypeOf(data)) {
            [:0]u8 => @as(*anyopaque, @ptrCast(@constCast(data.ptr))),
            else => @as(*anyopaque, @ptrCast(@constCast(data.data.ptr))),
        };
        std.c.free(ptr);
    }

    /// Reply to a message (use inside a callback).
    pub fn reply(self: *Self, reply_to: [:0]const u8, data: []const u8) Error!void {
        try status.check(c.pubsub_reply(
            self.handle,
            reply_to.ptr,
            if (data.len > 0) data.ptr else null,
            @intCast(data.len),
        ));
    }

    /// Reply with a string.
    pub fn replyStr(self: *Self, reply_to: [:0]const u8, str: [:0]const u8) Error!void {
        try status.check(c.pubsub_reply_str(self.handle, reply_to.ptr, str.ptr));
    }

    // ----------------------------------------------------------
    //  Statistics
    // ----------------------------------------------------------

    pub fn getStats(self: *const Self) Error!Stats {
        var raw: c.PubSubStats = .{};
        try status.check(c.pubsub_get_stats(self.handle, &raw));
        return Stats{
            .msgs_published = raw.msgs_published,
            .msgs_received = raw.msgs_received,
            .active_subscriptions = raw.active_subscriptions,
        };
    }
};