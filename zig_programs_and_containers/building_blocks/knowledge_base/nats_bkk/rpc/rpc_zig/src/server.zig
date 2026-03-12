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
    instance_id: ?[:0]const u8 = null,
    enable_health: bool = true,

    pub fn toCConfig(self: Config) c.RpcConfig {
        return .{
            .server = self.server.ptr,
            .namespace_ = self.namespace.ptr,
            .instance_id = if (self.instance_id) |id| id.ptr else null,
            .enable_health = self.enable_health,
        };
    }
};

// ----------------------------------------------------------------
//  Handler stats
// ----------------------------------------------------------------

pub const HandlerStats = struct {
    method: []const u8,
    call_count: i64,
    error_count: i64,
    instance_specific: bool,
};

pub const StatsArray = struct {
    items: []HandlerStats,
    raw: [*]c.RpcHandlerStats,

    pub fn deinit(self: *StatsArray) void {
        std.heap.c_allocator.free(self.items);
        std.c.free(@ptrCast(self.raw));
        self.items = &.{};
    }
};

// ----------------------------------------------------------------
//  Handler callback types
// ----------------------------------------------------------------

/// Zig-friendly handler signature.
/// Return null for success with no result, or a result string.
/// Return error to send an error response.
pub const HandlerFn = *const fn (params_json: []const u8, user_data: ?*anyopaque) HandlerResult;

pub const HandlerResult = union(enum) {
    ok: ?[]const u8,
    err: []const u8,
};

/// Internal context for C callback → Zig callback bridge.
const HandlerContext = struct {
    zig_fn: HandlerFn,
    user_data: ?*anyopaque,
    /// Scratch buffer for NUL-terminated result strings returned to C.
    result_buf: [4096]u8 = undefined,
};

/// C-ABI trampoline: converts C handler call → Zig handler call.
fn cHandlerTrampoline(
    params_json: [*:0]const u8,
    closure: ?*anyopaque,
    result_json: *?[*:0]u8,
) callconv(.C) c.rpc_status_t {
    const ctx: *HandlerContext = @ptrCast(@alignCast(closure orelse return c.RPC_ERR_INVALID_ARG));

    const params = std.mem.span(params_json);
    const hr = ctx.zig_fn(params, ctx.user_data);

    switch (hr) {
        .ok => |maybe_result| {
            if (maybe_result) |res| {
                // Copy result into malloc'd buffer for C to own
                const buf = std.c.malloc(res.len + 1) orelse return c.RPC_ERR_MEMORY;
                const dest: [*]u8 = @ptrCast(buf);
                @memcpy(dest[0..res.len], res);
                dest[res.len] = 0;
                result_json.* = @ptrCast(dest);
            } else {
                result_json.* = null;
            }
            return c.RPC_OK;
        },
        .err => |msg| {
            const buf = std.c.malloc(msg.len + 1) orelse return c.RPC_ERR_HANDLER;
            const dest: [*]u8 = @ptrCast(buf);
            @memcpy(dest[0..msg.len], msg);
            dest[msg.len] = 0;
            result_json.* = @ptrCast(dest);
            return c.RPC_ERR_HANDLER;
        },
    }
}

// ----------------------------------------------------------------
//  Server
// ----------------------------------------------------------------

pub const Server = struct {
    handle: *c.RpcServer,
    contexts: std.ArrayList(*HandlerContext),

    const Self = @This();

    pub fn init(cfg: Config) Error!Self {
        var handle: ?*c.RpcServer = null;
        var cc = cfg.toCConfig();
        try status.check(c.rpc_server_create(&handle, &cc));
        return Self{
            .handle = handle.?,
            .contexts = std.ArrayList(*HandlerContext).init(std.heap.c_allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        for (self.contexts.items) |ctx| {
            std.heap.c_allocator.destroy(ctx);
        }
        self.contexts.deinit();
        c.rpc_server_destroy(self.handle);
        self.handle = undefined;
    }

    /// Register a method handler.
    pub fn register(
        self: *Self,
        method: [:0]const u8,
        handler: HandlerFn,
        user_data: ?*anyopaque,
        instance_specific: bool,
    ) Error!void {
        const ctx = std.heap.c_allocator.create(HandlerContext) catch
            return Error.OutOfMemory;
        ctx.* = .{
            .zig_fn = handler,
            .user_data = user_data,
        };

        status.check(c.rpc_server_register(
            self.handle,
            method.ptr,
            cHandlerTrampoline,
            @ptrCast(ctx),
            instance_specific,
        )) catch |err| {
            std.heap.c_allocator.destroy(ctx);
            return err;
        };

        self.contexts.append(ctx) catch {
            std.heap.c_allocator.destroy(ctx);
            return Error.OutOfMemory;
        };
    }

    /// Start the server. Returns immediately; requests handled on nats.c threads.
    pub fn start(self: *Self, prefix: ?[:0]const u8) Error!void {
        const prefix_ptr: ?[*:0]const u8 = if (prefix) |p| p.ptr else null;
        try status.check(c.rpc_server_start(self.handle, prefix_ptr));
    }

    /// Block until stop() is called.
    pub fn wait(self: *Self) void {
        c.rpc_server_wait(self.handle);
    }

    /// Stop the server.
    pub fn stop(self: *Self) Error!void {
        try status.check(c.rpc_server_stop(self.handle));
    }

    pub fn instanceId(self: *const Self) []const u8 {
        const ptr = c.rpc_server_instance_id(self.handle);
        if (ptr) |p| return std.mem.span(p);
        return "";
    }

    pub fn isRunning(self: *const Self) bool {
        return c.rpc_server_is_running(self.handle);
    }

    /// Get per-handler statistics. Caller must call `result.deinit()`.
    pub fn getStats(self: *const Self) Error!StatsArray {
        var raw: ?[*]c.RpcHandlerStats = null;
        var count: usize = 0;
        try status.check(c.rpc_server_get_stats(self.handle, &raw, &count));

        const raw_ptr = raw orelse return StatsArray{
            .items = &.{},
            .raw = undefined,
        };

        const items = std.heap.c_allocator.alloc(HandlerStats, count) catch
            return Error.OutOfMemory;

        for (0..count) |i| {
            items[i] = .{
                .method = if (raw_ptr[i].method) |m| std.mem.span(m) else "",
                .call_count = raw_ptr[i].call_count,
                .error_count = raw_ptr[i].error_count,
                .instance_specific = raw_ptr[i].instance_specific,
            };
        }

        return StatsArray{
            .items = items,
            .raw = raw_ptr,
        };
    }
};