const std = @import("std");
const c = @import("c_api.zig");
const status = @import("status.zig");
const server_mod = @import("server.zig");
pub const Error = status.Error;
pub const Config = server_mod.Config;

// ----------------------------------------------------------------
//  Call result
// ----------------------------------------------------------------

pub const CallResult = struct {
    json: [:0]u8,

    /// Free the malloc'd result string.
    pub fn deinit(self: *CallResult) void {
        std.c.free(@ptrCast(self.json.ptr));
        self.json = undefined;
    }

    /// Get the result as a slice.
    pub fn str(self: *const CallResult) []const u8 {
        return self.json;
    }
};

// ----------------------------------------------------------------
//  Batch types
// ----------------------------------------------------------------

pub const BatchEntry = struct {
    method: [:0]const u8,
    params_json: ?[:0]const u8 = null,
    target_instance: ?[:0]const u8 = null,
};

pub const BatchResult = struct {
    status_code: c.rpc_status_t,
    json: ?[:0]u8,

    pub fn isOk(self: *const BatchResult) bool {
        return self.status_code == c.RPC_OK;
    }

    pub fn str(self: *const BatchResult) ?[]const u8 {
        if (self.json) |j| return j;
        return null;
    }

    /// Free the malloc'd result string (if any).
    pub fn deinit(self: *BatchResult) void {
        if (self.json) |j| {
            std.c.free(@ptrCast(j.ptr));
            self.json = null;
        }
    }
};

// ----------------------------------------------------------------
//  Client
// ----------------------------------------------------------------

pub const Client = struct {
    handle: *c.RpcClient,

    const Self = @This();

    pub fn init(cfg: Config) Error!Self {
        var handle: ?*c.RpcClient = null;
        var cc = cfg.toCConfig();
        try status.check(c.rpc_client_create(&handle, &cc));
        return Self{ .handle = handle.? };
    }

    pub fn deinit(self: *Self) void {
        c.rpc_client_destroy(self.handle);
        self.handle = undefined;
    }

    pub fn connect(self: *Self) Error!void {
        try status.check(c.rpc_client_connect(self.handle));
    }

    pub fn disconnect(self: *Self) Error!void {
        try status.check(c.rpc_client_disconnect(self.handle));
    }

    pub fn isConnected(self: *const Self) bool {
        return c.rpc_client_is_connected(self.handle);
    }

    pub fn instanceId(self: *const Self) []const u8 {
        const ptr = c.rpc_client_instance_id(self.handle);
        if (ptr) |p| return std.mem.span(p);
        return "";
    }

    /// Make a synchronous RPC call.
    /// On HandlerError, the result contains the error message from the server.
    pub fn call(
        self: *Self,
        method: [:0]const u8,
        params_json: ?[:0]const u8,
        timeout_sec: f64,
    ) Error!CallResult {
        var result_ptr: ?[*:0]u8 = null;
        const params_ptr: ?[*:0]const u8 = if (params_json) |p| p.ptr else null;

        const st = c.rpc_client_call(
            self.handle,
            method.ptr,
            params_ptr,
            timeout_sec,
            &result_ptr,
        );

        // On HandlerError, result_ptr contains the error message
        if (st == c.RPC_ERR_HANDLER) {
            if (result_ptr) |rp| {
                std.c.free(@ptrCast(rp));
            }
            return Error.HandlerError;
        }

        try status.check(st);

        if (result_ptr) |rp| {
            return CallResult{ .json = std.mem.span(rp) };
        }
        return Error.DecodeError;
    }

    /// Call targeting a specific server instance.
    pub fn callInstance(
        self: *Self,
        method: [:0]const u8,
        params_json: ?[:0]const u8,
        timeout_sec: f64,
        target_instance: [:0]const u8,
    ) Error!CallResult {
        var result_ptr: ?[*:0]u8 = null;
        const params_ptr: ?[*:0]const u8 = if (params_json) |p| p.ptr else null;

        const st = c.rpc_client_call_instance(
            self.handle,
            method.ptr,
            params_ptr,
            timeout_sec,
            target_instance.ptr,
            &result_ptr,
        );

        if (st == c.RPC_ERR_HANDLER) {
            if (result_ptr) |rp| {
                std.c.free(@ptrCast(rp));
            }
            return Error.HandlerError;
        }

        try status.check(st);

        if (result_ptr) |rp| {
            return CallResult{ .json = std.mem.span(rp) };
        }
        return Error.DecodeError;
    }

    /// Execute multiple RPC calls sequentially.
    /// Caller must call `deinit()` on each result.
    pub fn callBatch(
        self: *Self,
        entries: []const BatchEntry,
        timeout_sec: f64,
        allocator: std.mem.Allocator,
    ) Error![]BatchResult {
        const count = entries.len;
        if (count == 0) return Error.InvalidArg;

        // Build C batch entry array
        const c_entries = allocator.alloc(c.RpcBatchEntry, count) catch
            return Error.OutOfMemory;
        defer allocator.free(c_entries);

        for (entries, 0..) |e, i| {
            c_entries[i] = .{
                .method = e.method.ptr,
                .params_json = if (e.params_json) |p| p.ptr else null,
                .target_instance = if (e.target_instance) |t| t.ptr else null,
            };
        }

        // Allocate C result array
        const c_results = allocator.alloc(c.RpcBatchResult, count) catch
            return Error.OutOfMemory;
        defer allocator.free(c_results);

        // Initialize
        for (c_results) |*r| {
            r.status = c.RPC_OK;
            r.result_json = null;
        }

        try status.check(c.rpc_client_call_batch(
            self.handle,
            c_entries.ptr,
            count,
            timeout_sec,
            c_results.ptr,
        ));

        // Convert to Zig results
        const results = allocator.alloc(BatchResult, count) catch
            return Error.OutOfMemory;

        for (c_results, 0..) |cr, i| {
            results[i] = .{
                .status_code = cr.status,
                .json = if (cr.result_json) |rp| std.mem.span(rp) else null,
            };
        }

        return results;
    }
};