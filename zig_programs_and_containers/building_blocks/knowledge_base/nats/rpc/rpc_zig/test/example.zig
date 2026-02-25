///  Example program demonstrating the rpc_zig library.
///
///  Requires a running NATS server:
///     docker run -p 4222:4222 nats:latest
///
///  Run with:  zig build example

const std = @import("std");
const rpc = @import("rpc_zig");

const SERVER: [:0]const u8 = "nats://127.0.0.1:4222";

// ================================================================
//  Handlers
// ================================================================

fn addHandler(params_json: []const u8, _: ?*anyopaque) rpc.HandlerResult {
    // Minimal integer extraction from {"a":N,"b":M}
    var a: i64 = 0;
    var b: i64 = 0;

    if (std.mem.indexOf(u8, params_json, "\"a\":")) |idx| {
        a = extractInt(params_json[idx + 4 ..]);
    }
    if (std.mem.indexOf(u8, params_json, "\"b\":")) |idx| {
        b = extractInt(params_json[idx + 4 ..]);
    }

    const S = struct {
        var buf: [128]u8 = undefined;
    };
    const result = std.fmt.bufPrint(&S.buf, "{{\"sum\":{d}}}", .{a + b}) catch
        return .{ .err = "format error" };
    return .{ .ok = result };
}

fn greetHandler(params_json: []const u8, _: ?*anyopaque) rpc.HandlerResult {
    _ = params_json;
    const S = struct {
        var buf: [256]u8 = undefined;
    };
    const result = std.fmt.bufPrint(&S.buf, "{{\"greeting\":\"Hello from Zig RPC!\"}}", .{}) catch
        return .{ .err = "format error" };
    return .{ .ok = result };
}

fn extractInt(s: []const u8) i64 {
    var i: usize = 0;
    var negative = false;
    while (i < s.len and s[i] == ' ') : (i += 1) {}
    if (i < s.len and s[i] == '-') {
        negative = true;
        i += 1;
    }
    var val: i64 = 0;
    while (i < s.len and s[i] >= '0' and s[i] <= '9') : (i += 1) {
        val = val * 10 + @as(i64, s[i] - '0');
    }
    return if (negative) -val else val;
}

// ================================================================
//  Main
// ================================================================

pub fn main() !void {
    const stdout = std.io.getStdOut().writer();

    try stdout.print("\n{s}\n", .{"=" ** 60});
    try stdout.print("  NATS RPC Zig Demo\n", .{});
    try stdout.print("{s}\n", .{"=" ** 60});

    // ============================================================
    //  1. Start server
    // ============================================================
    try stdout.print("\n--- 1. Starting RPC Server ---\n\n", .{});

    var srv = try rpc.Server.init(.{
        .server = SERVER,
        .namespace = "demo",
        .instance_id = "zig-demo-srv",
        .enable_health = true,
    });
    defer srv.deinit();

    try srv.register("math.add", addHandler, null, false);
    try srv.register("greet", greetHandler, null, false);
    try srv.start("rpc");
    defer srv.stop() catch {};

    try stdout.print("  Server running: instance={s}\n", .{srv.instanceId()});
    std.time.sleep(100 * std.time.ns_per_ms);

    // ============================================================
    //  2. Client calls
    // ============================================================
    try stdout.print("\n--- 2. RPC Calls ---\n\n", .{});

    var cli = try rpc.Client.init(.{
        .server = SERVER,
        .namespace = "demo",
    });
    defer cli.deinit();
    try cli.connect();

    // math.add
    {
        var result = try cli.call("rpc.math.add", "{\"a\":42,\"b\":58}", 5.0);
        defer result.deinit();
        try stdout.print("  math.add(42, 58) = {s}\n", .{result.str()});
    }

    // greet
    {
        var result = try cli.call("rpc.greet", "{}", 5.0);
        defer result.deinit();
        try stdout.print("  greet() = {s}\n", .{result.str()});
    }

    // ============================================================
    //  3. Health check
    // ============================================================
    try stdout.print("\n--- 3. Health Check ---\n\n", .{});

    {
        var result = try cli.callInstance("rpc._health", "{}", 5.0, "zig-demo-srv");
        defer result.deinit();
        try stdout.print("  health = {s}\n", .{result.str()});
    }

    // ============================================================
    //  4. Batch calls
    // ============================================================
    try stdout.print("\n--- 4. Batch Calls ---\n\n", .{});

    {
        const entries = [_]rpc.BatchEntry{
            .{ .method = "rpc.math.add", .params_json = "{\"a\":1,\"b\":2}" },
            .{ .method = "rpc.math.add", .params_json = "{\"a\":10,\"b\":20}" },
            .{ .method = "rpc.greet", .params_json = "{}" },
        };

        const results = try cli.callBatch(&entries, 5.0, std.heap.c_allocator);
        defer {
            for (results) |*r| r.deinit();
            std.heap.c_allocator.free(results);
        }

        for (results, 0..) |r, i| {
            const ok_str: []const u8 = if (r.isOk()) "OK" else "ERR";
            try stdout.print("  batch[{d}] {s}: {s}\n", .{
                i,
                ok_str,
                r.str() orelse "(null)",
            });
        }
    }

    // ============================================================
    //  5. Stats
    // ============================================================
    try stdout.print("\n--- 5. Server Stats ---\n\n", .{});

    {
        var stats = try srv.getStats();
        defer stats.deinit();
        for (stats.items) |s| {
            try stdout.print("  {s}: calls={d} errors={d}\n", .{
                s.method,
                s.call_count,
                s.error_count,
            });
        }
    }

    // ============================================================
    //  6. Timeout demo
    // ============================================================
    try stdout.print("\n--- 6. Timeout ---\n\n", .{});

    {
        const timeout_result = cli.call("rpc.no.such.method", "{}", 0.2);
        if (timeout_result) |_| {
            try stdout.print("  Unexpected reply\n", .{});
        } else |err| {
            try stdout.print("  Timed out as expected: {any}\n", .{err});
        }
    }

    try stdout.print("\n{s}\n", .{"=" ** 60});
    try stdout.print("  Done.\n", .{});
    try stdout.print("{s}\n\n", .{"=" ** 60});
}