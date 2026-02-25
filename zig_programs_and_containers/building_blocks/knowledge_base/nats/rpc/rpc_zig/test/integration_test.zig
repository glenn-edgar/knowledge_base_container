///  Integration tests for rpc_zig.
///  Requires a running NATS server at 127.0.0.1:4222:
///
///     docker run -p 4222:4222 nats:latest
///
///  Run with:  zig build integration

const std = @import("std");
const rpc = @import("rpc_zig");

const TEST_SERVER: [:0]const u8 = "nats://127.0.0.1:4222";
const TEST_NS: [:0]const u8 = "test_zig_rpc";

// ================================================================
//  Test handlers
// ================================================================

fn addHandler(params_json: []const u8, _: ?*anyopaque) rpc.HandlerResult {
    // Simple parser: look for "a" and "b" values
    // In production you'd use a proper JSON parser
    var a: i64 = 0;
    var b: i64 = 0;

    // Find "a": and "b": values
    if (std.mem.indexOf(u8, params_json, "\"a\":")) |idx| {
        const start = idx + 4;
        const rest = params_json[start..];
        a = parseNumber(rest);
    }
    if (std.mem.indexOf(u8, params_json, "\"b\":")) |idx| {
        const start = idx + 4;
        const rest = params_json[start..];
        b = parseNumber(rest);
    }

    const sum = a + b;

    // Build JSON result — use a static buffer
    const S = struct {
        var buf: [128]u8 = undefined;
    };
    const result = std.fmt.bufPrint(&S.buf, "{{\"sum\":{d}}}", .{sum}) catch
        return .{ .err = "format error" };

    return .{ .ok = result };
}

fn echoHandler(params_json: []const u8, _: ?*anyopaque) rpc.HandlerResult {
    _ = params_json;
    const S = struct {
        var buf: [256]u8 = undefined;
    };
    const result = std.fmt.bufPrint(&S.buf, "{{\"echo\":\"hello\"}}", .{}) catch
        return .{ .err = "format error" };
    return .{ .ok = result };
}

fn errorHandler(_: []const u8, _: ?*anyopaque) rpc.HandlerResult {
    return .{ .err = "intentional error" };
}

fn parseNumber(s: []const u8) i64 {
    var i: usize = 0;
    var negative = false;
    // Skip whitespace
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
//  Tests
// ================================================================

test "rpc: server create and destroy" {
    var srv = try rpc.Server.init(.{
        .server = TEST_SERVER,
        .namespace = TEST_NS,
        .instance_id = "test-srv-1",
        .enable_health = false,
    });
    defer srv.deinit();

    try std.testing.expectEqualStrings("test-srv-1", srv.instanceId());
}

test "rpc: client create and connect" {
    var cli = try rpc.Client.init(.{
        .server = TEST_SERVER,
        .namespace = TEST_NS,
    });
    defer cli.deinit();

    try cli.connect();
    try std.testing.expect(cli.isConnected());

    try cli.disconnect();
    try std.testing.expect(!cli.isConnected());
}

test "rpc: basic call" {
    // Start server
    var srv = try rpc.Server.init(.{
        .server = TEST_SERVER,
        .namespace = TEST_NS,
        .instance_id = "test-basic-srv",
        .enable_health = false,
    });
    defer srv.deinit();

    try srv.register("math.add", addHandler, null, false);
    try srv.start("rpc");
    defer srv.stop() catch {};

    std.time.sleep(100 * std.time.ns_per_ms);

    // Client call
    var cli = try rpc.Client.init(.{
        .server = TEST_SERVER,
        .namespace = TEST_NS,
    });
    defer cli.deinit();
    try cli.connect();

    var result = try cli.call("rpc.math.add", "{\"a\":5,\"b\":3}", 5.0);
    defer result.deinit();

    // Result should contain "sum":8
    try std.testing.expect(std.mem.indexOf(u8, result.str(), "8") != null);
}

test "rpc: handler error" {
    var srv = try rpc.Server.init(.{
        .server = TEST_SERVER,
        .namespace = TEST_NS,
        .instance_id = "test-err-srv",
        .enable_health = false,
    });
    defer srv.deinit();

    try srv.register("fail", errorHandler, null, false);
    try srv.start("rpc");
    defer srv.stop() catch {};

    std.time.sleep(100 * std.time.ns_per_ms);

    var cli = try rpc.Client.init(.{
        .server = TEST_SERVER,
        .namespace = TEST_NS,
    });
    defer cli.deinit();
    try cli.connect();

    const result = cli.call("rpc.fail", "{}", 5.0);
    try std.testing.expectError(rpc.Error.HandlerError, result);
}

test "rpc: timeout" {
    var cli = try rpc.Client.init(.{
        .server = TEST_SERVER,
        .namespace = TEST_NS,
    });
    defer cli.deinit();
    try cli.connect();

    const result = cli.call("rpc.no.such.method", "{}", 0.2);
    try std.testing.expectError(rpc.Error.Timeout, result);
}

test "rpc: instance-specific call" {
    var srv = try rpc.Server.init(.{
        .server = TEST_SERVER,
        .namespace = TEST_NS,
        .instance_id = "specific-inst-1",
        .enable_health = false,
    });
    defer srv.deinit();

    try srv.register("echo", echoHandler, null, true);
    try srv.start("rpc");
    defer srv.stop() catch {};

    std.time.sleep(100 * std.time.ns_per_ms);

    var cli = try rpc.Client.init(.{
        .server = TEST_SERVER,
        .namespace = TEST_NS,
    });
    defer cli.deinit();
    try cli.connect();

    var result = try cli.callInstance("rpc.echo", "{}", 5.0, "specific-inst-1");
    defer result.deinit();

    try std.testing.expect(std.mem.indexOf(u8, result.str(), "echo") != null);
}

test "rpc: health check" {
    var srv = try rpc.Server.init(.{
        .server = TEST_SERVER,
        .namespace = TEST_NS,
        .instance_id = "health-test-srv",
        .enable_health = true,
    });
    defer srv.deinit();

    try srv.register("dummy", echoHandler, null, false);
    try srv.start("rpc");
    defer srv.stop() catch {};

    std.time.sleep(100 * std.time.ns_per_ms);

    var cli = try rpc.Client.init(.{
        .server = TEST_SERVER,
        .namespace = TEST_NS,
    });
    defer cli.deinit();
    try cli.connect();

    var result = try cli.callInstance("rpc._health", "{}", 5.0, "health-test-srv");
    defer result.deinit();

    try std.testing.expect(std.mem.indexOf(u8, result.str(), "healthy") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.str(), "health-test-srv") != null);
}

test "rpc: server stats" {
    var srv = try rpc.Server.init(.{
        .server = TEST_SERVER,
        .namespace = TEST_NS,
        .instance_id = "stats-test-srv",
        .enable_health = false,
    });
    defer srv.deinit();

    try srv.register("math.add", addHandler, null, false);
    try srv.start("rpc");
    defer srv.stop() catch {};

    std.time.sleep(100 * std.time.ns_per_ms);

    // Make a call
    var cli = try rpc.Client.init(.{
        .server = TEST_SERVER,
        .namespace = TEST_NS,
    });
    defer cli.deinit();
    try cli.connect();

    var r1 = try cli.call("rpc.math.add", "{\"a\":1,\"b\":2}", 5.0);
    r1.deinit();

    std.time.sleep(50 * std.time.ns_per_ms);

    var stats = try srv.getStats();
    defer stats.deinit();

    try std.testing.expect(stats.items.len >= 1);
    try std.testing.expectEqualStrings("math.add", stats.items[0].method);
    try std.testing.expect(stats.items[0].call_count >= 1);
}

test "rpc: batch calls" {
    var srv = try rpc.Server.init(.{
        .server = TEST_SERVER,
        .namespace = TEST_NS,
        .instance_id = "batch-test-srv",
        .enable_health = false,
    });
    defer srv.deinit();

    try srv.register("math.add", addHandler, null, false);
    try srv.register("echo", echoHandler, null, false);
    try srv.start("rpc");
    defer srv.stop() catch {};

    std.time.sleep(100 * std.time.ns_per_ms);

    var cli = try rpc.Client.init(.{
        .server = TEST_SERVER,
        .namespace = TEST_NS,
    });
    defer cli.deinit();
    try cli.connect();

    const entries = [_]rpc.BatchEntry{
        .{ .method = "rpc.math.add", .params_json = "{\"a\":10,\"b\":20}" },
        .{ .method = "rpc.echo", .params_json = "{}" },
    };

    var results = try cli.callBatch(&entries, 5.0, std.heap.c_allocator);
    defer {
        for (results) |*r| r.deinit();
        std.heap.c_allocator.free(results);
    }

    try std.testing.expectEqual(@as(usize, 2), results.len);
    try std.testing.expect(results[0].isOk());
    try std.testing.expect(results[1].isOk());

    if (results[0].str()) |s| {
        try std.testing.expect(std.mem.indexOf(u8, s, "30") != null);
    }
}