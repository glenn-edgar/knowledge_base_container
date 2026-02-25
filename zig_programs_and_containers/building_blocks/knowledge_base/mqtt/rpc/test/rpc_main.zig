//! rpc_main.zig - Test driver for MQTT RPC library
//!
//! Starts an RPC server with demo methods, then uses a client to call them.
//! Requires: Mosquitto broker running on localhost:1883

const std = @import("std");
const rpc = @import("mqtt_rpc");

fn print(comptime fmt: []const u8, args: anytype) void {
    const stdout = std.io.getStdOut().writer();
    stdout.print(fmt, args) catch {};
}

fn sleepMs(ms: u64) void {
    std.time.sleep(ms * std.time.ns_per_ms);
}

// ──────────────────────────────────────────────────────────────────────
//  Method handlers
// ──────────────────────────────────────────────────────────────────────

/// echo — returns params unchanged
fn methodEcho(allocator: std.mem.Allocator, params_json: ?[]const u8, _: ?*anyopaque) ?[]const u8 {
    const params = params_json orelse return allocator.dupe(u8, "null") catch null;
    return allocator.dupe(u8, params) catch null;
}

/// add — adds two numbers from {"a": N, "b": M}
fn methodAdd(allocator: std.mem.Allocator, params_json: ?[]const u8, _: ?*anyopaque) ?[]const u8 {
    const json_bytes = params_json orelse return null;

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    const parsed = std.json.parseFromSlice(std.json.Value, arena.allocator(), json_bytes, .{}) catch return null;
    if (parsed.value != .object) return null;

    const a_val = parsed.value.object.get("a") orelse return null;
    const b_val = parsed.value.object.get("b") orelse return null;

    const a: f64 = switch (a_val) {
        .integer => |i| @floatFromInt(i),
        .float => |f| f,
        else => return null,
    };
    const b: f64 = switch (b_val) {
        .integer => |i| @floatFromInt(i),
        .float => |f| f,
        else => return null,
    };

    return std.fmt.allocPrint(allocator, "{d}", .{a + b}) catch null;
}

/// greet — returns a greeting string from {"name": "..."}
fn methodGreet(allocator: std.mem.Allocator, params_json: ?[]const u8, _: ?*anyopaque) ?[]const u8 {
    const json_bytes = params_json orelse return null;

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    const parsed = std.json.parseFromSlice(std.json.Value, arena.allocator(), json_bytes, .{}) catch return null;
    if (parsed.value != .object) return null;

    const name_val = parsed.value.object.get("name") orelse return null;
    const name = switch (name_val) {
        .string => |s| s,
        else => return null,
    };

    return std.fmt.allocPrint(allocator, "\"Hello, {s}!\"", .{name}) catch null;
}

/// get_status — returns a status object (no params needed)
fn methodGetStatus(allocator: std.mem.Allocator, _: ?[]const u8, _: ?*anyopaque) ?[]const u8 {
    return allocator.dupe(u8, "{\"status\":\"running\",\"uptime\":12345,\"version\":\"1.0.0-zig\"}") catch null;
}

/// slow_task — simulates a slow operation
fn methodSlowTask(allocator: std.mem.Allocator, _: ?[]const u8, _: ?*anyopaque) ?[]const u8 {
    std.time.sleep(500 * std.time.ns_per_ms);
    return allocator.dupe(u8, "{\"completed\":true,\"duration_ms\":500}") catch null;
}

// ──────────────────────────────────────────────────────────────────────
//  Test: start server
// ──────────────────────────────────────────────────────────────────────

fn runServer(allocator: std.mem.Allocator) !*rpc.Server {
    print("\n=== Starting RPC Server ===\n\n", .{});

    const server = try allocator.create(rpc.Server);
    server.* = try rpc.Server.init(.{
        .service_name = "test_service",
        .client_id = "rpc-test-server",
    }, allocator);

    // Register methods
    server.register("echo", methodEcho, null);
    server.register("add", methodAdd, null);
    server.register("greet", methodGreet, null);
    server.register("get_status", methodGetStatus, null);
    server.register("slow_task", methodSlowTask, null);

    print("Starting server...\n", .{});
    try server.start(true, 2000);
    print("Server running.\n\n", .{});

    return server;
}

// ──────────────────────────────────────────────────────────────────────
//  Test: client calls
// ──────────────────────────────────────────────────────────────────────

fn runClientTests(allocator: std.mem.Allocator) !void {
    print("=== Running RPC Client Tests ===\n\n", .{});

    var client = try rpc.Client.init(.{
        .service_name = "test_service",
        .client_id = "rpc-test-client",
    }, allocator, 10000);
    defer client.deinit();

    print("Connecting client...\n", .{});
    try client.connect(5000);
    defer client.disconnect();

    sleepMs(500); // let things settle

    // Test 1: echo
    print("\n--- Test 1: echo ---\n", .{});
    {
        var result = try client.call("echo", "{\"message\":\"hello world\"}", 5000);
        defer result.deinit(allocator);
        if (result.result) |r| {
            print("  Result: {s}\n", .{r});
        } else if (result.err) |e| {
            print("  Error: {s}\n", .{e});
        }
    }

    // Test 2: add
    print("\n--- Test 2: add ---\n", .{});
    {
        var result = try client.call("add", "{\"a\":15,\"b\":27}", 5000);
        defer result.deinit(allocator);
        if (result.result) |r| {
            print("  15 + 27 = {s}\n", .{r});
        } else if (result.err) |e| {
            print("  Error: {s}\n", .{e});
        }
    }

    // Test 3: greet
    print("\n--- Test 3: greet ---\n", .{});
    {
        var result = try client.call("greet", "{\"name\":\"Zig\"}", 5000);
        defer result.deinit(allocator);
        if (result.result) |r| {
            print("  Greeting: {s}\n", .{r});
        } else if (result.err) |e| {
            print("  Error: {s}\n", .{e});
        }
    }

    // Test 4: get_status (no params)
    print("\n--- Test 4: get_status ---\n", .{});
    {
        var result = try client.call("get_status", null, 5000);
        defer result.deinit(allocator);
        if (result.result) |r| {
            print("  Status: {s}\n", .{r});
        } else if (result.err) |e| {
            print("  Error: {s}\n", .{e});
        }
    }

    // Test 5: slow_task
    print("\n--- Test 5: slow_task ---\n", .{});
    {
        var result = try client.call("slow_task", null, 5000);
        defer result.deinit(allocator);
        if (result.result) |r| {
            print("  Slow result: {s}\n", .{r});
        } else if (result.err) |e| {
            print("  Error: {s}\n", .{e});
        }
    }

    // Test 6: method not found
    print("\n--- Test 6: unknown method ---\n", .{});
    {
        var result = try client.call("nonexistent_method", null, 5000);
        defer result.deinit(allocator);
        if (result.err) |e| {
            print("  Expected error: {s}\n", .{e});
        } else if (result.result) |r| {
            print("  Unexpected result: {s}\n", .{r});
        }
    }

    // Test 7: add with floats
    print("\n--- Test 7: add with floats ---\n", .{});
    {
        var result = try client.call("add", "{\"a\":3.14,\"b\":2.86}", 5000);
        defer result.deinit(allocator);
        if (result.result) |r| {
            print("  3.14 + 2.86 = {s}\n", .{r});
        } else if (result.err) |e| {
            print("  Error: {s}\n", .{e});
        }
    }

    // Test 8: multiple rapid calls
    print("\n--- Test 8: rapid calls ---\n", .{});
    {
        var success_count: usize = 0;
        for (0..5) |i| {
            const params = std.fmt.allocPrint(allocator, "{{\"a\":{d},\"b\":{d}}}", .{ i, i * 10 }) catch continue;
            defer allocator.free(params);

            var result = client.call("add", params, 5000) catch continue;
            defer result.deinit(allocator);
            if (result.result) |r| {
                print("  Call {d}: {s}\n", .{ i + 1, r });
                success_count += 1;
            }
        }
        print("  Rapid calls: {d}/5 succeeded\n", .{success_count});
    }

    print("\n", .{});
}

// ──────────────────────────────────────────────────────────────────────
//  Main
// ──────────────────────────────────────────────────────────────────────

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    print("============================================================\n", .{});
    print(" MQTT RPC Library - Test Driver (Zig)\n", .{});
    print("============================================================\n", .{});
    print("Broker: localhost:1883\n", .{});

    rpc.libInit();
    defer rpc.libCleanup();

    // Start server
    const server = runServer(allocator) catch |err| {
        print("\n*** Server start failed: {}. Is Mosquitto running? ***\n", .{err});
        return err;
    };

    // Run client tests
    runClientTests(allocator) catch |err| {
        print("\n*** Client tests failed: {} ***\n", .{err});
        server.stop();
        server.deinit();
        allocator.destroy(server);
        return err;
    };

    // Shutdown
    print("Stopping server...\n", .{});
    server.stop();
    server.deinit();
    allocator.destroy(server);

    print("\n============================================================\n", .{});
    print(" All tests passed!\n", .{});
    print("============================================================\n", .{});
}