//! pubsub_test.zig — integration test for mqtt_pubsub
//!
//! Requires a Mosquitto broker on localhost:1883.
//! Build & run:  zig build run-test -Doptimize=ReleaseFast

const std = @import("std");
const pubsub = @import("mqtt_pubsub");

const allocator = std.heap.page_allocator;

// ──────────────────────────────────────────────────────────────────────
//  Method handlers
// ──────────────────────────────────────────────────────────────────────

fn echoHandler(alloc: std.mem.Allocator, params_json: ?[]const u8, _: ?*anyopaque) ?[]const u8 {
    if (params_json) |p| return alloc.dupe(u8, p) catch null;
    return alloc.dupe(u8, "null") catch null;
}

fn addHandler(alloc: std.mem.Allocator, params_json: ?[]const u8, _: ?*anyopaque) ?[]const u8 {
    const p = params_json orelse return null;

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const parsed = std.json.parseFromSlice(std.json.Value, a, p, .{}) catch return null;
    const root = parsed.value;
    if (root != .object) return null;

    const a_val = root.object.get("a") orelse return null;
    const b_val = root.object.get("b") orelse return null;

    const a_num: f64 = switch (a_val) {
        .integer => |i| @floatFromInt(i),
        .float => |f| f,
        else => return null,
    };
    const b_num: f64 = switch (b_val) {
        .integer => |i| @floatFromInt(i),
        .float => |f| f,
        else => return null,
    };

    const sum = a_num + b_num;

    // Return integer if whole number
    if (sum == @trunc(sum) and sum >= -2147483648 and sum <= 2147483647) {
        return std.fmt.allocPrint(alloc, "{d}", .{@as(i64, @intFromFloat(sum))}) catch null;
    }
    return std.fmt.allocPrint(alloc, "{d:.6}", .{sum}) catch null;
}

fn greetHandler(alloc: std.mem.Allocator, params_json: ?[]const u8, _: ?*anyopaque) ?[]const u8 {
    const p = params_json orelse return alloc.dupe(u8, "\"Hello, stranger!\"") catch null;

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const parsed = std.json.parseFromSlice(std.json.Value, a, p, .{}) catch
        return alloc.dupe(u8, "\"Hello, stranger!\"") catch null;

    const name = switch (parsed.value) {
        .object => |obj| blk: {
            const n = obj.get("name") orelse break :blk "stranger";
            break :blk switch (n) {
                .string => |s| s,
                else => "stranger",
            };
        },
        else => "stranger",
    };

    return std.fmt.allocPrint(alloc, "\"Hello, {s}!\"", .{name}) catch null;
}

fn getStatusHandler(alloc: std.mem.Allocator, _: ?[]const u8, _: ?*anyopaque) ?[]const u8 {
    return alloc.dupe(u8, "{\"status\":\"running\",\"uptime_s\":12345}") catch null;
}

fn slowTaskHandler(alloc: std.mem.Allocator, _: ?[]const u8, _: ?*anyopaque) ?[]const u8 {
    std.time.sleep(500 * std.time.ns_per_ms);
    return alloc.dupe(u8, "{\"done\":true,\"elapsed_ms\":500}") catch null;
}

// ──────────────────────────────────────────────────────────────────────
//  Async callback
// ──────────────────────────────────────────────────────────────────────

const AsyncState = struct {
    done: bool = false,
    got_result: bool = false,
    got_error: bool = false,
    mutex: std.Thread.Mutex = .{},
    cond: std.Thread.Condition = .{},
};

fn asyncCallback(error_json: ?[]const u8, result_json: ?[]const u8, userdata: ?*anyopaque) void {
    const state: *AsyncState = @ptrCast(@alignCast(userdata));
    if (result_json) |r| {
        std.debug.print("  Async result: {s}\n", .{r});
    }
    if (error_json) |e| {
        std.debug.print("  Async error: {s}\n", .{e});
    }
    state.mutex.lock();
    defer state.mutex.unlock();
    state.got_result = (result_json != null);
    state.got_error = (error_json != null);
    state.done = true;
    state.cond.signal();
}

// ──────────────────────────────────────────────────────────────────────
//  Main
// ──────────────────────────────────────────────────────────────────────

pub fn main() !void {
    std.debug.print(
        \\
        \\============================================================
        \\ MQTT Pubsub Library - Test Driver (Zig)
        \\============================================================
        \\Broker: localhost:1883
        \\
    , .{});

    pubsub.libInit();
    defer pubsub.libCleanup();

    const cfg = pubsub.Config{
        .service_name = "test_pubsub",
    };

    // ── Start server ─────────────────────────────────────────────────
    std.debug.print("=== Starting Pubsub Server ===\n", .{});
    std.debug.print("Starting server...\n", .{});

    var server = try pubsub.Server.init(cfg, allocator);
    defer server.deinit();

    server.register("echo", echoHandler, null);
    server.register("add", addHandler, null);
    server.register("greet", greetHandler, null);
    server.register("get_status", getStatusHandler, null);
    server.register("slow_task", slowTaskHandler, null);

    try server.start(true, 5000);
    std.debug.print("Server running.\n\n", .{});

    // Let server settle
    std.time.sleep(500 * std.time.ns_per_ms);

    // ── Run client tests ─────────────────────────────────────────────
    std.debug.print("=== Running Pubsub Client Tests ===\n", .{});
    std.debug.print("Connecting client...\n", .{});

    var client = try pubsub.Client.init(cfg, allocator, 10000);
    defer client.deinit();
    try client.connect(5000);

    var pass_count: u32 = 0;
    var fail_count: u32 = 0;

    // Test 1: echo
    {
        std.debug.print("\n--- Test 1: echo ---\n", .{});
        var result = client.call("echo", "{\"hello\":\"world\"}", 5000) catch |err| {
            std.debug.print("*** FAILED: {any}\n", .{err});
            fail_count += 1;
            return err;
        };
        defer result.deinit(allocator);
        if (result.result) |r| {
            std.debug.print("  Result: {s}\n", .{r});
            pass_count += 1;
        } else if (result.err) |e| {
            std.debug.print("  Error: {s}\n", .{e});
            fail_count += 1;
        }
    }

    // Test 2: add integers
    {
        std.debug.print("\n--- Test 2: add integers ---\n", .{});
        var result = try client.call("add", "{\"a\":10,\"b\":32}", 5000);
        defer result.deinit(allocator);
        if (result.result) |r| {
            std.debug.print("  Result: {s}\n", .{r});
            pass_count += 1;
        } else if (result.err) |e| {
            std.debug.print("  Error: {s}\n", .{e});
            fail_count += 1;
        }
    }

    // Test 3: add floats
    {
        std.debug.print("\n--- Test 3: add floats ---\n", .{});
        var result = try client.call("add", "{\"a\":1.5,\"b\":2.7}", 5000);
        defer result.deinit(allocator);
        if (result.result) |r| {
            std.debug.print("  Result: {s}\n", .{r});
            pass_count += 1;
        } else if (result.err) |e| {
            std.debug.print("  Error: {s}\n", .{e});
            fail_count += 1;
        }
    }

    // Test 4: greet with name
    {
        std.debug.print("\n--- Test 4: greet ---\n", .{});
        var result = try client.call("greet", "{\"name\":\"Glenn\"}", 5000);
        defer result.deinit(allocator);
        if (result.result) |r| {
            std.debug.print("  Result: {s}\n", .{r});
            pass_count += 1;
        } else if (result.err) |e| {
            std.debug.print("  Error: {s}\n", .{e});
            fail_count += 1;
        }
    }

    // Test 5: get_status (no params)
    {
        std.debug.print("\n--- Test 5: get_status ---\n", .{});
        var result = try client.call("get_status", null, 5000);
        defer result.deinit(allocator);
        if (result.result) |r| {
            std.debug.print("  Result: {s}\n", .{r});
            pass_count += 1;
        } else if (result.err) |e| {
            std.debug.print("  Error: {s}\n", .{e});
            fail_count += 1;
        }
    }

    // Test 6: slow_task
    {
        std.debug.print("\n--- Test 6: slow_task ---\n", .{});
        var result = try client.call("slow_task", null, 5000);
        defer result.deinit(allocator);
        if (result.result) |r| {
            std.debug.print("  Result: {s}\n", .{r});
            pass_count += 1;
        } else if (result.err) |e| {
            std.debug.print("  Error: {s}\n", .{e});
            fail_count += 1;
        }
    }

    // Test 7: method not found
    {
        std.debug.print("\n--- Test 7: method not found ---\n", .{});
        var result = try client.call("nonexistent", null, 5000);
        defer result.deinit(allocator);
        if (result.err) |e| {
            std.debug.print("  Expected error: {s}\n", .{e});
            pass_count += 1;
        } else {
            std.debug.print("  UNEXPECTED: got result instead of error\n", .{});
            fail_count += 1;
        }
    }

    // Test 8: 5 rapid sequential calls
    {
        std.debug.print("\n--- Test 8: rapid sequential calls ---\n", .{});
        var rapid_ok: u32 = 0;
        for (0..5) |i| {
            const params = std.fmt.allocPrint(allocator, "{{\"a\":{d},\"b\":{d}}}", .{ i, i + 1 }) catch continue;
            defer allocator.free(params);
            var result = client.call("add", params, 5000) catch continue;
            defer result.deinit(allocator);
            if (result.result) |r| {
                std.debug.print("  rapid[{d}]: {s}\n", .{ i, r });
                rapid_ok += 1;
            }
        }
        if (rapid_ok == 5) {
            std.debug.print("  All 5 rapid calls succeeded\n", .{});
            pass_count += 1;
        } else {
            std.debug.print("  Only {d}/5 rapid calls succeeded\n", .{rapid_ok});
            fail_count += 1;
        }
    }

    // Test 9: async call
    {
        std.debug.print("\n--- Test 9: async call ---\n", .{});
        var async_state = AsyncState{};
        client.callAsync("echo", "{\"async\":true}", 5000, asyncCallback, @ptrCast(&async_state));

        // Wait for async completion
        async_state.mutex.lock();
        if (!async_state.done) {
            async_state.cond.timedWait(&async_state.mutex, 10000 * std.time.ns_per_ms) catch {};
        }
        const done = async_state.done;
        const got_result = async_state.got_result;
        async_state.mutex.unlock();

        if (done and got_result) {
            std.debug.print("  Async call completed successfully\n", .{});
            pass_count += 1;
        } else {
            std.debug.print("  Async call failed (done={}, got_result={})\n", .{ done, got_result });
            fail_count += 1;
        }
    }

    // ── Summary ──────────────────────────────────────────────────────
    std.debug.print(
        \\
        \\============================================================
        \\ Results: {d} passed, {d} failed
        \\============================================================
        \\
    , .{ pass_count, fail_count });

    // Cleanup
    client.disconnect();
    server.stop();

    std.time.sleep(500 * std.time.ns_per_ms);

    if (fail_count > 0) {
        std.debug.print("*** SOME TESTS FAILED ***\n", .{});
        std.process.exit(1);
    }
    std.debug.print("All tests passed.\n", .{});
}