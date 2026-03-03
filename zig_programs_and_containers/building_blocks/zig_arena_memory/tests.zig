const std = @import("std");
const testing = std.testing;
const arena_env = @import("arena_env.zig");

// ====================================================================
// Test value types
// ====================================================================

const SimpleValue = struct {
    int_val: i64 = 0,
    float_val: f64 = 0.0,
};

const TextValue = struct {
    name: []const u8,
    count: u32,
};

const TaggedValue = union(enum) {
    integer: i64,
    text: []const u8,
    config: SimpleValue,
};

// ====================================================================
// 1. Basic put/get — write a value, read it back in a read transaction
// ====================================================================
test "basic put get" {
    const Env = arena_env.ServerEnv(SimpleValue);
    var owner = try Env.Owner.init(testing.allocator);
    defer owner.deinit();

    // Write
    {
        var tx = try owner.beginWrite();
        defer tx.commit();
        try tx.put("timeout", .{ .int_val = 1000, .float_val = 3.14 });
        try tx.put("threshold", .{ .int_val = 42, .float_val = 2.718 });
    }

    // Read
    {
        var tx = try owner.beginRead();
        defer tx.commit();

        const val = tx.get("timeout") orelse return error.TestUnexpectedResult;
        try testing.expectEqual(@as(i64, 1000), val.int_val);
        try testing.expectApproxEqAbs(@as(f64, 3.14), val.float_val, 0.001);

        const val2 = tx.get("threshold") orelse return error.TestUnexpectedResult;
        try testing.expectEqual(@as(i64, 42), val2.int_val);

        // Non-existent key
        try testing.expect(tx.get("nonexistent") == null);
    }
}

// ====================================================================
// 2. Transfer — write to owner, transfer, read from new owner,
//    verify original owner returns error
// ====================================================================
test "transfer ownership" {
    const Env = arena_env.ServerEnv(SimpleValue);
    var owner = try Env.Owner.init(testing.allocator);

    // Populate
    {
        var tx = try owner.beginWrite();
        defer tx.commit();
        try tx.put("sensor_rate", .{ .int_val = 500, .float_val = 0.0 });
    }

    // Transfer
    var new_owner = try owner.transfer();
    defer new_owner.deinit();

    // Original owner is inert
    try testing.expectError(error.OwnershipReleased, owner.beginWrite());
    try testing.expectError(error.OwnershipReleased, owner.beginRead());

    // New owner can read the data
    {
        var tx = try new_owner.beginRead();
        defer tx.commit();
        const val = tx.get("sensor_rate") orelse return error.TestUnexpectedResult;
        try testing.expectEqual(@as(i64, 500), val.int_val);
    }
}

// ====================================================================
// 3. Double transfer — verify second transfer returns AlreadyTransferred
// ====================================================================
test "double transfer" {
    const Env = arena_env.ServerEnv(SimpleValue);
    var owner = try Env.Owner.init(testing.allocator);

    var new_owner = try owner.transfer();
    defer new_owner.deinit();

    // Second transfer on the original (now-null) owner
    try testing.expectError(error.AlreadyTransferred, owner.transfer());
}

// ====================================================================
// 4. Deinit safety — deinit owner, verify subsequent operations return error
// ====================================================================
test "deinit safety" {
    const Env = arena_env.ServerEnv(SimpleValue);
    var owner = try Env.Owner.init(testing.allocator);

    // Populate
    {
        var tx = try owner.beginWrite();
        defer tx.commit();
        try tx.put("key", .{ .int_val = 1, .float_val = 0.0 });
    }

    owner.deinit();

    // All operations return error after deinit
    try testing.expectError(error.OwnershipReleased, owner.beginWrite());
    try testing.expectError(error.OwnershipReleased, owner.beginRead());

    // Double deinit is safe (no-op)
    owner.deinit();
}

// ====================================================================
// 5. Embedded config — MicroEnv has no threading overhead
// ====================================================================
test "micro env no threading" {
    const Env = arena_env.MicroEnv(SimpleValue);

    // Comptime verification: no threading
    try testing.expect(!Env.is_threadsafe);
    try testing.expect(!Env.has_concurrent_reads);
    try testing.expect(!Env.interns_strings);

    // Functional test: works without any thread primitives
    var owner = try Env.Owner.init(testing.allocator);
    defer owner.deinit();

    {
        var tx = try owner.beginWrite();
        defer tx.commit();
        try tx.put("valve", .{ .int_val = 1, .float_val = 0.0 });
    }
    {
        var tx = try owner.beginRead();
        defer tx.commit();
        const val = tx.get("valve") orelse return error.TestUnexpectedResult;
        try testing.expectEqual(@as(i64, 1), val.int_val);
    }
}

// ====================================================================
// 6. WriteTx remove
// ====================================================================
test "write tx remove" {
    const Env = arena_env.ServerEnv(SimpleValue);
    var owner = try Env.Owner.init(testing.allocator);
    defer owner.deinit();

    {
        var tx = try owner.beginWrite();
        defer tx.commit();
        try tx.put("ephemeral", .{ .int_val = 99, .float_val = 0.0 });
        try testing.expect(tx.get("ephemeral") != null);

        const removed = tx.remove("ephemeral");
        try testing.expect(removed);
        try testing.expect(tx.get("ephemeral") == null);

        // Removing non-existent key
        try testing.expect(!tx.remove("ghost"));
    }
}

// ====================================================================
// 7. Multi-put — insert 100 symbols, verify all retrievable
// ====================================================================
test "multi put 100 symbols" {
    const Env = arena_env.ServerEnv(SimpleValue);
    var owner = try Env.Owner.init(testing.allocator);
    defer owner.deinit();

    // Write 100 entries
    {
        var tx = try owner.beginWrite();
        defer tx.commit();

        var buf: [32]u8 = undefined;
        for (0..100) |i| {
            const key = std.fmt.bufPrint(&buf, "symbol_{d}", .{i}) catch unreachable;
            try tx.put(key, .{
                .int_val = @as(i64, @intCast(i)),
                .float_val = @as(f64, @floatFromInt(i)) * 1.1,
            });
        }

        try testing.expectEqual(@as(u32, 100), tx.count());
    }

    // Read all 100 back
    {
        var tx = try owner.beginRead();
        defer tx.commit();

        try testing.expectEqual(@as(u32, 100), tx.count());

        var buf: [32]u8 = undefined;
        for (0..100) |i| {
            const key = std.fmt.bufPrint(&buf, "symbol_{d}", .{i}) catch unreachable;
            const val = tx.get(key) orelse {
                std.debug.print("Missing key: {s}\n", .{key});
                return error.TestUnexpectedResult;
            };
            try testing.expectEqual(@as(i64, @intCast(i)), val.int_val);
        }
    }
}

// ====================================================================
// 8. String interning — put a value with text field, verify arena owns it
// ====================================================================
test "string interning" {
    const Env = arena_env.ServerEnv(TextValue);
    var owner = try Env.Owner.init(testing.allocator);
    defer owner.deinit();

    // Create a temporary string buffer on the heap
    const heap_str = try testing.allocator.dupe(u8, "pump_controller_alpha");
    defer testing.allocator.free(heap_str);

    // Write with the heap string as a field value
    {
        var tx = try owner.beginWrite();
        defer tx.commit();
        try tx.put("handler", .{ .name = heap_str, .count = 42 });
    }

    // Overwrite the heap buffer to prove the arena copy is independent
    @memset(heap_str, 'X');

    // Read back — the interned copy should be intact
    {
        var tx = try owner.beginRead();
        defer tx.commit();
        const val = tx.get("handler") orelse return error.TestUnexpectedResult;
        try testing.expectEqualStrings("pump_controller_alpha", val.name);
        try testing.expectEqual(@as(u32, 42), val.count);
    }
}

// ====================================================================
// 9. Tagged union value type
// ====================================================================
test "tagged union value" {
    const Env = arena_env.ServerEnv(TaggedValue);
    var owner = try Env.Owner.init(testing.allocator);
    defer owner.deinit();

    // Write various union variants
    {
        var tx = try owner.beginWrite();
        defer tx.commit();
        try tx.put("timeout_ms", .{ .integer = 5000 });
        try tx.put("node_name", .{ .text = "irrigation_zone_1" });
        try tx.put("defaults", .{ .config = .{ .int_val = 10, .float_val = 0.5 } });
    }

    // Read back and verify discriminants and payloads
    {
        var tx = try owner.beginRead();
        defer tx.commit();

        const t = tx.get("timeout_ms") orelse return error.TestUnexpectedResult;
        try testing.expectEqual(TaggedValue{ .integer = 5000 }, t.*);

        const n = tx.get("node_name") orelse return error.TestUnexpectedResult;
        switch (n.*) {
            .text => |s| try testing.expectEqualStrings("irrigation_zone_1", s),
            else => return error.TestUnexpectedResult,
        }

        const d = tx.get("defaults") orelse return error.TestUnexpectedResult;
        switch (d.*) {
            .config => |c| {
                try testing.expectEqual(@as(i64, 10), c.int_val);
                try testing.expectApproxEqAbs(@as(f64, 0.5), c.float_val, 0.001);
            },
            else => return error.TestUnexpectedResult,
        }
    }
}

// ====================================================================
// 10. Tagged union string interning
// ====================================================================
test "tagged union string interning" {
    const Env = arena_env.ServerEnv(TaggedValue);
    var owner = try Env.Owner.init(testing.allocator);
    defer owner.deinit();

    const heap_str = try testing.allocator.dupe(u8, "nats_subject_pump");
    defer testing.allocator.free(heap_str);

    {
        var tx = try owner.beginWrite();
        defer tx.commit();
        try tx.put("subject", .{ .text = heap_str });
    }

    // Corrupt the original
    @memset(heap_str, 0);

    {
        var tx = try owner.beginRead();
        defer tx.commit();
        const val = tx.get("subject") orelse return error.TestUnexpectedResult;
        switch (val.*) {
            .text => |s| try testing.expectEqualStrings("nats_subject_pump", s),
            else => return error.TestUnexpectedResult,
        }
    }
}

// ====================================================================
// 11. SmallEnv — threadsafe, no concurrent reads
// ====================================================================
test "small env config" {
    const Env = arena_env.SmallEnv(SimpleValue);

    try testing.expect(Env.is_threadsafe);
    try testing.expect(!Env.has_concurrent_reads);
    try testing.expect(Env.interns_strings);

    var owner = try Env.Owner.init(testing.allocator);
    defer owner.deinit();

    {
        var tx = try owner.beginWrite();
        defer tx.commit();
        try tx.put("rtos_task", .{ .int_val = 7, .float_val = 0.0 });
    }
    {
        var tx = try owner.beginRead();
        defer tx.commit();
        const val = tx.get("rtos_task") orelse return error.TestUnexpectedResult;
        try testing.expectEqual(@as(i64, 7), val.int_val);
    }
}

// ====================================================================
// 12. Custom allocator type parameter
// ====================================================================
test "custom allocator type" {
    // Use a concrete GPA pointer as the Allocator type parameter
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const Env = arena_env.ConfiguredEnvironment(.{
        .Value = SimpleValue,
        .Allocator = *std.heap.GeneralPurposeAllocator(.{}),
        .threadsafe = true,
        .concurrent_reads = true,
        .initial_capacity = 32,
        .intern_strings = false,
    });

    var owner = try Env.Owner.init(&gpa);
    defer owner.deinit();

    {
        var tx = try owner.beginWrite();
        defer tx.commit();
        try tx.put("gpa_test", .{ .int_val = 999, .float_val = 1.23 });
    }
    {
        var tx = try owner.beginRead();
        defer tx.commit();
        const val = tx.get("gpa_test") orelse return error.TestUnexpectedResult;
        try testing.expectEqual(@as(i64, 999), val.int_val);
    }
}

// ====================================================================
// 13. Write transaction mutation through pointer
// ====================================================================
test "write tx mutate value" {
    const Env = arena_env.ServerEnv(SimpleValue);
    var owner = try Env.Owner.init(testing.allocator);
    defer owner.deinit();

    {
        var tx = try owner.beginWrite();
        defer tx.commit();
        try tx.put("counter", .{ .int_val = 0, .float_val = 0.0 });

        // Mutate through the returned pointer
        const ptr = tx.get("counter") orelse return error.TestUnexpectedResult;
        ptr.int_val = 42;
    }

    {
        var tx = try owner.beginRead();
        defer tx.commit();
        const val = tx.get("counter") orelse return error.TestUnexpectedResult;
        try testing.expectEqual(@as(i64, 42), val.int_val);
    }
}

// ====================================================================
// 14. Transfer then write on new owner
// ====================================================================
test "transfer then extend" {
    const Env = arena_env.ServerEnv(SimpleValue);
    var owner = try Env.Owner.init(testing.allocator);

    {
        var tx = try owner.beginWrite();
        defer tx.commit();
        try tx.put("init_key", .{ .int_val = 1, .float_val = 0.0 });
    }

    var runtime_owner = try owner.transfer();
    defer runtime_owner.deinit();

    // New owner can also write
    {
        var tx = try runtime_owner.beginWrite();
        defer tx.commit();
        try tx.put("runtime_key", .{ .int_val = 2, .float_val = 0.0 });
    }

    {
        var tx = try runtime_owner.beginRead();
        defer tx.commit();
        try testing.expect(tx.contains("init_key"));
        try testing.expect(tx.contains("runtime_key"));
        try testing.expectEqual(@as(u32, 2), tx.count());
    }
}

// ====================================================================
// 15. Concurrent readers — deinit waits for readers to drain
// ====================================================================
test "concurrent readers with deinit" {
    const Env = arena_env.ServerEnv(SimpleValue);
    var owner = try Env.Owner.init(testing.allocator);

    // Populate
    {
        var tx = try owner.beginWrite();
        defer tx.commit();
        for (0..50) |i| {
            var buf: [32]u8 = undefined;
            const key = std.fmt.bufPrint(&buf, "k_{d}", .{i}) catch unreachable;
            try tx.put(key, .{ .int_val = @as(i64, @intCast(i)), .float_val = 0.0 });
        }
    }

    // Transfer to a shared owner for the reader threads
    var shared_owner = try owner.transfer();

    // Spawn reader threads that hold read transactions for a while
    const num_readers = 4;
    var threads: [num_readers]std.Thread = undefined;
    var read_counts = [_]std.atomic.Value(u32){
        std.atomic.Value(u32).init(0),
        std.atomic.Value(u32).init(0),
        std.atomic.Value(u32).init(0),
        std.atomic.Value(u32).init(0),
    };

    for (0..num_readers) |i| {
        const Context = struct {
            owner_ptr: *Env.Owner,
            counter: *std.atomic.Value(u32),
        };
        const ctx = Context{
            .owner_ptr = &shared_owner,
            .counter = &read_counts[i],
        };
        threads[i] = try std.Thread.spawn(.{}, struct {
            fn run(c: Context) void {
                // Perform multiple read transactions
                for (0..20) |_| {
                    var tx = c.owner_ptr.beginRead() catch return;
                    defer tx.commit();
                    // Do some reads
                    for (0..50) |j| {
                        var buf: [32]u8 = undefined;
                        const key = std.fmt.bufPrint(&buf, "k_{d}", .{j}) catch unreachable;
                        _ = tx.get(key);
                    }
                    _ = c.counter.fetchAdd(1, .release);
                }
            }
        }.run, .{ctx});
    }

    // Let readers start, then deinit — should block until all readers finish
    std.time.sleep(1 * std.time.ns_per_ms);
    shared_owner.deinit();

    // Join all threads
    for (&threads) |*t| t.join();

    // Verify readers actually ran (at least some iterations before deinit killed it)
    var total: u32 = 0;
    for (&read_counts) |*c| total += c.load(.acquire);
    try testing.expect(total > 0);
}