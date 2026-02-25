//! kv_store_test.zig - Test driver for the MQTT KV Store library.
//!
//! Uses the writer API to populate the broker with retained test data,
//! then uses the reader API to exercise all read modes (pattern, single,
//! wildcard, sentinel-based).
//!
//! Prerequisites:
//!   - Mosquitto broker running on localhost:1883

const std = @import("std");
const kv = @import("mqtt_kv_store");

fn print(comptime fmt: []const u8, args: anytype) void {
    const stdout = std.io.getStdOut().writer();
    stdout.print(fmt, args) catch {};
}

fn sleepMs(ms: u64) void {
    std.time.sleep(ms * std.time.ns_per_ms);
}

// ──────────────────────────────────────────────────────────────────────
//  Test data
// ──────────────────────────────────────────────────────────────────────

const TestKV = struct {
    topic: [:0]const u8,
    value: [:0]const u8,
};

const test_data = [_]TestKV{
    // Configuration
    .{ .topic = "kv/example/config/host", .value = "192.168.1.100" },
    .{ .topic = "kv/example/config/port", .value = "8080" },
    .{ .topic = "kv/example/config/enabled", .value = "true" },
    .{ .topic = "kv/example/config/timeout", .value = "30" },
    .{ .topic = "kv/example/config/retry_count", .value = "3" },

    // Status
    .{ .topic = "kv/example/status/uptime", .value = "3600" },
    .{ .topic = "kv/example/status/connections", .value = "42" },
    .{ .topic = "kv/example/status/last_error", .value = "none" },
    .{ .topic = "kv/example/status/cpu_usage", .value = "15.7" },

    // System
    .{ .topic = "kv/system/version", .value = "1.2.3" },
    .{ .topic = "kv/system/build", .value = "2024.12.20" },
    .{ .topic = "kv/system/hostname", .value = "mqtt-server-01" },
    .{ .topic = "kv/system/os", .value = "Linux 5.15.0" },

    // Application
    .{ .topic = "kv/app/users/count", .value = "1250" },
    .{ .topic = "kv/app/users/active", .value = "523" },
    .{ .topic = "kv/app/database/connected", .value = "true" },
    .{ .topic = "kv/app/database/pool_size", .value = "10" },

    // Sensors
    .{ .topic = "kv/sensors/temperature/living_room", .value = "22.5" },
    .{ .topic = "kv/sensors/temperature/bedroom", .value = "20.1" },
    .{ .topic = "kv/sensors/humidity/living_room", .value = "45" },
    .{ .topic = "kv/sensors/humidity/bedroom", .value = "50" },

    // Sentinels
    .{ .topic = "kv/example/.sentinel", .value = "done" },
    .{ .topic = "kv/example/config/.sentinel", .value = "done" },
    .{ .topic = "kv/sensors/.sentinel/1", .value = "done" },
    .{ .topic = "kv/app/.sentinel/1", .value = "done" },
    .{ .topic = "kv/.sentinel", .value = "done" },
};

// ──────────────────────────────────────────────────────────────────────
//  Write test data
// ──────────────────────────────────────────────────────────────────────

fn writeTestData() !void {
    print("=== Writing Test Data via KVStoreWriter API ===\n\n", .{});

    const cfg = kv.WriterConfig{
        .client_id = "kv-test-writer",
    };

    var writer = kv.Writer.init(cfg) catch {
        print("Failed to initialise writer\n", .{});
        return error.InitFailed;
    };
    defer writer.deinit();

    print("Connecting writer to localhost:1883...\n", .{});
    writer.connect(5000) catch {
        print("Writer connect failed\n", .{});
        return error.ConnectFailed;
    };

    print("Publishing {d} retained messages...\n", .{test_data.len});
    var success_count: usize = 0;

    for (test_data) |item| {
        if (writer.writeSingle(item.topic, item.value, .at_least_once, true, 5000)) {
            success_count += 1;
        } else {
            print("  x Failed: {s}\n", .{item.topic});
        }
    }

    print("\nPublished {d}/{d} messages\n", .{ success_count, test_data.len });

    writer.disconnect();
    print("Writer disconnected\n\n", .{});
}

// ──────────────────────────────────────────────────────────────────────
//  Demonstrate reader
// ──────────────────────────────────────────────────────────────────────

fn demonstrateReader() !void {
    print("=== Demonstrating KVStoreReader API ===\n\n", .{});

    // 1. Create reader
    print("1. Creating KVStoreReader instance...\n", .{});
    const cfg = kv.ReaderConfig{
        .client_id = "kv-test-reader",
    };

    var reader = kv.Reader.init(cfg) catch {
        print("Failed to initialise reader\n", .{});
        return error.InitFailed;
    };
    defer reader.deinit();

    // 2. Connect
    print("\n2. Testing connection to broker...\n", .{});
    reader.connect(5000) catch {
        print("Failed to connect to broker\n", .{});
        return error.ConnectFailed;
    };
    print("  Connection status: {s}\n", .{if (reader.isConnected()) "true" else "false"});

    var entries: [kv.MAX_ENTRIES]kv.Entry = undefined;
    for (&entries) |*e| e.* = kv.Entry{};

    // 3. Read all under kv/example/# with sentinel
    print("\n3. Reading all values under 'kv/example/#' (wildcards):\n", .{});
    print("--------------------------------------------------\n", .{});
    {
        const sents = [_][:0]const u8{"kv/example/.sentinel"};
        const n = reader.readPattern("kv/example/#", 1, 2000, &sents, true, &entries);
        if (n > 0) {
            print("Found {d} entries:\n", .{n});
            for (0..n) |i| {
                const topic = entries[i].topicSlice();
                const val = entries[i].valueSlice();
                const rest = if (topic.len > 11) topic[11..] else topic;
                print("  [{s}]: {s}\n", .{ rest, val });
            }
        } else {
            print("  No retained messages found under kv/example/#\n", .{});
        }
    }

    // 4. Single-level wildcard kv/example/config/+
    print("\n4. Reading config values with 'kv/example/config/+' (single-level wildcard):\n", .{});
    print("--------------------------------------------------\n", .{});
    {
        const sents = [_][:0]const u8{"kv/example/config/.sentinel"};
        const n = reader.readPattern("kv/example/config/+", 1, 2000, &sents, true, &entries);
        if (n > 0) {
            print("Configuration parameters:\n", .{});
            for (0..n) |i| {
                const topic = entries[i].topicSlice();
                const val = entries[i].valueSlice();
                // Find last '/'
                var param = topic;
                if (std.mem.lastIndexOfScalar(u8, topic, '/')) |pos| {
                    param = topic[pos + 1 ..];
                }
                print("  {s} = {s}\n", .{ param, val });
            }
        } else {
            print("  No config values found\n", .{});
        }
    }

    // 5. Read single value
    print("\n5. Reading single value 'kv/system/version' (exact topic):\n", .{});
    print("--------------------------------------------------\n", .{});
    {
        var val_buf: [kv.MAX_VALUE_LEN]u8 = undefined;
        if (reader.readSingle("kv/system/version", 1000, &val_buf)) {
            const val = std.mem.sliceTo(&val_buf, 0);
            print("  System version: {s}\n", .{val});
        } else {
            print("  Version not found\n", .{});
        }
        if (reader.readSingle("kv/system/build", 1000, &val_buf)) {
            const val = std.mem.sliceTo(&val_buf, 0);
            print("  Build date: {s}\n", .{val});
        }
        if (reader.readSingle("kv/system/hostname", 1000, &val_buf)) {
            const val = std.mem.sliceTo(&val_buf, 0);
            print("  Hostname: {s}\n", .{val});
        }
    }

    // 6. Multiple wildcards kv/sensors/+/+
    print("\n6. Reading sensor data 'kv/sensors/+/+' (multiple wildcards):\n", .{});
    print("--------------------------------------------------\n", .{});
    {
        const sents = [_][:0]const u8{"kv/sensors/.sentinel/1"};
        const n = reader.readPattern("kv/sensors/+/+", 1, 2000, &sents, true, &entries);
        if (n > 0) {
            for (0..n) |i| {
                const topic = entries[i].topicSlice();
                const val = entries[i].valueSlice();
                // Parse type/location from "kv/sensors/TYPE/LOCATION"
                if (topic.len > 11) {
                    const rest = topic[11..]; // skip "kv/sensors/"
                    print("  {s}: {s}\n", .{ rest, val });
                }
            }
        } else {
            print("  No sensor data found\n", .{});
        }
    }

    // 7. ALL retained messages
    print("\n7. Reading ALL retained messages on broker with '#':\n", .{});
    print("--------------------------------------------------\n", .{});
    {
        const sents = [_][:0]const u8{"kv/.sentinel"};
        const n = reader.readAll("#", 2000, &sents, true, &entries);
        if (n > 0) {
            print("Total retained messages on broker: {d}\n", .{n});
            print("\nFirst 5 messages:\n", .{});
            const show = @min(n, 5);
            for (0..show) |i| {
                print("  {s} = {s}\n", .{ entries[i].topicSlice(), entries[i].valueSlice() });
            }
        } else {
            print("  No retained messages found on broker\n", .{});
        }
    }

    // 8. App metrics
    print("\n8. Reading application metrics 'kv/app/+/+':\n", .{});
    print("--------------------------------------------------\n", .{});
    {
        const sents = [_][:0]const u8{"kv/app/.sentinel/1"};
        const n = reader.readPattern("kv/app/+/+", 1, 2000, &sents, true, &entries);
        if (n > 0) {
            print("Application metrics:\n", .{});
            for (0..n) |i| {
                const topic = entries[i].topicSlice();
                const val = entries[i].valueSlice();
                if (topic.len > 7) {
                    print("  {s} = {s}\n", .{ topic[7..], val }); // skip "kv/app/"
                }
            }
        } else {
            print("  No application metrics found\n", .{});
        }
    }

    print("\n* Demonstration completed successfully!\n", .{});

    // 9. Cleanup
    print("\n9. Cleaning up...\n", .{});
    print("  Final connection status: {s}\n", .{if (reader.isConnected()) "true" else "false"});
    reader.disconnect();
    print("  Reader disconnected\n", .{});
}

// ──────────────────────────────────────────────────────────────────────
//  Main
// ──────────────────────────────────────────────────────────────────────

pub fn main() !void {
    print("============================================================\n", .{});
    print(" MQTT KV Store - Unified Library Test (Zig)\n", .{});
    print("============================================================\n\n", .{});

    kv.libInit();
    defer kv.libCleanup();

    // Step 1: Write test data
    writeTestData() catch |err| {
        print("Failed to write test data: {}. Exiting.\n", .{err});
        return err;
    };

    // Small delay
    print("Waiting for messages to settle...\n\n", .{});
    sleepMs(1000);

    // Step 2: Read back
    demonstrateReader() catch |err| {
        print("\nReader demonstration failed: {}\n", .{err});
        return err;
    };

    print("\n============================================================\n", .{});
    print(" Test completed! All connections closed.\n", .{});
    print("============================================================\n", .{});
}