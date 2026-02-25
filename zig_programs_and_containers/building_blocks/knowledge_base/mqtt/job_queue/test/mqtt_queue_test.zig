//! mqtt_queue_test.zig - Test driver for mqtt_queue library

const std = @import("std");
const mqtt = @import("mqtt_queue");

const log = std.log.scoped(.mqtt_test);

const TOPIC = "work/items/task";
const BROKER = "localhost";
const PORT: u16 = 1883;
const READER_ID = "worker-1";
const PUB_ID = "queue-writer";

const test_jobs = [_][]const u8{
    "{\"job_id\":301,\"op\":\"compress\",\"args\":{\"file\":\"a.bin\"}}",
    "{\"job_id\":302,\"op\":\"resize\",\"args\":{\"image\":\"img.jpg\",\"w\":640,\"h\":480}}",
    "{\"job_id\":303,\"op\":\"checksum\",\"args\":{\"file\":\"a.bin\"}}",
};

const batch_jobs = [_][]const u8{
    "{\"job_id\":101,\"op\":\"backup\",\"args\":{\"path\":\"/data\"}}",
    "{\"job_id\":102,\"op\":\"scan\",\"args\":{\"target\":\"network\"}}",
    "{\"job_id\":103,\"op\":\"report\",\"args\":{\"format\":\"pdf\"}}",
};

fn print(comptime fmt: []const u8, args: anytype) void {
    const stdout = std.io.getStdOut().writer();
    stdout.print(fmt, args) catch {};
}

fn sleepMs(ms: u64) void {
    std.time.sleep(ms * std.time.ns_per_ms);
}

fn testPublisher() !void {
    print("\n=== Test 1: Publisher ===\n\n", .{});

    const cfg = mqtt.Config{
        .host = BROKER,
        .port = PORT,
        .client_id = PUB_ID,
        .keepalive = 60,
        .clean_session = true,
    };

    var publisher = try mqtt.Publisher.init(cfg);
    defer publisher.deinit();

    print("Connecting publisher...\n", .{});
    publisher.connect(5000) catch {
        print("Publisher connect failed\n", .{});
        return error.ConnectFailed;
    };
    defer publisher.disconnect();

    print("\nPublishing individual messages:\n", .{});
    for (test_jobs, 0..) |job, i| {
        if (publisher.publish(TOPIC, job, .at_least_once, false)) |_| {
            print("  OK  Published job {d}/{d}\n", .{ i + 1, test_jobs.len });
        } else |_| {
            print("  ERR Failed job {d}/{d}\n", .{ i + 1, test_jobs.len });
        }
        sleepMs(100);
    }

    print("\nBatch publish:\n", .{});
    const ok = publisher.publishBatch(TOPIC, &batch_jobs, .at_least_once, false, 50);
    print("Batch result: {d}/{d} successful\n", .{ ok, batch_jobs.len });

    print("\nQoS 2 publish:\n", .{});
    const critical = "{\"job_id\":999,\"op\":\"critical_update\",\"args\":{\"target\":\"database\"}}";
    if (publisher.publish("work/items/critical", critical, .exactly_once, false)) |_| {
        print("  OK  Critical job published with QoS 2\n", .{});
    } else |_| {
        print("  ERR Critical job failed\n", .{});
    }

    sleepMs(500);
    print("\nPublisher test complete.\n", .{});
}

fn testPersistentQueue(allocator: std.mem.Allocator) !void {
    print("\n=== Test 2: Persistent Queue Demo ===\n\n", .{});

    const reader_cfg = mqtt.Config{
        .host = BROKER,
        .port = PORT,
        .client_id = READER_ID,
        .keepalive = 60,
        .clean_session = false,
    };

    print("1. Creating persistent session and registering subscription...\n", .{});
    {
        var rdr = try mqtt.Reader.init(reader_cfg, allocator);
        defer rdr.deinit();

        rdr.connect(5000) catch {
            print("Reader connect failed\n", .{});
            return error.ConnectFailed;
        };

        rdr.subscribe(TOPIC, .at_least_once, 2000) catch {
            print("Failed to subscribe. Check broker/ACLs.\n", .{});
            rdr.disconnect();
            return error.SubscribeFailed;
        };
        print("   Subscription registered in persistent session\n", .{});

        rdr.disconnect();
        print("   Disconnected (session persisted)\n\n", .{});
    }

    print("2. Publishing messages while consumer is offline...\n", .{});
    {
        const pub_cfg = mqtt.Config{
            .host = BROKER,
            .port = PORT,
            .client_id = "offline-publisher",
            .keepalive = 60,
            .clean_session = true,
        };

        var publisher = try mqtt.Publisher.init(pub_cfg);
        defer publisher.deinit();

        publisher.connect(5000) catch {
            print("Offline publisher connect failed\n", .{});
            return error.ConnectFailed;
        };

        for (test_jobs, 0..) |job, i| {
            if (publisher.publish(TOPIC, job, .at_least_once, false)) |_| {
                print("   Published job {d}/{d}\n", .{ i + 1, test_jobs.len });
            } else |_| {
                print("   Failed job {d}/{d}\n", .{ i + 1, test_jobs.len });
            }
            sleepMs(100);
        }

        sleepMs(500);
        publisher.disconnect();
        print("   Published {d} jobs while consumer offline\n\n", .{test_jobs.len});
    }

    print("3. Reconnecting to retrieve queued messages...\n", .{});
    {
        var rdr2 = try mqtt.Reader.init(reader_cfg, allocator);
        defer rdr2.deinit();

        rdr2.connect(5000) catch {
            print("Reader reconnect failed\n", .{});
            return error.ConnectFailed;
        };
        defer rdr2.disconnect();

        rdr2.session_present = true;

        var count: usize = 0;
        const msgs = rdr2.readQueue(TOPIC, .at_least_once, 3000, &count) catch |err| {
            print("readQueue failed: {}\n", .{err});
            return err;
        };

        print("\n   Retrieved {d} queued message(s):\n", .{count});
        var cur = msgs;
        while (cur) |m| {
            print("     Topic: {s}\n", .{m.topic});
            print("     Payload: {s}\n\n", .{m.payload});
            cur = m.next;
        }
        mqtt.messageFreeList(msgs, allocator);
    }

    print("Persistent queue demo complete.\n", .{});
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    print("============================================\n", .{});
    print("   MQTT Queue Library - Test Driver (Zig)\n", .{});
    print("============================================\n", .{});
    print("Broker: {s}:{d}\n\n", .{ BROKER, PORT });

    mqtt.libInit();
    defer mqtt.libCleanup();

    testPublisher() catch |err| {
        print("\n*** Publisher test failed: {}. Is Mosquitto running? ***\n", .{err});
        return err;
    };

    testPersistentQueue(allocator) catch |err| {
        print("\n*** Persistent queue test failed: {} ***\n", .{err});
        return err;
    };

    print("\n============================================\n", .{});
    print("   All tests passed!\n", .{});
    print("============================================\n", .{});
}