///  Integration tests for pub_sub_zig.
///  Requires a running NATS server at 127.0.0.1:4222:
///
///     docker run -p 4222:4222 nats:latest
///
///  Run with:  zig build integration

const std = @import("std");
const ps = @import("pub_sub_zig");

const TEST_SERVER: [:0]const u8 = "nats://127.0.0.1:4222";

// ================================================================
//  Publish / Subscribe
// ================================================================

const TestState = struct {
    received: bool = false,
    data: [256]u8 = undefined,
    data_len: usize = 0,
    subject: [256]u8 = undefined,
    subject_len: usize = 0,
};

fn testCallback(msg: *const ps.Message, user_data: ?*anyopaque) void {
    const state: *TestState = @ptrCast(@alignCast(user_data.?));
    state.received = true;
    const len = @min(msg.data.len, state.data.len);
    @memcpy(state.data[0..len], msg.data[0..len]);
    state.data_len = len;
    const slen = @min(msg.original_subject.len, state.subject.len);
    @memcpy(state.subject[0..slen], msg.original_subject[0..slen]);
    state.subject_len = slen;
}

test "pubsub: connect and disconnect" {
    var client = try ps.PubSub.init(.{
        .server = TEST_SERVER,
        .namespace = "test_zig",
        .client_name = "test-client",
    });
    defer client.deinit();

    try client.connect();
    try std.testing.expect(client.isConnected());
    try std.testing.expectEqualStrings("test_zig", client.getNamespace());
    try std.testing.expectEqualStrings("test-client", client.clientName());

    try client.disconnect();
    try std.testing.expect(!client.isConnected());
}

test "pubsub: publish and subscribe" {
    var client = try ps.PubSub.init(.{
        .server = TEST_SERVER,
        .namespace = "test_zig_ps",
    });
    defer client.deinit();
    try client.connect();

    var state = TestState{};
    var sub = try client.subscribe("sensor.temp", testCallback, @ptrCast(&state), null);

    // Small delay for subscription to be active
    std.time.sleep(50 * std.time.ns_per_ms);

    try client.publishStr("sensor.temp", "{\"value\":23.5}");

    // Wait for callback
    std.time.sleep(100 * std.time.ns_per_ms);

    try std.testing.expect(state.received);
    try std.testing.expect(state.data_len > 0);
    try std.testing.expectEqualStrings("sensor.temp", state.subject[0..state.subject_len]);

    const data_str = state.data[0..state.data_len];
    try std.testing.expect(std.mem.indexOf(u8, data_str, "23.5") != null);

    try client.unsubscribe(&sub);
}

test "pubsub: publish raw bytes" {
    var client = try ps.PubSub.init(.{
        .server = TEST_SERVER,
        .namespace = "test_zig_raw",
    });
    defer client.deinit();
    try client.connect();

    var state = TestState{};
    var sub = try client.subscribe("binary", testCallback, @ptrCast(&state), null);
    std.time.sleep(50 * std.time.ns_per_ms);

    const bytes = [_]u8{ 0x01, 0x02, 0x03, 0xFF };
    try client.publish("binary", &bytes);
    std.time.sleep(100 * std.time.ns_per_ms);

    try std.testing.expect(state.received);
    try std.testing.expectEqual(@as(usize, 4), state.data_len);
    try std.testing.expectEqual(@as(u8, 0x01), state.data[0]);
    try std.testing.expectEqual(@as(u8, 0xFF), state.data[3]);

    try client.unsubscribe(&sub);
}

test "pubsub: wildcard subscription" {
    var client = try ps.PubSub.init(.{
        .server = TEST_SERVER,
        .namespace = "test_zig_wild",
    });
    defer client.deinit();
    try client.connect();

    var state = TestState{};
    var sub = try client.subscribe("sensor.*", testCallback, @ptrCast(&state), null);
    std.time.sleep(50 * std.time.ns_per_ms);

    try client.publishStr("sensor.humidity", "88");
    std.time.sleep(100 * std.time.ns_per_ms);

    try std.testing.expect(state.received);

    try client.unsubscribe(&sub);
}

test "pubsub: request/reply" {
    var client = try ps.PubSub.init(.{
        .server = TEST_SERVER,
        .namespace = "test_zig_rr",
    });
    defer client.deinit();
    try client.connect();

    // Set up a responder
    const Responder = struct {
        fn callback(msg: *const ps.Message, user_data: ?*anyopaque) void {
            const self: *ps.PubSub = @ptrCast(@alignCast(user_data.?));
            if (msg.reply_to) |rt| {
                // Need to make a sentinel-terminated copy
                var buf: [256:0]u8 = undefined;
                const len = @min(rt.len, buf.len - 1);
                @memcpy(buf[0..len], rt[0..len]);
                buf[len] = 0;
                self.replyStr(buf[0..len :0], "pong") catch {};
            }
        }
    };

    var sub = try client.subscribe("service.echo", Responder.callback, @ptrCast(&client), null);
    std.time.sleep(50 * std.time.ns_per_ms);

    const reply = try client.requestStr("service.echo", "ping", 5.0);
    defer ps.PubSub.freeReply(reply);

    try std.testing.expectEqualStrings("pong", reply);

    try client.unsubscribe(&sub);
}

test "pubsub: stats" {
    var client = try ps.PubSub.init(.{
        .server = TEST_SERVER,
        .namespace = "test_zig_stats",
    });
    defer client.deinit();
    try client.connect();

    try client.publishStr("test.stats", "one");
    try client.publishStr("test.stats", "two");

    const stats = try client.getStats();
    try std.testing.expect(stats.msgs_published >= 2);
}

test "pubsub: request timeout" {
    var client = try ps.PubSub.init(.{
        .server = TEST_SERVER,
        .namespace = "test_zig_timeout",
    });
    defer client.deinit();
    try client.connect();

    const result = client.requestStr("no.responder", "hello", 0.2);
    try std.testing.expectError(ps.Error.Timeout, result);
}

test "pubsub: not connected error" {
    var client = try ps.PubSub.init(.{
        .server = TEST_SERVER,
    });
    defer client.deinit();

    // Don't connect — should get NotConnected
    const result = client.publishStr("test", "data");
    try std.testing.expectError(ps.Error.NotConnected, result);
}