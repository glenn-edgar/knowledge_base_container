///  Example program demonstrating the pub_sub_zig library.
///
///  Requires a running NATS server:
///     docker run -p 4222:4222 nats:latest
///
///  Run with:  zig build example

const std = @import("std");
const ps = @import("pub_sub_zig");

const SERVER: [:0]const u8 = "nats://127.0.0.1:4222";

var msg_count: i32 = 0;

fn onMessage(msg: *const ps.Message, _: ?*anyopaque) void {
    msg_count += 1;
    std.debug.print("  [{d}] subject={s}  data={s}\n", .{
        msg_count,
        msg.original_subject,
        msg.data,
    });
}

fn echoHandler(msg: *const ps.Message, user_data: ?*anyopaque) void {
    const client: *ps.PubSub = @ptrCast(@alignCast(user_data.?));
    if (msg.reply_to) |rt| {
        var buf: [256:0]u8 = undefined;
        const len = @min(rt.len, buf.len - 1);
        @memcpy(buf[0..len], rt[0..len]);
        buf[len] = 0;

        var reply_buf: [256:0]u8 = undefined;
        const reply = std.fmt.bufPrintZ(&reply_buf, "echo: {s}", .{msg.data}) catch "echo: ?";
        client.replyStr(buf[0..len :0], reply) catch {};
    }
}

pub fn main() !void {
    const stdout = std.io.getStdOut().writer();

    try stdout.print("\n{s}\n", .{"=" ** 60});
    try stdout.print("  NATS PubSub Zig Demo\n", .{});
    try stdout.print("{s}\n", .{"=" ** 60});

    // ============================================================
    //  1. Basic publish/subscribe
    // ============================================================
    try stdout.print("\n--- 1. Publish / Subscribe ---\n\n", .{});

    var client = try ps.PubSub.init(.{
        .server = SERVER,
        .namespace = "demo",
        .client_name = "zig-demo",
    });
    defer client.deinit();
    try client.connect();

    try stdout.print("  Connected: namespace={s}  client={s}\n", .{
        client.getNamespace(),
        client.clientName(),
    });

    // Subscribe
    var sub = try client.subscribe("sensor.*", onMessage, null, null);
    std.time.sleep(50 * std.time.ns_per_ms);

    // Publish
    try client.publishStr("sensor.temp", "{\"value\":23.5}");
    try client.publishStr("sensor.humidity", "{\"value\":65}");
    try client.publishStr("sensor.pressure", "{\"value\":1013}");
    std.time.sleep(200 * std.time.ns_per_ms);

    try stdout.print("  Received {d} messages via wildcard\n", .{msg_count});
    try client.unsubscribe(&sub);

    // ============================================================
    //  2. Request / Reply
    // ============================================================
    try stdout.print("\n--- 2. Request / Reply ---\n\n", .{});

    var echo_sub = try client.subscribe("service.echo", echoHandler, @ptrCast(&client), null);
    std.time.sleep(50 * std.time.ns_per_ms);

    const reply = try client.requestStr("service.echo", "Hello from Zig!", 5.0);
    defer ps.PubSub.freeReply(reply);
    try stdout.print("  Request: 'Hello from Zig!'  →  Reply: '{s}'\n", .{reply});

    try client.unsubscribe(&echo_sub);

    // ============================================================
    //  3. Statistics
    // ============================================================
    try stdout.print("\n--- 3. Statistics ---\n\n", .{});

    const stats = try client.getStats();
    try stdout.print("  Published: {d}\n", .{stats.msgs_published});
    try stdout.print("  Received:  {d}\n", .{stats.msgs_received});
    try stdout.print("  Active subscriptions: {d}\n", .{stats.active_subscriptions});

    // ============================================================
    //  4. Timeout demo
    // ============================================================
    try stdout.print("\n--- 4. Timeout ---\n\n", .{});

    const timeout_result = client.requestStr("no.responder", "hello?", 0.2);
    if (timeout_result) |_| {
        try stdout.print("  Unexpected reply\n", .{});
    } else |err| {
        try stdout.print("  Request timed out as expected: {any}\n", .{err});
    }

    try stdout.print("\n{s}\n", .{"=" ** 60});
    try stdout.print("  Done.\n", .{});
    try stdout.print("{s}\n\n", .{"=" ** 60});
}