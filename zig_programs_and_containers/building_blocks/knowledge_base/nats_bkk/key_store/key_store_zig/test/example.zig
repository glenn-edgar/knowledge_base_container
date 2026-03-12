///  Example program demonstrating the key_store_zig library.
///
///  Requires a running NATS server with JetStream:
///     docker run -p 4222:4222 nats:latest -js
///
///  Run with:  zig build example

const std = @import("std");
const nats = @import("key_store_zig");

const SERVER: [:0]const u8 = "nats://127.0.0.1:4222";

fn cleanupBucket(ks: *nats.KeyStore) void {
    var kl = ks.keys(null) catch return;
    for (kl.keys) |k| {
        ks.delete(k) catch {};
    }
    kl.deinit();
}

pub fn main() !void {
    const stdout = std.io.getStdOut().writer();

    try stdout.print("\n{s}\n", .{"=" ** 60});
    try stdout.print("  NATS Zig Library Demo\n", .{});
    try stdout.print("{s}\n", .{"=" ** 60});

    // ============================================================
    //  1. KeyStore basics
    // ============================================================
    try stdout.print("\n--- 1. KeyStore ---\n\n", .{});

    var ks = try nats.KeyStore.init(.{
        .server = SERVER,
        .bucket = "demo_zig",
    });
    defer ks.deinit();
    try ks.connect();
    cleanupBucket(&ks);

    // Put / Get
    _ = try ks.put("demo.name", "\"Alice\"");
    {
        const val = try ks.getRaw("demo.name");
        defer nats.KeyStore.freeRaw(val);
        try stdout.print("  demo.name = {s}\n", .{val});
    }

    _ = try ks.put("demo.user", "{\"id\":1,\"name\":\"Bob\",\"age\":30}");
    {
        const val = try ks.getRaw("demo.user");
        defer nats.KeyStore.freeRaw(val);
        try stdout.print("  demo.user = {s}\n", .{val});
    }

    // Counters
    try stdout.print("\n  Counters:\n", .{});
    var cnt = try ks.increment("demo.visits", 1);
    try stdout.print("    visits = {d}\n", .{cnt});
    cnt = try ks.increment("demo.visits", 5);
    try stdout.print("    visits = {d}\n", .{cnt});
    cnt = try ks.decrement("demo.visits", 2);
    try stdout.print("    visits = {d}\n", .{cnt});

    // Key listing
    try stdout.print("\n  Keys matching 'demo.*':\n", .{});
    {
        var kl = try ks.keys("demo.*");
        defer kl.deinit();
        for (kl.keys) |k| {
            try stdout.print("    - {s}\n", .{k});
        }
    }

    cleanupBucket(&ks);

    // ============================================================
    //  2. KbStore (Knowledge Base)
    // ============================================================
    try stdout.print("\n--- 2. KbStore ---\n\n", .{});

    var kb = try nats.KbStore.init(SERVER, "demo_zig_kb", "Demo KB");
    defer kb.deinit();
    {
        var kb_ks = kb.getKeyStore();
        try kb_ks.connect();
        cleanupBucket(&kb_ks);
    }

    // Store
    const label_json = "{\"type\":\"entity\",\"description\":\"Person\",\"category\":\"human\"}";
    const node_json = "{\"id\":\"p001\",\"data\":{\"name\":\"Alice Johnson\",\"age\":30,\"skills\":[\"Zig\",\"C\"]}}";

    var store_result = try kb.store(
        "company.employees",
        "person",
        "alice_johnson",
        label_json,
        node_json,
        true,
    );
    defer store_result.deinit();
    try stdout.print("  Stored: {s}\n", .{store_result.key});

    // Retrieve
    var entry = try kb.get(store_result.key);
    defer entry.deinit();
    try stdout.print("  Label: {s}\n", .{entry.label_json});
    try stdout.print("  Node:  {s}\n", .{entry.node_json});

    // Pop key
    const popped = try nats.KbStore.popKey(store_result.key);
    defer nats.KbStore.freePopKey(popped);
    try stdout.print("  Pop:   {s} -> {s}\n", .{ store_result.key, popped });

    // Validation
    try stdout.print("\n  Key validation:\n", .{});
    const test_keys = [_][:0]const u8{
        "valid.topic.label.node",
        "a.b.c",
    };
    for (test_keys) |tk| {
        try stdout.print("    '{s}' -> {s}\n", .{
            tk,
            if (nats.KbStore.validateKeyFormat(tk)) "valid" else "invalid",
        });
    }

    {
        var kb_ks = kb.getKeyStore();
        cleanupBucket(&kb_ks);
    }

    // ============================================================
    //  3. JobQueue
    // ============================================================
    try stdout.print("\n--- 3. JobQueue ---\n\n", .{});

    var jq_ks = try nats.KeyStore.init(.{
        .server = SERVER,
        .bucket = "demo_zig_jq",
    });
    defer jq_ks.deinit();
    try jq_ks.connect();
    cleanupBucket(&jq_ks);

    var jq = try nats.JobQueue.init(&jq_ks, "demo-worker");
    defer jq.deinit();

    // Submit jobs with different priorities
    try stdout.print("  Submit jobs:\n", .{});
    const submissions = [_]struct { prio: i32, task: [:0]const u8 }{
        .{ .prio = 10, .task = "urgent-task" },
        .{ .prio = 5, .task = "normal-task" },
        .{ .prio = 1, .task = "low-priority" },
        .{ .prio = 8, .task = "high-priority" },
    };

    for (submissions) |sub| {
        var buf: [128]u8 = undefined;
        const payload = std.fmt.bufPrintZ(&buf, "{{\"task\":\"{s}\"}}", .{sub.task}) catch unreachable;
        const jid = try jq.submit(payload, .{
            .queue = "demo",
            .priority = sub.prio,
        });
        try stdout.print("    {s} (priority={d}, id={s}...)\n", .{
            sub.task,
            sub.prio,
            jid[0..@min(8, jid.len)],
        });
        nats.JobQueue.freeJobId(jid);
    }

    // Process in priority order
    try stdout.print("\n  Process in priority order:\n", .{});
    const queues = [_][:0]const u8{"demo"};
    for (0..4) |_| {
        var job = jq.claimJob(&queues) catch break;

        // Build null-terminated copy for API calls
        var id_buf: [64:0]u8 = undefined;
        const jid = job.id();
        @memcpy(id_buf[0..jid.len], jid);
        id_buf[jid.len] = 0;

        try stdout.print("    Processing priority={d}  payload={s}\n", .{
            job.priority(),
            job.payloadJson() orelse "null",
        });
        _ = try jq.completeJob(id_buf[0..jid.len :0], "{\"done\":true}");
        job.deinit();
    }

    // Stats
    try stdout.print("\n  Queue statistics:\n", .{});
    const stats = try jq.getStats("demo");
    try stdout.print("    pending:   {d}\n", .{stats.pending});
    try stdout.print("    running:   {d}\n", .{stats.running});
    try stdout.print("    completed: {d}\n", .{stats.completed});
    try stdout.print("    failed:    {d}\n", .{stats.failed});
    try stdout.print("    cancelled: {d}\n", .{stats.cancelled});

    cleanupBucket(&jq_ks);

    try stdout.print("\n{s}\n", .{"=" ** 60});
    try stdout.print("  Done.\n", .{});
    try stdout.print("{s}\n\n", .{"=" ** 60});
}