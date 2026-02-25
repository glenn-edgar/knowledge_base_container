///  Integration tests for key_store_zig.
///  Requires a running NATS server with JetStream at 127.0.0.1:4222:
///
///     docker run -p 4222:4222 nats:latest -js
///
///  Run with:  zig build integration

const std = @import("std");
const nats = @import("key_store_zig");

const TEST_SERVER: [:0]const u8 = "nats://127.0.0.1:4222";

// ----------------------------------------------------------------
//  Helper
// ----------------------------------------------------------------

fn cleanupBucket(ks: *nats.KeyStore) void {
    var kl = ks.keys(null) catch return;
    for (kl.keys) |k| {
        ks.delete(k) catch {};
    }
    kl.deinit();
}

fn makeKs(bucket: [:0]const u8) !nats.KeyStore {
    return nats.KeyStore.init(.{
        .server = TEST_SERVER,
        .bucket = bucket,
    });
}

// ================================================================
//  KeyStore tests
// ================================================================

test "ks: put and get string" {
    var ks = try makeKs("test_zig_ks");
    defer ks.deinit();
    try ks.connect();
    cleanupBucket(&ks);

    const rev = try ks.put("test.string", "\"Hello from Zig!\"");
    try std.testing.expect(rev > 0);

    const val = try ks.getRaw("test.string");
    defer nats.KeyStore.freeRaw(val);
    try std.testing.expect(std.mem.indexOf(u8, val, "Hello from Zig!") != null);

    cleanupBucket(&ks);
}

test "ks: put and get JSON" {
    var ks = try makeKs("test_zig_ks");
    defer ks.deinit();
    try ks.connect();
    cleanupBucket(&ks);

    try ks.putNoRev("test.json", "{\"name\":\"Alice\",\"age\":30}");

    const val = try ks.getRaw("test.json");
    defer nats.KeyStore.freeRaw(val);
    try std.testing.expect(std.mem.indexOf(u8, val, "Alice") != null);

    cleanupBucket(&ks);
}

test "ks: delete" {
    var ks = try makeKs("test_zig_ks");
    defer ks.deinit();
    try ks.connect();
    cleanupBucket(&ks);

    try ks.putNoRev("test.del", "\"temp\"");
    try std.testing.expect(try ks.exists("test.del"));

    try ks.delete("test.del");
    try std.testing.expect(!try ks.exists("test.del"));

    const result = ks.getRaw("test.del");
    try std.testing.expectError(nats.Error.NotFound, result);

    cleanupBucket(&ks);
}

test "ks: exists" {
    var ks = try makeKs("test_zig_ks");
    defer ks.deinit();
    try ks.connect();
    cleanupBucket(&ks);

    try std.testing.expect(!try ks.exists("test.no_such_key"));
    try ks.putNoRev("test.exists", "\"yes\"");
    try std.testing.expect(try ks.exists("test.exists"));

    cleanupBucket(&ks);
}

test "ks: keys with pattern" {
    var ks = try makeKs("test_zig_ks_keys");
    defer ks.deinit();
    try ks.connect();
    cleanupBucket(&ks);

    try ks.putNoRev("test.user.1", "\"Alice\"");
    try ks.putNoRev("test.user.2", "\"Bob\"");
    try ks.putNoRev("test.admin.1", "\"Charlie\"");
    try ks.putNoRev("test.config", "\"settings\"");

    var kl = try ks.keys("test.user.*");
    defer kl.deinit();
    try std.testing.expectEqual(@as(usize, 2), kl.count);

    cleanupBucket(&ks);
}

test "ks: increment" {
    var ks = try makeKs("test_zig_ks_inc");
    defer ks.deinit();
    try ks.connect();
    cleanupBucket(&ks);

    try std.testing.expectEqual(@as(i64, 1), try ks.increment("test.ctr", 1));
    try std.testing.expectEqual(@as(i64, 2), try ks.increment("test.ctr", 1));
    try std.testing.expectEqual(@as(i64, 7), try ks.increment("test.ctr", 5));

    cleanupBucket(&ks);
}

test "ks: decrement" {
    var ks = try makeKs("test_zig_ks_dec");
    defer ks.deinit();
    try ks.connect();
    cleanupBucket(&ks);

    try ks.putNoRev("test.cd", "10");
    try std.testing.expectEqual(@as(i64, 9), try ks.decrement("test.cd", 1));
    try std.testing.expectEqual(@as(i64, 6), try ks.decrement("test.cd", 3));

    cleanupBucket(&ks);
}

test "ks: increment non-numeric" {
    var ks = try makeKs("test_zig_ks_nan");
    defer ks.deinit();
    try ks.connect();
    cleanupBucket(&ks);

    try ks.putNoRev("test.txt", "\"not a number\"");
    const result = ks.increment("test.txt", 1);
    try std.testing.expectError(nats.Error.NotNumeric, result);

    cleanupBucket(&ks);
}

test "ks: missing key" {
    var ks = try makeKs("test_zig_ks_miss");
    defer ks.deinit();
    try ks.connect();

    const result = ks.getRaw("totally.missing");
    try std.testing.expectError(nats.Error.NotFound, result);
}

test "ks: sync wrappers" {
    var ks = try makeKs("test_zig_sync");
    defer ks.deinit();

    _ = try ks.putSync("sync.key", "\"sync_value\"");

    const val = try ks.getSync("sync.key");
    defer nats.KeyStore.freeRaw(val);
    try std.testing.expect(std.mem.indexOf(u8, val, "sync_value") != null);

    try std.testing.expect(try ks.existsSync("sync.key"));
    try ks.deleteSync("sync.key");
    try std.testing.expect(!try ks.existsSync("sync.key"));
}

// ================================================================
//  KbStore tests
// ================================================================

test "kb: validate topic" {
    try nats.KbStore.validateTopic("valid.topic");
    try nats.KbStore.validateTopic("a.b.c");
    try nats.KbStore.validateTopic("simple");

    try std.testing.expectError(nats.Error.InvalidArg, nats.KbStore.validateTopic(".leading"));
    try std.testing.expectError(nats.Error.InvalidArg, nats.KbStore.validateTopic("trailing."));
    try std.testing.expectError(nats.Error.InvalidArg, nats.KbStore.validateTopic("double..dot"));
}

test "kb: store and get" {
    var kb = try nats.KbStore.init(TEST_SERVER, "test_zig_kb", "Test KB");
    defer kb.deinit();

    var ks = kb.getKeyStore();
    try ks.connect();
    cleanupBucket(&ks);

    const label_json = "{\"type\":\"entity\",\"description\":\"A person\"}";
    const node_json = "{\"id\":\"p001\",\"data\":{\"name\":\"Alice\",\"age\":30}}";

    var result = try kb.store("company.employees", "person", "alice", label_json, node_json, true);
    defer result.deinit();

    try std.testing.expectEqualStrings("company.employees.person.alice", result.key);

    var entry = try kb.get(result.key);
    defer entry.deinit();

    try std.testing.expect(std.mem.indexOf(u8, entry.label_json, "entity") != null);
    try std.testing.expect(std.mem.indexOf(u8, entry.node_json, "Alice") != null);

    cleanupBucket(&ks);
}

test "kb: delete" {
    var kb = try nats.KbStore.init(TEST_SERVER, "test_zig_kb_del", "Test");
    defer kb.deinit();

    var ks = kb.getKeyStore();
    try ks.connect();
    cleanupBucket(&ks);

    const lj = "{\"type\":\"x\",\"description\":\"y\"}";
    const nj = "{\"id\":\"1\",\"data\":{}}";

    var result = try kb.store("test.topic", "lbl", "nd", lj, nj, true);
    try kb.delete(result.key);

    const get_result = kb.get(result.key);
    try std.testing.expectError(nats.Error.NotFound, get_result);
    result.deinit();

    cleanupBucket(&ks);
}

test "kb: list keys" {
    var kb = try nats.KbStore.init(TEST_SERVER, "test_zig_kb_list", "Test");
    defer kb.deinit();

    var ks = kb.getKeyStore();
    try ks.connect();
    cleanupBucket(&ks);

    const lj = "{\"type\":\"x\",\"description\":\"y\"}";
    const nj = "{\"id\":\"1\",\"data\":{}}";

    try kb.storeNoKey("alpha", "l1", "n1", lj, nj);
    try kb.storeNoKey("alpha", "l2", "n2", lj, nj);
    try kb.storeNoKey("beta", "l1", "n1", lj, nj);

    var all = try kb.listKeys(null);
    defer all.deinit();
    try std.testing.expectEqual(@as(usize, 3), all.count);

    var alpha = try kb.listKeys("alpha");
    defer alpha.deinit();
    try std.testing.expectEqual(@as(usize, 2), alpha.count);

    cleanupBucket(&ks);
}

test "kb: stats" {
    var kb = try nats.KbStore.init(TEST_SERVER, "test_zig_kb_stats", "Test");
    defer kb.deinit();

    var ks = kb.getKeyStore();
    try ks.connect();
    cleanupBucket(&ks);

    const lj = "{\"type\":\"x\",\"description\":\"y\"}";
    const nj = "{\"id\":\"1\",\"data\":{}}";

    try kb.storeNoKey("topic1", "l1", "n1", lj, nj);
    try kb.storeNoKey("topic1", "l2", "n2", lj, nj);
    try kb.storeNoKey("topic2.sub", "l1", "n1", lj, nj);

    var stats = try kb.getStats();
    defer stats.deinit();
    try std.testing.expectEqual(@as(usize, 3), stats.total_kb_keys);
    try std.testing.expectEqual(@as(usize, 2), stats.total_topics);

    cleanupBucket(&ks);
}

test "kb: validate key format" {
    try std.testing.expect(nats.KbStore.validateKeyFormat("valid.topic.label.node"));
    try std.testing.expect(nats.KbStore.validateKeyFormat("a.b.c"));
    try std.testing.expect(!nats.KbStore.validateKeyFormat("invalid"));
    try std.testing.expect(!nats.KbStore.validateKeyFormat("also.invalid"));
}

test "kb: pop key" {
    const popped = try nats.KbStore.popKey("company.employees.person.alice");
    defer nats.KbStore.freePopKey(popped);
    try std.testing.expectEqualStrings("company.employees", popped);
}

// ================================================================
//  JobQueue tests
// ================================================================

test "jq: submit and get" {
    var ks = try makeKs("test_zig_jq_submit");
    defer ks.deinit();
    try ks.connect();
    cleanupBucket(&ks);

    var jq = try nats.JobQueue.init(&ks, "test-worker");
    defer jq.deinit();

    const job_id = try jq.submit("{\"task\":\"test\",\"data\":123}", .{
        .queue = "test",
        .priority = 5,
    });
    defer nats.JobQueue.freeJobId(job_id);

    try std.testing.expect(job_id.len > 0);

    var job = try jq.getJob(job_id);
    defer job.deinit();

    try std.testing.expectEqual(nats.JobStatus.pending, job.getStatus());
    try std.testing.expectEqual(@as(i32, 5), job.priority());
    try std.testing.expect(job.payloadJson() != null);

    cleanupBucket(&ks);
}

test "jq: claim and complete" {
    var ks = try makeKs("test_zig_jq_claim");
    defer ks.deinit();
    try ks.connect();
    cleanupBucket(&ks);

    var jq = try nats.JobQueue.init(&ks, "test-worker");
    defer jq.deinit();

    const job_id = try jq.submit("{\"task\":\"process\"}", .{ .queue = "test" });
    defer nats.JobQueue.freeJobId(job_id);

    const queues = [_][:0]const u8{"test"};
    var claimed = try jq.claimJob(&queues);
    defer claimed.deinit();

    try std.testing.expectEqualStrings(job_id, claimed.id());
    try std.testing.expectEqual(nats.JobStatus.running, claimed.getStatus());
    try std.testing.expectEqualStrings("test-worker", claimed.workerId());

    const ok = try jq.completeJob(job_id, "{\"output\":42}");
    try std.testing.expect(ok);

    var done = try jq.getJob(job_id);
    defer done.deinit();
    try std.testing.expectEqual(nats.JobStatus.completed, done.getStatus());
    try std.testing.expect(done.resultJson() != null);

    cleanupBucket(&ks);
}

test "jq: priority ordering" {
    var ks = try makeKs("test_zig_jq_prio");
    defer ks.deinit();
    try ks.connect();
    cleanupBucket(&ks);

    var jq = try nats.JobQueue.init(&ks, "test-worker");
    defer jq.deinit();

    const prios = [_]i32{ 1, 10, 5, 8 };
    for (prios) |p| {
        var buf: [64]u8 = undefined;
        const payload = std.fmt.bufPrintZ(&buf, "{{\"priority\":{d}}}", .{p}) catch unreachable;
        _ = try jq.submit(payload, .{ .queue = "prio_test", .priority = p });
    }

    const queues = [_][:0]const u8{"prio_test"};
    var claimed_prios: [4]i32 = undefined;

    for (0..4) |i| {
        var job = try jq.claimJob(&queues);
        claimed_prios[i] = job.priority();
        const id_buf = job.id();
        var id_z: [64:0]u8 = undefined;
        @memcpy(id_z[0..id_buf.len], id_buf);
        id_z[id_buf.len] = 0;
        _ = try jq.completeJob(id_z[0..id_buf.len :0], null);
        job.deinit();
    }

    try std.testing.expectEqual(@as(i32, 10), claimed_prios[0]);
    try std.testing.expectEqual(@as(i32, 8), claimed_prios[1]);
    try std.testing.expectEqual(@as(i32, 5), claimed_prios[2]);
    try std.testing.expectEqual(@as(i32, 1), claimed_prios[3]);

    cleanupBucket(&ks);
}

test "jq: cancel job" {
    var ks = try makeKs("test_zig_jq_cancel");
    defer ks.deinit();
    try ks.connect();
    cleanupBucket(&ks);

    var jq = try nats.JobQueue.init(&ks, "test-worker");
    defer jq.deinit();

    const job_id = try jq.submit("{\"task\":\"cancel_me\"}", .{ .queue = "test" });
    defer nats.JobQueue.freeJobId(job_id);

    try std.testing.expect(try jq.cancelJob(job_id));

    var job = try jq.getJob(job_id);
    defer job.deinit();
    try std.testing.expectEqual(nats.JobStatus.cancelled, job.getStatus());

    // Cannot cancel again
    try std.testing.expect(!try jq.cancelJob(job_id));

    cleanupBucket(&ks);
}

test "jq: retry on failure" {
    var ks = try makeKs("test_zig_jq_retry");
    defer ks.deinit();
    try ks.connect();
    cleanupBucket(&ks);

    var jq = try nats.JobQueue.init(&ks, "test-worker");
    defer jq.deinit();

    const job_id = try jq.submit("{\"task\":\"retry_test\"}", .{
        .queue = "test",
        .max_retries = 2,
    });
    defer nats.JobQueue.freeJobId(job_id);

    const queues = [_][:0]const u8{"test"};

    // First attempt – fail
    {
        var job = try jq.claimJob(&queues);
        defer job.deinit();
        _ = try jq.failJob(job_id, "First failure");
    }

    // Should be pending with retry_count=1
    {
        var job = try jq.getJob(job_id);
        defer job.deinit();
        try std.testing.expectEqual(nats.JobStatus.pending, job.getStatus());
        try std.testing.expectEqual(@as(i32, 1), job.retryCount());
    }

    // Second attempt – fail again
    {
        var job = try jq.claimJob(&queues);
        defer job.deinit();
        _ = try jq.failJob(job_id, "Second failure");
    }

    // Should be permanently failed
    {
        var job = try jq.getJob(job_id);
        defer job.deinit();
        try std.testing.expectEqual(nats.JobStatus.failed, job.getStatus());
        try std.testing.expectEqual(@as(i32, 2), job.retryCount());
        try std.testing.expect(job.errorMsg() != null);
    }

    cleanupBucket(&ks);
}

test "jq: queue stats" {
    var ks = try makeKs("test_zig_jq_stats");
    defer ks.deinit();
    try ks.connect();
    cleanupBucket(&ks);

    var jq = try nats.JobQueue.init(&ks, "test-worker");
    defer jq.deinit();

    const id1 = try jq.submit("{\"task\":1}", .{ .queue = "stats_test" });
    defer nats.JobQueue.freeJobId(id1);
    const id2 = try jq.submit("{\"task\":2}", .{ .queue = "stats_test" });
    defer nats.JobQueue.freeJobId(id2);

    var stats = try jq.getStats("stats_test");
    try std.testing.expectEqual(@as(i64, 2), stats.pending);

    const queues = [_][:0]const u8{"stats_test"};
    var job = try jq.claimJob(&queues);

    // Build a null-terminated copy of the job id
    var claimed_id_buf: [64:0]u8 = undefined;
    const cid = job.id();
    @memcpy(claimed_id_buf[0..cid.len], cid);
    claimed_id_buf[cid.len] = 0;
    const claimed_id: [:0]const u8 = claimed_id_buf[0..cid.len :0];

    _ = try jq.completeJob(claimed_id, null);

    // Cancel the other
    const other_id = if (std.mem.eql(u8, cid, id1)) id2 else id1;
    _ = try jq.cancelJob(other_id);

    stats = try jq.getStats("stats_test");
    try std.testing.expectEqual(@as(i64, 1), stats.completed);
    try std.testing.expectEqual(@as(i64, 1), stats.cancelled);
    try std.testing.expectEqual(@as(i64, 0), stats.pending);

    job.deinit();
    cleanupBucket(&ks);
}

test "jq: claim empty queue" {
    var ks = try makeKs("test_zig_jq_empty");
    defer ks.deinit();
    try ks.connect();
    cleanupBucket(&ks);

    var jq = try nats.JobQueue.init(&ks, "test-worker");
    defer jq.deinit();

    const queues = [_][:0]const u8{"empty"};
    const result = jq.claimJob(&queues);
    try std.testing.expectError(nats.Error.NotFound, result);

    cleanupBucket(&ks);
}

test "jq: complete by wrong worker" {
    var ks = try makeKs("test_zig_jq_wrong");
    defer ks.deinit();
    try ks.connect();
    cleanupBucket(&ks);

    var w1 = try nats.JobQueue.init(&ks, "worker-1");
    defer w1.deinit();
    var w2 = try nats.JobQueue.init(&ks, "worker-2");
    defer w2.deinit();

    const job_id = try w1.submit("{\"task\":\"test\"}", .{ .queue = "test" });
    defer nats.JobQueue.freeJobId(job_id);

    const queues = [_][:0]const u8{"test"};
    var job = try w1.claimJob(&queues);
    defer job.deinit();

    // Worker-2 tries to complete – should return ok=false
    const ok = try w2.completeJob(job_id, null);
    try std.testing.expect(!ok);

    // Job still running
    var check = try w1.getJob(job_id);
    defer check.deinit();
    try std.testing.expectEqual(nats.JobStatus.running, check.getStatus());

    cleanupBucket(&ks);
}