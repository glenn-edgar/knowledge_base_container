//! test_data_structures.zig
//! Mirrors test_kb_data_structures.c — Integration tests.
//! Uses an in-memory database with synthetic seed data.

const std = @import("std");
const kb = @import("kb");
const c = kb.c;

/// Build in-memory test database with all subsystem tables and seed data.
fn createTestDatabase() !*c.sqlite3 {
    var db: ?*c.sqlite3 = null;
    if (c.sqlite3_open(":memory:", &db) != c.SQLITE_OK) return error.SqliteOpen;
    const real_db = db orelse return error.SqliteOpen;

    const stmts = [_][*:0]const u8{
        // ── Schema ─────────────────────────────────────────────────
        "CREATE TABLE test_kb ("
        ++ "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        ++ "  knowledge_base TEXT, label TEXT, name TEXT,"
        ++ "  path TEXT UNIQUE, properties TEXT, data TEXT,"
        ++ "  has_link INTEGER DEFAULT 0, has_link_mount INTEGER DEFAULT 0"
        ++ ");",

        "CREATE TABLE test_kb_status_table ("
        ++ "  id INTEGER PRIMARY KEY AUTOINCREMENT, path TEXT UNIQUE, data TEXT);",

        "CREATE TABLE test_kb_job_queue ("
        ++ "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        ++ "  path TEXT, state TEXT DEFAULT 'free',"
        ++ "  data TEXT, priority INTEGER DEFAULT 0, queued_at TEXT);",

        "CREATE TABLE test_kb_stream_table ("
        ++ "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        ++ "  path TEXT, entry_index INTEGER, write_index INTEGER DEFAULT 0,"
        ++ "  max_entries INTEGER DEFAULT 10, data TEXT, recorded_at TEXT);",

        "CREATE TABLE test_kb_bit_mask_store ("
        ++ "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        ++ "  path TEXT UNIQUE, bit_mask INTEGER DEFAULT 0, change_mask INTEGER DEFAULT 0);",

        "CREATE TABLE test_kb_rpc_server_queue ("
        ++ "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        ++ "  path TEXT, state TEXT DEFAULT 'empty',"
        ++ "  request_uuid TEXT, rpc_action TEXT, data TEXT,"
        ++ "  priority INTEGER DEFAULT 0, rpc_client_queue TEXT, queued_at TEXT);",

        "CREATE TABLE test_kb_rpc_client_queue ("
        ++ "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        ++ "  path TEXT, state TEXT DEFAULT 'free',"
        ++ "  request_uuid TEXT, server_path TEXT, rpc_action TEXT,"
        ++ "  transaction_tag TEXT, reply_data TEXT, replied_at TEXT);",

        "CREATE TABLE test_kb_link_table ("
        ++ "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        ++ "  link_name TEXT, node_path TEXT, link_order INTEGER);",

        "CREATE TABLE test_kb_link_mount_table ("
        ++ "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        ++ "  link_name TEXT, mount_path TEXT);",

        // ── Seed: KB nodes ─────────────────────────────────────────
        "INSERT INTO test_kb (knowledge_base,label,name,path,properties,data) VALUES"
        ++ " ('kb1','KB_STATUS_FIELD','temp','kb1.sensors.temp',"
        ++ "  '{\"description\":\"Temperature\"}','{\"value\":22}');",

        "INSERT INTO test_kb (knowledge_base,label,name,path,properties,data) VALUES"
        ++ " ('kb1','KB_JOB_FIELD','worker','kb1.jobs.worker',"
        ++ "  '{\"description\":\"Worker queue\"}','{}');",

        "INSERT INTO test_kb (knowledge_base,label,name,path,properties,data) VALUES"
        ++ " ('kb1','KB_BIT_FIELD','flags','kb1.flags.main',"
        ++ "  '{\"description\":\"Main flags\"}','{}');",

        // ── Seed: status ───────────────────────────────────────────
        "INSERT INTO test_kb_status_table (path,data) VALUES ('kb1.sensors.temp','{\"value\":22}');",

        // ── Seed: job queue (5 free slots) ─────────────────────────
        "INSERT INTO test_kb_job_queue (path,state) VALUES ('kb1.jobs.worker','free');",
        "INSERT INTO test_kb_job_queue (path,state) VALUES ('kb1.jobs.worker','free');",
        "INSERT INTO test_kb_job_queue (path,state) VALUES ('kb1.jobs.worker','free');",
        "INSERT INTO test_kb_job_queue (path,state) VALUES ('kb1.jobs.worker','free');",
        "INSERT INTO test_kb_job_queue (path,state) VALUES ('kb1.jobs.worker','free');",

        // ── Seed: bit mask ─────────────────────────────────────────
        "INSERT INTO test_kb_bit_mask_store (path,bit_mask,change_mask) VALUES ('kb1.flags.main',0,0);",

        // ── Seed: stream (5 entry slots) ───────────────────────────
        "INSERT INTO test_kb_stream_table (path,entry_index,write_index,max_entries) VALUES ('kb1.stream.data',0,0,10);",
        "INSERT INTO test_kb_stream_table (path,entry_index) VALUES ('kb1.stream.data',1);",
        "INSERT INTO test_kb_stream_table (path,entry_index) VALUES ('kb1.stream.data',2);",
        "INSERT INTO test_kb_stream_table (path,entry_index) VALUES ('kb1.stream.data',3);",
        "INSERT INTO test_kb_stream_table (path,entry_index) VALUES ('kb1.stream.data',4);",

        // ── Seed: RPC server (3 empty slots) ───────────────────────
        "INSERT INTO test_kb_rpc_server_queue (path,state) VALUES ('kb1.rpc.server','empty');",
        "INSERT INTO test_kb_rpc_server_queue (path,state) VALUES ('kb1.rpc.server','empty');",
        "INSERT INTO test_kb_rpc_server_queue (path,state) VALUES ('kb1.rpc.server','empty');",

        // ── Seed: RPC client (2 free slots) ────────────────────────
        "INSERT INTO test_kb_rpc_client_queue (path,state) VALUES ('kb1.rpc.client','free');",
        "INSERT INTO test_kb_rpc_client_queue (path,state) VALUES ('kb1.rpc.client','free');",

        // ── Seed: link tables ──────────────────────────────────────
        "INSERT INTO test_kb_link_table (link_name,node_path,link_order) VALUES ('link1','kb1.child1',0);",
        "INSERT INTO test_kb_link_table (link_name,node_path,link_order) VALUES ('link1','kb1.child2',1);",
        "INSERT INTO test_kb_link_mount_table (link_name,mount_path) VALUES ('mount1','kb1.mount.point');",
    };

    for (stmts) |sql| {
        var errmsg: ?[*:0]u8 = null;
        if (c.sqlite3_exec(real_db, sql, null, null, @ptrCast(&errmsg)) != c.SQLITE_OK) {
            if (errmsg) |e| c.sqlite3_free(e);
            _ = c.sqlite3_close(real_db);
            return error.SqliteDdl;
        }
    }
    return real_db;
}

fn closeDb(db_ptr: *c.sqlite3) void {
    _ = c.sqlite3_close(db_ptr);
}

/// Helper: create DataStructures over in-memory test DB.
fn createTestDs() !struct { ds: kb.DataStructures, db: *c.sqlite3 } {
    const db_ptr = try createTestDatabase();
    const ds = kb.DataStructures.createFromDb(db_ptr, "test_kb") catch {
        closeDb(db_ptr);
        return error.DsCreate;
    };
    return .{ .ds = ds, .db = db_ptr };
}

// ═══════════════════════════════════════════════════════════════════
// Status Table
// ═══════════════════════════════════════════════════════════════════

test "status: get initial data" {
    var ctx = try createTestDs();
    defer ctx.ds.destroy();

    var status = try ctx.ds.status();
    const data = try status.getData("kb1.sensors.temp");
    try std.testing.expect(data != null);
    if (data) |d| {
        defer kb.freeCStr(@ptrCast(d.ptr));
        try std.testing.expect(std.mem.indexOf(u8, d, "22") != null);
    }
}

test "status: set and read back" {
    var ctx = try createTestDs();
    defer ctx.ds.destroy();

    var status = try ctx.ds.status();
    try status.setData("kb1.sensors.temp", "{\"value\":25.5}");

    const data = try status.getData("kb1.sensors.temp");
    try std.testing.expect(data != null);
    if (data) |d| {
        defer kb.freeCStr(@ptrCast(d.ptr));
        try std.testing.expect(std.mem.indexOf(u8, d, "25.5") != null);
    }
}

// ═══════════════════════════════════════════════════════════════════
// Job Queue
// ═══════════════════════════════════════════════════════════════════

test "job queue: free count" {
    var ctx = try createTestDs();
    defer ctx.ds.destroy();

    var jq = try ctx.ds.jobQueue();
    const free_count = try jq.getFreeNumber("kb1.jobs.worker");
    try std.testing.expectEqual(@as(i32, 5), free_count);
}

test "job queue: push/peek/complete cycle" {
    var ctx = try createTestDs();
    defer ctx.ds.destroy();

    var jq = try ctx.ds.jobQueue();

    // Push
    try jq.push("kb1.jobs.worker", "{\"task\":\"process_data\"}", 1);
    const queued = try jq.getQueuedNumber("kb1.jobs.worker");
    try std.testing.expectEqual(@as(i32, 1), queued);

    // Peek
    var peek = try jq.peek("kb1.jobs.worker");
    defer peek.deinit();
    try std.testing.expect(peek.data != null);

    // Complete
    try jq.complete("kb1.jobs.worker", peek.record_id);
    const free_after = try jq.getFreeNumber("kb1.jobs.worker");
    try std.testing.expectEqual(@as(i32, 5), free_after);
}

// ═══════════════════════════════════════════════════════════════════
// RPC Server
// ═══════════════════════════════════════════════════════════════════

test "rpc server: push/peek/claim/complete" {
    var ctx = try createTestDs();
    defer ctx.ds.destroy();

    var srv = try ctx.ds.rpcServer();

    // Push
    const uuid = try srv.push("kb1.rpc.server", "do_something", "{\"arg\":1}", 1, "kb1.rpc.client");
    try std.testing.expect(uuid[0] != 0); // non-empty UUID

    // Peek
    var peek = try srv.peek("kb1.rpc.server");
    defer peek.deinit();
    try std.testing.expect(peek.data != null);
    try std.testing.expect(peek.action != null);

    // Claim → Complete
    try srv.claim("kb1.rpc.server", peek.record_id);
    try srv.complete_job("kb1.rpc.server", peek.record_id);

    // Verify all slots empty
    const counts = try srv.getStateCounts("kb1.rpc.server");
    try std.testing.expectEqual(@as(i32, 3), counts.empty);
}

// ═══════════════════════════════════════════════════════════════════
// RPC Client
// ═══════════════════════════════════════════════════════════════════

test "rpc client: push_and_claim / peek / clear" {
    var ctx = try createTestDs();
    defer ctx.ds.destroy();

    var client = try ctx.ds.rpcClient();

    try client.pushAndClaim(
        "kb1.rpc.client",
        "test-uuid-123",
        "kb1.rpc.server",
        "do_something",
        "tag1",
        "{\"result\":\"ok\"}",
    );

    const counts = try client.getStateCounts("kb1.rpc.client");
    try std.testing.expectEqual(@as(i32, 1), counts.queued);

    var peek = try client.peekReply("kb1.rpc.client");
    defer peek.deinit();
    try std.testing.expect(peek.reply_data != null);

    try client.clearReply("kb1.rpc.client", peek.record_id);
}

// ═══════════════════════════════════════════════════════════════════
// Bit Mask
// ═══════════════════════════════════════════════════════════════════

test "bit mask: set/get individual bits" {
    var ctx = try createTestDs();
    defer ctx.ds.destroy();

    var bits = try ctx.ds.bitStructures();
    var ops = try bits.getOps();

    try ops.setBit("kb1.flags.main", 0, 1);
    try ops.setBit("kb1.flags.main", 2, 1);

    try std.testing.expectEqual(@as(i32, 1), try ops.getBit("kb1.flags.main", 0));
    try std.testing.expectEqual(@as(i32, 0), try ops.getBit("kb1.flags.main", 1));
}

test "bit mask: full mask value" {
    var ctx = try createTestDs();
    defer ctx.ds.destroy();

    var bits = try ctx.ds.bitStructures();
    var ops = try bits.getOps();

    try ops.setBit("kb1.flags.main", 0, 1);
    try ops.setBit("kb1.flags.main", 2, 1);

    const mask = try ops.getMask("kb1.flags.main");
    try std.testing.expectEqual(@as(i64, 5), mask); // 0x05
}

test "bit mask: change mask" {
    var ctx = try createTestDs();
    defer ctx.ds.destroy();

    var bits = try ctx.ds.bitStructures();
    var ops = try bits.getOps();

    try ops.setBit("kb1.flags.main", 0, 1);

    const cm = try ops.getChangeMask("kb1.flags.main");
    try std.testing.expect(cm != 0);

    try ops.clearChangeMask("kb1.flags.main");
    const cm_after = try ops.getChangeMask("kb1.flags.main");
    try std.testing.expectEqual(@as(i64, 0), cm_after);
}

// ═══════════════════════════════════════════════════════════════════
// Link Tables
// ═══════════════════════════════════════════════════════════════════

test "link table: get by name" {
    var ctx = try createTestDs();
    defer ctx.ds.destroy();

    var lt = try ctx.ds.linkTable();
    var result = try lt.getByLinkName("link1");
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 2), result.count());
}

test "link mount table: get by name" {
    var ctx = try createTestDs();
    defer ctx.ds.destroy();

    var lmt = try ctx.ds.linkMountTable();
    var result = try lmt.getByLinkName("mount1");
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.count());
}