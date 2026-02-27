const std = @import("std");
const kb = @import("kb");
const c = kb.c;

/// Build in-memory test database with all subsystem tables and seed data.
fn createTestDatabase() !*c.sqlite3 {
    var db: ?*c.sqlite3 = null;
    if (c.sqlite3_open(":memory:", &db) != c.SQLITE_OK) return error.SqliteOpen;
    const real_db = db orelse return error.SqliteOpen;

    const stmts = [_][*:0]const u8{
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

        // Seed KB nodes
        "INSERT INTO test_kb (knowledge_base,label,name,path,properties,data) VALUES"
        ++ " ('kb1','KB_STATUS_FIELD','temp','kb1.sensors.temp',"
        ++ "  '{\"description\":\"Temperature\"}','{\"value\":22}');",
        "INSERT INTO test_kb (knowledge_base,label,name,path,properties,data) VALUES"
        ++ " ('kb1','KB_JOB_FIELD','worker','kb1.jobs.worker',"
        ++ "  '{\"description\":\"Worker queue\"}','{}');",
        "INSERT INTO test_kb (knowledge_base,label,name,path,properties,data) VALUES"
        ++ " ('kb1','KB_BIT_FIELD','flags','kb1.flags.main',"
        ++ "  '{\"description\":\"Main flags\"}','{}');",

        // Seed status
        "INSERT INTO test_kb_status_table (path,data) VALUES ('kb1.sensors.temp','{\"value\":22}');",

        // Seed job queue (5 free slots)
        "INSERT INTO test_kb_job_queue (path,state) VALUES ('kb1.jobs.worker','free');",
        "INSERT INTO test_kb_job_queue (path,state) VALUES ('kb1.jobs.worker','free');",
        "INSERT INTO test_kb_job_queue (path,state) VALUES ('kb1.jobs.worker','free');",
        "INSERT INTO test_kb_job_queue (path,state) VALUES ('kb1.jobs.worker','free');",
        "INSERT INTO test_kb_job_queue (path,state) VALUES ('kb1.jobs.worker','free');",

        // Seed bit mask
        "INSERT INTO test_kb_bit_mask_store (path,bit_mask,change_mask) VALUES ('kb1.flags.main',0,0);",

        // Seed RPC server (3 empty slots)
        "INSERT INTO test_kb_rpc_server_queue (path,state) VALUES ('kb1.rpc.server','empty');",
        "INSERT INTO test_kb_rpc_server_queue (path,state) VALUES ('kb1.rpc.server','empty');",
        "INSERT INTO test_kb_rpc_server_queue (path,state) VALUES ('kb1.rpc.server','empty');",

        // Seed RPC client (2 free slots)
        "INSERT INTO test_kb_rpc_client_queue (path,state) VALUES ('kb1.rpc.client','free');",
        "INSERT INTO test_kb_rpc_client_queue (path,state) VALUES ('kb1.rpc.client','free');",

        // Seed link tables
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

pub fn main() !void {
    const stdout = std.io.getStdOut().writer();

    try stdout.print("=== KB Zig Wrapper Example (in-memory DB) ===\n\n", .{});

    // ── Create in-memory database and aggregator ────────────────────
    const db = createTestDatabase() catch {
        try stdout.print("FATAL: Cannot create in-memory database\n", .{});
        return;
    };

    var ds = kb.DataStructures.createFromDb(db, "test_kb") catch {
        try stdout.print("FATAL: Cannot create DataStructures\n", .{});
        _ = c.sqlite3_close(db);
        return;
    };
    defer ds.destroy();

    // ── Search: list all nodes ──────────────────────────────────────
    var search = try ds.search();
    search.clearFilters();
    try search.execute();
    const res = search.results();
    try stdout.print("Total KB nodes: {d}\n", .{res.count});

    // ── Status: get / set / get ─────────────────────────────────────
    try stdout.print("\n-- Status Table --\n", .{});
    var status = try ds.status();

    if (status.getData("kb1.sensors.temp")) |maybe_data| {
        if (maybe_data) |data| {
            defer kb.freeCStr(@ptrCast(data.ptr));
            try stdout.print("  initial: {s}\n", .{data});
        }
    } else |_| {}

    try status.setData("kb1.sensors.temp", "{\"value\":25.5}");

    if (status.getData("kb1.sensors.temp")) |maybe_data| {
        if (maybe_data) |data| {
            defer kb.freeCStr(@ptrCast(data.ptr));
            try stdout.print("  updated: {s}\n", .{data});
        }
    } else |_| {}

    // ── Job Queue: push / peek / complete ───────────────────────────
    try stdout.print("\n-- Job Queue --\n", .{});
    var jq = try ds.jobQueue();

    const free_before = try jq.getFreeNumber("kb1.jobs.worker");
    try stdout.print("  free slots: {d}\n", .{free_before});

    try jq.push("kb1.jobs.worker", "{\"task\":\"process_data\"}", 1);
    const queued = try jq.getQueuedNumber("kb1.jobs.worker");
    try stdout.print("  queued after push: {d}\n", .{queued});

    var peek = try jq.peek("kb1.jobs.worker");
    if (peek.data) |d| {
        try stdout.print("  peek data: {s} (id={d})\n", .{ d, peek.record_id });
    }
    try jq.complete("kb1.jobs.worker", peek.record_id);
    peek.deinit();

    const free_after = try jq.getFreeNumber("kb1.jobs.worker");
    try stdout.print("  free after complete: {d}\n", .{free_after});

    // ── Bit Mask: set bits / read mask ──────────────────────────────
    try stdout.print("\n-- Bit Mask --\n", .{});
    var bits = try ds.bitStructures();
    var ops = try bits.getOps();

    try ops.setBit("kb1.flags.main", 0, 1);
    try ops.setBit("kb1.flags.main", 2, 1);

    const mask = try ops.getMask("kb1.flags.main");
    try stdout.print("  mask after setting bits 0,2: 0x{x} ({d})\n", .{ @as(u64, @bitCast(mask)), mask });

    const b0 = try ops.getBit("kb1.flags.main", 0);
    const b1 = try ops.getBit("kb1.flags.main", 1);
    const b2 = try ops.getBit("kb1.flags.main", 2);
    try stdout.print("  bit0={d} bit1={d} bit2={d}\n", .{ b0, b1, b2 });

    // ── RPC Server: push / peek / claim / complete ──────────────────
    try stdout.print("\n-- RPC Server --\n", .{});
    var srv = try ds.rpcServer();

    const uuid = try srv.push("kb1.rpc.server", "do_something", "{\"arg\":1}", 1, "kb1.rpc.client");
    try stdout.print("  pushed uuid: {s}\n", .{@as([]const u8, &uuid)});

    var srv_peek = try srv.peek("kb1.rpc.server");
    if (srv_peek.action) |a| {
        try stdout.print("  peek action: {s} (id={d})\n", .{ a, srv_peek.record_id });
    }
    try srv.claim("kb1.rpc.server", srv_peek.record_id);
    try srv.complete_job("kb1.rpc.server", srv_peek.record_id);
    srv_peek.deinit();

    const counts = try srv.getStateCounts("kb1.rpc.server");
    try stdout.print("  final: empty={d} new={d} processing={d}\n", .{ counts.empty, counts.new_job, counts.processing });

    // ── Link Tables ─────────────────────────────────────────────────
    try stdout.print("\n-- Link Tables --\n", .{});
    var lt = try ds.linkTable();
    var link_result = try lt.getByLinkName("link1");
    defer link_result.deinit();
    try stdout.print("  link1 entries: {d}\n", .{link_result.count()});

    var lmt = try ds.linkMountTable();
    var mount_result = try lmt.getByLinkName("mount1");
    defer mount_result.deinit();
    try stdout.print("  mount1 entries: {d}\n", .{mount_result.count()});

    try stdout.print("\ndone.\n", .{});
}