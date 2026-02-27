//! test_query_support.zig
//! Mirrors test_kb_query_support.c — KB_Search unit tests.
//! Creates an in-memory test database with sample KB data,
//! then exercises the CTE filter chain.

const std = @import("std");
const kb = @import("kb");
const c = kb.c;

/// Create an in-memory SQLite database with test data.
fn createTestDb() !*c.sqlite3 {
    var db: ?*c.sqlite3 = null;
    if (c.sqlite3_open(":memory:", &db) != c.SQLITE_OK) return error.SqliteOpen;
    const real_db = db orelse return error.SqliteOpen;

    const ddl =
        \\CREATE TABLE test_kb (
        \\  id INTEGER PRIMARY KEY AUTOINCREMENT,
        \\  knowledge_base TEXT, label TEXT, name TEXT,
        \\  path TEXT, properties TEXT, data TEXT,
        \\  has_link INTEGER DEFAULT 0, has_link_mount INTEGER DEFAULT 0
        \\);
        \\INSERT INTO test_kb (knowledge_base,label,name,path,properties,data,has_link) VALUES
        \\('kb1','article','intro','kb1.docs.intro',
        \\ '{"difficulty":"beginner","description":"Introduction"}','{"content":"hello"}',0),
        \\('kb1','article','advanced','kb1.docs.advanced',
        \\ '{"difficulty":"advanced","description":"Deep dive"}','{"content":"deep"}',1),
        \\('kb1','KB_STATUS_FIELD','temperature','kb1.sensors.temperature',
        \\ '{"unit":"celsius","description":"Temp sensor"}','{"value":22.5}',0),
        \\('kb2','article','overview','kb2.docs.overview',
        \\ '{"difficulty":"beginner","description":"Overview"}','{"content":"summary"}',0),
        \\('kb1','KB_JOB_FIELD','processor','kb1.jobs.processor',
        \\ '{"description":"Job processor"}','{}',0);
    ;

    var errmsg: ?[*:0]u8 = null;
    if (c.sqlite3_exec(real_db, ddl, null, null, @ptrCast(&errmsg)) != c.SQLITE_OK) {
        if (errmsg) |e| c.sqlite3_free(e);
        _ = c.sqlite3_close(real_db);
        return error.SqliteDdl;
    }
    return real_db;
}

fn closeDb(db: *c.sqlite3) void {
    _ = c.sqlite3_close(db);
}

// ── No Filters ──────────────────────────────────────────────────────

test "no filters (select all)" {
    const db = try createTestDb();
    defer closeDb(db);

    var ks = try kb.Search.createFromDb(db, "test_kb");
    defer ks.destroy();

    try ks.execute();
    const r = ks.results();
    try std.testing.expectEqual(@as(c_int, 5), r.count);
}

// ── Label Filter ────────────────────────────────────────────────────

test "label filter: article" {
    const db = try createTestDb();
    defer closeDb(db);

    var ks = try kb.Search.createFromDb(db, "test_kb");
    defer ks.destroy();

    try ks.label("article");
    try ks.execute();
    const r = ks.results();
    try std.testing.expectEqual(@as(c_int, 3), r.count);
}

test "label filter: KB_STATUS_FIELD" {
    const db = try createTestDb();
    defer closeDb(db);

    var ks = try kb.Search.createFromDb(db, "test_kb");
    defer ks.destroy();

    try ks.label("KB_STATUS_FIELD");
    try ks.execute();
    const r = ks.results();
    try std.testing.expectEqual(@as(c_int, 1), r.count);
}

// ── Combined Filters ────────────────────────────────────────────────

test "combined: kb + label" {
    const db = try createTestDb();
    defer closeDb(db);

    var ks = try kb.Search.createFromDb(db, "test_kb");
    defer ks.destroy();

    try ks.kb("kb1");
    try ks.label("article");
    try ks.execute();
    const r = ks.results();
    try std.testing.expectEqual(@as(c_int, 2), r.count);
}

// ── Name Filter ─────────────────────────────────────────────────────

test "name filter" {
    const db = try createTestDb();
    defer closeDb(db);

    var ks = try kb.Search.createFromDb(db, "test_kb");
    defer ks.destroy();

    try ks.name("intro");
    try ks.execute();
    const r = ks.results();
    try std.testing.expectEqual(@as(c_int, 1), r.count);

    const path_val = c.kb_row_get(r, 0, "path");
    try std.testing.expect(path_val != null);
    try std.testing.expectEqualStrings("kb1.docs.intro", std.mem.span(path_val.?));
}

// ── Has Link Filter ─────────────────────────────────────────────────

test "has_link filter" {
    const db = try createTestDb();
    defer closeDb(db);

    var ks = try kb.Search.createFromDb(db, "test_kb");
    defer ks.destroy();

    try ks.hasLink();
    try ks.execute();
    const r = ks.results();
    try std.testing.expectEqual(@as(c_int, 1), r.count);
}

// ── Find Path Values ────────────────────────────────────────────────

test "find_path_values" {
    const db = try createTestDb();
    defer closeDb(db);

    var ks = try kb.Search.createFromDb(db, "test_kb");
    defer ks.destroy();

    try ks.label("article");
    try ks.execute();
    const r = ks.results();

    var paths: ?[*]?[*:0]u8 = null;
    var path_count: c_int = 0;
    try kb.check(c.kb_search_find_path_values(r, @ptrCast(&paths), &path_count));
    defer c.kb_path_values_free(@ptrCast(paths), path_count);

    try std.testing.expectEqual(@as(c_int, 3), path_count);
}

// ── Decode Link Nodes ───────────────────────────────────────────────

test "decode_link_nodes" {
    var kb_name: ?[*:0]u8 = null;
    var pairs: ?[*]c.kb_link_pair_t = null;
    var pair_count: c_int = 0;

    try kb.check(c.kb_search_decode_link_nodes(
        "kb_main.uuid1.parent.uuid2.child",
        @ptrCast(&kb_name),
        @ptrCast(&pairs),
        &pair_count,
    ));
    defer {
        if (kb_name) |n| std.c.free(n);
        if (pairs) |p| c.kb_link_pairs_free(p, pair_count);
    }

    try std.testing.expectEqualStrings("kb_main", std.mem.span(kb_name.?));
    try std.testing.expectEqual(@as(c_int, 2), pair_count);

    const p = pairs.?;
    try std.testing.expectEqualStrings("uuid1", std.mem.span(p[0].link.?));
    try std.testing.expectEqualStrings("parent", std.mem.span(p[0].name.?));
    try std.testing.expectEqualStrings("uuid2", std.mem.span(p[1].link.?));
    try std.testing.expectEqualStrings("child", std.mem.span(p[1].name.?));
}