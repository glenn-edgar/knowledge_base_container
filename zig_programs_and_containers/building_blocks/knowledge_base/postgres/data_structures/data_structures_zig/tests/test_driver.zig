//! test_driver.zig — Comprehensive integration test for the PostgreSQL knowledge base Zig wrapper.
//!
//! Mirrors test_driver.c exactly: same test sequence, same assertions, same output format.
//! Requires a live PostgreSQL database with the knowledge base already constructed.
//!
//! Environment variables:
//!   POSTGRES_PASSWORD — required
//!   POSTGRES_HOST     — default: localhost
//!   POSTGRES_PORT     — default: 5432
//!   POSTGRES_DB       — default: knowledge_base
//!   POSTGRES_USER     — default: gedgar
//!   KB_DATABASE       — default: knowledge_base (table prefix)
//!
//! Run:
//!   POSTGRES_PASSWORD=secret zig build test-driver
//!   POSTGRES_PASSWORD=secret ./zig-out/bin/test_driver

const std = @import("std");
const kb = @import("kb");

// ═══════════════════════════════════════════════════════════════════
// Test Framework
// ═══════════════════════════════════════════════════════════════════

var test_pass: u32 = 0;
var test_fail: u32 = 0;
var test_total: u32 = 0;

fn print(comptime fmt: []const u8, args: anytype) void {
    std.io.getStdOut().writer().print(fmt, args) catch {};
}

fn section(name: []const u8) void {
    print("\n===== {s} =====\n", .{name});
}

fn assertOk(result: kb.KbError!void, msg: []const u8) void {
    test_total += 1;
    if (result) |_| {
        test_pass += 1;
        print("  [PASS] {s}\n", .{msg});
    } else |err| {
        test_fail += 1;
        print("  [FAIL] {s} — error: {s}\n", .{ msg, kb.errorString(err) });
    }
}

fn assertTrue(cond: bool, msg: []const u8) void {
    test_total += 1;
    if (cond) {
        test_pass += 1;
        print("  [PASS] {s}\n", .{msg});
    } else {
        test_fail += 1;
        print("  [FAIL] {s}\n", .{msg});
    }
}

fn assertIntEq(a: i64, b: i64, msg: []const u8) void {
    test_total += 1;
    if (a == b) {
        test_pass += 1;
        print("  [PASS] {s} ({d} == {d})\n", .{ msg, a, b });
    } else {
        test_fail += 1;
        print("  [FAIL] {s} ({d} != {d})\n", .{ msg, a, b });
    }
}

fn assertStrContains(haystack: ?[:0]const u8, needle: []const u8, msg: []const u8) void {
    test_total += 1;
    if (haystack) |h| {
        if (std.mem.indexOf(u8, h, needle) != null) {
            test_pass += 1;
            print("  [PASS] {s}\n", .{msg});
        } else {
            test_fail += 1;
            print("  [FAIL] {s} — '{s}' not in '{s}'\n", .{ msg, needle, h });
        }
    } else {
        test_fail += 1;
        print("  [FAIL] {s} — '{s}' not in (null)\n", .{ msg, needle });
    }
}

// ═══════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════

fn getEnv(key: [:0]const u8, default: [:0]const u8) [:0]const u8 {
    return std.posix.getenvZ(key) orelse default;
}

fn freeStr(val: ?[:0]const u8) void {
    if (val) |v| kb.freeCStr(@ptrCast(@constCast(v.ptr)));
}

/// Generate a UUID v4 string.
fn generateUuid() [36:0]u8 {
    var bytes: [16]u8 = undefined;
    std.crypto.random.bytes(&bytes);
    bytes[6] = (bytes[6] & 0x0F) | 0x40;
    bytes[8] = (bytes[8] & 0x3F) | 0x80;
    var buf: [36:0]u8 = undefined;
    _ = std.fmt.bufPrint(&buf, "{x:0>2}{x:0>2}{x:0>2}{x:0>2}-{x:0>2}{x:0>2}-{x:0>2}{x:0>2}-{x:0>2}{x:0>2}-{x:0>2}{x:0>2}{x:0>2}{x:0>2}{x:0>2}{x:0>2}", .{
        bytes[0],  bytes[1],  bytes[2],  bytes[3],
        bytes[4],  bytes[5],  bytes[6],  bytes[7],
        bytes[8],  bytes[9],  bytes[10], bytes[11],
        bytes[12], bytes[13], bytes[14], bytes[15],
    }) catch unreachable;
    buf[36] = 0;
    return buf;
}

// ═══════════════════════════════════════════════════════════════════
// Test: Search / Discovery
// ═══════════════════════════════════════════════════════════════════

fn testSearch(ks: *kb.Search) void {
    section("KB Search / Discovery");

    // Find status paths
    if (ks.findStatusPaths()) |pl_const| {
        var pl = pl_const;
        defer pl.deinit();
        assertOk({}, "find_status_paths");
        print("    Found {d} status path(s)\n", .{pl.len()});
        assertTrue(pl.len() > 0, "at least one status path found");
        for (0..pl.len()) |i| {
            if (pl.get(i)) |p| print("      [{d}] {s}\n", .{ i, p });
        }
    } else |err| {
        assertOk(err, "find_status_paths");
    }

    // Find job paths
    if (ks.findJobPaths()) |pl_const| {
        var pl = pl_const;
        defer pl.deinit();
        assertOk({}, "find_job_paths");
        print("    Found {d} job path(s)\n", .{pl.len()});
    } else |err| {
        assertOk(err, "find_job_paths");
    }

    // Find stream paths
    if (ks.findStreamPaths()) |pl_const| {
        var pl = pl_const;
        defer pl.deinit();
        assertOk({}, "find_stream_paths");
        print("    Found {d} stream path(s)\n", .{pl.len()});
    } else |err| {
        assertOk(err, "find_stream_paths");
    }

    // Find bit structure paths
    if (ks.findBitStructurePaths()) |pl_const| {
        var pl = pl_const;
        defer pl.deinit();
        assertOk({}, "find_bit_structure_paths");
        print("    Found {d} bit structure path(s)\n", .{pl.len()});
    } else |err| {
        assertOk(err, "find_bit_structure_paths");
    }

    // Find RPC server paths
    if (ks.findRpcServerPaths()) |pl_const| {
        var pl = pl_const;
        defer pl.deinit();
        assertOk({}, "find_rpc_server_paths");
        print("    Found {d} RPC server path(s)\n", .{pl.len()});
    } else |err| {
        assertOk(err, "find_rpc_server_paths");
    }

    // Find RPC client paths
    if (ks.findRpcClientPaths()) |pl_const| {
        var pl = pl_const;
        defer pl.deinit();
        assertOk({}, "find_rpc_client_paths");
        print("    Found {d} RPC client path(s)\n", .{pl.len()});
    } else |err| {
        assertOk(err, "find_rpc_client_paths");
    }

    // Test CTE filter chain
    ks.clear();
    if (ks.label("KB_STATUS_FIELD")) {
        if (ks.execute()) {
            assertOk({}, "CTE filter: label only");
            const rs = ks.results();
            assertTrue(rs != null and rs.?.nrows > 0, "CTE filter returned rows");
            if (rs) |r| print("    CTE label filter: {d} rows\n", .{r.nrows});
        } else |err| {
            assertOk(err, "CTE filter: label only");
        }
    } else |err| {
        assertOk(err, "CTE filter: label setup");
    }
}

// ═══════════════════════════════════════════════════════════════════
// Test: Status
// ═══════════════════════════════════════════════════════════════════

fn testStatus(conn: *kb.Connection, ks: *kb.Search, database: [:0]const u8) void {
    section("Status Data");

    var pl = ks.findStatusPaths() catch {
        print("  [SKIP] No status paths found\n", .{});
        return;
    };
    defer pl.deinit();
    if (pl.len() == 0) {
        print("  [SKIP] No status paths found\n", .{});
        return;
    }
    const path = pl.get(0) orelse return;
    print("    Using status path: {s}\n", .{path});

    const status = kb.Status.init(conn, database);

    // Set
    assertOk(status.setDefault(path, "{\"value\":42,\"name\":\"test\"}"), "status_set");

    // Get
    if (status.get(path)) |data_opt| {
        assertOk({}, "status_get");
        assertStrContains(data_opt, "42", "status data contains 42");
        if (data_opt) |d| print("    Got: {s}\n", .{d});
        freeStr(data_opt);
    } else |err| {
        assertOk(err, "status_get");
    }

    // Overwrite
    assertOk(status.setDefault(path, "{\"value\":99}"), "status_set overwrite");

    if (status.get(path)) |data_opt| {
        assertOk({}, "status_get after overwrite");
        assertStrContains(data_opt, "99", "status data updated to 99");
        freeStr(data_opt);
    } else |err| {
        assertOk(err, "status_get after overwrite");
    }
}

// ═══════════════════════════════════════════════════════════════════
// Test: Job Queue
// ═══════════════════════════════════════════════════════════════════

fn testJobQueue(conn: *kb.Connection, ks: *kb.Search, database: [:0]const u8) void {
    section("Job Queue");

    var pl = ks.findJobPaths() catch {
        print("  [SKIP] No job paths found\n", .{});
        return;
    };
    defer pl.deinit();
    if (pl.len() == 0) {
        print("  [SKIP] No job paths found\n", .{});
        return;
    }
    const path = pl.get(0) orelse return;
    print("    Using job path: {s}\n", .{path});

    const jq = kb.JobQueue.init(conn, database);

    // Clear
    assertOk(jq.clearDefault(path), "job_clear");

    // Free count
    if (jq.freeCount(path)) |fc| {
        assertOk({}, "job_free_count");
        print("    Free slots: {d}\n", .{fc});
        assertTrue(fc > 0, "have free slots after clear");
    } else |err| {
        assertOk(err, "job_free_count");
    }

    // Push
    assertOk(jq.pushDefault(path, "{\"task\":\"backup\",\"priority\":1}"), "job_push");

    // Queued count
    if (jq.queuedCount(path)) |qc| {
        assertOk({}, "job_queued_count");
        assertIntEq(qc, 1, "one job queued");
    } else |err| {
        assertOk(err, "job_queued_count");
    }

    // Peek
    if (jq.peekDefault(path)) |info_const| {
        var info = info_const;
        defer info.deinit();
        assertOk({}, "job_peek");
        assertTrue(info.found, "peek found a job");
        assertStrContains(info.data, "backup", "job data contains backup");
        if (info.data) |d| print("    Peeked job id={d} data={s}\n", .{ info.id, d });

        // Complete
        if (info.found) {
            assertOk(jq.completeDefault(info.id), "job_complete");
        }
    } else |err| {
        assertOk(err, "job_peek");
    }

    // Verify free count restored
    if (jq.freeCount(path)) |fc| {
        assertOk({}, "job_free_count after complete");
        print("    Free slots after complete: {d}\n", .{fc});
    } else |err| {
        assertOk(err, "job_free_count after complete");
    }
}

// ═══════════════════════════════════════════════════════════════════
// Test: Stream
// ═══════════════════════════════════════════════════════════════════

fn testStream(conn: *kb.Connection, ks: *kb.Search, database: [:0]const u8) void {
    section("Stream Data");

    var pl = ks.findStreamPaths() catch {
        print("  [SKIP] No stream paths found\n", .{});
        return;
    };
    defer pl.deinit();
    if (pl.len() == 0) {
        print("  [SKIP] No stream paths found\n", .{});
        return;
    }
    const path = pl.get(0) orelse return;
    print("    Using stream path: {s}\n", .{path});

    const stream = kb.Stream.init(conn, database);

    // Clear
    assertOk(stream.clearDefault(path), "stream_clear");

    // Count after clear
    if (stream.count(path)) |vc| {
        assertOk({}, "stream_count after clear");
        assertIntEq(vc, 0, "no valid entries after clear");
    } else |err| {
        assertOk(err, "stream_count after clear");
    }

    // Total count
    var total_count: i64 = 0;
    if (stream.countTotal(path)) |tc| {
        assertOk({}, "stream_count_total");
        total_count = tc;
        print("    Total slots: {d}\n", .{tc});
        assertTrue(tc > 0, "have pre-allocated slots");
    } else |err| {
        assertOk(err, "stream_count_total");
    }

    // Push 3 entries
    assertOk(stream.pushDefault(path, "{\"temp\":72.5,\"unit\":\"F\"}"), "stream_push 1");
    assertOk(stream.pushDefault(path, "{\"temp\":73.1,\"unit\":\"F\"}"), "stream_push 2");
    assertOk(stream.pushDefault(path, "{\"temp\":74.0,\"unit\":\"F\"}"), "stream_push 3");

    // Count valid
    if (stream.count(path)) |vc| {
        assertOk({}, "stream_count after push");
        assertIntEq(vc, 3, "3 valid entries after push");
    } else |err| {
        assertOk(err, "stream_count after push");
    }

    // List all
    if (stream.list(path, null, null)) |rs_const| {
        var rs = rs_const;
        defer rs.deinit();
        assertOk({}, "stream_list all");
        print("    Stream entries: {d}\n", .{rs.rowCount()});
        assertTrue(rs.rowCount() == 3, "list returns 3 entries");
        var i: usize = 0;
        while (i < rs.rowCount() and i < 3) : (i += 1) {
            const d = rs.get(i, "data");
            const ts = rs.get(i, "recorded_at");
            print("      [{d}] {s} @ {s}\n", .{ i, d orelse "(null)", ts orelse "?" });
        }
    } else |err| {
        assertOk(err, "stream_list all");
    }

    // Latest
    if (stream.latest(path)) |rs_const| {
        var rs = rs_const;
        defer rs.deinit();
        assertOk({}, "stream_latest");
        assertTrue(rs.rowCount() == 1, "latest returns 1 row");
        const d = rs.get(0, "data");
        if (d) |data| print("    Latest: {s}\n", .{data});
        assertStrContains(d, "74.0", "latest is 74.0");
    } else |err| {
        assertOk(err, "stream_latest");
    }

    // Statistics
    if (stream.statistics(path)) |rs_const| {
        var rs = rs_const;
        defer rs.deinit();
        assertOk({}, "stream_statistics");
        if (rs.rowCount() > 0) {
            const tc = rs.getInt64(0, "total_count");
            const vc = rs.getInt64(0, "valid_count");
            const ic = rs.getInt64(0, "invalid_count");
            print("    Stats: total={d} valid={d} invalid={d}\n", .{ tc, vc, ic });
            assertIntEq(vc, 3, "stats valid_count = 3");
            assertIntEq(tc, total_count, "stats total matches count_total");
        }
    } else |err| {
        assertOk(err, "stream_statistics");
    }

    // Clear final
    assertOk(stream.clearDefault(path), "stream_clear final");
    if (stream.count(path)) |vc| {
        assertOk({}, "stream_count after final clear");
        assertIntEq(vc, 0, "no valid after final clear");
    } else |err| {
        assertOk(err, "stream_count after final clear");
    }
}

// ═══════════════════════════════════════════════════════════════════
// Test: RPC Server
// ═══════════════════════════════════════════════════════════════════

fn testRpcServer(conn: *kb.Connection, ks: *kb.Search, database: [:0]const u8) void {
    section("RPC Server");

    var pl = ks.findRpcServerPaths() catch {
        print("  [SKIP] No RPC server paths found\n", .{});
        return;
    };
    defer pl.deinit();
    if (pl.len() == 0) {
        print("  [SKIP] No RPC server paths found\n", .{});
        return;
    }
    const path = pl.get(0) orelse return;
    print("    Using RPC server path: {s}\n", .{path});

    // Find client path
    var client_pl = ks.findRpcClientPaths() catch kb.Search.PathList{ .paths = null, .count = 0 };
    defer client_pl.deinit();
    const client_path = client_pl.get(0) orelse path;

    const rpc = kb.RpcServer.init(conn, database);

    // Clear
    assertOk(rpc.clearDefault(path), "rpc_server_clear");

    if (rpc.countNew(path)) |nc| {
        assertOk({}, "rpc_server_count_new after clear");
        assertIntEq(nc, 0, "no new jobs after clear");
    } else |err| {
        assertOk(err, "rpc_server_count_new after clear");
    }

    // Push with priority
    const uuid1 = generateUuid();
    const uuid2 = generateUuid();

    assertOk(rpc.push(path, &uuid1, "process_data", "{\"input\":\"test\"}", "tx_001", 2, client_path, kb.default_max_retries, kb.default_base_delay_ms), "rpc_server_push priority=2");
    assertOk(rpc.push(path, &uuid2, "urgent_task", "{\"input\":\"urgent\"}", "tx_002", 1, client_path, kb.default_max_retries, kb.default_base_delay_ms), "rpc_server_push priority=1");

    // Count
    if (rpc.countNew(path)) |nc| {
        assertOk({}, "rpc_server_count_new after push");
        assertIntEq(nc, 2, "two new jobs");
    } else |err| {
        assertOk(err, "rpc_server_count_new after push");
    }

    // Peek (should get priority=1 first)
    if (rpc.peekDefault(path)) |job_const| {
        var job = job_const;
        defer job.deinit();
        assertOk({}, "rpc_server_peek");
        assertTrue(job.found, "peek found a job");
        if (job.found) {
            print("    Peeked: id={d} action={s} priority={d}\n", .{
                job.id,
                job.rpc_action orelse "?",
                job.priority,
            });
            assertIntEq(job.priority, 1, "highest priority first");
            assertTrue(
                job.rpc_action != null and std.mem.eql(u8, job.rpc_action.?, "urgent_task"),
                "urgent_task peeked first",
            );
            assertOk(rpc.completeDefault(path, job.id), "rpc_server_complete");
        }
    } else |err| {
        assertOk(err, "rpc_server_peek");
    }

    // Peek again (priority=2)
    if (rpc.peekDefault(path)) |job2_const| {
        var job2 = job2_const;
        defer job2.deinit();
        assertOk({}, "rpc_server_peek second");
        if (job2.found) {
            assertIntEq(job2.priority, 2, "second priority next");
            assertOk(rpc.completeDefault(path, job2.id), "rpc_server_complete second");
        }
    } else |err| {
        assertOk(err, "rpc_server_peek second");
    }
}

// ═══════════════════════════════════════════════════════════════════
// Test: RPC Client
// ═══════════════════════════════════════════════════════════════════

fn testRpcClient(conn: *kb.Connection, ks: *kb.Search, database: [:0]const u8) void {
    section("RPC Client");

    var pl = ks.findRpcClientPaths() catch {
        print("  [SKIP] No RPC client paths found\n", .{});
        return;
    };
    defer pl.deinit();
    if (pl.len() == 0) {
        print("  [SKIP] No RPC client paths found\n", .{});
        return;
    }
    const path = pl.get(0) orelse return;
    print("    Using RPC client path: {s}\n", .{path});

    const rpc_client = kb.RpcClient.init(conn, database);

    // Clear
    assertOk(rpc_client.clearDefault(path), "rpc_client_clear");

    // Free slots
    if (rpc_client.freeSlots(path)) |fs| {
        assertOk({}, "rpc_client_free_slots");
        print("    Free slots: {d}\n", .{fs});
        assertTrue(fs > 0, "have free slots");
    } else |err| {
        assertOk(err, "rpc_client_free_slots");
    }

    // Push reply
    const client_uuid = generateUuid();
    assertOk(
        rpc_client.pushReply(path, &client_uuid, "server.path", "process_data", "tx_001", "{\"result\":\"success\"}", kb.default_max_retries, kb.default_base_delay_ms),
        "rpc_client_push_reply",
    );

    // Queued count
    if (rpc_client.queuedSlots(path)) |qs| {
        assertOk({}, "rpc_client_queued_slots");
        assertIntEq(qs, 1, "one reply queued");
    } else |err| {
        assertOk(err, "rpc_client_queued_slots");
    }

    // Peek
    if (rpc_client.peekReplyDefault(path)) |reply_const| {
        var reply = reply_const;
        defer reply.deinit();
        assertOk({}, "rpc_client_peek_reply");
        assertTrue(reply.found, "peek found reply");
        if (reply.found) {
            print("    Reply: id={d} payload={s}\n", .{
                reply.id,
                reply.response_payload orelse "?",
            });
            assertStrContains(reply.response_payload, "success", "reply contains success");
        }
    } else |err| {
        assertOk(err, "rpc_client_peek_reply");
    }

    // Clear
    assertOk(rpc_client.clearDefault(path), "rpc_client_clear final");
}

// ═══════════════════════════════════════════════════════════════════
// Test: Bit Structures
// ═══════════════════════════════════════════════════════════════════

fn testBitStructures(conn: *kb.Connection, ks: *kb.Search, database: [:0]const u8) void {
    section("Bit Structures");

    ks.clear();
    ks.label("KB_BIT_MASK") catch {
        print("  [SKIP] KB search for bit structures failed\n", .{});
        return;
    };
    ks.execute() catch {
        print("  [SKIP] KB search execute failed\n", .{});
        return;
    };

    const rs_ptr = ks.results();
    if (rs_ptr == null or rs_ptr.?.nrows == 0) {
        print("  [SKIP] No bit structure nodes found\n", .{});
        return;
    }
    print("    Found {d} bit structure node(s)\n", .{rs_ptr.?.nrows});

    // Extract record_id from properties JSON
    const props_json = kb.c.kb_rs_get(rs_ptr.?, 0, "properties");
    if (props_json == null) {
        print("  [SKIP] No properties column in bit structure row\n", .{});
        return;
    }

    const cjson = kb.c.cJSON_Parse(props_json);
    if (cjson == null) {
        print("  [SKIP] Could not parse properties JSON\n", .{});
        return;
    }
    defer kb.c.cJSON_Delete(cjson);

    const rid_item = kb.c.cJSON_GetObjectItem(cjson, "record_id");
    if (rid_item == null or kb.c.cJSON_IsString(rid_item) == 0) {
        print("  [SKIP] No record_id in properties\n", .{});
        return;
    }

    const node_id: [:0]const u8 = std.mem.span(rid_item.?.*.valuestring);
    print("    Using node_id: {s}\n", .{node_id});

    const bits = kb.BitStructures.init(conn, database);

    // Read initial mask
    if (bits.getMask(node_id)) |mask| {
        assertOk({}, "bit_get_mask initial read");
        print("    Initial mask: {d}\n", .{mask});
    } else |err| {
        assertOk(err, "bit_get_mask initial read");
    }

    // Reset mask to 0
    assertOk(bits.setMaskDefault(node_id, 0), "bit_set_mask to 0");

    // Set bit 0
    assertOk(bits.setBitDefault(node_id, 0, true), "bit_set bit 0 = true");

    // Verify mask = 1
    if (bits.getMask(node_id)) |mask| {
        assertOk({}, "bit_get_mask after set bit 0");
        assertIntEq(mask, 1, "mask = 1 after bit 0 set");
    } else |err| {
        assertOk(err, "bit_get_mask after set bit 0");
    }

    // Set bit 4
    assertOk(bits.setBitDefault(node_id, 4, true), "bit_set bit 4 = true");

    if (bits.getMask(node_id)) |mask| {
        assertOk({}, "bit_get_mask after set bit 4");
        assertIntEq(mask, 17, "mask = 17 (bit0 + bit4)");
    } else |err| {
        assertOk(err, "bit_get_mask after set bit 4");
    }

    // Get individual bits
    if (bits.getBit(node_id, 0)) |val| {
        assertOk({}, "bit_get bit 0");
        assertTrue(val, "bit 0 is set");
    } else |err| {
        assertOk(err, "bit_get bit 0");
    }

    if (bits.getBit(node_id, 1)) |val| {
        assertOk({}, "bit_get bit 1");
        assertTrue(!val, "bit 1 is not set");
    } else |err| {
        assertOk(err, "bit_get bit 1");
    }

    if (bits.getBit(node_id, 4)) |val| {
        assertOk({}, "bit_get bit 4");
        assertTrue(val, "bit 4 is set");
    } else |err| {
        assertOk(err, "bit_get bit 4");
    }

    // S-expression evaluation
    if (bits.evalSexpr(node_id, "[\"and\", [\"bit\", 0], [\"bit\", 4]]", null, 0)) |result| {
        assertOk({}, "sexpr: and(bit0, bit4)");
        assertTrue(result, "and(bit0, bit4) = true");
    } else |err| {
        assertOk(err, "sexpr: and(bit0, bit4)");
    }

    if (bits.evalSexpr(node_id, "[\"or\", [\"bit\", 1], [\"bit\", 2]]", null, 0)) |result| {
        assertOk({}, "sexpr: or(bit1, bit2)");
        assertTrue(!result, "or(bit1, bit2) = false");
    } else |err| {
        assertOk(err, "sexpr: or(bit1, bit2)");
    }

    if (bits.evalSexpr(node_id, "[\"not\", [\"bit\", 1]]", null, 0)) |result| {
        assertOk({}, "sexpr: not(bit1)");
        assertTrue(result, "not(bit1) = true");
    } else |err| {
        assertOk(err, "sexpr: not(bit1)");
    }

    if (bits.evalSexpr(node_id, "[\"bit_changed\", 0]", null, 0)) |result| {
        assertOk({}, "sexpr: bit_changed(0) prev=0");
        assertTrue(result, "bit_changed(0) with prev=0 = true");
    } else |err| {
        assertOk(err, "sexpr: bit_changed(0) prev=0");
    }

    if (bits.evalSexpr(node_id, "[\"bit_changed\", 0]", null, 17)) |result| {
        assertOk({}, "sexpr: bit_changed(0) prev=17");
        assertTrue(!result, "bit_changed(0) with prev=17 = false");
    } else |err| {
        assertOk(err, "sexpr: bit_changed(0) prev=17");
    }

    // Reset
    assertOk(bits.setMaskDefault(node_id, 0), "bit_set_mask reset to 0");
}

// ═══════════════════════════════════════════════════════════════════
// Test: Document Table
// ═══════════════════════════════════════════════════════════════════

fn testDocumentTable(conn: *kb.Connection, ks: *kb.Search, database: [:0]const u8, label_field: [:0]const u8) void {
    var section_buf: [128]u8 = undefined;
    const section_name = std.fmt.bufPrint(&section_buf, "Document Table ({s})", .{label_field}) catch "Document Table";
    section(section_name);

    // Discover document path
    ks.clear();
    ks.label("KB_JSONB_FIELD") catch return;
    ks.name(label_field) catch return;
    ks.execute() catch return;

    const search_rs = ks.results();
    if (search_rs == null or search_rs.?.nrows == 0) {
        print("  [SKIP] No document node found for '{s}'\n", .{label_field});
        return;
    }

    const doc_path_ptr = kb.c.kb_rs_get(search_rs.?, 0, "path");
    if (doc_path_ptr == null) return;
    const path: [:0]const u8 = std.mem.span(doc_path_ptr.?);
    print("    document path: {s}\n", .{path});

    const doc = kb.Document.init(conn, database);

    // Set entire document
    assertOk(
        doc.set(path, "", "{\"name\":\"Test\",\"role\":\"admin\",\"tags\":[\"python\",\"postgres\"],\"address\":{\"city\":\"LA\",\"zip\":\"90001\"}}", true, null),
        "jsonb_set entire document",
    );

    // Get entire document
    if (doc.get(path, "", false, null)) |val_opt| {
        assertOk({}, "jsonb_get entire document");
        assertStrContains(val_opt, "Test", "doc contains Test");
        if (val_opt) |v| print("    doc: {s}\n", .{v});
        freeStr(val_opt);
    } else |err| {
        assertOk(err, "jsonb_get entire document");
    }

    // Get name as JSON
    if (doc.get(path, "name", false, null)) |val_opt| {
        assertOk({}, "jsonb_get name (JSON)");
        if (val_opt) |v| print("    name (JSON): {s}\n", .{v});
        freeStr(val_opt);
    } else |err| {
        assertOk(err, "jsonb_get name (JSON)");
    }

    // Get name as text
    if (doc.get(path, "name", true, null)) |val_opt| {
        assertOk({}, "jsonb_get name (text)");
        assertStrContains(val_opt, "Test", "name text = Test");
        if (val_opt) |v| print("    name (text): {s}\n", .{v});
        freeStr(val_opt);
    } else |err| {
        assertOk(err, "jsonb_get name (text)");
    }

    // Get nested path
    if (doc.get(path, "address.city", true, null)) |val_opt| {
        assertOk({}, "jsonb_get address.city");
        assertStrContains(val_opt, "LA", "city = LA");
        if (val_opt) |v| print("    address.city: {s}\n", .{v});
        freeStr(val_opt);
    } else |err| {
        assertOk(err, "jsonb_get address.city");
    }

    // Key existence
    if (doc.hasKey(path, "role", null)) |val| {
        assertOk({}, "has_key role");
        assertTrue(val, "has role key");
    } else |err| {
        assertOk(err, "has_key role");
    }

    // has_any_keys — use raw C for multi-key
    {
        const keys = [_][*:0]const u8{ "role", "nonexistent" };
        var bval: bool = false;
        const err = kb.c.kb_doc_has_any_keys(conn.handle, database.ptr, path.ptr, @ptrCast(@constCast(&keys)), 2, null, &bval);
        if (err == kb.c.KB_OK) {
            assertOk({}, "has_any_keys [role, nonexistent]");
            assertTrue(bval, "has any of role/nonexistent");
        } else {
            assertOk(kb.check(err), "has_any_keys [role, nonexistent]");
        }
    }

    // has_all_keys [name, role]
    {
        const keys = [_][*:0]const u8{ "name", "role" };
        var bval: bool = false;
        const err = kb.c.kb_doc_has_all_keys(conn.handle, database.ptr, path.ptr, @ptrCast(@constCast(&keys)), 2, null, &bval);
        if (err == kb.c.KB_OK) {
            assertOk({}, "has_all_keys [name, role]");
            assertTrue(bval, "has all name+role");
        } else {
            assertOk(kb.check(err), "has_all_keys [name, role]");
        }
    }

    // has_all_keys [name, nonexistent]
    {
        const keys = [_][*:0]const u8{ "name", "nonexistent" };
        var bval: bool = false;
        const err = kb.c.kb_doc_has_all_keys(conn.handle, database.ptr, path.ptr, @ptrCast(@constCast(&keys)), 2, null, &bval);
        if (err == kb.c.KB_OK) {
            assertOk({}, "has_all_keys [name, nonexistent]");
            assertTrue(!bval, "does not have all name+nonexistent");
        } else {
            assertOk(kb.check(err), "has_all_keys [name, nonexistent]");
        }
    }

    // Containment
    if (doc.contains(path, "{\"role\":\"admin\"}", null)) |val| {
        assertOk({}, "contains {role:admin}");
        assertTrue(val, "contains role=admin");
    } else |err| {
        assertOk(err, "contains {role:admin}");
    }

    if (doc.contains(path, "{\"role\":\"user\"}", null)) |val| {
        assertOk({}, "contains {role:user}");
        assertTrue(!val, "does not contain role=user");
    } else |err| {
        assertOk(err, "contains {role:user}");
    }

    // Array contains
    if (doc.arrayContains(path, "tags", "\"python\"", null)) |val| {
        assertOk({}, "array tags contains python");
        assertTrue(val, "tags has python");
    } else |err| {
        assertOk(err, "array tags contains python");
    }

    if (doc.arrayContains(path, "tags", "\"ruby\"", null)) |val| {
        assertOk({}, "array tags contains ruby");
        assertTrue(!val, "tags does not have ruby");
    } else |err| {
        assertOk(err, "array tags contains ruby");
    }

    // JSONPath
    if (doc.pathExists(path, "$.role ? (@ == \"admin\")", null)) |val| {
        assertOk({}, "path_exists role==admin");
        assertTrue(val, "jsonpath role==admin exists");
    } else |err| {
        assertOk(err, "path_exists role==admin");
    }

    if (doc.pathQuery(path, "$.tags[*]", null)) |val_opt| {
        assertOk({}, "path_query $.tags[*]");
        if (val_opt) |v| print("    path_query tags: {s}\n", .{v});
        assertStrContains(val_opt, "python", "path_query has python");
        freeStr(val_opt);
    } else |err| {
        assertOk(err, "path_query $.tags[*]");
    }

    // Set and delete
    assertOk(doc.set(path, "status", "\"active\"", true, null), "jsonb_set status=active");

    if (doc.get(path, "status", true, null)) |val_opt| {
        assertOk({}, "jsonb_get status");
        assertStrContains(val_opt, "active", "status = active");
        freeStr(val_opt);
    } else |err| {
        assertOk(err, "jsonb_get status");
    }

    assertOk(doc.deleteKey(path, "status", null), "jsonb_delete_key status");

    {
        const result = doc.get(path, "status", true, null);
        if (result) |val_opt| {
            assertTrue(val_opt == null, "status deleted (null)");
            freeStr(val_opt);
        } else |_| {
            assertTrue(true, "status deleted (null)");
        }
    }

    assertOk(doc.deletePath(path, "address.zip", null), "jsonb_delete_path address.zip");

    // Array elements
    {
        var elem_rs: ?*kb.c.kb_resultset_t = null;
        const err = kb.c.kb_doc_array_elements(conn.handle, database.ptr, path.ptr, "tags", null, &elem_rs);
        if (err == kb.c.KB_OK and elem_rs != null) {
            assertOk({}, "array_elements tags");
            print("    tag elements: {d} rows\n", .{elem_rs.?.nrows});
            assertTrue(elem_rs.?.nrows >= 2, "at least 2 tag elements");
            kb.c.kb_resultset_free(elem_rs);
        } else {
            assertOk(kb.check(err), "array_elements tags");
        }
    }

    // Queue (FIFO)
    print("\n  --- Queue (FIFO) ---\n", .{});
    assertOk(doc.queueClear(path, null, null), "queue_clear");

    assertOk(doc.enqueue(path, "{\"task\":\"Task 1\",\"priority\":1}", null, null), "enqueue Task 1");
    assertOk(doc.enqueue(path, "{\"task\":\"Task 2\",\"priority\":2}", null, null), "enqueue Task 2");
    assertOk(doc.enqueue(path, "{\"task\":\"Task 3\",\"priority\":3}", null, null), "enqueue Task 3");

    if (doc.queueSize(path, null, null)) |sz| {
        assertOk({}, "queue_size after 3 enqueues");
        assertIntEq(sz, 3, "queue size = 3");
    } else |err| {
        assertOk(err, "queue_size after 3 enqueues");
    }

    if (doc.dequeue(path, null, null)) |val_opt| {
        assertOk({}, "dequeue (FIFO)");
        if (val_opt) |v| print("    dequeued: {s}\n", .{v});
        assertStrContains(val_opt, "Task 1", "dequeued Task 1 (FIFO)");
        freeStr(val_opt);
    } else |err| {
        assertOk(err, "dequeue (FIFO)");
    }

    if (doc.queuePeek(path, null, 0, null)) |val_opt| {
        assertOk({}, "peek index 0");
        assertStrContains(val_opt, "Task 2", "peek shows Task 2");
        freeStr(val_opt);
    } else |err| {
        assertOk(err, "peek index 0");
    }

    if (doc.queueSize(path, null, null)) |sz| {
        assertOk({}, "queue_size after dequeue");
        assertIntEq(sz, 2, "queue size = 2 after dequeue");
    } else |err| {
        assertOk(err, "queue_size after dequeue");
    }

    // Stack (LIFO)
    print("\n  --- Stack (LIFO) ---\n", .{});
    assertOk(doc.queueClear(path, null, null), "clear for stack test");

    assertOk(doc.stackPush(path, "{\"message\":\"First\"}", null, null), "push First");
    assertOk(doc.stackPush(path, "{\"message\":\"Second\"}", null, null), "push Second");
    assertOk(doc.stackPush(path, "{\"message\":\"Third\"}", null, null), "push Third");

    if (doc.stackPop(path, null, null)) |val_opt| {
        assertOk({}, "pop (LIFO)");
        if (val_opt) |v| print("    popped: {s}\n", .{v});
        assertStrContains(val_opt, "Third", "popped Third (LIFO)");
        freeStr(val_opt);
    } else |err| {
        assertOk(err, "pop (LIFO)");
    }

    if (doc.stackPop(path, null, null)) |val_opt| {
        assertOk({}, "pop second (LIFO)");
        assertStrContains(val_opt, "Second", "popped Second (LIFO)");
        freeStr(val_opt);
    } else |err| {
        assertOk(err, "pop second (LIFO)");
    }

    if (doc.queueSize(path, null, null)) |sz| {
        assertOk({}, "stack size after 2 pops");
        assertIntEq(sz, 1, "stack size = 1");
    } else |err| {
        assertOk(err, "stack size after 2 pops");
    }

    // Edge cases
    print("\n  --- Edge Cases ---\n", .{});
    assertOk(doc.queueClear(path, null, null), "clear for edge cases");

    {
        const result = doc.dequeue(path, null, null);
        if (result) |val_opt| {
            print("    dequeue from empty: {s} (err=0)\n", .{val_opt orelse "NULL"});
            freeStr(val_opt);
        } else |_| {
            print("    dequeue from empty: NULL (err=-3)\n", .{});
        }
    }

    {
        const result = doc.stackPop(path, null, null);
        if (result) |val_opt| {
            print("    pop from empty: {s} (err=0)\n", .{val_opt orelse "NULL"});
            freeStr(val_opt);
        } else |_| {
            print("    pop from empty: NULL (err=-3)\n", .{});
        }
    }

    if (doc.queueIsEmpty(path, null, null)) |empty| {
        assertOk({}, "queue_is_empty on empty");
        assertTrue(empty, "empty queue is empty");
    } else |err| {
        assertOk(err, "queue_is_empty on empty");
    }

    assertOk(doc.enqueue(path, "{\"data\":\"test\"}", null, null), "enqueue for get_all");

    if (doc.queueGetAll(path, null, null)) |val_opt| {
        assertOk({}, "queue_get_all");
        if (val_opt) |v| print("    get_all: {s}\n", .{v});
        assertStrContains(val_opt, "test", "get_all contains test");
        freeStr(val_opt);
    } else |err| {
        assertOk(err, "queue_get_all");
    }

    assertOk(doc.queueClear(path, null, null), "final queue_clear");
}

// ═══════════════════════════════════════════════════════════════════
// Test: Link Tables
// ═══════════════════════════════════════════════════════════════════

fn testLinkTables(conn: *kb.Connection, ks: *kb.Search, database: [:0]const u8) void {
    section("Link Tables");

    if (ks.findLinkPaths()) |link_pl_const| {
        var link_pl = link_pl_const;
        defer link_pl.deinit();
        print("    Link paths found: {d}\n", .{link_pl.len()});
        var i: usize = 0;
        while (i < link_pl.len() and i < 5) : (i += 1) {
            if (link_pl.get(i)) |p| print("      [{d}] {s}\n", .{ i, p });
        }

        const lt = kb.LinkTable.init(conn, database);

        if (link_pl.len() > 0) {
            if (link_pl.get(0)) |first_path| {
                if (lt.queryByPath(first_path)) |rs_const| {
                    var rs = rs_const;
                    defer rs.deinit();
                    print("    Link entries for {s}: {d} rows\n", .{ first_path, rs.rowCount() });
                } else |_| {}

                if (lt.decodeNodes(first_path)) |decoded_const| {
                    var decoded = decoded_const;
                    defer decoded.deinit();
                    print("    Decoded link nodes: {d}\n", .{decoded.len()});
                    var j: usize = 0;
                    while (j < decoded.len() and j < 5) : (j += 1) {
                        if (decoded.get(j)) |p| print("      → {s}\n", .{p});
                    }
                } else |_| {}
            }
        }
    } else |_| {}

    if (ks.findLinkMountPaths()) |mount_pl_const| {
        var mount_pl = mount_pl_const;
        defer mount_pl.deinit();
        print("    Link mount paths found: {d}\n", .{mount_pl.len()});
        var i: usize = 0;
        while (i < mount_pl.len() and i < 5) : (i += 1) {
            if (mount_pl.get(i)) |p| print("      [{d}] {s}\n", .{ i, p });
        }
    } else |_| {}

    assertTrue(true, "link tables queried without crash");
}

// ═══════════════════════════════════════════════════════════════════
// Main
// ═══════════════════════════════════════════════════════════════════

pub fn main() !void {
    print("Knowledge Base Zig Library (PostgreSQL) — Integration Test\n", .{});
    print("========================================================\n", .{});

    const password = std.posix.getenvZ("POSTGRES_PASSWORD") orelse {
        print("Error: POSTGRES_PASSWORD environment variable required\n", .{});
        return;
    };
    const host = getEnv("POSTGRES_HOST", "localhost");
    const port = getEnv("POSTGRES_PORT", "5432");
    const dbname = getEnv("POSTGRES_DB", "knowledge_base");
    const user = getEnv("POSTGRES_USER", "gedgar");
    const database = getEnv("KB_DATABASE", "knowledge_base");

    print("Connecting: host={s} port={s} db={s} user={s} kb={s}\n", .{ host, port, dbname, user, database });

    var conn = kb.Connection.connectParams(host, port, dbname, user, password) catch |err| {
        print("Failed to connect: {s}\n", .{kb.errorString(err)});
        return;
    };
    defer conn.disconnect();
    print("Connected successfully.\n", .{});

    var ks = kb.Search.create(&conn, database) catch |err| {
        print("Failed to create search: {s}\n", .{kb.errorString(err)});
        return;
    };
    defer ks.destroy();

    // Run all tests — same sequence as C test_driver
    testSearch(&ks);
    testStatus(&conn, &ks, database);
    testJobQueue(&conn, &ks, database);
    testStream(&conn, &ks, database);
    testRpcServer(&conn, &ks, database);
    testRpcClient(&conn, &ks, database);
    testBitStructures(&conn, &ks, database);
    testDocumentTable(&conn, &ks, database, "info1_jsonb");
    testDocumentTable(&conn, &ks, database, "info2_jsonb");
    testDocumentTable(&conn, &ks, database, "info3_jsonb");
    testLinkTables(&conn, &ks, database);

    // Summary
    print("\n========================================================\n", .{});
    print("Results: {d}/{d} passed", .{ test_pass, test_total });
    if (test_fail > 0) print(", {d} FAILED", .{test_fail});
    print("\n========================================================\n", .{});
}

// Zig test entry point — wraps main() so `zig build test-driver` works
test "integration" {
    try main();
    if (test_fail > 0) return error.TestFailed;
}