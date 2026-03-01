//! kb.zig — Idiomatic Zig wrapper for the Knowledge Base C library (PostgreSQL)
//!
//! Wraps `data_structures_c` (PostgreSQL backend) providing:
//! - Error unions replacing C error codes
//! - RAII-style init/deinit patterns
//! - Sentinel-terminated string slices for C interop
//! - Optional pointer unwrapping for all C return values
//!
//! Raw C bindings are always available via `kb.c`.

const std = @import("std");

/// Raw C bindings — import all KB headers via the master include.
pub const c = @cImport({
    @cInclude("kb_all.h");
    @cInclude("cjson/cJSON.h");
});

// ═══════════════════════════════════════════════════════════════════
// Error Handling
// ═══════════════════════════════════════════════════════════════════

pub const KbError = error{
    NullArg,
    Pg,
    NotFound,
    Json,
    NoMem,
    Busy,
    Invalid,
    Overflow,
    State,
    Unknown,
};

/// Convert a C error code to a Zig error union.
pub fn check(err: c.kb_error_t) KbError!void {
    return switch (err) {
        c.KB_OK => {},
        c.KB_ERR_NULL_ARG => error.NullArg,
        c.KB_ERR_PG => error.Pg,
        c.KB_ERR_NOT_FOUND => error.NotFound,
        c.KB_ERR_JSON => error.Json,
        c.KB_ERR_NOMEM => error.NoMem,
        c.KB_ERR_BUSY => error.Busy,
        c.KB_ERR_INVALID => error.Invalid,
        c.KB_ERR_OVERFLOW => error.Overflow,
        c.KB_ERR_STATE => error.State,
        else => error.Unknown,
    };
}

/// Get the C error string for a Zig KbError.
pub fn errorString(err: KbError) [:0]const u8 {
    const code: c.kb_error_t = switch (err) {
        error.NullArg => c.KB_ERR_NULL_ARG,
        error.Pg => c.KB_ERR_PG,
        error.NotFound => c.KB_ERR_NOT_FOUND,
        error.Json => c.KB_ERR_JSON,
        error.NoMem => c.KB_ERR_NOMEM,
        error.Busy => c.KB_ERR_BUSY,
        error.Invalid => c.KB_ERR_INVALID,
        error.Overflow => c.KB_ERR_OVERFLOW,
        error.State => c.KB_ERR_STATE,
        error.Unknown => -99,
    };
    return std.mem.span(c.kb_error_str(code));
}

/// Free a C-allocated string (from getData, peek, etc.)
pub fn freeCStr(ptr: [*]u8) void {
    std.c.free(ptr);
}

/// Free a C-allocated string that may be null.
pub fn freeCStrOpt(ptr: ?[*]u8) void {
    if (ptr) |p| std.c.free(p);
}

// ═══════════════════════════════════════════════════════════════════
// Default retry parameters
// ═══════════════════════════════════════════════════════════════════

pub const default_max_retries: c_int = 3;
pub const default_base_delay_ms: c_int = 100;

// ═══════════════════════════════════════════════════════════════════
// Connection
// ═══════════════════════════════════════════════════════════════════

pub const Connection = struct {
    handle: *c.kb_conn_t,

    /// Connect using a libpq connection string.
    pub fn connect(conninfo: [:0]const u8) KbError!Connection {
        var conn: ?*c.kb_conn_t = null;
        try check(c.kb_connect(conninfo.ptr, &conn));
        return .{ .handle = conn orelse return error.NullArg };
    }

    /// Connect using individual parameters.
    pub fn connectParams(
        host: [:0]const u8,
        port: [:0]const u8,
        dbname: [:0]const u8,
        user: [:0]const u8,
        password: [:0]const u8,
    ) KbError!Connection {
        var conn: ?*c.kb_conn_t = null;
        try check(c.kb_connect_params(host.ptr, port.ptr, dbname.ptr, user.ptr, password.ptr, &conn));
        return .{ .handle = conn orelse return error.NullArg };
    }

    pub fn disconnect(self: *Connection) void {
        c.kb_disconnect(self.handle);
    }

    pub fn commit(self: *Connection) KbError!void {
        try check(c.kb_commit(self.handle));
    }

    pub fn rollback(self: *Connection) KbError!void {
        try check(c.kb_rollback(self.handle));
    }

    pub fn begin(self: *Connection) KbError!void {
        try check(c.kb_begin(self.handle));
    }
};

// ═══════════════════════════════════════════════════════════════════
// ResultSet
// ═══════════════════════════════════════════════════════════════════

pub const ResultSet = struct {
    handle: *c.kb_resultset_t,

    pub fn deinit(self: *ResultSet) void {
        c.kb_resultset_free(self.handle);
    }

    pub fn rowCount(self: *const ResultSet) usize {
        return @intCast(self.handle.nrows);
    }

    pub fn colCount(self: *const ResultSet) usize {
        return @intCast(self.handle.ncols);
    }

    /// Get a string value by row index and column name.
    pub fn get(self: *const ResultSet, row: usize, col: [:0]const u8) ?[:0]const u8 {
        const ptr = c.kb_rs_get(self.handle, @intCast(row), col.ptr);
        if (ptr) |p| return std.mem.span(p);
        return null;
    }

    pub fn getInt(self: *const ResultSet, row: usize, col: [:0]const u8) c_int {
        return c.kb_rs_get_int(self.handle, @intCast(row), col.ptr);
    }

    pub fn getInt64(self: *const ResultSet, row: usize, col: [:0]const u8) i64 {
        return c.kb_rs_get_int64(self.handle, @intCast(row), col.ptr);
    }

    pub fn getBool(self: *const ResultSet, row: usize, col: [:0]const u8) bool {
        return c.kb_rs_get_bool(self.handle, @intCast(row), col.ptr);
    }
};

// ═══════════════════════════════════════════════════════════════════
// Search
// ═══════════════════════════════════════════════════════════════════

pub const Search = struct {
    handle: *c.kb_search_t,

    pub fn create(conn: *Connection, database: [:0]const u8) KbError!Search {
        var ks: ?*c.kb_search_t = null;
        try check(c.kb_search_create(conn.handle, database.ptr, &ks));
        return .{ .handle = ks orelse return error.NullArg };
    }

    pub fn destroy(self: *Search) void {
        c.kb_search_destroy(self.handle);
    }

    pub fn clear(self: *Search) void {
        c.kb_search_clear(self.handle);
    }

    // ── Filter chain ────────────────────────────────────────────────

    pub fn label(self: *Search, lbl: [:0]const u8) KbError!void {
        try check(c.kb_search_label(self.handle, lbl.ptr));
    }

    pub fn name(self: *Search, n: [:0]const u8) KbError!void {
        try check(c.kb_search_name(self.handle, n.ptr));
    }

    pub fn propertyKey(self: *Search, key: [:0]const u8) KbError!void {
        try check(c.kb_search_property_key(self.handle, key.ptr));
    }

    pub fn propertyValue(self: *Search, key: [:0]const u8, value: [:0]const u8) KbError!void {
        try check(c.kb_search_property_value(self.handle, key.ptr, value.ptr));
    }

    pub fn path(self: *Search, pattern: [:0]const u8) KbError!void {
        try check(c.kb_search_path(self.handle, pattern.ptr));
    }

    pub fn execute(self: *Search) KbError!void {
        try check(c.kb_search_execute(self.handle));
    }

    pub fn results(self: *const Search) ?*const c.kb_resultset_t {
        return c.kb_search_results(self.handle);
    }

    // ── Convenience finders ─────────────────────────────────────────

    pub const PathList = struct {
        paths: ?[*]?[*:0]u8,
        count: c_int,

        pub fn deinit(self: *PathList) void {
            c.kb_free_paths(@ptrCast(self.paths), self.count);
        }

        pub fn len(self: *const PathList) usize {
            return @intCast(self.count);
        }

        pub fn get(self: *const PathList, idx: usize) ?[:0]const u8 {
            if (self.paths) |p| {
                if (p[@intCast(idx)]) |s| return std.mem.span(s);
            }
            return null;
        }
    };

    pub fn findStatusPaths(self: *Search) KbError!PathList {
        var pl = PathList{ .paths = null, .count = 0 };
        try check(c.kb_find_status_paths(self.handle, @ptrCast(&pl.paths), &pl.count));
        return pl;
    }

    pub fn findJobPaths(self: *Search) KbError!PathList {
        var pl = PathList{ .paths = null, .count = 0 };
        try check(c.kb_find_job_paths(self.handle, @ptrCast(&pl.paths), &pl.count));
        return pl;
    }

    pub fn findStreamPaths(self: *Search) KbError!PathList {
        var pl = PathList{ .paths = null, .count = 0 };
        try check(c.kb_find_stream_paths(self.handle, @ptrCast(&pl.paths), &pl.count));
        return pl;
    }

    pub fn findBitStructurePaths(self: *Search) KbError!PathList {
        var pl = PathList{ .paths = null, .count = 0 };
        try check(c.kb_find_bit_structure_paths(self.handle, @ptrCast(&pl.paths), &pl.count));
        return pl;
    }

    pub fn findRpcServerPaths(self: *Search) KbError!PathList {
        var pl = PathList{ .paths = null, .count = 0 };
        try check(c.kb_find_rpc_server_paths(self.handle, @ptrCast(&pl.paths), &pl.count));
        return pl;
    }

    pub fn findRpcClientPaths(self: *Search) KbError!PathList {
        var pl = PathList{ .paths = null, .count = 0 };
        try check(c.kb_find_rpc_client_paths(self.handle, @ptrCast(&pl.paths), &pl.count));
        return pl;
    }

    pub fn findDocumentPaths(self: *Search) KbError!PathList {
        var pl = PathList{ .paths = null, .count = 0 };
        try check(c.kb_find_document_paths(self.handle, @ptrCast(&pl.paths), &pl.count));
        return pl;
    }

    pub fn findLinkPaths(self: *Search) KbError!PathList {
        var pl = PathList{ .paths = null, .count = 0 };
        try check(c.kb_find_link_paths(self.handle, @ptrCast(&pl.paths), &pl.count));
        return pl;
    }

    pub fn findLinkMountPaths(self: *Search) KbError!PathList {
        var pl = PathList{ .paths = null, .count = 0 };
        try check(c.kb_find_link_mount_paths(self.handle, @ptrCast(&pl.paths), &pl.count));
        return pl;
    }

    pub fn findNodePaths(self: *Search, lbl: [:0]const u8) KbError!PathList {
        var pl = PathList{ .paths = null, .count = 0 };
        try check(c.kb_find_node_paths(self.handle, lbl.ptr, @ptrCast(&pl.paths), &pl.count));
        return pl;
    }

    pub fn findDescription(self: *Search, p: [:0]const u8) KbError!?[:0]const u8 {
        var out: ?[*:0]u8 = null;
        try check(c.kb_search_find_description(self.handle, p.ptr, @ptrCast(&out)));
        if (out) |o| return std.mem.span(o);
        return null;
    }
};

// ═══════════════════════════════════════════════════════════════════
// Status
// ═══════════════════════════════════════════════════════════════════

pub const Status = struct {
    conn: *Connection,
    database: [:0]const u8,

    pub fn init(conn: *Connection, database: [:0]const u8) Status {
        return .{ .conn = conn, .database = database };
    }

    pub fn get(self: *const Status, path: [:0]const u8) KbError!?[:0]const u8 {
        var out: ?[*:0]u8 = null;
        try check(c.kb_status_get(self.conn.handle, self.database.ptr, path.ptr, @ptrCast(&out)));
        if (out) |o| return std.mem.span(o);
        return null;
    }

    pub fn set(
        self: *const Status,
        path: [:0]const u8,
        data: [:0]const u8,
        max_retries: c_int,
        base_delay_ms: c_int,
    ) KbError!void {
        try check(c.kb_status_set(
            self.conn.handle,
            self.database.ptr,
            path.ptr,
            data.ptr,
            max_retries,
            base_delay_ms,
        ));
    }

    /// Set with default retry parameters.
    pub fn setDefault(self: *const Status, path: [:0]const u8, data: [:0]const u8) KbError!void {
        try self.set(path, data, default_max_retries, default_base_delay_ms);
    }
};

// ═══════════════════════════════════════════════════════════════════
// Job Queue
// ═══════════════════════════════════════════════════════════════════

pub const JobQueue = struct {
    conn: *Connection,
    database: [:0]const u8,

    pub fn init(conn: *Connection, database: [:0]const u8) JobQueue {
        return .{ .conn = conn, .database = database };
    }

    pub fn freeCount(self: *const JobQueue, path: [:0]const u8) KbError!c_int {
        var count: c_int = 0;
        try check(c.kb_job_free_count(self.conn.handle, self.database.ptr, path.ptr, &count));
        return count;
    }

    pub fn queuedCount(self: *const JobQueue, path: [:0]const u8) KbError!c_int {
        var count: c_int = 0;
        try check(c.kb_job_queued_count(self.conn.handle, self.database.ptr, path.ptr, &count));
        return count;
    }

    pub fn activeCount(self: *const JobQueue, path: [:0]const u8) KbError!c_int {
        var count: c_int = 0;
        try check(c.kb_job_active_count(self.conn.handle, self.database.ptr, path.ptr, &count));
        return count;
    }

    pub fn push(
        self: *const JobQueue,
        path: [:0]const u8,
        data: [:0]const u8,
        max_retries: c_int,
        base_delay_ms: c_int,
    ) KbError!void {
        try check(c.kb_job_push(
            self.conn.handle,
            self.database.ptr,
            path.ptr,
            data.ptr,
            max_retries,
            base_delay_ms,
        ));
    }

    pub fn pushDefault(self: *const JobQueue, path: [:0]const u8, data: [:0]const u8) KbError!void {
        try self.push(path, data, default_max_retries, default_base_delay_ms);
    }

    pub const PeekResult = struct {
        found: bool,
        id: c_int,
        data: ?[:0]const u8,
        _raw: c.kb_job_info_t,

        pub fn deinit(self: *PeekResult) void {
            if (self._raw.data) |d| std.c.free(d);
        }
    };

    pub fn peek(
        self: *const JobQueue,
        path: [:0]const u8,
        max_retries: c_int,
        base_delay_ms: c_int,
    ) KbError!PeekResult {
        var info: c.kb_job_info_t = std.mem.zeroes(c.kb_job_info_t);
        try check(c.kb_job_peek(
            self.conn.handle,
            self.database.ptr,
            path.ptr,
            &info,
            max_retries,
            base_delay_ms,
        ));
        return .{
            .found = info.found,
            .id = info.id,
            .data = if (info.data) |d| std.mem.span(d) else null,
            ._raw = info,
        };
    }

    pub fn peekDefault(self: *const JobQueue, path: [:0]const u8) KbError!PeekResult {
        return self.peek(path, default_max_retries, default_base_delay_ms);
    }

    pub fn complete(self: *const JobQueue, job_id: c_int, max_retries: c_int, base_delay_ms: c_int) KbError!void {
        try check(c.kb_job_complete(self.conn.handle, self.database.ptr, job_id, max_retries, base_delay_ms));
    }

    pub fn completeDefault(self: *const JobQueue, job_id: c_int) KbError!void {
        try self.complete(job_id, default_max_retries, default_base_delay_ms);
    }

    pub fn clear(self: *const JobQueue, path: [:0]const u8, max_retries: c_int, base_delay_ms: c_int) KbError!void {
        try check(c.kb_job_clear(self.conn.handle, self.database.ptr, path.ptr, max_retries, base_delay_ms));
    }

    pub fn clearDefault(self: *const JobQueue, path: [:0]const u8) KbError!void {
        try self.clear(path, default_max_retries, default_base_delay_ms);
    }

    pub fn listPending(self: *const JobQueue, path: [:0]const u8) KbError!ResultSet {
        var rs: ?*c.kb_resultset_t = null;
        try check(c.kb_job_list_pending(self.conn.handle, self.database.ptr, path.ptr, &rs));
        return .{ .handle = rs orelse return error.NullArg };
    }

    pub fn listActive(self: *const JobQueue, path: [:0]const u8) KbError!ResultSet {
        var rs: ?*c.kb_resultset_t = null;
        try check(c.kb_job_list_active(self.conn.handle, self.database.ptr, path.ptr, &rs));
        return .{ .handle = rs orelse return error.NullArg };
    }
};

// ═══════════════════════════════════════════════════════════════════
// Stream
// ═══════════════════════════════════════════════════════════════════

pub const Stream = struct {
    conn: *Connection,
    database: [:0]const u8,

    pub fn init(conn: *Connection, database: [:0]const u8) Stream {
        return .{ .conn = conn, .database = database };
    }

    pub fn push(
        self: *const Stream,
        path: [:0]const u8,
        data: [:0]const u8,
        max_retries: c_int,
        base_delay_ms: c_int,
    ) KbError!void {
        try check(c.kb_stream_push(self.conn.handle, self.database.ptr, path.ptr, data.ptr, max_retries, base_delay_ms));
    }

    pub fn pushDefault(self: *const Stream, path: [:0]const u8, data: [:0]const u8) KbError!void {
        try self.push(path, data, default_max_retries, default_base_delay_ms);
    }

    pub fn list(
        self: *const Stream,
        path: [:0]const u8,
        after: ?[:0]const u8,
        before: ?[:0]const u8,
    ) KbError!ResultSet {
        var rs: ?*c.kb_resultset_t = null;
        try check(c.kb_stream_list(
            self.conn.handle,
            self.database.ptr,
            path.ptr,
            if (after) |a| a.ptr else null,
            if (before) |b| b.ptr else null,
            &rs,
        ));
        return .{ .handle = rs orelse return error.NullArg };
    }

    pub fn clear(self: *const Stream, path: [:0]const u8, max_retries: c_int, base_delay_ms: c_int) KbError!void {
        try check(c.kb_stream_clear(self.conn.handle, self.database.ptr, path.ptr, max_retries, base_delay_ms));
    }

    pub fn clearDefault(self: *const Stream, path: [:0]const u8) KbError!void {
        try self.clear(path, default_max_retries, default_base_delay_ms);
    }

    pub fn count(self: *const Stream, path: [:0]const u8) KbError!c_int {
        var cnt: c_int = 0;
        try check(c.kb_stream_count(self.conn.handle, self.database.ptr, path.ptr, &cnt));
        return cnt;
    }

    pub fn countTotal(self: *const Stream, path: [:0]const u8) KbError!c_int {
        var cnt: c_int = 0;
        try check(c.kb_stream_count_total(self.conn.handle, self.database.ptr, path.ptr, &cnt));
        return cnt;
    }

    pub fn latest(self: *const Stream, path: [:0]const u8) KbError!ResultSet {
        var rs: ?*c.kb_resultset_t = null;
        try check(c.kb_stream_latest(self.conn.handle, self.database.ptr, path.ptr, &rs));
        return .{ .handle = rs orelse return error.NullArg };
    }

    pub fn range(
        self: *const Stream,
        path: [:0]const u8,
        start_time: [:0]const u8,
        end_time: [:0]const u8,
    ) KbError!ResultSet {
        var rs: ?*c.kb_resultset_t = null;
        try check(c.kb_stream_range(self.conn.handle, self.database.ptr, path.ptr, start_time.ptr, end_time.ptr, &rs));
        return .{ .handle = rs orelse return error.NullArg };
    }

    pub fn getById(self: *const Stream, row_id: c_int) KbError!ResultSet {
        var rs: ?*c.kb_resultset_t = null;
        try check(c.kb_stream_get_by_id(self.conn.handle, self.database.ptr, row_id, &rs));
        return .{ .handle = rs orelse return error.NullArg };
    }

    pub fn statistics(self: *const Stream, path: [:0]const u8) KbError!ResultSet {
        var rs: ?*c.kb_resultset_t = null;
        try check(c.kb_stream_statistics(self.conn.handle, self.database.ptr, path.ptr, &rs));
        return .{ .handle = rs orelse return error.NullArg };
    }
};

// ═══════════════════════════════════════════════════════════════════
// Bit Structures
// ═══════════════════════════════════════════════════════════════════

pub const BitStructures = struct {
    conn: *Connection,
    database: [:0]const u8,

    pub fn init(conn: *Connection, database: [:0]const u8) BitStructures {
        return .{ .conn = conn, .database = database };
    }

    pub fn getMask(self: *const BitStructures, path: [:0]const u8) KbError!i64 {
        var mask: i64 = 0;
        try check(c.kb_bit_get_mask(self.conn.handle, self.database.ptr, path.ptr, &mask));
        return mask;
    }

    pub fn setMask(
        self: *const BitStructures,
        path: [:0]const u8,
        mask: i64,
        max_retries: c_int,
        base_delay_ms: c_int,
    ) KbError!void {
        try check(c.kb_bit_set_mask(self.conn.handle, self.database.ptr, path.ptr, mask, max_retries, base_delay_ms));
    }

    pub fn setMaskDefault(self: *const BitStructures, path: [:0]const u8, mask: i64) KbError!void {
        try self.setMask(path, mask, default_max_retries, default_base_delay_ms);
    }

    pub fn setBit(
        self: *const BitStructures,
        path: [:0]const u8,
        bit_pos: c_int,
        value: bool,
        max_retries: c_int,
        base_delay_ms: c_int,
    ) KbError!void {
        try check(c.kb_bit_set(self.conn.handle, self.database.ptr, path.ptr, bit_pos, value, max_retries, base_delay_ms));
    }

    pub fn setBitDefault(self: *const BitStructures, path: [:0]const u8, bit_pos: c_int, value: bool) KbError!void {
        try self.setBit(path, bit_pos, value, default_max_retries, default_base_delay_ms);
    }

    pub fn getBit(self: *const BitStructures, path: [:0]const u8, bit_pos: c_int) KbError!bool {
        var val: bool = false;
        try check(c.kb_bit_get(self.conn.handle, self.database.ptr, path.ptr, bit_pos, &val));
        return val;
    }

    /// Evaluate an S-expression (JSON array format) against the bit mask.
    pub fn evalSexpr(
        self: *const BitStructures,
        path: [:0]const u8,
        sexpr_json: [:0]const u8,
        defs: ?*const c.kb_bit_defs_t,
        prev_mask: i64,
    ) KbError!bool {
        var result: bool = false;
        try check(c.kb_bit_eval_sexpr(
            self.conn.handle,
            self.database.ptr,
            path.ptr,
            sexpr_json.ptr,
            defs,
            prev_mask,
            &result,
        ));
        return result;
    }
};

// ═══════════════════════════════════════════════════════════════════
// RPC Server
// ═══════════════════════════════════════════════════════════════════

pub const RpcServer = struct {
    conn: *Connection,
    database: [:0]const u8,

    pub fn init(conn: *Connection, database: [:0]const u8) RpcServer {
        return .{ .conn = conn, .database = database };
    }

    pub fn countNew(self: *const RpcServer, path: [:0]const u8) KbError!c_int {
        var cnt: c_int = 0;
        try check(c.kb_rpc_server_count_new(self.conn.handle, self.database.ptr, path.ptr, &cnt));
        return cnt;
    }

    pub fn countProcessing(self: *const RpcServer, path: [:0]const u8) KbError!c_int {
        var cnt: c_int = 0;
        try check(c.kb_rpc_server_count_processing(self.conn.handle, self.database.ptr, path.ptr, &cnt));
        return cnt;
    }

    pub fn push(
        self: *const RpcServer,
        server_path: [:0]const u8,
        request_id: [:0]const u8,
        rpc_action: [:0]const u8,
        request_payload: [:0]const u8,
        transaction_tag: [:0]const u8,
        priority: c_int,
        client_path: [:0]const u8,
        max_retries: c_int,
        base_delay_ms: c_int,
    ) KbError!void {
        try check(c.kb_rpc_server_push(
            self.conn.handle,
            self.database.ptr,
            server_path.ptr,
            request_id.ptr,
            rpc_action.ptr,
            request_payload.ptr,
            transaction_tag.ptr,
            priority,
            client_path.ptr,
            max_retries,
            base_delay_ms,
        ));
    }

    pub const PeekResult = struct {
        found: bool,
        id: c_int,
        server_path: ?[:0]const u8,
        request_id: ?[:0]const u8,
        rpc_action: ?[:0]const u8,
        request_payload: ?[:0]const u8,
        transaction_tag: ?[:0]const u8,
        state: ?[:0]const u8,
        priority: c_int,
        rpc_client_queue: ?[:0]const u8,
        _raw: c.kb_rpc_server_job_t,

        pub fn deinit(self: *PeekResult) void {
            c.kb_rpc_server_job_free(&self._raw);
        }
    };

    pub fn peek(
        self: *const RpcServer,
        path: [:0]const u8,
        max_retries: c_int,
        base_delay_ms: c_int,
    ) KbError!PeekResult {
        var job: c.kb_rpc_server_job_t = std.mem.zeroes(c.kb_rpc_server_job_t);
        try check(c.kb_rpc_server_peek(self.conn.handle, self.database.ptr, path.ptr, &job, max_retries, base_delay_ms));
        return .{
            .found = job.found,
            .id = job.id,
            .server_path = optSpan(job.server_path),
            .request_id = optSpan(job.request_id),
            .rpc_action = optSpan(job.rpc_action),
            .request_payload = optSpan(job.request_payload),
            .transaction_tag = optSpan(job.transaction_tag),
            .state = optSpan(job.state),
            .priority = job.priority,
            .rpc_client_queue = optSpan(job.rpc_client_queue),
            ._raw = job,
        };
    }

    pub fn peekDefault(self: *const RpcServer, path: [:0]const u8) KbError!PeekResult {
        return self.peek(path, default_max_retries, default_base_delay_ms);
    }

    pub fn complete(
        self: *const RpcServer,
        path: [:0]const u8,
        job_id: c_int,
        max_retries: c_int,
        base_delay_ms: c_int,
    ) KbError!void {
        try check(c.kb_rpc_server_complete(self.conn.handle, self.database.ptr, path.ptr, job_id, max_retries, base_delay_ms));
    }

    pub fn completeDefault(self: *const RpcServer, path: [:0]const u8, job_id: c_int) KbError!void {
        try self.complete(path, job_id, default_max_retries, default_base_delay_ms);
    }

    pub fn clear(self: *const RpcServer, path: [:0]const u8, max_retries: c_int, base_delay_ms: c_int) KbError!void {
        try check(c.kb_rpc_server_clear(self.conn.handle, self.database.ptr, path.ptr, max_retries, base_delay_ms));
    }

    pub fn clearDefault(self: *const RpcServer, path: [:0]const u8) KbError!void {
        try self.clear(path, default_max_retries, default_base_delay_ms);
    }
};

// ═══════════════════════════════════════════════════════════════════
// RPC Client
// ═══════════════════════════════════════════════════════════════════

pub const RpcClient = struct {
    conn: *Connection,
    database: [:0]const u8,

    pub fn init(conn: *Connection, database: [:0]const u8) RpcClient {
        return .{ .conn = conn, .database = database };
    }

    pub fn freeSlots(self: *const RpcClient, path: [:0]const u8) KbError!c_int {
        var cnt: c_int = 0;
        try check(c.kb_rpc_client_free_slots(self.conn.handle, self.database.ptr, path.ptr, &cnt));
        return cnt;
    }

    pub fn queuedSlots(self: *const RpcClient, path: [:0]const u8) KbError!c_int {
        var cnt: c_int = 0;
        try check(c.kb_rpc_client_queued_slots(self.conn.handle, self.database.ptr, path.ptr, &cnt));
        return cnt;
    }

    pub fn pushReply(
        self: *const RpcClient,
        client_path: [:0]const u8,
        request_id: [:0]const u8,
        server_path: [:0]const u8,
        rpc_action: [:0]const u8,
        transaction_tag: [:0]const u8,
        response_payload: [:0]const u8,
        max_retries: c_int,
        base_delay_ms: c_int,
    ) KbError!void {
        try check(c.kb_rpc_client_push_reply(
            self.conn.handle,
            self.database.ptr,
            client_path.ptr,
            request_id.ptr,
            server_path.ptr,
            rpc_action.ptr,
            transaction_tag.ptr,
            response_payload.ptr,
            max_retries,
            base_delay_ms,
        ));
    }

    pub const PeekReplyResult = struct {
        found: bool,
        id: c_int,
        request_id: ?[:0]const u8,
        client_path: ?[:0]const u8,
        server_path: ?[:0]const u8,
        rpc_action: ?[:0]const u8,
        response_payload: ?[:0]const u8,
        _raw: c.kb_rpc_client_reply_t,

        pub fn deinit(self: *PeekReplyResult) void {
            c.kb_rpc_client_reply_free(&self._raw);
        }
    };

    pub fn peekReply(
        self: *const RpcClient,
        path: [:0]const u8,
        max_retries: c_int,
        base_delay_ms: c_int,
    ) KbError!PeekReplyResult {
        var reply: c.kb_rpc_client_reply_t = std.mem.zeroes(c.kb_rpc_client_reply_t);
        try check(c.kb_rpc_client_peek_reply(self.conn.handle, self.database.ptr, path.ptr, &reply, max_retries, base_delay_ms));
        return .{
            .found = reply.found,
            .id = reply.id,
            .request_id = optSpan(reply.request_id),
            .client_path = optSpan(reply.client_path),
            .server_path = optSpan(reply.server_path),
            .rpc_action = optSpan(reply.rpc_action),
            .response_payload = optSpan(reply.response_payload),
            ._raw = reply,
        };
    }

    pub fn peekReplyDefault(self: *const RpcClient, path: [:0]const u8) KbError!PeekReplyResult {
        return self.peekReply(path, default_max_retries, default_base_delay_ms);
    }

    pub fn clear(self: *const RpcClient, path: [:0]const u8, max_retries: c_int, base_delay_ms: c_int) KbError!void {
        try check(c.kb_rpc_client_clear(self.conn.handle, self.database.ptr, path.ptr, max_retries, base_delay_ms));
    }

    pub fn clearDefault(self: *const RpcClient, path: [:0]const u8) KbError!void {
        try self.clear(path, default_max_retries, default_base_delay_ms);
    }
};

// ═══════════════════════════════════════════════════════════════════
// Link Tables
// ═══════════════════════════════════════════════════════════════════

pub const LinkTable = struct {
    conn: *Connection,
    database: [:0]const u8,

    pub fn init(conn: *Connection, database: [:0]const u8) LinkTable {
        return .{ .conn = conn, .database = database };
    }

    pub fn queryByPath(self: *const LinkTable, path: [:0]const u8) KbError!ResultSet {
        var rs: ?*c.kb_resultset_t = null;
        try check(c.kb_link_query(self.conn.handle, self.database.ptr, path.ptr, &rs));
        return .{ .handle = rs orelse return error.NullArg };
    }

    pub fn queryByName(self: *const LinkTable, link_name: [:0]const u8) KbError!ResultSet {
        var rs: ?*c.kb_resultset_t = null;
        try check(c.kb_link_query_by_name(self.conn.handle, self.database.ptr, link_name.ptr, &rs));
        return .{ .handle = rs orelse return error.NullArg };
    }

    pub fn decodeNodes(self: *const LinkTable, path: [:0]const u8) KbError!Search.PathList {
        var pl = Search.PathList{ .paths = null, .count = 0 };
        try check(c.kb_link_decode_nodes(self.conn.handle, self.database.ptr, path.ptr, @ptrCast(&pl.paths), &pl.count));
        return pl;
    }
};

pub const LinkMountTable = struct {
    conn: *Connection,
    database: [:0]const u8,

    pub fn init(conn: *Connection, database: [:0]const u8) LinkMountTable {
        return .{ .conn = conn, .database = database };
    }

    pub fn queryByPath(self: *const LinkMountTable, path: [:0]const u8) KbError!ResultSet {
        var rs: ?*c.kb_resultset_t = null;
        try check(c.kb_link_mount_query(self.conn.handle, self.database.ptr, path.ptr, &rs));
        return .{ .handle = rs orelse return error.NullArg };
    }

    pub fn queryByName(self: *const LinkMountTable, link_name: [:0]const u8) KbError!ResultSet {
        var rs: ?*c.kb_resultset_t = null;
        try check(c.kb_link_mount_query_by_name(self.conn.handle, self.database.ptr, link_name.ptr, &rs));
        return .{ .handle = rs orelse return error.NullArg };
    }
};

// ═══════════════════════════════════════════════════════════════════
// Document Table
// ═══════════════════════════════════════════════════════════════════

pub const Document = struct {
    conn: *Connection,
    database: [:0]const u8,

    pub fn init(conn: *Connection, database: [:0]const u8) Document {
        return .{ .conn = conn, .database = database };
    }

    // ── Core Read ───────────────────────────────────────────────────

    /// Get a value from the JSONB data field.
    /// json_path: dot-separated ("address.city"), or null for entire doc.
    /// as_text: true for text extraction (->>), false for JSON (->).
    /// doc_type: optional type filter, or null.
    /// Returns heap-allocated string; caller must free with freeCStr.
    pub fn get(
        self: *const Document,
        ltree_path: [:0]const u8,
        json_path: ?[:0]const u8,
        as_text: bool,
        doc_type: ?[:0]const u8,
    ) KbError!?[:0]const u8 {
        var out: ?[*:0]u8 = null;
        try check(c.kb_doc_get(
            self.conn.handle,
            self.database.ptr,
            ltree_path.ptr,
            if (json_path) |p| p.ptr else null,
            as_text,
            if (doc_type) |t| t.ptr else null,
            @ptrCast(&out),
        ));
        if (out) |o| return std.mem.span(o);
        return null;
    }

    // ── Core Write ──────────────────────────────────────────────────

    pub fn set(
        self: *const Document,
        ltree_path: [:0]const u8,
        json_path: ?[:0]const u8,
        value_json: [:0]const u8,
        create_missing: bool,
        doc_type: ?[:0]const u8,
    ) KbError!void {
        try check(c.kb_doc_set(
            self.conn.handle,
            self.database.ptr,
            ltree_path.ptr,
            if (json_path) |p| p.ptr else null,
            value_json.ptr,
            create_missing,
            if (doc_type) |t| t.ptr else null,
        ));
    }

    pub fn deleteKey(
        self: *const Document,
        ltree_path: [:0]const u8,
        key: [:0]const u8,
        doc_type: ?[:0]const u8,
    ) KbError!void {
        try check(c.kb_doc_delete_key(
            self.conn.handle,
            self.database.ptr,
            ltree_path.ptr,
            key.ptr,
            if (doc_type) |t| t.ptr else null,
        ));
    }

    pub fn deletePath(
        self: *const Document,
        ltree_path: [:0]const u8,
        json_path: [:0]const u8,
        doc_type: ?[:0]const u8,
    ) KbError!void {
        try check(c.kb_doc_delete_path(
            self.conn.handle,
            self.database.ptr,
            ltree_path.ptr,
            json_path.ptr,
            if (doc_type) |t| t.ptr else null,
        ));
    }

    // ── Key Existence ───────────────────────────────────────────────

    pub fn hasKey(
        self: *const Document,
        ltree_path: [:0]const u8,
        key: [:0]const u8,
        doc_type: ?[:0]const u8,
    ) KbError!bool {
        var result: bool = false;
        try check(c.kb_doc_has_key(
            self.conn.handle,
            self.database.ptr,
            ltree_path.ptr,
            key.ptr,
            if (doc_type) |t| t.ptr else null,
            &result,
        ));
        return result;
    }

    // ── Containment ─────────────────────────────────────────────────

    pub fn contains(
        self: *const Document,
        ltree_path: [:0]const u8,
        contained_json: [:0]const u8,
        doc_type: ?[:0]const u8,
    ) KbError!bool {
        var result: bool = false;
        try check(c.kb_doc_contains(
            self.conn.handle,
            self.database.ptr,
            ltree_path.ptr,
            contained_json.ptr,
            if (doc_type) |t| t.ptr else null,
            &result,
        ));
        return result;
    }

    pub fn containedBy(
        self: *const Document,
        ltree_path: [:0]const u8,
        container_json: [:0]const u8,
        doc_type: ?[:0]const u8,
    ) KbError!bool {
        var result: bool = false;
        try check(c.kb_doc_contained_by(
            self.conn.handle,
            self.database.ptr,
            ltree_path.ptr,
            container_json.ptr,
            if (doc_type) |t| t.ptr else null,
            &result,
        ));
        return result;
    }

    // ── JSONPath ────────────────────────────────────────────────────

    pub fn pathExists(
        self: *const Document,
        ltree_path: [:0]const u8,
        jsonpath_query: [:0]const u8,
        doc_type: ?[:0]const u8,
    ) KbError!bool {
        var result: bool = false;
        try check(c.kb_doc_path_exists(
            self.conn.handle,
            self.database.ptr,
            ltree_path.ptr,
            jsonpath_query.ptr,
            if (doc_type) |t| t.ptr else null,
            &result,
        ));
        return result;
    }

    pub fn pathQuery(
        self: *const Document,
        ltree_path: [:0]const u8,
        jsonpath_query: [:0]const u8,
        doc_type: ?[:0]const u8,
    ) KbError!?[:0]const u8 {
        var out: ?[*:0]u8 = null;
        try check(c.kb_doc_path_query(
            self.conn.handle,
            self.database.ptr,
            ltree_path.ptr,
            jsonpath_query.ptr,
            if (doc_type) |t| t.ptr else null,
            @ptrCast(&out),
        ));
        if (out) |o| return std.mem.span(o);
        return null;
    }

    // ── Array Operations ────────────────────────────────────────────

    pub fn arrayAppend(
        self: *const Document,
        ltree_path: [:0]const u8,
        json_path: [:0]const u8,
        item_json: [:0]const u8,
        doc_type: ?[:0]const u8,
    ) KbError!void {
        try check(c.kb_doc_array_append(
            self.conn.handle,
            self.database.ptr,
            ltree_path.ptr,
            json_path.ptr,
            item_json.ptr,
            if (doc_type) |t| t.ptr else null,
        ));
    }

    pub fn arrayPrepend(
        self: *const Document,
        ltree_path: [:0]const u8,
        json_path: [:0]const u8,
        item_json: [:0]const u8,
        doc_type: ?[:0]const u8,
    ) KbError!void {
        try check(c.kb_doc_array_prepend(
            self.conn.handle,
            self.database.ptr,
            ltree_path.ptr,
            json_path.ptr,
            item_json.ptr,
            if (doc_type) |t| t.ptr else null,
        ));
    }

    pub fn arrayContains(
        self: *const Document,
        ltree_path: [:0]const u8,
        json_path: [:0]const u8,
        item_json: [:0]const u8,
        doc_type: ?[:0]const u8,
    ) KbError!bool {
        var result: bool = false;
        try check(c.kb_doc_array_contains(
            self.conn.handle,
            self.database.ptr,
            ltree_path.ptr,
            json_path.ptr,
            item_json.ptr,
            if (doc_type) |t| t.ptr else null,
            &result,
        ));
        return result;
    }

    // ── Queue (FIFO) ────────────────────────────────────────────────

    pub fn enqueue(
        self: *const Document,
        ltree_path: [:0]const u8,
        item_json: [:0]const u8,
        queue_path: ?[:0]const u8,
        doc_type: ?[:0]const u8,
    ) KbError!void {
        try check(c.kb_doc_enqueue(
            self.conn.handle,
            self.database.ptr,
            ltree_path.ptr,
            item_json.ptr,
            if (queue_path) |q| q.ptr else null,
            if (doc_type) |t| t.ptr else null,
        ));
    }

    pub fn dequeue(
        self: *const Document,
        ltree_path: [:0]const u8,
        queue_path: ?[:0]const u8,
        doc_type: ?[:0]const u8,
    ) KbError!?[:0]const u8 {
        var out: ?[*:0]u8 = null;
        try check(c.kb_doc_dequeue(
            self.conn.handle,
            self.database.ptr,
            ltree_path.ptr,
            if (queue_path) |q| q.ptr else null,
            if (doc_type) |t| t.ptr else null,
            @ptrCast(&out),
        ));
        if (out) |o| return std.mem.span(o);
        return null;
    }

    pub fn queuePeek(
        self: *const Document,
        ltree_path: [:0]const u8,
        queue_path: ?[:0]const u8,
        index: c_int,
        doc_type: ?[:0]const u8,
    ) KbError!?[:0]const u8 {
        var out: ?[*:0]u8 = null;
        try check(c.kb_doc_peek(
            self.conn.handle,
            self.database.ptr,
            ltree_path.ptr,
            if (queue_path) |q| q.ptr else null,
            index,
            if (doc_type) |t| t.ptr else null,
            @ptrCast(&out),
        ));
        if (out) |o| return std.mem.span(o);
        return null;
    }

    pub fn queueSize(
        self: *const Document,
        ltree_path: [:0]const u8,
        queue_path: ?[:0]const u8,
        doc_type: ?[:0]const u8,
    ) KbError!c_int {
        var size: c_int = 0;
        try check(c.kb_doc_queue_size(
            self.conn.handle,
            self.database.ptr,
            ltree_path.ptr,
            if (queue_path) |q| q.ptr else null,
            if (doc_type) |t| t.ptr else null,
            &size,
        ));
        return size;
    }

    pub fn queueIsEmpty(
        self: *const Document,
        ltree_path: [:0]const u8,
        queue_path: ?[:0]const u8,
        doc_type: ?[:0]const u8,
    ) KbError!bool {
        var empty: bool = false;
        try check(c.kb_doc_queue_is_empty(
            self.conn.handle,
            self.database.ptr,
            ltree_path.ptr,
            if (queue_path) |q| q.ptr else null,
            if (doc_type) |t| t.ptr else null,
            &empty,
        ));
        return empty;
    }

    pub fn queueClear(
        self: *const Document,
        ltree_path: [:0]const u8,
        queue_path: ?[:0]const u8,
        doc_type: ?[:0]const u8,
    ) KbError!void {
        try check(c.kb_doc_queue_clear(
            self.conn.handle,
            self.database.ptr,
            ltree_path.ptr,
            if (queue_path) |q| q.ptr else null,
            if (doc_type) |t| t.ptr else null,
        ));
    }

    pub fn queueGetAll(
        self: *const Document,
        ltree_path: [:0]const u8,
        queue_path: ?[:0]const u8,
        doc_type: ?[:0]const u8,
    ) KbError!?[:0]const u8 {
        var out: ?[*:0]u8 = null;
        try check(c.kb_doc_queue_get_all(
            self.conn.handle,
            self.database.ptr,
            ltree_path.ptr,
            if (queue_path) |q| q.ptr else null,
            if (doc_type) |t| t.ptr else null,
            @ptrCast(&out),
        ));
        if (out) |o| return std.mem.span(o);
        return null;
    }

    // ── Stack (LIFO) ────────────────────────────────────────────────

    pub fn stackPush(
        self: *const Document,
        ltree_path: [:0]const u8,
        item_json: [:0]const u8,
        queue_path: ?[:0]const u8,
        doc_type: ?[:0]const u8,
    ) KbError!void {
        try check(c.kb_doc_push(
            self.conn.handle,
            self.database.ptr,
            ltree_path.ptr,
            item_json.ptr,
            if (queue_path) |q| q.ptr else null,
            if (doc_type) |t| t.ptr else null,
        ));
    }

    pub fn stackPop(
        self: *const Document,
        ltree_path: [:0]const u8,
        queue_path: ?[:0]const u8,
        doc_type: ?[:0]const u8,
    ) KbError!?[:0]const u8 {
        var out: ?[*:0]u8 = null;
        try check(c.kb_doc_pop(
            self.conn.handle,
            self.database.ptr,
            ltree_path.ptr,
            if (queue_path) |q| q.ptr else null,
            if (doc_type) |t| t.ptr else null,
            @ptrCast(&out),
        ));
        if (out) |o| return std.mem.span(o);
        return null;
    }

    // ── Metadata ────────────────────────────────────────────────────

    pub fn getMetadata(
        self: *const Document,
        ltree_path: [:0]const u8,
        metadata_path: [:0]const u8,
        doc_type: ?[:0]const u8,
    ) KbError!?[:0]const u8 {
        var out: ?[*:0]u8 = null;
        try check(c.kb_doc_get_metadata(
            self.conn.handle,
            self.database.ptr,
            ltree_path.ptr,
            metadata_path.ptr,
            if (doc_type) |t| t.ptr else null,
            @ptrCast(&out),
        ));
        if (out) |o| return std.mem.span(o);
        return null;
    }

    pub fn setMetadata(
        self: *const Document,
        ltree_path: [:0]const u8,
        metadata_path: [:0]const u8,
        metadata_json: [:0]const u8,
        doc_type: ?[:0]const u8,
    ) KbError!void {
        try check(c.kb_doc_set_metadata(
            self.conn.handle,
            self.database.ptr,
            ltree_path.ptr,
            metadata_path.ptr,
            metadata_json.ptr,
            if (doc_type) |t| t.ptr else null,
        ));
    }
};

// ═══════════════════════════════════════════════════════════════════
// Utility
// ═══════════════════════════════════════════════════════════════════

/// Get current UTC timestamp as ISO-8601 string. Caller must free.
pub fn timestampNow() ?[:0]const u8 {
    const ptr = c.kb_timestamp_now();
    if (ptr) |p| return std.mem.span(p);
    return null;
}

// ── Internal helpers ────────────────────────────────────────────────

fn optSpan(ptr: ?[*:0]u8) ?[:0]const u8 {
    if (ptr) |p| return std.mem.span(p);
    return null;
}

// ═══════════════════════════════════════════════════════════════════
// Compile-time tests (basic type checks)
// ═══════════════════════════════════════════════════════════════════

test "error round-trip" {
    const err = KbError.Pg;
    const s = errorString(err);
    try std.testing.expect(s.len > 0);
}

test "check KB_OK" {
    try check(c.KB_OK);
}

test "check KB_ERR_NOT_FOUND" {
    const result = check(c.KB_ERR_NOT_FOUND);
    try std.testing.expectError(error.NotFound, result);
}