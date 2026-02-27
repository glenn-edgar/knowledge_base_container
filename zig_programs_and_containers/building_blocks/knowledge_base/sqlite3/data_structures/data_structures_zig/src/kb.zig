//! kb.zig — Idiomatic Zig wrapper for the Knowledge Base C library.
//!
//! Provides RAII-style handles (init/deinit), Zig error unions,
//! slice helpers, and optional-pointer semantics over the C API.
//!
//! Usage:
//!   var ds = try kb.DataStructures.create("knowledge_base.db", "knowledge_base", "./ltree");
//!   defer ds.destroy();
//!
//!   var status = ds.status();
//!   try status.setData("some.path", "{\"val\":42}");

const std = @import("std");

// ═══════════════════════════════════════════════════════════════════
// Raw C bindings via @cImport
// ═══════════════════════════════════════════════════════════════════
pub const c = @cImport({
    @cInclude("kb_data_structures.h");
    @cInclude("kb_common.h");
    @cInclude("kb_query_support.h");
    @cInclude("kb_bit_structures.h");
    @cInclude("bit_mask_rt_operations.h");
    @cInclude("bit_s_expression.h");
    @cInclude("kb_status_table.h");
    @cInclude("kb_stream.h");
    @cInclude("kb_job_queue.h");
    @cInclude("kb_link_table.h");
    @cInclude("kb_link_mount_table.h");
    @cInclude("kb_rpc_server.h");
    @cInclude("kb_rpc_client.h");
    @cInclude("kb_uuid.h");
    @cInclude("kb_json.h");
});

// ═══════════════════════════════════════════════════════════════════
// Error mapping
// ═══════════════════════════════════════════════════════════════════

pub const KbError = error{
    NullArg,
    Sqlite,
    NotFound,
    Json,
    NoMem,
    Busy,
    Invalid,
    Overflow,
    State,
    Extension,
};

/// Convert a C kb_error_t into a Zig error union.
fn mapError(err: c.kb_error_t) KbError!void {
    return switch (err) {
        c.KB_OK => {},
        c.KB_ERR_NULL_ARG => error.NullArg,
        c.KB_ERR_SQLITE => error.Sqlite,
        c.KB_ERR_NOT_FOUND => error.NotFound,
        c.KB_ERR_JSON => error.Json,
        c.KB_ERR_NOMEM => error.NoMem,
        c.KB_ERR_BUSY => error.Busy,
        c.KB_ERR_INVALID => error.Invalid,
        c.KB_ERR_OVERFLOW => error.Overflow,
        c.KB_ERR_STATE => error.State,
        c.KB_ERR_EXTENSION => error.Extension,
        else => error.Invalid,
    };
}

/// Check a C error code; return void on KB_OK, Zig error otherwise.
pub fn check(err: c.kb_error_t) KbError!void {
    return mapError(err);
}

/// Get human-readable description of a KbError.
pub fn errorString(err: KbError) []const u8 {
    return switch (err) {
        error.NullArg => "null argument",
        error.Sqlite => "sqlite error",
        error.NotFound => "not found",
        error.Json => "JSON error",
        error.NoMem => "out of memory",
        error.Busy => "busy",
        error.Invalid => "invalid",
        error.Overflow => "overflow",
        error.State => "state error",
        error.Extension => "extension error",
    };
}

// ═══════════════════════════════════════════════════════════════════
// String helpers
// ═══════════════════════════════════════════════════════════════════

/// Convert a Zig slice to a C null-terminated string.
/// Returns null for null optional slices.
fn toCStr(s: ?[]const u8) ?[*:0]const u8 {
    if (s) |slice| {
        // Zig string literals and slices from @ptrCast are typically
        // already null-terminated when they come from string literals.
        // For runtime slices, caller must ensure null termination.
        return @ptrCast(slice.ptr);
    }
    return null;
}

/// Convert a C null-terminated string to a Zig slice.
/// Returns null for null pointers.
pub fn fromCStr(ptr: ?[*:0]const u8) ?[]const u8 {
    if (ptr) |p| {
        return std.mem.span(p);
    }
    return null;
}

/// Convert a C-allocated string to a Zig slice. The caller is
/// responsible for freeing via `std.c.free`.
fn ownedCStr(ptr: ?[*:0]u8) ?[]u8 {
    if (ptr) |p| {
        return std.mem.span(p);
    }
    return null;
}

/// Free a C-allocated string (from kb_strdup, kb_sprintf, etc.)
pub fn freeCStr(ptr: ?[*:0]u8) void {
    if (ptr) |p| {
        std.c.free(p);
    }
}

// ═══════════════════════════════════════════════════════════════════
// Result set wrapper
// ═══════════════════════════════════════════════════════════════════

pub const Result = struct {
    inner: c.kb_result_t,

    pub fn init() Result {
        var r: c.kb_result_t = undefined;
        c.kb_result_init(&r);
        return .{ .inner = r };
    }

    pub fn deinit(self: *Result) void {
        c.kb_result_free(&self.inner);
    }

    pub fn count(self: *const Result) usize {
        return @intCast(self.inner.count);
    }

    pub fn changes(self: *const Result) i32 {
        return self.inner.changes;
    }

    /// Get a column value as a string slice, or null if not found.
    pub fn get(self: *const Result, row_idx: usize, col_name: [:0]const u8) ?[]const u8 {
        const ptr = c.kb_row_get(&self.inner, @intCast(row_idx), col_name.ptr);
        return fromCStr(@ptrCast(ptr));
    }

    pub fn getInt(self: *const Result, row_idx: usize, col_name: [:0]const u8, default: i32) i32 {
        return c.kb_row_get_int(&self.inner, @intCast(row_idx), col_name.ptr, default);
    }

    pub fn getInt64(self: *const Result, row_idx: usize, col_name: [:0]const u8, default: i64) i64 {
        return c.kb_row_get_int64(&self.inner, @intCast(row_idx), col_name.ptr, default);
    }

    pub fn getDouble(self: *const Result, row_idx: usize, col_name: [:0]const u8, default: f64) f64 {
        return c.kb_row_get_double(&self.inner, @intCast(row_idx), col_name.ptr, default);
    }
};

// ═══════════════════════════════════════════════════════════════════
// Bind parameters
// ═══════════════════════════════════════════════════════════════════

pub const BindParam = union(enum) {
    null_val: void,
    text: [:0]const u8,
    int_val: i32,
    int64_val: i64,
    double_val: f64,

    fn toC(self: BindParam) c.kb_bind_param_t {
        return switch (self) {
            .null_val => .{ .type = c.KB_BIND_NULL, .val = undefined },
            .text => |s| .{ .type = c.KB_BIND_TEXT, .val = .{ .text = s.ptr } },
            .int_val => |v| .{ .type = c.KB_BIND_INT, .val = .{ .i = v } },
            .int64_val => |v| .{ .type = c.KB_BIND_INT64, .val = .{ .i64 = v } },
            .double_val => |v| .{ .type = c.KB_BIND_DOUBLE, .val = .{ .d = v } },
        };
    }
};

// ═══════════════════════════════════════════════════════════════════
// Low-level SQL helpers
// ═══════════════════════════════════════════════════════════════════

pub const Sql = struct {
    /// Execute a simple SQL statement (no results).
    pub fn exec(db: *c.sqlite3, sql: [:0]const u8) KbError!void {
        var err_msg: ?[*:0]u8 = null;
        try check(c.kb_sql_exec(db, sql.ptr, @ptrCast(&err_msg)));
        if (err_msg) |msg| freeCStr(msg);
    }

    /// Execute a parameterized query and return results.
    pub fn query(db: *c.sqlite3, sql: [:0]const u8, params: []const BindParam) KbError!Result {
        // Convert Zig params to C array on stack (max reasonable size).
        var c_params: [64]c.kb_bind_param_t = undefined;
        const n: usize = @min(params.len, 64);
        for (0..n) |i| {
            c_params[i] = params[i].toC();
        }
        var result = Result.init();
        try check(c.kb_query_exec(db, sql.ptr, &c_params, @intCast(n), &result.inner));
        return result;
    }

    pub fn beginImmediate(db: *c.sqlite3, max_retries: i32, retry_delay_ms: i32) KbError!void {
        try check(c.kb_begin_immediate(db, max_retries, retry_delay_ms));
    }

    pub fn commit(db: *c.sqlite3) KbError!void {
        try check(c.kb_commit(db));
    }

    pub fn rollback(db: *c.sqlite3) KbError!void {
        try check(c.kb_rollback(db));
    }
};

// ═══════════════════════════════════════════════════════════════════
// Database open/close helpers
// ═══════════════════════════════════════════════════════════════════

pub fn openDatabase(db_path: [:0]const u8, ltree_path: ?[:0]const u8) KbError!*c.sqlite3 {
    var db: ?*c.sqlite3 = null;
    const lp = if (ltree_path) |p| p.ptr else null;
    try check(c.kb_open_database(db_path.ptr, lp, &db));
    return db orelse return error.NullArg;
}

pub fn closeDatabase(db: *c.sqlite3) void {
    c.kb_close_database(db);
}

// ═══════════════════════════════════════════════════════════════════
// UUID
// ═══════════════════════════════════════════════════════════════════

pub const UUID_LEN = c.KB_UUID_LEN;

pub fn uuid4() [UUID_LEN - 1:0]u8 {
    var buf: [UUID_LEN]u8 = undefined;
    c.kb_uuid4(&buf, UUID_LEN);
    return buf[0 .. UUID_LEN - 1 :0].*;
}

pub fn uuidSeed() void {
    c.kb_uuid_seed();
}

// ═══════════════════════════════════════════════════════════════════
// Timestamp
// ═══════════════════════════════════════════════════════════════════

pub fn timestampNow() [63:0]u8 {
    var buf: [64]u8 = undefined;
    c.kb_timestamp_now(&buf, 64);
    return buf[0..63 :0].*;
}

// ═══════════════════════════════════════════════════════════════════
// S-Expression evaluator
// ═══════════════════════════════════════════════════════════════════

pub const BitData = struct {
    bit_mask: i64,
    change_mask: i64,
};

pub fn sexprEval(expr: [:0]const u8, bit_data: BitData) KbError!i32 {
    const cd = c.kb_bit_data_t{
        .bit_mask = bit_data.bit_mask,
        .change_mask = bit_data.change_mask,
    };
    var result: c_int = 0;
    try check(c.kb_sexpr_eval(expr.ptr, &cd, &result));
    return @intCast(result);
}



// ═══════════════════════════════════════════════════════════════════
// Search (kb_query_support)
// ═══════════════════════════════════════════════════════════════════

pub const Search = struct {
    handle: *c.kb_search_t,
    owned: bool,

    pub fn create(db_path: [:0]const u8, database: [:0]const u8, ltree_path: ?[:0]const u8) KbError!Search {
        const lp = if (ltree_path) |p| p.ptr else null;
        const h = c.kb_search_create(db_path.ptr, database.ptr, lp) orelse return error.NoMem;
        return .{ .handle = h, .owned = true };
    }

    pub fn createFromDb(db: *c.sqlite3, database: [:0]const u8) KbError!Search {
        const h = c.kb_search_create_from_db(db, database.ptr) orelse return error.NoMem;
        return .{ .handle = h, .owned = true };
    }

    /// Wrap an existing C handle (non-owning).
    pub fn wrap(ptr: *c.kb_search_t) Search {
        return .{ .handle = ptr, .owned = false };
    }

    pub fn destroy(self: *Search) void {
        if (self.owned) c.kb_search_destroy(self.handle);
        self.handle = undefined;
    }

    pub fn clearFilters(self: *Search) void {
        c.kb_search_clear_filters(self.handle);
    }

    pub fn kb(self: *Search, kb_name: [:0]const u8) KbError!void {
        try check(c.kb_search_kb(self.handle, kb_name.ptr));
    }

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

    pub fn hasLink(self: *Search) KbError!void {
        try check(c.kb_search_has_link(self.handle));
    }

    pub fn hasLinkMount(self: *Search) KbError!void {
        try check(c.kb_search_has_link_mount(self.handle));
    }

    pub fn path(self: *Search, path_expr: [:0]const u8) KbError!void {
        try check(c.kb_search_path(self.handle, path_expr.ptr));
    }

    pub fn startingPath(self: *Search, sp: [:0]const u8) KbError!void {
        try check(c.kb_search_starting_path(self.handle, sp.ptr));
    }

    pub fn execute(self: *Search) KbError!void {
        try check(c.kb_search_execute(self.handle));
    }

    /// Access the result set. Pointer is valid until next execute() or destroy().
    pub fn results(self: *const Search) *const c.kb_result_t {
        return c.kb_search_results(self.handle);
    }

    pub fn getDb(self: *const Search) KbError!*c.sqlite3 {
        return @constCast(c.kb_search_get_db(self.handle) orelse return error.NullArg);
    }

    pub fn getDatabase(self: *const Search) ?[]const u8 {
        return fromCStr(@ptrCast(c.kb_search_get_database(self.handle)));
    }
};

// ═══════════════════════════════════════════════════════════════════
// Bit Mask Operations
// ═══════════════════════════════════════════════════════════════════

pub const BitMaskOps = struct {
    handle: *c.kb_bit_mask_ops_t,
    owned: bool,

    pub fn create(db: *c.sqlite3, database: [:0]const u8) KbError!BitMaskOps {
        const h = c.kb_bit_mask_ops_create(db, database.ptr) orelse return error.NoMem;
        return .{ .handle = h, .owned = true };
    }

    pub fn wrap(ptr: *c.kb_bit_mask_ops_t) BitMaskOps {
        return .{ .handle = ptr, .owned = false };
    }

    pub fn destroy(self: *BitMaskOps) void {
        if (self.owned) c.kb_bit_mask_ops_destroy(self.handle);
    }

    pub fn getBit(self: *BitMaskOps, pth: [:0]const u8, bit_pos: i32) KbError!i32 {
        var val: c_int = 0;
        try check(c.kb_bit_get(self.handle, pth.ptr, bit_pos, &val));
        return @intCast(val);
    }

    pub fn setBit(self: *BitMaskOps, pth: [:0]const u8, bit_pos: i32, value: i32) KbError!void {
        try check(c.kb_bit_set(self.handle, pth.ptr, bit_pos, value));
    }

    pub fn getMask(self: *BitMaskOps, pth: [:0]const u8) KbError!i64 {
        var mask: i64 = 0;
        try check(c.kb_bit_get_mask(self.handle, pth.ptr, &mask));
        return mask;
    }

    pub fn setMask(self: *BitMaskOps, pth: [:0]const u8, mask: i64) KbError!void {
        try check(c.kb_bit_set_mask(self.handle, pth.ptr, mask));
    }

    pub fn getChangeMask(self: *BitMaskOps, pth: [:0]const u8) KbError!i64 {
        var mask: i64 = 0;
        try check(c.kb_bit_get_change_mask(self.handle, pth.ptr, &mask));
        return mask;
    }

    pub fn clearChangeMask(self: *BitMaskOps, pth: [:0]const u8) KbError!void {
        try check(c.kb_bit_clear_change_mask(self.handle, pth.ptr));
    }
};

// ═══════════════════════════════════════════════════════════════════
// Bit Structures (orchestrator)
// ═══════════════════════════════════════════════════════════════════

pub const BitStructures = struct {
    handle: *c.kb_bit_structures_t,
    owned: bool,

    pub fn wrap(ptr: *c.kb_bit_structures_t) BitStructures {
        return .{ .handle = ptr, .owned = false };
    }

    pub fn create(ks: *c.kb_search_t, database: [:0]const u8) KbError!BitStructures {
        const h = c.kb_bit_structures_create(ks, database.ptr) orelse return error.NoMem;
        return .{ .handle = h, .owned = true };
    }

    pub fn destroy(self: *BitStructures) void {
        if (self.owned) c.kb_bit_structures_destroy(self.handle);
    }

    pub fn findNodeId(self: *BitStructures, node_name: ?[:0]const u8, properties_json: ?[:0]const u8, node_path: ?[:0]const u8) KbError!i32 {
        var node_id: c_int = 0;
        const nn = if (node_name) |s| s.ptr else null;
        const pj = if (properties_json) |s| s.ptr else null;
        const np = if (node_path) |s| s.ptr else null;
        try check(c.kb_bit_find_node_id(self.handle, nn, pj, np, &node_id));
        return @intCast(node_id);
    }

    pub fn getBit(self: *BitStructures, pth: [:0]const u8, bit_pos: i32) KbError!i32 {
        var val: c_int = 0;
        try check(c.kb_bit_get_by_path(self.handle, pth.ptr, bit_pos, &val));
        return @intCast(val);
    }

    pub fn setBit(self: *BitStructures, pth: [:0]const u8, bit_pos: i32, value: i32) KbError!void {
        try check(c.kb_bit_set_by_path(self.handle, pth.ptr, bit_pos, value));
    }

    pub fn getMask(self: *BitStructures, pth: [:0]const u8) KbError!i64 {
        var mask: i64 = 0;
        try check(c.kb_bit_get_mask_by_path(self.handle, pth.ptr, &mask));
        return mask;
    }

    pub fn setMask(self: *BitStructures, pth: [:0]const u8, mask: i64) KbError!void {
        try check(c.kb_bit_set_mask_by_path(self.handle, pth.ptr, mask));
    }

    pub fn evalSexpr(self: *BitStructures, pth: [:0]const u8, expr: [:0]const u8) KbError!i32 {
        var result: c_int = 0;
        try check(c.kb_bit_eval_sexpr(self.handle, pth.ptr, expr.ptr, &result));
        return @intCast(result);
    }

    pub fn getOps(self: *BitStructures) KbError!BitMaskOps {
        return BitMaskOps.wrap(c.kb_bit_structures_get_ops(self.handle) orelse return error.NullArg);
    }
};

// ═══════════════════════════════════════════════════════════════════
// Status Table
// ═══════════════════════════════════════════════════════════════════

pub const StatusTable = struct {
    handle: *c.kb_status_table_t,
    owned: bool,

    pub fn wrap(ptr: *c.kb_status_table_t) StatusTable {
        return .{ .handle = ptr, .owned = false };
    }

    pub fn create(ks: *c.kb_search_t, database: [:0]const u8) KbError!StatusTable {
        const h = c.kb_status_table_create(ks, database.ptr) orelse return error.NoMem;
        return .{ .handle = h, .owned = true };
    }

    pub fn destroy(self: *StatusTable) void {
        if (self.owned) c.kb_status_table_destroy(self.handle);
    }

    pub fn findNodeId(self: *StatusTable, node_name: ?[:0]const u8, node_path: ?[:0]const u8) KbError!i32 {
        var nid: c_int = 0;
        const nn = if (node_name) |s| s.ptr else null;
        const np = if (node_path) |s| s.ptr else null;
        try check(c.kb_status_find_node_id(self.handle, nn, np, &nid));
        return @intCast(nid);
    }

    /// Get status data. Caller owns returned slice and must free it with `freeCStr`.
    pub fn getData(self: *StatusTable, pth: [:0]const u8) KbError!?[]u8 {
        var data: ?[*:0]u8 = null;
        try check(c.kb_status_get_data(self.handle, pth.ptr, @ptrCast(&data)));
        return ownedCStr(data);
    }

    pub fn setData(self: *StatusTable, pth: [:0]const u8, data_json: [:0]const u8) KbError!void {
        try check(c.kb_status_set_data(self.handle, pth.ptr, data_json.ptr));
    }
};

// ═══════════════════════════════════════════════════════════════════
// Stream
// ═══════════════════════════════════════════════════════════════════

pub const Stream = struct {
    handle: *c.kb_stream_t,
    owned: bool,

    pub fn wrap(ptr: *c.kb_stream_t) Stream {
        return .{ .handle = ptr, .owned = false };
    }

    pub fn create(ks: *c.kb_search_t, database: [:0]const u8) KbError!Stream {
        const h = c.kb_stream_create(ks, database.ptr) orelse return error.NoMem;
        return .{ .handle = h, .owned = true };
    }

    pub fn destroy(self: *Stream) void {
        if (self.owned) c.kb_stream_destroy(self.handle);
    }

    pub fn pushData(self: *Stream, pth: [:0]const u8, data_json: [:0]const u8) KbError!void {
        try check(c.kb_stream_push_data(self.handle, pth.ptr, data_json.ptr));
    }

    pub fn listData(self: *Stream, pth: [:0]const u8, recorded_after: ?[:0]const u8, recorded_before: ?[:0]const u8) KbError!Result {
        const ra = if (recorded_after) |s| s.ptr else null;
        const rb = if (recorded_before) |s| s.ptr else null;
        var result = Result.init();
        try check(c.kb_stream_list_data(self.handle, pth.ptr, ra, rb, &result.inner));
        return result;
    }

    pub fn clearData(self: *Stream, pth: [:0]const u8) KbError!void {
        try check(c.kb_stream_clear_data(self.handle, pth.ptr));
    }

    pub fn getWriteIndex(self: *Stream, pth: [:0]const u8) KbError!i32 {
        var idx: c_int = 0;
        try check(c.kb_stream_get_write_index(self.handle, pth.ptr, &idx));
        return @intCast(idx);
    }
};

// ═══════════════════════════════════════════════════════════════════
// Job Queue
// ═══════════════════════════════════════════════════════════════════

pub const JobQueue = struct {
    handle: *c.kb_job_queue_t,
    owned: bool,

    pub fn wrap(ptr: *c.kb_job_queue_t) JobQueue {
        return .{ .handle = ptr, .owned = false };
    }

    pub fn create(ks: *c.kb_search_t, database: [:0]const u8) KbError!JobQueue {
        const h = c.kb_job_queue_create(ks, database.ptr) orelse return error.NoMem;
        return .{ .handle = h, .owned = true };
    }

    pub fn destroy(self: *JobQueue) void {
        if (self.owned) c.kb_job_queue_destroy(self.handle);
    }

    pub fn findNodeId(self: *JobQueue, node_name: ?[:0]const u8, node_path: ?[:0]const u8) KbError!i32 {
        var nid: c_int = 0;
        const nn = if (node_name) |s| s.ptr else null;
        const np = if (node_path) |s| s.ptr else null;
        try check(c.kb_job_find_node_id(self.handle, nn, np, &nid));
        return @intCast(nid);
    }

    pub fn getQueuedNumber(self: *JobQueue, pth: [:0]const u8) KbError!i32 {
        var cnt: c_int = 0;
        try check(c.kb_job_get_queued_number(self.handle, pth.ptr, &cnt));
        return @intCast(cnt);
    }

    pub fn getFreeNumber(self: *JobQueue, pth: [:0]const u8) KbError!i32 {
        var cnt: c_int = 0;
        try check(c.kb_job_get_free_number(self.handle, pth.ptr, &cnt));
        return @intCast(cnt);
    }

    pub fn push(self: *JobQueue, pth: [:0]const u8, data_json: [:0]const u8, priority: i32) KbError!void {
        try check(c.kb_job_push(self.handle, pth.ptr, data_json.ptr, priority));
    }

    /// Peek at highest-priority job. Returns data (owned, free with freeCStr) and record_id.
    pub const PeekResult = struct {
        data: ?[]u8,
        record_id: i32,

        pub fn deinit(self: *PeekResult) void {
            if (self.data) |d| {
                freeCStr(@ptrCast(d.ptr));
            }
        }
    };

    pub fn peek(self: *JobQueue, pth: [:0]const u8) KbError!PeekResult {
        var data: ?[*:0]u8 = null;
        var rid: c_int = 0;
        try check(c.kb_job_peek(self.handle, pth.ptr, @ptrCast(&data), &rid));
        return .{
            .data = ownedCStr(data),
            .record_id = @intCast(rid),
        };
    }

    pub fn complete(self: *JobQueue, pth: [:0]const u8, record_id: i32) KbError!void {
        try check(c.kb_job_complete(self.handle, pth.ptr, record_id));
    }

    pub fn clear(self: *JobQueue, pth: [:0]const u8) KbError!void {
        try check(c.kb_job_clear(self.handle, pth.ptr));
    }
};

// ═══════════════════════════════════════════════════════════════════
// Link Table
// ═══════════════════════════════════════════════════════════════════

pub const LinkTable = struct {
    handle: *c.kb_link_table_t,
    owned: bool,

    pub fn wrap(ptr: *c.kb_link_table_t) LinkTable {
        return .{ .handle = ptr, .owned = false };
    }

    pub fn create(db: *c.sqlite3, database: [:0]const u8) KbError!LinkTable {
        const h = c.kb_link_table_create(db, database.ptr) orelse return error.NoMem;
        return .{ .handle = h, .owned = true };
    }

    pub fn destroy(self: *LinkTable) void {
        if (self.owned) c.kb_link_table_destroy(self.handle);
    }

    pub fn getByLinkName(self: *LinkTable, link_name: [:0]const u8) KbError!Result {
        var result = Result.init();
        try check(c.kb_link_get_by_link_name(self.handle, link_name.ptr, &result.inner));
        return result;
    }

    pub fn getByNodePath(self: *LinkTable, node_path: [:0]const u8) KbError!Result {
        var result = Result.init();
        try check(c.kb_link_get_by_node_path(self.handle, node_path.ptr, &result.inner));
        return result;
    }
};

// ═══════════════════════════════════════════════════════════════════
// Link Mount Table
// ═══════════════════════════════════════════════════════════════════

pub const LinkMountTable = struct {
    handle: *c.kb_link_mount_table_t,
    owned: bool,

    pub fn wrap(ptr: *c.kb_link_mount_table_t) LinkMountTable {
        return .{ .handle = ptr, .owned = false };
    }

    pub fn create(db: *c.sqlite3, database: [:0]const u8) KbError!LinkMountTable {
        const h = c.kb_link_mount_table_create(db, database.ptr) orelse return error.NoMem;
        return .{ .handle = h, .owned = true };
    }

    pub fn destroy(self: *LinkMountTable) void {
        if (self.owned) c.kb_link_mount_table_destroy(self.handle);
    }

    pub fn getByLinkName(self: *LinkMountTable, link_name: [:0]const u8) KbError!Result {
        var result = Result.init();
        try check(c.kb_link_mount_get_by_link_name(self.handle, link_name.ptr, &result.inner));
        return result;
    }

    pub fn getByMountPath(self: *LinkMountTable, mount_path: [:0]const u8) KbError!Result {
        var result = Result.init();
        try check(c.kb_link_mount_get_by_mount_path(self.handle, mount_path.ptr, &result.inner));
        return result;
    }
};

// ═══════════════════════════════════════════════════════════════════
// RPC Server
// ═══════════════════════════════════════════════════════════════════

pub const RpcServer = struct {
    handle: *c.kb_rpc_server_t,
    owned: bool,

    pub fn wrap(ptr: *c.kb_rpc_server_t) RpcServer {
        return .{ .handle = ptr, .owned = false };
    }

    pub fn create(ks: *c.kb_search_t, database: [:0]const u8) KbError!RpcServer {
        const h = c.kb_rpc_server_create(ks, database.ptr) orelse return error.NoMem;
        return .{ .handle = h, .owned = true };
    }

    pub fn destroy(self: *RpcServer) void {
        if (self.owned) c.kb_rpc_server_destroy(self.handle);
    }

    /// Push a new RPC job. Returns the assigned UUID.
    pub fn push(
        self: *RpcServer,
        pth: [:0]const u8,
        rpc_action: [:0]const u8,
        data_json: [:0]const u8,
        priority: i32,
        rpc_client_queue: [:0]const u8,
    ) KbError![UUID_LEN - 1:0]u8 {
        var uuid_buf: [UUID_LEN]u8 = undefined;
        try check(c.kb_rpc_server_push(
            self.handle,
            pth.ptr,
            rpc_action.ptr,
            data_json.ptr,
            priority,
            rpc_client_queue.ptr,
            &uuid_buf,
            UUID_LEN,
        ));
        return uuid_buf[0 .. UUID_LEN - 1 :0].*;
    }

    pub const PeekResult = struct {
        data: ?[]u8,
        uuid: ?[]u8,
        action: ?[]u8,
        record_id: i32,

        pub fn deinit(self: *PeekResult) void {
            if (self.data) |d| freeCStr(@ptrCast(d.ptr));
            if (self.uuid) |u| freeCStr(@ptrCast(u.ptr));
            if (self.action) |a| freeCStr(@ptrCast(a.ptr));
        }
    };

    pub fn peek(self: *RpcServer, pth: [:0]const u8) KbError!PeekResult {
        var data: ?[*:0]u8 = null;
        var uuid_ptr: ?[*:0]u8 = null;
        var action: ?[*:0]u8 = null;
        var rid: c_int = 0;
        try check(c.kb_rpc_server_peek(
            self.handle,
            pth.ptr,
            @ptrCast(&data),
            @ptrCast(&uuid_ptr),
            @ptrCast(&action),
            &rid,
        ));
        return .{
            .data = ownedCStr(data),
            .uuid = ownedCStr(uuid_ptr),
            .action = ownedCStr(action),
            .record_id = @intCast(rid),
        };
    }

    pub fn claim(self: *RpcServer, pth: [:0]const u8, record_id: i32) KbError!void {
        try check(c.kb_rpc_server_claim(self.handle, pth.ptr, record_id));
    }

    pub fn complete_job(self: *RpcServer, pth: [:0]const u8, record_id: i32) KbError!void {
        try check(c.kb_rpc_server_complete(self.handle, pth.ptr, record_id));
    }

    pub const StateCounts = struct {
        empty: i32,
        new_job: i32,
        processing: i32,
    };

    pub fn getStateCounts(self: *RpcServer, pth: [:0]const u8) KbError!StateCounts {
        var e: c_int = 0;
        var n: c_int = 0;
        var p: c_int = 0;
        try check(c.kb_rpc_server_get_state_counts(self.handle, pth.ptr, &e, &n, &p));
        return .{ .empty = @intCast(e), .new_job = @intCast(n), .processing = @intCast(p) };
    }
};

// ═══════════════════════════════════════════════════════════════════
// RPC Client
// ═══════════════════════════════════════════════════════════════════

pub const RpcClient = struct {
    handle: *c.kb_rpc_client_t,
    owned: bool,

    pub fn wrap(ptr: *c.kb_rpc_client_t) RpcClient {
        return .{ .handle = ptr, .owned = false };
    }

    pub fn create(ks: *c.kb_search_t, database: [:0]const u8) KbError!RpcClient {
        const h = c.kb_rpc_client_create(ks, database.ptr) orelse return error.NoMem;
        return .{ .handle = h, .owned = true };
    }

    pub fn destroy(self: *RpcClient) void {
        if (self.owned) c.kb_rpc_client_destroy(self.handle);
    }

    pub fn pushAndClaim(
        self: *RpcClient,
        client_path: [:0]const u8,
        request_uuid: [:0]const u8,
        server_path: [:0]const u8,
        rpc_action: [:0]const u8,
        transaction_tag: [:0]const u8,
        reply_data_json: [:0]const u8,
    ) KbError!void {
        try check(c.kb_rpc_client_push_and_claim(
            self.handle,
            client_path.ptr,
            request_uuid.ptr,
            server_path.ptr,
            rpc_action.ptr,
            transaction_tag.ptr,
            reply_data_json.ptr,
        ));
    }

    pub const PeekReplyResult = struct {
        reply_data: ?[]u8,
        uuid: ?[]u8,
        action: ?[]u8,
        record_id: i32,

        pub fn deinit(self: *PeekReplyResult) void {
            if (self.reply_data) |d| freeCStr(@ptrCast(d.ptr));
            if (self.uuid) |u| freeCStr(@ptrCast(u.ptr));
            if (self.action) |a| freeCStr(@ptrCast(a.ptr));
        }
    };

    pub fn peekReply(self: *RpcClient, client_path: [:0]const u8) KbError!PeekReplyResult {
        var data: ?[*:0]u8 = null;
        var uuid_ptr: ?[*:0]u8 = null;
        var action: ?[*:0]u8 = null;
        var rid: c_int = 0;
        try check(c.kb_rpc_client_peek_reply(
            self.handle,
            client_path.ptr,
            @ptrCast(&data),
            @ptrCast(&uuid_ptr),
            @ptrCast(&action),
            &rid,
        ));
        return .{
            .reply_data = ownedCStr(data),
            .uuid = ownedCStr(uuid_ptr),
            .action = ownedCStr(action),
            .record_id = @intCast(rid),
        };
    }

    pub fn clearReply(self: *RpcClient, client_path: [:0]const u8, record_id: i32) KbError!void {
        try check(c.kb_rpc_client_clear_reply(self.handle, client_path.ptr, record_id));
    }

    pub const StateCounts = struct {
        free: i32,
        queued: i32,
    };

    pub fn getStateCounts(self: *RpcClient, client_path: [:0]const u8) KbError!StateCounts {
        var f: c_int = 0;
        var q: c_int = 0;
        try check(c.kb_rpc_client_get_state_counts(self.handle, client_path.ptr, &f, &q));
        return .{ .free = @intCast(f), .queued = @intCast(q) };
    }
};

// ═══════════════════════════════════════════════════════════════════
// DataStructures — top-level aggregator facade
// ═══════════════════════════════════════════════════════════════════

pub const DataStructures = struct {
    handle: *c.kb_ds_t,

    /// Create the aggregator. Opens the database and creates all subsystems.
    pub fn create(db_path: [:0]const u8, database: [:0]const u8, ltree_path: ?[:0]const u8) KbError!DataStructures {
        const lp = if (ltree_path) |p| p.ptr else null;
        const h = c.kb_ds_create(db_path.ptr, database.ptr, lp) orelse return error.NoMem;
        return .{ .handle = h };
    }

    /// Create from an existing open sqlite3 handle (non-owning).
    pub fn createFromDb(db: *c.sqlite3, database: [:0]const u8) KbError!DataStructures {
        const h = c.kb_ds_create_from_db(db, database.ptr) orelse return error.NoMem;
        return .{ .handle = h };
    }

    /// Destroy and free all resources.
    pub fn destroy(self: *DataStructures) void {
        c.kb_ds_destroy(self.handle);
    }

    // ── Subsystem accessors (non-owning wrappers) ──────────────────

    pub fn search(self: *DataStructures) KbError!Search {
        return Search.wrap(c.kb_ds_search(self.handle) orelse return error.NullArg);
    }

    pub fn bitStructures(self: *DataStructures) KbError!BitStructures {
        return BitStructures.wrap(c.kb_ds_bit_structures(self.handle) orelse return error.NullArg);
    }

    pub fn status(self: *DataStructures) KbError!StatusTable {
        return StatusTable.wrap(c.kb_ds_status(self.handle) orelse return error.NullArg);
    }

    pub fn stream(self: *DataStructures) KbError!Stream {
        return Stream.wrap(c.kb_ds_stream(self.handle) orelse return error.NullArg);
    }

    pub fn jobQueue(self: *DataStructures) KbError!JobQueue {
        return JobQueue.wrap(c.kb_ds_job_queue(self.handle) orelse return error.NullArg);
    }

    pub fn linkTable(self: *DataStructures) KbError!LinkTable {
        return LinkTable.wrap(c.kb_ds_link_table(self.handle) orelse return error.NullArg);
    }

    pub fn linkMountTable(self: *DataStructures) KbError!LinkMountTable {
        return LinkMountTable.wrap(c.kb_ds_link_mount_table(self.handle) orelse return error.NullArg);
    }

    pub fn rpcServer(self: *DataStructures) KbError!RpcServer {
        return RpcServer.wrap(c.kb_ds_rpc_server(self.handle) orelse return error.NullArg);
    }

    pub fn rpcClient(self: *DataStructures) KbError!RpcClient {
        return RpcClient.wrap(c.kb_ds_rpc_client(self.handle) orelse return error.NullArg);
    }

    pub fn getDb(self: *DataStructures) KbError!*c.sqlite3 {
        return c.kb_ds_get_db(self.handle) orelse return error.NullArg;
    }
};

// ═══════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════

test "error mapping round-trip" {
    const testing = std.testing;
    try testing.expectEqual(@as(KbError!void, {}), mapError(c.KB_OK));
    try testing.expectError(error.NotFound, mapError(c.KB_ERR_NOT_FOUND));
    try testing.expectError(error.Sqlite, mapError(c.KB_ERR_SQLITE));
}

test "uuid generation" {
    uuidSeed();
    const uuid_a = uuid4();
    const uuid_b = uuid4();
    // Two UUIDs should differ
    const s1: []const u8 = &uuid_a;
    const s2: []const u8 = &uuid_b;
    try std.testing.expect(!std.mem.eql(u8, s1, s2));
}