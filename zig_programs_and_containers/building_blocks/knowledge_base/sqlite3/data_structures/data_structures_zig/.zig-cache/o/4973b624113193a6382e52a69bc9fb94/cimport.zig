pub const __builtin_bswap16 = @import("std").zig.c_builtins.__builtin_bswap16;
pub const __builtin_bswap32 = @import("std").zig.c_builtins.__builtin_bswap32;
pub const __builtin_bswap64 = @import("std").zig.c_builtins.__builtin_bswap64;
pub const __builtin_signbit = @import("std").zig.c_builtins.__builtin_signbit;
pub const __builtin_signbitf = @import("std").zig.c_builtins.__builtin_signbitf;
pub const __builtin_popcount = @import("std").zig.c_builtins.__builtin_popcount;
pub const __builtin_ctz = @import("std").zig.c_builtins.__builtin_ctz;
pub const __builtin_clz = @import("std").zig.c_builtins.__builtin_clz;
pub const __builtin_sqrt = @import("std").zig.c_builtins.__builtin_sqrt;
pub const __builtin_sqrtf = @import("std").zig.c_builtins.__builtin_sqrtf;
pub const __builtin_sin = @import("std").zig.c_builtins.__builtin_sin;
pub const __builtin_sinf = @import("std").zig.c_builtins.__builtin_sinf;
pub const __builtin_cos = @import("std").zig.c_builtins.__builtin_cos;
pub const __builtin_cosf = @import("std").zig.c_builtins.__builtin_cosf;
pub const __builtin_exp = @import("std").zig.c_builtins.__builtin_exp;
pub const __builtin_expf = @import("std").zig.c_builtins.__builtin_expf;
pub const __builtin_exp2 = @import("std").zig.c_builtins.__builtin_exp2;
pub const __builtin_exp2f = @import("std").zig.c_builtins.__builtin_exp2f;
pub const __builtin_log = @import("std").zig.c_builtins.__builtin_log;
pub const __builtin_logf = @import("std").zig.c_builtins.__builtin_logf;
pub const __builtin_log2 = @import("std").zig.c_builtins.__builtin_log2;
pub const __builtin_log2f = @import("std").zig.c_builtins.__builtin_log2f;
pub const __builtin_log10 = @import("std").zig.c_builtins.__builtin_log10;
pub const __builtin_log10f = @import("std").zig.c_builtins.__builtin_log10f;
pub const __builtin_abs = @import("std").zig.c_builtins.__builtin_abs;
pub const __builtin_labs = @import("std").zig.c_builtins.__builtin_labs;
pub const __builtin_llabs = @import("std").zig.c_builtins.__builtin_llabs;
pub const __builtin_fabs = @import("std").zig.c_builtins.__builtin_fabs;
pub const __builtin_fabsf = @import("std").zig.c_builtins.__builtin_fabsf;
pub const __builtin_floor = @import("std").zig.c_builtins.__builtin_floor;
pub const __builtin_floorf = @import("std").zig.c_builtins.__builtin_floorf;
pub const __builtin_ceil = @import("std").zig.c_builtins.__builtin_ceil;
pub const __builtin_ceilf = @import("std").zig.c_builtins.__builtin_ceilf;
pub const __builtin_trunc = @import("std").zig.c_builtins.__builtin_trunc;
pub const __builtin_truncf = @import("std").zig.c_builtins.__builtin_truncf;
pub const __builtin_round = @import("std").zig.c_builtins.__builtin_round;
pub const __builtin_roundf = @import("std").zig.c_builtins.__builtin_roundf;
pub const __builtin_strlen = @import("std").zig.c_builtins.__builtin_strlen;
pub const __builtin_strcmp = @import("std").zig.c_builtins.__builtin_strcmp;
pub const __builtin_object_size = @import("std").zig.c_builtins.__builtin_object_size;
pub const __builtin___memset_chk = @import("std").zig.c_builtins.__builtin___memset_chk;
pub const __builtin_memset = @import("std").zig.c_builtins.__builtin_memset;
pub const __builtin___memcpy_chk = @import("std").zig.c_builtins.__builtin___memcpy_chk;
pub const __builtin_memcpy = @import("std").zig.c_builtins.__builtin_memcpy;
pub const __builtin_expect = @import("std").zig.c_builtins.__builtin_expect;
pub const __builtin_nanf = @import("std").zig.c_builtins.__builtin_nanf;
pub const __builtin_huge_valf = @import("std").zig.c_builtins.__builtin_huge_valf;
pub const __builtin_inff = @import("std").zig.c_builtins.__builtin_inff;
pub const __builtin_isnan = @import("std").zig.c_builtins.__builtin_isnan;
pub const __builtin_isinf = @import("std").zig.c_builtins.__builtin_isinf;
pub const __builtin_isinf_sign = @import("std").zig.c_builtins.__builtin_isinf_sign;
pub const __has_builtin = @import("std").zig.c_builtins.__has_builtin;
pub const __builtin_assume = @import("std").zig.c_builtins.__builtin_assume;
pub const __builtin_unreachable = @import("std").zig.c_builtins.__builtin_unreachable;
pub const __builtin_constant_p = @import("std").zig.c_builtins.__builtin_constant_p;
pub const __builtin_mul_overflow = @import("std").zig.c_builtins.__builtin_mul_overflow;
pub const struct___va_list_1 = extern struct {
    __stack: ?*anyopaque = @import("std").mem.zeroes(?*anyopaque),
    __gr_top: ?*anyopaque = @import("std").mem.zeroes(?*anyopaque),
    __vr_top: ?*anyopaque = @import("std").mem.zeroes(?*anyopaque),
    __gr_offs: c_int = @import("std").mem.zeroes(c_int),
    __vr_offs: c_int = @import("std").mem.zeroes(c_int),
};
pub const __builtin_va_list = struct___va_list_1;
pub const __gnuc_va_list = __builtin_va_list;
pub const va_list = __builtin_va_list;
pub const sqlite3_version: [*c]const u8 = @extern([*c]const u8, .{
    .name = "sqlite3_version",
});
pub extern fn sqlite3_libversion() [*c]const u8;
pub extern fn sqlite3_sourceid() [*c]const u8;
pub extern fn sqlite3_libversion_number() c_int;
pub extern fn sqlite3_compileoption_used(zOptName: [*c]const u8) c_int;
pub extern fn sqlite3_compileoption_get(N: c_int) [*c]const u8;
pub extern fn sqlite3_threadsafe() c_int;
pub const struct_sqlite3 = opaque {};
pub const sqlite3 = struct_sqlite3;
pub const sqlite_int64 = c_longlong;
pub const sqlite_uint64 = c_ulonglong;
pub const sqlite3_int64 = sqlite_int64;
pub const sqlite3_uint64 = sqlite_uint64;
pub extern fn sqlite3_close(?*sqlite3) c_int;
pub extern fn sqlite3_close_v2(?*sqlite3) c_int;
pub const sqlite3_callback = ?*const fn (?*anyopaque, c_int, [*c][*c]u8, [*c][*c]u8) callconv(.C) c_int;
pub extern fn sqlite3_exec(?*sqlite3, sql: [*c]const u8, callback: ?*const fn (?*anyopaque, c_int, [*c][*c]u8, [*c][*c]u8) callconv(.C) c_int, ?*anyopaque, errmsg: [*c][*c]u8) c_int;
pub const sqlite3_file = struct_sqlite3_file;
pub const struct_sqlite3_io_methods = extern struct {
    iVersion: c_int = @import("std").mem.zeroes(c_int),
    xClose: ?*const fn ([*c]sqlite3_file) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn ([*c]sqlite3_file) callconv(.C) c_int),
    xRead: ?*const fn ([*c]sqlite3_file, ?*anyopaque, c_int, sqlite3_int64) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn ([*c]sqlite3_file, ?*anyopaque, c_int, sqlite3_int64) callconv(.C) c_int),
    xWrite: ?*const fn ([*c]sqlite3_file, ?*const anyopaque, c_int, sqlite3_int64) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn ([*c]sqlite3_file, ?*const anyopaque, c_int, sqlite3_int64) callconv(.C) c_int),
    xTruncate: ?*const fn ([*c]sqlite3_file, sqlite3_int64) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn ([*c]sqlite3_file, sqlite3_int64) callconv(.C) c_int),
    xSync: ?*const fn ([*c]sqlite3_file, c_int) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn ([*c]sqlite3_file, c_int) callconv(.C) c_int),
    xFileSize: ?*const fn ([*c]sqlite3_file, [*c]sqlite3_int64) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn ([*c]sqlite3_file, [*c]sqlite3_int64) callconv(.C) c_int),
    xLock: ?*const fn ([*c]sqlite3_file, c_int) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn ([*c]sqlite3_file, c_int) callconv(.C) c_int),
    xUnlock: ?*const fn ([*c]sqlite3_file, c_int) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn ([*c]sqlite3_file, c_int) callconv(.C) c_int),
    xCheckReservedLock: ?*const fn ([*c]sqlite3_file, [*c]c_int) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn ([*c]sqlite3_file, [*c]c_int) callconv(.C) c_int),
    xFileControl: ?*const fn ([*c]sqlite3_file, c_int, ?*anyopaque) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn ([*c]sqlite3_file, c_int, ?*anyopaque) callconv(.C) c_int),
    xSectorSize: ?*const fn ([*c]sqlite3_file) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn ([*c]sqlite3_file) callconv(.C) c_int),
    xDeviceCharacteristics: ?*const fn ([*c]sqlite3_file) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn ([*c]sqlite3_file) callconv(.C) c_int),
    xShmMap: ?*const fn ([*c]sqlite3_file, c_int, c_int, c_int, [*c]?*volatile anyopaque) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn ([*c]sqlite3_file, c_int, c_int, c_int, [*c]?*volatile anyopaque) callconv(.C) c_int),
    xShmLock: ?*const fn ([*c]sqlite3_file, c_int, c_int, c_int) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn ([*c]sqlite3_file, c_int, c_int, c_int) callconv(.C) c_int),
    xShmBarrier: ?*const fn ([*c]sqlite3_file) callconv(.C) void = @import("std").mem.zeroes(?*const fn ([*c]sqlite3_file) callconv(.C) void),
    xShmUnmap: ?*const fn ([*c]sqlite3_file, c_int) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn ([*c]sqlite3_file, c_int) callconv(.C) c_int),
    xFetch: ?*const fn ([*c]sqlite3_file, sqlite3_int64, c_int, [*c]?*anyopaque) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn ([*c]sqlite3_file, sqlite3_int64, c_int, [*c]?*anyopaque) callconv(.C) c_int),
    xUnfetch: ?*const fn ([*c]sqlite3_file, sqlite3_int64, ?*anyopaque) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn ([*c]sqlite3_file, sqlite3_int64, ?*anyopaque) callconv(.C) c_int),
};
pub const struct_sqlite3_file = extern struct {
    pMethods: [*c]const struct_sqlite3_io_methods = @import("std").mem.zeroes([*c]const struct_sqlite3_io_methods),
};
pub const sqlite3_io_methods = struct_sqlite3_io_methods;
pub const struct_sqlite3_mutex = opaque {};
pub const sqlite3_mutex = struct_sqlite3_mutex;
pub const struct_sqlite3_api_routines = opaque {};
pub const sqlite3_api_routines = struct_sqlite3_api_routines;
pub const sqlite3_filename = [*c]const u8;
pub const sqlite3_vfs = struct_sqlite3_vfs;
pub const sqlite3_syscall_ptr = ?*const fn () callconv(.C) void;
pub const struct_sqlite3_vfs = extern struct {
    iVersion: c_int = @import("std").mem.zeroes(c_int),
    szOsFile: c_int = @import("std").mem.zeroes(c_int),
    mxPathname: c_int = @import("std").mem.zeroes(c_int),
    pNext: [*c]sqlite3_vfs = @import("std").mem.zeroes([*c]sqlite3_vfs),
    zName: [*c]const u8 = @import("std").mem.zeroes([*c]const u8),
    pAppData: ?*anyopaque = @import("std").mem.zeroes(?*anyopaque),
    xOpen: ?*const fn ([*c]sqlite3_vfs, sqlite3_filename, [*c]sqlite3_file, c_int, [*c]c_int) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn ([*c]sqlite3_vfs, sqlite3_filename, [*c]sqlite3_file, c_int, [*c]c_int) callconv(.C) c_int),
    xDelete: ?*const fn ([*c]sqlite3_vfs, [*c]const u8, c_int) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn ([*c]sqlite3_vfs, [*c]const u8, c_int) callconv(.C) c_int),
    xAccess: ?*const fn ([*c]sqlite3_vfs, [*c]const u8, c_int, [*c]c_int) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn ([*c]sqlite3_vfs, [*c]const u8, c_int, [*c]c_int) callconv(.C) c_int),
    xFullPathname: ?*const fn ([*c]sqlite3_vfs, [*c]const u8, c_int, [*c]u8) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn ([*c]sqlite3_vfs, [*c]const u8, c_int, [*c]u8) callconv(.C) c_int),
    xDlOpen: ?*const fn ([*c]sqlite3_vfs, [*c]const u8) callconv(.C) ?*anyopaque = @import("std").mem.zeroes(?*const fn ([*c]sqlite3_vfs, [*c]const u8) callconv(.C) ?*anyopaque),
    xDlError: ?*const fn ([*c]sqlite3_vfs, c_int, [*c]u8) callconv(.C) void = @import("std").mem.zeroes(?*const fn ([*c]sqlite3_vfs, c_int, [*c]u8) callconv(.C) void),
    xDlSym: ?*const fn ([*c]sqlite3_vfs, ?*anyopaque, [*c]const u8) callconv(.C) ?*const fn () callconv(.C) void = @import("std").mem.zeroes(?*const fn ([*c]sqlite3_vfs, ?*anyopaque, [*c]const u8) callconv(.C) ?*const fn () callconv(.C) void),
    xDlClose: ?*const fn ([*c]sqlite3_vfs, ?*anyopaque) callconv(.C) void = @import("std").mem.zeroes(?*const fn ([*c]sqlite3_vfs, ?*anyopaque) callconv(.C) void),
    xRandomness: ?*const fn ([*c]sqlite3_vfs, c_int, [*c]u8) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn ([*c]sqlite3_vfs, c_int, [*c]u8) callconv(.C) c_int),
    xSleep: ?*const fn ([*c]sqlite3_vfs, c_int) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn ([*c]sqlite3_vfs, c_int) callconv(.C) c_int),
    xCurrentTime: ?*const fn ([*c]sqlite3_vfs, [*c]f64) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn ([*c]sqlite3_vfs, [*c]f64) callconv(.C) c_int),
    xGetLastError: ?*const fn ([*c]sqlite3_vfs, c_int, [*c]u8) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn ([*c]sqlite3_vfs, c_int, [*c]u8) callconv(.C) c_int),
    xCurrentTimeInt64: ?*const fn ([*c]sqlite3_vfs, [*c]sqlite3_int64) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn ([*c]sqlite3_vfs, [*c]sqlite3_int64) callconv(.C) c_int),
    xSetSystemCall: ?*const fn ([*c]sqlite3_vfs, [*c]const u8, sqlite3_syscall_ptr) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn ([*c]sqlite3_vfs, [*c]const u8, sqlite3_syscall_ptr) callconv(.C) c_int),
    xGetSystemCall: ?*const fn ([*c]sqlite3_vfs, [*c]const u8) callconv(.C) sqlite3_syscall_ptr = @import("std").mem.zeroes(?*const fn ([*c]sqlite3_vfs, [*c]const u8) callconv(.C) sqlite3_syscall_ptr),
    xNextSystemCall: ?*const fn ([*c]sqlite3_vfs, [*c]const u8) callconv(.C) [*c]const u8 = @import("std").mem.zeroes(?*const fn ([*c]sqlite3_vfs, [*c]const u8) callconv(.C) [*c]const u8),
};
pub extern fn sqlite3_initialize() c_int;
pub extern fn sqlite3_shutdown() c_int;
pub extern fn sqlite3_os_init() c_int;
pub extern fn sqlite3_os_end() c_int;
pub extern fn sqlite3_config(c_int, ...) c_int;
pub extern fn sqlite3_db_config(?*sqlite3, op: c_int, ...) c_int;
pub const struct_sqlite3_mem_methods = extern struct {
    xMalloc: ?*const fn (c_int) callconv(.C) ?*anyopaque = @import("std").mem.zeroes(?*const fn (c_int) callconv(.C) ?*anyopaque),
    xFree: ?*const fn (?*anyopaque) callconv(.C) void = @import("std").mem.zeroes(?*const fn (?*anyopaque) callconv(.C) void),
    xRealloc: ?*const fn (?*anyopaque, c_int) callconv(.C) ?*anyopaque = @import("std").mem.zeroes(?*const fn (?*anyopaque, c_int) callconv(.C) ?*anyopaque),
    xSize: ?*const fn (?*anyopaque) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn (?*anyopaque) callconv(.C) c_int),
    xRoundup: ?*const fn (c_int) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn (c_int) callconv(.C) c_int),
    xInit: ?*const fn (?*anyopaque) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn (?*anyopaque) callconv(.C) c_int),
    xShutdown: ?*const fn (?*anyopaque) callconv(.C) void = @import("std").mem.zeroes(?*const fn (?*anyopaque) callconv(.C) void),
    pAppData: ?*anyopaque = @import("std").mem.zeroes(?*anyopaque),
};
pub const sqlite3_mem_methods = struct_sqlite3_mem_methods;
pub extern fn sqlite3_extended_result_codes(?*sqlite3, onoff: c_int) c_int;
pub extern fn sqlite3_last_insert_rowid(?*sqlite3) sqlite3_int64;
pub extern fn sqlite3_set_last_insert_rowid(?*sqlite3, sqlite3_int64) void;
pub extern fn sqlite3_changes(?*sqlite3) c_int;
pub extern fn sqlite3_changes64(?*sqlite3) sqlite3_int64;
pub extern fn sqlite3_total_changes(?*sqlite3) c_int;
pub extern fn sqlite3_total_changes64(?*sqlite3) sqlite3_int64;
pub extern fn sqlite3_interrupt(?*sqlite3) void;
pub extern fn sqlite3_is_interrupted(?*sqlite3) c_int;
pub extern fn sqlite3_complete(sql: [*c]const u8) c_int;
pub extern fn sqlite3_complete16(sql: ?*const anyopaque) c_int;
pub extern fn sqlite3_busy_handler(?*sqlite3, ?*const fn (?*anyopaque, c_int) callconv(.C) c_int, ?*anyopaque) c_int;
pub extern fn sqlite3_busy_timeout(?*sqlite3, ms: c_int) c_int;
pub extern fn sqlite3_get_table(db: ?*sqlite3, zSql: [*c]const u8, pazResult: [*c][*c][*c]u8, pnRow: [*c]c_int, pnColumn: [*c]c_int, pzErrmsg: [*c][*c]u8) c_int;
pub extern fn sqlite3_free_table(result: [*c][*c]u8) void;
pub extern fn sqlite3_mprintf([*c]const u8, ...) [*c]u8;
pub extern fn sqlite3_vmprintf([*c]const u8, va_list) [*c]u8;
pub extern fn sqlite3_snprintf(c_int, [*c]u8, [*c]const u8, ...) [*c]u8;
pub extern fn sqlite3_vsnprintf(c_int, [*c]u8, [*c]const u8, va_list) [*c]u8;
pub extern fn sqlite3_malloc(c_int) ?*anyopaque;
pub extern fn sqlite3_malloc64(sqlite3_uint64) ?*anyopaque;
pub extern fn sqlite3_realloc(?*anyopaque, c_int) ?*anyopaque;
pub extern fn sqlite3_realloc64(?*anyopaque, sqlite3_uint64) ?*anyopaque;
pub extern fn sqlite3_free(?*anyopaque) void;
pub extern fn sqlite3_msize(?*anyopaque) sqlite3_uint64;
pub extern fn sqlite3_memory_used() sqlite3_int64;
pub extern fn sqlite3_memory_highwater(resetFlag: c_int) sqlite3_int64;
pub extern fn sqlite3_randomness(N: c_int, P: ?*anyopaque) void;
pub extern fn sqlite3_set_authorizer(?*sqlite3, xAuth: ?*const fn (?*anyopaque, c_int, [*c]const u8, [*c]const u8, [*c]const u8, [*c]const u8) callconv(.C) c_int, pUserData: ?*anyopaque) c_int;
pub extern fn sqlite3_trace(?*sqlite3, xTrace: ?*const fn (?*anyopaque, [*c]const u8) callconv(.C) void, ?*anyopaque) ?*anyopaque;
pub extern fn sqlite3_profile(?*sqlite3, xProfile: ?*const fn (?*anyopaque, [*c]const u8, sqlite3_uint64) callconv(.C) void, ?*anyopaque) ?*anyopaque;
pub extern fn sqlite3_trace_v2(?*sqlite3, uMask: c_uint, xCallback: ?*const fn (c_uint, ?*anyopaque, ?*anyopaque, ?*anyopaque) callconv(.C) c_int, pCtx: ?*anyopaque) c_int;
pub extern fn sqlite3_progress_handler(?*sqlite3, c_int, ?*const fn (?*anyopaque) callconv(.C) c_int, ?*anyopaque) void;
pub extern fn sqlite3_open(filename: [*c]const u8, ppDb: [*c]?*sqlite3) c_int;
pub extern fn sqlite3_open16(filename: ?*const anyopaque, ppDb: [*c]?*sqlite3) c_int;
pub extern fn sqlite3_open_v2(filename: [*c]const u8, ppDb: [*c]?*sqlite3, flags: c_int, zVfs: [*c]const u8) c_int;
pub extern fn sqlite3_uri_parameter(z: sqlite3_filename, zParam: [*c]const u8) [*c]const u8;
pub extern fn sqlite3_uri_boolean(z: sqlite3_filename, zParam: [*c]const u8, bDefault: c_int) c_int;
pub extern fn sqlite3_uri_int64(sqlite3_filename, [*c]const u8, sqlite3_int64) sqlite3_int64;
pub extern fn sqlite3_uri_key(z: sqlite3_filename, N: c_int) [*c]const u8;
pub extern fn sqlite3_filename_database(sqlite3_filename) [*c]const u8;
pub extern fn sqlite3_filename_journal(sqlite3_filename) [*c]const u8;
pub extern fn sqlite3_filename_wal(sqlite3_filename) [*c]const u8;
pub extern fn sqlite3_database_file_object([*c]const u8) [*c]sqlite3_file;
pub extern fn sqlite3_create_filename(zDatabase: [*c]const u8, zJournal: [*c]const u8, zWal: [*c]const u8, nParam: c_int, azParam: [*c][*c]const u8) sqlite3_filename;
pub extern fn sqlite3_free_filename(sqlite3_filename) void;
pub extern fn sqlite3_errcode(db: ?*sqlite3) c_int;
pub extern fn sqlite3_extended_errcode(db: ?*sqlite3) c_int;
pub extern fn sqlite3_errmsg(?*sqlite3) [*c]const u8;
pub extern fn sqlite3_errmsg16(?*sqlite3) ?*const anyopaque;
pub extern fn sqlite3_errstr(c_int) [*c]const u8;
pub extern fn sqlite3_error_offset(db: ?*sqlite3) c_int;
pub const struct_sqlite3_stmt = opaque {};
pub const sqlite3_stmt = struct_sqlite3_stmt;
pub extern fn sqlite3_limit(?*sqlite3, id: c_int, newVal: c_int) c_int;
pub extern fn sqlite3_prepare(db: ?*sqlite3, zSql: [*c]const u8, nByte: c_int, ppStmt: [*c]?*sqlite3_stmt, pzTail: [*c][*c]const u8) c_int;
pub extern fn sqlite3_prepare_v2(db: ?*sqlite3, zSql: [*c]const u8, nByte: c_int, ppStmt: [*c]?*sqlite3_stmt, pzTail: [*c][*c]const u8) c_int;
pub extern fn sqlite3_prepare_v3(db: ?*sqlite3, zSql: [*c]const u8, nByte: c_int, prepFlags: c_uint, ppStmt: [*c]?*sqlite3_stmt, pzTail: [*c][*c]const u8) c_int;
pub extern fn sqlite3_prepare16(db: ?*sqlite3, zSql: ?*const anyopaque, nByte: c_int, ppStmt: [*c]?*sqlite3_stmt, pzTail: [*c]?*const anyopaque) c_int;
pub extern fn sqlite3_prepare16_v2(db: ?*sqlite3, zSql: ?*const anyopaque, nByte: c_int, ppStmt: [*c]?*sqlite3_stmt, pzTail: [*c]?*const anyopaque) c_int;
pub extern fn sqlite3_prepare16_v3(db: ?*sqlite3, zSql: ?*const anyopaque, nByte: c_int, prepFlags: c_uint, ppStmt: [*c]?*sqlite3_stmt, pzTail: [*c]?*const anyopaque) c_int;
pub extern fn sqlite3_sql(pStmt: ?*sqlite3_stmt) [*c]const u8;
pub extern fn sqlite3_expanded_sql(pStmt: ?*sqlite3_stmt) [*c]u8;
pub extern fn sqlite3_stmt_readonly(pStmt: ?*sqlite3_stmt) c_int;
pub extern fn sqlite3_stmt_isexplain(pStmt: ?*sqlite3_stmt) c_int;
pub extern fn sqlite3_stmt_explain(pStmt: ?*sqlite3_stmt, eMode: c_int) c_int;
pub extern fn sqlite3_stmt_busy(?*sqlite3_stmt) c_int;
pub const struct_sqlite3_value = opaque {};
pub const sqlite3_value = struct_sqlite3_value;
pub const struct_sqlite3_context = opaque {};
pub const sqlite3_context = struct_sqlite3_context;
pub extern fn sqlite3_bind_blob(?*sqlite3_stmt, c_int, ?*const anyopaque, n: c_int, ?*const fn (?*anyopaque) callconv(.C) void) c_int;
pub extern fn sqlite3_bind_blob64(?*sqlite3_stmt, c_int, ?*const anyopaque, sqlite3_uint64, ?*const fn (?*anyopaque) callconv(.C) void) c_int;
pub extern fn sqlite3_bind_double(?*sqlite3_stmt, c_int, f64) c_int;
pub extern fn sqlite3_bind_int(?*sqlite3_stmt, c_int, c_int) c_int;
pub extern fn sqlite3_bind_int64(?*sqlite3_stmt, c_int, sqlite3_int64) c_int;
pub extern fn sqlite3_bind_null(?*sqlite3_stmt, c_int) c_int;
pub extern fn sqlite3_bind_text(?*sqlite3_stmt, c_int, [*c]const u8, c_int, ?*const fn (?*anyopaque) callconv(.C) void) c_int;
pub extern fn sqlite3_bind_text16(?*sqlite3_stmt, c_int, ?*const anyopaque, c_int, ?*const fn (?*anyopaque) callconv(.C) void) c_int;
pub extern fn sqlite3_bind_text64(?*sqlite3_stmt, c_int, [*c]const u8, sqlite3_uint64, ?*const fn (?*anyopaque) callconv(.C) void, encoding: u8) c_int;
pub extern fn sqlite3_bind_value(?*sqlite3_stmt, c_int, ?*const sqlite3_value) c_int;
pub extern fn sqlite3_bind_pointer(?*sqlite3_stmt, c_int, ?*anyopaque, [*c]const u8, ?*const fn (?*anyopaque) callconv(.C) void) c_int;
pub extern fn sqlite3_bind_zeroblob(?*sqlite3_stmt, c_int, n: c_int) c_int;
pub extern fn sqlite3_bind_zeroblob64(?*sqlite3_stmt, c_int, sqlite3_uint64) c_int;
pub extern fn sqlite3_bind_parameter_count(?*sqlite3_stmt) c_int;
pub extern fn sqlite3_bind_parameter_name(?*sqlite3_stmt, c_int) [*c]const u8;
pub extern fn sqlite3_bind_parameter_index(?*sqlite3_stmt, zName: [*c]const u8) c_int;
pub extern fn sqlite3_clear_bindings(?*sqlite3_stmt) c_int;
pub extern fn sqlite3_column_count(pStmt: ?*sqlite3_stmt) c_int;
pub extern fn sqlite3_column_name(?*sqlite3_stmt, N: c_int) [*c]const u8;
pub extern fn sqlite3_column_name16(?*sqlite3_stmt, N: c_int) ?*const anyopaque;
pub extern fn sqlite3_column_database_name(?*sqlite3_stmt, c_int) [*c]const u8;
pub extern fn sqlite3_column_database_name16(?*sqlite3_stmt, c_int) ?*const anyopaque;
pub extern fn sqlite3_column_table_name(?*sqlite3_stmt, c_int) [*c]const u8;
pub extern fn sqlite3_column_table_name16(?*sqlite3_stmt, c_int) ?*const anyopaque;
pub extern fn sqlite3_column_origin_name(?*sqlite3_stmt, c_int) [*c]const u8;
pub extern fn sqlite3_column_origin_name16(?*sqlite3_stmt, c_int) ?*const anyopaque;
pub extern fn sqlite3_column_decltype(?*sqlite3_stmt, c_int) [*c]const u8;
pub extern fn sqlite3_column_decltype16(?*sqlite3_stmt, c_int) ?*const anyopaque;
pub extern fn sqlite3_step(?*sqlite3_stmt) c_int;
pub extern fn sqlite3_data_count(pStmt: ?*sqlite3_stmt) c_int;
pub extern fn sqlite3_column_blob(?*sqlite3_stmt, iCol: c_int) ?*const anyopaque;
pub extern fn sqlite3_column_double(?*sqlite3_stmt, iCol: c_int) f64;
pub extern fn sqlite3_column_int(?*sqlite3_stmt, iCol: c_int) c_int;
pub extern fn sqlite3_column_int64(?*sqlite3_stmt, iCol: c_int) sqlite3_int64;
pub extern fn sqlite3_column_text(?*sqlite3_stmt, iCol: c_int) [*c]const u8;
pub extern fn sqlite3_column_text16(?*sqlite3_stmt, iCol: c_int) ?*const anyopaque;
pub extern fn sqlite3_column_value(?*sqlite3_stmt, iCol: c_int) ?*sqlite3_value;
pub extern fn sqlite3_column_bytes(?*sqlite3_stmt, iCol: c_int) c_int;
pub extern fn sqlite3_column_bytes16(?*sqlite3_stmt, iCol: c_int) c_int;
pub extern fn sqlite3_column_type(?*sqlite3_stmt, iCol: c_int) c_int;
pub extern fn sqlite3_finalize(pStmt: ?*sqlite3_stmt) c_int;
pub extern fn sqlite3_reset(pStmt: ?*sqlite3_stmt) c_int;
pub extern fn sqlite3_create_function(db: ?*sqlite3, zFunctionName: [*c]const u8, nArg: c_int, eTextRep: c_int, pApp: ?*anyopaque, xFunc: ?*const fn (?*sqlite3_context, c_int, [*c]?*sqlite3_value) callconv(.C) void, xStep: ?*const fn (?*sqlite3_context, c_int, [*c]?*sqlite3_value) callconv(.C) void, xFinal: ?*const fn (?*sqlite3_context) callconv(.C) void) c_int;
pub extern fn sqlite3_create_function16(db: ?*sqlite3, zFunctionName: ?*const anyopaque, nArg: c_int, eTextRep: c_int, pApp: ?*anyopaque, xFunc: ?*const fn (?*sqlite3_context, c_int, [*c]?*sqlite3_value) callconv(.C) void, xStep: ?*const fn (?*sqlite3_context, c_int, [*c]?*sqlite3_value) callconv(.C) void, xFinal: ?*const fn (?*sqlite3_context) callconv(.C) void) c_int;
pub extern fn sqlite3_create_function_v2(db: ?*sqlite3, zFunctionName: [*c]const u8, nArg: c_int, eTextRep: c_int, pApp: ?*anyopaque, xFunc: ?*const fn (?*sqlite3_context, c_int, [*c]?*sqlite3_value) callconv(.C) void, xStep: ?*const fn (?*sqlite3_context, c_int, [*c]?*sqlite3_value) callconv(.C) void, xFinal: ?*const fn (?*sqlite3_context) callconv(.C) void, xDestroy: ?*const fn (?*anyopaque) callconv(.C) void) c_int;
pub extern fn sqlite3_create_window_function(db: ?*sqlite3, zFunctionName: [*c]const u8, nArg: c_int, eTextRep: c_int, pApp: ?*anyopaque, xStep: ?*const fn (?*sqlite3_context, c_int, [*c]?*sqlite3_value) callconv(.C) void, xFinal: ?*const fn (?*sqlite3_context) callconv(.C) void, xValue: ?*const fn (?*sqlite3_context) callconv(.C) void, xInverse: ?*const fn (?*sqlite3_context, c_int, [*c]?*sqlite3_value) callconv(.C) void, xDestroy: ?*const fn (?*anyopaque) callconv(.C) void) c_int;
pub extern fn sqlite3_aggregate_count(?*sqlite3_context) c_int;
pub extern fn sqlite3_expired(?*sqlite3_stmt) c_int;
pub extern fn sqlite3_transfer_bindings(?*sqlite3_stmt, ?*sqlite3_stmt) c_int;
pub extern fn sqlite3_global_recover() c_int;
pub extern fn sqlite3_thread_cleanup() void;
pub extern fn sqlite3_memory_alarm(?*const fn (?*anyopaque, sqlite3_int64, c_int) callconv(.C) void, ?*anyopaque, sqlite3_int64) c_int;
pub extern fn sqlite3_value_blob(?*sqlite3_value) ?*const anyopaque;
pub extern fn sqlite3_value_double(?*sqlite3_value) f64;
pub extern fn sqlite3_value_int(?*sqlite3_value) c_int;
pub extern fn sqlite3_value_int64(?*sqlite3_value) sqlite3_int64;
pub extern fn sqlite3_value_pointer(?*sqlite3_value, [*c]const u8) ?*anyopaque;
pub extern fn sqlite3_value_text(?*sqlite3_value) [*c]const u8;
pub extern fn sqlite3_value_text16(?*sqlite3_value) ?*const anyopaque;
pub extern fn sqlite3_value_text16le(?*sqlite3_value) ?*const anyopaque;
pub extern fn sqlite3_value_text16be(?*sqlite3_value) ?*const anyopaque;
pub extern fn sqlite3_value_bytes(?*sqlite3_value) c_int;
pub extern fn sqlite3_value_bytes16(?*sqlite3_value) c_int;
pub extern fn sqlite3_value_type(?*sqlite3_value) c_int;
pub extern fn sqlite3_value_numeric_type(?*sqlite3_value) c_int;
pub extern fn sqlite3_value_nochange(?*sqlite3_value) c_int;
pub extern fn sqlite3_value_frombind(?*sqlite3_value) c_int;
pub extern fn sqlite3_value_encoding(?*sqlite3_value) c_int;
pub extern fn sqlite3_value_subtype(?*sqlite3_value) c_uint;
pub extern fn sqlite3_value_dup(?*const sqlite3_value) ?*sqlite3_value;
pub extern fn sqlite3_value_free(?*sqlite3_value) void;
pub extern fn sqlite3_aggregate_context(?*sqlite3_context, nBytes: c_int) ?*anyopaque;
pub extern fn sqlite3_user_data(?*sqlite3_context) ?*anyopaque;
pub extern fn sqlite3_context_db_handle(?*sqlite3_context) ?*sqlite3;
pub extern fn sqlite3_get_auxdata(?*sqlite3_context, N: c_int) ?*anyopaque;
pub extern fn sqlite3_set_auxdata(?*sqlite3_context, N: c_int, ?*anyopaque, ?*const fn (?*anyopaque) callconv(.C) void) void;
pub extern fn sqlite3_get_clientdata(?*sqlite3, [*c]const u8) ?*anyopaque;
pub extern fn sqlite3_set_clientdata(?*sqlite3, [*c]const u8, ?*anyopaque, ?*const fn (?*anyopaque) callconv(.C) void) c_int;
pub const sqlite3_destructor_type = ?*const fn (?*anyopaque) callconv(.C) void;
pub extern fn sqlite3_result_blob(?*sqlite3_context, ?*const anyopaque, c_int, ?*const fn (?*anyopaque) callconv(.C) void) void;
pub extern fn sqlite3_result_blob64(?*sqlite3_context, ?*const anyopaque, sqlite3_uint64, ?*const fn (?*anyopaque) callconv(.C) void) void;
pub extern fn sqlite3_result_double(?*sqlite3_context, f64) void;
pub extern fn sqlite3_result_error(?*sqlite3_context, [*c]const u8, c_int) void;
pub extern fn sqlite3_result_error16(?*sqlite3_context, ?*const anyopaque, c_int) void;
pub extern fn sqlite3_result_error_toobig(?*sqlite3_context) void;
pub extern fn sqlite3_result_error_nomem(?*sqlite3_context) void;
pub extern fn sqlite3_result_error_code(?*sqlite3_context, c_int) void;
pub extern fn sqlite3_result_int(?*sqlite3_context, c_int) void;
pub extern fn sqlite3_result_int64(?*sqlite3_context, sqlite3_int64) void;
pub extern fn sqlite3_result_null(?*sqlite3_context) void;
pub extern fn sqlite3_result_text(?*sqlite3_context, [*c]const u8, c_int, ?*const fn (?*anyopaque) callconv(.C) void) void;
pub extern fn sqlite3_result_text64(?*sqlite3_context, [*c]const u8, sqlite3_uint64, ?*const fn (?*anyopaque) callconv(.C) void, encoding: u8) void;
pub extern fn sqlite3_result_text16(?*sqlite3_context, ?*const anyopaque, c_int, ?*const fn (?*anyopaque) callconv(.C) void) void;
pub extern fn sqlite3_result_text16le(?*sqlite3_context, ?*const anyopaque, c_int, ?*const fn (?*anyopaque) callconv(.C) void) void;
pub extern fn sqlite3_result_text16be(?*sqlite3_context, ?*const anyopaque, c_int, ?*const fn (?*anyopaque) callconv(.C) void) void;
pub extern fn sqlite3_result_value(?*sqlite3_context, ?*sqlite3_value) void;
pub extern fn sqlite3_result_pointer(?*sqlite3_context, ?*anyopaque, [*c]const u8, ?*const fn (?*anyopaque) callconv(.C) void) void;
pub extern fn sqlite3_result_zeroblob(?*sqlite3_context, n: c_int) void;
pub extern fn sqlite3_result_zeroblob64(?*sqlite3_context, n: sqlite3_uint64) c_int;
pub extern fn sqlite3_result_subtype(?*sqlite3_context, c_uint) void;
pub extern fn sqlite3_create_collation(?*sqlite3, zName: [*c]const u8, eTextRep: c_int, pArg: ?*anyopaque, xCompare: ?*const fn (?*anyopaque, c_int, ?*const anyopaque, c_int, ?*const anyopaque) callconv(.C) c_int) c_int;
pub extern fn sqlite3_create_collation_v2(?*sqlite3, zName: [*c]const u8, eTextRep: c_int, pArg: ?*anyopaque, xCompare: ?*const fn (?*anyopaque, c_int, ?*const anyopaque, c_int, ?*const anyopaque) callconv(.C) c_int, xDestroy: ?*const fn (?*anyopaque) callconv(.C) void) c_int;
pub extern fn sqlite3_create_collation16(?*sqlite3, zName: ?*const anyopaque, eTextRep: c_int, pArg: ?*anyopaque, xCompare: ?*const fn (?*anyopaque, c_int, ?*const anyopaque, c_int, ?*const anyopaque) callconv(.C) c_int) c_int;
pub extern fn sqlite3_collation_needed(?*sqlite3, ?*anyopaque, ?*const fn (?*anyopaque, ?*sqlite3, c_int, [*c]const u8) callconv(.C) void) c_int;
pub extern fn sqlite3_collation_needed16(?*sqlite3, ?*anyopaque, ?*const fn (?*anyopaque, ?*sqlite3, c_int, ?*const anyopaque) callconv(.C) void) c_int;
pub extern fn sqlite3_sleep(c_int) c_int;
pub extern var sqlite3_temp_directory: [*c]u8;
pub extern var sqlite3_data_directory: [*c]u8;
pub extern fn sqlite3_win32_set_directory(@"type": c_ulong, zValue: ?*anyopaque) c_int;
pub extern fn sqlite3_win32_set_directory8(@"type": c_ulong, zValue: [*c]const u8) c_int;
pub extern fn sqlite3_win32_set_directory16(@"type": c_ulong, zValue: ?*const anyopaque) c_int;
pub extern fn sqlite3_get_autocommit(?*sqlite3) c_int;
pub extern fn sqlite3_db_handle(?*sqlite3_stmt) ?*sqlite3;
pub extern fn sqlite3_db_name(db: ?*sqlite3, N: c_int) [*c]const u8;
pub extern fn sqlite3_db_filename(db: ?*sqlite3, zDbName: [*c]const u8) sqlite3_filename;
pub extern fn sqlite3_db_readonly(db: ?*sqlite3, zDbName: [*c]const u8) c_int;
pub extern fn sqlite3_txn_state(?*sqlite3, zSchema: [*c]const u8) c_int;
pub extern fn sqlite3_next_stmt(pDb: ?*sqlite3, pStmt: ?*sqlite3_stmt) ?*sqlite3_stmt;
pub extern fn sqlite3_commit_hook(?*sqlite3, ?*const fn (?*anyopaque) callconv(.C) c_int, ?*anyopaque) ?*anyopaque;
pub extern fn sqlite3_rollback_hook(?*sqlite3, ?*const fn (?*anyopaque) callconv(.C) void, ?*anyopaque) ?*anyopaque;
pub extern fn sqlite3_autovacuum_pages(db: ?*sqlite3, ?*const fn (?*anyopaque, [*c]const u8, c_uint, c_uint, c_uint) callconv(.C) c_uint, ?*anyopaque, ?*const fn (?*anyopaque) callconv(.C) void) c_int;
pub extern fn sqlite3_update_hook(?*sqlite3, ?*const fn (?*anyopaque, c_int, [*c]const u8, [*c]const u8, sqlite3_int64) callconv(.C) void, ?*anyopaque) ?*anyopaque;
pub extern fn sqlite3_enable_shared_cache(c_int) c_int;
pub extern fn sqlite3_release_memory(c_int) c_int;
pub extern fn sqlite3_db_release_memory(?*sqlite3) c_int;
pub extern fn sqlite3_soft_heap_limit64(N: sqlite3_int64) sqlite3_int64;
pub extern fn sqlite3_hard_heap_limit64(N: sqlite3_int64) sqlite3_int64;
pub extern fn sqlite3_soft_heap_limit(N: c_int) void;
pub extern fn sqlite3_table_column_metadata(db: ?*sqlite3, zDbName: [*c]const u8, zTableName: [*c]const u8, zColumnName: [*c]const u8, pzDataType: [*c][*c]const u8, pzCollSeq: [*c][*c]const u8, pNotNull: [*c]c_int, pPrimaryKey: [*c]c_int, pAutoinc: [*c]c_int) c_int;
pub extern fn sqlite3_load_extension(db: ?*sqlite3, zFile: [*c]const u8, zProc: [*c]const u8, pzErrMsg: [*c][*c]u8) c_int;
pub extern fn sqlite3_enable_load_extension(db: ?*sqlite3, onoff: c_int) c_int;
pub extern fn sqlite3_auto_extension(xEntryPoint: ?*const fn () callconv(.C) void) c_int;
pub extern fn sqlite3_cancel_auto_extension(xEntryPoint: ?*const fn () callconv(.C) void) c_int;
pub extern fn sqlite3_reset_auto_extension() void;
pub const sqlite3_vtab = struct_sqlite3_vtab;
pub const struct_sqlite3_index_constraint_2 = extern struct {
    iColumn: c_int = @import("std").mem.zeroes(c_int),
    op: u8 = @import("std").mem.zeroes(u8),
    usable: u8 = @import("std").mem.zeroes(u8),
    iTermOffset: c_int = @import("std").mem.zeroes(c_int),
};
pub const struct_sqlite3_index_orderby_3 = extern struct {
    iColumn: c_int = @import("std").mem.zeroes(c_int),
    desc: u8 = @import("std").mem.zeroes(u8),
};
pub const struct_sqlite3_index_constraint_usage_4 = extern struct {
    argvIndex: c_int = @import("std").mem.zeroes(c_int),
    omit: u8 = @import("std").mem.zeroes(u8),
};
pub const struct_sqlite3_index_info = extern struct {
    nConstraint: c_int = @import("std").mem.zeroes(c_int),
    aConstraint: [*c]struct_sqlite3_index_constraint_2 = @import("std").mem.zeroes([*c]struct_sqlite3_index_constraint_2),
    nOrderBy: c_int = @import("std").mem.zeroes(c_int),
    aOrderBy: [*c]struct_sqlite3_index_orderby_3 = @import("std").mem.zeroes([*c]struct_sqlite3_index_orderby_3),
    aConstraintUsage: [*c]struct_sqlite3_index_constraint_usage_4 = @import("std").mem.zeroes([*c]struct_sqlite3_index_constraint_usage_4),
    idxNum: c_int = @import("std").mem.zeroes(c_int),
    idxStr: [*c]u8 = @import("std").mem.zeroes([*c]u8),
    needToFreeIdxStr: c_int = @import("std").mem.zeroes(c_int),
    orderByConsumed: c_int = @import("std").mem.zeroes(c_int),
    estimatedCost: f64 = @import("std").mem.zeroes(f64),
    estimatedRows: sqlite3_int64 = @import("std").mem.zeroes(sqlite3_int64),
    idxFlags: c_int = @import("std").mem.zeroes(c_int),
    colUsed: sqlite3_uint64 = @import("std").mem.zeroes(sqlite3_uint64),
};
pub const sqlite3_index_info = struct_sqlite3_index_info;
pub const struct_sqlite3_vtab_cursor = extern struct {
    pVtab: [*c]sqlite3_vtab = @import("std").mem.zeroes([*c]sqlite3_vtab),
};
pub const sqlite3_vtab_cursor = struct_sqlite3_vtab_cursor;
pub const struct_sqlite3_module = extern struct {
    iVersion: c_int = @import("std").mem.zeroes(c_int),
    xCreate: ?*const fn (?*sqlite3, ?*anyopaque, c_int, [*c]const [*c]const u8, [*c][*c]sqlite3_vtab, [*c][*c]u8) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn (?*sqlite3, ?*anyopaque, c_int, [*c]const [*c]const u8, [*c][*c]sqlite3_vtab, [*c][*c]u8) callconv(.C) c_int),
    xConnect: ?*const fn (?*sqlite3, ?*anyopaque, c_int, [*c]const [*c]const u8, [*c][*c]sqlite3_vtab, [*c][*c]u8) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn (?*sqlite3, ?*anyopaque, c_int, [*c]const [*c]const u8, [*c][*c]sqlite3_vtab, [*c][*c]u8) callconv(.C) c_int),
    xBestIndex: ?*const fn ([*c]sqlite3_vtab, [*c]sqlite3_index_info) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn ([*c]sqlite3_vtab, [*c]sqlite3_index_info) callconv(.C) c_int),
    xDisconnect: ?*const fn ([*c]sqlite3_vtab) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn ([*c]sqlite3_vtab) callconv(.C) c_int),
    xDestroy: ?*const fn ([*c]sqlite3_vtab) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn ([*c]sqlite3_vtab) callconv(.C) c_int),
    xOpen: ?*const fn ([*c]sqlite3_vtab, [*c][*c]sqlite3_vtab_cursor) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn ([*c]sqlite3_vtab, [*c][*c]sqlite3_vtab_cursor) callconv(.C) c_int),
    xClose: ?*const fn ([*c]sqlite3_vtab_cursor) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn ([*c]sqlite3_vtab_cursor) callconv(.C) c_int),
    xFilter: ?*const fn ([*c]sqlite3_vtab_cursor, c_int, [*c]const u8, c_int, [*c]?*sqlite3_value) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn ([*c]sqlite3_vtab_cursor, c_int, [*c]const u8, c_int, [*c]?*sqlite3_value) callconv(.C) c_int),
    xNext: ?*const fn ([*c]sqlite3_vtab_cursor) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn ([*c]sqlite3_vtab_cursor) callconv(.C) c_int),
    xEof: ?*const fn ([*c]sqlite3_vtab_cursor) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn ([*c]sqlite3_vtab_cursor) callconv(.C) c_int),
    xColumn: ?*const fn ([*c]sqlite3_vtab_cursor, ?*sqlite3_context, c_int) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn ([*c]sqlite3_vtab_cursor, ?*sqlite3_context, c_int) callconv(.C) c_int),
    xRowid: ?*const fn ([*c]sqlite3_vtab_cursor, [*c]sqlite3_int64) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn ([*c]sqlite3_vtab_cursor, [*c]sqlite3_int64) callconv(.C) c_int),
    xUpdate: ?*const fn ([*c]sqlite3_vtab, c_int, [*c]?*sqlite3_value, [*c]sqlite3_int64) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn ([*c]sqlite3_vtab, c_int, [*c]?*sqlite3_value, [*c]sqlite3_int64) callconv(.C) c_int),
    xBegin: ?*const fn ([*c]sqlite3_vtab) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn ([*c]sqlite3_vtab) callconv(.C) c_int),
    xSync: ?*const fn ([*c]sqlite3_vtab) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn ([*c]sqlite3_vtab) callconv(.C) c_int),
    xCommit: ?*const fn ([*c]sqlite3_vtab) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn ([*c]sqlite3_vtab) callconv(.C) c_int),
    xRollback: ?*const fn ([*c]sqlite3_vtab) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn ([*c]sqlite3_vtab) callconv(.C) c_int),
    xFindFunction: ?*const fn ([*c]sqlite3_vtab, c_int, [*c]const u8, [*c]?*const fn (?*sqlite3_context, c_int, [*c]?*sqlite3_value) callconv(.C) void, [*c]?*anyopaque) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn ([*c]sqlite3_vtab, c_int, [*c]const u8, [*c]?*const fn (?*sqlite3_context, c_int, [*c]?*sqlite3_value) callconv(.C) void, [*c]?*anyopaque) callconv(.C) c_int),
    xRename: ?*const fn ([*c]sqlite3_vtab, [*c]const u8) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn ([*c]sqlite3_vtab, [*c]const u8) callconv(.C) c_int),
    xSavepoint: ?*const fn ([*c]sqlite3_vtab, c_int) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn ([*c]sqlite3_vtab, c_int) callconv(.C) c_int),
    xRelease: ?*const fn ([*c]sqlite3_vtab, c_int) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn ([*c]sqlite3_vtab, c_int) callconv(.C) c_int),
    xRollbackTo: ?*const fn ([*c]sqlite3_vtab, c_int) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn ([*c]sqlite3_vtab, c_int) callconv(.C) c_int),
    xShadowName: ?*const fn ([*c]const u8) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn ([*c]const u8) callconv(.C) c_int),
    xIntegrity: ?*const fn ([*c]sqlite3_vtab, [*c]const u8, [*c]const u8, c_int, [*c][*c]u8) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn ([*c]sqlite3_vtab, [*c]const u8, [*c]const u8, c_int, [*c][*c]u8) callconv(.C) c_int),
};
pub const sqlite3_module = struct_sqlite3_module;
pub const struct_sqlite3_vtab = extern struct {
    pModule: [*c]const sqlite3_module = @import("std").mem.zeroes([*c]const sqlite3_module),
    nRef: c_int = @import("std").mem.zeroes(c_int),
    zErrMsg: [*c]u8 = @import("std").mem.zeroes([*c]u8),
};
pub extern fn sqlite3_create_module(db: ?*sqlite3, zName: [*c]const u8, p: [*c]const sqlite3_module, pClientData: ?*anyopaque) c_int;
pub extern fn sqlite3_create_module_v2(db: ?*sqlite3, zName: [*c]const u8, p: [*c]const sqlite3_module, pClientData: ?*anyopaque, xDestroy: ?*const fn (?*anyopaque) callconv(.C) void) c_int;
pub extern fn sqlite3_drop_modules(db: ?*sqlite3, azKeep: [*c][*c]const u8) c_int;
pub extern fn sqlite3_declare_vtab(?*sqlite3, zSQL: [*c]const u8) c_int;
pub extern fn sqlite3_overload_function(?*sqlite3, zFuncName: [*c]const u8, nArg: c_int) c_int;
pub const struct_sqlite3_blob = opaque {};
pub const sqlite3_blob = struct_sqlite3_blob;
pub extern fn sqlite3_blob_open(?*sqlite3, zDb: [*c]const u8, zTable: [*c]const u8, zColumn: [*c]const u8, iRow: sqlite3_int64, flags: c_int, ppBlob: [*c]?*sqlite3_blob) c_int;
pub extern fn sqlite3_blob_reopen(?*sqlite3_blob, sqlite3_int64) c_int;
pub extern fn sqlite3_blob_close(?*sqlite3_blob) c_int;
pub extern fn sqlite3_blob_bytes(?*sqlite3_blob) c_int;
pub extern fn sqlite3_blob_read(?*sqlite3_blob, Z: ?*anyopaque, N: c_int, iOffset: c_int) c_int;
pub extern fn sqlite3_blob_write(?*sqlite3_blob, z: ?*const anyopaque, n: c_int, iOffset: c_int) c_int;
pub extern fn sqlite3_vfs_find(zVfsName: [*c]const u8) [*c]sqlite3_vfs;
pub extern fn sqlite3_vfs_register([*c]sqlite3_vfs, makeDflt: c_int) c_int;
pub extern fn sqlite3_vfs_unregister([*c]sqlite3_vfs) c_int;
pub extern fn sqlite3_mutex_alloc(c_int) ?*sqlite3_mutex;
pub extern fn sqlite3_mutex_free(?*sqlite3_mutex) void;
pub extern fn sqlite3_mutex_enter(?*sqlite3_mutex) void;
pub extern fn sqlite3_mutex_try(?*sqlite3_mutex) c_int;
pub extern fn sqlite3_mutex_leave(?*sqlite3_mutex) void;
pub const struct_sqlite3_mutex_methods = extern struct {
    xMutexInit: ?*const fn () callconv(.C) c_int = @import("std").mem.zeroes(?*const fn () callconv(.C) c_int),
    xMutexEnd: ?*const fn () callconv(.C) c_int = @import("std").mem.zeroes(?*const fn () callconv(.C) c_int),
    xMutexAlloc: ?*const fn (c_int) callconv(.C) ?*sqlite3_mutex = @import("std").mem.zeroes(?*const fn (c_int) callconv(.C) ?*sqlite3_mutex),
    xMutexFree: ?*const fn (?*sqlite3_mutex) callconv(.C) void = @import("std").mem.zeroes(?*const fn (?*sqlite3_mutex) callconv(.C) void),
    xMutexEnter: ?*const fn (?*sqlite3_mutex) callconv(.C) void = @import("std").mem.zeroes(?*const fn (?*sqlite3_mutex) callconv(.C) void),
    xMutexTry: ?*const fn (?*sqlite3_mutex) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn (?*sqlite3_mutex) callconv(.C) c_int),
    xMutexLeave: ?*const fn (?*sqlite3_mutex) callconv(.C) void = @import("std").mem.zeroes(?*const fn (?*sqlite3_mutex) callconv(.C) void),
    xMutexHeld: ?*const fn (?*sqlite3_mutex) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn (?*sqlite3_mutex) callconv(.C) c_int),
    xMutexNotheld: ?*const fn (?*sqlite3_mutex) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn (?*sqlite3_mutex) callconv(.C) c_int),
};
pub const sqlite3_mutex_methods = struct_sqlite3_mutex_methods;
pub extern fn sqlite3_mutex_held(?*sqlite3_mutex) c_int;
pub extern fn sqlite3_mutex_notheld(?*sqlite3_mutex) c_int;
pub extern fn sqlite3_db_mutex(?*sqlite3) ?*sqlite3_mutex;
pub extern fn sqlite3_file_control(?*sqlite3, zDbName: [*c]const u8, op: c_int, ?*anyopaque) c_int;
pub extern fn sqlite3_test_control(op: c_int, ...) c_int;
pub extern fn sqlite3_keyword_count() c_int;
pub extern fn sqlite3_keyword_name(c_int, [*c][*c]const u8, [*c]c_int) c_int;
pub extern fn sqlite3_keyword_check([*c]const u8, c_int) c_int;
pub const struct_sqlite3_str = opaque {};
pub const sqlite3_str = struct_sqlite3_str;
pub extern fn sqlite3_str_new(?*sqlite3) ?*sqlite3_str;
pub extern fn sqlite3_str_finish(?*sqlite3_str) [*c]u8;
pub extern fn sqlite3_str_appendf(?*sqlite3_str, zFormat: [*c]const u8, ...) void;
pub extern fn sqlite3_str_vappendf(?*sqlite3_str, zFormat: [*c]const u8, va_list) void;
pub extern fn sqlite3_str_append(?*sqlite3_str, zIn: [*c]const u8, N: c_int) void;
pub extern fn sqlite3_str_appendall(?*sqlite3_str, zIn: [*c]const u8) void;
pub extern fn sqlite3_str_appendchar(?*sqlite3_str, N: c_int, C: u8) void;
pub extern fn sqlite3_str_reset(?*sqlite3_str) void;
pub extern fn sqlite3_str_errcode(?*sqlite3_str) c_int;
pub extern fn sqlite3_str_length(?*sqlite3_str) c_int;
pub extern fn sqlite3_str_value(?*sqlite3_str) [*c]u8;
pub extern fn sqlite3_status(op: c_int, pCurrent: [*c]c_int, pHighwater: [*c]c_int, resetFlag: c_int) c_int;
pub extern fn sqlite3_status64(op: c_int, pCurrent: [*c]sqlite3_int64, pHighwater: [*c]sqlite3_int64, resetFlag: c_int) c_int;
pub extern fn sqlite3_db_status(?*sqlite3, op: c_int, pCur: [*c]c_int, pHiwtr: [*c]c_int, resetFlg: c_int) c_int;
pub extern fn sqlite3_stmt_status(?*sqlite3_stmt, op: c_int, resetFlg: c_int) c_int;
pub const struct_sqlite3_pcache = opaque {};
pub const sqlite3_pcache = struct_sqlite3_pcache;
pub const struct_sqlite3_pcache_page = extern struct {
    pBuf: ?*anyopaque = @import("std").mem.zeroes(?*anyopaque),
    pExtra: ?*anyopaque = @import("std").mem.zeroes(?*anyopaque),
};
pub const sqlite3_pcache_page = struct_sqlite3_pcache_page;
pub const struct_sqlite3_pcache_methods2 = extern struct {
    iVersion: c_int = @import("std").mem.zeroes(c_int),
    pArg: ?*anyopaque = @import("std").mem.zeroes(?*anyopaque),
    xInit: ?*const fn (?*anyopaque) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn (?*anyopaque) callconv(.C) c_int),
    xShutdown: ?*const fn (?*anyopaque) callconv(.C) void = @import("std").mem.zeroes(?*const fn (?*anyopaque) callconv(.C) void),
    xCreate: ?*const fn (c_int, c_int, c_int) callconv(.C) ?*sqlite3_pcache = @import("std").mem.zeroes(?*const fn (c_int, c_int, c_int) callconv(.C) ?*sqlite3_pcache),
    xCachesize: ?*const fn (?*sqlite3_pcache, c_int) callconv(.C) void = @import("std").mem.zeroes(?*const fn (?*sqlite3_pcache, c_int) callconv(.C) void),
    xPagecount: ?*const fn (?*sqlite3_pcache) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn (?*sqlite3_pcache) callconv(.C) c_int),
    xFetch: ?*const fn (?*sqlite3_pcache, c_uint, c_int) callconv(.C) [*c]sqlite3_pcache_page = @import("std").mem.zeroes(?*const fn (?*sqlite3_pcache, c_uint, c_int) callconv(.C) [*c]sqlite3_pcache_page),
    xUnpin: ?*const fn (?*sqlite3_pcache, [*c]sqlite3_pcache_page, c_int) callconv(.C) void = @import("std").mem.zeroes(?*const fn (?*sqlite3_pcache, [*c]sqlite3_pcache_page, c_int) callconv(.C) void),
    xRekey: ?*const fn (?*sqlite3_pcache, [*c]sqlite3_pcache_page, c_uint, c_uint) callconv(.C) void = @import("std").mem.zeroes(?*const fn (?*sqlite3_pcache, [*c]sqlite3_pcache_page, c_uint, c_uint) callconv(.C) void),
    xTruncate: ?*const fn (?*sqlite3_pcache, c_uint) callconv(.C) void = @import("std").mem.zeroes(?*const fn (?*sqlite3_pcache, c_uint) callconv(.C) void),
    xDestroy: ?*const fn (?*sqlite3_pcache) callconv(.C) void = @import("std").mem.zeroes(?*const fn (?*sqlite3_pcache) callconv(.C) void),
    xShrink: ?*const fn (?*sqlite3_pcache) callconv(.C) void = @import("std").mem.zeroes(?*const fn (?*sqlite3_pcache) callconv(.C) void),
};
pub const sqlite3_pcache_methods2 = struct_sqlite3_pcache_methods2;
pub const struct_sqlite3_pcache_methods = extern struct {
    pArg: ?*anyopaque = @import("std").mem.zeroes(?*anyopaque),
    xInit: ?*const fn (?*anyopaque) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn (?*anyopaque) callconv(.C) c_int),
    xShutdown: ?*const fn (?*anyopaque) callconv(.C) void = @import("std").mem.zeroes(?*const fn (?*anyopaque) callconv(.C) void),
    xCreate: ?*const fn (c_int, c_int) callconv(.C) ?*sqlite3_pcache = @import("std").mem.zeroes(?*const fn (c_int, c_int) callconv(.C) ?*sqlite3_pcache),
    xCachesize: ?*const fn (?*sqlite3_pcache, c_int) callconv(.C) void = @import("std").mem.zeroes(?*const fn (?*sqlite3_pcache, c_int) callconv(.C) void),
    xPagecount: ?*const fn (?*sqlite3_pcache) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn (?*sqlite3_pcache) callconv(.C) c_int),
    xFetch: ?*const fn (?*sqlite3_pcache, c_uint, c_int) callconv(.C) ?*anyopaque = @import("std").mem.zeroes(?*const fn (?*sqlite3_pcache, c_uint, c_int) callconv(.C) ?*anyopaque),
    xUnpin: ?*const fn (?*sqlite3_pcache, ?*anyopaque, c_int) callconv(.C) void = @import("std").mem.zeroes(?*const fn (?*sqlite3_pcache, ?*anyopaque, c_int) callconv(.C) void),
    xRekey: ?*const fn (?*sqlite3_pcache, ?*anyopaque, c_uint, c_uint) callconv(.C) void = @import("std").mem.zeroes(?*const fn (?*sqlite3_pcache, ?*anyopaque, c_uint, c_uint) callconv(.C) void),
    xTruncate: ?*const fn (?*sqlite3_pcache, c_uint) callconv(.C) void = @import("std").mem.zeroes(?*const fn (?*sqlite3_pcache, c_uint) callconv(.C) void),
    xDestroy: ?*const fn (?*sqlite3_pcache) callconv(.C) void = @import("std").mem.zeroes(?*const fn (?*sqlite3_pcache) callconv(.C) void),
};
pub const sqlite3_pcache_methods = struct_sqlite3_pcache_methods;
pub const struct_sqlite3_backup = opaque {};
pub const sqlite3_backup = struct_sqlite3_backup;
pub extern fn sqlite3_backup_init(pDest: ?*sqlite3, zDestName: [*c]const u8, pSource: ?*sqlite3, zSourceName: [*c]const u8) ?*sqlite3_backup;
pub extern fn sqlite3_backup_step(p: ?*sqlite3_backup, nPage: c_int) c_int;
pub extern fn sqlite3_backup_finish(p: ?*sqlite3_backup) c_int;
pub extern fn sqlite3_backup_remaining(p: ?*sqlite3_backup) c_int;
pub extern fn sqlite3_backup_pagecount(p: ?*sqlite3_backup) c_int;
pub extern fn sqlite3_unlock_notify(pBlocked: ?*sqlite3, xNotify: ?*const fn ([*c]?*anyopaque, c_int) callconv(.C) void, pNotifyArg: ?*anyopaque) c_int;
pub extern fn sqlite3_stricmp([*c]const u8, [*c]const u8) c_int;
pub extern fn sqlite3_strnicmp([*c]const u8, [*c]const u8, c_int) c_int;
pub extern fn sqlite3_strglob(zGlob: [*c]const u8, zStr: [*c]const u8) c_int;
pub extern fn sqlite3_strlike(zGlob: [*c]const u8, zStr: [*c]const u8, cEsc: c_uint) c_int;
pub extern fn sqlite3_log(iErrCode: c_int, zFormat: [*c]const u8, ...) void;
pub extern fn sqlite3_wal_hook(?*sqlite3, ?*const fn (?*anyopaque, ?*sqlite3, [*c]const u8, c_int) callconv(.C) c_int, ?*anyopaque) ?*anyopaque;
pub extern fn sqlite3_wal_autocheckpoint(db: ?*sqlite3, N: c_int) c_int;
pub extern fn sqlite3_wal_checkpoint(db: ?*sqlite3, zDb: [*c]const u8) c_int;
pub extern fn sqlite3_wal_checkpoint_v2(db: ?*sqlite3, zDb: [*c]const u8, eMode: c_int, pnLog: [*c]c_int, pnCkpt: [*c]c_int) c_int;
pub extern fn sqlite3_vtab_config(?*sqlite3, op: c_int, ...) c_int;
pub extern fn sqlite3_vtab_on_conflict(?*sqlite3) c_int;
pub extern fn sqlite3_vtab_nochange(?*sqlite3_context) c_int;
pub extern fn sqlite3_vtab_collation([*c]sqlite3_index_info, c_int) [*c]const u8;
pub extern fn sqlite3_vtab_distinct([*c]sqlite3_index_info) c_int;
pub extern fn sqlite3_vtab_in([*c]sqlite3_index_info, iCons: c_int, bHandle: c_int) c_int;
pub extern fn sqlite3_vtab_in_first(pVal: ?*sqlite3_value, ppOut: [*c]?*sqlite3_value) c_int;
pub extern fn sqlite3_vtab_in_next(pVal: ?*sqlite3_value, ppOut: [*c]?*sqlite3_value) c_int;
pub extern fn sqlite3_vtab_rhs_value([*c]sqlite3_index_info, c_int, ppVal: [*c]?*sqlite3_value) c_int;
pub extern fn sqlite3_stmt_scanstatus(pStmt: ?*sqlite3_stmt, idx: c_int, iScanStatusOp: c_int, pOut: ?*anyopaque) c_int;
pub extern fn sqlite3_stmt_scanstatus_v2(pStmt: ?*sqlite3_stmt, idx: c_int, iScanStatusOp: c_int, flags: c_int, pOut: ?*anyopaque) c_int;
pub extern fn sqlite3_stmt_scanstatus_reset(?*sqlite3_stmt) void;
pub extern fn sqlite3_db_cacheflush(?*sqlite3) c_int;
pub extern fn sqlite3_system_errno(?*sqlite3) c_int;
pub const struct_sqlite3_snapshot = extern struct {
    hidden: [48]u8 = @import("std").mem.zeroes([48]u8),
};
pub const sqlite3_snapshot = struct_sqlite3_snapshot;
pub extern fn sqlite3_snapshot_get(db: ?*sqlite3, zSchema: [*c]const u8, ppSnapshot: [*c][*c]sqlite3_snapshot) c_int;
pub extern fn sqlite3_snapshot_open(db: ?*sqlite3, zSchema: [*c]const u8, pSnapshot: [*c]sqlite3_snapshot) c_int;
pub extern fn sqlite3_snapshot_free([*c]sqlite3_snapshot) void;
pub extern fn sqlite3_snapshot_cmp(p1: [*c]sqlite3_snapshot, p2: [*c]sqlite3_snapshot) c_int;
pub extern fn sqlite3_snapshot_recover(db: ?*sqlite3, zDb: [*c]const u8) c_int;
pub extern fn sqlite3_serialize(db: ?*sqlite3, zSchema: [*c]const u8, piSize: [*c]sqlite3_int64, mFlags: c_uint) [*c]u8;
pub extern fn sqlite3_deserialize(db: ?*sqlite3, zSchema: [*c]const u8, pData: [*c]u8, szDb: sqlite3_int64, szBuf: sqlite3_int64, mFlags: c_uint) c_int;
pub const sqlite3_rtree_dbl = f64;
pub const struct_sqlite3_rtree_geometry = extern struct {
    pContext: ?*anyopaque = @import("std").mem.zeroes(?*anyopaque),
    nParam: c_int = @import("std").mem.zeroes(c_int),
    aParam: [*c]sqlite3_rtree_dbl = @import("std").mem.zeroes([*c]sqlite3_rtree_dbl),
    pUser: ?*anyopaque = @import("std").mem.zeroes(?*anyopaque),
    xDelUser: ?*const fn (?*anyopaque) callconv(.C) void = @import("std").mem.zeroes(?*const fn (?*anyopaque) callconv(.C) void),
};
pub const sqlite3_rtree_geometry = struct_sqlite3_rtree_geometry;
pub const struct_sqlite3_rtree_query_info = extern struct {
    pContext: ?*anyopaque = @import("std").mem.zeroes(?*anyopaque),
    nParam: c_int = @import("std").mem.zeroes(c_int),
    aParam: [*c]sqlite3_rtree_dbl = @import("std").mem.zeroes([*c]sqlite3_rtree_dbl),
    pUser: ?*anyopaque = @import("std").mem.zeroes(?*anyopaque),
    xDelUser: ?*const fn (?*anyopaque) callconv(.C) void = @import("std").mem.zeroes(?*const fn (?*anyopaque) callconv(.C) void),
    aCoord: [*c]sqlite3_rtree_dbl = @import("std").mem.zeroes([*c]sqlite3_rtree_dbl),
    anQueue: [*c]c_uint = @import("std").mem.zeroes([*c]c_uint),
    nCoord: c_int = @import("std").mem.zeroes(c_int),
    iLevel: c_int = @import("std").mem.zeroes(c_int),
    mxLevel: c_int = @import("std").mem.zeroes(c_int),
    iRowid: sqlite3_int64 = @import("std").mem.zeroes(sqlite3_int64),
    rParentScore: sqlite3_rtree_dbl = @import("std").mem.zeroes(sqlite3_rtree_dbl),
    eParentWithin: c_int = @import("std").mem.zeroes(c_int),
    eWithin: c_int = @import("std").mem.zeroes(c_int),
    rScore: sqlite3_rtree_dbl = @import("std").mem.zeroes(sqlite3_rtree_dbl),
    apSqlParam: [*c]?*sqlite3_value = @import("std").mem.zeroes([*c]?*sqlite3_value),
};
pub const sqlite3_rtree_query_info = struct_sqlite3_rtree_query_info;
pub extern fn sqlite3_rtree_geometry_callback(db: ?*sqlite3, zGeom: [*c]const u8, xGeom: ?*const fn ([*c]sqlite3_rtree_geometry, c_int, [*c]sqlite3_rtree_dbl, [*c]c_int) callconv(.C) c_int, pContext: ?*anyopaque) c_int;
pub extern fn sqlite3_rtree_query_callback(db: ?*sqlite3, zQueryFunc: [*c]const u8, xQueryFunc: ?*const fn ([*c]sqlite3_rtree_query_info) callconv(.C) c_int, pContext: ?*anyopaque, xDestructor: ?*const fn (?*anyopaque) callconv(.C) void) c_int;
pub const struct_Fts5Context = opaque {};
pub const Fts5Context = struct_Fts5Context;
pub const Fts5ExtensionApi = struct_Fts5ExtensionApi;
pub const struct_Fts5PhraseIter = extern struct {
    a: [*c]const u8 = @import("std").mem.zeroes([*c]const u8),
    b: [*c]const u8 = @import("std").mem.zeroes([*c]const u8),
};
pub const Fts5PhraseIter = struct_Fts5PhraseIter;
pub const struct_Fts5ExtensionApi = extern struct {
    iVersion: c_int = @import("std").mem.zeroes(c_int),
    xUserData: ?*const fn (?*Fts5Context) callconv(.C) ?*anyopaque = @import("std").mem.zeroes(?*const fn (?*Fts5Context) callconv(.C) ?*anyopaque),
    xColumnCount: ?*const fn (?*Fts5Context) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn (?*Fts5Context) callconv(.C) c_int),
    xRowCount: ?*const fn (?*Fts5Context, [*c]sqlite3_int64) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn (?*Fts5Context, [*c]sqlite3_int64) callconv(.C) c_int),
    xColumnTotalSize: ?*const fn (?*Fts5Context, c_int, [*c]sqlite3_int64) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn (?*Fts5Context, c_int, [*c]sqlite3_int64) callconv(.C) c_int),
    xTokenize: ?*const fn (?*Fts5Context, [*c]const u8, c_int, ?*anyopaque, ?*const fn (?*anyopaque, c_int, [*c]const u8, c_int, c_int, c_int) callconv(.C) c_int) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn (?*Fts5Context, [*c]const u8, c_int, ?*anyopaque, ?*const fn (?*anyopaque, c_int, [*c]const u8, c_int, c_int, c_int) callconv(.C) c_int) callconv(.C) c_int),
    xPhraseCount: ?*const fn (?*Fts5Context) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn (?*Fts5Context) callconv(.C) c_int),
    xPhraseSize: ?*const fn (?*Fts5Context, c_int) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn (?*Fts5Context, c_int) callconv(.C) c_int),
    xInstCount: ?*const fn (?*Fts5Context, [*c]c_int) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn (?*Fts5Context, [*c]c_int) callconv(.C) c_int),
    xInst: ?*const fn (?*Fts5Context, c_int, [*c]c_int, [*c]c_int, [*c]c_int) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn (?*Fts5Context, c_int, [*c]c_int, [*c]c_int, [*c]c_int) callconv(.C) c_int),
    xRowid: ?*const fn (?*Fts5Context) callconv(.C) sqlite3_int64 = @import("std").mem.zeroes(?*const fn (?*Fts5Context) callconv(.C) sqlite3_int64),
    xColumnText: ?*const fn (?*Fts5Context, c_int, [*c][*c]const u8, [*c]c_int) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn (?*Fts5Context, c_int, [*c][*c]const u8, [*c]c_int) callconv(.C) c_int),
    xColumnSize: ?*const fn (?*Fts5Context, c_int, [*c]c_int) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn (?*Fts5Context, c_int, [*c]c_int) callconv(.C) c_int),
    xQueryPhrase: ?*const fn (?*Fts5Context, c_int, ?*anyopaque, ?*const fn ([*c]const Fts5ExtensionApi, ?*Fts5Context, ?*anyopaque) callconv(.C) c_int) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn (?*Fts5Context, c_int, ?*anyopaque, ?*const fn ([*c]const Fts5ExtensionApi, ?*Fts5Context, ?*anyopaque) callconv(.C) c_int) callconv(.C) c_int),
    xSetAuxdata: ?*const fn (?*Fts5Context, ?*anyopaque, ?*const fn (?*anyopaque) callconv(.C) void) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn (?*Fts5Context, ?*anyopaque, ?*const fn (?*anyopaque) callconv(.C) void) callconv(.C) c_int),
    xGetAuxdata: ?*const fn (?*Fts5Context, c_int) callconv(.C) ?*anyopaque = @import("std").mem.zeroes(?*const fn (?*Fts5Context, c_int) callconv(.C) ?*anyopaque),
    xPhraseFirst: ?*const fn (?*Fts5Context, c_int, [*c]Fts5PhraseIter, [*c]c_int, [*c]c_int) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn (?*Fts5Context, c_int, [*c]Fts5PhraseIter, [*c]c_int, [*c]c_int) callconv(.C) c_int),
    xPhraseNext: ?*const fn (?*Fts5Context, [*c]Fts5PhraseIter, [*c]c_int, [*c]c_int) callconv(.C) void = @import("std").mem.zeroes(?*const fn (?*Fts5Context, [*c]Fts5PhraseIter, [*c]c_int, [*c]c_int) callconv(.C) void),
    xPhraseFirstColumn: ?*const fn (?*Fts5Context, c_int, [*c]Fts5PhraseIter, [*c]c_int) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn (?*Fts5Context, c_int, [*c]Fts5PhraseIter, [*c]c_int) callconv(.C) c_int),
    xPhraseNextColumn: ?*const fn (?*Fts5Context, [*c]Fts5PhraseIter, [*c]c_int) callconv(.C) void = @import("std").mem.zeroes(?*const fn (?*Fts5Context, [*c]Fts5PhraseIter, [*c]c_int) callconv(.C) void),
    xQueryToken: ?*const fn (?*Fts5Context, c_int, c_int, [*c][*c]const u8, [*c]c_int) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn (?*Fts5Context, c_int, c_int, [*c][*c]const u8, [*c]c_int) callconv(.C) c_int),
    xInstToken: ?*const fn (?*Fts5Context, c_int, c_int, [*c][*c]const u8, [*c]c_int) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn (?*Fts5Context, c_int, c_int, [*c][*c]const u8, [*c]c_int) callconv(.C) c_int),
};
pub const fts5_extension_function = ?*const fn ([*c]const Fts5ExtensionApi, ?*Fts5Context, ?*sqlite3_context, c_int, [*c]?*sqlite3_value) callconv(.C) void;
pub const struct_Fts5Tokenizer = opaque {};
pub const Fts5Tokenizer = struct_Fts5Tokenizer;
pub const struct_fts5_tokenizer = extern struct {
    xCreate: ?*const fn (?*anyopaque, [*c][*c]const u8, c_int, [*c]?*Fts5Tokenizer) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn (?*anyopaque, [*c][*c]const u8, c_int, [*c]?*Fts5Tokenizer) callconv(.C) c_int),
    xDelete: ?*const fn (?*Fts5Tokenizer) callconv(.C) void = @import("std").mem.zeroes(?*const fn (?*Fts5Tokenizer) callconv(.C) void),
    xTokenize: ?*const fn (?*Fts5Tokenizer, ?*anyopaque, c_int, [*c]const u8, c_int, ?*const fn (?*anyopaque, c_int, [*c]const u8, c_int, c_int, c_int) callconv(.C) c_int) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn (?*Fts5Tokenizer, ?*anyopaque, c_int, [*c]const u8, c_int, ?*const fn (?*anyopaque, c_int, [*c]const u8, c_int, c_int, c_int) callconv(.C) c_int) callconv(.C) c_int),
};
pub const fts5_tokenizer = struct_fts5_tokenizer;
pub const fts5_api = struct_fts5_api;
pub const struct_fts5_api = extern struct {
    iVersion: c_int = @import("std").mem.zeroes(c_int),
    xCreateTokenizer: ?*const fn ([*c]fts5_api, [*c]const u8, ?*anyopaque, [*c]fts5_tokenizer, ?*const fn (?*anyopaque) callconv(.C) void) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn ([*c]fts5_api, [*c]const u8, ?*anyopaque, [*c]fts5_tokenizer, ?*const fn (?*anyopaque) callconv(.C) void) callconv(.C) c_int),
    xFindTokenizer: ?*const fn ([*c]fts5_api, [*c]const u8, [*c]?*anyopaque, [*c]fts5_tokenizer) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn ([*c]fts5_api, [*c]const u8, [*c]?*anyopaque, [*c]fts5_tokenizer) callconv(.C) c_int),
    xCreateFunction: ?*const fn ([*c]fts5_api, [*c]const u8, ?*anyopaque, fts5_extension_function, ?*const fn (?*anyopaque) callconv(.C) void) callconv(.C) c_int = @import("std").mem.zeroes(?*const fn ([*c]fts5_api, [*c]const u8, ?*anyopaque, fts5_extension_function, ?*const fn (?*anyopaque) callconv(.C) void) callconv(.C) c_int),
};
pub const __u_char = u8;
pub const __u_short = c_ushort;
pub const __u_int = c_uint;
pub const __u_long = c_ulong;
pub const __int8_t = i8;
pub const __uint8_t = u8;
pub const __int16_t = c_short;
pub const __uint16_t = c_ushort;
pub const __int32_t = c_int;
pub const __uint32_t = c_uint;
pub const __int64_t = c_long;
pub const __uint64_t = c_ulong;
pub const __int_least8_t = __int8_t;
pub const __uint_least8_t = __uint8_t;
pub const __int_least16_t = __int16_t;
pub const __uint_least16_t = __uint16_t;
pub const __int_least32_t = __int32_t;
pub const __uint_least32_t = __uint32_t;
pub const __int_least64_t = __int64_t;
pub const __uint_least64_t = __uint64_t;
pub const __quad_t = c_long;
pub const __u_quad_t = c_ulong;
pub const __intmax_t = c_long;
pub const __uintmax_t = c_ulong;
pub const __dev_t = c_ulong;
pub const __uid_t = c_uint;
pub const __gid_t = c_uint;
pub const __ino_t = c_ulong;
pub const __ino64_t = c_ulong;
pub const __mode_t = c_uint;
pub const __nlink_t = c_uint;
pub const __off_t = c_long;
pub const __off64_t = c_long;
pub const __pid_t = c_int;
pub const __fsid_t = extern struct {
    __val: [2]c_int = @import("std").mem.zeroes([2]c_int),
};
pub const __clock_t = c_long;
pub const __rlim_t = c_ulong;
pub const __rlim64_t = c_ulong;
pub const __id_t = c_uint;
pub const __time_t = c_long;
pub const __useconds_t = c_uint;
pub const __suseconds_t = c_long;
pub const __suseconds64_t = c_long;
pub const __daddr_t = c_int;
pub const __key_t = c_int;
pub const __clockid_t = c_int;
pub const __timer_t = ?*anyopaque;
pub const __blksize_t = c_int;
pub const __blkcnt_t = c_long;
pub const __blkcnt64_t = c_long;
pub const __fsblkcnt_t = c_ulong;
pub const __fsblkcnt64_t = c_ulong;
pub const __fsfilcnt_t = c_ulong;
pub const __fsfilcnt64_t = c_ulong;
pub const __fsword_t = c_long;
pub const __ssize_t = c_long;
pub const __syscall_slong_t = c_long;
pub const __syscall_ulong_t = c_ulong;
pub const __loff_t = __off64_t;
pub const __caddr_t = [*c]u8;
pub const __intptr_t = c_long;
pub const __socklen_t = c_uint;
pub const __sig_atomic_t = c_int;
pub const int_least8_t = __int_least8_t;
pub const int_least16_t = __int_least16_t;
pub const int_least32_t = __int_least32_t;
pub const int_least64_t = __int_least64_t;
pub const uint_least8_t = __uint_least8_t;
pub const uint_least16_t = __uint_least16_t;
pub const uint_least32_t = __uint_least32_t;
pub const uint_least64_t = __uint_least64_t;
pub const int_fast8_t = i8;
pub const int_fast16_t = c_long;
pub const int_fast32_t = c_long;
pub const int_fast64_t = c_long;
pub const uint_fast8_t = u8;
pub const uint_fast16_t = c_ulong;
pub const uint_fast32_t = c_ulong;
pub const uint_fast64_t = c_ulong;
pub const intmax_t = __intmax_t;
pub const uintmax_t = __uintmax_t;
pub const ptrdiff_t = c_long;
pub const wchar_t = c_uint;
pub const max_align_t = extern struct {
    __clang_max_align_nonce1: c_longlong align(8) = @import("std").mem.zeroes(c_longlong),
    __clang_max_align_nonce2: c_longdouble align(16) = @import("std").mem.zeroes(c_longdouble),
};
pub const KB_OK: c_int = 0;
pub const KB_ERR_NULL_ARG: c_int = -1;
pub const KB_ERR_SQLITE: c_int = -2;
pub const KB_ERR_NOT_FOUND: c_int = -3;
pub const KB_ERR_JSON: c_int = -4;
pub const KB_ERR_NOMEM: c_int = -5;
pub const KB_ERR_BUSY: c_int = -6;
pub const KB_ERR_INVALID: c_int = -7;
pub const KB_ERR_OVERFLOW: c_int = -8;
pub const KB_ERR_STATE: c_int = -9;
pub const KB_ERR_EXTENSION: c_int = -10;
pub const kb_error_t = c_int;
pub extern fn kb_error_str(err: kb_error_t) [*c]const u8;
pub const kb_row_t = extern struct {
    col_count: c_int = @import("std").mem.zeroes(c_int),
    col_names: [32][*c]u8 = @import("std").mem.zeroes([32][*c]u8),
    col_values: [32][*c]u8 = @import("std").mem.zeroes([32][*c]u8),
};
pub const kb_result_t = extern struct {
    rows: [*c]kb_row_t = @import("std").mem.zeroes([*c]kb_row_t),
    count: c_int = @import("std").mem.zeroes(c_int),
    capacity: c_int = @import("std").mem.zeroes(c_int),
    changes: c_int = @import("std").mem.zeroes(c_int),
};
pub extern fn kb_result_init(result: [*c]kb_result_t) void;
pub extern fn kb_result_free(result: [*c]kb_result_t) void;
pub const KB_BIND_NULL: c_int = 0;
pub const KB_BIND_TEXT: c_int = 1;
pub const KB_BIND_INT: c_int = 2;
pub const KB_BIND_INT64: c_int = 3;
pub const KB_BIND_DOUBLE: c_int = 4;
pub const kb_bind_type_t = c_uint;
const union_unnamed_5 = extern union {
    text: [*c]const u8,
    i: c_int,
    i64: i64,
    d: f64,
};
pub const kb_bind_param_t = extern struct {
    type: kb_bind_type_t = @import("std").mem.zeroes(kb_bind_type_t),
    val: union_unnamed_5 = @import("std").mem.zeroes(union_unnamed_5),
};
pub extern fn kb_sql_exec(db: ?*sqlite3, sql: [*c]const u8, err_msg: [*c][*c]u8) kb_error_t;
pub extern fn kb_query_exec(db: ?*sqlite3, sql: [*c]const u8, params: [*c]const kb_bind_param_t, n_params: c_int, result: [*c]kb_result_t) kb_error_t;
pub extern fn kb_row_get(result: [*c]const kb_result_t, row_idx: c_int, col_name: [*c]const u8) [*c]const u8;
pub extern fn kb_row_get_int(result: [*c]const kb_result_t, row_idx: c_int, col_name: [*c]const u8, dflt: c_int) c_int;
pub extern fn kb_row_get_int64(result: [*c]const kb_result_t, row_idx: c_int, col_name: [*c]const u8, dflt: i64) i64;
pub extern fn kb_row_get_double(result: [*c]const kb_result_t, row_idx: c_int, col_name: [*c]const u8, dflt: f64) f64;
pub extern fn kb_begin_immediate(db: ?*sqlite3, max_retries: c_int, retry_delay_ms: c_int) kb_error_t;
pub extern fn kb_commit(db: ?*sqlite3) kb_error_t;
pub extern fn kb_rollback(db: ?*sqlite3) kb_error_t;
pub extern fn kb_timestamp_now(buf: [*c]u8, buf_size: usize) void;
pub extern fn kb_open_database(db_path: [*c]const u8, ltree_path: [*c]const u8, db_out: [*c]?*sqlite3) kb_error_t;
pub extern fn kb_close_database(db: ?*sqlite3) void;
pub extern fn kb_strdup(s: [*c]const u8) [*c]u8;
pub extern fn kb_sprintf(fmt: [*c]const u8, ...) [*c]u8;
pub const KB_FILTER_KB: c_int = 0;
pub const KB_FILTER_LABEL: c_int = 1;
pub const KB_FILTER_NAME: c_int = 2;
pub const KB_FILTER_PROPERTY_KEY: c_int = 3;
pub const KB_FILTER_PROPERTY_VALUE: c_int = 4;
pub const KB_FILTER_HAS_LINK: c_int = 5;
pub const KB_FILTER_HAS_LINK_MOUNT: c_int = 6;
pub const KB_FILTER_PATH: c_int = 7;
pub const KB_FILTER_STARTING_PATH: c_int = 8;
pub const kb_filter_type_t = c_uint;
pub const kb_filter_t = extern struct {
    type: kb_filter_type_t = @import("std").mem.zeroes(kb_filter_type_t),
    param1: [*c]u8 = @import("std").mem.zeroes([*c]u8),
    param2: [*c]u8 = @import("std").mem.zeroes([*c]u8),
};
pub const struct_kb_search = opaque {};
pub const kb_search_t = struct_kb_search;
pub extern fn kb_search_create(db_path: [*c]const u8, database: [*c]const u8, ltree_path: [*c]const u8) ?*kb_search_t;
pub extern fn kb_search_create_from_db(db: ?*sqlite3, database: [*c]const u8) ?*kb_search_t;
pub extern fn kb_search_destroy(ks: ?*kb_search_t) void;
pub extern fn kb_search_clear_filters(ks: ?*kb_search_t) void;
pub extern fn kb_search_kb(ks: ?*kb_search_t, kb_name: [*c]const u8) kb_error_t;
pub extern fn kb_search_label(ks: ?*kb_search_t, label: [*c]const u8) kb_error_t;
pub extern fn kb_search_name(ks: ?*kb_search_t, name: [*c]const u8) kb_error_t;
pub extern fn kb_search_property_key(ks: ?*kb_search_t, key: [*c]const u8) kb_error_t;
pub extern fn kb_search_property_value(ks: ?*kb_search_t, key: [*c]const u8, value: [*c]const u8) kb_error_t;
pub extern fn kb_search_has_link(ks: ?*kb_search_t) kb_error_t;
pub extern fn kb_search_has_link_mount(ks: ?*kb_search_t) kb_error_t;
pub extern fn kb_search_path(ks: ?*kb_search_t, path_expr: [*c]const u8) kb_error_t;
pub extern fn kb_search_starting_path(ks: ?*kb_search_t, starting_path: [*c]const u8) kb_error_t;
pub extern fn kb_search_execute(ks: ?*kb_search_t) kb_error_t;
pub extern fn kb_search_results(ks: ?*const kb_search_t) [*c]const kb_result_t;
pub extern fn kb_search_get_db(ks: ?*const kb_search_t) ?*sqlite3;
pub extern fn kb_search_get_database(ks: ?*const kb_search_t) [*c]const u8;
pub const kb_description_t = extern struct {
    path: [*c]u8 = @import("std").mem.zeroes([*c]u8),
    description: [*c]u8 = @import("std").mem.zeroes([*c]u8),
};
pub extern fn kb_search_find_description(result: [*c]const kb_result_t, out: [*c][*c]kb_description_t, out_count: [*c]c_int) kb_error_t;
pub extern fn kb_description_free(descs: [*c]kb_description_t, count: c_int) void;
pub extern fn kb_search_find_description_paths(ks: ?*kb_search_t, paths: [*c][*c]const u8, n_paths: c_int, result: [*c]kb_result_t) kb_error_t;
pub extern fn kb_search_find_path_values(result: [*c]const kb_result_t, out_paths: [*c][*c][*c]u8, out_count: [*c]c_int) kb_error_t;
pub extern fn kb_path_values_free(paths: [*c][*c]u8, count: c_int) void;
pub const kb_link_pair_t = extern struct {
    link: [*c]u8 = @import("std").mem.zeroes([*c]u8),
    name: [*c]u8 = @import("std").mem.zeroes([*c]u8),
};
pub extern fn kb_search_decode_link_nodes(path: [*c]const u8, kb_name_out: [*c][*c]u8, pairs_out: [*c][*c]kb_link_pair_t, pair_count_out: [*c]c_int) kb_error_t;
pub extern fn kb_link_pairs_free(pairs: [*c]kb_link_pair_t, count: c_int) void;
pub const struct_kb_bit_mask_ops = opaque {};
pub const kb_bit_mask_ops_t = struct_kb_bit_mask_ops;
pub extern fn kb_bit_mask_ops_create(db: ?*sqlite3, database: [*c]const u8) ?*kb_bit_mask_ops_t;
pub extern fn kb_bit_mask_ops_destroy(ops: ?*kb_bit_mask_ops_t) void;
pub extern fn kb_bit_get(ops: ?*kb_bit_mask_ops_t, path: [*c]const u8, bit_position: c_int, value_out: [*c]c_int) kb_error_t;
pub extern fn kb_bit_set(ops: ?*kb_bit_mask_ops_t, path: [*c]const u8, bit_position: c_int, value: c_int) kb_error_t;
pub extern fn kb_bit_get_mask(ops: ?*kb_bit_mask_ops_t, path: [*c]const u8, mask_out: [*c]i64) kb_error_t;
pub extern fn kb_bit_set_mask(ops: ?*kb_bit_mask_ops_t, path: [*c]const u8, mask: i64) kb_error_t;
pub extern fn kb_bit_get_change_mask(ops: ?*kb_bit_mask_ops_t, path: [*c]const u8, mask_out: [*c]i64) kb_error_t;
pub extern fn kb_bit_clear_change_mask(ops: ?*kb_bit_mask_ops_t, path: [*c]const u8) kb_error_t;
pub const kb_bit_data_t = extern struct {
    bit_mask: i64 = @import("std").mem.zeroes(i64),
    change_mask: i64 = @import("std").mem.zeroes(i64),
};
pub extern fn kb_sexpr_eval(expr: [*c]const u8, bit_data: [*c]const kb_bit_data_t, result: [*c]c_int) kb_error_t;
pub const struct_kb_bit_structures = opaque {};
pub const kb_bit_structures_t = struct_kb_bit_structures;
pub extern fn kb_bit_structures_create(ks: ?*kb_search_t, database: [*c]const u8) ?*kb_bit_structures_t;
pub extern fn kb_bit_structures_destroy(bs: ?*kb_bit_structures_t) void;
pub extern fn kb_bit_find_node_id(bs: ?*kb_bit_structures_t, node_name: [*c]const u8, properties_json: [*c]const u8, node_path: [*c]const u8, node_id_out: [*c]c_int) kb_error_t;
pub extern fn kb_bit_get_by_path(bs: ?*kb_bit_structures_t, path: [*c]const u8, bit_position: c_int, value_out: [*c]c_int) kb_error_t;
pub extern fn kb_bit_set_by_path(bs: ?*kb_bit_structures_t, path: [*c]const u8, bit_position: c_int, value: c_int) kb_error_t;
pub extern fn kb_bit_get_mask_by_path(bs: ?*kb_bit_structures_t, path: [*c]const u8, mask_out: [*c]i64) kb_error_t;
pub extern fn kb_bit_set_mask_by_path(bs: ?*kb_bit_structures_t, path: [*c]const u8, mask: i64) kb_error_t;
pub extern fn kb_bit_eval_sexpr(bs: ?*kb_bit_structures_t, path: [*c]const u8, expr: [*c]const u8, result_out: [*c]c_int) kb_error_t;
pub extern fn kb_bit_structures_get_ops(bs: ?*kb_bit_structures_t) ?*kb_bit_mask_ops_t;
pub const struct_kb_status_table = opaque {};
pub const kb_status_table_t = struct_kb_status_table;
pub extern fn kb_status_table_create(ks: ?*kb_search_t, database: [*c]const u8) ?*kb_status_table_t;
pub extern fn kb_status_table_destroy(st: ?*kb_status_table_t) void;
pub extern fn kb_status_find_node_id(st: ?*kb_status_table_t, node_name: [*c]const u8, node_path: [*c]const u8, node_id_out: [*c]c_int) kb_error_t;
pub extern fn kb_status_get_data(st: ?*kb_status_table_t, path: [*c]const u8, data_out: [*c][*c]u8) kb_error_t;
pub extern fn kb_status_set_data(st: ?*kb_status_table_t, path: [*c]const u8, data_json: [*c]const u8) kb_error_t;
pub const struct_kb_stream = opaque {};
pub const kb_stream_t = struct_kb_stream;
pub extern fn kb_stream_create(ks: ?*kb_search_t, database: [*c]const u8) ?*kb_stream_t;
pub extern fn kb_stream_destroy(st: ?*kb_stream_t) void;
pub extern fn kb_stream_push_data(st: ?*kb_stream_t, path: [*c]const u8, data_json: [*c]const u8) kb_error_t;
pub extern fn kb_stream_list_data(st: ?*kb_stream_t, path: [*c]const u8, recorded_after: [*c]const u8, recorded_before: [*c]const u8, result: [*c]kb_result_t) kb_error_t;
pub extern fn kb_stream_clear_data(st: ?*kb_stream_t, path: [*c]const u8) kb_error_t;
pub extern fn kb_stream_get_write_index(st: ?*kb_stream_t, path: [*c]const u8, index_out: [*c]c_int) kb_error_t;
pub const struct_kb_job_queue = opaque {};
pub const kb_job_queue_t = struct_kb_job_queue;
pub extern fn kb_job_queue_create(ks: ?*kb_search_t, database: [*c]const u8) ?*kb_job_queue_t;
pub extern fn kb_job_queue_destroy(jq: ?*kb_job_queue_t) void;
pub extern fn kb_job_find_node_id(jq: ?*kb_job_queue_t, node_name: [*c]const u8, node_path: [*c]const u8, node_id_out: [*c]c_int) kb_error_t;
pub extern fn kb_job_get_queued_number(jq: ?*kb_job_queue_t, path: [*c]const u8, count_out: [*c]c_int) kb_error_t;
pub extern fn kb_job_get_free_number(jq: ?*kb_job_queue_t, path: [*c]const u8, count_out: [*c]c_int) kb_error_t;
pub extern fn kb_job_push(jq: ?*kb_job_queue_t, path: [*c]const u8, data_json: [*c]const u8, priority: c_int) kb_error_t;
pub extern fn kb_job_peek(jq: ?*kb_job_queue_t, path: [*c]const u8, data_out: [*c][*c]u8, record_id_out: [*c]c_int) kb_error_t;
pub extern fn kb_job_complete(jq: ?*kb_job_queue_t, path: [*c]const u8, record_id: c_int) kb_error_t;
pub extern fn kb_job_clear(jq: ?*kb_job_queue_t, path: [*c]const u8) kb_error_t;
pub const struct_kb_link_table = opaque {};
pub const kb_link_table_t = struct_kb_link_table;
pub extern fn kb_link_table_create(db: ?*sqlite3, database: [*c]const u8) ?*kb_link_table_t;
pub extern fn kb_link_table_destroy(lt: ?*kb_link_table_t) void;
pub extern fn kb_link_get_by_link_name(lt: ?*kb_link_table_t, link_name: [*c]const u8, result: [*c]kb_result_t) kb_error_t;
pub extern fn kb_link_get_by_node_path(lt: ?*kb_link_table_t, node_path: [*c]const u8, result: [*c]kb_result_t) kb_error_t;
pub const struct_kb_link_mount_table = opaque {};
pub const kb_link_mount_table_t = struct_kb_link_mount_table;
pub extern fn kb_link_mount_table_create(db: ?*sqlite3, database: [*c]const u8) ?*kb_link_mount_table_t;
pub extern fn kb_link_mount_table_destroy(lmt: ?*kb_link_mount_table_t) void;
pub extern fn kb_link_mount_get_by_link_name(lmt: ?*kb_link_mount_table_t, link_name: [*c]const u8, result: [*c]kb_result_t) kb_error_t;
pub extern fn kb_link_mount_get_by_mount_path(lmt: ?*kb_link_mount_table_t, mount_path: [*c]const u8, result: [*c]kb_result_t) kb_error_t;
pub const struct_kb_rpc_server = opaque {};
pub const kb_rpc_server_t = struct_kb_rpc_server;
pub extern fn kb_rpc_server_create(ks: ?*kb_search_t, database: [*c]const u8) ?*kb_rpc_server_t;
pub extern fn kb_rpc_server_destroy(rs: ?*kb_rpc_server_t) void;
pub extern fn kb_rpc_server_push(rs: ?*kb_rpc_server_t, path: [*c]const u8, rpc_action: [*c]const u8, data_json: [*c]const u8, priority: c_int, rpc_client_queue: [*c]const u8, uuid_out: [*c]u8, uuid_size: usize) kb_error_t;
pub extern fn kb_rpc_server_peek(rs: ?*kb_rpc_server_t, path: [*c]const u8, data_out: [*c][*c]u8, uuid_out: [*c][*c]u8, action_out: [*c][*c]u8, record_id_out: [*c]c_int) kb_error_t;
pub extern fn kb_rpc_server_claim(rs: ?*kb_rpc_server_t, path: [*c]const u8, record_id: c_int) kb_error_t;
pub extern fn kb_rpc_server_complete(rs: ?*kb_rpc_server_t, path: [*c]const u8, record_id: c_int) kb_error_t;
pub extern fn kb_rpc_server_get_state_counts(rs: ?*kb_rpc_server_t, path: [*c]const u8, empty_out: [*c]c_int, new_job_out: [*c]c_int, processing_out: [*c]c_int) kb_error_t;
pub const struct_kb_rpc_client = opaque {};
pub const kb_rpc_client_t = struct_kb_rpc_client;
pub extern fn kb_rpc_client_create(ks: ?*kb_search_t, database: [*c]const u8) ?*kb_rpc_client_t;
pub extern fn kb_rpc_client_destroy(rc: ?*kb_rpc_client_t) void;
pub extern fn kb_rpc_client_push_and_claim(rc: ?*kb_rpc_client_t, client_path: [*c]const u8, request_uuid: [*c]const u8, server_path: [*c]const u8, rpc_action: [*c]const u8, transaction_tag: [*c]const u8, reply_data_json: [*c]const u8) kb_error_t;
pub extern fn kb_rpc_client_peek_reply(rc: ?*kb_rpc_client_t, client_path: [*c]const u8, reply_data_out: [*c][*c]u8, uuid_out: [*c][*c]u8, action_out: [*c][*c]u8, record_id_out: [*c]c_int) kb_error_t;
pub extern fn kb_rpc_client_clear_reply(rc: ?*kb_rpc_client_t, client_path: [*c]const u8, record_id: c_int) kb_error_t;
pub extern fn kb_rpc_client_get_state_counts(rc: ?*kb_rpc_client_t, client_path: [*c]const u8, free_out: [*c]c_int, queued_out: [*c]c_int) kb_error_t;
pub const struct_kb_data_structures = opaque {};
pub const kb_ds_t = struct_kb_data_structures;
pub extern fn kb_ds_create(db_path: [*c]const u8, database: [*c]const u8, ltree_path: [*c]const u8) ?*kb_ds_t;
pub extern fn kb_ds_create_from_db(db: ?*sqlite3, database: [*c]const u8) ?*kb_ds_t;
pub extern fn kb_ds_destroy(ds: ?*kb_ds_t) void;
pub extern fn kb_ds_search(ds: ?*kb_ds_t) ?*kb_search_t;
pub extern fn kb_ds_bit_structures(ds: ?*kb_ds_t) ?*kb_bit_structures_t;
pub extern fn kb_ds_status(ds: ?*kb_ds_t) ?*kb_status_table_t;
pub extern fn kb_ds_stream(ds: ?*kb_ds_t) ?*kb_stream_t;
pub extern fn kb_ds_job_queue(ds: ?*kb_ds_t) ?*kb_job_queue_t;
pub extern fn kb_ds_link_table(ds: ?*kb_ds_t) ?*kb_link_table_t;
pub extern fn kb_ds_link_mount_table(ds: ?*kb_ds_t) ?*kb_link_mount_table_t;
pub extern fn kb_ds_rpc_server(ds: ?*kb_ds_t) ?*kb_rpc_server_t;
pub extern fn kb_ds_rpc_client(ds: ?*kb_ds_t) ?*kb_rpc_client_t;
pub extern fn kb_ds_get_db(ds: ?*kb_ds_t) ?*sqlite3;
pub extern fn kb_uuid4(buf: [*c]u8, buf_size: usize) void;
pub extern fn kb_uuid_seed() void;
pub const struct_cJSON = extern struct {
    next: [*c]struct_cJSON = @import("std").mem.zeroes([*c]struct_cJSON),
    prev: [*c]struct_cJSON = @import("std").mem.zeroes([*c]struct_cJSON),
    child: [*c]struct_cJSON = @import("std").mem.zeroes([*c]struct_cJSON),
    type: c_int = @import("std").mem.zeroes(c_int),
    valuestring: [*c]u8 = @import("std").mem.zeroes([*c]u8),
    valueint: c_int = @import("std").mem.zeroes(c_int),
    valuedouble: f64 = @import("std").mem.zeroes(f64),
    string: [*c]u8 = @import("std").mem.zeroes([*c]u8),
};
pub const cJSON = struct_cJSON;
pub const struct_cJSON_Hooks = extern struct {
    malloc_fn: ?*const fn (usize) callconv(.C) ?*anyopaque = @import("std").mem.zeroes(?*const fn (usize) callconv(.C) ?*anyopaque),
    free_fn: ?*const fn (?*anyopaque) callconv(.C) void = @import("std").mem.zeroes(?*const fn (?*anyopaque) callconv(.C) void),
};
pub const cJSON_Hooks = struct_cJSON_Hooks;
pub const cJSON_bool = c_int;
pub extern fn cJSON_Version() [*c]const u8;
pub extern fn cJSON_InitHooks(hooks: [*c]cJSON_Hooks) void;
pub extern fn cJSON_Parse(value: [*c]const u8) [*c]cJSON;
pub extern fn cJSON_ParseWithLength(value: [*c]const u8, buffer_length: usize) [*c]cJSON;
pub extern fn cJSON_ParseWithOpts(value: [*c]const u8, return_parse_end: [*c][*c]const u8, require_null_terminated: cJSON_bool) [*c]cJSON;
pub extern fn cJSON_ParseWithLengthOpts(value: [*c]const u8, buffer_length: usize, return_parse_end: [*c][*c]const u8, require_null_terminated: cJSON_bool) [*c]cJSON;
pub extern fn cJSON_Print(item: [*c]const cJSON) [*c]u8;
pub extern fn cJSON_PrintUnformatted(item: [*c]const cJSON) [*c]u8;
pub extern fn cJSON_PrintBuffered(item: [*c]const cJSON, prebuffer: c_int, fmt: cJSON_bool) [*c]u8;
pub extern fn cJSON_PrintPreallocated(item: [*c]cJSON, buffer: [*c]u8, length: c_int, format: cJSON_bool) cJSON_bool;
pub extern fn cJSON_Delete(item: [*c]cJSON) void;
pub extern fn cJSON_GetArraySize(array: [*c]const cJSON) c_int;
pub extern fn cJSON_GetArrayItem(array: [*c]const cJSON, index: c_int) [*c]cJSON;
pub extern fn cJSON_GetObjectItem(object: [*c]const cJSON, string: [*c]const u8) [*c]cJSON;
pub extern fn cJSON_GetObjectItemCaseSensitive(object: [*c]const cJSON, string: [*c]const u8) [*c]cJSON;
pub extern fn cJSON_HasObjectItem(object: [*c]const cJSON, string: [*c]const u8) cJSON_bool;
pub extern fn cJSON_GetErrorPtr() [*c]const u8;
pub extern fn cJSON_GetStringValue(item: [*c]const cJSON) [*c]u8;
pub extern fn cJSON_GetNumberValue(item: [*c]const cJSON) f64;
pub extern fn cJSON_IsInvalid(item: [*c]const cJSON) cJSON_bool;
pub extern fn cJSON_IsFalse(item: [*c]const cJSON) cJSON_bool;
pub extern fn cJSON_IsTrue(item: [*c]const cJSON) cJSON_bool;
pub extern fn cJSON_IsBool(item: [*c]const cJSON) cJSON_bool;
pub extern fn cJSON_IsNull(item: [*c]const cJSON) cJSON_bool;
pub extern fn cJSON_IsNumber(item: [*c]const cJSON) cJSON_bool;
pub extern fn cJSON_IsString(item: [*c]const cJSON) cJSON_bool;
pub extern fn cJSON_IsArray(item: [*c]const cJSON) cJSON_bool;
pub extern fn cJSON_IsObject(item: [*c]const cJSON) cJSON_bool;
pub extern fn cJSON_IsRaw(item: [*c]const cJSON) cJSON_bool;
pub extern fn cJSON_CreateNull() [*c]cJSON;
pub extern fn cJSON_CreateTrue() [*c]cJSON;
pub extern fn cJSON_CreateFalse() [*c]cJSON;
pub extern fn cJSON_CreateBool(boolean: cJSON_bool) [*c]cJSON;
pub extern fn cJSON_CreateNumber(num: f64) [*c]cJSON;
pub extern fn cJSON_CreateString(string: [*c]const u8) [*c]cJSON;
pub extern fn cJSON_CreateRaw(raw: [*c]const u8) [*c]cJSON;
pub extern fn cJSON_CreateArray() [*c]cJSON;
pub extern fn cJSON_CreateObject() [*c]cJSON;
pub extern fn cJSON_CreateStringReference(string: [*c]const u8) [*c]cJSON;
pub extern fn cJSON_CreateObjectReference(child: [*c]const cJSON) [*c]cJSON;
pub extern fn cJSON_CreateArrayReference(child: [*c]const cJSON) [*c]cJSON;
pub extern fn cJSON_CreateIntArray(numbers: [*c]const c_int, count: c_int) [*c]cJSON;
pub extern fn cJSON_CreateFloatArray(numbers: [*c]const f32, count: c_int) [*c]cJSON;
pub extern fn cJSON_CreateDoubleArray(numbers: [*c]const f64, count: c_int) [*c]cJSON;
pub extern fn cJSON_CreateStringArray(strings: [*c]const [*c]const u8, count: c_int) [*c]cJSON;
pub extern fn cJSON_AddItemToArray(array: [*c]cJSON, item: [*c]cJSON) cJSON_bool;
pub extern fn cJSON_AddItemToObject(object: [*c]cJSON, string: [*c]const u8, item: [*c]cJSON) cJSON_bool;
pub extern fn cJSON_AddItemToObjectCS(object: [*c]cJSON, string: [*c]const u8, item: [*c]cJSON) cJSON_bool;
pub extern fn cJSON_AddItemReferenceToArray(array: [*c]cJSON, item: [*c]cJSON) cJSON_bool;
pub extern fn cJSON_AddItemReferenceToObject(object: [*c]cJSON, string: [*c]const u8, item: [*c]cJSON) cJSON_bool;
pub extern fn cJSON_DetachItemViaPointer(parent: [*c]cJSON, item: [*c]cJSON) [*c]cJSON;
pub extern fn cJSON_DetachItemFromArray(array: [*c]cJSON, which: c_int) [*c]cJSON;
pub extern fn cJSON_DeleteItemFromArray(array: [*c]cJSON, which: c_int) void;
pub extern fn cJSON_DetachItemFromObject(object: [*c]cJSON, string: [*c]const u8) [*c]cJSON;
pub extern fn cJSON_DetachItemFromObjectCaseSensitive(object: [*c]cJSON, string: [*c]const u8) [*c]cJSON;
pub extern fn cJSON_DeleteItemFromObject(object: [*c]cJSON, string: [*c]const u8) void;
pub extern fn cJSON_DeleteItemFromObjectCaseSensitive(object: [*c]cJSON, string: [*c]const u8) void;
pub extern fn cJSON_InsertItemInArray(array: [*c]cJSON, which: c_int, newitem: [*c]cJSON) cJSON_bool;
pub extern fn cJSON_ReplaceItemViaPointer(parent: [*c]cJSON, item: [*c]cJSON, replacement: [*c]cJSON) cJSON_bool;
pub extern fn cJSON_ReplaceItemInArray(array: [*c]cJSON, which: c_int, newitem: [*c]cJSON) cJSON_bool;
pub extern fn cJSON_ReplaceItemInObject(object: [*c]cJSON, string: [*c]const u8, newitem: [*c]cJSON) cJSON_bool;
pub extern fn cJSON_ReplaceItemInObjectCaseSensitive(object: [*c]cJSON, string: [*c]const u8, newitem: [*c]cJSON) cJSON_bool;
pub extern fn cJSON_Duplicate(item: [*c]const cJSON, recurse: cJSON_bool) [*c]cJSON;
pub extern fn cJSON_Compare(a: [*c]const cJSON, b: [*c]const cJSON, case_sensitive: cJSON_bool) cJSON_bool;
pub extern fn cJSON_Minify(json: [*c]u8) void;
pub extern fn cJSON_AddNullToObject(object: [*c]cJSON, name: [*c]const u8) [*c]cJSON;
pub extern fn cJSON_AddTrueToObject(object: [*c]cJSON, name: [*c]const u8) [*c]cJSON;
pub extern fn cJSON_AddFalseToObject(object: [*c]cJSON, name: [*c]const u8) [*c]cJSON;
pub extern fn cJSON_AddBoolToObject(object: [*c]cJSON, name: [*c]const u8, boolean: cJSON_bool) [*c]cJSON;
pub extern fn cJSON_AddNumberToObject(object: [*c]cJSON, name: [*c]const u8, number: f64) [*c]cJSON;
pub extern fn cJSON_AddStringToObject(object: [*c]cJSON, name: [*c]const u8, string: [*c]const u8) [*c]cJSON;
pub extern fn cJSON_AddRawToObject(object: [*c]cJSON, name: [*c]const u8, raw: [*c]const u8) [*c]cJSON;
pub extern fn cJSON_AddObjectToObject(object: [*c]cJSON, name: [*c]const u8) [*c]cJSON;
pub extern fn cJSON_AddArrayToObject(object: [*c]cJSON, name: [*c]const u8) [*c]cJSON;
pub extern fn cJSON_SetNumberHelper(object: [*c]cJSON, number: f64) f64;
pub extern fn cJSON_SetValuestring(object: [*c]cJSON, valuestring: [*c]const u8) [*c]u8;
pub extern fn cJSON_malloc(size: usize) ?*anyopaque;
pub extern fn cJSON_free(object: ?*anyopaque) void;
pub extern fn kb_json_encode(json: [*c]const cJSON) [*c]u8;
pub extern fn kb_json_decode(json_str: [*c]const u8) [*c]cJSON;
pub extern fn kb_json_has_key_value(json_str: [*c]const u8, key: [*c]const u8, value: [*c]const u8) bool;
pub extern fn kb_json_has_key(json_str: [*c]const u8, key: [*c]const u8) bool;
pub const __llvm__ = @as(c_int, 1);
pub const __clang__ = @as(c_int, 1);
pub const __clang_major__ = @as(c_int, 18);
pub const __clang_minor__ = @as(c_int, 1);
pub const __clang_patchlevel__ = @as(c_int, 6);
pub const __clang_version__ = "18.1.6 (https://github.com/ziglang/zig-bootstrap 98bc6bf4fc4009888d33941daf6b600d20a42a56)";
pub const __GNUC__ = @as(c_int, 4);
pub const __GNUC_MINOR__ = @as(c_int, 2);
pub const __GNUC_PATCHLEVEL__ = @as(c_int, 1);
pub const __GXX_ABI_VERSION = @as(c_int, 1002);
pub const __ATOMIC_RELAXED = @as(c_int, 0);
pub const __ATOMIC_CONSUME = @as(c_int, 1);
pub const __ATOMIC_ACQUIRE = @as(c_int, 2);
pub const __ATOMIC_RELEASE = @as(c_int, 3);
pub const __ATOMIC_ACQ_REL = @as(c_int, 4);
pub const __ATOMIC_SEQ_CST = @as(c_int, 5);
pub const __MEMORY_SCOPE_SYSTEM = @as(c_int, 0);
pub const __MEMORY_SCOPE_DEVICE = @as(c_int, 1);
pub const __MEMORY_SCOPE_WRKGRP = @as(c_int, 2);
pub const __MEMORY_SCOPE_WVFRNT = @as(c_int, 3);
pub const __MEMORY_SCOPE_SINGLE = @as(c_int, 4);
pub const __OPENCL_MEMORY_SCOPE_WORK_ITEM = @as(c_int, 0);
pub const __OPENCL_MEMORY_SCOPE_WORK_GROUP = @as(c_int, 1);
pub const __OPENCL_MEMORY_SCOPE_DEVICE = @as(c_int, 2);
pub const __OPENCL_MEMORY_SCOPE_ALL_SVM_DEVICES = @as(c_int, 3);
pub const __OPENCL_MEMORY_SCOPE_SUB_GROUP = @as(c_int, 4);
pub const __FPCLASS_SNAN = @as(c_int, 0x0001);
pub const __FPCLASS_QNAN = @as(c_int, 0x0002);
pub const __FPCLASS_NEGINF = @as(c_int, 0x0004);
pub const __FPCLASS_NEGNORMAL = @as(c_int, 0x0008);
pub const __FPCLASS_NEGSUBNORMAL = @as(c_int, 0x0010);
pub const __FPCLASS_NEGZERO = @as(c_int, 0x0020);
pub const __FPCLASS_POSZERO = @as(c_int, 0x0040);
pub const __FPCLASS_POSSUBNORMAL = @as(c_int, 0x0080);
pub const __FPCLASS_POSNORMAL = @as(c_int, 0x0100);
pub const __FPCLASS_POSINF = @as(c_int, 0x0200);
pub const __PRAGMA_REDEFINE_EXTNAME = @as(c_int, 1);
pub const __VERSION__ = "Clang 18.1.6 (https://github.com/ziglang/zig-bootstrap 98bc6bf4fc4009888d33941daf6b600d20a42a56)";
pub const __OBJC_BOOL_IS_BOOL = @as(c_int, 0);
pub const __CONSTANT_CFSTRINGS__ = @as(c_int, 1);
pub const __clang_literal_encoding__ = "UTF-8";
pub const __clang_wide_literal_encoding__ = "UTF-32";
pub const __ORDER_LITTLE_ENDIAN__ = @as(c_int, 1234);
pub const __ORDER_BIG_ENDIAN__ = @as(c_int, 4321);
pub const __ORDER_PDP_ENDIAN__ = @as(c_int, 3412);
pub const __BYTE_ORDER__ = __ORDER_LITTLE_ENDIAN__;
pub const __LITTLE_ENDIAN__ = @as(c_int, 1);
pub const _LP64 = @as(c_int, 1);
pub const __LP64__ = @as(c_int, 1);
pub const __CHAR_BIT__ = @as(c_int, 8);
pub const __BOOL_WIDTH__ = @as(c_int, 8);
pub const __SHRT_WIDTH__ = @as(c_int, 16);
pub const __INT_WIDTH__ = @as(c_int, 32);
pub const __LONG_WIDTH__ = @as(c_int, 64);
pub const __LLONG_WIDTH__ = @as(c_int, 64);
pub const __BITINT_MAXWIDTH__ = @as(c_int, 128);
pub const __SCHAR_MAX__ = @as(c_int, 127);
pub const __SHRT_MAX__ = @as(c_int, 32767);
pub const __INT_MAX__ = @import("std").zig.c_translation.promoteIntLiteral(c_int, 2147483647, .decimal);
pub const __LONG_MAX__ = @import("std").zig.c_translation.promoteIntLiteral(c_long, 9223372036854775807, .decimal);
pub const __LONG_LONG_MAX__ = @as(c_longlong, 9223372036854775807);
pub const __WCHAR_MAX__ = @import("std").zig.c_translation.promoteIntLiteral(c_uint, 4294967295, .decimal);
pub const __WCHAR_WIDTH__ = @as(c_int, 32);
pub const __WINT_MAX__ = @import("std").zig.c_translation.promoteIntLiteral(c_uint, 4294967295, .decimal);
pub const __WINT_WIDTH__ = @as(c_int, 32);
pub const __INTMAX_MAX__ = @import("std").zig.c_translation.promoteIntLiteral(c_long, 9223372036854775807, .decimal);
pub const __INTMAX_WIDTH__ = @as(c_int, 64);
pub const __SIZE_MAX__ = @import("std").zig.c_translation.promoteIntLiteral(c_ulong, 18446744073709551615, .decimal);
pub const __SIZE_WIDTH__ = @as(c_int, 64);
pub const __UINTMAX_MAX__ = @import("std").zig.c_translation.promoteIntLiteral(c_ulong, 18446744073709551615, .decimal);
pub const __UINTMAX_WIDTH__ = @as(c_int, 64);
pub const __PTRDIFF_MAX__ = @import("std").zig.c_translation.promoteIntLiteral(c_long, 9223372036854775807, .decimal);
pub const __PTRDIFF_WIDTH__ = @as(c_int, 64);
pub const __INTPTR_MAX__ = @import("std").zig.c_translation.promoteIntLiteral(c_long, 9223372036854775807, .decimal);
pub const __INTPTR_WIDTH__ = @as(c_int, 64);
pub const __UINTPTR_MAX__ = @import("std").zig.c_translation.promoteIntLiteral(c_ulong, 18446744073709551615, .decimal);
pub const __UINTPTR_WIDTH__ = @as(c_int, 64);
pub const __SIZEOF_DOUBLE__ = @as(c_int, 8);
pub const __SIZEOF_FLOAT__ = @as(c_int, 4);
pub const __SIZEOF_INT__ = @as(c_int, 4);
pub const __SIZEOF_LONG__ = @as(c_int, 8);
pub const __SIZEOF_LONG_DOUBLE__ = @as(c_int, 16);
pub const __SIZEOF_LONG_LONG__ = @as(c_int, 8);
pub const __SIZEOF_POINTER__ = @as(c_int, 8);
pub const __SIZEOF_SHORT__ = @as(c_int, 2);
pub const __SIZEOF_PTRDIFF_T__ = @as(c_int, 8);
pub const __SIZEOF_SIZE_T__ = @as(c_int, 8);
pub const __SIZEOF_WCHAR_T__ = @as(c_int, 4);
pub const __SIZEOF_WINT_T__ = @as(c_int, 4);
pub const __SIZEOF_INT128__ = @as(c_int, 16);
pub const __INTMAX_TYPE__ = c_long;
pub const __INTMAX_FMTd__ = "ld";
pub const __INTMAX_FMTi__ = "li";
pub const __INTMAX_C_SUFFIX__ = @compileError("unable to translate macro: undefined identifier `L`");
// (no file):95:9
pub const __UINTMAX_TYPE__ = c_ulong;
pub const __UINTMAX_FMTo__ = "lo";
pub const __UINTMAX_FMTu__ = "lu";
pub const __UINTMAX_FMTx__ = "lx";
pub const __UINTMAX_FMTX__ = "lX";
pub const __UINTMAX_C_SUFFIX__ = @compileError("unable to translate macro: undefined identifier `UL`");
// (no file):101:9
pub const __PTRDIFF_TYPE__ = c_long;
pub const __PTRDIFF_FMTd__ = "ld";
pub const __PTRDIFF_FMTi__ = "li";
pub const __INTPTR_TYPE__ = c_long;
pub const __INTPTR_FMTd__ = "ld";
pub const __INTPTR_FMTi__ = "li";
pub const __SIZE_TYPE__ = c_ulong;
pub const __SIZE_FMTo__ = "lo";
pub const __SIZE_FMTu__ = "lu";
pub const __SIZE_FMTx__ = "lx";
pub const __SIZE_FMTX__ = "lX";
pub const __WCHAR_TYPE__ = c_uint;
pub const __WINT_TYPE__ = c_uint;
pub const __SIG_ATOMIC_MAX__ = @import("std").zig.c_translation.promoteIntLiteral(c_int, 2147483647, .decimal);
pub const __SIG_ATOMIC_WIDTH__ = @as(c_int, 32);
pub const __CHAR16_TYPE__ = c_ushort;
pub const __CHAR32_TYPE__ = c_uint;
pub const __UINTPTR_TYPE__ = c_ulong;
pub const __UINTPTR_FMTo__ = "lo";
pub const __UINTPTR_FMTu__ = "lu";
pub const __UINTPTR_FMTx__ = "lx";
pub const __UINTPTR_FMTX__ = "lX";
pub const __FLT16_DENORM_MIN__ = @as(f16, 5.9604644775390625e-8);
pub const __FLT16_HAS_DENORM__ = @as(c_int, 1);
pub const __FLT16_DIG__ = @as(c_int, 3);
pub const __FLT16_DECIMAL_DIG__ = @as(c_int, 5);
pub const __FLT16_EPSILON__ = @as(f16, 9.765625e-4);
pub const __FLT16_HAS_INFINITY__ = @as(c_int, 1);
pub const __FLT16_HAS_QUIET_NAN__ = @as(c_int, 1);
pub const __FLT16_MANT_DIG__ = @as(c_int, 11);
pub const __FLT16_MAX_10_EXP__ = @as(c_int, 4);
pub const __FLT16_MAX_EXP__ = @as(c_int, 16);
pub const __FLT16_MAX__ = @as(f16, 6.5504e+4);
pub const __FLT16_MIN_10_EXP__ = -@as(c_int, 4);
pub const __FLT16_MIN_EXP__ = -@as(c_int, 13);
pub const __FLT16_MIN__ = @as(f16, 6.103515625e-5);
pub const __FLT_DENORM_MIN__ = @as(f32, 1.40129846e-45);
pub const __FLT_HAS_DENORM__ = @as(c_int, 1);
pub const __FLT_DIG__ = @as(c_int, 6);
pub const __FLT_DECIMAL_DIG__ = @as(c_int, 9);
pub const __FLT_EPSILON__ = @as(f32, 1.19209290e-7);
pub const __FLT_HAS_INFINITY__ = @as(c_int, 1);
pub const __FLT_HAS_QUIET_NAN__ = @as(c_int, 1);
pub const __FLT_MANT_DIG__ = @as(c_int, 24);
pub const __FLT_MAX_10_EXP__ = @as(c_int, 38);
pub const __FLT_MAX_EXP__ = @as(c_int, 128);
pub const __FLT_MAX__ = @as(f32, 3.40282347e+38);
pub const __FLT_MIN_10_EXP__ = -@as(c_int, 37);
pub const __FLT_MIN_EXP__ = -@as(c_int, 125);
pub const __FLT_MIN__ = @as(f32, 1.17549435e-38);
pub const __DBL_DENORM_MIN__ = @as(f64, 4.9406564584124654e-324);
pub const __DBL_HAS_DENORM__ = @as(c_int, 1);
pub const __DBL_DIG__ = @as(c_int, 15);
pub const __DBL_DECIMAL_DIG__ = @as(c_int, 17);
pub const __DBL_EPSILON__ = @as(f64, 2.2204460492503131e-16);
pub const __DBL_HAS_INFINITY__ = @as(c_int, 1);
pub const __DBL_HAS_QUIET_NAN__ = @as(c_int, 1);
pub const __DBL_MANT_DIG__ = @as(c_int, 53);
pub const __DBL_MAX_10_EXP__ = @as(c_int, 308);
pub const __DBL_MAX_EXP__ = @as(c_int, 1024);
pub const __DBL_MAX__ = @as(f64, 1.7976931348623157e+308);
pub const __DBL_MIN_10_EXP__ = -@as(c_int, 307);
pub const __DBL_MIN_EXP__ = -@as(c_int, 1021);
pub const __DBL_MIN__ = @as(f64, 2.2250738585072014e-308);
pub const __LDBL_DENORM_MIN__ = @as(c_longdouble, 6.47517511943802511092443895822764655e-4966);
pub const __LDBL_HAS_DENORM__ = @as(c_int, 1);
pub const __LDBL_DIG__ = @as(c_int, 33);
pub const __LDBL_DECIMAL_DIG__ = @as(c_int, 36);
pub const __LDBL_EPSILON__ = @as(c_longdouble, 1.92592994438723585305597794258492732e-34);
pub const __LDBL_HAS_INFINITY__ = @as(c_int, 1);
pub const __LDBL_HAS_QUIET_NAN__ = @as(c_int, 1);
pub const __LDBL_MANT_DIG__ = @as(c_int, 113);
pub const __LDBL_MAX_10_EXP__ = @as(c_int, 4932);
pub const __LDBL_MAX_EXP__ = @as(c_int, 16384);
pub const __LDBL_MAX__ = @as(c_longdouble, 1.18973149535723176508575932662800702e+4932);
pub const __LDBL_MIN_10_EXP__ = -@as(c_int, 4931);
pub const __LDBL_MIN_EXP__ = -@as(c_int, 16381);
pub const __LDBL_MIN__ = @as(c_longdouble, 3.36210314311209350626267781732175260e-4932);
pub const __POINTER_WIDTH__ = @as(c_int, 64);
pub const __BIGGEST_ALIGNMENT__ = @as(c_int, 16);
pub const __CHAR_UNSIGNED__ = @as(c_int, 1);
pub const __WCHAR_UNSIGNED__ = @as(c_int, 1);
pub const __WINT_UNSIGNED__ = @as(c_int, 1);
pub const __INT8_TYPE__ = i8;
pub const __INT8_FMTd__ = "hhd";
pub const __INT8_FMTi__ = "hhi";
pub const __INT8_C_SUFFIX__ = "";
pub const __INT16_TYPE__ = c_short;
pub const __INT16_FMTd__ = "hd";
pub const __INT16_FMTi__ = "hi";
pub const __INT16_C_SUFFIX__ = "";
pub const __INT32_TYPE__ = c_int;
pub const __INT32_FMTd__ = "d";
pub const __INT32_FMTi__ = "i";
pub const __INT32_C_SUFFIX__ = "";
pub const __INT64_TYPE__ = c_long;
pub const __INT64_FMTd__ = "ld";
pub const __INT64_FMTi__ = "li";
pub const __INT64_C_SUFFIX__ = @compileError("unable to translate macro: undefined identifier `L`");
// (no file):200:9
pub const __UINT8_TYPE__ = u8;
pub const __UINT8_FMTo__ = "hho";
pub const __UINT8_FMTu__ = "hhu";
pub const __UINT8_FMTx__ = "hhx";
pub const __UINT8_FMTX__ = "hhX";
pub const __UINT8_C_SUFFIX__ = "";
pub const __UINT8_MAX__ = @as(c_int, 255);
pub const __INT8_MAX__ = @as(c_int, 127);
pub const __UINT16_TYPE__ = c_ushort;
pub const __UINT16_FMTo__ = "ho";
pub const __UINT16_FMTu__ = "hu";
pub const __UINT16_FMTx__ = "hx";
pub const __UINT16_FMTX__ = "hX";
pub const __UINT16_C_SUFFIX__ = "";
pub const __UINT16_MAX__ = @import("std").zig.c_translation.promoteIntLiteral(c_int, 65535, .decimal);
pub const __INT16_MAX__ = @as(c_int, 32767);
pub const __UINT32_TYPE__ = c_uint;
pub const __UINT32_FMTo__ = "o";
pub const __UINT32_FMTu__ = "u";
pub const __UINT32_FMTx__ = "x";
pub const __UINT32_FMTX__ = "X";
pub const __UINT32_C_SUFFIX__ = @compileError("unable to translate macro: undefined identifier `U`");
// (no file):222:9
pub const __UINT32_MAX__ = @import("std").zig.c_translation.promoteIntLiteral(c_uint, 4294967295, .decimal);
pub const __INT32_MAX__ = @import("std").zig.c_translation.promoteIntLiteral(c_int, 2147483647, .decimal);
pub const __UINT64_TYPE__ = c_ulong;
pub const __UINT64_FMTo__ = "lo";
pub const __UINT64_FMTu__ = "lu";
pub const __UINT64_FMTx__ = "lx";
pub const __UINT64_FMTX__ = "lX";
pub const __UINT64_C_SUFFIX__ = @compileError("unable to translate macro: undefined identifier `UL`");
// (no file):230:9
pub const __UINT64_MAX__ = @import("std").zig.c_translation.promoteIntLiteral(c_ulong, 18446744073709551615, .decimal);
pub const __INT64_MAX__ = @import("std").zig.c_translation.promoteIntLiteral(c_long, 9223372036854775807, .decimal);
pub const __INT_LEAST8_TYPE__ = i8;
pub const __INT_LEAST8_MAX__ = @as(c_int, 127);
pub const __INT_LEAST8_WIDTH__ = @as(c_int, 8);
pub const __INT_LEAST8_FMTd__ = "hhd";
pub const __INT_LEAST8_FMTi__ = "hhi";
pub const __UINT_LEAST8_TYPE__ = u8;
pub const __UINT_LEAST8_MAX__ = @as(c_int, 255);
pub const __UINT_LEAST8_FMTo__ = "hho";
pub const __UINT_LEAST8_FMTu__ = "hhu";
pub const __UINT_LEAST8_FMTx__ = "hhx";
pub const __UINT_LEAST8_FMTX__ = "hhX";
pub const __INT_LEAST16_TYPE__ = c_short;
pub const __INT_LEAST16_MAX__ = @as(c_int, 32767);
pub const __INT_LEAST16_WIDTH__ = @as(c_int, 16);
pub const __INT_LEAST16_FMTd__ = "hd";
pub const __INT_LEAST16_FMTi__ = "hi";
pub const __UINT_LEAST16_TYPE__ = c_ushort;
pub const __UINT_LEAST16_MAX__ = @import("std").zig.c_translation.promoteIntLiteral(c_int, 65535, .decimal);
pub const __UINT_LEAST16_FMTo__ = "ho";
pub const __UINT_LEAST16_FMTu__ = "hu";
pub const __UINT_LEAST16_FMTx__ = "hx";
pub const __UINT_LEAST16_FMTX__ = "hX";
pub const __INT_LEAST32_TYPE__ = c_int;
pub const __INT_LEAST32_MAX__ = @import("std").zig.c_translation.promoteIntLiteral(c_int, 2147483647, .decimal);
pub const __INT_LEAST32_WIDTH__ = @as(c_int, 32);
pub const __INT_LEAST32_FMTd__ = "d";
pub const __INT_LEAST32_FMTi__ = "i";
pub const __UINT_LEAST32_TYPE__ = c_uint;
pub const __UINT_LEAST32_MAX__ = @import("std").zig.c_translation.promoteIntLiteral(c_uint, 4294967295, .decimal);
pub const __UINT_LEAST32_FMTo__ = "o";
pub const __UINT_LEAST32_FMTu__ = "u";
pub const __UINT_LEAST32_FMTx__ = "x";
pub const __UINT_LEAST32_FMTX__ = "X";
pub const __INT_LEAST64_TYPE__ = c_long;
pub const __INT_LEAST64_MAX__ = @import("std").zig.c_translation.promoteIntLiteral(c_long, 9223372036854775807, .decimal);
pub const __INT_LEAST64_WIDTH__ = @as(c_int, 64);
pub const __INT_LEAST64_FMTd__ = "ld";
pub const __INT_LEAST64_FMTi__ = "li";
pub const __UINT_LEAST64_TYPE__ = c_ulong;
pub const __UINT_LEAST64_MAX__ = @import("std").zig.c_translation.promoteIntLiteral(c_ulong, 18446744073709551615, .decimal);
pub const __UINT_LEAST64_FMTo__ = "lo";
pub const __UINT_LEAST64_FMTu__ = "lu";
pub const __UINT_LEAST64_FMTx__ = "lx";
pub const __UINT_LEAST64_FMTX__ = "lX";
pub const __INT_FAST8_TYPE__ = i8;
pub const __INT_FAST8_MAX__ = @as(c_int, 127);
pub const __INT_FAST8_WIDTH__ = @as(c_int, 8);
pub const __INT_FAST8_FMTd__ = "hhd";
pub const __INT_FAST8_FMTi__ = "hhi";
pub const __UINT_FAST8_TYPE__ = u8;
pub const __UINT_FAST8_MAX__ = @as(c_int, 255);
pub const __UINT_FAST8_FMTo__ = "hho";
pub const __UINT_FAST8_FMTu__ = "hhu";
pub const __UINT_FAST8_FMTx__ = "hhx";
pub const __UINT_FAST8_FMTX__ = "hhX";
pub const __INT_FAST16_TYPE__ = c_short;
pub const __INT_FAST16_MAX__ = @as(c_int, 32767);
pub const __INT_FAST16_WIDTH__ = @as(c_int, 16);
pub const __INT_FAST16_FMTd__ = "hd";
pub const __INT_FAST16_FMTi__ = "hi";
pub const __UINT_FAST16_TYPE__ = c_ushort;
pub const __UINT_FAST16_MAX__ = @import("std").zig.c_translation.promoteIntLiteral(c_int, 65535, .decimal);
pub const __UINT_FAST16_FMTo__ = "ho";
pub const __UINT_FAST16_FMTu__ = "hu";
pub const __UINT_FAST16_FMTx__ = "hx";
pub const __UINT_FAST16_FMTX__ = "hX";
pub const __INT_FAST32_TYPE__ = c_int;
pub const __INT_FAST32_MAX__ = @import("std").zig.c_translation.promoteIntLiteral(c_int, 2147483647, .decimal);
pub const __INT_FAST32_WIDTH__ = @as(c_int, 32);
pub const __INT_FAST32_FMTd__ = "d";
pub const __INT_FAST32_FMTi__ = "i";
pub const __UINT_FAST32_TYPE__ = c_uint;
pub const __UINT_FAST32_MAX__ = @import("std").zig.c_translation.promoteIntLiteral(c_uint, 4294967295, .decimal);
pub const __UINT_FAST32_FMTo__ = "o";
pub const __UINT_FAST32_FMTu__ = "u";
pub const __UINT_FAST32_FMTx__ = "x";
pub const __UINT_FAST32_FMTX__ = "X";
pub const __INT_FAST64_TYPE__ = c_long;
pub const __INT_FAST64_MAX__ = @import("std").zig.c_translation.promoteIntLiteral(c_long, 9223372036854775807, .decimal);
pub const __INT_FAST64_WIDTH__ = @as(c_int, 64);
pub const __INT_FAST64_FMTd__ = "ld";
pub const __INT_FAST64_FMTi__ = "li";
pub const __UINT_FAST64_TYPE__ = c_ulong;
pub const __UINT_FAST64_MAX__ = @import("std").zig.c_translation.promoteIntLiteral(c_ulong, 18446744073709551615, .decimal);
pub const __UINT_FAST64_FMTo__ = "lo";
pub const __UINT_FAST64_FMTu__ = "lu";
pub const __UINT_FAST64_FMTx__ = "lx";
pub const __UINT_FAST64_FMTX__ = "lX";
pub const __USER_LABEL_PREFIX__ = "";
pub const __FINITE_MATH_ONLY__ = @as(c_int, 0);
pub const __GNUC_STDC_INLINE__ = @as(c_int, 1);
pub const __GCC_ATOMIC_TEST_AND_SET_TRUEVAL = @as(c_int, 1);
pub const __CLANG_ATOMIC_BOOL_LOCK_FREE = @as(c_int, 2);
pub const __CLANG_ATOMIC_CHAR_LOCK_FREE = @as(c_int, 2);
pub const __CLANG_ATOMIC_CHAR16_T_LOCK_FREE = @as(c_int, 2);
pub const __CLANG_ATOMIC_CHAR32_T_LOCK_FREE = @as(c_int, 2);
pub const __CLANG_ATOMIC_WCHAR_T_LOCK_FREE = @as(c_int, 2);
pub const __CLANG_ATOMIC_SHORT_LOCK_FREE = @as(c_int, 2);
pub const __CLANG_ATOMIC_INT_LOCK_FREE = @as(c_int, 2);
pub const __CLANG_ATOMIC_LONG_LOCK_FREE = @as(c_int, 2);
pub const __CLANG_ATOMIC_LLONG_LOCK_FREE = @as(c_int, 2);
pub const __CLANG_ATOMIC_POINTER_LOCK_FREE = @as(c_int, 2);
pub const __GCC_ATOMIC_BOOL_LOCK_FREE = @as(c_int, 2);
pub const __GCC_ATOMIC_CHAR_LOCK_FREE = @as(c_int, 2);
pub const __GCC_ATOMIC_CHAR16_T_LOCK_FREE = @as(c_int, 2);
pub const __GCC_ATOMIC_CHAR32_T_LOCK_FREE = @as(c_int, 2);
pub const __GCC_ATOMIC_WCHAR_T_LOCK_FREE = @as(c_int, 2);
pub const __GCC_ATOMIC_SHORT_LOCK_FREE = @as(c_int, 2);
pub const __GCC_ATOMIC_INT_LOCK_FREE = @as(c_int, 2);
pub const __GCC_ATOMIC_LONG_LOCK_FREE = @as(c_int, 2);
pub const __GCC_ATOMIC_LLONG_LOCK_FREE = @as(c_int, 2);
pub const __GCC_ATOMIC_POINTER_LOCK_FREE = @as(c_int, 2);
pub const __NO_INLINE__ = @as(c_int, 1);
pub const __PIC__ = @as(c_int, 2);
pub const __pic__ = @as(c_int, 2);
pub const __FLT_RADIX__ = @as(c_int, 2);
pub const __DECIMAL_DIG__ = __LDBL_DECIMAL_DIG__;
pub const __SSP_STRONG__ = @as(c_int, 2);
pub const __ELF__ = @as(c_int, 1);
pub const __AARCH64EL__ = @as(c_int, 1);
pub const __aarch64__ = @as(c_int, 1);
pub const __GCC_ASM_FLAG_OUTPUTS__ = @as(c_int, 1);
pub const __AARCH64_CMODEL_SMALL__ = @as(c_int, 1);
pub const __ARM_ACLE = @as(c_int, 200);
pub const __ARM_ARCH = @as(c_int, 8);
pub const __ARM_ARCH_PROFILE = 'A';
pub const __ARM_64BIT_STATE = @as(c_int, 1);
pub const __ARM_PCS_AAPCS64 = @as(c_int, 1);
pub const __ARM_ARCH_ISA_A64 = @as(c_int, 1);
pub const __ARM_FEATURE_CLZ = @as(c_int, 1);
pub const __ARM_FEATURE_FMA = @as(c_int, 1);
pub const __ARM_FEATURE_LDREX = @as(c_int, 0xF);
pub const __ARM_FEATURE_IDIV = @as(c_int, 1);
pub const __ARM_FEATURE_DIV = @as(c_int, 1);
pub const __ARM_FEATURE_NUMERIC_MAXMIN = @as(c_int, 1);
pub const __ARM_FEATURE_DIRECTED_ROUNDING = @as(c_int, 1);
pub const __ARM_ALIGN_MAX_STACK_PWR = @as(c_int, 4);
pub const __ARM_STATE_ZA = @as(c_int, 1);
pub const __ARM_STATE_ZT0 = @as(c_int, 1);
pub const __ARM_FP = @as(c_int, 0xE);
pub const __ARM_FP16_FORMAT_IEEE = @as(c_int, 1);
pub const __ARM_FP16_ARGS = @as(c_int, 1);
pub const __ARM_SIZEOF_WCHAR_T = @as(c_int, 4);
pub const __ARM_SIZEOF_MINIMAL_ENUM = @as(c_int, 4);
pub const __ARM_NEON = @as(c_int, 1);
pub const __ARM_NEON_FP = @as(c_int, 0xE);
pub const __ARM_FEATURE_UNALIGNED = @as(c_int, 1);
pub const __GCC_HAVE_SYNC_COMPARE_AND_SWAP_1 = @as(c_int, 1);
pub const __GCC_HAVE_SYNC_COMPARE_AND_SWAP_2 = @as(c_int, 1);
pub const __GCC_HAVE_SYNC_COMPARE_AND_SWAP_4 = @as(c_int, 1);
pub const __GCC_HAVE_SYNC_COMPARE_AND_SWAP_8 = @as(c_int, 1);
pub const __GCC_HAVE_SYNC_COMPARE_AND_SWAP_16 = @as(c_int, 1);
pub const __FP_FAST_FMA = @as(c_int, 1);
pub const __FP_FAST_FMAF = @as(c_int, 1);
pub const unix = @as(c_int, 1);
pub const __unix = @as(c_int, 1);
pub const __unix__ = @as(c_int, 1);
pub const linux = @as(c_int, 1);
pub const __linux = @as(c_int, 1);
pub const __linux__ = @as(c_int, 1);
pub const __gnu_linux__ = @as(c_int, 1);
pub const __STDC__ = @as(c_int, 1);
pub const __STDC_HOSTED__ = @as(c_int, 1);
pub const __STDC_VERSION__ = @as(c_long, 201710);
pub const __STDC_UTF_16__ = @as(c_int, 1);
pub const __STDC_UTF_32__ = @as(c_int, 1);
pub const __GLIBC_MINOR__ = @as(c_int, 39);
pub const _DEBUG = @as(c_int, 1);
pub const __GCC_HAVE_DWARF2_CFI_ASM = @as(c_int, 1);
pub const KB_DATA_STRUCTURES_H = "";
pub const KB_COMMON_H = "";
pub const SQLITE3_H = "";
pub const __STDARG_H = "";
pub const __need___va_list = "";
pub const __need_va_list = "";
pub const __need_va_arg = "";
pub const __need___va_copy = "";
pub const __need_va_copy = "";
pub const __GNUC_VA_LIST = "";
pub const _VA_LIST = "";
pub const va_start = @compileError("unable to translate macro: undefined identifier `__builtin_va_start`");
// /home/gedgar/zig-linux-aarch64-0.13.0/lib/include/__stdarg_va_arg.h:17:9
pub const va_end = @compileError("unable to translate macro: undefined identifier `__builtin_va_end`");
// /home/gedgar/zig-linux-aarch64-0.13.0/lib/include/__stdarg_va_arg.h:19:9
pub const va_arg = @compileError("unable to translate C expr: unexpected token 'an identifier'");
// /home/gedgar/zig-linux-aarch64-0.13.0/lib/include/__stdarg_va_arg.h:20:9
pub const __va_copy = @compileError("unable to translate macro: undefined identifier `__builtin_va_copy`");
// /home/gedgar/zig-linux-aarch64-0.13.0/lib/include/__stdarg___va_copy.h:11:9
pub const va_copy = @compileError("unable to translate macro: undefined identifier `__builtin_va_copy`");
// /home/gedgar/zig-linux-aarch64-0.13.0/lib/include/__stdarg_va_copy.h:11:9
pub const SQLITE_EXTERN = @compileError("unable to translate C expr: unexpected token 'extern'");
// /usr/include/sqlite3.h:72:10
pub const SQLITE_API = "";
pub const SQLITE_CDECL = "";
pub const SQLITE_APICALL = "";
pub const SQLITE_STDCALL = "";
pub const SQLITE_CALLBACK = "";
pub const SQLITE_SYSAPI = "";
pub const SQLITE_DEPRECATED = "";
pub const SQLITE_EXPERIMENTAL = "";
pub const SQLITE_VERSION = "3.45.1";
pub const SQLITE_VERSION_NUMBER = @import("std").zig.c_translation.promoteIntLiteral(c_int, 3045001, .decimal);
pub const SQLITE_SOURCE_ID = "2024-01-30 16:01:20 e876e51a0ed5c5b3126f52e532044363a014bc594cfefa87ffb5b82257ccalt1";
pub const SQLITE_OK = @as(c_int, 0);
pub const SQLITE_ERROR = @as(c_int, 1);
pub const SQLITE_INTERNAL = @as(c_int, 2);
pub const SQLITE_PERM = @as(c_int, 3);
pub const SQLITE_ABORT = @as(c_int, 4);
pub const SQLITE_BUSY = @as(c_int, 5);
pub const SQLITE_LOCKED = @as(c_int, 6);
pub const SQLITE_NOMEM = @as(c_int, 7);
pub const SQLITE_READONLY = @as(c_int, 8);
pub const SQLITE_INTERRUPT = @as(c_int, 9);
pub const SQLITE_IOERR = @as(c_int, 10);
pub const SQLITE_CORRUPT = @as(c_int, 11);
pub const SQLITE_NOTFOUND = @as(c_int, 12);
pub const SQLITE_FULL = @as(c_int, 13);
pub const SQLITE_CANTOPEN = @as(c_int, 14);
pub const SQLITE_PROTOCOL = @as(c_int, 15);
pub const SQLITE_EMPTY = @as(c_int, 16);
pub const SQLITE_SCHEMA = @as(c_int, 17);
pub const SQLITE_TOOBIG = @as(c_int, 18);
pub const SQLITE_CONSTRAINT = @as(c_int, 19);
pub const SQLITE_MISMATCH = @as(c_int, 20);
pub const SQLITE_MISUSE = @as(c_int, 21);
pub const SQLITE_NOLFS = @as(c_int, 22);
pub const SQLITE_AUTH = @as(c_int, 23);
pub const SQLITE_FORMAT = @as(c_int, 24);
pub const SQLITE_RANGE = @as(c_int, 25);
pub const SQLITE_NOTADB = @as(c_int, 26);
pub const SQLITE_NOTICE = @as(c_int, 27);
pub const SQLITE_WARNING = @as(c_int, 28);
pub const SQLITE_ROW = @as(c_int, 100);
pub const SQLITE_DONE = @as(c_int, 101);
pub const SQLITE_ERROR_MISSING_COLLSEQ = SQLITE_ERROR | (@as(c_int, 1) << @as(c_int, 8));
pub const SQLITE_ERROR_RETRY = SQLITE_ERROR | (@as(c_int, 2) << @as(c_int, 8));
pub const SQLITE_ERROR_SNAPSHOT = SQLITE_ERROR | (@as(c_int, 3) << @as(c_int, 8));
pub const SQLITE_IOERR_READ = SQLITE_IOERR | (@as(c_int, 1) << @as(c_int, 8));
pub const SQLITE_IOERR_SHORT_READ = SQLITE_IOERR | (@as(c_int, 2) << @as(c_int, 8));
pub const SQLITE_IOERR_WRITE = SQLITE_IOERR | (@as(c_int, 3) << @as(c_int, 8));
pub const SQLITE_IOERR_FSYNC = SQLITE_IOERR | (@as(c_int, 4) << @as(c_int, 8));
pub const SQLITE_IOERR_DIR_FSYNC = SQLITE_IOERR | (@as(c_int, 5) << @as(c_int, 8));
pub const SQLITE_IOERR_TRUNCATE = SQLITE_IOERR | (@as(c_int, 6) << @as(c_int, 8));
pub const SQLITE_IOERR_FSTAT = SQLITE_IOERR | (@as(c_int, 7) << @as(c_int, 8));
pub const SQLITE_IOERR_UNLOCK = SQLITE_IOERR | (@as(c_int, 8) << @as(c_int, 8));
pub const SQLITE_IOERR_RDLOCK = SQLITE_IOERR | (@as(c_int, 9) << @as(c_int, 8));
pub const SQLITE_IOERR_DELETE = SQLITE_IOERR | (@as(c_int, 10) << @as(c_int, 8));
pub const SQLITE_IOERR_BLOCKED = SQLITE_IOERR | (@as(c_int, 11) << @as(c_int, 8));
pub const SQLITE_IOERR_NOMEM = SQLITE_IOERR | (@as(c_int, 12) << @as(c_int, 8));
pub const SQLITE_IOERR_ACCESS = SQLITE_IOERR | (@as(c_int, 13) << @as(c_int, 8));
pub const SQLITE_IOERR_CHECKRESERVEDLOCK = SQLITE_IOERR | (@as(c_int, 14) << @as(c_int, 8));
pub const SQLITE_IOERR_LOCK = SQLITE_IOERR | (@as(c_int, 15) << @as(c_int, 8));
pub const SQLITE_IOERR_CLOSE = SQLITE_IOERR | (@as(c_int, 16) << @as(c_int, 8));
pub const SQLITE_IOERR_DIR_CLOSE = SQLITE_IOERR | (@as(c_int, 17) << @as(c_int, 8));
pub const SQLITE_IOERR_SHMOPEN = SQLITE_IOERR | (@as(c_int, 18) << @as(c_int, 8));
pub const SQLITE_IOERR_SHMSIZE = SQLITE_IOERR | (@as(c_int, 19) << @as(c_int, 8));
pub const SQLITE_IOERR_SHMLOCK = SQLITE_IOERR | (@as(c_int, 20) << @as(c_int, 8));
pub const SQLITE_IOERR_SHMMAP = SQLITE_IOERR | (@as(c_int, 21) << @as(c_int, 8));
pub const SQLITE_IOERR_SEEK = SQLITE_IOERR | (@as(c_int, 22) << @as(c_int, 8));
pub const SQLITE_IOERR_DELETE_NOENT = SQLITE_IOERR | (@as(c_int, 23) << @as(c_int, 8));
pub const SQLITE_IOERR_MMAP = SQLITE_IOERR | (@as(c_int, 24) << @as(c_int, 8));
pub const SQLITE_IOERR_GETTEMPPATH = SQLITE_IOERR | (@as(c_int, 25) << @as(c_int, 8));
pub const SQLITE_IOERR_CONVPATH = SQLITE_IOERR | (@as(c_int, 26) << @as(c_int, 8));
pub const SQLITE_IOERR_VNODE = SQLITE_IOERR | (@as(c_int, 27) << @as(c_int, 8));
pub const SQLITE_IOERR_AUTH = SQLITE_IOERR | (@as(c_int, 28) << @as(c_int, 8));
pub const SQLITE_IOERR_BEGIN_ATOMIC = SQLITE_IOERR | (@as(c_int, 29) << @as(c_int, 8));
pub const SQLITE_IOERR_COMMIT_ATOMIC = SQLITE_IOERR | (@as(c_int, 30) << @as(c_int, 8));
pub const SQLITE_IOERR_ROLLBACK_ATOMIC = SQLITE_IOERR | (@as(c_int, 31) << @as(c_int, 8));
pub const SQLITE_IOERR_DATA = SQLITE_IOERR | (@as(c_int, 32) << @as(c_int, 8));
pub const SQLITE_IOERR_CORRUPTFS = SQLITE_IOERR | (@as(c_int, 33) << @as(c_int, 8));
pub const SQLITE_IOERR_IN_PAGE = SQLITE_IOERR | (@as(c_int, 34) << @as(c_int, 8));
pub const SQLITE_LOCKED_SHAREDCACHE = SQLITE_LOCKED | (@as(c_int, 1) << @as(c_int, 8));
pub const SQLITE_LOCKED_VTAB = SQLITE_LOCKED | (@as(c_int, 2) << @as(c_int, 8));
pub const SQLITE_BUSY_RECOVERY = SQLITE_BUSY | (@as(c_int, 1) << @as(c_int, 8));
pub const SQLITE_BUSY_SNAPSHOT = SQLITE_BUSY | (@as(c_int, 2) << @as(c_int, 8));
pub const SQLITE_BUSY_TIMEOUT = SQLITE_BUSY | (@as(c_int, 3) << @as(c_int, 8));
pub const SQLITE_CANTOPEN_NOTEMPDIR = SQLITE_CANTOPEN | (@as(c_int, 1) << @as(c_int, 8));
pub const SQLITE_CANTOPEN_ISDIR = SQLITE_CANTOPEN | (@as(c_int, 2) << @as(c_int, 8));
pub const SQLITE_CANTOPEN_FULLPATH = SQLITE_CANTOPEN | (@as(c_int, 3) << @as(c_int, 8));
pub const SQLITE_CANTOPEN_CONVPATH = SQLITE_CANTOPEN | (@as(c_int, 4) << @as(c_int, 8));
pub const SQLITE_CANTOPEN_DIRTYWAL = SQLITE_CANTOPEN | (@as(c_int, 5) << @as(c_int, 8));
pub const SQLITE_CANTOPEN_SYMLINK = SQLITE_CANTOPEN | (@as(c_int, 6) << @as(c_int, 8));
pub const SQLITE_CORRUPT_VTAB = SQLITE_CORRUPT | (@as(c_int, 1) << @as(c_int, 8));
pub const SQLITE_CORRUPT_SEQUENCE = SQLITE_CORRUPT | (@as(c_int, 2) << @as(c_int, 8));
pub const SQLITE_CORRUPT_INDEX = SQLITE_CORRUPT | (@as(c_int, 3) << @as(c_int, 8));
pub const SQLITE_READONLY_RECOVERY = SQLITE_READONLY | (@as(c_int, 1) << @as(c_int, 8));
pub const SQLITE_READONLY_CANTLOCK = SQLITE_READONLY | (@as(c_int, 2) << @as(c_int, 8));
pub const SQLITE_READONLY_ROLLBACK = SQLITE_READONLY | (@as(c_int, 3) << @as(c_int, 8));
pub const SQLITE_READONLY_DBMOVED = SQLITE_READONLY | (@as(c_int, 4) << @as(c_int, 8));
pub const SQLITE_READONLY_CANTINIT = SQLITE_READONLY | (@as(c_int, 5) << @as(c_int, 8));
pub const SQLITE_READONLY_DIRECTORY = SQLITE_READONLY | (@as(c_int, 6) << @as(c_int, 8));
pub const SQLITE_ABORT_ROLLBACK = SQLITE_ABORT | (@as(c_int, 2) << @as(c_int, 8));
pub const SQLITE_CONSTRAINT_CHECK = SQLITE_CONSTRAINT | (@as(c_int, 1) << @as(c_int, 8));
pub const SQLITE_CONSTRAINT_COMMITHOOK = SQLITE_CONSTRAINT | (@as(c_int, 2) << @as(c_int, 8));
pub const SQLITE_CONSTRAINT_FOREIGNKEY = SQLITE_CONSTRAINT | (@as(c_int, 3) << @as(c_int, 8));
pub const SQLITE_CONSTRAINT_FUNCTION = SQLITE_CONSTRAINT | (@as(c_int, 4) << @as(c_int, 8));
pub const SQLITE_CONSTRAINT_NOTNULL = SQLITE_CONSTRAINT | (@as(c_int, 5) << @as(c_int, 8));
pub const SQLITE_CONSTRAINT_PRIMARYKEY = SQLITE_CONSTRAINT | (@as(c_int, 6) << @as(c_int, 8));
pub const SQLITE_CONSTRAINT_TRIGGER = SQLITE_CONSTRAINT | (@as(c_int, 7) << @as(c_int, 8));
pub const SQLITE_CONSTRAINT_UNIQUE = SQLITE_CONSTRAINT | (@as(c_int, 8) << @as(c_int, 8));
pub const SQLITE_CONSTRAINT_VTAB = SQLITE_CONSTRAINT | (@as(c_int, 9) << @as(c_int, 8));
pub const SQLITE_CONSTRAINT_ROWID = SQLITE_CONSTRAINT | (@as(c_int, 10) << @as(c_int, 8));
pub const SQLITE_CONSTRAINT_PINNED = SQLITE_CONSTRAINT | (@as(c_int, 11) << @as(c_int, 8));
pub const SQLITE_CONSTRAINT_DATATYPE = SQLITE_CONSTRAINT | (@as(c_int, 12) << @as(c_int, 8));
pub const SQLITE_NOTICE_RECOVER_WAL = SQLITE_NOTICE | (@as(c_int, 1) << @as(c_int, 8));
pub const SQLITE_NOTICE_RECOVER_ROLLBACK = SQLITE_NOTICE | (@as(c_int, 2) << @as(c_int, 8));
pub const SQLITE_NOTICE_RBU = SQLITE_NOTICE | (@as(c_int, 3) << @as(c_int, 8));
pub const SQLITE_WARNING_AUTOINDEX = SQLITE_WARNING | (@as(c_int, 1) << @as(c_int, 8));
pub const SQLITE_AUTH_USER = SQLITE_AUTH | (@as(c_int, 1) << @as(c_int, 8));
pub const SQLITE_OK_LOAD_PERMANENTLY = SQLITE_OK | (@as(c_int, 1) << @as(c_int, 8));
pub const SQLITE_OK_SYMLINK = SQLITE_OK | (@as(c_int, 2) << @as(c_int, 8));
pub const SQLITE_OPEN_READONLY = @as(c_int, 0x00000001);
pub const SQLITE_OPEN_READWRITE = @as(c_int, 0x00000002);
pub const SQLITE_OPEN_CREATE = @as(c_int, 0x00000004);
pub const SQLITE_OPEN_DELETEONCLOSE = @as(c_int, 0x00000008);
pub const SQLITE_OPEN_EXCLUSIVE = @as(c_int, 0x00000010);
pub const SQLITE_OPEN_AUTOPROXY = @as(c_int, 0x00000020);
pub const SQLITE_OPEN_URI = @as(c_int, 0x00000040);
pub const SQLITE_OPEN_MEMORY = @as(c_int, 0x00000080);
pub const SQLITE_OPEN_MAIN_DB = @as(c_int, 0x00000100);
pub const SQLITE_OPEN_TEMP_DB = @as(c_int, 0x00000200);
pub const SQLITE_OPEN_TRANSIENT_DB = @as(c_int, 0x00000400);
pub const SQLITE_OPEN_MAIN_JOURNAL = @as(c_int, 0x00000800);
pub const SQLITE_OPEN_TEMP_JOURNAL = @as(c_int, 0x00001000);
pub const SQLITE_OPEN_SUBJOURNAL = @as(c_int, 0x00002000);
pub const SQLITE_OPEN_SUPER_JOURNAL = @as(c_int, 0x00004000);
pub const SQLITE_OPEN_NOMUTEX = @import("std").zig.c_translation.promoteIntLiteral(c_int, 0x00008000, .hex);
pub const SQLITE_OPEN_FULLMUTEX = @import("std").zig.c_translation.promoteIntLiteral(c_int, 0x00010000, .hex);
pub const SQLITE_OPEN_SHAREDCACHE = @import("std").zig.c_translation.promoteIntLiteral(c_int, 0x00020000, .hex);
pub const SQLITE_OPEN_PRIVATECACHE = @import("std").zig.c_translation.promoteIntLiteral(c_int, 0x00040000, .hex);
pub const SQLITE_OPEN_WAL = @import("std").zig.c_translation.promoteIntLiteral(c_int, 0x00080000, .hex);
pub const SQLITE_OPEN_NOFOLLOW = @import("std").zig.c_translation.promoteIntLiteral(c_int, 0x01000000, .hex);
pub const SQLITE_OPEN_EXRESCODE = @import("std").zig.c_translation.promoteIntLiteral(c_int, 0x02000000, .hex);
pub const SQLITE_OPEN_MASTER_JOURNAL = @as(c_int, 0x00004000);
pub const SQLITE_IOCAP_ATOMIC = @as(c_int, 0x00000001);
pub const SQLITE_IOCAP_ATOMIC512 = @as(c_int, 0x00000002);
pub const SQLITE_IOCAP_ATOMIC1K = @as(c_int, 0x00000004);
pub const SQLITE_IOCAP_ATOMIC2K = @as(c_int, 0x00000008);
pub const SQLITE_IOCAP_ATOMIC4K = @as(c_int, 0x00000010);
pub const SQLITE_IOCAP_ATOMIC8K = @as(c_int, 0x00000020);
pub const SQLITE_IOCAP_ATOMIC16K = @as(c_int, 0x00000040);
pub const SQLITE_IOCAP_ATOMIC32K = @as(c_int, 0x00000080);
pub const SQLITE_IOCAP_ATOMIC64K = @as(c_int, 0x00000100);
pub const SQLITE_IOCAP_SAFE_APPEND = @as(c_int, 0x00000200);
pub const SQLITE_IOCAP_SEQUENTIAL = @as(c_int, 0x00000400);
pub const SQLITE_IOCAP_UNDELETABLE_WHEN_OPEN = @as(c_int, 0x00000800);
pub const SQLITE_IOCAP_POWERSAFE_OVERWRITE = @as(c_int, 0x00001000);
pub const SQLITE_IOCAP_IMMUTABLE = @as(c_int, 0x00002000);
pub const SQLITE_IOCAP_BATCH_ATOMIC = @as(c_int, 0x00004000);
pub const SQLITE_LOCK_NONE = @as(c_int, 0);
pub const SQLITE_LOCK_SHARED = @as(c_int, 1);
pub const SQLITE_LOCK_RESERVED = @as(c_int, 2);
pub const SQLITE_LOCK_PENDING = @as(c_int, 3);
pub const SQLITE_LOCK_EXCLUSIVE = @as(c_int, 4);
pub const SQLITE_SYNC_NORMAL = @as(c_int, 0x00002);
pub const SQLITE_SYNC_FULL = @as(c_int, 0x00003);
pub const SQLITE_SYNC_DATAONLY = @as(c_int, 0x00010);
pub const SQLITE_FCNTL_LOCKSTATE = @as(c_int, 1);
pub const SQLITE_FCNTL_GET_LOCKPROXYFILE = @as(c_int, 2);
pub const SQLITE_FCNTL_SET_LOCKPROXYFILE = @as(c_int, 3);
pub const SQLITE_FCNTL_LAST_ERRNO = @as(c_int, 4);
pub const SQLITE_FCNTL_SIZE_HINT = @as(c_int, 5);
pub const SQLITE_FCNTL_CHUNK_SIZE = @as(c_int, 6);
pub const SQLITE_FCNTL_FILE_POINTER = @as(c_int, 7);
pub const SQLITE_FCNTL_SYNC_OMITTED = @as(c_int, 8);
pub const SQLITE_FCNTL_WIN32_AV_RETRY = @as(c_int, 9);
pub const SQLITE_FCNTL_PERSIST_WAL = @as(c_int, 10);
pub const SQLITE_FCNTL_OVERWRITE = @as(c_int, 11);
pub const SQLITE_FCNTL_VFSNAME = @as(c_int, 12);
pub const SQLITE_FCNTL_POWERSAFE_OVERWRITE = @as(c_int, 13);
pub const SQLITE_FCNTL_PRAGMA = @as(c_int, 14);
pub const SQLITE_FCNTL_BUSYHANDLER = @as(c_int, 15);
pub const SQLITE_FCNTL_TEMPFILENAME = @as(c_int, 16);
pub const SQLITE_FCNTL_MMAP_SIZE = @as(c_int, 18);
pub const SQLITE_FCNTL_TRACE = @as(c_int, 19);
pub const SQLITE_FCNTL_HAS_MOVED = @as(c_int, 20);
pub const SQLITE_FCNTL_SYNC = @as(c_int, 21);
pub const SQLITE_FCNTL_COMMIT_PHASETWO = @as(c_int, 22);
pub const SQLITE_FCNTL_WIN32_SET_HANDLE = @as(c_int, 23);
pub const SQLITE_FCNTL_WAL_BLOCK = @as(c_int, 24);
pub const SQLITE_FCNTL_ZIPVFS = @as(c_int, 25);
pub const SQLITE_FCNTL_RBU = @as(c_int, 26);
pub const SQLITE_FCNTL_VFS_POINTER = @as(c_int, 27);
pub const SQLITE_FCNTL_JOURNAL_POINTER = @as(c_int, 28);
pub const SQLITE_FCNTL_WIN32_GET_HANDLE = @as(c_int, 29);
pub const SQLITE_FCNTL_PDB = @as(c_int, 30);
pub const SQLITE_FCNTL_BEGIN_ATOMIC_WRITE = @as(c_int, 31);
pub const SQLITE_FCNTL_COMMIT_ATOMIC_WRITE = @as(c_int, 32);
pub const SQLITE_FCNTL_ROLLBACK_ATOMIC_WRITE = @as(c_int, 33);
pub const SQLITE_FCNTL_LOCK_TIMEOUT = @as(c_int, 34);
pub const SQLITE_FCNTL_DATA_VERSION = @as(c_int, 35);
pub const SQLITE_FCNTL_SIZE_LIMIT = @as(c_int, 36);
pub const SQLITE_FCNTL_CKPT_DONE = @as(c_int, 37);
pub const SQLITE_FCNTL_RESERVE_BYTES = @as(c_int, 38);
pub const SQLITE_FCNTL_CKPT_START = @as(c_int, 39);
pub const SQLITE_FCNTL_EXTERNAL_READER = @as(c_int, 40);
pub const SQLITE_FCNTL_CKSM_FILE = @as(c_int, 41);
pub const SQLITE_FCNTL_RESET_CACHE = @as(c_int, 42);
pub const SQLITE_GET_LOCKPROXYFILE = SQLITE_FCNTL_GET_LOCKPROXYFILE;
pub const SQLITE_SET_LOCKPROXYFILE = SQLITE_FCNTL_SET_LOCKPROXYFILE;
pub const SQLITE_LAST_ERRNO = SQLITE_FCNTL_LAST_ERRNO;
pub const SQLITE_ACCESS_EXISTS = @as(c_int, 0);
pub const SQLITE_ACCESS_READWRITE = @as(c_int, 1);
pub const SQLITE_ACCESS_READ = @as(c_int, 2);
pub const SQLITE_SHM_UNLOCK = @as(c_int, 1);
pub const SQLITE_SHM_LOCK = @as(c_int, 2);
pub const SQLITE_SHM_SHARED = @as(c_int, 4);
pub const SQLITE_SHM_EXCLUSIVE = @as(c_int, 8);
pub const SQLITE_SHM_NLOCK = @as(c_int, 8);
pub const SQLITE_CONFIG_SINGLETHREAD = @as(c_int, 1);
pub const SQLITE_CONFIG_MULTITHREAD = @as(c_int, 2);
pub const SQLITE_CONFIG_SERIALIZED = @as(c_int, 3);
pub const SQLITE_CONFIG_MALLOC = @as(c_int, 4);
pub const SQLITE_CONFIG_GETMALLOC = @as(c_int, 5);
pub const SQLITE_CONFIG_SCRATCH = @as(c_int, 6);
pub const SQLITE_CONFIG_PAGECACHE = @as(c_int, 7);
pub const SQLITE_CONFIG_HEAP = @as(c_int, 8);
pub const SQLITE_CONFIG_MEMSTATUS = @as(c_int, 9);
pub const SQLITE_CONFIG_MUTEX = @as(c_int, 10);
pub const SQLITE_CONFIG_GETMUTEX = @as(c_int, 11);
pub const SQLITE_CONFIG_LOOKASIDE = @as(c_int, 13);
pub const SQLITE_CONFIG_PCACHE = @as(c_int, 14);
pub const SQLITE_CONFIG_GETPCACHE = @as(c_int, 15);
pub const SQLITE_CONFIG_LOG = @as(c_int, 16);
pub const SQLITE_CONFIG_URI = @as(c_int, 17);
pub const SQLITE_CONFIG_PCACHE2 = @as(c_int, 18);
pub const SQLITE_CONFIG_GETPCACHE2 = @as(c_int, 19);
pub const SQLITE_CONFIG_COVERING_INDEX_SCAN = @as(c_int, 20);
pub const SQLITE_CONFIG_SQLLOG = @as(c_int, 21);
pub const SQLITE_CONFIG_MMAP_SIZE = @as(c_int, 22);
pub const SQLITE_CONFIG_WIN32_HEAPSIZE = @as(c_int, 23);
pub const SQLITE_CONFIG_PCACHE_HDRSZ = @as(c_int, 24);
pub const SQLITE_CONFIG_PMASZ = @as(c_int, 25);
pub const SQLITE_CONFIG_STMTJRNL_SPILL = @as(c_int, 26);
pub const SQLITE_CONFIG_SMALL_MALLOC = @as(c_int, 27);
pub const SQLITE_CONFIG_SORTERREF_SIZE = @as(c_int, 28);
pub const SQLITE_CONFIG_MEMDB_MAXSIZE = @as(c_int, 29);
pub const SQLITE_DBCONFIG_MAINDBNAME = @as(c_int, 1000);
pub const SQLITE_DBCONFIG_LOOKASIDE = @as(c_int, 1001);
pub const SQLITE_DBCONFIG_ENABLE_FKEY = @as(c_int, 1002);
pub const SQLITE_DBCONFIG_ENABLE_TRIGGER = @as(c_int, 1003);
pub const SQLITE_DBCONFIG_ENABLE_FTS3_TOKENIZER = @as(c_int, 1004);
pub const SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION = @as(c_int, 1005);
pub const SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE = @as(c_int, 1006);
pub const SQLITE_DBCONFIG_ENABLE_QPSG = @as(c_int, 1007);
pub const SQLITE_DBCONFIG_TRIGGER_EQP = @as(c_int, 1008);
pub const SQLITE_DBCONFIG_RESET_DATABASE = @as(c_int, 1009);
pub const SQLITE_DBCONFIG_DEFENSIVE = @as(c_int, 1010);
pub const SQLITE_DBCONFIG_WRITABLE_SCHEMA = @as(c_int, 1011);
pub const SQLITE_DBCONFIG_LEGACY_ALTER_TABLE = @as(c_int, 1012);
pub const SQLITE_DBCONFIG_DQS_DML = @as(c_int, 1013);
pub const SQLITE_DBCONFIG_DQS_DDL = @as(c_int, 1014);
pub const SQLITE_DBCONFIG_ENABLE_VIEW = @as(c_int, 1015);
pub const SQLITE_DBCONFIG_LEGACY_FILE_FORMAT = @as(c_int, 1016);
pub const SQLITE_DBCONFIG_TRUSTED_SCHEMA = @as(c_int, 1017);
pub const SQLITE_DBCONFIG_STMT_SCANSTATUS = @as(c_int, 1018);
pub const SQLITE_DBCONFIG_REVERSE_SCANORDER = @as(c_int, 1019);
pub const SQLITE_DBCONFIG_MAX = @as(c_int, 1019);
pub const SQLITE_DENY = @as(c_int, 1);
pub const SQLITE_IGNORE = @as(c_int, 2);
pub const SQLITE_CREATE_INDEX = @as(c_int, 1);
pub const SQLITE_CREATE_TABLE = @as(c_int, 2);
pub const SQLITE_CREATE_TEMP_INDEX = @as(c_int, 3);
pub const SQLITE_CREATE_TEMP_TABLE = @as(c_int, 4);
pub const SQLITE_CREATE_TEMP_TRIGGER = @as(c_int, 5);
pub const SQLITE_CREATE_TEMP_VIEW = @as(c_int, 6);
pub const SQLITE_CREATE_TRIGGER = @as(c_int, 7);
pub const SQLITE_CREATE_VIEW = @as(c_int, 8);
pub const SQLITE_DELETE = @as(c_int, 9);
pub const SQLITE_DROP_INDEX = @as(c_int, 10);
pub const SQLITE_DROP_TABLE = @as(c_int, 11);
pub const SQLITE_DROP_TEMP_INDEX = @as(c_int, 12);
pub const SQLITE_DROP_TEMP_TABLE = @as(c_int, 13);
pub const SQLITE_DROP_TEMP_TRIGGER = @as(c_int, 14);
pub const SQLITE_DROP_TEMP_VIEW = @as(c_int, 15);
pub const SQLITE_DROP_TRIGGER = @as(c_int, 16);
pub const SQLITE_DROP_VIEW = @as(c_int, 17);
pub const SQLITE_INSERT = @as(c_int, 18);
pub const SQLITE_PRAGMA = @as(c_int, 19);
pub const SQLITE_READ = @as(c_int, 20);
pub const SQLITE_SELECT = @as(c_int, 21);
pub const SQLITE_TRANSACTION = @as(c_int, 22);
pub const SQLITE_UPDATE = @as(c_int, 23);
pub const SQLITE_ATTACH = @as(c_int, 24);
pub const SQLITE_DETACH = @as(c_int, 25);
pub const SQLITE_ALTER_TABLE = @as(c_int, 26);
pub const SQLITE_REINDEX = @as(c_int, 27);
pub const SQLITE_ANALYZE = @as(c_int, 28);
pub const SQLITE_CREATE_VTABLE = @as(c_int, 29);
pub const SQLITE_DROP_VTABLE = @as(c_int, 30);
pub const SQLITE_FUNCTION = @as(c_int, 31);
pub const SQLITE_SAVEPOINT = @as(c_int, 32);
pub const SQLITE_COPY = @as(c_int, 0);
pub const SQLITE_RECURSIVE = @as(c_int, 33);
pub const SQLITE_TRACE_STMT = @as(c_int, 0x01);
pub const SQLITE_TRACE_PROFILE = @as(c_int, 0x02);
pub const SQLITE_TRACE_ROW = @as(c_int, 0x04);
pub const SQLITE_TRACE_CLOSE = @as(c_int, 0x08);
pub const SQLITE_LIMIT_LENGTH = @as(c_int, 0);
pub const SQLITE_LIMIT_SQL_LENGTH = @as(c_int, 1);
pub const SQLITE_LIMIT_COLUMN = @as(c_int, 2);
pub const SQLITE_LIMIT_EXPR_DEPTH = @as(c_int, 3);
pub const SQLITE_LIMIT_COMPOUND_SELECT = @as(c_int, 4);
pub const SQLITE_LIMIT_VDBE_OP = @as(c_int, 5);
pub const SQLITE_LIMIT_FUNCTION_ARG = @as(c_int, 6);
pub const SQLITE_LIMIT_ATTACHED = @as(c_int, 7);
pub const SQLITE_LIMIT_LIKE_PATTERN_LENGTH = @as(c_int, 8);
pub const SQLITE_LIMIT_VARIABLE_NUMBER = @as(c_int, 9);
pub const SQLITE_LIMIT_TRIGGER_DEPTH = @as(c_int, 10);
pub const SQLITE_LIMIT_WORKER_THREADS = @as(c_int, 11);
pub const SQLITE_PREPARE_PERSISTENT = @as(c_int, 0x01);
pub const SQLITE_PREPARE_NORMALIZE = @as(c_int, 0x02);
pub const SQLITE_PREPARE_NO_VTAB = @as(c_int, 0x04);
pub const SQLITE_INTEGER = @as(c_int, 1);
pub const SQLITE_FLOAT = @as(c_int, 2);
pub const SQLITE_BLOB = @as(c_int, 4);
pub const SQLITE_NULL = @as(c_int, 5);
pub const SQLITE_TEXT = @as(c_int, 3);
pub const SQLITE3_TEXT = @as(c_int, 3);
pub const SQLITE_UTF8 = @as(c_int, 1);
pub const SQLITE_UTF16LE = @as(c_int, 2);
pub const SQLITE_UTF16BE = @as(c_int, 3);
pub const SQLITE_UTF16 = @as(c_int, 4);
pub const SQLITE_ANY = @as(c_int, 5);
pub const SQLITE_UTF16_ALIGNED = @as(c_int, 8);
pub const SQLITE_DETERMINISTIC = @as(c_int, 0x000000800);
pub const SQLITE_DIRECTONLY = @import("std").zig.c_translation.promoteIntLiteral(c_int, 0x000080000, .hex);
pub const SQLITE_SUBTYPE = @import("std").zig.c_translation.promoteIntLiteral(c_int, 0x000100000, .hex);
pub const SQLITE_INNOCUOUS = @import("std").zig.c_translation.promoteIntLiteral(c_int, 0x000200000, .hex);
pub const SQLITE_RESULT_SUBTYPE = @import("std").zig.c_translation.promoteIntLiteral(c_int, 0x001000000, .hex);
pub const SQLITE_STATIC = @import("std").zig.c_translation.cast(sqlite3_destructor_type, @as(c_int, 0));
pub const SQLITE_TRANSIENT = @import("std").zig.c_translation.cast(sqlite3_destructor_type, -@as(c_int, 1));
pub const SQLITE_WIN32_DATA_DIRECTORY_TYPE = @as(c_int, 1);
pub const SQLITE_WIN32_TEMP_DIRECTORY_TYPE = @as(c_int, 2);
pub const SQLITE_TXN_NONE = @as(c_int, 0);
pub const SQLITE_TXN_READ = @as(c_int, 1);
pub const SQLITE_TXN_WRITE = @as(c_int, 2);
pub const SQLITE_INDEX_SCAN_UNIQUE = @as(c_int, 1);
pub const SQLITE_INDEX_CONSTRAINT_EQ = @as(c_int, 2);
pub const SQLITE_INDEX_CONSTRAINT_GT = @as(c_int, 4);
pub const SQLITE_INDEX_CONSTRAINT_LE = @as(c_int, 8);
pub const SQLITE_INDEX_CONSTRAINT_LT = @as(c_int, 16);
pub const SQLITE_INDEX_CONSTRAINT_GE = @as(c_int, 32);
pub const SQLITE_INDEX_CONSTRAINT_MATCH = @as(c_int, 64);
pub const SQLITE_INDEX_CONSTRAINT_LIKE = @as(c_int, 65);
pub const SQLITE_INDEX_CONSTRAINT_GLOB = @as(c_int, 66);
pub const SQLITE_INDEX_CONSTRAINT_REGEXP = @as(c_int, 67);
pub const SQLITE_INDEX_CONSTRAINT_NE = @as(c_int, 68);
pub const SQLITE_INDEX_CONSTRAINT_ISNOT = @as(c_int, 69);
pub const SQLITE_INDEX_CONSTRAINT_ISNOTNULL = @as(c_int, 70);
pub const SQLITE_INDEX_CONSTRAINT_ISNULL = @as(c_int, 71);
pub const SQLITE_INDEX_CONSTRAINT_IS = @as(c_int, 72);
pub const SQLITE_INDEX_CONSTRAINT_LIMIT = @as(c_int, 73);
pub const SQLITE_INDEX_CONSTRAINT_OFFSET = @as(c_int, 74);
pub const SQLITE_INDEX_CONSTRAINT_FUNCTION = @as(c_int, 150);
pub const SQLITE_MUTEX_FAST = @as(c_int, 0);
pub const SQLITE_MUTEX_RECURSIVE = @as(c_int, 1);
pub const SQLITE_MUTEX_STATIC_MAIN = @as(c_int, 2);
pub const SQLITE_MUTEX_STATIC_MEM = @as(c_int, 3);
pub const SQLITE_MUTEX_STATIC_MEM2 = @as(c_int, 4);
pub const SQLITE_MUTEX_STATIC_OPEN = @as(c_int, 4);
pub const SQLITE_MUTEX_STATIC_PRNG = @as(c_int, 5);
pub const SQLITE_MUTEX_STATIC_LRU = @as(c_int, 6);
pub const SQLITE_MUTEX_STATIC_LRU2 = @as(c_int, 7);
pub const SQLITE_MUTEX_STATIC_PMEM = @as(c_int, 7);
pub const SQLITE_MUTEX_STATIC_APP1 = @as(c_int, 8);
pub const SQLITE_MUTEX_STATIC_APP2 = @as(c_int, 9);
pub const SQLITE_MUTEX_STATIC_APP3 = @as(c_int, 10);
pub const SQLITE_MUTEX_STATIC_VFS1 = @as(c_int, 11);
pub const SQLITE_MUTEX_STATIC_VFS2 = @as(c_int, 12);
pub const SQLITE_MUTEX_STATIC_VFS3 = @as(c_int, 13);
pub const SQLITE_MUTEX_STATIC_MASTER = @as(c_int, 2);
pub const SQLITE_TESTCTRL_FIRST = @as(c_int, 5);
pub const SQLITE_TESTCTRL_PRNG_SAVE = @as(c_int, 5);
pub const SQLITE_TESTCTRL_PRNG_RESTORE = @as(c_int, 6);
pub const SQLITE_TESTCTRL_PRNG_RESET = @as(c_int, 7);
pub const SQLITE_TESTCTRL_FK_NO_ACTION = @as(c_int, 7);
pub const SQLITE_TESTCTRL_BITVEC_TEST = @as(c_int, 8);
pub const SQLITE_TESTCTRL_FAULT_INSTALL = @as(c_int, 9);
pub const SQLITE_TESTCTRL_BENIGN_MALLOC_HOOKS = @as(c_int, 10);
pub const SQLITE_TESTCTRL_PENDING_BYTE = @as(c_int, 11);
pub const SQLITE_TESTCTRL_ASSERT = @as(c_int, 12);
pub const SQLITE_TESTCTRL_ALWAYS = @as(c_int, 13);
pub const SQLITE_TESTCTRL_RESERVE = @as(c_int, 14);
pub const SQLITE_TESTCTRL_JSON_SELFCHECK = @as(c_int, 14);
pub const SQLITE_TESTCTRL_OPTIMIZATIONS = @as(c_int, 15);
pub const SQLITE_TESTCTRL_ISKEYWORD = @as(c_int, 16);
pub const SQLITE_TESTCTRL_SCRATCHMALLOC = @as(c_int, 17);
pub const SQLITE_TESTCTRL_INTERNAL_FUNCTIONS = @as(c_int, 17);
pub const SQLITE_TESTCTRL_LOCALTIME_FAULT = @as(c_int, 18);
pub const SQLITE_TESTCTRL_EXPLAIN_STMT = @as(c_int, 19);
pub const SQLITE_TESTCTRL_ONCE_RESET_THRESHOLD = @as(c_int, 19);
pub const SQLITE_TESTCTRL_NEVER_CORRUPT = @as(c_int, 20);
pub const SQLITE_TESTCTRL_VDBE_COVERAGE = @as(c_int, 21);
pub const SQLITE_TESTCTRL_BYTEORDER = @as(c_int, 22);
pub const SQLITE_TESTCTRL_ISINIT = @as(c_int, 23);
pub const SQLITE_TESTCTRL_SORTER_MMAP = @as(c_int, 24);
pub const SQLITE_TESTCTRL_IMPOSTER = @as(c_int, 25);
pub const SQLITE_TESTCTRL_PARSER_COVERAGE = @as(c_int, 26);
pub const SQLITE_TESTCTRL_RESULT_INTREAL = @as(c_int, 27);
pub const SQLITE_TESTCTRL_PRNG_SEED = @as(c_int, 28);
pub const SQLITE_TESTCTRL_EXTRA_SCHEMA_CHECKS = @as(c_int, 29);
pub const SQLITE_TESTCTRL_SEEK_COUNT = @as(c_int, 30);
pub const SQLITE_TESTCTRL_TRACEFLAGS = @as(c_int, 31);
pub const SQLITE_TESTCTRL_TUNE = @as(c_int, 32);
pub const SQLITE_TESTCTRL_LOGEST = @as(c_int, 33);
pub const SQLITE_TESTCTRL_USELONGDOUBLE = @as(c_int, 34);
pub const SQLITE_TESTCTRL_LAST = @as(c_int, 34);
pub const SQLITE_STATUS_MEMORY_USED = @as(c_int, 0);
pub const SQLITE_STATUS_PAGECACHE_USED = @as(c_int, 1);
pub const SQLITE_STATUS_PAGECACHE_OVERFLOW = @as(c_int, 2);
pub const SQLITE_STATUS_SCRATCH_USED = @as(c_int, 3);
pub const SQLITE_STATUS_SCRATCH_OVERFLOW = @as(c_int, 4);
pub const SQLITE_STATUS_MALLOC_SIZE = @as(c_int, 5);
pub const SQLITE_STATUS_PARSER_STACK = @as(c_int, 6);
pub const SQLITE_STATUS_PAGECACHE_SIZE = @as(c_int, 7);
pub const SQLITE_STATUS_SCRATCH_SIZE = @as(c_int, 8);
pub const SQLITE_STATUS_MALLOC_COUNT = @as(c_int, 9);
pub const SQLITE_DBSTATUS_LOOKASIDE_USED = @as(c_int, 0);
pub const SQLITE_DBSTATUS_CACHE_USED = @as(c_int, 1);
pub const SQLITE_DBSTATUS_SCHEMA_USED = @as(c_int, 2);
pub const SQLITE_DBSTATUS_STMT_USED = @as(c_int, 3);
pub const SQLITE_DBSTATUS_LOOKASIDE_HIT = @as(c_int, 4);
pub const SQLITE_DBSTATUS_LOOKASIDE_MISS_SIZE = @as(c_int, 5);
pub const SQLITE_DBSTATUS_LOOKASIDE_MISS_FULL = @as(c_int, 6);
pub const SQLITE_DBSTATUS_CACHE_HIT = @as(c_int, 7);
pub const SQLITE_DBSTATUS_CACHE_MISS = @as(c_int, 8);
pub const SQLITE_DBSTATUS_CACHE_WRITE = @as(c_int, 9);
pub const SQLITE_DBSTATUS_DEFERRED_FKS = @as(c_int, 10);
pub const SQLITE_DBSTATUS_CACHE_USED_SHARED = @as(c_int, 11);
pub const SQLITE_DBSTATUS_CACHE_SPILL = @as(c_int, 12);
pub const SQLITE_DBSTATUS_MAX = @as(c_int, 12);
pub const SQLITE_STMTSTATUS_FULLSCAN_STEP = @as(c_int, 1);
pub const SQLITE_STMTSTATUS_SORT = @as(c_int, 2);
pub const SQLITE_STMTSTATUS_AUTOINDEX = @as(c_int, 3);
pub const SQLITE_STMTSTATUS_VM_STEP = @as(c_int, 4);
pub const SQLITE_STMTSTATUS_REPREPARE = @as(c_int, 5);
pub const SQLITE_STMTSTATUS_RUN = @as(c_int, 6);
pub const SQLITE_STMTSTATUS_FILTER_MISS = @as(c_int, 7);
pub const SQLITE_STMTSTATUS_FILTER_HIT = @as(c_int, 8);
pub const SQLITE_STMTSTATUS_MEMUSED = @as(c_int, 99);
pub const SQLITE_CHECKPOINT_PASSIVE = @as(c_int, 0);
pub const SQLITE_CHECKPOINT_FULL = @as(c_int, 1);
pub const SQLITE_CHECKPOINT_RESTART = @as(c_int, 2);
pub const SQLITE_CHECKPOINT_TRUNCATE = @as(c_int, 3);
pub const SQLITE_VTAB_CONSTRAINT_SUPPORT = @as(c_int, 1);
pub const SQLITE_VTAB_INNOCUOUS = @as(c_int, 2);
pub const SQLITE_VTAB_DIRECTONLY = @as(c_int, 3);
pub const SQLITE_VTAB_USES_ALL_SCHEMAS = @as(c_int, 4);
pub const SQLITE_ROLLBACK = @as(c_int, 1);
pub const SQLITE_FAIL = @as(c_int, 3);
pub const SQLITE_REPLACE = @as(c_int, 5);
pub const SQLITE_SCANSTAT_NLOOP = @as(c_int, 0);
pub const SQLITE_SCANSTAT_NVISIT = @as(c_int, 1);
pub const SQLITE_SCANSTAT_EST = @as(c_int, 2);
pub const SQLITE_SCANSTAT_NAME = @as(c_int, 3);
pub const SQLITE_SCANSTAT_EXPLAIN = @as(c_int, 4);
pub const SQLITE_SCANSTAT_SELECTID = @as(c_int, 5);
pub const SQLITE_SCANSTAT_PARENTID = @as(c_int, 6);
pub const SQLITE_SCANSTAT_NCYCLE = @as(c_int, 7);
pub const SQLITE_SCANSTAT_COMPLEX = @as(c_int, 0x0001);
pub const SQLITE_SERIALIZE_NOCOPY = @as(c_int, 0x001);
pub const SQLITE_DESERIALIZE_FREEONCLOSE = @as(c_int, 1);
pub const SQLITE_DESERIALIZE_RESIZEABLE = @as(c_int, 2);
pub const SQLITE_DESERIALIZE_READONLY = @as(c_int, 4);
pub const _SQLITE3RTREE_H_ = "";
pub const NOT_WITHIN = @as(c_int, 0);
pub const PARTLY_WITHIN = @as(c_int, 1);
pub const FULLY_WITHIN = @as(c_int, 2);
pub const _FTS5_H = "";
pub const FTS5_TOKENIZE_QUERY = @as(c_int, 0x0001);
pub const FTS5_TOKENIZE_PREFIX = @as(c_int, 0x0002);
pub const FTS5_TOKENIZE_DOCUMENT = @as(c_int, 0x0004);
pub const FTS5_TOKENIZE_AUX = @as(c_int, 0x0008);
pub const FTS5_TOKEN_COLOCATED = @as(c_int, 0x0001);
pub const __CLANG_STDINT_H = "";
pub const _STDINT_H = @as(c_int, 1);
pub const __GLIBC_INTERNAL_STARTING_HEADER_IMPLEMENTATION = "";
pub const _FEATURES_H = @as(c_int, 1);
pub const __KERNEL_STRICT_NAMES = "";
pub inline fn __GNUC_PREREQ(maj: anytype, min: anytype) @TypeOf(((__GNUC__ << @as(c_int, 16)) + __GNUC_MINOR__) >= ((maj << @as(c_int, 16)) + min)) {
    _ = &maj;
    _ = &min;
    return ((__GNUC__ << @as(c_int, 16)) + __GNUC_MINOR__) >= ((maj << @as(c_int, 16)) + min);
}
pub inline fn __glibc_clang_prereq(maj: anytype, min: anytype) @TypeOf(((__clang_major__ << @as(c_int, 16)) + __clang_minor__) >= ((maj << @as(c_int, 16)) + min)) {
    _ = &maj;
    _ = &min;
    return ((__clang_major__ << @as(c_int, 16)) + __clang_minor__) >= ((maj << @as(c_int, 16)) + min);
}
pub const __GLIBC_USE = @compileError("unable to translate macro: undefined identifier `__GLIBC_USE_`");
// /usr/include/features.h:188:9
pub const _DEFAULT_SOURCE = @as(c_int, 1);
pub const __GLIBC_USE_ISOC2X = @as(c_int, 0);
pub const __USE_ISOC11 = @as(c_int, 1);
pub const __USE_ISOC99 = @as(c_int, 1);
pub const __USE_ISOC95 = @as(c_int, 1);
pub const __USE_POSIX_IMPLICITLY = @as(c_int, 1);
pub const _POSIX_SOURCE = @as(c_int, 1);
pub const _POSIX_C_SOURCE = @as(c_long, 200809);
pub const __USE_POSIX = @as(c_int, 1);
pub const __USE_POSIX2 = @as(c_int, 1);
pub const __USE_POSIX199309 = @as(c_int, 1);
pub const __USE_POSIX199506 = @as(c_int, 1);
pub const __USE_XOPEN2K = @as(c_int, 1);
pub const __USE_XOPEN2K8 = @as(c_int, 1);
pub const _ATFILE_SOURCE = @as(c_int, 1);
pub const __WORDSIZE = @as(c_int, 64);
pub const __WORDSIZE_TIME64_COMPAT32 = @as(c_int, 0);
pub const __TIMESIZE = @as(c_int, 64);
pub const __USE_MISC = @as(c_int, 1);
pub const __USE_ATFILE = @as(c_int, 1);
pub const __USE_FORTIFY_LEVEL = @as(c_int, 0);
pub const __GLIBC_USE_DEPRECATED_GETS = @as(c_int, 0);
pub const __GLIBC_USE_DEPRECATED_SCANF = @as(c_int, 0);
pub const __GLIBC_USE_C2X_STRTOL = @as(c_int, 0);
pub const _STDC_PREDEF_H = @as(c_int, 1);
pub const __STDC_IEC_559__ = @as(c_int, 1);
pub const __STDC_IEC_60559_BFP__ = @as(c_long, 201404);
pub const __STDC_IEC_559_COMPLEX__ = @as(c_int, 1);
pub const __STDC_IEC_60559_COMPLEX__ = @as(c_long, 201404);
pub const __STDC_ISO_10646__ = @as(c_long, 201706);
pub const __GNU_LIBRARY__ = @as(c_int, 6);
pub const __GLIBC__ = @as(c_int, 2);
pub inline fn __GLIBC_PREREQ(maj: anytype, min: anytype) @TypeOf(((__GLIBC__ << @as(c_int, 16)) + __GLIBC_MINOR__) >= ((maj << @as(c_int, 16)) + min)) {
    _ = &maj;
    _ = &min;
    return ((__GLIBC__ << @as(c_int, 16)) + __GLIBC_MINOR__) >= ((maj << @as(c_int, 16)) + min);
}
pub const _SYS_CDEFS_H = @as(c_int, 1);
pub const __glibc_has_attribute = @compileError("unable to translate macro: undefined identifier `__has_attribute`");
// /usr/include/aarch64-linux-gnu/sys/cdefs.h:45:10
pub inline fn __glibc_has_builtin(name: anytype) @TypeOf(__has_builtin(name)) {
    _ = &name;
    return __has_builtin(name);
}
pub const __glibc_has_extension = @compileError("unable to translate macro: undefined identifier `__has_extension`");
// /usr/include/aarch64-linux-gnu/sys/cdefs.h:55:10
pub const __LEAF = "";
pub const __LEAF_ATTR = "";
pub const __THROW = @compileError("unable to translate macro: undefined identifier `__nothrow__`");
// /usr/include/aarch64-linux-gnu/sys/cdefs.h:79:11
pub const __THROWNL = @compileError("unable to translate macro: undefined identifier `__nothrow__`");
// /usr/include/aarch64-linux-gnu/sys/cdefs.h:80:11
pub const __NTH = @compileError("unable to translate macro: undefined identifier `__nothrow__`");
// /usr/include/aarch64-linux-gnu/sys/cdefs.h:81:11
pub const __NTHNL = @compileError("unable to translate macro: undefined identifier `__nothrow__`");
// /usr/include/aarch64-linux-gnu/sys/cdefs.h:82:11
pub const __COLD = @compileError("unable to translate macro: undefined identifier `__cold__`");
// /usr/include/aarch64-linux-gnu/sys/cdefs.h:102:11
pub inline fn __P(args: anytype) @TypeOf(args) {
    _ = &args;
    return args;
}
pub inline fn __PMT(args: anytype) @TypeOf(args) {
    _ = &args;
    return args;
}
pub const __CONCAT = @compileError("unable to translate C expr: unexpected token '##'");
// /usr/include/aarch64-linux-gnu/sys/cdefs.h:131:9
pub const __STRING = @compileError("unable to translate C expr: unexpected token '#'");
// /usr/include/aarch64-linux-gnu/sys/cdefs.h:132:9
pub const __ptr_t = ?*anyopaque;
pub const __BEGIN_DECLS = "";
pub const __END_DECLS = "";
pub inline fn __bos(ptr: anytype) @TypeOf(__builtin_object_size(ptr, __USE_FORTIFY_LEVEL > @as(c_int, 1))) {
    _ = &ptr;
    return __builtin_object_size(ptr, __USE_FORTIFY_LEVEL > @as(c_int, 1));
}
pub inline fn __bos0(ptr: anytype) @TypeOf(__builtin_object_size(ptr, @as(c_int, 0))) {
    _ = &ptr;
    return __builtin_object_size(ptr, @as(c_int, 0));
}
pub inline fn __glibc_objsize0(__o: anytype) @TypeOf(__bos0(__o)) {
    _ = &__o;
    return __bos0(__o);
}
pub inline fn __glibc_objsize(__o: anytype) @TypeOf(__bos(__o)) {
    _ = &__o;
    return __bos(__o);
}
pub const __warnattr = @compileError("unable to translate C expr: unexpected token ''");
// /usr/include/aarch64-linux-gnu/sys/cdefs.h:216:10
pub const __errordecl = @compileError("unable to translate C expr: unexpected token 'extern'");
// /usr/include/aarch64-linux-gnu/sys/cdefs.h:217:10
pub const __flexarr = @compileError("unable to translate C expr: unexpected token '['");
// /usr/include/aarch64-linux-gnu/sys/cdefs.h:225:10
pub const __glibc_c99_flexarr_available = @as(c_int, 1);
pub const __REDIRECT = @compileError("unable to translate C expr: unexpected token '__asm__'");
// /usr/include/aarch64-linux-gnu/sys/cdefs.h:256:10
pub const __REDIRECT_NTH = @compileError("unable to translate C expr: unexpected token '__asm__'");
// /usr/include/aarch64-linux-gnu/sys/cdefs.h:263:11
pub const __REDIRECT_NTHNL = @compileError("unable to translate C expr: unexpected token '__asm__'");
// /usr/include/aarch64-linux-gnu/sys/cdefs.h:265:11
pub const __ASMNAME = @compileError("unable to translate C expr: unexpected token ','");
// /usr/include/aarch64-linux-gnu/sys/cdefs.h:268:10
pub inline fn __ASMNAME2(prefix: anytype, cname: anytype) @TypeOf(__STRING(prefix) ++ cname) {
    _ = &prefix;
    _ = &cname;
    return __STRING(prefix) ++ cname;
}
pub const __REDIRECT_FORTIFY = __REDIRECT;
pub const __REDIRECT_FORTIFY_NTH = __REDIRECT_NTH;
pub const __attribute_malloc__ = @compileError("unable to translate macro: undefined identifier `__malloc__`");
// /usr/include/aarch64-linux-gnu/sys/cdefs.h:298:10
pub const __attribute_alloc_size__ = @compileError("unable to translate C expr: unexpected token ''");
// /usr/include/aarch64-linux-gnu/sys/cdefs.h:309:10
pub const __attribute_alloc_align__ = @compileError("unable to translate macro: undefined identifier `__alloc_align__`");
// /usr/include/aarch64-linux-gnu/sys/cdefs.h:315:10
pub const __attribute_pure__ = @compileError("unable to translate macro: undefined identifier `__pure__`");
// /usr/include/aarch64-linux-gnu/sys/cdefs.h:325:10
pub const __attribute_const__ = @compileError("unable to translate C expr: unexpected token '__attribute__'");
// /usr/include/aarch64-linux-gnu/sys/cdefs.h:332:10
pub const __attribute_maybe_unused__ = @compileError("unable to translate macro: undefined identifier `__unused__`");
// /usr/include/aarch64-linux-gnu/sys/cdefs.h:338:10
pub const __attribute_used__ = @compileError("unable to translate macro: undefined identifier `__used__`");
// /usr/include/aarch64-linux-gnu/sys/cdefs.h:347:10
pub const __attribute_noinline__ = @compileError("unable to translate macro: undefined identifier `__noinline__`");
// /usr/include/aarch64-linux-gnu/sys/cdefs.h:348:10
pub const __attribute_deprecated__ = @compileError("unable to translate macro: undefined identifier `__deprecated__`");
// /usr/include/aarch64-linux-gnu/sys/cdefs.h:356:10
pub const __attribute_deprecated_msg__ = @compileError("unable to translate macro: undefined identifier `__deprecated__`");
// /usr/include/aarch64-linux-gnu/sys/cdefs.h:366:10
pub const __attribute_format_arg__ = @compileError("unable to translate macro: undefined identifier `__format_arg__`");
// /usr/include/aarch64-linux-gnu/sys/cdefs.h:379:10
pub const __attribute_format_strfmon__ = @compileError("unable to translate macro: undefined identifier `__format__`");
// /usr/include/aarch64-linux-gnu/sys/cdefs.h:389:10
pub const __attribute_nonnull__ = @compileError("unable to translate macro: undefined identifier `__nonnull__`");
// /usr/include/aarch64-linux-gnu/sys/cdefs.h:401:11
pub inline fn __nonnull(params: anytype) @TypeOf(__attribute_nonnull__(params)) {
    _ = &params;
    return __attribute_nonnull__(params);
}
pub const __returns_nonnull = @compileError("unable to translate macro: undefined identifier `__returns_nonnull__`");
// /usr/include/aarch64-linux-gnu/sys/cdefs.h:414:10
pub const __attribute_warn_unused_result__ = @compileError("unable to translate macro: undefined identifier `__warn_unused_result__`");
// /usr/include/aarch64-linux-gnu/sys/cdefs.h:423:10
pub const __wur = "";
pub const __always_inline = @compileError("unable to translate macro: undefined identifier `__always_inline__`");
// /usr/include/aarch64-linux-gnu/sys/cdefs.h:441:10
pub const __attribute_artificial__ = @compileError("unable to translate macro: undefined identifier `__artificial__`");
// /usr/include/aarch64-linux-gnu/sys/cdefs.h:450:10
pub const __extern_inline = @compileError("unable to translate macro: undefined identifier `__gnu_inline__`");
// /usr/include/aarch64-linux-gnu/sys/cdefs.h:468:11
pub const __extern_always_inline = @compileError("unable to translate macro: undefined identifier `__gnu_inline__`");
// /usr/include/aarch64-linux-gnu/sys/cdefs.h:469:11
pub const __fortify_function = __extern_always_inline ++ __attribute_artificial__;
pub const __restrict_arr = @compileError("unable to translate C expr: unexpected token '__restrict'");
// /usr/include/aarch64-linux-gnu/sys/cdefs.h:512:10
pub inline fn __glibc_unlikely(cond: anytype) @TypeOf(__builtin_expect(cond, @as(c_int, 0))) {
    _ = &cond;
    return __builtin_expect(cond, @as(c_int, 0));
}
pub inline fn __glibc_likely(cond: anytype) @TypeOf(__builtin_expect(cond, @as(c_int, 1))) {
    _ = &cond;
    return __builtin_expect(cond, @as(c_int, 1));
}
pub const __attribute_nonstring__ = "";
pub const __attribute_copy__ = @compileError("unable to translate C expr: unexpected token ''");
// /usr/include/aarch64-linux-gnu/sys/cdefs.h:561:10
pub const __LDOUBLE_REDIRECTS_TO_FLOAT128_ABI = @as(c_int, 0);
pub inline fn __LDBL_REDIR1(name: anytype, proto: anytype, alias: anytype) @TypeOf(name ++ proto) {
    _ = &name;
    _ = &proto;
    _ = &alias;
    return name ++ proto;
}
pub inline fn __LDBL_REDIR(name: anytype, proto: anytype) @TypeOf(name ++ proto) {
    _ = &name;
    _ = &proto;
    return name ++ proto;
}
pub inline fn __LDBL_REDIR1_NTH(name: anytype, proto: anytype, alias: anytype) @TypeOf(name ++ proto ++ __THROW) {
    _ = &name;
    _ = &proto;
    _ = &alias;
    return name ++ proto ++ __THROW;
}
pub inline fn __LDBL_REDIR_NTH(name: anytype, proto: anytype) @TypeOf(name ++ proto ++ __THROW) {
    _ = &name;
    _ = &proto;
    return name ++ proto ++ __THROW;
}
pub const __LDBL_REDIR2_DECL = @compileError("unable to translate C expr: unexpected token ''");
// /usr/include/aarch64-linux-gnu/sys/cdefs.h:638:10
pub const __LDBL_REDIR_DECL = @compileError("unable to translate C expr: unexpected token ''");
// /usr/include/aarch64-linux-gnu/sys/cdefs.h:639:10
pub inline fn __REDIRECT_LDBL(name: anytype, proto: anytype, alias: anytype) @TypeOf(__REDIRECT(name, proto, alias)) {
    _ = &name;
    _ = &proto;
    _ = &alias;
    return __REDIRECT(name, proto, alias);
}
pub inline fn __REDIRECT_NTH_LDBL(name: anytype, proto: anytype, alias: anytype) @TypeOf(__REDIRECT_NTH(name, proto, alias)) {
    _ = &name;
    _ = &proto;
    _ = &alias;
    return __REDIRECT_NTH(name, proto, alias);
}
pub const __glibc_macro_warning1 = @compileError("unable to translate macro: undefined identifier `_Pragma`");
// /usr/include/aarch64-linux-gnu/sys/cdefs.h:653:10
pub const __glibc_macro_warning = @compileError("unable to translate macro: undefined identifier `GCC`");
// /usr/include/aarch64-linux-gnu/sys/cdefs.h:654:10
pub const __HAVE_GENERIC_SELECTION = @as(c_int, 1);
pub const __fortified_attr_access = @compileError("unable to translate C expr: unexpected token ''");
// /usr/include/aarch64-linux-gnu/sys/cdefs.h:699:11
pub const __attr_access = @compileError("unable to translate C expr: unexpected token ''");
// /usr/include/aarch64-linux-gnu/sys/cdefs.h:700:11
pub const __attr_access_none = @compileError("unable to translate C expr: unexpected token ''");
// /usr/include/aarch64-linux-gnu/sys/cdefs.h:701:11
pub const __attr_dealloc = @compileError("unable to translate C expr: unexpected token ''");
// /usr/include/aarch64-linux-gnu/sys/cdefs.h:711:10
pub const __attr_dealloc_free = "";
pub const __attribute_returns_twice__ = @compileError("unable to translate macro: undefined identifier `__returns_twice__`");
// /usr/include/aarch64-linux-gnu/sys/cdefs.h:718:10
pub const __stub___compat_bdflush = "";
pub const __stub___compat_create_module = "";
pub const __stub___compat_get_kernel_syms = "";
pub const __stub___compat_query_module = "";
pub const __stub___compat_uselib = "";
pub const __stub_chflags = "";
pub const __stub_fchflags = "";
pub const __stub_gtty = "";
pub const __stub_revoke = "";
pub const __stub_setlogin = "";
pub const __stub_sigreturn = "";
pub const __stub_stty = "";
pub const __GLIBC_USE_LIB_EXT2 = @as(c_int, 0);
pub const __GLIBC_USE_IEC_60559_BFP_EXT = @as(c_int, 0);
pub const __GLIBC_USE_IEC_60559_BFP_EXT_C2X = @as(c_int, 0);
pub const __GLIBC_USE_IEC_60559_EXT = @as(c_int, 0);
pub const __GLIBC_USE_IEC_60559_FUNCS_EXT = @as(c_int, 0);
pub const __GLIBC_USE_IEC_60559_FUNCS_EXT_C2X = @as(c_int, 0);
pub const __GLIBC_USE_IEC_60559_TYPES_EXT = @as(c_int, 0);
pub const _BITS_TYPES_H = @as(c_int, 1);
pub const __S16_TYPE = c_short;
pub const __U16_TYPE = c_ushort;
pub const __S32_TYPE = c_int;
pub const __U32_TYPE = c_uint;
pub const __SLONGWORD_TYPE = c_long;
pub const __ULONGWORD_TYPE = c_ulong;
pub const __SQUAD_TYPE = c_long;
pub const __UQUAD_TYPE = c_ulong;
pub const __SWORD_TYPE = c_long;
pub const __UWORD_TYPE = c_ulong;
pub const __SLONG32_TYPE = c_int;
pub const __ULONG32_TYPE = c_uint;
pub const __S64_TYPE = c_long;
pub const __U64_TYPE = c_ulong;
pub const __STD_TYPE = @compileError("unable to translate C expr: unexpected token 'typedef'");
// /usr/include/aarch64-linux-gnu/bits/types.h:137:10
pub const _BITS_TYPESIZES_H = @as(c_int, 1);
pub const __INO_T_TYPE = __ULONGWORD_TYPE;
pub const __OFF_T_TYPE = __SLONGWORD_TYPE;
pub const __RLIM_T_TYPE = __ULONGWORD_TYPE;
pub const __BLKCNT_T_TYPE = __SLONGWORD_TYPE;
pub const __FSBLKCNT_T_TYPE = __ULONGWORD_TYPE;
pub const __FSFILCNT_T_TYPE = __ULONGWORD_TYPE;
pub const __TIME_T_TYPE = __SLONGWORD_TYPE;
pub const __SUSECONDS_T_TYPE = __SLONGWORD_TYPE;
pub const __DEV_T_TYPE = __UQUAD_TYPE;
pub const __UID_T_TYPE = __U32_TYPE;
pub const __GID_T_TYPE = __U32_TYPE;
pub const __INO64_T_TYPE = __UQUAD_TYPE;
pub const __MODE_T_TYPE = __U32_TYPE;
pub const __NLINK_T_TYPE = __U32_TYPE;
pub const __OFF64_T_TYPE = __SQUAD_TYPE;
pub const __PID_T_TYPE = __S32_TYPE;
pub const __RLIM64_T_TYPE = __UQUAD_TYPE;
pub const __BLKCNT64_T_TYPE = __SQUAD_TYPE;
pub const __FSBLKCNT64_T_TYPE = __UQUAD_TYPE;
pub const __FSFILCNT64_T_TYPE = __UQUAD_TYPE;
pub const __FSWORD_T_TYPE = __SWORD_TYPE;
pub const __ID_T_TYPE = __U32_TYPE;
pub const __CLOCK_T_TYPE = __SLONGWORD_TYPE;
pub const __USECONDS_T_TYPE = __U32_TYPE;
pub const __SUSECONDS64_T_TYPE = __SQUAD_TYPE;
pub const __DADDR_T_TYPE = __S32_TYPE;
pub const __KEY_T_TYPE = __S32_TYPE;
pub const __CLOCKID_T_TYPE = __S32_TYPE;
pub const __TIMER_T_TYPE = ?*anyopaque;
pub const __BLKSIZE_T_TYPE = __S32_TYPE;
pub const __FSID_T_TYPE = @compileError("unable to translate macro: undefined identifier `__val`");
// /usr/include/aarch64-linux-gnu/bits/typesizes.h:72:9
pub const __SSIZE_T_TYPE = __SWORD_TYPE;
pub const __SYSCALL_SLONG_TYPE = __SLONGWORD_TYPE;
pub const __SYSCALL_ULONG_TYPE = __ULONGWORD_TYPE;
pub const __CPU_MASK_TYPE = __ULONGWORD_TYPE;
pub const __OFF_T_MATCHES_OFF64_T = @as(c_int, 1);
pub const __INO_T_MATCHES_INO64_T = @as(c_int, 1);
pub const __RLIM_T_MATCHES_RLIM64_T = @as(c_int, 1);
pub const __STATFS_MATCHES_STATFS64 = @as(c_int, 1);
pub const __KERNEL_OLD_TIMEVAL_MATCHES_TIMEVAL64 = __WORDSIZE == @as(c_int, 64);
pub const __FD_SETSIZE = @as(c_int, 1024);
pub const _BITS_TIME64_H = @as(c_int, 1);
pub const __TIME64_T_TYPE = __TIME_T_TYPE;
pub const _BITS_WCHAR_H = @as(c_int, 1);
pub const __WCHAR_MAX = __WCHAR_MAX__;
pub const __WCHAR_MIN = '\x00' + @as(c_int, 0);
pub const _BITS_STDINT_INTN_H = @as(c_int, 1);
pub const _BITS_STDINT_UINTN_H = @as(c_int, 1);
pub const _BITS_STDINT_LEAST_H = @as(c_int, 1);
pub const __intptr_t_defined = "";
pub const __INT64_C = @import("std").zig.c_translation.Macros.L_SUFFIX;
pub const __UINT64_C = @import("std").zig.c_translation.Macros.UL_SUFFIX;
pub const INT8_MIN = -@as(c_int, 128);
pub const INT16_MIN = -@as(c_int, 32767) - @as(c_int, 1);
pub const INT32_MIN = -@import("std").zig.c_translation.promoteIntLiteral(c_int, 2147483647, .decimal) - @as(c_int, 1);
pub const INT64_MIN = -__INT64_C(@import("std").zig.c_translation.promoteIntLiteral(c_int, 9223372036854775807, .decimal)) - @as(c_int, 1);
pub const INT8_MAX = @as(c_int, 127);
pub const INT16_MAX = @as(c_int, 32767);
pub const INT32_MAX = @import("std").zig.c_translation.promoteIntLiteral(c_int, 2147483647, .decimal);
pub const INT64_MAX = __INT64_C(@import("std").zig.c_translation.promoteIntLiteral(c_int, 9223372036854775807, .decimal));
pub const UINT8_MAX = @as(c_int, 255);
pub const UINT16_MAX = @import("std").zig.c_translation.promoteIntLiteral(c_int, 65535, .decimal);
pub const UINT32_MAX = @import("std").zig.c_translation.promoteIntLiteral(c_uint, 4294967295, .decimal);
pub const UINT64_MAX = __UINT64_C(@import("std").zig.c_translation.promoteIntLiteral(c_int, 18446744073709551615, .decimal));
pub const INT_LEAST8_MIN = -@as(c_int, 128);
pub const INT_LEAST16_MIN = -@as(c_int, 32767) - @as(c_int, 1);
pub const INT_LEAST32_MIN = -@import("std").zig.c_translation.promoteIntLiteral(c_int, 2147483647, .decimal) - @as(c_int, 1);
pub const INT_LEAST64_MIN = -__INT64_C(@import("std").zig.c_translation.promoteIntLiteral(c_int, 9223372036854775807, .decimal)) - @as(c_int, 1);
pub const INT_LEAST8_MAX = @as(c_int, 127);
pub const INT_LEAST16_MAX = @as(c_int, 32767);
pub const INT_LEAST32_MAX = @import("std").zig.c_translation.promoteIntLiteral(c_int, 2147483647, .decimal);
pub const INT_LEAST64_MAX = __INT64_C(@import("std").zig.c_translation.promoteIntLiteral(c_int, 9223372036854775807, .decimal));
pub const UINT_LEAST8_MAX = @as(c_int, 255);
pub const UINT_LEAST16_MAX = @import("std").zig.c_translation.promoteIntLiteral(c_int, 65535, .decimal);
pub const UINT_LEAST32_MAX = @import("std").zig.c_translation.promoteIntLiteral(c_uint, 4294967295, .decimal);
pub const UINT_LEAST64_MAX = __UINT64_C(@import("std").zig.c_translation.promoteIntLiteral(c_int, 18446744073709551615, .decimal));
pub const INT_FAST8_MIN = -@as(c_int, 128);
pub const INT_FAST16_MIN = -@import("std").zig.c_translation.promoteIntLiteral(c_long, 9223372036854775807, .decimal) - @as(c_int, 1);
pub const INT_FAST32_MIN = -@import("std").zig.c_translation.promoteIntLiteral(c_long, 9223372036854775807, .decimal) - @as(c_int, 1);
pub const INT_FAST64_MIN = -__INT64_C(@import("std").zig.c_translation.promoteIntLiteral(c_int, 9223372036854775807, .decimal)) - @as(c_int, 1);
pub const INT_FAST8_MAX = @as(c_int, 127);
pub const INT_FAST16_MAX = @import("std").zig.c_translation.promoteIntLiteral(c_long, 9223372036854775807, .decimal);
pub const INT_FAST32_MAX = @import("std").zig.c_translation.promoteIntLiteral(c_long, 9223372036854775807, .decimal);
pub const INT_FAST64_MAX = __INT64_C(@import("std").zig.c_translation.promoteIntLiteral(c_int, 9223372036854775807, .decimal));
pub const UINT_FAST8_MAX = @as(c_int, 255);
pub const UINT_FAST16_MAX = @import("std").zig.c_translation.promoteIntLiteral(c_ulong, 18446744073709551615, .decimal);
pub const UINT_FAST32_MAX = @import("std").zig.c_translation.promoteIntLiteral(c_ulong, 18446744073709551615, .decimal);
pub const UINT_FAST64_MAX = __UINT64_C(@import("std").zig.c_translation.promoteIntLiteral(c_int, 18446744073709551615, .decimal));
pub const INTPTR_MIN = -@import("std").zig.c_translation.promoteIntLiteral(c_long, 9223372036854775807, .decimal) - @as(c_int, 1);
pub const INTPTR_MAX = @import("std").zig.c_translation.promoteIntLiteral(c_long, 9223372036854775807, .decimal);
pub const UINTPTR_MAX = @import("std").zig.c_translation.promoteIntLiteral(c_ulong, 18446744073709551615, .decimal);
pub const INTMAX_MIN = -__INT64_C(@import("std").zig.c_translation.promoteIntLiteral(c_int, 9223372036854775807, .decimal)) - @as(c_int, 1);
pub const INTMAX_MAX = __INT64_C(@import("std").zig.c_translation.promoteIntLiteral(c_int, 9223372036854775807, .decimal));
pub const UINTMAX_MAX = __UINT64_C(@import("std").zig.c_translation.promoteIntLiteral(c_int, 18446744073709551615, .decimal));
pub const PTRDIFF_MIN = -@import("std").zig.c_translation.promoteIntLiteral(c_long, 9223372036854775807, .decimal) - @as(c_int, 1);
pub const PTRDIFF_MAX = @import("std").zig.c_translation.promoteIntLiteral(c_long, 9223372036854775807, .decimal);
pub const SIG_ATOMIC_MIN = -@import("std").zig.c_translation.promoteIntLiteral(c_int, 2147483647, .decimal) - @as(c_int, 1);
pub const SIG_ATOMIC_MAX = @import("std").zig.c_translation.promoteIntLiteral(c_int, 2147483647, .decimal);
pub const SIZE_MAX = @import("std").zig.c_translation.promoteIntLiteral(c_ulong, 18446744073709551615, .decimal);
pub const WCHAR_MIN = __WCHAR_MIN;
pub const WCHAR_MAX = __WCHAR_MAX;
pub const WINT_MIN = @as(c_uint, 0);
pub const WINT_MAX = @import("std").zig.c_translation.promoteIntLiteral(c_uint, 4294967295, .decimal);
pub inline fn INT8_C(c: anytype) @TypeOf(c) {
    _ = &c;
    return c;
}
pub inline fn INT16_C(c: anytype) @TypeOf(c) {
    _ = &c;
    return c;
}
pub inline fn INT32_C(c: anytype) @TypeOf(c) {
    _ = &c;
    return c;
}
pub const INT64_C = @import("std").zig.c_translation.Macros.L_SUFFIX;
pub inline fn UINT8_C(c: anytype) @TypeOf(c) {
    _ = &c;
    return c;
}
pub inline fn UINT16_C(c: anytype) @TypeOf(c) {
    _ = &c;
    return c;
}
pub const UINT32_C = @import("std").zig.c_translation.Macros.U_SUFFIX;
pub const UINT64_C = @import("std").zig.c_translation.Macros.UL_SUFFIX;
pub const INTMAX_C = @import("std").zig.c_translation.Macros.L_SUFFIX;
pub const UINTMAX_C = @import("std").zig.c_translation.Macros.UL_SUFFIX;
pub const __STDBOOL_H = "";
pub const __bool_true_false_are_defined = @as(c_int, 1);
pub const @"bool" = bool;
pub const @"true" = @as(c_int, 1);
pub const @"false" = @as(c_int, 0);
pub const __STDDEF_H = "";
pub const __need_ptrdiff_t = "";
pub const __need_size_t = "";
pub const __need_wchar_t = "";
pub const __need_NULL = "";
pub const __need_max_align_t = "";
pub const __need_offsetof = "";
pub const _PTRDIFF_T = "";
pub const _SIZE_T = "";
pub const _WCHAR_T = "";
pub const NULL = @import("std").zig.c_translation.cast(?*anyopaque, @as(c_int, 0));
pub const __CLANG_MAX_ALIGN_T_DEFINED = "";
pub const offsetof = @compileError("unable to translate C expr: unexpected token 'an identifier'");
// /home/gedgar/zig-linux-aarch64-0.13.0/lib/include/__stddef_offsetof.h:16:9
pub const KB_MAX_COLUMNS = @as(c_int, 32);
pub const KB_PARAM_NULL = @import("std").mem.zeroInit(kb_bind_param_t, .{
    .type = KB_BIND_NULL,
});
pub const KB_PARAM_TEXT = @compileError("unable to translate C expr: expected '=' instead got '.'");
// /home/gedgar/knowledge_base_assembly/zig_programs_and_containers/building_blocks/knowledge_base/sqlite3/data_structures/data_structures_c/include/kb_common.h:71:9
pub const KB_PARAM_INT = @compileError("unable to translate C expr: expected '=' instead got '.'");
// /home/gedgar/knowledge_base_assembly/zig_programs_and_containers/building_blocks/knowledge_base/sqlite3/data_structures/data_structures_c/include/kb_common.h:72:9
pub const KB_PARAM_INT64 = @compileError("unable to translate C expr: expected '=' instead got '.'");
// /home/gedgar/knowledge_base_assembly/zig_programs_and_containers/building_blocks/knowledge_base/sqlite3/data_structures/data_structures_c/include/kb_common.h:73:9
pub const KB_PARAM_DOUBLE = @compileError("unable to translate C expr: expected '=' instead got '.'");
// /home/gedgar/knowledge_base_assembly/zig_programs_and_containers/building_blocks/knowledge_base/sqlite3/data_structures/data_structures_c/include/kb_common.h:74:9
pub const KB_QUERY_SUPPORT_H = "";
pub const KB_MAX_FILTERS = @as(c_int, 16);
pub const KB_BIT_STRUCTURES_H = "";
pub const BIT_MASK_RT_OPERATIONS_H = "";
pub const BIT_S_EXPRESSION_H = "";
pub const KB_STATUS_TABLE_H = "";
pub const KB_STREAM_H = "";
pub const KB_JOB_QUEUE_H = "";
pub const KB_LINK_TABLE_H = "";
pub const KB_LINK_MOUNT_TABLE_H = "";
pub const KB_RPC_SERVER_H = "";
pub const KB_RPC_CLIENT_H = "";
pub const KB_UUID_H = "";
pub const KB_UUID_LEN = @as(c_int, 37);
pub const KB_JSON_H = "";
pub const cJSON__h = "";
pub const CJSON_CDECL = "";
pub const CJSON_STDCALL = "";
pub inline fn CJSON_PUBLIC(@"type": anytype) @TypeOf(@"type") {
    _ = &@"type";
    return @"type";
}
pub const CJSON_VERSION_MAJOR = @as(c_int, 1);
pub const CJSON_VERSION_MINOR = @as(c_int, 7);
pub const CJSON_VERSION_PATCH = @as(c_int, 17);
pub const cJSON_Invalid = @as(c_int, 0);
pub const cJSON_False = @as(c_int, 1) << @as(c_int, 0);
pub const cJSON_True = @as(c_int, 1) << @as(c_int, 1);
pub const cJSON_NULL = @as(c_int, 1) << @as(c_int, 2);
pub const cJSON_Number = @as(c_int, 1) << @as(c_int, 3);
pub const cJSON_String = @as(c_int, 1) << @as(c_int, 4);
pub const cJSON_Array = @as(c_int, 1) << @as(c_int, 5);
pub const cJSON_Object = @as(c_int, 1) << @as(c_int, 6);
pub const cJSON_Raw = @as(c_int, 1) << @as(c_int, 7);
pub const cJSON_IsReference = @as(c_int, 256);
pub const cJSON_StringIsConst = @as(c_int, 512);
pub const CJSON_NESTING_LIMIT = @as(c_int, 1000);
pub const cJSON_SetIntValue = @compileError("unable to translate C expr: expected ':' instead got '='");
// /usr/include/cjson/cJSON.h:275:9
pub inline fn cJSON_SetNumberValue(object: anytype, number: anytype) @TypeOf(if (object != NULL) cJSON_SetNumberHelper(object, @import("std").zig.c_translation.cast(f64, number)) else number) {
    _ = &object;
    _ = &number;
    return if (object != NULL) cJSON_SetNumberHelper(object, @import("std").zig.c_translation.cast(f64, number)) else number;
}
pub const cJSON_SetBoolValue = @compileError("unable to translate C expr: expected ':' instead got '='");
// /usr/include/cjson/cJSON.h:283:9
pub const cJSON_ArrayForEach = @compileError("unable to translate C expr: unexpected token 'for'");
// /usr/include/cjson/cJSON.h:290:9
pub const kb_search = struct_kb_search;
pub const kb_bit_mask_ops = struct_kb_bit_mask_ops;
pub const kb_bit_structures = struct_kb_bit_structures;
pub const kb_status_table = struct_kb_status_table;
pub const kb_stream = struct_kb_stream;
pub const kb_job_queue = struct_kb_job_queue;
pub const kb_link_table = struct_kb_link_table;
pub const kb_link_mount_table = struct_kb_link_mount_table;
pub const kb_rpc_server = struct_kb_rpc_server;
pub const kb_rpc_client = struct_kb_rpc_client;
pub const kb_data_structures = struct_kb_data_structures;
