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
pub extern fn setup_abort_handler() void;
pub extern fn cfl_exception_handler(file: [*c]const u8, func: [*c]const u8, line: u16, msg: [*c]const u8) void;
pub const CflHeapStats = extern struct {
    total_allocations: u16 = @import("std").mem.zeroes(u16),
    total_frees: u16 = @import("std").mem.zeroes(u16),
    current_blocks: u16 = @import("std").mem.zeroes(u16),
    current_used_bytes: u16 = @import("std").mem.zeroes(u16),
    peak_used_bytes: u16 = @import("std").mem.zeroes(u16),
    largest_free_block: u16 = @import("std").mem.zeroes(u16),
    free_blocks: u16 = @import("std").mem.zeroes(u16),
    allocated_blocks: u16 = @import("std").mem.zeroes(u16),
};
pub const struct_CflHeap = extern struct {
    pool: [*c]u8 = @import("std").mem.zeroes([*c]u8),
    pool_size: u16 = @import("std").mem.zeroes(u16),
    initialized: bool = @import("std").mem.zeroes(bool),
    owns_pool: bool = @import("std").mem.zeroes(bool),
    stats: CflHeapStats = @import("std").mem.zeroes(CflHeapStats),
};
pub const CflHeap = struct_CflHeap;
pub const cfl_heap_t = struct_CflHeap;
pub const struct_CflPerm = extern struct {
    pool: [*c]u8 = @import("std").mem.zeroes([*c]u8),
    pool_size: u16 = @import("std").mem.zeroes(u16),
    used: u16 = @import("std").mem.zeroes(u16),
    initialized: bool = @import("std").mem.zeroes(bool),
    owns_pool: bool = @import("std").mem.zeroes(bool),
    stats: CflPermStats = @import("std").mem.zeroes(CflPermStats),
};
pub extern fn cfl_heap_init(perm: [*c]struct_CflPerm, buffer_size: u16) [*c]CflHeap;
pub extern fn cfl_heap_reset(heap: [*c]CflHeap) void;
pub extern fn cfl_heap_malloc(heap: [*c]CflHeap, size_bytes: u16) u16;
pub extern fn cfl_heap_free(heap: [*c]CflHeap, idx: u16) void;
pub extern fn cfl_heap_ptr(heap: [*c]CflHeap, idx: u16) ?*anyopaque;
pub extern fn cfl_heap_ptr_to_idx(heap: [*c]CflHeap, ptr: ?*anyopaque) u16;
pub extern fn cfl_heap_malloc_pointer(heap: [*c]CflHeap, size_bytes: u16) ?*anyopaque;
pub extern fn cfl_heap_free_pointer(heap: [*c]CflHeap, ptr: ?*anyopaque) void;
pub extern fn cfl_heap_arena_alloc_aligned(heap: [*c]CflHeap, requesting_node_id: u16, size_bytes: u16, alignment: u16) u16;
pub extern fn cfl_heap_used_bytes(heap: [*c]CflHeap) u16;
pub extern fn cfl_heap_free_bytes(heap: [*c]CflHeap) u16;
pub extern fn cfl_heap_get_stats(heap: [*c]CflHeap, stats: [*c]CflHeapStats) void;
pub extern fn cfl_heap_dump_stats(heap: [*c]CflHeap) void;
pub extern fn cfl_heap_validate(heap: [*c]CflHeap) bool;
pub extern fn cfl_heap_walk(heap: [*c]CflHeap, callback: ?*const fn (?*anyopaque, u16, bool, u16) callconv(.C) void) void;
pub extern fn cfl_heap_get_node_id(heap: [*c]CflHeap, idx: u16) u16;
pub const cfl_heap_allocator_id_t = u8;
pub const struct_CflHeapArenaControl = opaque {};
pub const CflHeapArenaControl = struct_CflHeapArenaControl;
pub const struct_CflHeapArenaStats = extern struct {
    active_count: u32 = @import("std").mem.zeroes(u32),
    total_data_allocated: u32 = @import("std").mem.zeroes(u32),
    total_data_used: u32 = @import("std").mem.zeroes(u32),
};
pub const CflHeapArenaStats = struct_CflHeapArenaStats;
pub const struct_CflHeapArenaSystem = extern struct {
    heap: [*c]cfl_heap_t = @import("std").mem.zeroes([*c]cfl_heap_t),
    max_allocator_count: u16 = @import("std").mem.zeroes(u16),
    control_blocks: ?*CflHeapArenaControl = @import("std").mem.zeroes(?*CflHeapArenaControl),
    arenas: [*c]?*CflHeapArenaControl = @import("std").mem.zeroes([*c]?*CflHeapArenaControl),
    node_allocator_ids: [*c]u8 = @import("std").mem.zeroes([*c]u8),
    node_memory_index: [*c]u16 = @import("std").mem.zeroes([*c]u16),
    total_node_count: u16 = @import("std").mem.zeroes(u16),
    next_allocator_id: cfl_heap_allocator_id_t = @import("std").mem.zeroes(cfl_heap_allocator_id_t),
    active_allocator_context: cfl_heap_allocator_id_t = @import("std").mem.zeroes(cfl_heap_allocator_id_t),
    allocator_0_buffer: ?*anyopaque = @import("std").mem.zeroes(?*anyopaque),
};
pub const CflHeapArenaSystem = struct_CflHeapArenaSystem;
pub const cfl_heap_arena_system_t = struct_CflHeapArenaSystem;
pub extern fn cfl_heap_arena_system_create(perm: [*c]struct_CflPerm, heap: [*c]cfl_heap_t, max_allocator_count: u16, total_node_count: u16, allocator_0_size: u16) [*c]CflHeapArenaSystem;
pub extern fn cfl_heap_arena_system_reset(sys: [*c]CflHeapArenaSystem) void;
pub extern fn cfl_heap_arena_create(sys: [*c]CflHeapArenaSystem, owner_node_id: u16, size_bytes: u16) cfl_heap_allocator_id_t;
pub extern fn cfl_heap_arena_destroy(sys: [*c]CflHeapArenaSystem, id: cfl_heap_allocator_id_t, owner_node_id: u16) void;
pub extern fn cfl_heap_arena_set_active_allocator(sys: [*c]CflHeapArenaSystem, owner_node_id: u16) void;
pub extern fn cfl_heap_arena_set_active_allocator_id(sys: [*c]CflHeapArenaSystem, allocator_id: cfl_heap_allocator_id_t) void;
pub extern fn cfl_heap_arena_set_node_allocator(sys: [*c]CflHeapArenaSystem, requesting_node_id: u16) void;
pub extern fn cfl_arena_system_alloc(sys: [*c]CflHeapArenaSystem, requesting_node_id: u16, size_bytes: u16) ?*anyopaque;
pub extern fn cfl_arena_system_alloc_aligned(sys: [*c]CflHeapArenaSystem, requesting_node_id: u16, size_bytes: u16, alignment: u8) ?*anyopaque;
pub extern fn cfl_arena_additional_alloc(sys: [*c]CflHeapArenaSystem, node_index: u16, size_bytes: u16) ?*anyopaque;
pub extern fn cfl_arena_additional_alloc_aligned(sys: [*c]CflHeapArenaSystem, node_index: u16, size_bytes: u16, alignment: u8) ?*anyopaque;
pub extern fn cfl_arena_alloc_from_active(sys: [*c]CflHeapArenaSystem, node_index: u16, size_bytes: u16) ?*anyopaque;
pub extern fn cfl_arena_alloc_from_active_aligned(sys: [*c]CflHeapArenaSystem, node_index: u16, size_bytes: u16, alignment: u8) ?*anyopaque;
pub extern fn cfl_heap_arena_get_node_ptr(sys: [*c]CflHeapArenaSystem, node_id: u16) ?*anyopaque;
pub extern fn cfl_heap_arena_get_node_allocator_id(sys: [*c]CflHeapArenaSystem, node_id: u16) cfl_heap_allocator_id_t;
pub extern fn cfl_heap_arena_set_node_allocator_id(sys: [*c]CflHeapArenaSystem, node_id: u16, allocator_id: cfl_heap_allocator_id_t) void;
pub extern fn cfl_heap_arena_get_node_memory_index(sys: [*c]CflHeapArenaSystem, node_id: u16) u16;
pub extern fn cfl_heap_arena_set_node_memory_index(sys: [*c]CflHeapArenaSystem, node_id: u16, memory_idx: u16) void;
pub extern fn cfl_heap_arena_used_bytes(sys: [*c]CflHeapArenaSystem, id: cfl_heap_allocator_id_t) u16;
pub extern fn cfl_heap_arena_free_bytes(sys: [*c]CflHeapArenaSystem, id: cfl_heap_allocator_id_t) u16;
pub extern fn cfl_heap_arena_dump_stats(sys: [*c]CflHeapArenaSystem) CflHeapArenaStats;
pub const CflPermStats = extern struct {
    total_allocations: u16 = @import("std").mem.zeroes(u16),
    current_used_bytes: u16 = @import("std").mem.zeroes(u16),
    peak_used_bytes: u16 = @import("std").mem.zeroes(u16),
    largest_allocation: u16 = @import("std").mem.zeroes(u16),
    smallest_allocation: u16 = @import("std").mem.zeroes(u16),
};
pub const CflPerm = struct_CflPerm;
pub const cfl_perm_t = struct_CflPerm;
pub extern fn cfl_perm_create() [*c]CflPerm;
pub extern fn cfl_perm_destroy(perm: [*c]CflPerm) void;
pub extern fn cfl_perm_set_instance(perm: [*c]cfl_perm_t) void;
pub extern fn cfl_perm_malloc_create(size: u16) [*c]cfl_perm_t;
pub extern fn cfl_perm_malloc_destroy(perm: [*c]cfl_perm_t) void;
pub extern fn cfl_perm_init(perm: [*c]CflPerm, buffer: ?*anyopaque, buffer_size: u16) void;
pub extern fn cfl_perm_reset(perm: [*c]CflPerm) void;
pub extern fn cfl_perm_alloc(perm: [*c]CflPerm, size_bytes: u16) u16;
pub extern fn cfl_perm_alloc_aligned(perm: [*c]CflPerm, size_bytes: u16, alignment: u16) u16;
pub extern fn cfl_perm_alloc_pointer(perm: [*c]CflPerm, size_bytes: u16) ?*anyopaque;
pub extern fn cfl_perm_alloc_pointer_aligned(perm: [*c]CflPerm, size_bytes: u16, alignment: u16) ?*anyopaque;
pub extern fn cfl_perm_ptr(perm: [*c]CflPerm, idx: u16) ?*anyopaque;
pub extern fn cfl_perm_ptr_to_idx(perm: [*c]CflPerm, ptr: ?*anyopaque) u16;
pub extern fn cfl_perm_used_bytes(perm: [*c]CflPerm) u16;
pub extern fn cfl_perm_free_bytes(perm: [*c]CflPerm) u16;
pub extern fn cfl_perm_get_stats(perm: [*c]CflPerm, stats: [*c]CflPermStats) void;
pub extern fn cfl_perm_validate(perm: [*c]CflPerm) bool;
pub const cfl_size_t = u64;
pub const cfl_int_t = i64;
pub const cfl_float_t = f64;
pub const CFL_EVENT_TYPE_PTR: c_int = 0;
pub const CFL_EVENT_TYPE_INT: c_int = 1;
pub const CFL_EVENT_TYPE_UINT: c_int = 2;
pub const CFL_EVENT_TYPE_FLOAT: c_int = 3;
pub const CFL_EVENT_TYPE_JSON_RECORD: c_int = 4;
pub const CFL_EVENT_TYPE_NULL: c_int = 5;
pub const cfl_event_type_t = c_uint;
pub const CFL_EVENT_VALUE_T = extern union {
    ptr: ?*anyopaque,
    integer: cfl_int_t,
    unsigned_val: cfl_size_t,
    floating: cfl_float_t,
};
pub const CFL_EVENT_DATA_T = extern struct {
    node_id: u16 = @import("std").mem.zeroes(u16),
    event_type: u8 = @import("std").mem.zeroes(u8),
    flags: u8 = @import("std").mem.zeroes(u8),
    event_id: u16 = @import("std").mem.zeroes(u16),
    queue_number: u16 = @import("std").mem.zeroes(u16),
    data: CFL_EVENT_VALUE_T = @import("std").mem.zeroes(CFL_EVENT_VALUE_T),
};
pub const CFL_EVENT_RING_T = extern struct {
    head: u16 = @import("std").mem.zeroes(u16),
    tail: u16 = @import("std").mem.zeroes(u16),
    capacity: u16 = @import("std").mem.zeroes(u16),
    mask: u16 = @import("std").mem.zeroes(u16),
    events: [*c]CFL_EVENT_DATA_T = @import("std").mem.zeroes([*c]CFL_EVENT_DATA_T),
};
pub const struct_CFL_EVENT_QUEUE_T = extern struct {
    high_priority: CFL_EVENT_RING_T = @import("std").mem.zeroes(CFL_EVENT_RING_T),
    low_priority: CFL_EVENT_RING_T = @import("std").mem.zeroes(CFL_EVENT_RING_T),
    queue_id: u16 = @import("std").mem.zeroes(u16),
    max_total_depth: u16 = @import("std").mem.zeroes(u16),
    max_high_depth: u16 = @import("std").mem.zeroes(u16),
    reserved: u16 = @import("std").mem.zeroes(u16),
};
pub const CFL_EVENT_QUEUE_T = struct_CFL_EVENT_QUEUE_T;
pub const cfl_event_queue_t = struct_CFL_EVENT_QUEUE_T;
pub extern fn cfl_create_event_queue(high_priority_size: c_uint, low_priority_size: c_uint, perm: [*c]CflPerm) [*c]CFL_EVENT_QUEUE_T;
pub extern fn cfl_clear_queue(queue_control: [*c]CFL_EVENT_QUEUE_T) void;
pub extern fn cfl_send_event(queue_control: [*c]CFL_EVENT_QUEUE_T, priority: c_uint, node_id: c_uint, event_type: c_uint, malloc_flag: bool, event_id: c_uint, data: ?*anyopaque) bool;
pub extern fn cfl_pop_event(queue_control: [*c]CFL_EVENT_QUEUE_T, event_data: [*c]CFL_EVENT_DATA_T) bool;
pub extern fn cfl_peek_event(queue_control: [*c]CFL_EVENT_QUEUE_T, event_data: [*c]CFL_EVENT_DATA_T) bool;
pub extern fn cfl_queue_number(event_data: [*c]CFL_EVENT_DATA_T) c_uint;
pub extern fn cfl_high_priority_count(queue_control: [*c]CFL_EVENT_QUEUE_T) c_uint;
pub extern fn cfl_low_priority_count(queue_control: [*c]CFL_EVENT_QUEUE_T) c_uint;
pub extern fn cfl_total_event_count(queue_control: [*c]CFL_EVENT_QUEUE_T) c_uint;
pub extern fn cfl_get_max_total_depth(queue_control: [*c]CFL_EVENT_QUEUE_T) c_uint;
pub extern fn cfl_get_max_high_depth(queue_control: [*c]CFL_EVENT_QUEUE_T) c_uint;
pub extern fn cfl_reset_queue_stats(queue_control: [*c]CFL_EVENT_QUEUE_T) void;
pub extern fn cfl_send_unsigned_event(queue_control: [*c]CFL_EVENT_QUEUE_T, priority: c_uint, node_id: c_uint, event_id: c_uint, value: cfl_size_t) bool;
pub extern fn cfl_send_integer_event(queue_control: [*c]CFL_EVENT_QUEUE_T, priority: c_uint, node_id: c_uint, event_id: c_uint, value: cfl_int_t) bool;
pub extern fn cfl_send_float_event(queue_control: [*c]CFL_EVENT_QUEUE_T, priority: c_uint, node_id: c_uint, event_id: c_uint, value: cfl_float_t) bool;
pub extern fn cfl_send_data_event(queue_control: [*c]CFL_EVENT_QUEUE_T, priority: c_uint, node_id: c_uint, malloc_flag: bool, event_id: c_uint, data: ?*anyopaque) bool;
pub extern fn cfl_send_null_event(queue_control: [*c]CFL_EVENT_QUEUE_T, priority: c_uint, node_id: c_uint, event_id: c_uint) bool;
pub extern fn cfl_send_json_event(queue_control: [*c]CFL_EVENT_QUEUE_T, priority: c_uint, node_id: c_uint, event_id: c_uint, record_index: u32) bool;
pub const CT_CONTINUE: c_int = 0;
pub const CT_SKIP_CHILDREN: c_int = 1;
pub const CT_STOP_BRANCH: c_int = 2;
pub const CT_STOP_SIBLINGS: c_int = 3;
pub const CT_STOP_LEVEL: c_int = 4;
pub const CT_STOP_ALL: c_int = 5;
pub const CT_ReturnCode = c_uint;
pub const CT_GetChildrenFunc = ?*const fn (?*anyopaque, c_uint, [*c]c_uint, c_uint) callconv(.C) c_uint;
pub const CT_ApplyFunc = ?*const fn (?*anyopaque, c_uint, c_uint, [*c]u8) callconv(.C) CT_ReturnCode;
pub const CT_StackEntry = extern struct {
    node_id: c_uint = @import("std").mem.zeroes(c_uint),
    level: c_uint = @import("std").mem.zeroes(c_uint),
    child_index: c_uint = @import("std").mem.zeroes(c_uint),
};
pub const struct_CT_TreeWalker = extern struct {
    user_handle: ?*anyopaque = @import("std").mem.zeroes(?*anyopaque),
    max_nodes: c_uint = @import("std").mem.zeroes(c_uint),
    flags: [*c]u8 = @import("std").mem.zeroes([*c]u8),
    get_children: CT_GetChildrenFunc = @import("std").mem.zeroes(CT_GetChildrenFunc),
    apply_func: CT_ApplyFunc = @import("std").mem.zeroes(CT_ApplyFunc),
    max_level: c_uint = @import("std").mem.zeroes(c_uint),
    max_node_id: c_uint = @import("std").mem.zeroes(c_uint),
    stop_all: bool = @import("std").mem.zeroes(bool),
};
pub const CT_TreeWalker = struct_CT_TreeWalker;
pub const CT_WalkerContext = extern struct {
    saved_flags: [*c]u8 = @import("std").mem.zeroes([*c]u8),
    saved_stop_all: bool = @import("std").mem.zeroes(bool),
    saved_max_level: c_uint = @import("std").mem.zeroes(c_uint),
    saved_max_node_id: c_uint = @import("std").mem.zeroes(c_uint),
    saved_apply_func: CT_ApplyFunc = @import("std").mem.zeroes(CT_ApplyFunc),
};
pub extern fn ct_walker_init(walker: [*c]CT_TreeWalker, max_nodes: c_uint, flags: [*c]u8, get_children: CT_GetChildrenFunc, apply_func: CT_ApplyFunc) bool;
pub extern fn ct_walker_walk(walker: [*c]CT_TreeWalker, user_handle: ?*anyopaque, root_id: c_uint, stack: [*c]CT_StackEntry, stack_capacity: c_uint, max_level: c_uint, max_node_id: c_uint) CT_ReturnCode;
pub extern fn ct_walker_reset(walker: [*c]CT_TreeWalker) void;
pub extern fn ct_walker_is_visited(walker: [*c]const CT_TreeWalker, node_id: c_uint) bool;
pub extern fn ct_walker_set_user_flags(walker: [*c]CT_TreeWalker, node_id: c_uint, flags: u8) void;
pub extern fn ct_walker_get_user_flags(walker: [*c]const CT_TreeWalker, node_id: c_uint) u8;
pub extern fn ct_walker_update_functions(walker: [*c]CT_TreeWalker, apply_func: CT_ApplyFunc, get_children: CT_GetChildrenFunc) void;
pub extern fn ct_walker_save_context(walker: [*c]CT_TreeWalker, context: [*c]CT_WalkerContext, backup_flags_buffer: [*c]u8) bool;
pub extern fn ct_walker_restore_context(walker: [*c]CT_TreeWalker, context: [*c]const CT_WalkerContext) void;
pub const clock_t = __clock_t;
pub const time_t = __time_t;
pub const struct_tm = extern struct {
    tm_sec: c_int = @import("std").mem.zeroes(c_int),
    tm_min: c_int = @import("std").mem.zeroes(c_int),
    tm_hour: c_int = @import("std").mem.zeroes(c_int),
    tm_mday: c_int = @import("std").mem.zeroes(c_int),
    tm_mon: c_int = @import("std").mem.zeroes(c_int),
    tm_year: c_int = @import("std").mem.zeroes(c_int),
    tm_wday: c_int = @import("std").mem.zeroes(c_int),
    tm_yday: c_int = @import("std").mem.zeroes(c_int),
    tm_isdst: c_int = @import("std").mem.zeroes(c_int),
    tm_gmtoff: c_long = @import("std").mem.zeroes(c_long),
    tm_zone: [*c]const u8 = @import("std").mem.zeroes([*c]const u8),
};
pub const struct_timespec = extern struct {
    tv_sec: __time_t = @import("std").mem.zeroes(__time_t),
    tv_nsec: __syscall_slong_t = @import("std").mem.zeroes(__syscall_slong_t),
};
pub const clockid_t = __clockid_t;
pub const timer_t = __timer_t;
pub const struct_itimerspec = extern struct {
    it_interval: struct_timespec = @import("std").mem.zeroes(struct_timespec),
    it_value: struct_timespec = @import("std").mem.zeroes(struct_timespec),
};
pub const struct_sigevent = opaque {};
pub const pid_t = __pid_t;
pub const struct___locale_data_1 = opaque {};
pub const struct___locale_struct = extern struct {
    __locales: [13]?*struct___locale_data_1 = @import("std").mem.zeroes([13]?*struct___locale_data_1),
    __ctype_b: [*c]const c_ushort = @import("std").mem.zeroes([*c]const c_ushort),
    __ctype_tolower: [*c]const c_int = @import("std").mem.zeroes([*c]const c_int),
    __ctype_toupper: [*c]const c_int = @import("std").mem.zeroes([*c]const c_int),
    __names: [13][*c]const u8 = @import("std").mem.zeroes([13][*c]const u8),
};
pub const __locale_t = [*c]struct___locale_struct;
pub const locale_t = __locale_t;
pub extern fn clock() clock_t;
pub extern fn time(__timer: [*c]time_t) time_t;
pub extern fn difftime(__time1: time_t, __time0: time_t) f64;
pub extern fn mktime(__tp: [*c]struct_tm) time_t;
pub extern fn strftime(noalias __s: [*c]u8, __maxsize: usize, noalias __format: [*c]const u8, noalias __tp: [*c]const struct_tm) usize;
pub extern fn strftime_l(noalias __s: [*c]u8, __maxsize: usize, noalias __format: [*c]const u8, noalias __tp: [*c]const struct_tm, __loc: locale_t) usize;
pub extern fn gmtime(__timer: [*c]const time_t) [*c]struct_tm;
pub extern fn localtime(__timer: [*c]const time_t) [*c]struct_tm;
pub extern fn gmtime_r(noalias __timer: [*c]const time_t, noalias __tp: [*c]struct_tm) [*c]struct_tm;
pub extern fn localtime_r(noalias __timer: [*c]const time_t, noalias __tp: [*c]struct_tm) [*c]struct_tm;
pub extern fn asctime(__tp: [*c]const struct_tm) [*c]u8;
pub extern fn ctime(__timer: [*c]const time_t) [*c]u8;
pub extern fn asctime_r(noalias __tp: [*c]const struct_tm, noalias __buf: [*c]u8) [*c]u8;
pub extern fn ctime_r(noalias __timer: [*c]const time_t, noalias __buf: [*c]u8) [*c]u8;
pub extern var __tzname: [2][*c]u8;
pub extern var __daylight: c_int;
pub extern var __timezone: c_long;
pub extern var tzname: [2][*c]u8;
pub extern fn tzset() void;
pub extern var daylight: c_int;
pub extern var timezone: c_long;
pub extern fn timegm(__tp: [*c]struct_tm) time_t;
pub extern fn timelocal(__tp: [*c]struct_tm) time_t;
pub extern fn dysize(__year: c_int) c_int;
pub extern fn nanosleep(__requested_time: [*c]const struct_timespec, __remaining: [*c]struct_timespec) c_int;
pub extern fn clock_getres(__clock_id: clockid_t, __res: [*c]struct_timespec) c_int;
pub extern fn clock_gettime(__clock_id: clockid_t, __tp: [*c]struct_timespec) c_int;
pub extern fn clock_settime(__clock_id: clockid_t, __tp: [*c]const struct_timespec) c_int;
pub extern fn clock_nanosleep(__clock_id: clockid_t, __flags: c_int, __req: [*c]const struct_timespec, __rem: [*c]struct_timespec) c_int;
pub extern fn clock_getcpuclockid(__pid: pid_t, __clock_id: [*c]clockid_t) c_int;
pub extern fn timer_create(__clock_id: clockid_t, noalias __evp: ?*struct_sigevent, noalias __timerid: [*c]timer_t) c_int;
pub extern fn timer_delete(__timerid: timer_t) c_int;
pub extern fn timer_settime(__timerid: timer_t, __flags: c_int, noalias __value: [*c]const struct_itimerspec, noalias __ovalue: [*c]struct_itimerspec) c_int;
pub extern fn timer_gettime(__timerid: timer_t, __value: [*c]struct_itimerspec) c_int;
pub extern fn timer_getoverrun(__timerid: timer_t) c_int;
pub extern fn timespec_get(__ts: [*c]struct_timespec, __base: c_int) c_int;
pub const ptrdiff_t = c_long;
pub const wchar_t = c_uint;
pub const max_align_t = extern struct {
    __clang_max_align_nonce1: c_longlong align(8) = @import("std").mem.zeroes(c_longlong),
    __clang_max_align_nonce2: c_longdouble align(16) = @import("std").mem.zeroes(c_longdouble),
};
pub const struct_cfl_timer_context = opaque {};
pub const cfl_timer_handle_t = ?*struct_cfl_timer_context;
pub const cfl_time_info_t = extern struct {
    year: i32 = @import("std").mem.zeroes(i32),
    month: i32 = @import("std").mem.zeroes(i32),
    day: i32 = @import("std").mem.zeroes(i32),
    dow: i32 = @import("std").mem.zeroes(i32),
    doy: i32 = @import("std").mem.zeroes(i32),
    hour: i32 = @import("std").mem.zeroes(i32),
    minute: i32 = @import("std").mem.zeroes(i32),
    second: i32 = @import("std").mem.zeroes(i32),
    timestamp: f64 = @import("std").mem.zeroes(f64),
};
pub const cfl_tick_result_t = extern struct {
    all_values: cfl_time_info_t = @import("std").mem.zeroes(cfl_time_info_t),
    changed_mask: u32 = @import("std").mem.zeroes(u32),
};
pub const CFL_TIMER_SUCCESS: c_int = 0;
pub const CFL_TIMER_ERROR_INVALID_HANDLE: c_int = -1;
pub const CFL_TIMER_ERROR_INVALID_PARAM: c_int = -2;
pub const CFL_TIMER_ERROR_ALLOCATION: c_int = -3;
pub const CFL_TIMER_ERROR_SYSTEM: c_int = -4;
pub const CFL_TIMER_ERROR_NOT_FOUND: c_int = -5;
pub const cfl_timer_error_t = c_int;
pub extern fn cfl_timer_create(wait_seconds: f64, perm: [*c]CflPerm) cfl_timer_handle_t;
pub extern fn cfl_timer_set_wait(handle: cfl_timer_handle_t, wait_seconds: f64) cfl_timer_error_t;
pub extern fn cfl_timer_get_wait(handle: cfl_timer_handle_t) f64;
pub extern fn cfl_timer_add_tick_data(handle: cfl_timer_handle_t, field_name: [*c]const u8, value: i64, perm: [*c]CflPerm) cfl_timer_error_t;
pub extern fn cfl_timer_get_tick_data(handle: cfl_timer_handle_t, field_name: [*c]const u8, value: [*c]i64) cfl_timer_error_t;
pub extern fn cfl_timer_wait(handle: cfl_timer_handle_t, wait_seconds: f64, result: [*c]cfl_tick_result_t) cfl_timer_error_t;
pub extern fn cfl_timer_get_current_time(handle: cfl_timer_handle_t, result: [*c]cfl_tick_result_t) cfl_timer_error_t;
pub extern fn cfl_timer_get_timestamp(handle: cfl_timer_handle_t) f64;
pub extern fn cfl_timer_get_time_simple(time_info: [*c]cfl_time_info_t) cfl_timer_error_t;
pub extern fn cfl_timer_tick(handle: cfl_timer_handle_t, result: [*c]cfl_tick_result_t) cfl_timer_error_t;
pub extern fn cfl_timer_format_time(time_info: [*c]const cfl_time_info_t, buffer: [*c]u8, buffer_size: usize) c_int;
pub extern fn cfl_timer_format_tick_result(result: [*c]const cfl_tick_result_t, buffer: [*c]u8, buffer_size: usize) c_int;
pub extern fn cfl_timer_print_time_info(time_info: [*c]const cfl_time_info_t) void;
pub extern fn cfl_timer_print_tick_result(result: [*c]const cfl_tick_result_t) void;
pub extern fn cfl_timer_error_string(@"error": cfl_timer_error_t) [*c]const u8;
pub const main_function_t = ?*const fn (?*anyopaque, c_uint, c_uint, c_uint, c_uint, ?*anyopaque) callconv(.C) c_uint;
pub const one_shot_function_t = ?*const fn (?*anyopaque, c_uint) callconv(.C) void;
pub const boolean_function_t = ?*const fn (?*anyopaque, c_uint, c_uint, c_uint, ?*anyopaque) callconv(.C) bool;
pub const chaintree_node_t = extern struct {
    node_index: u16 = @import("std").mem.zeroes(u16),
    parent_index: u16 = @import("std").mem.zeroes(u16),
    depth: u16 = @import("std").mem.zeroes(u16),
    link_start: u16 = @import("std").mem.zeroes(u16),
    link_count: u16 = @import("std").mem.zeroes(u16),
    main_function_index: u16 = @import("std").mem.zeroes(u16),
    init_function_index: u16 = @import("std").mem.zeroes(u16),
    aux_function_index: u16 = @import("std").mem.zeroes(u16),
    term_function_index: u16 = @import("std").mem.zeroes(u16),
    node_data_id: u16 = @import("std").mem.zeroes(u16),
};
pub const chaintree_kb_info_t = extern struct {
    kb_name: [*c]const u8 = @import("std").mem.zeroes([*c]const u8),
    root_node_index: u16 = @import("std").mem.zeroes(u16),
    start_index: u16 = @import("std").mem.zeroes(u16),
    node_count: u16 = @import("std").mem.zeroes(u16),
    max_depth: u16 = @import("std").mem.zeroes(u16),
    memory_factor: u16 = @import("std").mem.zeroes(u16),
};
pub const JSON_TYPE_STRING: c_int = 0;
pub const JSON_TYPE_INT32: c_int = 1;
pub const JSON_TYPE_FLOAT32: c_int = 2;
pub const JSON_TYPE_NULL: c_int = 3;
pub const JSON_TYPE_BOOL: c_int = 4;
pub const JSON_TYPE_ARRAY: c_int = 5;
pub const JSON_TYPE_OBJECT: c_int = 6;
pub const json_type_t = c_uint;
const union_unnamed_2 = extern union {
    string_offset: u32,
    i32_value: i32,
    f32_value: f32,
    bool_value: u8,
    container_count: u32,
};
pub const json_record_t = extern struct {
    object_type: json_type_t = @import("std").mem.zeroes(json_type_t),
    value: union_unnamed_2 = @import("std").mem.zeroes(union_unnamed_2),
};
pub const record_control_t = extern struct {
    start_position: u32 = @import("std").mem.zeroes(u32),
    num_records: u32 = @import("std").mem.zeroes(u32),
};
pub const chaintree_handle_t = extern struct {
    unique_id: [*c]const u8 = @import("std").mem.zeroes([*c]const u8),
    nodes: [*c]const chaintree_node_t = @import("std").mem.zeroes([*c]const chaintree_node_t),
    node_count: u16 = @import("std").mem.zeroes(u16),
    main_functions: [*c]const main_function_t = @import("std").mem.zeroes([*c]const main_function_t),
    main_function_count: u16 = @import("std").mem.zeroes(u16),
    one_shot_functions: [*c]const one_shot_function_t = @import("std").mem.zeroes([*c]const one_shot_function_t),
    one_shot_function_count: u16 = @import("std").mem.zeroes(u16),
    boolean_functions: [*c]const boolean_function_t = @import("std").mem.zeroes([*c]const boolean_function_t),
    boolean_function_count: u16 = @import("std").mem.zeroes(u16),
    main_function_names: [*c][*c]const u8 = @import("std").mem.zeroes([*c][*c]const u8),
    one_shot_function_names: [*c][*c]const u8 = @import("std").mem.zeroes([*c][*c]const u8),
    boolean_function_names: [*c][*c]const u8 = @import("std").mem.zeroes([*c][*c]const u8),
    main_function_usage_count: [*c]const u16 = @import("std").mem.zeroes([*c]const u16),
    link_table: [*c]const u16 = @import("std").mem.zeroes([*c]const u16),
    link_table_size: u16 = @import("std").mem.zeroes(u16),
    event_strings: [*c][*c]const u8 = @import("std").mem.zeroes([*c][*c]const u8),
    event_count: u16 = @import("std").mem.zeroes(u16),
    bitmask_names: [*c][*c]const u8 = @import("std").mem.zeroes([*c][*c]const u8),
    bitmask_count: u16 = @import("std").mem.zeroes(u16),
    kb_table: [*c]const chaintree_kb_info_t = @import("std").mem.zeroes([*c]const chaintree_kb_info_t),
    kb_count: u16 = @import("std").mem.zeroes(u16),
    node_data_records: [*c]const json_record_t = @import("std").mem.zeroes([*c]const json_record_t),
    node_data_records_count: u16 = @import("std").mem.zeroes(u16),
    node_data_strings: [*c]const u8 = @import("std").mem.zeroes([*c]const u8),
    node_data_strings_size: u16 = @import("std").mem.zeroes(u16),
    node_data_controls: [*c]const record_control_t = @import("std").mem.zeroes([*c]const record_control_t),
    node_data_controls_count: u16 = @import("std").mem.zeroes(u16),
};
pub extern fn ct_get_main_function_name(handle: [*c]const chaintree_handle_t, func_index: u16) [*c]const u8;
pub extern fn ct_get_main_function_index(handle: [*c]const chaintree_handle_t, func_name: [*c]const u8) c_int;
pub extern fn ct_get_one_shot_function_name(handle: [*c]const chaintree_handle_t, func_index: u16) [*c]const u8;
pub extern fn ct_get_one_shot_function_index(handle: [*c]const chaintree_handle_t, func_name: [*c]const u8) c_int;
pub extern fn ct_get_boolean_function_name(handle: [*c]const chaintree_handle_t, func_index: u16) [*c]const u8;
pub extern fn ct_get_boolean_function_index(handle: [*c]const chaintree_handle_t, func_name: [*c]const u8) c_int;
pub extern fn ct_get_event_name(handle: [*c]const chaintree_handle_t, event_index: u16) [*c]const u8;
pub extern fn ct_get_event_index(handle: [*c]const chaintree_handle_t, name: [*c]const u8) c_int;
pub extern fn ct_get_bitmask_name(handle: [*c]const chaintree_handle_t, bit_index: u8) [*c]const u8;
pub extern fn ct_get_bitmask_index(handle: [*c]const chaintree_handle_t, name: [*c]const u8) c_int;
pub extern fn ct_get_kb_count(handle: [*c]const chaintree_handle_t) u16;
pub const CFL_INIT_EVENT: c_int = 0;
pub const CFL_TERMINATE_EVENT: c_int = 1;
pub const CFL_START_TESTS: c_int = 2;
pub const CFL_TERMINATE_TESTS: c_int = 3;
pub const CFL_TIMER_EVENT: c_int = 4;
pub const CFL_SECOND_EVENT: c_int = 5;
pub const CFL_MINUTE_EVENT: c_int = 6;
pub const CFL_HOUR_EVENT: c_int = 7;
pub const CFL_DAY_EVENT: c_int = 8;
pub const CFL_WEEK_EVENT: c_int = 9;
pub const CFL_MONTH_EVENT: c_int = 10;
pub const CFL_YEAR_EVENT: c_int = 11;
pub const CFL_RAISE_EXCEPTION_EVENT: c_int = 12;
pub const CFL_TURN_HEARTBEAT_ON_EVENT: c_int = 13;
pub const CFL_TURN_HEARTBEAT_OFF_EVENT: c_int = 14;
pub const CFL_HEARTBEAT_EVENT: c_int = 15;
pub const CFL_SET_EXCEPTION_STEP_EVENT: c_int = 16;
pub const CFL_CHANGE_STATE_EVENT: c_int = 17;
pub const CFL_RESET_STATE_MACHINE_EVENT: c_int = 18;
pub const CFL_TERMINATE_STATE_MACHINE_EVENT: c_int = 19;
pub const cfl_engine_event_t = c_uint;
pub const sequence_aggregate_data_t = extern struct {
    finalize_function_id: i32 = @import("std").mem.zeroes(i32),
    try_node_count: i32 = @import("std").mem.zeroes(i32),
    try_node_indexes: [*c]u16 = @import("std").mem.zeroes([*c]u16),
    auxiliary_data: ?*anyopaque = @import("std").mem.zeroes(?*anyopaque),
};
pub const json_decoder_ctx_t = extern struct {
    records: [*c]const json_record_t = @import("std").mem.zeroes([*c]const json_record_t),
    records_count: u32 = @import("std").mem.zeroes(u32),
    strings: [*c]const u8 = @import("std").mem.zeroes([*c]const u8),
    strings_size: u32 = @import("std").mem.zeroes(u32),
    controls: [*c]const record_control_t = @import("std").mem.zeroes([*c]const record_control_t),
    controls_count: u32 = @import("std").mem.zeroes(u32),
    current_control_idx: u32 = @import("std").mem.zeroes(u32),
    error_code: c_int = @import("std").mem.zeroes(c_int),
};
pub const main_function_data_t = extern struct {
    main_function_ids: [6]u16 = @import("std").mem.zeroes([6]u16),
};
pub const struct_CFL_RUNTIME_HANDLE = extern struct {
    perm: [*c]cfl_perm_t = @import("std").mem.zeroes([*c]cfl_perm_t),
    heap: [*c]cfl_heap_t = @import("std").mem.zeroes([*c]cfl_heap_t),
    arena_system: [*c]cfl_heap_arena_system_t = @import("std").mem.zeroes([*c]cfl_heap_arena_system_t),
    event_queue: [*c]cfl_event_queue_t = @import("std").mem.zeroes([*c]cfl_event_queue_t),
    flags: [*c]u8 = @import("std").mem.zeroes([*c]u8),
    allocator_id: cfl_heap_allocator_id_t = @import("std").mem.zeroes(cfl_heap_allocator_id_t),
    timer_handle: cfl_timer_handle_t = @import("std").mem.zeroes(cfl_timer_handle_t),
    delta_time: f64 = @import("std").mem.zeroes(f64),
    test_count: c_uint = @import("std").mem.zeroes(c_uint),
    active_test_bitmap: [*c]u32 = @import("std").mem.zeroes([*c]u32),
    active_test_count: c_uint = @import("std").mem.zeroes(c_uint),
    kb_allocator_ids: [*c]cfl_heap_allocator_id_t = @import("std").mem.zeroes([*c]cfl_heap_allocator_id_t),
    test_has_arena: [*c]u8 = @import("std").mem.zeroes([*c]u8),
    walker: [*c]CT_TreeWalker = @import("std").mem.zeroes([*c]CT_TreeWalker),
    bitmask: i32 = @import("std").mem.zeroes(i32),
    event_data_ptr: [*c]CFL_EVENT_DATA_T = @import("std").mem.zeroes([*c]CFL_EVENT_DATA_T),
    cfl_engine_flag: bool = @import("std").mem.zeroes(bool),
    cfl_node_execution_count: c_uint = @import("std").mem.zeroes(c_uint),
    node_start_index: c_uint = @import("std").mem.zeroes(c_uint),
    kb_start_index: c_uint = @import("std").mem.zeroes(c_uint),
    kb_node_count: c_uint = @import("std").mem.zeroes(c_uint),
    kb_max_level: c_uint = @import("std").mem.zeroes(c_uint),
    current_kb_idx: c_uint = @import("std").mem.zeroes(c_uint),
    max_level: c_uint = @import("std").mem.zeroes(c_uint),
    stack: [*c]CT_StackEntry = @import("std").mem.zeroes([*c]CT_StackEntry),
    nested_stack: [*c]CT_StackEntry = @import("std").mem.zeroes([*c]CT_StackEntry),
    json_decoder_ctx: [*c]json_decoder_ctx_t = @import("std").mem.zeroes([*c]json_decoder_ctx_t),
    backup_flags: [*c]u8 = @import("std").mem.zeroes([*c]u8),
    walker_context_ptr: [*c]CT_WalkerContext = @import("std").mem.zeroes([*c]CT_WalkerContext),
    future_time_stamp: f64 = @import("std").mem.zeroes(f64),
    main_function_data: [*c]main_function_data_t = @import("std").mem.zeroes([*c]main_function_data_t),
    flash_handle: [*c]const chaintree_handle_t = @import("std").mem.zeroes([*c]const chaintree_handle_t),
};
pub const cfl_runtime_handle_t = struct_CFL_RUNTIME_HANDLE;
pub extern fn cfl_engine_create(handle: [*c]cfl_runtime_handle_t) void;
pub extern fn cfl_engine_init(handle: [*c]cfl_runtime_handle_t) void;
pub extern fn cfl_engine_init_test(handle: [*c]cfl_runtime_handle_t, start_node: c_uint, node_count: c_uint) void;
pub extern fn cfl_engine_node_is_enabled(handle: [*c]cfl_runtime_handle_t, node_index: c_uint) bool;
pub extern fn cfl_engine_node_is_initialized(handle: [*c]cfl_runtime_handle_t, node_index: c_uint) bool;
pub extern fn cfl_execute_event(handle: [*c]cfl_runtime_handle_t) bool;
pub extern fn cfl_enable_node(handle: [*c]cfl_runtime_handle_t, node_index: c_uint) void;
pub extern fn cfl_disable_node_flag(handle: [*c]cfl_runtime_handle_t, node_index: c_uint) void;
pub extern fn cfl_terminate_node_tree(handle: [*c]cfl_runtime_handle_t, node_id: c_uint) void;
pub extern fn cfl_find_try_node_indexes(handle: [*c]cfl_runtime_handle_t, node_index: c_uint, sequence_aggregate_data: [*c]sequence_aggregate_data_t) void;
pub extern fn cfl_terminate_all_nodes_in_kb(handle: [*c]cfl_runtime_handle_t, start_node: c_uint, node_count: c_uint) void;
pub extern fn cfl_memory_allocator_assignment(handle: [*c]cfl_runtime_handle_t, node_index: c_uint, allocator_id: cfl_heap_allocator_id_t) void;
pub const cfl_runtime_create_params_t = extern struct {
    perm: [*c]cfl_perm_t = @import("std").mem.zeroes([*c]cfl_perm_t),
    perm_buffer: [*c]u8 = @import("std").mem.zeroes([*c]u8),
    perm_buffer_size: u16 = @import("std").mem.zeroes(u16),
    heap_size: u16 = @import("std").mem.zeroes(u16),
    max_allocator_count: u16 = @import("std").mem.zeroes(u16),
    total_node_count: u16 = @import("std").mem.zeroes(u16),
    allocator_0_size: u16 = @import("std").mem.zeroes(u16),
    event_queue_high_priority_size: u16 = @import("std").mem.zeroes(u16),
    event_queue_low_priority_size: u16 = @import("std").mem.zeroes(u16),
    delta_time: f64 = @import("std").mem.zeroes(f64),
};
pub extern fn cfl_runtime_create_params_create() [*c]cfl_runtime_create_params_t;
pub extern fn cfl_runtime_create_params_destroy(params: [*c]cfl_runtime_create_params_t) void;
pub extern fn cfl_runtime_create(perm: [*c]cfl_perm_t, params: [*c]cfl_runtime_create_params_t, flash_handle: [*c]const chaintree_handle_t) [*c]cfl_runtime_handle_t;
pub extern fn cfl_runtime_reset(handle: [*c]cfl_runtime_handle_t) void;
pub extern fn cfl_runtime_run(handle: [*c]cfl_runtime_handle_t) bool;
pub extern fn cfl_add_test_by_index(handle: [*c]cfl_runtime_handle_t, kb_index: u16) bool;
pub extern fn cfl_delete_test_by_index(handle: [*c]cfl_runtime_handle_t, kb_index: u16) bool;
pub extern fn cfl_calculate_arrena_number(flash_handle: [*c]const chaintree_handle_t) u16;
pub extern const ct_deqxr7z9_nodes: [1252]chaintree_node_t;
pub extern const ct_deqxr7z9_link_table: [1124]u16;
pub extern fn memcpy(__dest: ?*anyopaque, __src: ?*const anyopaque, __n: c_ulong) ?*anyopaque;
pub extern fn memmove(__dest: ?*anyopaque, __src: ?*const anyopaque, __n: c_ulong) ?*anyopaque;
pub extern fn memccpy(__dest: ?*anyopaque, __src: ?*const anyopaque, __c: c_int, __n: c_ulong) ?*anyopaque;
pub extern fn memset(__s: ?*anyopaque, __c: c_int, __n: c_ulong) ?*anyopaque;
pub extern fn memcmp(__s1: ?*const anyopaque, __s2: ?*const anyopaque, __n: c_ulong) c_int;
pub extern fn __memcmpeq(__s1: ?*const anyopaque, __s2: ?*const anyopaque, __n: usize) c_int;
pub extern fn memchr(__s: ?*const anyopaque, __c: c_int, __n: c_ulong) ?*anyopaque;
pub extern fn strcpy(__dest: [*c]u8, __src: [*c]const u8) [*c]u8;
pub extern fn strncpy(__dest: [*c]u8, __src: [*c]const u8, __n: c_ulong) [*c]u8;
pub extern fn strcat(__dest: [*c]u8, __src: [*c]const u8) [*c]u8;
pub extern fn strncat(__dest: [*c]u8, __src: [*c]const u8, __n: c_ulong) [*c]u8;
pub extern fn strcmp(__s1: [*c]const u8, __s2: [*c]const u8) c_int;
pub extern fn strncmp(__s1: [*c]const u8, __s2: [*c]const u8, __n: c_ulong) c_int;
pub extern fn strcoll(__s1: [*c]const u8, __s2: [*c]const u8) c_int;
pub extern fn strxfrm(__dest: [*c]u8, __src: [*c]const u8, __n: c_ulong) c_ulong;
pub extern fn strcoll_l(__s1: [*c]const u8, __s2: [*c]const u8, __l: locale_t) c_int;
pub extern fn strxfrm_l(__dest: [*c]u8, __src: [*c]const u8, __n: usize, __l: locale_t) usize;
pub extern fn strdup(__s: [*c]const u8) [*c]u8;
pub extern fn strndup(__string: [*c]const u8, __n: c_ulong) [*c]u8;
pub extern fn strchr(__s: [*c]const u8, __c: c_int) [*c]u8;
pub extern fn strrchr(__s: [*c]const u8, __c: c_int) [*c]u8;
pub extern fn strchrnul(__s: [*c]const u8, __c: c_int) [*c]u8;
pub extern fn strcspn(__s: [*c]const u8, __reject: [*c]const u8) c_ulong;
pub extern fn strspn(__s: [*c]const u8, __accept: [*c]const u8) c_ulong;
pub extern fn strpbrk(__s: [*c]const u8, __accept: [*c]const u8) [*c]u8;
pub extern fn strstr(__haystack: [*c]const u8, __needle: [*c]const u8) [*c]u8;
pub extern fn strtok(__s: [*c]u8, __delim: [*c]const u8) [*c]u8;
pub extern fn __strtok_r(noalias __s: [*c]u8, noalias __delim: [*c]const u8, noalias __save_ptr: [*c][*c]u8) [*c]u8;
pub extern fn strtok_r(noalias __s: [*c]u8, noalias __delim: [*c]const u8, noalias __save_ptr: [*c][*c]u8) [*c]u8;
pub extern fn strcasestr(__haystack: [*c]const u8, __needle: [*c]const u8) [*c]u8;
pub extern fn memmem(__haystack: ?*const anyopaque, __haystacklen: usize, __needle: ?*const anyopaque, __needlelen: usize) ?*anyopaque;
pub extern fn __mempcpy(noalias __dest: ?*anyopaque, noalias __src: ?*const anyopaque, __n: usize) ?*anyopaque;
pub extern fn mempcpy(__dest: ?*anyopaque, __src: ?*const anyopaque, __n: c_ulong) ?*anyopaque;
pub extern fn strlen(__s: [*c]const u8) c_ulong;
pub extern fn strnlen(__string: [*c]const u8, __maxlen: usize) usize;
pub extern fn strerror(__errnum: c_int) [*c]u8;
pub extern fn strerror_r(__errnum: c_int, __buf: [*c]u8, __buflen: usize) c_int;
pub extern fn strerror_l(__errnum: c_int, __l: locale_t) [*c]u8;
pub extern fn bcmp(__s1: ?*const anyopaque, __s2: ?*const anyopaque, __n: c_ulong) c_int;
pub extern fn bcopy(__src: ?*const anyopaque, __dest: ?*anyopaque, __n: c_ulong) void;
pub extern fn bzero(__s: ?*anyopaque, __n: c_ulong) void;
pub extern fn index(__s: [*c]const u8, __c: c_int) [*c]u8;
pub extern fn rindex(__s: [*c]const u8, __c: c_int) [*c]u8;
pub extern fn ffs(__i: c_int) c_int;
pub extern fn ffsl(__l: c_long) c_int;
pub extern fn ffsll(__ll: c_longlong) c_int;
pub extern fn strcasecmp(__s1: [*c]const u8, __s2: [*c]const u8) c_int;
pub extern fn strncasecmp(__s1: [*c]const u8, __s2: [*c]const u8, __n: c_ulong) c_int;
pub extern fn strcasecmp_l(__s1: [*c]const u8, __s2: [*c]const u8, __loc: locale_t) c_int;
pub extern fn strncasecmp_l(__s1: [*c]const u8, __s2: [*c]const u8, __n: usize, __loc: locale_t) c_int;
pub extern fn explicit_bzero(__s: ?*anyopaque, __n: usize) void;
pub extern fn strsep(noalias __stringp: [*c][*c]u8, noalias __delim: [*c]const u8) [*c]u8;
pub extern fn strsignal(__sig: c_int) [*c]u8;
pub extern fn __stpcpy(noalias __dest: [*c]u8, noalias __src: [*c]const u8) [*c]u8;
pub extern fn stpcpy(__dest: [*c]u8, __src: [*c]const u8) [*c]u8;
pub extern fn __stpncpy(noalias __dest: [*c]u8, noalias __src: [*c]const u8, __n: usize) [*c]u8;
pub extern fn stpncpy(__dest: [*c]u8, __src: [*c]const u8, __n: c_ulong) [*c]u8;
pub extern fn strlcpy(__dest: [*c]u8, __src: [*c]const u8, __n: c_ulong) c_ulong;
pub extern fn strlcat(__dest: [*c]u8, __src: [*c]const u8, __n: c_ulong) c_ulong;
pub const EVENT_CFL_INIT_EVENT: c_int = 0;
pub const EVENT_CFL_TERMINATE_EVENT: c_int = 1;
pub const EVENT_CFL_START_TESTS: c_int = 2;
pub const EVENT_CFL_TERMINATE_TESTS: c_int = 3;
pub const EVENT_CFL_TIMER_EVENT: c_int = 4;
pub const EVENT_CFL_SECOND_EVENT: c_int = 5;
pub const EVENT_CFL_MINUTE_EVENT: c_int = 6;
pub const EVENT_CFL_HOUR_EVENT: c_int = 7;
pub const EVENT_CFL_DAY_EVENT: c_int = 8;
pub const EVENT_CFL_WEEK_EVENT: c_int = 9;
pub const EVENT_CFL_MONTH_EVENT: c_int = 10;
pub const EVENT_CFL_YEAR_EVENT: c_int = 11;
pub const EVENT_CFL_RAISE_EXCEPTION_EVENT: c_int = 12;
pub const EVENT_CFL_TURN_HEARTBEAT_ON_EVENT: c_int = 13;
pub const EVENT_CFL_TURN_HEARTBEAT_OFF_EVENT: c_int = 14;
pub const EVENT_CFL_HEARTBEAT_EVENT: c_int = 15;
pub const EVENT_CFL_SET_EXCEPTION_STEP_EVENT: c_int = 16;
pub const EVENT_CFL_CHANGE_STATE_EVENT: c_int = 17;
pub const EVENT_CFL_RESET_STATE_MACHINE_EVENT: c_int = 18;
pub const EVENT_CFL_TERMINATE_STATE_MACHINE_EVENT: c_int = 19;
pub const EVENT_WAIT_FOR_EVENT: c_int = 20;
pub const EVENT_PUBLISH_EVENT: c_int = 21;
pub const EVENT_TEST_EVENT_1: c_int = 22;
pub const EVENT_TEST_EVENT_2: c_int = 23;
pub const EVENT_TEST_EVENT_3: c_int = 24;
pub const EVENT_SYNC_EVENT: c_int = 25;
pub const EVENT_TEST_EVENT: c_int = 26;
pub const EVENT_CF_TIMER_EVENT: c_int = 27;
pub const EVENT_COUNT: c_int = 28;
pub const event_index_t = c_uint;
pub extern var ct_deqxr7z9_event_strings: [28][*c]const u8;
pub extern var ct_deqxr7z9_bitmask_names: [3][*c]const u8;
pub extern const ct_deqxr7z9_kb_table: [19]chaintree_kb_info_t;
pub extern const ct_deqxr7z9_node_data_records: [5092]json_record_t;
pub extern const ct_deqxr7z9_node_data_strings: [4915]u8;
pub extern const ct_deqxr7z9_node_data_controls: [811]record_control_t;
pub const MAIN_FUNC_CFL_NULL: c_int = 0;
pub const MAIN_FUNC_CFL_COLUMN_MAIN: c_int = 1;
pub const MAIN_FUNC_CFL_DF_MASK_MAIN: c_int = 2;
pub const MAIN_FUNC_CFL_DISABLE: c_int = 3;
pub const MAIN_FUNC_CFL_EVENT_LOGGER: c_int = 4;
pub const MAIN_FUNC_CFL_EXCEPTION_CATCH_ALL_MAIN: c_int = 5;
pub const MAIN_FUNC_CFL_EXCEPTION_CATCH_MAIN: c_int = 6;
pub const MAIN_FUNC_CFL_FORK_MAIN: c_int = 7;
pub const MAIN_FUNC_CFL_FOR_MAIN: c_int = 8;
pub const MAIN_FUNC_CFL_GATE_NODE_MAIN: c_int = 9;
pub const MAIN_FUNC_CFL_HALT: c_int = 10;
pub const MAIN_FUNC_CFL_JOIN_MAIN: c_int = 11;
pub const MAIN_FUNC_CFL_JOIN_SEQUENCE_ELEMENT: c_int = 12;
pub const MAIN_FUNC_CFL_LOCAL_ARENA_MAIN: c_int = 13;
pub const MAIN_FUNC_CFL_RECOVERY_MAIN: c_int = 14;
pub const MAIN_FUNC_CFL_RESET: c_int = 15;
pub const MAIN_FUNC_CFL_SEQUENCE_FAIL_MAIN: c_int = 16;
pub const MAIN_FUNC_CFL_SEQUENCE_PASS_MAIN: c_int = 17;
pub const MAIN_FUNC_CFL_SEQUENCE_START_MAIN: c_int = 18;
pub const MAIN_FUNC_CFL_STATE_MACHINE_MAIN: c_int = 19;
pub const MAIN_FUNC_CFL_SUPERVISOR_MAIN: c_int = 20;
pub const MAIN_FUNC_CFL_TERMINATE: c_int = 21;
pub const MAIN_FUNC_CFL_TERMINATE_SYSTEM: c_int = 22;
pub const MAIN_FUNC_CFL_VERIFY: c_int = 23;
pub const MAIN_FUNC_CFL_WAIT: c_int = 24;
pub const MAIN_FUNC_CFL_WAIT_TIME: c_int = 25;
pub const MAIN_FUNC_CFL_WATCH_DOG_MAIN: c_int = 26;
pub const MAIN_FUNC_CFL_WHILE_MAIN: c_int = 27;
pub const MAIN_FUNC_SM_EVENT_FILTERING_MAIN: c_int = 28;
pub const MAIN_FUNC_COUNT: c_int = 29;
pub const MAIN_FUNC_t = c_uint;
pub const ONE_SHOT_FUNC_CFL_NULL: c_int = 0;
pub const ONE_SHOT_FUNC_ACTIVATE_VALVE: c_int = 1;
pub const ONE_SHOT_FUNC_CFL_CATCH_ALL_EXCEPTION_INIT: c_int = 2;
pub const ONE_SHOT_FUNC_CFL_CATCH_ALL_EXCEPTION_TERM: c_int = 3;
pub const ONE_SHOT_FUNC_CFL_CHANGE_STATE: c_int = 4;
pub const ONE_SHOT_FUNC_CFL_CLEAR_BITMASK: c_int = 5;
pub const ONE_SHOT_FUNC_CFL_COLUMN_INIT: c_int = 6;
pub const ONE_SHOT_FUNC_CFL_COLUMN_TERM: c_int = 7;
pub const ONE_SHOT_FUNC_CFL_DF_MASK_INIT: c_int = 8;
pub const ONE_SHOT_FUNC_CFL_DF_MASK_TERM: c_int = 9;
pub const ONE_SHOT_FUNC_CFL_DISABLE_NODES: c_int = 10;
pub const ONE_SHOT_FUNC_CFL_DISABLE_WATCH_DOG: c_int = 11;
pub const ONE_SHOT_FUNC_CFL_ENABLE_NODES: c_int = 12;
pub const ONE_SHOT_FUNC_CFL_ENABLE_WATCH_DOG: c_int = 13;
pub const ONE_SHOT_FUNC_CFL_EVENT_LOGGER_INIT: c_int = 14;
pub const ONE_SHOT_FUNC_CFL_EVENT_LOGGER_TERM: c_int = 15;
pub const ONE_SHOT_FUNC_CFL_EXCEPTION_CATCH_INIT: c_int = 16;
pub const ONE_SHOT_FUNC_CFL_EXCEPTION_CATCH_TERM: c_int = 17;
pub const ONE_SHOT_FUNC_CFL_FORK_INIT: c_int = 18;
pub const ONE_SHOT_FUNC_CFL_FORK_TERM: c_int = 19;
pub const ONE_SHOT_FUNC_CFL_FOR_INIT: c_int = 20;
pub const ONE_SHOT_FUNC_CFL_FOR_TERM: c_int = 21;
pub const ONE_SHOT_FUNC_CFL_GATE_NODE_INIT: c_int = 22;
pub const ONE_SHOT_FUNC_CFL_GATE_NODE_TERM: c_int = 23;
pub const ONE_SHOT_FUNC_CFL_HEARTBEAT_EVENT: c_int = 24;
pub const ONE_SHOT_FUNC_CFL_JOIN_INIT: c_int = 25;
pub const ONE_SHOT_FUNC_CFL_JOIN_SEQUENCE_ELEMENT_INIT: c_int = 26;
pub const ONE_SHOT_FUNC_CFL_JOIN_SEQUENCE_ELEMENT_TERM: c_int = 27;
pub const ONE_SHOT_FUNC_CFL_JOIN_TERM: c_int = 28;
pub const ONE_SHOT_FUNC_CFL_LOCAL_ARENA_INIT: c_int = 29;
pub const ONE_SHOT_FUNC_CFL_LOCAL_ARENA_TERM: c_int = 30;
pub const ONE_SHOT_FUNC_CFL_LOG_MESSAGE: c_int = 31;
pub const ONE_SHOT_FUNC_CFL_MARK_SEQUENCE: c_int = 32;
pub const ONE_SHOT_FUNC_CFL_MARK_SUPERVISOR_NODE_FAILURE_INIT: c_int = 33;
pub const ONE_SHOT_FUNC_CFL_PAT_WATCH_DOG: c_int = 34;
pub const ONE_SHOT_FUNC_CFL_RAISE_EXCEPTION: c_int = 35;
pub const ONE_SHOT_FUNC_CFL_RECOVERY_INIT: c_int = 36;
pub const ONE_SHOT_FUNC_CFL_RECOVERY_TERM: c_int = 37;
pub const ONE_SHOT_FUNC_CFL_RESET_STATE_MACHINE: c_int = 38;
pub const ONE_SHOT_FUNC_CFL_SEND_NAMED_EVENT: c_int = 39;
pub const ONE_SHOT_FUNC_CFL_SEQUENCE_FAIL_INIT: c_int = 40;
pub const ONE_SHOT_FUNC_CFL_SEQUENCE_FAIL_TERM: c_int = 41;
pub const ONE_SHOT_FUNC_CFL_SEQUENCE_PASS_INIT: c_int = 42;
pub const ONE_SHOT_FUNC_CFL_SEQUENCE_PASS_TERM: c_int = 43;
pub const ONE_SHOT_FUNC_CFL_SEQUENCE_START_INIT: c_int = 44;
pub const ONE_SHOT_FUNC_CFL_SEQUENCE_START_TERM: c_int = 45;
pub const ONE_SHOT_FUNC_CFL_SET_BITMASK: c_int = 46;
pub const ONE_SHOT_FUNC_CFL_SET_EXCEPTION_STEP: c_int = 47;
pub const ONE_SHOT_FUNC_CFL_START_STOP_TESTS: c_int = 48;
pub const ONE_SHOT_FUNC_CFL_STATE_MACHINE_INIT: c_int = 49;
pub const ONE_SHOT_FUNC_CFL_STATE_MACHINE_TERM: c_int = 50;
pub const ONE_SHOT_FUNC_CFL_SUPERVISOR_INIT: c_int = 51;
pub const ONE_SHOT_FUNC_CFL_SUPERVISOR_TERM: c_int = 52;
pub const ONE_SHOT_FUNC_CFL_TERMINATE_STATE_MACHINE: c_int = 53;
pub const ONE_SHOT_FUNC_CFL_TURN_HEARTBEAT_OFF: c_int = 54;
pub const ONE_SHOT_FUNC_CFL_TURN_HEARTBEAT_ON: c_int = 55;
pub const ONE_SHOT_FUNC_CFL_VERIFY_INIT: c_int = 56;
pub const ONE_SHOT_FUNC_CFL_VERIFY_TERM: c_int = 57;
pub const ONE_SHOT_FUNC_CFL_WAIT_INIT: c_int = 58;
pub const ONE_SHOT_FUNC_CFL_WAIT_TERM: c_int = 59;
pub const ONE_SHOT_FUNC_CFL_WAIT_TIME_INIT: c_int = 60;
pub const ONE_SHOT_FUNC_CFL_WATCH_DOG_INIT: c_int = 61;
pub const ONE_SHOT_FUNC_CFL_WATCH_DOG_TERM: c_int = 62;
pub const ONE_SHOT_FUNC_CFL_WHILE_INIT: c_int = 63;
pub const ONE_SHOT_FUNC_CFL_WHILE_TERM: c_int = 64;
pub const ONE_SHOT_FUNC_SM_EVENT_FILTERING_INIT: c_int = 65;
pub const ONE_SHOT_FUNC_WAIT_FOR_EVENT_ERROR: c_int = 66;
pub const ONE_SHOT_FUNC_VERIFY_ERROR: c_int = 67;
pub const ONE_SHOT_FUNC_INITIALIZE_SEQUENCE: c_int = 68;
pub const ONE_SHOT_FUNC_DISPLAY_SEQUENCE_RESULT: c_int = 69;
pub const ONE_SHOT_FUNC_DISPLAY_SEQUENCE_TILL_RESULT: c_int = 70;
pub const ONE_SHOT_FUNC_DISPLAY_FAILURE_WINDOW_RESULT: c_int = 71;
pub const ONE_SHOT_FUNC_WATCH_DOG_TIME_OUT: c_int = 72;
pub const ONE_SHOT_FUNC_EXCEPTION_LOGGING: c_int = 73;
pub const ONE_SHOT_FUNC_WHILE_BITMASK_FAILURE: c_int = 74;
pub const ONE_SHOT_FUNC_VERIFY_BITMASK_FAILURE: c_int = 75;
pub const ONE_SHOT_FUNC_WAIT_FOR_TEST_COMPLETE_ERROR: c_int = 76;
pub const ONE_SHOT_FUNC_VERIFY_TESTS_ACTIVE_ERROR: c_int = 77;
pub const ONE_SHOT_FUNC_COUNT: c_int = 78;
pub const ONE_SHOT_FUNC_t = c_uint;
pub const BOOL_FUNC_CFL_NULL: c_int = 0;
pub const BOOL_FUNC_CATCH_ALL_EXCEPTION: c_int = 1;
pub const BOOL_FUNC_CFL_BOOL_FALSE: c_int = 2;
pub const BOOL_FUNC_CFL_COLUMN_NULL: c_int = 3;
pub const BOOL_FUNC_CFL_GATE_NODE_NULL: c_int = 4;
pub const BOOL_FUNC_CFL_SM_EVENT_SYNC: c_int = 5;
pub const BOOL_FUNC_CFL_STATE_MACHINE_NULL: c_int = 6;
pub const BOOL_FUNC_CFL_VERIFY_BITMASK: c_int = 7;
pub const BOOL_FUNC_CFL_VERIFY_TESTS_ACTIVE: c_int = 8;
pub const BOOL_FUNC_CFL_VERIFY_TIME_OUT: c_int = 9;
pub const BOOL_FUNC_CFL_WAIT_FOR_BITMASK: c_int = 10;
pub const BOOL_FUNC_CFL_WAIT_FOR_EVENT: c_int = 11;
pub const BOOL_FUNC_CFL_WAIT_FOR_TESTS_COMPLETE: c_int = 12;
pub const BOOL_FUNC_EXCEPTION_FILTER: c_int = 13;
pub const BOOL_FUNC_USER_SKIP_CONDITION: c_int = 14;
pub const BOOL_FUNC_WHILE_TEST: c_int = 15;
pub const BOOL_FUNC_COUNT: c_int = 16;
pub const BOOL_FUNC_t = c_uint;
pub extern const ct_deqxr7z9_main_functions: [29]main_function_t;
pub extern const ct_deqxr7z9_one_shot_functions: [78]one_shot_function_t;
pub extern const ct_deqxr7z9_boolean_functions: [16]boolean_function_t;
pub extern const ct_deqxr7z9_main_function_usage_count: [29]u16;
pub extern var ct_deqxr7z9_main_function_names: [29][*c]const u8;
pub extern var ct_deqxr7z9_one_shot_function_names: [78][*c]const u8;
pub extern var ct_deqxr7z9_boolean_function_names: [16][*c]const u8;
pub extern const g_test_header: chaintree_handle_t;
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
pub const CFL_RUNTIME_H = "";
pub const CFL_GLOBAL_DEFINITIONS_H = "";
pub const CFL_64BIT = @as(c_int, 1);
pub const BLOCK_ALIGNMENT = @as(c_int, 8);
pub const MIN_BLOCK_SIZE = @as(c_int, 8);
pub const ARENA_ALIGNMENT = @as(c_int, 8);
pub const JSON_DEBUG = @as(c_int, 1);
pub const CFL_EXCEPTION_H = "";
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
pub const EXCEPTION = @compileError("unable to translate macro: undefined identifier `__FILE__`");
// /home/gedgar/knowledge_base_assembly/zig_programs_and_containers/building_blocks/chain_tree/chain_tree_low_ram_c_test/testing/library_files/cfl_exception.h:7:9
pub const CFL_HEAP_H = "";
pub const __STDBOOL_H = "";
pub const __bool_true_false_are_defined = @as(c_int, 1);
pub const @"bool" = bool;
pub const @"true" = @as(c_int, 1);
pub const @"false" = @as(c_int, 0);
pub const INVALID_HEAP_IDX = @import("std").zig.c_translation.promoteIntLiteral(c_int, 0xFFFF, .hex);
pub const NODE_ID_NONE = @import("std").zig.c_translation.promoteIntLiteral(c_int, 0xFFFF, .hex);
pub inline fn CFL_HEAP_TOTAL_SIZE(buffer_size: anytype) @TypeOf(@import("std").zig.c_translation.sizeof(CflHeap) + buffer_size) {
    _ = &buffer_size;
    return @import("std").zig.c_translation.sizeof(CflHeap) + buffer_size;
}
pub const CFL_HEAP_DEFINE_STATIC = @compileError("unable to translate macro: undefined identifier `_storage`");
// /home/gedgar/knowledge_base_assembly/zig_programs_and_containers/building_blocks/chain_tree/chain_tree_low_ram_c_test/testing/library_files/cfl_heap.h:182:9
pub const CFL_HEAP_ARENA_ALLOCATE_H = "";
pub const CFL_PERM_H = "";
pub const INVALID_PERM_IDX = @import("std").zig.c_translation.promoteIntLiteral(c_int, 0xFFFF, .hex);
pub inline fn CFL_PERM_TOTAL_SIZE(buffer_size: anytype) @TypeOf(@import("std").zig.c_translation.sizeof(CflPerm) + buffer_size) {
    _ = &buffer_size;
    return @import("std").zig.c_translation.sizeof(CflPerm) + buffer_size;
}
pub const CFL_PERM_DEFINE_STATIC = @compileError("unable to translate macro: undefined identifier `_storage`");
// /home/gedgar/knowledge_base_assembly/zig_programs_and_containers/building_blocks/chain_tree/chain_tree_low_ram_c_test/testing/library_files/cfl_perm.h:69:9
pub const CFL_EVENT_QUEUE_H = "";
pub const CFL_EVENT_BROADCAST_NODE = @import("std").zig.c_translation.promoteIntLiteral(c_int, 0xFFFF, .hex);
pub const CFL_EVENT_MALLOC_FLAG = @as(c_int, 0x01);
pub const CFL_EVENT_PRIORITY_LOW = @as(c_int, 0);
pub const CFL_EVENT_PRIORITY_HIGH = @as(c_int, 1);
pub const CFL_EVENT_QUEUE_MIN_SIZE = @as(c_int, 2);
pub const CT_TREE_WALKER_H = "";
pub const CT_FLAG_VISITED = @as(c_int, 0x01);
pub const CT_FLAG_RESERVED1 = @as(c_int, 0x02);
pub const CT_FLAG_RESERVED2 = @as(c_int, 0x04);
pub const CT_FLAG_RESERVED3 = @as(c_int, 0x08);
pub const CT_FLAG_USER0 = @as(c_int, 0x10);
pub const CT_FLAG_USER1 = @as(c_int, 0x20);
pub const CT_FLAG_USER2 = @as(c_int, 0x40);
pub const CT_FLAG_USER3 = @as(c_int, 0x80);
pub const CT_FLAG_ENGINE_MASK = @as(c_int, 0x0F);
pub const CT_FLAG_USER_MASK = @as(c_int, 0xF0);
pub const CFL_TIMER_H = "";
pub const _TIME_H = @as(c_int, 1);
pub const __need_size_t = "";
pub const __need_NULL = "";
pub const _SIZE_T = "";
pub const NULL = @import("std").zig.c_translation.cast(?*anyopaque, @as(c_int, 0));
pub const _BITS_TIME_H = @as(c_int, 1);
pub const CLOCKS_PER_SEC = @import("std").zig.c_translation.cast(__clock_t, @import("std").zig.c_translation.promoteIntLiteral(c_int, 1000000, .decimal));
pub const CLOCK_REALTIME = @as(c_int, 0);
pub const CLOCK_MONOTONIC = @as(c_int, 1);
pub const CLOCK_PROCESS_CPUTIME_ID = @as(c_int, 2);
pub const CLOCK_THREAD_CPUTIME_ID = @as(c_int, 3);
pub const CLOCK_MONOTONIC_RAW = @as(c_int, 4);
pub const CLOCK_REALTIME_COARSE = @as(c_int, 5);
pub const CLOCK_MONOTONIC_COARSE = @as(c_int, 6);
pub const CLOCK_BOOTTIME = @as(c_int, 7);
pub const CLOCK_REALTIME_ALARM = @as(c_int, 8);
pub const CLOCK_BOOTTIME_ALARM = @as(c_int, 9);
pub const CLOCK_TAI = @as(c_int, 11);
pub const TIMER_ABSTIME = @as(c_int, 1);
pub const __clock_t_defined = @as(c_int, 1);
pub const __time_t_defined = @as(c_int, 1);
pub const __struct_tm_defined = @as(c_int, 1);
pub const _STRUCT_TIMESPEC = @as(c_int, 1);
pub const _BITS_ENDIAN_H = @as(c_int, 1);
pub const __LITTLE_ENDIAN = @as(c_int, 1234);
pub const __BIG_ENDIAN = @as(c_int, 4321);
pub const __PDP_ENDIAN = @as(c_int, 3412);
pub const _BITS_ENDIANNESS_H = @as(c_int, 1);
pub const __BYTE_ORDER = __LITTLE_ENDIAN;
pub const __FLOAT_WORD_ORDER = __BYTE_ORDER;
pub inline fn __LONG_LONG_PAIR(HI: anytype, LO: anytype) @TypeOf(HI) {
    _ = &HI;
    _ = &LO;
    return blk: {
        _ = &LO;
        break :blk HI;
    };
}
pub const __clockid_t_defined = @as(c_int, 1);
pub const __timer_t_defined = @as(c_int, 1);
pub const __itimerspec_defined = @as(c_int, 1);
pub const __pid_t_defined = "";
pub const _BITS_TYPES_LOCALE_T_H = @as(c_int, 1);
pub const _BITS_TYPES___LOCALE_T_H = @as(c_int, 1);
pub const TIME_UTC = @as(c_int, 1);
pub inline fn __isleap(year: anytype) @TypeOf((@import("std").zig.c_translation.MacroArithmetic.rem(year, @as(c_int, 4)) == @as(c_int, 0)) and ((@import("std").zig.c_translation.MacroArithmetic.rem(year, @as(c_int, 100)) != @as(c_int, 0)) or (@import("std").zig.c_translation.MacroArithmetic.rem(year, @as(c_int, 400)) == @as(c_int, 0)))) {
    _ = &year;
    return (@import("std").zig.c_translation.MacroArithmetic.rem(year, @as(c_int, 4)) == @as(c_int, 0)) and ((@import("std").zig.c_translation.MacroArithmetic.rem(year, @as(c_int, 100)) != @as(c_int, 0)) or (@import("std").zig.c_translation.MacroArithmetic.rem(year, @as(c_int, 400)) == @as(c_int, 0)));
}
pub const __STDDEF_H = "";
pub const __need_ptrdiff_t = "";
pub const __need_wchar_t = "";
pub const __need_max_align_t = "";
pub const __need_offsetof = "";
pub const _PTRDIFF_T = "";
pub const _WCHAR_T = "";
pub const __CLANG_MAX_ALIGN_T_DEFINED = "";
pub const offsetof = @compileError("unable to translate C expr: unexpected token 'an identifier'");
// /home/gedgar/zig-linux-aarch64-0.13.0/lib/include/__stddef_offsetof.h:16:9
pub const CFL_CHANGED_SECOND = @as(c_uint, 1) << @as(c_int, 0);
pub const CFL_CHANGED_MINUTE = @as(c_uint, 1) << @as(c_int, 1);
pub const CFL_CHANGED_HOUR = @as(c_uint, 1) << @as(c_int, 2);
pub const CFL_CHANGED_DAY = @as(c_uint, 1) << @as(c_int, 3);
pub const CFL_CHANGED_DOW = @as(c_uint, 1) << @as(c_int, 4);
pub const CFL_CHANGED_DOY = @as(c_uint, 1) << @as(c_int, 5);
pub const CFL_CHANGED_MONTH = @as(c_uint, 1) << @as(c_int, 6);
pub const CFL_CHANGED_YEAR = @as(c_uint, 1) << @as(c_int, 7);
pub const CFL_FIELD_CHANGED = @compileError("unable to translate macro: undefined identifier `CFL_CHANGED_`");
// /home/gedgar/knowledge_base_assembly/zig_programs_and_containers/building_blocks/chain_tree/chain_tree_low_ram_c_test/testing/library_files/cfl_timer_system.h:391:10
pub const CHAINTREE_SUPPORT_H = "";
pub const LINK_COUNT_MASK = @as(c_int, 0x7FFF);
pub const AUTO_START_BIT = @import("std").zig.c_translation.promoteIntLiteral(c_int, 0x8000, .hex);
pub inline fn GET_LINK_COUNT(node: anytype) @TypeOf(node.*.link_count & LINK_COUNT_MASK) {
    _ = &node;
    return node.*.link_count & LINK_COUNT_MASK;
}
pub inline fn GET_AUTO_START(node: anytype) @TypeOf((node.*.link_count & AUTO_START_BIT) != @as(c_int, 0)) {
    _ = &node;
    return (node.*.link_count & AUTO_START_BIT) != @as(c_int, 0);
}
pub inline fn PACK_LINK_COUNT(count: anytype, auto_start: anytype) @TypeOf((count & LINK_COUNT_MASK) | (if (auto_start) AUTO_START_BIT else @as(c_int, 0))) {
    _ = &count;
    _ = &auto_start;
    return (count & LINK_COUNT_MASK) | (if (auto_start) AUTO_START_BIT else @as(c_int, 0));
}
pub const CFL_ENGINE_H = "";
pub const CFL_TERMINATE_SYSTEM_EVENT = @import("std").zig.c_translation.promoteIntLiteral(c_int, 0xFFFF, .hex);
pub const CFL_STOP_START_TESTS_EVENT = @import("std").zig.c_translation.promoteIntLiteral(c_int, 0xFFF0, .hex);
pub const CFL_CONTINUE = @as(c_int, 0);
pub const CFL_HALT = @as(c_int, 1);
pub const CFL_TERMINATE = @as(c_int, 2);
pub const CFL_RESET = @as(c_int, 3);
pub const CFL_DISABLE = @as(c_int, 4);
pub const CFL_SKIP_CONTINUE = @as(c_int, 5);
pub const CFL_TERMINATE_SYSTEM = @as(c_int, 6);
pub const CFL_FUNCTION_ID_STATE_MACHINE = @as(c_int, 0);
pub const CFL_FUNCTION_ID_SEQUENCE_TRY_PASS = @as(c_int, 1);
pub const CFL_FUNCTION_ID_SEQUENCE_TRY_FAIL = @as(c_int, 2);
pub const CFL_FUNCTION_ID_SUPERVISOR_MAIN = @as(c_int, 3);
pub const CFL_FUNCTION_ID_EXCEPTION_CATCH_ALL_MAIN = @as(c_int, 4);
pub const CFL_FUNCTION_ID_EXCEPTION_CATCH_MAIN = @as(c_int, 5);
pub const TEST_ACTIVE_SET = @compileError("unable to translate C expr: expected ')' instead got '|='");
// /home/gedgar/knowledge_base_assembly/zig_programs_and_containers/building_blocks/chain_tree/chain_tree_low_ram_c_test/testing/library_files/cfl_runtime.h:20:9
pub const TEST_ACTIVE_CLR = @compileError("unable to translate C expr: expected ')' instead got '&='");
// /home/gedgar/knowledge_base_assembly/zig_programs_and_containers/building_blocks/chain_tree/chain_tree_low_ram_c_test/testing/library_files/cfl_runtime.h:23:9
pub inline fn TEST_IS_ACTIVE(handle: anytype, kb_idx: anytype) @TypeOf((handle.*.active_test_bitmap[@as(usize, @intCast(@import("std").zig.c_translation.MacroArithmetic.div(kb_idx, @as(c_int, 32))))] & (@as(c_uint, 1) << @import("std").zig.c_translation.MacroArithmetic.rem(kb_idx, @as(c_int, 32)))) != @as(c_int, 0)) {
    _ = &handle;
    _ = &kb_idx;
    return (handle.*.active_test_bitmap[@as(usize, @intCast(@import("std").zig.c_translation.MacroArithmetic.div(kb_idx, @as(c_int, 32))))] & (@as(c_uint, 1) << @import("std").zig.c_translation.MacroArithmetic.rem(kb_idx, @as(c_int, 32)))) != @as(c_int, 0);
}
pub const TEST_HEADER_H = "";
pub const TEST_HEADER_NODES_H = "";
pub const CT_DEQXR7Z9_NODE_COUNT = @as(c_int, 1252);
pub const TEST_HEADER_LINKS_H = "";
pub const CT_DEQXR7Z9_LINK_TABLE_SIZE = @as(c_int, 1124);
pub const TEST_HEADER_EVENTS_H = "";
pub const _STRING_H = @as(c_int, 1);
pub const _STRINGS_H = @as(c_int, 1);
pub const CT_DEQXR7Z9_EVENT_STRING_COUNT = @as(c_int, 28);
pub const TEST_HEADER_BITMASKS_H = "";
pub const BIT_A = @as(c_int, 0);
pub const BIT_C = @as(c_int, 1);
pub const BIT_B = @as(c_int, 2);
pub const MASK_A = @as(c_uint, 1) << BIT_A;
pub const MASK_C = @as(c_uint, 1) << BIT_C;
pub const MASK_B = @as(c_uint, 1) << BIT_B;
pub const CT_DEQXR7Z9_BITMASK_COUNT = @as(c_int, 3);
pub const TEST_HEADER_KB_INFO_H = "";
pub const CT_DEQXR7Z9_KB_COUNT = @as(c_int, 19);
pub const TEST_HEADER_NODE_DATA_H = "";
pub const TEST_HEADER_FUNCTIONS_H = "";
pub const tm = struct_tm;
pub const timespec = struct_timespec;
pub const itimerspec = struct_itimerspec;
pub const sigevent = struct_sigevent;
pub const __locale_struct = struct___locale_struct;
pub const cfl_timer_context = struct_cfl_timer_context;
pub const CFL_RUNTIME_HANDLE = struct_CFL_RUNTIME_HANDLE;
