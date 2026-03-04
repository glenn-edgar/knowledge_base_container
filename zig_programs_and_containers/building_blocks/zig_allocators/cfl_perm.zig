// ============= REENTRANT cfl_perm.zig =============
//
// Permanent (bump) allocator — Zig port of cfl_perm.c/.h
//
// Three creation paths, each with a matching destroy:
//   1. initStatic()        — caller owns both struct and buffer (no free needed)
//   2. create()            — heap-allocated struct, external buffer
//   3. mallocCreate(size)  — heap-allocated struct + pool
//
// Allocations are permanent: there is no per-allocation free.
// Call reset() to reclaim all memory at once.

const std = @import("std");

// ========================================================================
// Global configuration — mirrors cfl_global_definitions.h
// ========================================================================
pub const BLOCK_ALIGNMENT: u16 = 8; // ARM64 8-byte alignment
pub const MIN_BLOCK_SIZE: u16 = 8;
pub const INVALID_PERM_IDX: u16 = 0xFFFF;

// Magic numbers — encode which creation path was used
pub const Magic = enum(u16) {
    none = 0x0000, // Stack/static instance (no free needed)
    create = 0x5045, // 'PE' — create() path
    malloc_create = 0x504D, // 'PM' — mallocCreate() path
    poisoned = 0x0000, // After destroy (same as none)
};

// ========================================================================
// Statistics
// ========================================================================
pub const Stats = struct {
    total_allocations: u16 = 0,
    current_used_bytes: u16 = 0,
    peak_used_bytes: u16 = 0,
    largest_allocation: u16 = 0,
    smallest_allocation: u16 = 0,
};

// ========================================================================
// Error type — replaces EXCEPTION() macro
// ========================================================================
pub const PermError = error{
    NullPointer,
    NotInitialized,
    BufferTooSmall,
    ZeroSizeAllocation,
    BadAlignment,
    OutOfMemory,
    IndexOutOfBounds,
    PointerOutOfBounds,
    AllocationFailed,
    WrongDestroyPath,
    InvalidParameters,
};

// ========================================================================
// CflPerm — permanent bump allocator instance
// ========================================================================
pub const CflPerm = struct {
    pool: []u8 = &.{}, // Slice over memory pool
    used: u16 = 0, // Bump pointer (bytes used)
    magic: Magic = .none, // Creation-path tag
    initialized: bool = false,
    owns_pool: bool = false, // True if allocator owns the pool memory
    stats: Stats = .{},

    const Self = @This();

    // ====================================================================
    // LIFECYCLE — STATIC / STACK INSTANCE
    // ====================================================================

    /// Initialize an existing (stack/static) instance.  Caller must still
    /// call `init()` with an external buffer before allocating.
    pub fn initStatic(self: *Self) void {
        self.* = .{};
        self.magic = .none;
    }

    // ====================================================================
    // LIFECYCLE — HEAP-ALLOCATED STRUCT, EXTERNAL BUFFER
    // ====================================================================

    /// Heap-allocate the CflPerm struct itself.  Caller provides the pool
    /// buffer via a subsequent `init()` call.  Destroy with `destroy()`.
    pub fn create(backing: std.mem.Allocator) PermError!*Self {
        const self = backing.create(Self) catch return PermError.AllocationFailed;
        self.* = .{};
        self.magic = .create;
        return self;
    }

    /// Destroy a struct that was made with `create()`.
    pub fn destroy(self: *Self, backing: std.mem.Allocator) PermError!void {
        if (self.magic != .create) return PermError.WrongDestroyPath;

        if (self.owns_pool and self.pool.len > 0) {
            backing.free(self.pool);
        }

        self.magic = .poisoned;
        backing.destroy(self);
    }

    // ====================================================================
    // LIFECYCLE — HEAP-ALLOCATED STRUCT + POOL
    // ====================================================================

    /// Heap-allocate both the struct and the pool.  Destroy with
    /// `mallocDestroy()`.
    pub fn mallocCreate(backing: std.mem.Allocator, size: u16) PermError!*Self {
        const self = backing.create(Self) catch return PermError.AllocationFailed;
        errdefer backing.destroy(self);

        const pool = backing.alloc(u8, size) catch return PermError.AllocationFailed;

        self.* = .{};
        self.init(pool);
        self.magic = .malloc_create;
        self.owns_pool = true;

        return self;
    }

    /// Destroy a struct that was made with `mallocCreate()`.
    pub fn mallocDestroy(self: *Self, backing: std.mem.Allocator) PermError!void {
        if (self.magic != .malloc_create) return PermError.WrongDestroyPath;

        if (self.pool.len > 0) {
            backing.free(self.pool);
        }

        self.magic = .poisoned;
        backing.destroy(self);
    }

    // ====================================================================
    // INITIALIZATION / RESET
    // ====================================================================

    /// Wire an external buffer and mark the allocator as ready.
    pub fn init(self: *Self, buffer: []u8) void {
        std.debug.assert(buffer.len >= MIN_BLOCK_SIZE);

        self.pool = buffer;
        self.used = 0;
        self.owns_pool = false;
        // magic is preserved — set by creation path
        self.stats = .{};
        @memset(self.pool, 0);
        self.initialized = true;
    }

    /// Reset bump pointer to zero — all prior allocations are logically freed.
    pub fn reset(self: *Self) void {
        std.debug.assert(self.pool.len > 0);

        self.stats = .{};
        self.used = 0;
        @memset(self.pool, 0);
        self.initialized = true;
    }

    // ====================================================================
    // ALLOCATION — returns index into pool
    // ====================================================================

    /// Allocate `size_bytes` with default ARM alignment; returns pool index.
    pub fn alloc(self: *Self, size_bytes: u16) PermError!u16 {
        return self.allocAligned(size_bytes, BLOCK_ALIGNMENT);
    }

    /// Allocate `size_bytes` with custom alignment; returns pool index.
    pub fn allocAligned(self: *Self, size_bytes_raw: u16, alignment: u16) PermError!u16 {
        if (!self.initialized) return PermError.NotInitialized;
        if (size_bytes_raw == 0) return PermError.ZeroSizeAllocation;
        if (alignment == 0 or !isPowerOf2(alignment)) return PermError.BadAlignment;

        // Round size up to BLOCK_ALIGNMENT
        var size_bytes = alignUp(size_bytes_raw, BLOCK_ALIGNMENT);
        if (size_bytes < MIN_BLOCK_SIZE) size_bytes = MIN_BLOCK_SIZE;

        // Calculate aligned position using absolute address
        const current_addr = @intFromPtr(self.pool.ptr) + self.used;
        const mask = @as(usize, alignment) - 1;
        const aligned_addr = (current_addr + mask) & ~mask;
        const padding: u16 = @intCast(aligned_addr - current_addr);

        const total_needed = padding + size_bytes;

        if (self.used + total_needed > @as(u16, @intCast(self.pool.len))) {
            return PermError.OutOfMemory;
        }

        const ret_idx = self.used + padding;
        self.used += total_needed;

        self.updateStats(size_bytes);

        return ret_idx;
    }

    // ====================================================================
    // ALLOCATION — returns pointer
    // ====================================================================

    /// Allocate and return a typed pointer with default alignment.
    pub fn allocPointer(self: *Self, comptime T: type) PermError!*T {
        const size: u16 = @intCast(@sizeOf(T));
        const alignment: u16 = @intCast(@alignOf(T));
        const actual_align = if (alignment < BLOCK_ALIGNMENT) BLOCK_ALIGNMENT else alignment;
        const idx = try self.allocAligned(size, actual_align);
        return self.ptrAs(T, idx);
    }

    /// Allocate a byte slice of `size_bytes` with default alignment.
    pub fn allocBytes(self: *Self, size_bytes: u16) PermError![]u8 {
        const idx = try self.alloc(size_bytes);
        return self.pool[idx..][0..size_bytes];
    }

    /// Allocate a byte slice with custom alignment.
    pub fn allocBytesAligned(self: *Self, size_bytes: u16, alignment: u16) PermError![]u8 {
        const idx = try self.allocAligned(size_bytes, alignment);
        return self.pool[idx..][0..size_bytes];
    }

    // ====================================================================
    // INDEX / POINTER CONVERSION
    // ====================================================================

    /// Convert pool index to a raw pointer.
    pub fn ptr(self: *Self, idx: u16) PermError!*u8 {
        if (!self.initialized) return PermError.NotInitialized;
        if (idx >= self.pool.len) return PermError.IndexOutOfBounds;
        return &self.pool[idx];
    }

    /// Convert pool index to a typed pointer (with alignment cast).
    pub fn ptrAs(self: *Self, comptime T: type, idx: u16) PermError!*T {
        if (!self.initialized) return PermError.NotInitialized;
        if (idx + @sizeOf(T) > self.pool.len) return PermError.IndexOutOfBounds;
        const raw = self.pool[idx..];
        return @ptrCast(@alignCast(raw.ptr));
    }

    /// Convert a pointer within the pool back to an index.
    pub fn ptrToIdx(self: *Self, p: *const u8) PermError!u16 {
        if (!self.initialized) return PermError.NotInitialized;
        const base = @intFromPtr(self.pool.ptr);
        const addr = @intFromPtr(p);
        if (addr < base or addr >= base + self.pool.len) {
            return PermError.PointerOutOfBounds;
        }
        return @intCast(addr - base);
    }

    // ====================================================================
    // DIAGNOSTICS
    // ====================================================================

    pub fn usedBytes(self: *const Self) u16 {
        if (!self.initialized) return 0;
        return self.used;
    }

    pub fn freeBytes(self: *const Self) u16 {
        if (!self.initialized) return 0;
        return @as(u16, @intCast(self.pool.len)) - self.used;
    }

    pub fn getStats(self: *const Self) PermError!Stats {
        if (!self.initialized) return PermError.NotInitialized;
        return self.stats;
    }

    pub fn validate(self: *const Self) bool {
        if (!self.initialized) return false;
        if (self.used > self.pool.len) return false;
        if (self.stats.current_used_bytes != self.used) return false;
        if (self.stats.peak_used_bytes < self.used) return false;
        return true;
    }

    // ====================================================================
    // Zig Allocator interface — allows using CflPerm as a std.mem.Allocator
    // ====================================================================

    pub fn allocator(self: *Self) std.mem.Allocator {
        return .{
            .ptr = self,
            .vtable = &.{
                .alloc = zigAlloc,
                .resize = zigResize,
                .free = zigFree,
            },
        };
    }

    fn zigAlloc(ctx: *anyopaque, len: usize, ptr_align: u8, _: usize) ?[*]u8 {
        const self: *Self = @ptrCast(@alignCast(ctx));
        const alignment: u16 = @as(u16, 1) << @intCast(ptr_align);
        const size: u16 = @intCast(@min(len, std.math.maxInt(u16)));
        const idx = self.allocAligned(size, alignment) catch return null;
        return self.pool.ptr + idx;
    }

    fn zigResize(_: *anyopaque, _: []u8, _: u8, _: usize, _: usize) bool {
        // Bump allocator cannot resize
        return false;
    }

    fn zigFree(_: *anyopaque, _: []u8, _: u8, _: usize) void {
        // Bump allocator: free is a no-op
    }

    // ====================================================================
    // INTERNAL HELPERS
    // ====================================================================

    fn updateStats(self: *Self, allocated_size: u16) void {
        self.stats.total_allocations += 1;
        self.stats.current_used_bytes = self.used;

        if (self.used > self.stats.peak_used_bytes) {
            self.stats.peak_used_bytes = self.used;
        }

        if (allocated_size > self.stats.largest_allocation) {
            self.stats.largest_allocation = allocated_size;
        }

        if (self.stats.total_allocations == 1 or allocated_size < self.stats.smallest_allocation) {
            self.stats.smallest_allocation = allocated_size;
        }
    }
};

// ========================================================================
// Free-standing helpers
// ========================================================================

fn alignUp(value: u16, alignment: u16) u16 {
    std.debug.assert(alignment != 0 and isPowerOf2(alignment));
    return (value + alignment - 1) & ~(alignment - 1);
}

fn isPowerOf2(value: u16) bool {
    return value != 0 and (value & (value - 1)) == 0;
}

// ========================================================================
// STATIC ALLOCATION HELPER
//
// Usage:
//   var my_perm = CflPerm.StaticStorage(4096).instance();
//   my_perm.init(my_perm.buffer());   // or use the combined initFromStatic()
// ========================================================================

pub fn StaticPerm(comptime size: usize) type {
    return struct {
        storage: [@sizeOf(CflPerm) + size]u8 align(BLOCK_ALIGNMENT) = undefined,

        const SelfStatic = @This();

        pub fn perm(self: *SelfStatic) *CflPerm {
            return @ptrCast(@alignCast(&self.storage));
        }

        pub fn buffer(self: *SelfStatic) []u8 {
            return self.storage[@sizeOf(CflPerm)..];
        }

        /// Combined init: sets up the CflPerm and wires the buffer.
        pub fn initAndWire(self: *SelfStatic) *CflPerm {
            const p = self.perm();
            p.initStatic();
            p.init(self.buffer());
            return p;
        }
    };
}

// ========================================================================
// TESTS
// ========================================================================

test "basic bump allocation" {
    var backing: [1024]u8 = undefined;
    var perm: CflPerm = .{};
    perm.initStatic();
    perm.init(&backing);

    const idx1 = try perm.alloc(16);
    try std.testing.expect(idx1 != INVALID_PERM_IDX);

    const idx2 = try perm.alloc(32);
    try std.testing.expect(idx2 > idx1);

    try std.testing.expect(perm.stats.total_allocations == 2);
    try std.testing.expect(perm.validate());
}

test "static perm helper" {
    var sp = StaticPerm(512){};
    const p = sp.initAndWire();

    const idx = try p.alloc(64);
    try std.testing.expect(idx != INVALID_PERM_IDX);
    try std.testing.expect(p.usedBytes() >= 64);
}

test "typed pointer allocation" {
    var backing: [1024]u8 = undefined;
    var perm: CflPerm = .{};
    perm.initStatic();
    perm.init(&backing);

    const val = try perm.allocPointer(u32);
    val.* = 0xDEADBEEF;
    try std.testing.expectEqual(@as(u32, 0xDEADBEEF), val.*);
}

test "out of memory" {
    var backing: [32]u8 = undefined;
    var perm: CflPerm = .{};
    perm.initStatic();
    perm.init(&backing);

    _ = try perm.alloc(8);
    _ = try perm.alloc(8);

    // Should fail — only 32 bytes total, already used ≥16
    const result = perm.alloc(24);
    try std.testing.expectError(PermError.OutOfMemory, result);
}

test "reset reclaims all memory" {
    var backing: [128]u8 = undefined;
    var perm: CflPerm = .{};
    perm.initStatic();
    perm.init(&backing);

    _ = try perm.alloc(64);
    try std.testing.expect(perm.usedBytes() >= 64);

    perm.reset();
    try std.testing.expectEqual(@as(u16, 0), perm.usedBytes());
    try std.testing.expectEqual(@as(u16, 0), perm.stats.total_allocations);
}

test "std.mem.Allocator interface" {
    var backing: [4096]u8 = undefined;
    var perm: CflPerm = .{};
    perm.initStatic();
    perm.init(&backing);

    const a = perm.allocator();
    const slice = try a.alloc(u8, 128);
    try std.testing.expectEqual(@as(usize, 128), slice.len);
}
