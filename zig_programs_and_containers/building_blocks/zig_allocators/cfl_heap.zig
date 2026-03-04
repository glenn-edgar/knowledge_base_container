// ============= REENTRANT cfl_heap.zig =============
//
// Heap allocator with splitting, coalescing, and node-ID tracking.
// Zig port of cfl_heap.c/.h
//
// The heap is initialised from a CflPerm bump allocator: both the
// CflHeap struct and the backing pool are permanently allocated from
// perm.  Individual blocks can be malloc'd and free'd as usual.
//
// Each block carries:
//   • header magic  (free 0xF2EE / allocated 0xA10C)
//   • footer magic  (0xF007)
//   • requesting node_id for arena-style tracking
//
// Freed adjacent blocks are coalesced automatically.

const std = @import("std");
const perm_mod = @import("cfl_perm.zig");
const CflPerm = perm_mod.CflPerm;

// ========================================================================
// Configuration — mirrors cfl_global_definitions.h
// ========================================================================
pub const BLOCK_ALIGNMENT: u16 = perm_mod.BLOCK_ALIGNMENT;
pub const MIN_BLOCK_SIZE: u16 = perm_mod.MIN_BLOCK_SIZE;
pub const INVALID_HEAP_IDX: u16 = 0xFFFF;
pub const NODE_ID_NONE: u16 = 0xFFFF;

// Guard magic numbers for corruption detection
const BLOCK_MAGIC_FREE: u16 = 0xF2EE;
const BLOCK_MAGIC_ALLOC: u16 = 0xA10C;
const BLOCK_FOOTER_MAGIC: u16 = 0xF007;

const FLAG_ALLOCATED: u16 = 0x0001;

// ========================================================================
// Block header — laid out to match C sizeof / alignment
// ========================================================================
const BlockHeader = extern struct {
    magic: u16, // Guard: BLOCK_MAGIC_FREE or BLOCK_MAGIC_ALLOC
    size: u16, // Size of data area (not including header overhead)
    flags: u16, // Allocation flags
    node_id: u16, // ID of requesting node/component
    padding: u16, // Alignment padding amount (bytes before data)
    _reserved0: u16 = 0, // Pad to 16 bytes for ARM64 alignment
    _reserved1: u16 = 0,
    _reserved2: u16 = 0,
};

// Header is 16 bytes (8 × u16) — multiple of 8 for ARM64 alignment.
// Footer is 8 bytes (padded from u16 magic) so that
// HEADER(16) + data(multiple of 8) + FOOTER(8) = multiple of 8,
// ensuring the next BlockHeader is always 8-byte aligned.
const HEADER_SIZE: u16 = @sizeOf(BlockHeader); // 16
const FOOTER_SIZE: u16 = 8; // Padded for ARM64 alignment
const FOOTER_MAGIC_OFFSET: u16 = 0; // magic u16 sits at start of footer region

// ========================================================================
// Statistics
// ========================================================================
pub const HeapStats = struct {
    total_allocations: u16 = 0,
    total_frees: u16 = 0,
    current_blocks: u16 = 0,
    current_used_bytes: u16 = 0,
    peak_used_bytes: u16 = 0,
    largest_free_block: u16 = 0,
    free_blocks: u16 = 0,
    allocated_blocks: u16 = 0,
};

// ========================================================================
// Error type — replaces EXCEPTION() macro
// ========================================================================
pub const HeapError = error{
    NullPointer,
    NotInitialized,
    BufferTooSmall,
    ZeroSizeAllocation,
    BadAlignment,
    OutOfMemory,
    IndexOutOfBounds,
    PointerOutOfBounds,
    HeapCorruption,
    DoubleFree,
    PointerNotFound,
    InvalidParameters,
};

// ========================================================================
// CflHeap — heap allocator instance
// ========================================================================
pub const CflHeap = struct {
    pool: []u8 = &.{},
    initialized: bool = false,
    owns_pool: bool = false,
    stats: HeapStats = .{},

    const Self = @This();

    // ====================================================================
    // LIFECYCLE
    // ====================================================================

    /// Initialize heap — allocates both struct and pool from a CflPerm.
    pub fn initFromPerm(perm: *CflPerm, buffer_size: u16) HeapError!*Self {
        if (buffer_size < HEADER_SIZE + MIN_BLOCK_SIZE + FOOTER_SIZE) {
            return HeapError.BufferTooSmall;
        }

        // Allocate heap struct from perm
        const heap = perm.allocPointer(Self) catch return HeapError.OutOfMemory;

        // Allocate pool from perm
        const pool = perm.allocBytes(buffer_size) catch return HeapError.OutOfMemory;

        heap.* = .{
            .pool = pool,
            .owns_pool = false,
            .stats = .{},
        };

        // Zero the pool
        @memset(heap.pool, 0);

        // Create the initial free block spanning the entire pool
        heap.initFreeBlock();
        heap.initialized = true;
        heap.recalcStats();

        return heap;
    }

    /// Reset heap to initial state — all allocations lost.
    pub fn reset(self: *Self) void {
        std.debug.assert(self.pool.len > 0);

        self.stats = .{};
        @memset(self.pool, 0);
        self.initFreeBlock();
        self.initialized = true;
        self.recalcStats();
    }

    // ====================================================================
    // ALLOCATION — returns pool index
    // ====================================================================

    /// Allocate with default (4-byte) alignment; returns pool index.
    pub fn malloc(self: *Self, size_bytes: u16) HeapError!u16 {
        return self.arenaAllocAligned(NODE_ID_NONE, size_bytes, BLOCK_ALIGNMENT);
    }

    /// Allocate with node-ID tracking and custom alignment; returns pool index.
    pub fn arenaAllocAligned(
        self: *Self,
        requesting_node_id: u16,
        size_bytes_raw: u16,
        alignment: u16,
    ) HeapError!u16 {
        if (!self.initialized) return HeapError.NotInitialized;
        if (size_bytes_raw == 0) return HeapError.ZeroSizeAllocation;
        if (alignment == 0 or !isPowerOf2(alignment)) return HeapError.BadAlignment;

        var size_bytes = alignUp(size_bytes_raw, BLOCK_ALIGNMENT);
        if (size_bytes < MIN_BLOCK_SIZE) size_bytes = MIN_BLOCK_SIZE;

        var offset: u16 = 0;
        while (offset < self.pool.len) {
            const block = self.blockAt(offset) orelse break;

            if (!self.validateBlock(block)) return HeapError.HeapCorruption;

            if (!isAllocated(block)) {
                // Calculate aligned data position
                const data_start = offset + HEADER_SIZE;
                const data_addr = @intFromPtr(self.pool.ptr) + data_start;
                const mask = @as(usize, alignment) - 1;
                const aligned_addr = (data_addr + mask) & ~mask;
                const padding: u16 = @intCast(aligned_addr - data_addr);
                const total_needed = padding + size_bytes;

                if (block.size >= total_needed) {
                    // Split if remainder is large enough for a new block
                    const remainder = block.size - total_needed;
                    if (remainder >= HEADER_SIZE + MIN_BLOCK_SIZE + FOOTER_SIZE) {
                        const new_offset = offset + HEADER_SIZE + total_needed + FOOTER_SIZE;
                        if (self.blockAt(new_offset)) |new_block| {
                            new_block.magic = BLOCK_MAGIC_FREE;
                            new_block.size = remainder - HEADER_SIZE - FOOTER_SIZE;
                            new_block.flags = 0;
                            new_block.node_id = NODE_ID_NONE;
                            new_block.padding = 0;
                            self.setFooter(new_block, new_offset);
                        }
                        block.size = total_needed;
                    }

                    block.padding = padding;
                    markAllocated(block, requesting_node_id);
                    self.setFooter(block, offset);

                    self.stats.total_allocations += 1;
                    self.recalcStats();

                    // Return index to the ALIGNED data pointer
                    return data_start + padding;
                }
            }

            // Advance to next block
            offset += HEADER_SIZE + block.size + FOOTER_SIZE;
        }

        return HeapError.OutOfMemory;
    }

    // ====================================================================
    // ALLOCATION — returns pointer
    // ====================================================================

    /// Allocate and return a byte slice.
    pub fn mallocPointer(self: *Self, size_bytes: u16) HeapError![]u8 {
        const idx = try self.malloc(size_bytes);
        return self.pool[idx..][0..size_bytes];
    }

    /// Allocate a typed pointer via arena alloc.
    pub fn mallocTyped(self: *Self, comptime T: type) HeapError!*T {
        const size: u16 = @intCast(@sizeOf(T));
        const alignment: u16 = @intCast(@max(@alignOf(T), BLOCK_ALIGNMENT));
        const idx = try self.arenaAllocAligned(NODE_ID_NONE, size, alignment);
        return @ptrCast(@alignCast(&self.pool[idx]));
    }

    // ====================================================================
    // FREE
    // ====================================================================

    /// Free a block by its pool index.
    pub fn free(self: *Self, idx: u16) HeapError!void {
        if (!self.initialized) return HeapError.NotInitialized;
        if (idx >= self.pool.len) return HeapError.IndexOutOfBounds;

        // Walk blocks to find which one contains this index
        var offset: u16 = 0;
        while (offset < self.pool.len) {
            const block = self.blockAt(offset) orelse break;

            const data_start = offset + HEADER_SIZE + block.padding;
            const data_end = offset + HEADER_SIZE + block.size;

            if (idx >= data_start and idx < data_end) {
                // Found the target block
                if (!self.validateBlock(block)) return HeapError.HeapCorruption;
                if (!isAllocated(block)) return HeapError.DoubleFree;

                markFree(block);
                block.padding = 0;
                self.setFooter(block, offset);

                self.stats.total_frees += 1;
                self.coalesceFreeBlocks();
                self.recalcStats();
                return;
            }

            offset += HEADER_SIZE + block.size + FOOTER_SIZE;
        }

        return HeapError.PointerNotFound;
    }

    /// Free a block by raw pointer.
    pub fn freePointer(self: *Self, p: [*]u8) HeapError!void {
        const idx = self.ptrToIdx(p) catch return HeapError.PointerOutOfBounds;
        return self.free(idx);
    }

    // ====================================================================
    // INDEX / POINTER CONVERSION
    // ====================================================================

    /// Convert pool index to pointer.
    pub fn ptr(self: *Self, idx: u16) HeapError!*u8 {
        if (!self.initialized) return HeapError.NotInitialized;
        if (idx >= self.pool.len) return HeapError.IndexOutOfBounds;
        return &self.pool[idx];
    }

    /// Convert pointer to pool index.
    pub fn ptrToIdx(self: *Self, p: [*]const u8) HeapError!u16 {
        if (!self.initialized) return HeapError.NotInitialized;
        const base = @intFromPtr(self.pool.ptr);
        const addr = @intFromPtr(p);
        if (addr < base or addr >= base + self.pool.len) {
            return HeapError.PointerOutOfBounds;
        }
        return @intCast(addr - base);
    }

    // ====================================================================
    // DIAGNOSTICS
    // ====================================================================

    pub fn usedBytes(self: *const Self) u16 {
        if (!self.initialized) return 0;
        return self.stats.current_used_bytes;
    }

    pub fn freeBytes(self: *const Self) u16 {
        if (!self.initialized) return 0;
        return @as(u16, @intCast(self.pool.len)) - self.stats.current_used_bytes;
    }

    pub fn getStats(self: *Self) HeapError!HeapStats {
        if (!self.initialized) return HeapError.NotInitialized;
        self.recalcStats();
        return self.stats;
    }

    pub fn validate(self: *Self) bool {
        if (!self.initialized) return false;

        var offset: u16 = 0;
        var total_size: u16 = 0;

        while (offset < self.pool.len) {
            const block = self.blockAt(offset) orelse break;

            if (!self.validateBlock(block)) return false;

            const block_total = HEADER_SIZE + block.size + FOOTER_SIZE;
            total_size += block_total;
            if (total_size > self.pool.len) return false;

            offset += block_total;
        }

        return true;
    }

    /// Get the node_id that owns the block containing `idx`.
    pub fn getNodeId(self: *Self, idx: u16) HeapError!u16 {
        if (!self.initialized) return HeapError.NotInitialized;
        if (idx >= self.pool.len) return HeapError.IndexOutOfBounds;

        var offset: u16 = 0;
        while (offset < self.pool.len) {
            const block = self.blockAt(offset) orelse break;

            const data_start = offset + HEADER_SIZE + block.padding;
            const data_end = offset + HEADER_SIZE + block.size;

            if (idx >= data_start and idx < data_end) {
                return block.node_id;
            }

            offset += HEADER_SIZE + block.size + FOOTER_SIZE;
        }

        return HeapError.PointerNotFound;
    }

    /// Walk all blocks, calling `callback` for each.
    pub fn walk(
        self: *Self,
        context: anytype,
        callback: fn (ctx: @TypeOf(context), data_ptr: []u8, allocated: bool, node_id: u16) void,
    ) void {
        if (!self.initialized) return;

        var offset: u16 = 0;
        while (offset < self.pool.len) {
            const block = self.blockAt(offset) orelse break;

            if (self.validateBlock(block)) {
                const data_start = offset + HEADER_SIZE + block.padding;
                const usable_size = block.size - block.padding;
                const data_slice = self.pool[data_start..][0..usable_size];
                callback(context, data_slice, isAllocated(block), block.node_id);
            }

            offset += HEADER_SIZE + block.size + FOOTER_SIZE;
        }
    }

    // ====================================================================
    // Zig Allocator interface
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
        const idx = self.arenaAllocAligned(NODE_ID_NONE, size, alignment) catch return null;
        return self.pool.ptr + idx;
    }

    fn zigResize(_: *anyopaque, _: []u8, _: u8, _: usize, _: usize) bool {
        return false;
    }

    fn zigFree(ctx: *anyopaque, buf: []u8, _: u8, _: usize) void {
        const self: *Self = @ptrCast(@alignCast(ctx));
        const idx = self.ptrToIdx(buf.ptr) catch return;
        self.free(idx) catch {};
    }

    // ====================================================================
    // INTERNAL HELPERS
    // ====================================================================

    /// Interpret pool bytes at `offset` as a BlockHeader.
    fn blockAt(self: *Self, offset: u16) ?*BlockHeader {
        if (offset + HEADER_SIZE > self.pool.len) return null;
        return @ptrCast(@alignCast(&self.pool[offset]));
    }

    /// Create the initial free block spanning the entire pool.
    fn initFreeBlock(self: *Self) void {
        const pool_size: u16 = @intCast(self.pool.len);
        if (self.blockAt(0)) |block| {
            block.magic = BLOCK_MAGIC_FREE;
            block.size = pool_size - HEADER_SIZE - FOOTER_SIZE;
            block.flags = 0;
            block.node_id = NODE_ID_NONE;
            block.padding = 0;
            self.setFooter(block, 0);
        }
    }

    /// Write footer magic after the data area of a block at `block_offset`.
    fn setFooter(self: *Self, block: *const BlockHeader, block_offset: u16) void {
        const footer_offset = block_offset + HEADER_SIZE + block.size + FOOTER_MAGIC_OFFSET;
        if (footer_offset + @sizeOf(u16) <= self.pool.len) {
            const footer: *align(1) u16 = @ptrCast(&self.pool[footer_offset]);
            footer.* = BLOCK_FOOTER_MAGIC;
        }
    }

    /// Validate header + footer magic of a block at its current position.
    fn validateBlock(self: *Self, block: *const BlockHeader) bool {
        if (block.magic != BLOCK_MAGIC_FREE and block.magic != BLOCK_MAGIC_ALLOC) {
            return false;
        }
        // Find offset of block in pool to locate footer
        const block_addr = @intFromPtr(block);
        const pool_addr = @intFromPtr(self.pool.ptr);
        if (block_addr < pool_addr) return false;
        const block_offset: u16 = @intCast(block_addr - pool_addr);
        const footer_offset = block_offset + HEADER_SIZE + block.size + FOOTER_MAGIC_OFFSET;
        if (footer_offset + @sizeOf(u16) > self.pool.len) return false;
        const footer: *align(1) const u16 = @ptrCast(&self.pool[footer_offset]);
        return footer.* == BLOCK_FOOTER_MAGIC;
    }

    /// Coalesce adjacent free blocks.
    fn coalesceFreeBlocks(self: *Self) void {
        var offset: u16 = 0;
        while (offset < self.pool.len) {
            const block = self.blockAt(offset) orelse break;
            const next_offset = offset + HEADER_SIZE + block.size + FOOTER_SIZE;

            if (!isAllocated(block)) {
                if (self.blockAt(next_offset)) |next| {
                    if (!isAllocated(next)) {
                        // Merge next block into current
                        block.size += HEADER_SIZE + next.size + FOOTER_SIZE;
                        block.padding = 0;
                        self.setFooter(block, offset);
                        continue; // Don't advance — check for further merges
                    }
                }
            }

            offset = next_offset;
        }
    }

    /// Recalculate heap statistics by walking all blocks.
    fn recalcStats(self: *Self) void {
        var used: u16 = 0;
        var free_blocks: u16 = 0;
        var allocated_blocks: u16 = 0;
        var largest_free: u16 = 0;
        var total_blocks: u16 = 0;

        var offset: u16 = 0;
        while (offset < self.pool.len) {
            const block = self.blockAt(offset) orelse break;
            total_blocks += 1;

            if (isAllocated(block)) {
                allocated_blocks += 1;
                used += HEADER_SIZE + block.size + FOOTER_SIZE;
            } else {
                free_blocks += 1;
                if (block.size > largest_free) largest_free = block.size;
            }

            offset += HEADER_SIZE + block.size + FOOTER_SIZE;
        }

        self.stats.current_blocks = total_blocks;
        self.stats.allocated_blocks = allocated_blocks;
        self.stats.free_blocks = free_blocks;
        self.stats.current_used_bytes = used;
        self.stats.largest_free_block = largest_free;

        if (used > self.stats.peak_used_bytes) {
            self.stats.peak_used_bytes = used;
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

fn isAllocated(block: *const BlockHeader) bool {
    return (block.flags & FLAG_ALLOCATED) != 0;
}

fn markAllocated(block: *BlockHeader, node_id: u16) void {
    block.flags |= FLAG_ALLOCATED;
    block.magic = BLOCK_MAGIC_ALLOC;
    block.node_id = node_id;
}

fn markFree(block: *BlockHeader) void {
    block.flags &= ~FLAG_ALLOCATED;
    block.magic = BLOCK_MAGIC_FREE;
    block.node_id = NODE_ID_NONE;
}

// ========================================================================
// TESTS
// ========================================================================

test "heap init and validate" {
    var perm_backing: [4096]u8 = undefined;
    var perm: CflPerm = .{};
    perm.initStatic();
    perm.init(&perm_backing);

    const heap = try CflHeap.initFromPerm(&perm, 512);
    try std.testing.expect(heap.initialized);
    try std.testing.expect(heap.validate());
}

test "malloc and free" {
    var perm_backing: [4096]u8 = undefined;
    var perm: CflPerm = .{};
    perm.initStatic();
    perm.init(&perm_backing);

    const heap = try CflHeap.initFromPerm(&perm, 512);

    const idx1 = try heap.malloc(32);
    try std.testing.expect(idx1 != INVALID_HEAP_IDX);

    const idx2 = try heap.malloc(64);
    try std.testing.expect(idx2 != INVALID_HEAP_IDX);
    try std.testing.expect(idx2 != idx1);

    // Free first block
    try heap.free(idx1);
    try std.testing.expect(heap.validate());

    // Free second block
    try heap.free(idx2);
    try std.testing.expect(heap.validate());
}

test "coalescing" {
    var perm_backing: [4096]u8 = undefined;
    var perm: CflPerm = .{};
    perm.initStatic();
    perm.init(&perm_backing);

    const heap = try CflHeap.initFromPerm(&perm, 512);

    const a = try heap.malloc(32);
    const b = try heap.malloc(32);
    const c = try heap.malloc(32);

    // Free middle, then neighbours — should coalesce
    try heap.free(b);
    try heap.free(a);
    try heap.free(c);

    // After coalescing everything, should have one large free block
    const stats = try heap.getStats();
    try std.testing.expectEqual(@as(u16, 1), stats.free_blocks);
    try std.testing.expect(heap.validate());
}

test "double free detection" {
    var perm_backing: [4096]u8 = undefined;
    var perm: CflPerm = .{};
    perm.initStatic();
    perm.init(&perm_backing);

    const heap = try CflHeap.initFromPerm(&perm, 512);
    const idx = try heap.malloc(32);
    try heap.free(idx);

    // Second free should fail
    const result = heap.free(idx);
    try std.testing.expectError(HeapError.DoubleFree, result);
}

test "node id tracking" {
    var perm_backing: [4096]u8 = undefined;
    var perm: CflPerm = .{};
    perm.initStatic();
    perm.init(&perm_backing);

    const heap = try CflHeap.initFromPerm(&perm, 512);

    const idx = try heap.arenaAllocAligned(42, 32, BLOCK_ALIGNMENT);
    const node_id = try heap.getNodeId(idx);
    try std.testing.expectEqual(@as(u16, 42), node_id);
}

test "reset reclaims everything" {
    var perm_backing: [4096]u8 = undefined;
    var perm: CflPerm = .{};
    perm.initStatic();
    perm.init(&perm_backing);

    const heap = try CflHeap.initFromPerm(&perm, 512);

    _ = try heap.malloc(32);
    _ = try heap.malloc(64);

    heap.reset();

    try std.testing.expect(heap.validate());
    // After reset should be able to allocate nearly the full pool
    _ = try heap.malloc(400);
}

test "out of memory" {
    var perm_backing: [4096]u8 = undefined;
    var perm: CflPerm = .{};
    perm.initStatic();
    perm.init(&perm_backing);

    const heap = try CflHeap.initFromPerm(&perm, 128);

    // Fill up the heap
    _ = try heap.malloc(32);
    _ = try heap.malloc(32);

    // This should fail
    const result = heap.malloc(128);
    try std.testing.expectError(HeapError.OutOfMemory, result);
}
