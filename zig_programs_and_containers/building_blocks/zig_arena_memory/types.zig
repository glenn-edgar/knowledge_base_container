const std = @import("std");

/// Configuration struct for the ArenaEnv type factory.
/// Each distinct configuration produces a distinct Zig type at comptime.
pub const EnvConfig = struct {
    /// The value type stored in the symbol table. May be a struct or tagged union.
    Value: type,

    /// Backing allocator type. Defaults to the std.mem.Allocator interface.
    /// May be a concrete allocator pointer type (e.g. *GeneralPurposeAllocator(.{}))
    /// in which case it must expose an allocator() method returning std.mem.Allocator.
    Allocator: type = std.mem.Allocator,

    /// Include mutex for thread-safe access. When false, mutex is void (zero cost).
    threadsafe: bool = true,

    /// When true, read transactions skip mutex acquisition.
    /// Only safe when write phase completes before read phase begins.
    concurrent_reads: bool = true,

    /// Pre-allocation hint for hash table bucket count.
    initial_capacity: u32 = 64,

    /// When true, []const u8 fields inside Value are duped into the arena at put() time.
    intern_strings: bool = true,
};