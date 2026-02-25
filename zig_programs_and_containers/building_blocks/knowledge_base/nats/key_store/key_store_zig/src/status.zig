const c = @import("c_api.zig");

/// Unified error set for all NATS KeyStore / KbStore / JobQueue operations.
pub const Error = error{
    InvalidArg,
    ConnectionFailed,
    NotFound,
    BucketError,
    EncodeError,
    DecodeError,
    OutOfMemory,
    RetryExhausted,
    NotNumeric,
    NatsError,
    Unknown,
};

/// Convert a C ks_status_t into a Zig error (or void on success).
pub fn check(status: c.CStatus) Error!void {
    return switch (status) {
        c.KS_OK => {},
        c.KS_ERR_INVALID_ARG => Error.InvalidArg,
        c.KS_ERR_CONNECTION => Error.ConnectionFailed,
        c.KS_ERR_NOT_FOUND => Error.NotFound,
        c.KS_ERR_BUCKET => Error.BucketError,
        c.KS_ERR_ENCODE => Error.EncodeError,
        c.KS_ERR_DECODE => Error.DecodeError,
        c.KS_ERR_MEMORY => Error.OutOfMemory,
        c.KS_ERR_RETRY_EXHAUSTED => Error.RetryExhausted,
        c.KS_ERR_NOT_NUMERIC => Error.NotNumeric,
        c.KS_ERR_NATS => Error.NatsError,
        else => Error.Unknown,
    };
}

/// Return the human-readable string for a C status code.
pub fn statusString(status: c.CStatus) []const u8 {
    const ptr = c.ks_status_str(status);
    if (ptr) |p| {
        return std.mem.span(p);
    }
    return "unknown error";
}

const std = @import("std");