const c = @import("c_api.zig");
const std = @import("std");

pub const Error = error{
    InvalidArg,
    ConnectionFailed,
    Timeout,
    OutOfMemory,
    NotConnected,
    NatsError,
    Unknown,
};

pub fn check(status: c.ps_status_t) Error!void {
    return switch (status) {
        c.PS_OK => {},
        c.PS_ERR_INVALID_ARG => Error.InvalidArg,
        c.PS_ERR_CONNECTION => Error.ConnectionFailed,
        c.PS_ERR_TIMEOUT => Error.Timeout,
        c.PS_ERR_MEMORY => Error.OutOfMemory,
        c.PS_ERR_NOT_CONNECTED => Error.NotConnected,
        c.PS_ERR_NATS => Error.NatsError,
        else => Error.Unknown,
    };
}

pub fn statusString(status: c.ps_status_t) []const u8 {
    const ptr = c.ps_status_str(status);
    if (ptr) |p| {
        return std.mem.span(p);
    }
    return "unknown";
}