const c = @import("c_api.zig");
const std = @import("std");

pub const Error = error{
    InvalidArg,
    ConnectionFailed,
    Timeout,
    EncodeError,
    DecodeError,
    OutOfMemory,
    HandlerError,
    NotFound,
    NatsError,
    Unknown,
};

pub fn check(st: c.rpc_status_t) Error!void {
    return switch (st) {
        c.RPC_OK => {},
        c.RPC_ERR_INVALID_ARG => Error.InvalidArg,
        c.RPC_ERR_CONNECTION => Error.ConnectionFailed,
        c.RPC_ERR_TIMEOUT => Error.Timeout,
        c.RPC_ERR_ENCODE => Error.EncodeError,
        c.RPC_ERR_DECODE => Error.DecodeError,
        c.RPC_ERR_MEMORY => Error.OutOfMemory,
        c.RPC_ERR_HANDLER => Error.HandlerError,
        c.RPC_ERR_NOT_FOUND => Error.NotFound,
        c.RPC_ERR_NATS => Error.NatsError,
        else => Error.Unknown,
    };
}

pub fn statusString(st: c.rpc_status_t) []const u8 {
    const ptr = c.rpc_status_str(st);
    if (ptr) |p| {
        return std.mem.span(p);
    }
    return "unknown";
}