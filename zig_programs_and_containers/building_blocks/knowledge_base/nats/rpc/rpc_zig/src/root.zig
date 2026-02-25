///  rpc_zig – Idiomatic Zig bindings for the NATS RPC C library.
///
///  ## Quick start — Server
///
///  ```zig
///  const rpc = @import("rpc_zig");
///
///  var srv = try rpc.Server.init(.{});
///  defer srv.deinit();
///  try srv.register("math.add", addHandler, null, false);
///  try srv.start("rpc");
///  srv.wait();
///  ```
///
///  ## Quick start — Client
///
///  ```zig
///  var cli = try rpc.Client.init(.{});
///  defer cli.deinit();
///  try cli.connect();
///  var result = try cli.call("rpc.math.add", "{\"a\":5,\"b\":3}", 5.0);
///  defer result.deinit();
///  ```

pub const server = @import("server.zig");
pub const client = @import("client.zig");
pub const status = @import("status.zig");
pub const c_api = @import("c_api.zig");

// Convenience re-exports
pub const Server = server.Server;
pub const Client = client.Client;
pub const Config = server.Config;
pub const HandlerFn = server.HandlerFn;
pub const HandlerResult = server.HandlerResult;
pub const HandlerStats = server.HandlerStats;
pub const StatsArray = server.StatsArray;
pub const CallResult = client.CallResult;
pub const BatchEntry = client.BatchEntry;
pub const BatchResult = client.BatchResult;
pub const Error = status.Error;

// Unit tests
test "Config defaults compile" {
    const cfg = Config{};
    try @import("std").testing.expectEqualStrings("nats://127.0.0.1:4222", cfg.server);
    try @import("std").testing.expectEqualStrings("default", cfg.namespace);
    try @import("std").testing.expect(cfg.instance_id == null);
    try @import("std").testing.expect(cfg.enable_health == true);
}

test "HandlerResult union" {
    const ok_result: HandlerResult = .{ .ok = "{\"sum\":8}" };
    const err_result: HandlerResult = .{ .err = "division by zero" };
    switch (ok_result) {
        .ok => |v| try @import("std").testing.expect(v != null),
        .err => unreachable,
    }
    switch (err_result) {
        .ok => unreachable,
        .err => |v| try @import("std").testing.expect(v.len > 0),
    }
}