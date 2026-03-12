///  pub_sub_zig – Idiomatic Zig bindings for the NATS PubSub C library.
///
///  ## Quick start
///
///  ```zig
///  const ps = @import("pub_sub_zig");
///
///  var client = try ps.PubSub.init(.{});
///  defer client.deinit();
///  try client.connect();
///
///  try client.publishStr("sensor.temp", "{\"value\":23.5}");
///  ```

pub const pubsub = @import("pubsub.zig");
pub const status = @import("status.zig");
pub const c_api = @import("c_api.zig");

// Convenience re-exports
pub const PubSub = pubsub.PubSub;
pub const Config = pubsub.Config;
pub const Message = pubsub.Message;
pub const MessageCallback = pubsub.MessageCallback;
pub const Subscription = pubsub.Subscription;
pub const Stats = pubsub.Stats;
pub const Error = status.Error;

// Unit tests
test "Config defaults compile" {
    const cfg = Config{};
    try @import("std").testing.expectEqualStrings("nats://127.0.0.1:4222", cfg.server);
    try @import("std").testing.expectEqualStrings("default", cfg.namespace);
    try @import("std").testing.expect(cfg.client_name == null);
}