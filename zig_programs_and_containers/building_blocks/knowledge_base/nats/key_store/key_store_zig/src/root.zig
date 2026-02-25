///  key_store_zig – Idiomatic Zig bindings for the NATS JetStream
///  KeyStore, KbStore (Knowledge Base), and JobQueue C libraries.
///
///  ## Quick start
///
///  ```zig
///  const nats = @import("key_store_zig");
///
///  var ks = try nats.KeyStore.init(.{});
///  defer ks.deinit();
///  try ks.connect();
///
///  _ = try ks.put("greeting", "\"Hello from Zig!\"");
///  const val = try ks.getRaw("greeting");
///  defer nats.KeyStore.freeRaw(val);
///  ```

// Sub-modules
pub const key_store = @import("key_store.zig");
pub const kb_store = @import("kb_store.zig");
pub const job_queue = @import("job_queue.zig");
pub const status = @import("status.zig");
pub const c_api = @import("c_api.zig");

// Convenience re-exports at top level
pub const KeyStore = key_store.KeyStore;
pub const Config = key_store.Config;

pub const KbStore = kb_store.KbStore;
pub const KbEntry = kb_store.KbEntry;
pub const KbStats = kb_store.KbStats;

pub const JobQueue = job_queue.JobQueue;
pub const Job = job_queue.Job;
pub const JobStatus = job_queue.JobStatus;
pub const JqStats = job_queue.JqStats;
pub const WorkerInfo = job_queue.WorkerInfo;

pub const Error = status.Error;

// Unit tests for compile-time validation
test "KeyStore.Config defaults compile" {
    const cfg = Config{};
    try std.testing.expectEqualStrings("nats://127.0.0.1:4222", cfg.server);
    try std.testing.expectEqualStrings("keystore", cfg.bucket);
    try std.testing.expect(cfg.create_bucket);
    try std.testing.expectEqual(@as(i32, 1), cfg.history);
}

test "JobStatus enum round-trip" {
    try std.testing.expectEqualStrings("pending", JobStatus.pending.string());
    try std.testing.expectEqualStrings("running", JobStatus.running.string());
    try std.testing.expectEqualStrings("completed", JobStatus.completed.string());
    try std.testing.expectEqualStrings("failed", JobStatus.failed.string());
    try std.testing.expectEqualStrings("cancelled", JobStatus.cancelled.string());
    try std.testing.expectEqualStrings("retrying", JobStatus.retrying.string());
}

const std = @import("std");