//! mqtt_kv_store - Public API

const kv_writer = @import("kv_store_writer.zig");
const kv_reader = @import("kv_store_reader.zig");

// ── Writer ───────────────────────────────────────────────────────────
pub const Writer = kv_writer.Writer;
pub const WriterConfig = kv_writer.Config;
pub const WriterError = kv_writer.Error;

// ── Reader ───────────────────────────────────────────────────────────
pub const Reader = kv_reader.Reader;
pub const ReaderConfig = kv_reader.Config;
pub const ReaderError = kv_reader.Error;
pub const Entry = kv_reader.Entry;

// ── Shared types ─────────────────────────────────────────────────────
pub const Qos = kv_writer.Qos;

// ── Constants ────────────────────────────────────────────────────────
pub const MAX_TOPIC_LEN = kv_reader.MAX_TOPIC_LEN;
pub const MAX_VALUE_LEN = kv_reader.MAX_VALUE_LEN;
pub const MAX_ENTRIES = kv_reader.MAX_ENTRIES;

// ── Library init/cleanup (mosquitto) ─────────────────────────────────

const c = @cImport({
    @cInclude("mosquitto.h");
});

pub fn libInit() void {
    _ = c.mosquitto_lib_init();
}

pub fn libCleanup() void {
    _ = c.mosquitto_lib_cleanup();
}

// ── Tests ────────────────────────────────────────────────────────────
test {
    @import("std").testing.refAllDecls(@This());
}