//! mqtt_queue - Public API
//!
//! Reliable queued messaging over MQTT v3.1.1 using persistent sessions.
//! Wraps the Mosquitto C client library (libmosquitto).

const mqtt_queue = @import("mqtt_queue.zig");

// ── Types ────────────────────────────────────────────────────────────
pub const Config = mqtt_queue.Config;
pub const Qos = mqtt_queue.Qos;
pub const Message = mqtt_queue.Message;
pub const Error = mqtt_queue.Error;

// ── Core structs ─────────────────────────────────────────────────────
pub const Publisher = mqtt_queue.Publisher;
pub const Reader = mqtt_queue.Reader;

// ── Free functions ───────────────────────────────────────────────────
pub const libInit = mqtt_queue.libInit;
pub const libCleanup = mqtt_queue.libCleanup;
pub const messageFreeList = mqtt_queue.messageFreeList;

// ── Tests ────────────────────────────────────────────────────────────
test {
    @import("std").testing.refAllDecls(@This());
}