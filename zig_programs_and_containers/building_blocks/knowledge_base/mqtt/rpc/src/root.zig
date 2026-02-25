//! mqtt_rpc - Public API
//!
//! JSON-RPC 2.0 over MQTT.
//! Server and Client for remote procedure calls over MQTT topics.

const mqtt_rpc = @import("mqtt_rpc.zig");

// ── Types ────────────────────────────────────────────────────────────
pub const Config = mqtt_rpc.Config;
pub const Error = mqtt_rpc.Error;
pub const MethodFn = mqtt_rpc.MethodFn;
pub const CallResult = mqtt_rpc.CallResult;

// ── Server & Client ──────────────────────────────────────────────────
pub const Server = mqtt_rpc.Server;
pub const Client = mqtt_rpc.Client;

// ── JSON-RPC error codes ─────────────────────────────────────────────
pub const JSONRPC_PARSE_ERROR = mqtt_rpc.JSONRPC_PARSE_ERROR;
pub const JSONRPC_INVALID_REQUEST = mqtt_rpc.JSONRPC_INVALID_REQUEST;
pub const JSONRPC_METHOD_NOT_FOUND = mqtt_rpc.JSONRPC_METHOD_NOT_FOUND;
pub const JSONRPC_INVALID_PARAMS = mqtt_rpc.JSONRPC_INVALID_PARAMS;
pub const JSONRPC_INTERNAL_ERROR = mqtt_rpc.JSONRPC_INTERNAL_ERROR;

// ── Library init/cleanup ─────────────────────────────────────────────
pub const libInit = mqtt_rpc.libInit;
pub const libCleanup = mqtt_rpc.libCleanup;

// ── Tests ────────────────────────────────────────────────────────────
test {
    @import("std").testing.refAllDecls(@This());
}