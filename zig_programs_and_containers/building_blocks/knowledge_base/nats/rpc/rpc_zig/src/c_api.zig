///  Manual C bindings for the NATS RPC C library.
///  No @cImport — works reliably across all Zig build configurations.

// ================================================================
//  Opaque C types
// ================================================================

pub const RpcServer = opaque {};
pub const RpcClient = opaque {};

// ================================================================
//  rpc_status_t
// ================================================================

pub const rpc_status_t = c_int;

pub const RPC_OK: rpc_status_t = 0;
pub const RPC_ERR_INVALID_ARG: rpc_status_t = 1;
pub const RPC_ERR_CONNECTION: rpc_status_t = 2;
pub const RPC_ERR_TIMEOUT: rpc_status_t = 3;
pub const RPC_ERR_ENCODE: rpc_status_t = 4;
pub const RPC_ERR_DECODE: rpc_status_t = 5;
pub const RPC_ERR_MEMORY: rpc_status_t = 6;
pub const RPC_ERR_HANDLER: rpc_status_t = 7;
pub const RPC_ERR_NOT_FOUND: rpc_status_t = 8;
pub const RPC_ERR_NATS: rpc_status_t = 9;

// ================================================================
//  RpcConfig (shared by server and client)
// ================================================================

pub const RpcConfig = extern struct {
    server: ?[*:0]const u8 = null,
    namespace_: ?[*:0]const u8 = null,
    instance_id: ?[*:0]const u8 = null,
    enable_health: bool = true,
};

// ================================================================
//  RpcHandlerStats
// ================================================================

pub const RpcHandlerStats = extern struct {
    method: ?[*:0]const u8,
    call_count: i64,
    error_count: i64,
    instance_specific: bool,
};

// ================================================================
//  RpcBatchEntry
// ================================================================

pub const RpcBatchEntry = extern struct {
    method: ?[*:0]const u8,
    params_json: ?[*:0]const u8,
    target_instance: ?[*:0]const u8,
};

// ================================================================
//  RpcBatchResult
// ================================================================

pub const RpcBatchResult = extern struct {
    status: rpc_status_t,
    result_json: ?[*:0]u8,
};

// ================================================================
//  Handler callback type
//  rpc_status_t (*)(const char *params_json, void *user_data, char **result_json)
// ================================================================

pub const rpc_handler_fn = *const fn (
    params_json: [*:0]const u8,
    user_data: ?*anyopaque,
    result_json: *?[*:0]u8,
) callconv(.C) rpc_status_t;

// ================================================================
//  Functions — status
// ================================================================

pub extern fn rpc_status_str(st: rpc_status_t) ?[*:0]const u8;
pub extern fn rpc_config_defaults(cfg: *RpcConfig) void;

// ================================================================
//  Functions — server
// ================================================================

pub extern fn rpc_server_create(out: *?*RpcServer, cfg: *const RpcConfig) rpc_status_t;
pub extern fn rpc_server_destroy(srv: ?*RpcServer) void;
pub extern fn rpc_server_register(srv: ?*RpcServer, method: [*:0]const u8, handler: rpc_handler_fn, user_data: ?*anyopaque, instance_specific: bool) rpc_status_t;
pub extern fn rpc_server_start(srv: ?*RpcServer, prefix: ?[*:0]const u8) rpc_status_t;
pub extern fn rpc_server_wait(srv: ?*RpcServer) void;
pub extern fn rpc_server_stop(srv: ?*RpcServer) rpc_status_t;
pub extern fn rpc_server_instance_id(srv: ?*const RpcServer) ?[*:0]const u8;
pub extern fn rpc_server_is_running(srv: ?*const RpcServer) bool;
pub extern fn rpc_server_get_stats(srv: ?*const RpcServer, stats: *?[*]RpcHandlerStats, count: *usize) rpc_status_t;

// ================================================================
//  Functions — client
// ================================================================

pub extern fn rpc_client_create(out: *?*RpcClient, cfg: *const RpcConfig) rpc_status_t;
pub extern fn rpc_client_destroy(cli: ?*RpcClient) void;
pub extern fn rpc_client_connect(cli: ?*RpcClient) rpc_status_t;
pub extern fn rpc_client_disconnect(cli: ?*RpcClient) rpc_status_t;
pub extern fn rpc_client_is_connected(cli: ?*const RpcClient) bool;
pub extern fn rpc_client_instance_id(cli: ?*const RpcClient) ?[*:0]const u8;

pub extern fn rpc_client_call(cli: ?*RpcClient, method: [*:0]const u8, params_json: ?[*:0]const u8, timeout_sec: f64, result_json: *?[*:0]u8) rpc_status_t;
pub extern fn rpc_client_call_instance(cli: ?*RpcClient, method: [*:0]const u8, params_json: ?[*:0]const u8, timeout_sec: f64, target_instance: [*:0]const u8, result_json: *?[*:0]u8) rpc_status_t;
pub extern fn rpc_client_call_batch(cli: ?*RpcClient, entries: [*]const RpcBatchEntry, count: usize, timeout_sec: f64, results: [*]RpcBatchResult) rpc_status_t;

// Convenience self-reference so `c.xyz` works in wrapper files
pub const c = @This();