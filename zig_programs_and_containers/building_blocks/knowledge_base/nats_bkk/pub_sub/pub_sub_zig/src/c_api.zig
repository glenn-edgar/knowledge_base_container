///  Manual C bindings for the NATS PubSub C library.
///  No @cImport — works reliably across all Zig build configurations.

// ================================================================
//  Opaque C types
// ================================================================

pub const PubSub = opaque {};
pub const PubSubSub = opaque {};

// ================================================================
//  ps_status_t
// ================================================================

pub const ps_status_t = c_int;

pub const PS_OK: ps_status_t = 0;
pub const PS_ERR_INVALID_ARG: ps_status_t = 1;
pub const PS_ERR_CONNECTION: ps_status_t = 2;
pub const PS_ERR_TIMEOUT: ps_status_t = 3;
pub const PS_ERR_MEMORY: ps_status_t = 4;
pub const PS_ERR_NOT_CONNECTED: ps_status_t = 5;
pub const PS_ERR_NATS: ps_status_t = 6;

// ================================================================
//  PubSubConfig
// ================================================================

pub const PubSubConfig = extern struct {
    server: ?[*:0]const u8 = null,
    namespace_: ?[*:0]const u8 = null,
    client_name: ?[*:0]const u8 = null,
};

// ================================================================
//  PubSubMsg (passed to callbacks)
// ================================================================

pub const PubSubMsg = extern struct {
    subject: ?[*:0]const u8,
    original_subject: ?[*:0]const u8,
    data: ?[*]const u8,
    data_len: c_int,
    reply_to: ?[*:0]const u8,
};

// ================================================================
//  PubSubStats
// ================================================================

pub const PubSubStats = extern struct {
    msgs_published: i64 = 0,
    msgs_received: i64 = 0,
    active_subscriptions: c_int = 0,
};

// ================================================================
//  Callback type
// ================================================================

pub const pubsub_msg_cb = *const fn (msg: *const PubSubMsg, user_data: ?*anyopaque) callconv(.C) void;

// ================================================================
//  Functions
// ================================================================

pub extern fn ps_status_str(st: ps_status_t) ?[*:0]const u8;
pub extern fn pubsub_config_defaults(cfg: *PubSubConfig) void;

pub extern fn pubsub_create(out: *?*PubSub, cfg: *const PubSubConfig) ps_status_t;
pub extern fn pubsub_destroy(ps: ?*PubSub) void;

pub extern fn pubsub_connect(ps: ?*PubSub) ps_status_t;
pub extern fn pubsub_disconnect(ps: ?*PubSub) ps_status_t;
pub extern fn pubsub_is_connected(ps: ?*const PubSub) bool;
pub extern fn pubsub_namespace(ps: ?*const PubSub) ?[*:0]const u8;
pub extern fn pubsub_client_name(ps: ?*const PubSub) ?[*:0]const u8;

pub extern fn pubsub_publish(ps: ?*PubSub, subject: [*:0]const u8, data: ?[*]const u8, data_len: c_int) ps_status_t;
pub extern fn pubsub_publish_str(ps: ?*PubSub, subject: [*:0]const u8, str: [*:0]const u8) ps_status_t;

pub extern fn pubsub_subscribe(ps: ?*PubSub, subject: [*:0]const u8, cb: pubsub_msg_cb, user_data: ?*anyopaque, queue: ?[*:0]const u8, sub: *?*PubSubSub) ps_status_t;
pub extern fn pubsub_subscribe_raw(ps: ?*PubSub, subject: [*:0]const u8, cb: pubsub_msg_cb, user_data: ?*anyopaque, queue: ?[*:0]const u8, sub: *?*PubSubSub) ps_status_t;
pub extern fn pubsub_unsubscribe(ps: ?*PubSub, sub: ?*PubSubSub) ps_status_t;
pub extern fn pubsub_auto_unsubscribe(sub: ?*PubSubSub, max_msgs: c_int) ps_status_t;
pub extern fn pubsub_sub_subject(sub: ?*const PubSubSub) ?[*:0]const u8;

pub extern fn pubsub_request(ps: ?*PubSub, subject: [*:0]const u8, data: ?[*]const u8, data_len: c_int, timeout_sec: f64, reply_data: *?[*:0]u8, reply_len: *c_int) ps_status_t;
pub extern fn pubsub_request_str(ps: ?*PubSub, subject: [*:0]const u8, str: [*:0]const u8, timeout_sec: f64, reply_str: *?[*:0]u8) ps_status_t;
pub extern fn pubsub_reply(ps: ?*PubSub, reply_to: [*:0]const u8, data: ?[*]const u8, data_len: c_int) ps_status_t;
pub extern fn pubsub_reply_str(ps: ?*PubSub, reply_to: [*:0]const u8, str: [*:0]const u8) ps_status_t;

pub extern fn pubsub_get_stats(ps: ?*const PubSub, stats: *PubSubStats) ps_status_t;

// Convenience self-reference so `c.xyz` works in wrapper files
pub const c = @This();