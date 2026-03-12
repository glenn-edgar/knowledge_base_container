///  Manual C bindings for the three NATS C libraries.
///  Replaces @cImport to avoid include-path propagation issues in Zig 0.13.

const std = @import("std");

// ================================================================
//  Opaque C types
// ================================================================

pub const KeyStore = opaque {};
pub const KbStore = opaque {};
pub const JobQueue = opaque {};

// ================================================================
//  ks_status_t
// ================================================================

pub const ks_status_t = c_int;

pub const KS_OK: ks_status_t = 0;
pub const KS_ERR_INVALID_ARG: ks_status_t = 1;
pub const KS_ERR_CONNECTION: ks_status_t = 2;
pub const KS_ERR_NOT_FOUND: ks_status_t = 3;
pub const KS_ERR_BUCKET: ks_status_t = 4;
pub const KS_ERR_ENCODE: ks_status_t = 5;
pub const KS_ERR_DECODE: ks_status_t = 6;
pub const KS_ERR_MEMORY: ks_status_t = 7;
pub const KS_ERR_RETRY_EXHAUSTED: ks_status_t = 8;
pub const KS_ERR_NOT_NUMERIC: ks_status_t = 9;
pub const KS_ERR_NATS: ks_status_t = 10;

pub const natsStatus = c_int;

// ================================================================
//  KeyStoreConfig
// ================================================================

pub const KeyStoreConfig = extern struct {
    server: ?[*:0]const u8 = null,
    bucket: ?[*:0]const u8 = null,
    description: ?[*:0]const u8 = null,
    client_name: ?[*:0]const u8 = null,
    create_bucket: bool = true,
    history: c_int = 1,
    ttl_seconds: i64 = 0,
    max_reconnect: c_int = 3,
    reconnect_delay_s: f64 = 1.0,
};

// ================================================================
//  KbEntry
// ================================================================

pub const KbEntry = extern struct {
    label_json: ?[*:0]u8 = null,
    node_json: ?[*:0]u8 = null,
};

// ================================================================
//  KbStats
// ================================================================

pub const KbStats = extern struct {
    total_kb_keys: usize = 0,
    total_topics: usize = 0,
    all_keys_count: usize = 0,
    topic_names: ?[*]?[*:0]u8 = null,
    topic_counts: ?[*]usize = null,
    topic_array_len: usize = 0,
};

// ================================================================
//  JobStatus
// ================================================================

pub const JobStatus = c_uint;

// ================================================================
//  Job
// ================================================================

pub const Job = extern struct {
    id: [64]u8 = std.mem.zeroes([64]u8),
    queue: [64]u8 = std.mem.zeroes([64]u8),
    payload_json: ?[*:0]u8 = null,
    status: JobStatus = 0,
    priority: c_int = 0,
    max_retries: c_int = 3,
    retry_count: c_int = 0,
    created_at: [32]u8 = std.mem.zeroes([32]u8),
    started_at: [32]u8 = std.mem.zeroes([32]u8),
    completed_at: [32]u8 = std.mem.zeroes([32]u8),
    @"error": ?[*:0]u8 = null,
    result_json: ?[*:0]u8 = null,
    worker_id: [64]u8 = std.mem.zeroes([64]u8),
    timeout_seconds: c_int = 300,
};

// ================================================================
//  JqStats
// ================================================================

pub const JqStats = extern struct {
    pending: i64 = 0,
    running: i64 = 0,
    completed: i64 = 0,
    failed: i64 = 0,
    cancelled: i64 = 0,
};

// ================================================================
//  JqWorkerInfo
// ================================================================

pub const JqWorkerInfo = extern struct {
    worker_id: [64]u8 = std.mem.zeroes([64]u8),
    last_seen: [32]u8 = std.mem.zeroes([32]u8),
    current_job: [64]u8 = std.mem.zeroes([64]u8),
};

// ================================================================
//  KeyStore functions
// ================================================================

pub extern fn ks_status_str(st: ks_status_t) ?[*:0]const u8;
pub extern fn ks_config_defaults(cfg: *KeyStoreConfig) void;
pub extern fn ks_create(out: *?*KeyStore, cfg: *const KeyStoreConfig) ks_status_t;
pub extern fn ks_destroy(ks: ?*KeyStore) void;
pub extern fn ks_connect(ks: ?*KeyStore) ks_status_t;
pub extern fn ks_disconnect(ks: ?*KeyStore) ks_status_t;
pub extern fn ks_is_connected(ks: ?*const KeyStore) bool;
pub extern fn ks_last_nats_status(ks: ?*const KeyStore) natsStatus;

pub extern fn ks_put(ks: ?*KeyStore, key: [*:0]const u8, value: [*:0]const u8, revision: ?*u64) ks_status_t;
pub extern fn ks_get(ks: ?*KeyStore, key: [*:0]const u8, value: *?[*:0]u8) ks_status_t;
pub extern fn ks_get_bytes(ks: ?*KeyStore, key: [*:0]const u8, data: *?*anyopaque, len: *usize) ks_status_t;
pub extern fn ks_delete(ks: ?*KeyStore, key: [*:0]const u8) ks_status_t;
pub extern fn ks_exists(ks: ?*KeyStore, key: [*:0]const u8, exists: *bool) ks_status_t;
pub extern fn ks_keys(ks: ?*KeyStore, pattern: ?[*:0]const u8, keys: *?[*]?[*:0]u8, count: *usize) ks_status_t;
pub extern fn ks_free_keys(keys: ?[*]?[*:0]u8, count: usize) void;
pub extern fn ks_increment(ks: ?*KeyStore, key: [*:0]const u8, delta: i64, new_value: ?*i64) ks_status_t;
pub extern fn ks_decrement(ks: ?*KeyStore, key: [*:0]const u8, delta: i64, new_value: ?*i64) ks_status_t;

pub extern fn ks_put_sync(ks: ?*KeyStore, key: [*:0]const u8, value: [*:0]const u8, revision: ?*u64) ks_status_t;
pub extern fn ks_get_sync(ks: ?*KeyStore, key: [*:0]const u8, value: *?[*:0]u8) ks_status_t;
pub extern fn ks_delete_sync(ks: ?*KeyStore, key: [*:0]const u8) ks_status_t;
pub extern fn ks_exists_sync(ks: ?*KeyStore, key: [*:0]const u8, exists: *bool) ks_status_t;
pub extern fn ks_keys_sync(ks: ?*KeyStore, pattern: ?[*:0]const u8, keys: *?[*]?[*:0]u8, count: *usize) ks_status_t;
pub extern fn ks_increment_sync(ks: ?*KeyStore, key: [*:0]const u8, delta: i64, new_value: ?*i64) ks_status_t;
pub extern fn ks_decrement_sync(ks: ?*KeyStore, key: [*:0]const u8, delta: i64, new_value: ?*i64) ks_status_t;

// ================================================================
//  KbStore functions
// ================================================================

pub extern fn kb_create(out: *?*KbStore, server: [*:0]const u8, bucket: [*:0]const u8, description: ?[*:0]const u8) ks_status_t;
pub extern fn kb_destroy(kb: ?*KbStore) void;
pub extern fn kb_get_keystore(kb: ?*KbStore) ?*KeyStore;
pub extern fn kb_validate_topic(topic: [*:0]const u8) ks_status_t;
pub extern fn kb_validate_label_name(name: [*:0]const u8) ks_status_t;
pub extern fn kb_validate_node_name(name: [*:0]const u8) ks_status_t;
pub extern fn kb_validate_key_format(key: [*:0]const u8) bool;
pub extern fn kb_entry_free(entry: *KbEntry) void;
pub extern fn kb_pop_key(key: [*:0]const u8, out: *?[*:0]u8) ks_status_t;

pub extern fn kb_store(kb: ?*KbStore, base_topic: [*:0]const u8, label_name: [*:0]const u8, node_name: [*:0]const u8, label_json: [*:0]const u8, node_json: [*:0]const u8, composite: bool, out_key: ?*?[*:0]u8) ks_status_t;
pub extern fn kb_get(kb: ?*KbStore, key: [*:0]const u8, entry: *KbEntry) ks_status_t;
pub extern fn kb_delete(kb: ?*KbStore, key: [*:0]const u8) ks_status_t;
pub extern fn kb_list_keys(kb: ?*KbStore, base_topic: ?[*:0]const u8, keys: *?[*]?[*:0]u8, count: *usize) ks_status_t;
pub extern fn kb_get_stats(kb: ?*KbStore, stats: *KbStats) ks_status_t;
pub extern fn kb_stats_free(stats: *KbStats) void;

pub extern fn kb_store_sync(kb: ?*KbStore, base_topic: [*:0]const u8, label_name: [*:0]const u8, node_name: [*:0]const u8, label_json: [*:0]const u8, node_json: [*:0]const u8, composite: bool, out_key: ?*?[*:0]u8) ks_status_t;
pub extern fn kb_get_sync(kb: ?*KbStore, key: [*:0]const u8, entry: *KbEntry) ks_status_t;
pub extern fn kb_delete_sync(kb: ?*KbStore, key: [*:0]const u8) ks_status_t;
pub extern fn kb_list_keys_sync(kb: ?*KbStore, base_topic: ?[*:0]const u8, keys: *?[*]?[*:0]u8, count: *usize) ks_status_t;
pub extern fn kb_get_stats_sync(kb: ?*KbStore, stats: *KbStats) ks_status_t;

// ================================================================
//  JobQueue functions
// ================================================================

pub extern fn job_status_str(st: JobStatus) ?[*:0]const u8;
pub extern fn job_status_from_str(s: [*:0]const u8) JobStatus;
pub extern fn job_init(job: *Job) void;
pub extern fn job_free(job: *Job) void;
pub extern fn job_to_json(job: *const Job) ?[*:0]u8;
pub extern fn job_from_json(json: [*:0]const u8, out: *Job) ks_status_t;

pub extern fn jq_create(out: *?*JobQueue, ks: ?*KeyStore, worker_id: ?[*:0]const u8) ks_status_t;
pub extern fn jq_destroy(jq: ?*JobQueue) void;
pub extern fn jq_worker_id(jq: ?*const JobQueue) ?[*:0]const u8;

pub extern fn jq_submit(jq: ?*JobQueue, payload_json: [*:0]const u8, queue: ?[*:0]const u8, priority: c_int, max_retries: c_int, timeout_sec: c_int, job_id: ?*?[*:0]u8) ks_status_t;
pub extern fn jq_get_job(jq: ?*JobQueue, job_id: [*:0]const u8, out: *Job) ks_status_t;
pub extern fn jq_cancel_job(jq: ?*JobQueue, job_id: [*:0]const u8, cancelled: *bool) ks_status_t;
pub extern fn jq_claim_job(jq: ?*JobQueue, queues: [*]const ?[*:0]const u8, num_queues: usize, out: *Job) ks_status_t;
pub extern fn jq_complete_job(jq: ?*JobQueue, job_id: [*:0]const u8, result_json: ?[*:0]const u8, ok: *bool) ks_status_t;
pub extern fn jq_fail_job(jq: ?*JobQueue, job_id: [*:0]const u8, err: ?[*:0]const u8, ok: *bool) ks_status_t;
pub extern fn jq_get_stats(jq: ?*JobQueue, queue: [*:0]const u8, stats: *JqStats) ks_status_t;
pub extern fn jq_get_active_workers(jq: ?*JobQueue, staleness_sec: c_int, workers: *?[*]JqWorkerInfo, count: *usize) ks_status_t;
pub extern fn jq_cleanup_stale_jobs(jq: ?*JobQueue, timeout_sec: c_int, cleaned: *c_int) ks_status_t;

// Re-exports for status.zig
pub const CStatus = ks_status_t;
pub const NatsStatus = natsStatus;

// C stdlib functions used by the wrappers
pub extern fn strdup(s: [*:0]const u8) ?[*:0]u8;