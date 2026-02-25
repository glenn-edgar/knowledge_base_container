const std = @import("std");
const c = @import("c_api.zig");
const status_mod = @import("status.zig");
const key_store = @import("key_store.zig");
pub const Error = status_mod.Error;

// ----------------------------------------------------------------
//  JobStatus
// ----------------------------------------------------------------

pub const JobStatus = enum(c_uint) {
    pending = 0,
    running = 1,
    completed = 2,
    failed = 3,
    cancelled = 4,
    retrying = 5,

    pub fn fromC(s: c.JobStatus) JobStatus {
        return @enumFromInt(s);
    }

    pub fn toC(self: JobStatus) c.JobStatus {
        return @intFromEnum(self);
    }

    pub fn string(self: JobStatus) []const u8 {
        return switch (self) {
            .pending => "pending",
            .running => "running",
            .completed => "completed",
            .failed => "failed",
            .cancelled => "cancelled",
            .retrying => "retrying",
        };
    }

    pub fn fromString(s: [:0]const u8) JobStatus {
        const result = c.job_status_from_str(s.ptr);
        return fromC(result);
    }
};

// ----------------------------------------------------------------
//  Job
// ----------------------------------------------------------------

pub const Job = struct {
    raw: c.Job,

    const Self = @This();

    /// Initialize a Job with defaults (generates id, sets created_at).
    pub fn init() Self {
        var raw: c.Job = undefined;
        c.job_init(&raw);
        return Self{ .raw = raw };
    }

    pub fn deinit(self: *Self) void {
        c.job_free(&self.raw);
    }

    // -- Accessors --

    pub fn id(self: *const Self) []const u8 {
        return std.mem.sliceTo(&self.raw.id, 0);
    }

    pub fn queue(self: *const Self) []const u8 {
        return std.mem.sliceTo(&self.raw.queue, 0);
    }

    pub fn getStatus(self: *const Self) JobStatus {
        return JobStatus.fromC(self.raw.status);
    }

    pub fn priority(self: *const Self) i32 {
        return self.raw.priority;
    }

    pub fn maxRetries(self: *const Self) i32 {
        return self.raw.max_retries;
    }

    pub fn retryCount(self: *const Self) i32 {
        return self.raw.retry_count;
    }

    pub fn createdAt(self: *const Self) []const u8 {
        return std.mem.sliceTo(&self.raw.created_at, 0);
    }

    pub fn startedAt(self: *const Self) []const u8 {
        return std.mem.sliceTo(&self.raw.started_at, 0);
    }

    pub fn completedAt(self: *const Self) []const u8 {
        return std.mem.sliceTo(&self.raw.completed_at, 0);
    }

    pub fn workerId(self: *const Self) []const u8 {
        return std.mem.sliceTo(&self.raw.worker_id, 0);
    }

    pub fn timeoutSeconds(self: *const Self) i32 {
        return self.raw.timeout_seconds;
    }

    pub fn payloadJson(self: *const Self) ?[]const u8 {
        if (self.raw.payload_json) |p| {
            return std.mem.span(p);
        }
        return null;
    }

    pub fn errorMsg(self: *const Self) ?[]const u8 {
        if (self.raw.@"error") |e| {
            return std.mem.span(e);
        }
        return null;
    }

    pub fn resultJson(self: *const Self) ?[]const u8 {
        if (self.raw.result_json) |r| {
            return std.mem.span(r);
        }
        return null;
    }

    // -- Serialization --

    /// Serialize to JSON string.  Caller must free with std.c.free().
    pub fn toJson(self: *const Self) Error![:0]u8 {
        const ptr = c.job_to_json(&self.raw);
        if (ptr) |p| {
            return std.mem.span(p);
        }
        return Error.EncodeError;
    }

    /// Deserialize from JSON string.
    pub fn fromJson(json: [:0]const u8) Error!Self {
        var raw: c.Job = undefined;
        try status_mod.check(c.job_from_json(json.ptr, &raw));
        return Self{ .raw = raw };
    }

    /// Free a JSON string returned by toJson.
    pub fn freeJson(s: [:0]u8) void {
        std.c.free(@ptrCast(@constCast(s.ptr)));
    }

    // -- Setters for building jobs before submit --

    pub fn setQueue(self: *Self, q: []const u8) void {
        const len = @min(q.len, self.raw.queue.len - 1);
        @memcpy(self.raw.queue[0..len], q[0..len]);
        self.raw.queue[len] = 0;
    }

    pub fn setPriority(self: *Self, p: i32) void {
        self.raw.priority = p;
    }

    pub fn setMaxRetries(self: *Self, n: i32) void {
        self.raw.max_retries = n;
    }

    pub fn setTimeoutSeconds(self: *Self, t: i32) void {
        self.raw.timeout_seconds = t;
    }

    pub fn setPayload(self: *Self, json: [:0]const u8) void {
        if (self.raw.payload_json) |p| {
            std.c.free(@ptrCast(p));
        }
        self.raw.payload_json = c.strdup(json.ptr);
    }
};

// ----------------------------------------------------------------
//  JqStats
// ----------------------------------------------------------------

pub const JqStats = struct {
    pending: i64,
    running: i64,
    completed: i64,
    failed: i64,
    cancelled: i64,
};

// ----------------------------------------------------------------
//  WorkerInfo
// ----------------------------------------------------------------

pub const WorkerInfo = struct {
    worker_id: []const u8,
    last_seen: []const u8,
    current_job: []const u8,
};

// ----------------------------------------------------------------
//  JobQueue
// ----------------------------------------------------------------

pub const JobQueue = struct {
    handle: *c.JobQueue,

    const Self = @This();

    /// Create a JobQueue using the given KeyStore for storage.
    /// The KeyStore must already exist; the JobQueue does NOT own it.
    pub fn init(ks: *key_store.KeyStore, worker_id: ?[:0]const u8) Error!Self {
        var handle: ?*c.JobQueue = null;
        const wid_ptr: ?[*:0]const u8 = if (worker_id) |w| w.ptr else null;
        try status_mod.check(c.jq_create(&handle, ks.handle, wid_ptr));
        return Self{ .handle = handle.? };
    }

    pub fn deinit(self: *Self) void {
        c.jq_destroy(self.handle);
        self.handle = undefined;
    }

    pub fn workerId(self: *const Self) []const u8 {
        const ptr = c.jq_worker_id(self.handle);
        if (ptr) |p| {
            return std.mem.span(p);
        }
        return "";
    }

    // ----------------------------------------------------------
    //  Submit
    // ----------------------------------------------------------

    pub const SubmitOptions = struct {
        queue: [:0]const u8 = "default",
        priority: i32 = 0,
        max_retries: i32 = 3,
        timeout_sec: i32 = 300,
    };

    /// Submit a job.  Returns the generated job ID.
    /// Caller must free with `std.c.free()` or use `submitAlloc`.
    pub fn submit(self: *Self, payload_json: [:0]const u8, opts: SubmitOptions) Error![:0]u8 {
        var job_id: ?[*:0]u8 = null;
        try status_mod.check(c.jq_submit(
            self.handle,
            payload_json.ptr,
            opts.queue.ptr,
            opts.priority,
            opts.max_retries,
            opts.timeout_sec,
            @ptrCast(&job_id),
        ));
        if (job_id) |j| {
            return std.mem.span(j);
        }
        return Error.OutOfMemory;
    }

    /// Submit and copy the job ID into a Zig allocator.
    pub fn submitAlloc(
        self: *Self,
        allocator: std.mem.Allocator,
        payload_json: [:0]const u8,
        opts: SubmitOptions,
    ) (Error || std.mem.Allocator.Error)![]u8 {
        const raw = try self.submit(payload_json, opts);
        defer std.c.free(@ptrCast(@constCast(raw.ptr)));
        return try allocator.dupe(u8, raw);
    }

    /// Free a job ID returned by submit.
    pub fn freeJobId(s: [:0]u8) void {
        std.c.free(@ptrCast(@constCast(s.ptr)));
    }

    // ----------------------------------------------------------
    //  Get job
    // ----------------------------------------------------------

    pub fn getJob(self: *Self, job_id: [:0]const u8) Error!Job {
        var raw: c.Job = undefined;
        try status_mod.check(c.jq_get_job(self.handle, job_id.ptr, &raw));
        return Job{ .raw = raw };
    }

    // ----------------------------------------------------------
    //  Cancel
    // ----------------------------------------------------------

    pub fn cancelJob(self: *Self, job_id: [:0]const u8) Error!bool {
        var cancelled: bool = false;
        try status_mod.check(c.jq_cancel_job(self.handle, job_id.ptr, &cancelled));
        return cancelled;
    }

    // ----------------------------------------------------------
    //  Claim
    // ----------------------------------------------------------

    /// Claim the next available job from the listed queues.
    /// Returns `error.NotFound` if no jobs are available.
    pub fn claimJob(self: *Self, queues: []const [:0]const u8) Error!Job {
        var raw: c.Job = undefined;
        try status_mod.check(c.jq_claim_job(
            self.handle,
            @ptrCast(@constCast(queues.ptr)),
            queues.len,
            &raw,
        ));
        return Job{ .raw = raw };
    }

    /// Claim from the default queue.
    pub fn claimDefault(self: *Self) Error!Job {
        const queues = [_][:0]const u8{"default"};
        return self.claimJob(&queues);
    }

    // ----------------------------------------------------------
    //  Complete
    // ----------------------------------------------------------

    pub fn completeJob(self: *Self, job_id: [:0]const u8, result_json: ?[:0]const u8) Error!bool {
        var ok: bool = false;
        const result_ptr: ?[*:0]const u8 = if (result_json) |r| r.ptr else null;
        try status_mod.check(c.jq_complete_job(self.handle, job_id.ptr, result_ptr, &ok));
        return ok;
    }

    // ----------------------------------------------------------
    //  Fail
    // ----------------------------------------------------------

    pub fn failJob(self: *Self, job_id: [:0]const u8, err_msg: ?[:0]const u8) Error!bool {
        var ok: bool = false;
        const err_ptr: ?[*:0]const u8 = if (err_msg) |e| e.ptr else null;
        try status_mod.check(c.jq_fail_job(self.handle, job_id.ptr, err_ptr, &ok));
        return ok;
    }

    // ----------------------------------------------------------
    //  Statistics
    // ----------------------------------------------------------

    pub fn getStats(self: *Self, queue_name: [:0]const u8) Error!JqStats {
        var raw: c.JqStats = undefined;
        try status_mod.check(c.jq_get_stats(self.handle, queue_name.ptr, &raw));
        return JqStats{
            .pending = raw.pending,
            .running = raw.running,
            .completed = raw.completed,
            .failed = raw.failed,
            .cancelled = raw.cancelled,
        };
    }

    // ----------------------------------------------------------
    //  Active workers
    // ----------------------------------------------------------

    pub const WorkerList = struct {
        items: []WorkerInfo,
        raw_ptr: ?*anyopaque,
        raw_workers: []c.JqWorkerInfo,

        pub fn deinit(self: *WorkerList) void {
            if (self.raw_ptr) |p| {
                std.c.free(p);
            }
            if (self.items.len > 0) {
                std.c.free(@ptrCast(self.items.ptr));
            }
        }
    };

    pub fn getActiveWorkers(self: *Self, staleness_sec: i32) Error!WorkerList {
        var raw_workers: ?[*]c.JqWorkerInfo = null;
        var count: usize = 0;
        try status_mod.check(c.jq_get_active_workers(
            self.handle,
            staleness_sec,
            &raw_workers,
            &count,
        ));

        if (raw_workers == null or count == 0) {
            return WorkerList{
                .items = &.{},
                .raw_ptr = @ptrCast(raw_workers),
                .raw_workers = &.{},
            };
        }

        const rw = raw_workers.?;
        // Build Zig-friendly slice by referencing into the C array
        const allocator = std.heap.c_allocator;
        const items = allocator.alloc(WorkerInfo, count) catch return Error.OutOfMemory;

        for (0..count) |i| {
            items[i] = WorkerInfo{
                .worker_id = std.mem.sliceTo(&rw[i].worker_id, 0),
                .last_seen = std.mem.sliceTo(&rw[i].last_seen, 0),
                .current_job = std.mem.sliceTo(&rw[i].current_job, 0),
            };
        }

        return WorkerList{
            .items = items,
            .raw_ptr = @ptrCast(rw),
            .raw_workers = rw[0..count],
        };
    }

    // ----------------------------------------------------------
    //  Stale job cleanup
    // ----------------------------------------------------------

    pub fn cleanupStaleJobs(self: *Self, timeout_sec: i32) Error!i32 {
        var cleaned: c_int = 0;
        try status_mod.check(c.jq_cleanup_stale_jobs(self.handle, timeout_sec, &cleaned));
        return @intCast(cleaned);
    }
};