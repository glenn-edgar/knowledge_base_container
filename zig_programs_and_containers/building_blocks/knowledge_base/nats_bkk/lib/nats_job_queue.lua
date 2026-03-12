--[[
  nats_job_queue.lua — LuaJIT FFI binding for libnats_job_queue

  Usage:
    local jq_lib = require("lib.nats_job_queue")
    local ks_lib = require("lib.nats_key_store")

    local ks = ks_lib.KeyStore.new({ server = "nats://127.0.0.1:4222", bucket = "jobs" })
    ks:connect()

    local jq = jq_lib.JobQueue.new(ks:handle(), "my-worker")
    local job_id = jq:submit('{"task":"process"}', "myqueue", 5, 3, 300)
    local job = jq:claim_job({"myqueue"})
    jq:complete_job(job.id, '{"result":42}')
    jq:destroy()
    ks:destroy()
]]

local ffi = require("ffi")

-- ----------------------------------------------------------------
--  C declarations
-- ----------------------------------------------------------------

ffi.cdef[[
/* ---- Job status ---- */
typedef enum {
    JOB_PENDING   = 0,
    JOB_RUNNING   = 1,
    JOB_COMPLETED = 2,
    JOB_FAILED    = 3,
    JOB_CANCELLED = 4,
    JOB_RETRYING  = 5,
} JobStatus;

const char *job_status_str(JobStatus st);
JobStatus   job_status_from_str(const char *s);

/* ---- Job structure ---- */
typedef struct {
    char        id[64];
    char        queue[64];
    char       *payload_json;
    JobStatus   status;
    int         priority;
    int         max_retries;
    int         retry_count;
    char        created_at[32];
    char        started_at[32];
    char        completed_at[32];
    char       *error;
    char       *result_json;
    char        worker_id[64];
    int         timeout_seconds;
} Job;

void job_init(Job *j);
void job_free(Job *j);

/* ---- JobQueue opaque handle ---- */
typedef struct JobQueue JobQueue;

/* ---- Lifecycle ---- */
ks_status_t jq_create(JobQueue **out, KeyStore *ks, const char *worker_id);
void        jq_destroy(JobQueue *jq);

/* ---- Submit ---- */
ks_status_t jq_submit(JobQueue *jq,
                      const char *payload_json,
                      const char *queue,
                      int priority,
                      int max_retries,
                      int timeout_seconds,
                      char **out_job_id);

/* ---- Get ---- */
ks_status_t jq_get_job(JobQueue *jq, const char *job_id, Job *job);

/* ---- Claim ---- */
ks_status_t jq_claim_job(JobQueue *jq,
                         const char **queues,
                         size_t num_queues,
                         Job *job);

/* ---- Complete / Fail / Cancel ---- */
ks_status_t jq_complete_job(JobQueue *jq, const char *job_id,
                            const char *result_json, bool *ok);
ks_status_t jq_fail_job(JobQueue *jq, const char *job_id,
                        const char *error_msg, bool *ok);
ks_status_t jq_cancel_job(JobQueue *jq, const char *job_id, bool *ok);

/* ---- Statistics ---- */
typedef struct {
    int pending;
    int running;
    int completed;
    int failed;
    int cancelled;
} JqStats;

ks_status_t jq_get_stats(JobQueue *jq, const char *queue, JqStats *stats);

/* ---- Workers ---- */
typedef struct {
    char worker_id[64];
    char current_job[64];
    char last_seen[32];
} JqWorkerInfo;

ks_status_t jq_get_active_workers(JobQueue *jq, int staleness_seconds,
                                  JqWorkerInfo **workers, size_t *count);

/* ---- Cleanup ---- */
ks_status_t jq_cleanup_stale_jobs(JobQueue *jq, int staleness_seconds,
                                  size_t *cleaned);
]]

-- ----------------------------------------------------------------
--  Load shared library
-- ----------------------------------------------------------------

local ks_lib = require("lib.nats_key_store")  -- ensure key_store cdef loaded first
local C = ffi.load("nats_job_queue")

-- ----------------------------------------------------------------
--  Helpers
-- ----------------------------------------------------------------

local KS_OK = 0
local KS_ERR_NOT_FOUND = 3

local function check(st)
    if st == KS_OK then return true end
    error("JobQueue error: " .. ffi.string(ks_lib._C.ks_status_str(st)), 2)
end

-- ----------------------------------------------------------------
--  JobQueue class
-- ----------------------------------------------------------------

local JobQueue = {}
JobQueue.__index = JobQueue

--- Create a new JobQueue.
-- @param ks_handle  cdata<KeyStore*>  Raw KeyStore handle (from KeyStore:handle())
-- @param worker_id  string
-- @return JobQueue object
function JobQueue.new(ks_handle, worker_id)
    local handle = ffi.new("JobQueue*[1]")
    check(C.jq_create(handle, ks_handle, worker_id))
    local self = setmetatable({}, JobQueue)
    self._handle = handle[0]
    self._destroyed = false
    return self
end

--- Destroy the JobQueue (does NOT destroy the borrowed KeyStore).
function JobQueue:destroy()
    if not self._destroyed and self._handle ~= nil then
        C.jq_destroy(self._handle)
        self._destroyed = true
    end
end

--- Convert a C Job struct to a Lua table.
local function job_to_table(job)
    local t = {
        id              = ffi.string(job.id),
        queue           = ffi.string(job.queue),
        payload_json    = job.payload_json ~= nil and ffi.string(job.payload_json) or nil,
        status          = tonumber(job.status),
        status_str      = ffi.string(C.job_status_str(job.status)),
        priority        = job.priority,
        max_retries     = job.max_retries,
        retry_count     = job.retry_count,
        created_at      = ffi.string(job.created_at),
        started_at      = ffi.string(job.started_at),
        completed_at    = ffi.string(job.completed_at),
        error           = job.error ~= nil and ffi.string(job.error) or nil,
        result_json     = job.result_json ~= nil and ffi.string(job.result_json) or nil,
        worker_id       = ffi.string(job.worker_id),
        timeout_seconds = job.timeout_seconds,
    }
    return t
end

--- Submit a job.
-- @param payload_json string  Job payload (JSON)
-- @param queue        string  Queue name
-- @param priority     number  Higher = processed first
-- @param max_retries  number  Max retry attempts
-- @param timeout_secs number  Timeout in seconds (0 = none)
-- @return job_id string
function JobQueue:submit(payload_json, queue, priority, max_retries, timeout_secs)
    priority     = priority or 5
    max_retries  = max_retries or 3
    timeout_secs = timeout_secs or 300
    local id_ptr = ffi.new("char*[1]")
    check(C.jq_submit(self._handle, payload_json, queue,
                      priority, max_retries, timeout_secs, id_ptr))
    local id = ffi.string(id_ptr[0])
    ffi.C.free(id_ptr[0])
    return id
end

--- Get a job by ID.
-- @param job_id string
-- @return job table or nil
function JobQueue:get_job(job_id)
    local job = ffi.new("Job")
    C.job_init(job)
    local st = C.jq_get_job(self._handle, job_id, job)
    if st == KS_ERR_NOT_FOUND then
        C.job_free(job)
        return nil
    end
    check(st)
    local t = job_to_table(job)
    C.job_free(job)
    return t
end

--- Claim the next available job from one or more queues.
-- @param queues table  Array of queue name strings
-- @return job table or nil if no jobs available
function JobQueue:claim_job(queues)
    local n = #queues
    local c_queues = ffi.new("const char*[?]", n)
    for i = 1, n do
        c_queues[i - 1] = queues[i]
    end
    local job = ffi.new("Job")
    C.job_init(job)
    local st = C.jq_claim_job(self._handle, c_queues, n, job)
    if st == KS_ERR_NOT_FOUND then
        C.job_free(job)
        return nil
    end
    check(st)
    local t = job_to_table(job)
    C.job_free(job)
    return t
end

--- Complete a job.
-- @param job_id string
-- @param result_json string (optional)
-- @return boolean  true if completed successfully
function JobQueue:complete_job(job_id, result_json)
    local ok = ffi.new("bool[1]")
    check(C.jq_complete_job(self._handle, job_id, result_json, ok))
    return ok[0]
end

--- Fail a job (may re-queue if retries remain).
-- @param job_id string
-- @param error_msg string
-- @return boolean  true if status changed
function JobQueue:fail_job(job_id, error_msg)
    local ok = ffi.new("bool[1]")
    check(C.jq_fail_job(self._handle, job_id, error_msg, ok))
    return ok[0]
end

--- Cancel a pending job.
-- @param job_id string
-- @return boolean  true if cancelled
function JobQueue:cancel_job(job_id)
    local ok = ffi.new("bool[1]")
    check(C.jq_cancel_job(self._handle, job_id, ok))
    return ok[0]
end

--- Get queue statistics.
-- @param queue string  Queue name
-- @return table { pending, running, completed, failed, cancelled }
function JobQueue:get_stats(queue)
    local stats = ffi.new("JqStats")
    check(C.jq_get_stats(self._handle, queue, stats))
    return {
        pending   = stats.pending,
        running   = stats.running,
        completed = stats.completed,
        failed    = stats.failed,
        cancelled = stats.cancelled,
    }
end

--- Get active workers.
-- @param staleness_seconds number  Window to consider a worker active
-- @return table of { worker_id, current_job, last_seen }
function JobQueue:get_active_workers(staleness_seconds)
    staleness_seconds = staleness_seconds or 30
    local workers = ffi.new("JqWorkerInfo*[1]")
    local cnt     = ffi.new("size_t[1]")
    check(C.jq_get_active_workers(self._handle, staleness_seconds, workers, cnt))
    local result = {}
    for i = 0, tonumber(cnt[0]) - 1 do
        result[#result + 1] = {
            worker_id   = ffi.string(workers[0][i].worker_id),
            current_job = ffi.string(workers[0][i].current_job),
            last_seen   = ffi.string(workers[0][i].last_seen),
        }
    end
    if workers[0] ~= nil then ffi.C.free(workers[0]) end
    return result
end

--- Clean up stale running jobs.
-- @param staleness_seconds number
-- @return number of jobs cleaned
function JobQueue:cleanup_stale_jobs(staleness_seconds)
    staleness_seconds = staleness_seconds or 60
    local cleaned = ffi.new("size_t[1]")
    check(C.jq_cleanup_stale_jobs(self._handle, staleness_seconds, cleaned))
    return tonumber(cleaned[0])
end

-- ----------------------------------------------------------------
--  Module
-- ----------------------------------------------------------------

return {
    JobQueue = JobQueue,
    -- Re-export status constants
    JOB_PENDING   = 0,
    JOB_RUNNING   = 1,
    JOB_COMPLETED = 2,
    JOB_FAILED    = 3,
    JOB_CANCELLED = 4,
    JOB_RETRYING  = 5,
    job_status_str = function(st)
        return ffi.string(C.job_status_str(st))
    end,
    _C = C,
}
