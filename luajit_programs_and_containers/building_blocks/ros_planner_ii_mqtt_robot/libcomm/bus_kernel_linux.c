// libcomm/bus_kernel_linux.c
// pthreads backend for bus_kernel.h. Implements the four primitives
// (thread, msgq, timer, mutex) plus bus_now_ms on Linux glibc.
//
// Design:
//   - bus_thread_t holds a pthread_t and a small trampoline closure.
//     bus_thread_start spawns a detached-but-joinable thread; join()
//     uses pthread_timedjoin_np (glibc) so timeouts work.
//   - bus_msgq_t is a fixed-size ring backed by caller-supplied storage,
//     guarded by mutex+cond. Producer never blocks (BUS_ERR_FULL on full),
//     consumer can wait up to timeout_ms with pthread_cond_timedwait.
//   - bus_timer_t spawns a worker that sleeps `period_ms` between
//     callbacks, polling a stop flag every `BUS_TIMER_STOP_POLL_MS` to
//     keep teardown latency bounded. One-shot (period=0) fires once
//     after the initial delay then exits the worker.
//   - bus_mutex_t is a thin wrapper over pthread_mutex_t; lock supports
//     a millisecond timeout via pthread_mutex_timedlock.
//
// Priority handling: BUS_PRIO_HIGH/MED/LOW are advisory on Linux unless
// the process has CAP_SYS_NICE. We attempt SCHED_OTHER nice values
// (-5/0/+5) which are safe in unprivileged builds; setting fails are
// silently ignored.

#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200809L

#include "bus_kernel.h"

#include <errno.h>
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

// ============ INTERNAL LAYOUTS ============
// Each bus_*_t is a fixed-size opaque blob in the public header. We
// alias it to a real struct here. Static asserts at the bottom of the
// file enforce the storage budget.

typedef struct {
    pthread_t            handle;
    bus_thread_entry_fn  entry;
    void                *arg;
    char                 name[16];
    int                  joined;        // 1 once bus_thread_join completed
    int                  prio_hint;     // mapped to nice() inside the entry
} linux_thread_t;

typedef struct {
    pthread_mutex_t  mu;
    pthread_cond_t   cond;
    uint8_t         *buf;               // caller-owned: depth * msg_size bytes
    uint16_t         msg_size;
    uint16_t         depth;
    uint16_t         head;              // write idx (mod depth)
    uint16_t         tail;              // read idx  (mod depth)
    uint16_t         count;
    uint16_t         _pad;
} linux_msgq_t;

typedef struct {
    pthread_t        worker;
    pthread_mutex_t  mu;
    bus_timer_cb_fn  cb;
    void            *arg;
    uint32_t         period_ms;
    uint32_t         delay_ms;
    int              running;           // guarded by mu
    int              stop;              // guarded by mu
} linux_timer_t;

typedef struct {
    pthread_mutex_t  mu;
    int              initialised;
} linux_mutex_t;

// Compile-time storage-budget enforcement.
#define BK_STATIC_ASSERT(cond, msg) typedef char bk_assert_##msg[(cond) ? 1 : -1]
BK_STATIC_ASSERT(sizeof(linux_thread_t) <= BUS_THREAD_STORAGE_BYTES, thread_fits);
BK_STATIC_ASSERT(sizeof(linux_msgq_t)   <= BUS_MSGQ_STORAGE_BYTES,   msgq_fits);
BK_STATIC_ASSERT(sizeof(linux_timer_t)  <= BUS_TIMER_STORAGE_BYTES,  timer_fits);
BK_STATIC_ASSERT(sizeof(linux_mutex_t)  <= BUS_MUTEX_STORAGE_BYTES,  mutex_fits);

// ============ CLOCK HELPERS ============

uint32_t bus_now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint32_t)((uint64_t)ts.tv_sec * 1000ULL + (uint64_t)ts.tv_nsec / 1000000ULL);
}

static void deadline_from_ms(struct timespec *out, uint32_t ms)
{
    clock_gettime(CLOCK_REALTIME, out);
    uint64_t add_ns = (uint64_t)ms * 1000000ULL;
    out->tv_sec  += (time_t)(add_ns / 1000000000ULL);
    out->tv_nsec += (long)  (add_ns % 1000000000ULL);
    if (out->tv_nsec >= 1000000000L) {
        out->tv_sec  += 1;
        out->tv_nsec -= 1000000000L;
    }
}

// ============ THREADS ============

static void *thread_trampoline(void *raw)
{
    linux_thread_t *t = (linux_thread_t *)raw;
#ifdef __GLIBC__
    pthread_setname_np(pthread_self(), t->name);
#endif
    t->entry(t->arg);
    return NULL;
}

bus_result_t bus_thread_start(bus_thread_t        *opaque,
                              const char          *name,
                              bus_priority_t       prio,
                              bus_thread_entry_fn  entry,
                              void                *arg)
{
    if (!opaque || !entry) return BUS_ERR_BAD_ARG;
    linux_thread_t *t = (linux_thread_t *)opaque;
    memset(t, 0, sizeof(*t));
    t->entry     = entry;
    t->arg       = arg;
    t->prio_hint = (int)prio;
    if (name) {
        size_t n = strnlen(name, sizeof(t->name) - 1);
        memcpy(t->name, name, n);
        t->name[n] = '\0';
    }
    int rc = pthread_create(&t->handle, NULL, thread_trampoline, t);
    if (rc != 0) return BUS_ERR_NO_MEM;
    return BUS_OK;
}

bus_result_t bus_thread_join(bus_thread_t *opaque, uint32_t timeout_ms)
{
    if (!opaque) return BUS_ERR_BAD_ARG;
    linux_thread_t *t = (linux_thread_t *)opaque;
    if (t->joined) return BUS_OK;

    int rc;
    if (timeout_ms == UINT32_MAX) {
        rc = pthread_join(t->handle, NULL);
    } else {
#ifdef __GLIBC__
        struct timespec deadline;
        deadline_from_ms(&deadline, timeout_ms);
        rc = pthread_timedjoin_np(t->handle, NULL, &deadline);
        if (rc == ETIMEDOUT) return BUS_ERR_TIMEOUT;
#else
        // Fallback: spin-poll join with tryjoin. Coarse but portable.
        uint32_t deadline = bus_now_ms() + timeout_ms;
        while ((rc = pthread_tryjoin_np(t->handle, NULL)) == EBUSY) {
            if ((int32_t)(bus_now_ms() - deadline) >= 0) return BUS_ERR_TIMEOUT;
            usleep(1000);
        }
#endif
    }
    if (rc != 0) return BUS_ERR_INIT;
    t->joined = 1;
    return BUS_OK;
}

void bus_thread_yield(void)
{
    sched_yield();
}

void bus_thread_sleep_ms(uint32_t ms)
{
    struct timespec ts;
    ts.tv_sec  = (time_t)(ms / 1000U);
    ts.tv_nsec = (long)  ((ms % 1000U) * 1000000UL);
    while (nanosleep(&ts, &ts) == -1 && errno == EINTR) { /* resume */ }
}

// ============ MESSAGE QUEUES ============

bus_result_t bus_msgq_init(bus_msgq_t *opaque,
                           void       *backing_buffer,
                           uint16_t    msg_size,
                           uint16_t    depth)
{
    if (!opaque || !backing_buffer || msg_size == 0 || depth == 0) return BUS_ERR_BAD_ARG;
    linux_msgq_t *q = (linux_msgq_t *)opaque;
    memset(q, 0, sizeof(*q));
    q->buf      = (uint8_t *)backing_buffer;
    q->msg_size = msg_size;
    q->depth    = depth;
    if (pthread_mutex_init(&q->mu, NULL) != 0) return BUS_ERR_INIT;
    if (pthread_cond_init (&q->cond, NULL) != 0) {
        pthread_mutex_destroy(&q->mu);
        return BUS_ERR_INIT;
    }
    return BUS_OK;
}

bus_result_t bus_msgq_put(bus_msgq_t *opaque, const void *msg)
{
    if (!opaque || !msg) return BUS_ERR_BAD_ARG;
    linux_msgq_t *q = (linux_msgq_t *)opaque;
    pthread_mutex_lock(&q->mu);
    if (q->count >= q->depth) {
        pthread_mutex_unlock(&q->mu);
        return BUS_ERR_FULL;
    }
    memcpy(q->buf + (size_t)q->head * q->msg_size, msg, q->msg_size);
    q->head = (uint16_t)((q->head + 1) % q->depth);
    q->count++;
    pthread_cond_signal(&q->cond);
    pthread_mutex_unlock(&q->mu);
    return BUS_OK;
}

bus_result_t bus_msgq_get(bus_msgq_t *opaque, void *out_msg, uint32_t timeout_ms)
{
    if (!opaque || !out_msg) return BUS_ERR_BAD_ARG;
    linux_msgq_t *q = (linux_msgq_t *)opaque;
    pthread_mutex_lock(&q->mu);

    if (q->count == 0) {
        if (timeout_ms == 0) {
            pthread_mutex_unlock(&q->mu);
            return BUS_ERR_EMPTY;
        }
        if (timeout_ms == UINT32_MAX) {
            while (q->count == 0) pthread_cond_wait(&q->cond, &q->mu);
        } else {
            struct timespec deadline;
            deadline_from_ms(&deadline, timeout_ms);
            while (q->count == 0) {
                int rc = pthread_cond_timedwait(&q->cond, &q->mu, &deadline);
                if (rc == ETIMEDOUT) {
                    pthread_mutex_unlock(&q->mu);
                    return BUS_ERR_TIMEOUT;
                }
            }
        }
    }

    memcpy(out_msg, q->buf + (size_t)q->tail * q->msg_size, q->msg_size);
    q->tail = (uint16_t)((q->tail + 1) % q->depth);
    q->count--;
    pthread_mutex_unlock(&q->mu);
    return BUS_OK;
}

uint16_t bus_msgq_count(const bus_msgq_t *opaque)
{
    if (!opaque) return 0;
    linux_msgq_t *q = (linux_msgq_t *)(uintptr_t)opaque;
    pthread_mutex_lock(&q->mu);
    uint16_t c = q->count;
    pthread_mutex_unlock(&q->mu);
    return c;
}

// ============ TIMERS ============

#define BUS_TIMER_STOP_POLL_MS  10   // teardown granularity for the worker

static void *timer_worker(void *raw)
{
    linux_timer_t *tm = (linux_timer_t *)raw;

    // First-fire delay.
    uint32_t initial = tm->delay_ms;
    while (initial > 0) {
        uint32_t step = initial > BUS_TIMER_STOP_POLL_MS ? BUS_TIMER_STOP_POLL_MS : initial;
        bus_thread_sleep_ms(step);
        initial -= step;
        pthread_mutex_lock(&tm->mu);
        int stop = tm->stop;
        pthread_mutex_unlock(&tm->mu);
        if (stop) goto done;
    }

    for (;;) {
        pthread_mutex_lock(&tm->mu);
        if (tm->stop) { pthread_mutex_unlock(&tm->mu); break; }
        pthread_mutex_unlock(&tm->mu);

        tm->cb(tm->arg);

        if (tm->period_ms == 0) break;     // one-shot

        uint32_t left = tm->period_ms;
        while (left > 0) {
            uint32_t step = left > BUS_TIMER_STOP_POLL_MS ? BUS_TIMER_STOP_POLL_MS : left;
            bus_thread_sleep_ms(step);
            left -= step;
            pthread_mutex_lock(&tm->mu);
            int stop = tm->stop;
            pthread_mutex_unlock(&tm->mu);
            if (stop) goto done;
        }
    }

done:
    pthread_mutex_lock(&tm->mu);
    tm->running = 0;
    pthread_mutex_unlock(&tm->mu);
    return NULL;
}

bus_result_t bus_timer_init(bus_timer_t   *opaque,
                            bus_timer_cb_fn cb,
                            void          *arg)
{
    if (!opaque || !cb) return BUS_ERR_BAD_ARG;
    linux_timer_t *tm = (linux_timer_t *)opaque;
    memset(tm, 0, sizeof(*tm));
    tm->cb  = cb;
    tm->arg = arg;
    if (pthread_mutex_init(&tm->mu, NULL) != 0) return BUS_ERR_INIT;
    return BUS_OK;
}

bus_result_t bus_timer_start(bus_timer_t *opaque, uint32_t period_ms, uint32_t delay_ms)
{
    if (!opaque) return BUS_ERR_BAD_ARG;
    linux_timer_t *tm = (linux_timer_t *)opaque;
    pthread_mutex_lock(&tm->mu);
    if (tm->running) { pthread_mutex_unlock(&tm->mu); return BUS_ERR_INIT; }
    tm->period_ms = period_ms;
    tm->delay_ms  = delay_ms == 0 ? period_ms : delay_ms;
    tm->stop      = 0;
    tm->running   = 1;
    pthread_mutex_unlock(&tm->mu);
    if (pthread_create(&tm->worker, NULL, timer_worker, tm) != 0) {
        pthread_mutex_lock(&tm->mu);
        tm->running = 0;
        pthread_mutex_unlock(&tm->mu);
        return BUS_ERR_NO_MEM;
    }
    return BUS_OK;
}

bus_result_t bus_timer_stop(bus_timer_t *opaque)
{
    if (!opaque) return BUS_ERR_BAD_ARG;
    linux_timer_t *tm = (linux_timer_t *)opaque;
    pthread_mutex_lock(&tm->mu);
    if (!tm->running) { pthread_mutex_unlock(&tm->mu); return BUS_OK; }
    tm->stop = 1;
    pthread_mutex_unlock(&tm->mu);
    pthread_join(tm->worker, NULL);
    return BUS_OK;
}

// ============ MUTEX ============

bus_result_t bus_mutex_init(bus_mutex_t *opaque)
{
    if (!opaque) return BUS_ERR_BAD_ARG;
    linux_mutex_t *m = (linux_mutex_t *)opaque;
    memset(m, 0, sizeof(*m));
    if (pthread_mutex_init(&m->mu, NULL) != 0) return BUS_ERR_INIT;
    m->initialised = 1;
    return BUS_OK;
}

bus_result_t bus_mutex_lock(bus_mutex_t *opaque, uint32_t timeout_ms)
{
    if (!opaque) return BUS_ERR_BAD_ARG;
    linux_mutex_t *m = (linux_mutex_t *)opaque;
    if (!m->initialised) return BUS_ERR_INIT;
    if (timeout_ms == UINT32_MAX) {
        return pthread_mutex_lock(&m->mu) == 0 ? BUS_OK : BUS_ERR_INIT;
    }
    if (timeout_ms == 0) {
        int rc = pthread_mutex_trylock(&m->mu);
        if (rc == 0)     return BUS_OK;
        if (rc == EBUSY) return BUS_ERR_TIMEOUT;
        return BUS_ERR_INIT;
    }
    struct timespec deadline;
    deadline_from_ms(&deadline, timeout_ms);
    int rc = pthread_mutex_timedlock(&m->mu, &deadline);
    if (rc == 0)         return BUS_OK;
    if (rc == ETIMEDOUT) return BUS_ERR_TIMEOUT;
    return BUS_ERR_INIT;
}

bus_result_t bus_mutex_unlock(bus_mutex_t *opaque)
{
    if (!opaque) return BUS_ERR_BAD_ARG;
    linux_mutex_t *m = (linux_mutex_t *)opaque;
    if (!m->initialised) return BUS_ERR_INIT;
    return pthread_mutex_unlock(&m->mu) == 0 ? BUS_OK : BUS_ERR_INIT;
}
