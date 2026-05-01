// libcomm/bus_kernel.h
// Portable kernel-primitive abstraction for the dongle decomposition
// (four-role: ext_bus / dongle_manager / internal_bus / logical_robot×N).
//
// Why this header exists: Linux pthreads, Zephyr k_*, and FreeRTOS
// xTask* all express "thread + bounded queue + timer" the same way at
// the conceptual level but bind to different ABIs. By defining a
// 3-or-4-primitive interface here and implementing it once per RTOS, the
// code above this line becomes target-portable. The interface is the
// upper boundary; everything below is a per-RTOS file
// (bus_kernel_linux.c, bus_kernel_zephyr.c, …).
//
// Discipline:
//   - This header is pure interface. No POSIX includes (no pthread.h),
//     no Zephyr includes. Implementations include their own kernel
//     headers in their .c files.
//   - Storage for handles is opaque; callers allocate
//     `bus_thread_t` etc. by value, the implementation defines the
//     struct in its own .c. (POSIX impl uses pthread_t/etc. inside;
//     Zephyr uses k_thread + stack inside.) The `bus_storage_*` block
//     below gives the worst-case sizes for static allocation.
//   - All blocking calls take a millisecond timeout. 0 = poll, UINT32_MAX
//     = forever. -ETIMEDOUT (-1) returned on timeout.
//   - Message queues are bounded: producer sees BUS_OK or BUS_ERR_FULL,
//     never blocks (consistent with embedded patterns; backpressure must
//     be handled by the caller, never absorbed by infinite buffering).
//
// What's NOT here:
//   - Mutex is intentionally optional — the dongle decomposition uses
//     msgq fan-out, not shared state behind a lock. If a future port
//     genuinely needs a mutex, add bus_mutex_t with init/lock/unlock and
//     keep it minimal. Avoid scope creep.
//   - Atomics, condition variables, semaphores, thread-cancellation:
//     out of scope. If any port needs them, that's a sign the design
//     above the contract leaked an RTOS-ism.

#pragma once

#include <stdint.h>
#include <stddef.h>

#include "bus_config.h"

#ifdef __cplusplus
extern "C" {
#endif

// ============ RESULT CODES ============

typedef enum {
    BUS_OK            =  0,
    BUS_ERR_INIT      = -1,   // primitive not initialised / impl unavailable
    BUS_ERR_BAD_ARG   = -2,   // null pointer, bad size, etc.
    BUS_ERR_FULL      = -3,   // bus_msgq_put on full queue
    BUS_ERR_EMPTY     = -4,   // bus_msgq_get with timeout=0 on empty queue
    BUS_ERR_TIMEOUT   = -5,   // bus_msgq_get hit timeout before a message arrived
    BUS_ERR_NO_MEM    = -6,   // backend ran out of stacks / TCBs / etc.
} bus_result_t;

// ============ STORAGE-SIZE OPAQUE BLOCKS ============
// Worst-case sizes across supported backends; the impl casts these to
// its native types. Sized once at the high-water mark of pthread_t +
// glibc internals on x86_64-linux today. If a smaller backend wins, we
// can shrink them; if a larger one appears, bump and recompile.
//
// Rule: callers always allocate by value, never via malloc, so static
// linkage works on every target.

#define BUS_THREAD_STORAGE_BYTES   128
#define BUS_MSGQ_STORAGE_BYTES     192
#define BUS_TIMER_STORAGE_BYTES     96
#define BUS_MUTEX_STORAGE_BYTES     64

typedef struct { uint64_t _opaque[BUS_THREAD_STORAGE_BYTES / 8]; } bus_thread_t;
typedef struct { uint64_t _opaque[BUS_MSGQ_STORAGE_BYTES   / 8]; } bus_msgq_t;
typedef struct { uint64_t _opaque[BUS_TIMER_STORAGE_BYTES  / 8]; } bus_timer_t;
typedef struct { uint64_t _opaque[BUS_MUTEX_STORAGE_BYTES  / 8]; } bus_mutex_t;

// ============ THREADS ============
// Each thread runs `entry(arg)` once and exits when entry returns.
// On POSIX: pthread + detached. On Zephyr: k_thread + provided stack.
// On embedded targets the per-thread stack is sized by
// BUS_THREAD_STACK_BYTES (overridable per-target in bus_config.h).
//
// Priority is a coarse ordinal — the impl maps it to its native
// priority space:
//   BUS_PRIO_HIGH  → ext_bus thread (ISR-adjacent on embedded)
//   BUS_PRIO_MED   → manager + internal_bus
//   BUS_PRIO_LOW   → logical_robot×N
// Linux pthreads ignores priority unless the process has SCHED_FIFO
// privileges; the impl is allowed to no-op the prio mapping there.

typedef enum {
    BUS_PRIO_LOW  = 1,
    BUS_PRIO_MED  = 2,
    BUS_PRIO_HIGH = 3,
} bus_priority_t;

typedef void (*bus_thread_entry_fn)(void *arg);

bus_result_t bus_thread_start(bus_thread_t      *t,
                              const char        *name,        // ≤16 chars; impl may truncate
                              bus_priority_t     prio,
                              bus_thread_entry_fn entry,
                              void              *arg);

// Wait for the thread to exit. timeout_ms = UINT32_MAX blocks forever.
// On Zephyr backends this maps to k_thread_join.
bus_result_t bus_thread_join(bus_thread_t *t, uint32_t timeout_ms);

// Cooperative yield. Required on FreeRTOS where same-priority round-robin
// needs explicit yield; on POSIX it's an advisory sched_yield.
void         bus_thread_yield(void);

// Sleep the calling thread. Coarse — millisecond resolution.
void         bus_thread_sleep_ms(uint32_t ms);

// ============ MESSAGE QUEUES ============
// Bounded fixed-size message queue. One producer, one consumer is the
// hot path; multi-producer is allowed if the backend supports it (k_msgq
// does, glibc does). msg_size_bytes and depth are fixed at init.
//
// put: BUS_OK on success, BUS_ERR_FULL on full (never blocks).
// get: blocks up to timeout_ms; 0 = poll. Returns BUS_ERR_TIMEOUT
//      (or BUS_ERR_EMPTY if timeout_ms=0).
//
// Discipline: msg sizes should match a struct, not be raw bytes. If the
// caller wants variable-length messages, use a fixed envelope holding a
// length + an inline payload up to BUS_MSGQ_MAX_PAYLOAD; do not
// dynamically allocate.

bus_result_t bus_msgq_init(bus_msgq_t *q,
                           void       *backing_buffer,   // depth * msg_size bytes; caller-owned
                           uint16_t    msg_size_bytes,
                           uint16_t    depth);

bus_result_t bus_msgq_put (bus_msgq_t *q, const void *msg);
bus_result_t bus_msgq_get (bus_msgq_t *q, void *out_msg, uint32_t timeout_ms);

// Drain the queue without consuming. Returns the number of messages
// currently available. Diagnostic only — not safe for flow-control
// decisions in producer code.
uint16_t     bus_msgq_count(const bus_msgq_t *q);

// ============ TIMERS ============
// Periodic timer. Fires `cb(arg)` from the kernel context (Zephyr: k_timer
// expiry handler, runs in ISR context; POSIX: timerfd thread). Period of
// 0 = one-shot.
//
// Discipline: cb MUST be short. NEVER do framing, msgq_put, or anything
// blocking. The supported pattern is "post a sentinel byte to the
// owning thread's msgq, then return." Kernel-context callbacks that take
// >50µs will starve other threads on tight RTOS targets.

typedef void (*bus_timer_cb_fn)(void *arg);

bus_result_t bus_timer_init (bus_timer_t   *tm,
                             bus_timer_cb_fn cb,
                             void          *arg);
bus_result_t bus_timer_start(bus_timer_t   *tm,
                             uint32_t       period_ms,
                             uint32_t       delay_ms);    // first-fire delay; 0 = period_ms
bus_result_t bus_timer_stop (bus_timer_t   *tm);

// ============ OPTIONAL: MUTEX ============
// Provided for backends that genuinely need it. Prefer msgq fan-out.
// All bus_kernel ports MUST implement these (even if unused above the
// contract); keeps the implementation surface uniform.

bus_result_t bus_mutex_init  (bus_mutex_t *m);
bus_result_t bus_mutex_lock  (bus_mutex_t *m, uint32_t timeout_ms);
bus_result_t bus_mutex_unlock(bus_mutex_t *m);

// ============ MONOTONIC CLOCK ============
// Same semantics as comm_now_ms() in comm.h: CLOCK_MONOTONIC ms,
// 32-bit truncated, ~49-day wrap, subtraction-correct across wrap.
// Provided here so non-libcomm code (logical_robot threads, internal_bus)
// can read monotonic time without importing comm.h.

uint32_t bus_now_ms(void);

#ifdef __cplusplus
}
#endif
