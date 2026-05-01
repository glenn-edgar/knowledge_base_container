// libcomm/bus_kernel_zephyr.c
// Zephyr backend for bus_kernel.h. SKELETON — does not compile under
// Linux. Will compile + link inside a Zephyr workspace once Track B.1
// builds samples/hello_world for rpi_pico (see continue.md Track B.2).
//
// Why this file exists today (Track A.4): it forces every porting
// decision to surface NOW, while the bus_kernel.h interface is still
// soft. Each primitive lists the exact Zephyr kernel call(s) it maps to,
// the per-thread storage (k_thread + stack) requirement, and any
// Zephyr-specific gotchas (timer ISR context, k_msgq alignment, etc.).
// When the actual Zephyr build is wired up the body changes from `// TODO`
// to working code with no surprises.
//
// Zephyr APIs referenced (Zephyr 3.x, kernel.h):
//   k_thread_create / k_thread_join / k_thread_priority_set
//   k_msgq_init / k_msgq_put / k_msgq_get / k_msgq_num_used_get
//   k_timer_init / k_timer_start / k_timer_stop
//   k_mutex_init / k_mutex_lock / k_mutex_unlock
//   k_uptime_get_32 / k_msleep / k_yield
//
// Gotchas surfaced by writing this skeleton:
//   1. Per-thread stack: Zephyr requires K_THREAD_STACK_DEFINE per thread
//      at file scope, OR K_THREAD_STACK_ARRAY_DEFINE for an array. The
//      bus_kernel_t opaque blob can't hold the stack itself (it's macro-
//      defined externally). Solution: caller supplies the stack via a
//      new bus_thread_start_with_stack() variant on Zephyr — OR — we
//      keep a static pool here (BUS_THREAD_POOL_SIZE × BUS_THREAD_STACK_BYTES
//      from bus_config.h) and hand out a stack per start(). Pool is the
//      right call: keeps the API uniform across backends.
//   2. k_msgq backing buffer must be aligned to msg_size and contiguous.
//      Caller-supplied buffer is fine if msg_size is a power of 2 ≥ 4.
//      Document this constraint in bus_kernel.h before the first port.
//   3. k_timer expiry callback runs in ISR context. Existing bus_timer.h
//      contract already says the cb must be short and non-blocking; the
//      Zephyr port just enforces it harder. logical_robot timer cbs
//      MUST post to a msgq, never do work inline.
//   4. k_thread_join exists since Zephyr 3.0; no fallback needed.
//   5. Priority mapping: Zephyr cooperative priorities are negative,
//      preemptive are 0..CONFIG_NUM_PREEMPT_PRIORITIES-1. We map
//      BUS_PRIO_HIGH=2, MED=5, LOW=8 (preemptive). ext_bus on HIGH;
//      manager+internal_bus on MED; logical_robot on LOW.
//
// Build wiring (when this becomes real):
//   - This file goes into the Zephyr application's CMakeLists.txt as
//     `target_sources(app PRIVATE libcomm/bus_kernel_zephyr.c)`.
//   - The Linux Makefile MUST NOT include this file in its srcs list.
//     A guard at the top makes accidental inclusion fail loud.

#ifndef __ZEPHYR__
#error "bus_kernel_zephyr.c must only be built inside a Zephyr workspace"
#endif

#include "bus_kernel.h"
#include <zephyr/kernel.h>
#include <zephyr/sys/printk.h>
#include <string.h>

// ============ INTERNAL LAYOUTS ============

typedef struct {
    struct k_thread       thread;
    k_tid_t               tid;
    bus_thread_entry_fn   entry;
    void                 *arg;
    k_thread_stack_t     *stack;
    int                   stack_pool_idx;
    char                  name[16];
} zephyr_thread_t;

typedef struct {
    struct k_msgq         q;
    uint16_t              msg_size;
    uint16_t              depth;
} zephyr_msgq_t;

typedef struct {
    struct k_timer        timer;
    bus_timer_cb_fn       cb;
    void                 *arg;
    uint32_t              period_ms;
    uint32_t              delay_ms;
} zephyr_timer_t;

typedef struct {
    struct k_mutex        mu;
    int                   initialised;
} zephyr_mutex_t;

// Storage-budget enforcement; bumped if any zephyr_*_t outgrows its slot.
#define BK_STATIC_ASSERT(cond, msg) typedef char bk_assert_##msg[(cond) ? 1 : -1]
BK_STATIC_ASSERT(sizeof(zephyr_thread_t) <= BUS_THREAD_STORAGE_BYTES, thread_fits);
BK_STATIC_ASSERT(sizeof(zephyr_msgq_t)   <= BUS_MSGQ_STORAGE_BYTES,   msgq_fits);
BK_STATIC_ASSERT(sizeof(zephyr_timer_t)  <= BUS_TIMER_STORAGE_BYTES,  timer_fits);
BK_STATIC_ASSERT(sizeof(zephyr_mutex_t)  <= BUS_MUTEX_STORAGE_BYTES,  mutex_fits);

// ============ STATIC THREAD-STACK POOL ============
// One pool, BUS_THREAD_POOL_SIZE entries × BUS_THREAD_STACK_BYTES bytes
// each. Allocated at static-link time so no malloc on RTOS targets.
// bus_thread_start grabs the first free entry; bus_thread_join releases
// it on success. If the pool runs dry, start returns BUS_ERR_NO_MEM.

#ifndef BUS_THREAD_POOL_SIZE
#define BUS_THREAD_POOL_SIZE 8       // covers 4-role decomposition × 2 dongles
#endif

K_THREAD_STACK_ARRAY_DEFINE(g_thread_stacks,
                            BUS_THREAD_POOL_SIZE,
                            BUS_THREAD_STACK_BYTES);
static uint8_t g_stack_used[BUS_THREAD_POOL_SIZE];   // 0 = free, 1 = in use

static int alloc_stack_slot(void) {
    for (int i = 0; i < BUS_THREAD_POOL_SIZE; i++) {
        if (!g_stack_used[i]) { g_stack_used[i] = 1; return i; }
    }
    return -1;
}
static void release_stack_slot(int idx) {
    if (idx >= 0 && idx < BUS_THREAD_POOL_SIZE) g_stack_used[idx] = 0;
}

// ============ CLOCK ============

uint32_t bus_now_ms(void) {
    return k_uptime_get_32();
}

// ============ THREADS ============

static void thread_trampoline(void *p1, void *p2, void *p3) {
    (void)p2; (void)p3;
    zephyr_thread_t *t = (zephyr_thread_t *)p1;
    t->entry(t->arg);
}

static int prio_map(bus_priority_t p) {
    switch (p) {
        case BUS_PRIO_HIGH: return 2;     // ext_bus
        case BUS_PRIO_MED:  return 5;     // manager / internal_bus
        case BUS_PRIO_LOW:  return 8;     // logical_robot
        default:            return 5;
    }
}

bus_result_t bus_thread_start(bus_thread_t        *opaque,
                              const char          *name,
                              bus_priority_t       prio,
                              bus_thread_entry_fn  entry,
                              void                *arg)
{
    if (!opaque || !entry) return BUS_ERR_BAD_ARG;
    zephyr_thread_t *t = (zephyr_thread_t *)opaque;
    memset(t, 0, sizeof(*t));

    int slot = alloc_stack_slot();
    if (slot < 0) return BUS_ERR_NO_MEM;
    t->stack          = g_thread_stacks[slot];
    t->stack_pool_idx = slot;
    t->entry = entry;
    t->arg   = arg;
    if (name) strncpy(t->name, name, sizeof(t->name) - 1);

    t->tid = k_thread_create(&t->thread,
                             t->stack,
                             BUS_THREAD_STACK_BYTES,
                             thread_trampoline, t, NULL, NULL,
                             prio_map(prio),
                             0,           // options
                             K_NO_WAIT);
    if (!t->tid) {
        release_stack_slot(slot);
        return BUS_ERR_NO_MEM;
    }
    if (name) k_thread_name_set(t->tid, t->name);
    return BUS_OK;
}

bus_result_t bus_thread_join(bus_thread_t *opaque, uint32_t timeout_ms) {
    if (!opaque) return BUS_ERR_BAD_ARG;
    zephyr_thread_t *t = (zephyr_thread_t *)opaque;
    k_timeout_t to = (timeout_ms == UINT32_MAX) ? K_FOREVER : K_MSEC(timeout_ms);
    int rc = k_thread_join(&t->thread, to);
    if (rc == -EAGAIN) return BUS_ERR_TIMEOUT;
    if (rc != 0)       return BUS_ERR_INIT;
    release_stack_slot(t->stack_pool_idx);
    return BUS_OK;
}

void bus_thread_yield(void)             { k_yield(); }
void bus_thread_sleep_ms(uint32_t ms)   { k_msleep((int32_t)ms); }

// ============ MESSAGE QUEUES ============

bus_result_t bus_msgq_init(bus_msgq_t *opaque,
                           void       *backing_buffer,
                           uint16_t    msg_size,
                           uint16_t    depth)
{
    if (!opaque || !backing_buffer || msg_size == 0 || depth == 0) return BUS_ERR_BAD_ARG;
    zephyr_msgq_t *q = (zephyr_msgq_t *)opaque;
    q->msg_size = msg_size;
    q->depth    = depth;
    k_msgq_init(&q->q, (char *)backing_buffer, msg_size, depth);
    return BUS_OK;
}

bus_result_t bus_msgq_put(bus_msgq_t *opaque, const void *msg) {
    if (!opaque || !msg) return BUS_ERR_BAD_ARG;
    zephyr_msgq_t *q = (zephyr_msgq_t *)opaque;
    int rc = k_msgq_put(&q->q, msg, K_NO_WAIT);
    if (rc == 0)        return BUS_OK;
    if (rc == -ENOMSG)  return BUS_ERR_FULL;
    return BUS_ERR_INIT;
}

bus_result_t bus_msgq_get(bus_msgq_t *opaque, void *out_msg, uint32_t timeout_ms) {
    if (!opaque || !out_msg) return BUS_ERR_BAD_ARG;
    zephyr_msgq_t *q = (zephyr_msgq_t *)opaque;
    k_timeout_t to = (timeout_ms == UINT32_MAX) ? K_FOREVER
                  : (timeout_ms == 0)            ? K_NO_WAIT
                                                 : K_MSEC(timeout_ms);
    int rc = k_msgq_get(&q->q, out_msg, to);
    if (rc == 0)        return BUS_OK;
    if (rc == -ENOMSG)  return (timeout_ms == 0) ? BUS_ERR_EMPTY : BUS_ERR_TIMEOUT;
    if (rc == -EAGAIN)  return BUS_ERR_TIMEOUT;
    return BUS_ERR_INIT;
}

uint16_t bus_msgq_count(const bus_msgq_t *opaque) {
    if (!opaque) return 0;
    zephyr_msgq_t *q = (zephyr_msgq_t *)(uintptr_t)opaque;
    return (uint16_t)k_msgq_num_used_get(&q->q);
}

// ============ TIMERS ============
// k_timer expiry runs in ISR context. The bus_kernel.h contract already
// forbids work in cb; the application is expected to k_msgq_put a
// sentinel and return.

static void timer_expiry(struct k_timer *kt) {
    zephyr_timer_t *tm = CONTAINER_OF(kt, zephyr_timer_t, timer);
    tm->cb(tm->arg);
}

bus_result_t bus_timer_init(bus_timer_t   *opaque,
                            bus_timer_cb_fn cb,
                            void          *arg)
{
    if (!opaque || !cb) return BUS_ERR_BAD_ARG;
    zephyr_timer_t *tm = (zephyr_timer_t *)opaque;
    memset(tm, 0, sizeof(*tm));
    tm->cb  = cb;
    tm->arg = arg;
    k_timer_init(&tm->timer, timer_expiry, NULL);
    return BUS_OK;
}

bus_result_t bus_timer_start(bus_timer_t *opaque, uint32_t period_ms, uint32_t delay_ms) {
    if (!opaque) return BUS_ERR_BAD_ARG;
    zephyr_timer_t *tm = (zephyr_timer_t *)opaque;
    tm->period_ms = period_ms;
    tm->delay_ms  = (delay_ms == 0) ? period_ms : delay_ms;
    k_timer_start(&tm->timer,
                  K_MSEC(tm->delay_ms),
                  (period_ms == 0) ? K_NO_WAIT : K_MSEC(period_ms));
    return BUS_OK;
}

bus_result_t bus_timer_stop(bus_timer_t *opaque) {
    if (!opaque) return BUS_ERR_BAD_ARG;
    zephyr_timer_t *tm = (zephyr_timer_t *)opaque;
    k_timer_stop(&tm->timer);
    return BUS_OK;
}

// ============ MUTEX ============

bus_result_t bus_mutex_init(bus_mutex_t *opaque) {
    if (!opaque) return BUS_ERR_BAD_ARG;
    zephyr_mutex_t *m = (zephyr_mutex_t *)opaque;
    k_mutex_init(&m->mu);
    m->initialised = 1;
    return BUS_OK;
}

bus_result_t bus_mutex_lock(bus_mutex_t *opaque, uint32_t timeout_ms) {
    if (!opaque) return BUS_ERR_BAD_ARG;
    zephyr_mutex_t *m = (zephyr_mutex_t *)opaque;
    if (!m->initialised) return BUS_ERR_INIT;
    k_timeout_t to = (timeout_ms == UINT32_MAX) ? K_FOREVER
                  : (timeout_ms == 0)            ? K_NO_WAIT
                                                 : K_MSEC(timeout_ms);
    int rc = k_mutex_lock(&m->mu, to);
    if (rc == 0)         return BUS_OK;
    if (rc == -EAGAIN)   return BUS_ERR_TIMEOUT;
    if (rc == -EBUSY)    return BUS_ERR_TIMEOUT;
    return BUS_ERR_INIT;
}

bus_result_t bus_mutex_unlock(bus_mutex_t *opaque) {
    if (!opaque) return BUS_ERR_BAD_ARG;
    zephyr_mutex_t *m = (zephyr_mutex_t *)opaque;
    if (!m->initialised) return BUS_ERR_INIT;
    return (k_mutex_unlock(&m->mu) == 0) ? BUS_OK : BUS_ERR_INIT;
}
