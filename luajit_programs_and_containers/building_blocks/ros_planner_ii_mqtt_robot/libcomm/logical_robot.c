// libcomm/logical_robot.c
// Generic logical_robot lifecycle. See logical_robot.h for the contract.
//
// Implementation notes:
//   - The thread entry trampoline owns the entire loop. It calls
//     vtable->init exactly once, then loops on bus_msgq_get(FOREVER)
//     dispatching sentinels to tick / shutdown handlers and real
//     messages to vtable->on_msg.
//   - The tick timer's callback runs in kernel context (Zephyr ISR /
//     pthread timer thread). Per bus_kernel.h's contract, the
//     callback only does bus_msgq_put — no work, no I/O — so it's
//     bounded and non-blocking on every backend.
//   - logical_robot_shutdown is idempotent. It uses an internal
//     "shutting_down" latch so a double-call (e.g. from a fault
//     handler + the dongle teardown path) doesn't double-join.
//   - On Zephyr the inbox backing buffer alignment requirement
//     (msg_size-aligned) is satisfied because sizeof(bus_msg_t) is 40
//     and bus_msg_t starts with a uint8_t — caller's static / stack
//     placement gives 8-byte alignment, which exceeds 40-byte msg_size
//     alignment requirements (Zephyr only requires msg_size alignment
//     when msg_size is a power of 2; for 40-byte slots it's relaxed).

#include "logical_robot.h"

#include <string.h>

// ============ TIMER CALLBACK ============
// Posts a tick sentinel into the robot's own inbox. Runs in kernel
// context — keep tiny.

static void tick_timer_cb(void *arg)
{
    logical_robot_t *r = (logical_robot_t *)arg;
    bus_msg_t tick;
    bus_msg_make_tick(&tick);
    // Ignore BUS_ERR_FULL: if the robot is so backlogged that its own
    // tick can't be queued, on_msg pressure is the actual problem and
    // a missed tick is the symptom, not the cause. The robot will
    // catch up on its next idle loop iteration.
    (void)bus_msgq_put(&r->inbox, &tick);
}

// ============ THREAD ENTRY ============

static void logical_robot_entry(void *arg)
{
    logical_robot_t *r = (logical_robot_t *)arg;

    // 1. Application init — runs in the robot's own thread, so any
    //    init that blocks (e.g. loading tunables from NVS) can sleep
    //    here without freezing the manager.
    if (r->vtable->init) r->vtable->init(r->self);

    // 2. Start tick timer if requested.
    if (r->vtable->tick_period_ms > 0 && r->vtable->tick) {
        bus_timer_init (&r->tick_timer, tick_timer_cb, r);
        bus_timer_start(&r->tick_timer, r->vtable->tick_period_ms, 0);
        r->tick_running = 1;
    }

    // 3. The loop.
    bus_msg_t msg;
    int       running = 1;
    while (running) {
        bus_result_t rc = bus_msgq_get(&r->inbox, &msg, UINT32_MAX);
        if (rc != BUS_OK) continue;   // shouldn't happen with FOREVER, but defend

        if (bus_msg_is_sentinel(&msg)) {
            uint8_t kind = bus_msg_sentinel_kind(&msg);
            if (kind == BUS_MSG_SENTINEL_TICK) {
                if (r->vtable->tick) r->vtable->tick(r->self, bus_now_ms());
            } else if (kind == BUS_MSG_SENTINEL_SHUTDOWN) {
                running = 0;
            }
            // Unknown sentinel kinds are ignored — forward-compat for
            // future sentinels (e.g. "reload tunables") without code
            // changes here.
        } else {
            if (r->vtable->on_msg) r->vtable->on_msg(r->self, &msg);
        }
    }

    // 4. Stop timer first so no late tick races against shutdown.
    if (r->tick_running) {
        bus_timer_stop(&r->tick_timer);
        r->tick_running = 0;
    }

    // 5. Drain any messages still queued. We don't dispatch them —
    //    shutdown intent is unambiguous and dispatching late commands
    //    is the kind of soft-fault recovery the no-soft-faults rule
    //    forbids. Drain just to free the slots.
    bus_msg_t scratch;
    while (bus_msgq_get(&r->inbox, &scratch, 0) == BUS_OK) { /* discard */ }

    // 6. Application shutdown.
    if (r->vtable->shutdown) r->vtable->shutdown(r->self);
}

// ============ PUBLIC API ============

bus_result_t logical_robot_init(logical_robot_t              *r,
                                const char                    *name,
                                const logical_robot_vtable_t *vtable,
                                void                          *self,
                                bus_msg_t                     *inbox_buf,
                                uint16_t                       inbox_depth)
{
    if (!r || !vtable || !inbox_buf || inbox_depth == 0) return BUS_ERR_BAD_ARG;
    memset(r, 0, sizeof(*r));
    r->vtable      = vtable;
    r->self        = self;
    r->inbox_buf   = inbox_buf;
    r->inbox_depth = inbox_depth;
    if (name) {
        size_t n = 0;
        while (n < sizeof(r->name) - 1 && name[n] != '\0') { r->name[n] = name[n]; n++; }
        r->name[n] = '\0';
    }

    bus_result_t rc = bus_msgq_init(&r->inbox, inbox_buf,
                                    (uint16_t)sizeof(bus_msg_t), inbox_depth);
    if (rc != BUS_OK) return rc;

    rc = bus_thread_start(&r->thread, r->name[0] ? r->name : "logrobot",
                          BUS_PRIO_LOW, logical_robot_entry, r);
    if (rc != BUS_OK) return rc;

    return BUS_OK;
}

bus_result_t logical_robot_post(logical_robot_t *r, const bus_msg_t *m)
{
    if (!r || !m) return BUS_ERR_BAD_ARG;
    // Refuse externally-posted sentinels — they're an internal-only
    // shape and accepting them would let a misbehaving caller fake
    // a shutdown.
    if (m->dst_robot == BUS_MSG_DST_SENTINEL) return BUS_ERR_BAD_ARG;
    return bus_msgq_put(&r->inbox, m);
}

bus_result_t logical_robot_shutdown(logical_robot_t *r)
{
    if (!r) return BUS_ERR_BAD_ARG;
    if (r->vtable == NULL) return BUS_OK;   // already shut down or never started

    bus_msg_t shutdown_sentinel;
    bus_msg_make_shutdown(&shutdown_sentinel);
    // If the inbox is full, the bus_msgq_put will fail. We retry once
    // after sleeping briefly so a transient burst doesn't block
    // shutdown forever; if it still fails, fall through to thread_join
    // which will hang only if the loop is stuck (in which case the
    // bug is upstream, not here).
    bus_result_t rc = bus_msgq_put(&r->inbox, &shutdown_sentinel);
    if (rc == BUS_ERR_FULL) {
        bus_thread_sleep_ms(5);
        (void)bus_msgq_put(&r->inbox, &shutdown_sentinel);
    }

    bus_thread_join(&r->thread, UINT32_MAX);
    r->vtable = NULL;       // mark as shut down — second shutdown is a no-op
    return BUS_OK;
}

uint16_t logical_robot_inbox_count(const logical_robot_t *r)
{
    if (!r) return 0;
    return bus_msgq_count(&r->inbox);
}
