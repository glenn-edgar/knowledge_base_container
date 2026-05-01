// libcomm/logical_robot.h
// Generic lifecycle for a logical_robot inside a dongle. Track C Q2 lock
// (2026-05-01): each logical_robot is a long-lived bus_thread blocking
// on its inbox; periodic ticks delivered as sentinel bus_msg_t posted
// by a bus_timer. The application supplies a 4-function vtable
// (init / on_msg / tick / shutdown); everything else is generic.
//
// Why this is a standalone library: drive_base, gripper, lidar, and
// every future logical_robot class share this exact entry-loop shape.
// The vtable + handle pattern keeps the library 1-file-per-class
// without recompiling the framework.
//
// Discipline carried in from earlier memories:
//   - on_msg / tick MUST NOT block on I/O (feedback_chain_tree_no_blocking_io).
//   - Handler budget ≤50 ms (feedback_phase6_handler_budget).
//   - Faults are fail-stop (feedback_no_soft_faults). on_msg / tick
//     should not retry; raise an event upward and let the manager
//     decide.
//
// Threading model:
//   - Each logical_robot owns ONE thread, ONE inbox msgq, ONE optional
//     timer. The thread blocks in bus_msgq_get(K_FOREVER) when idle —
//     no CPU cost while waiting.
//   - Sentinels (dst_robot=0xFF) drive tick + shutdown. Real bus_msg_t
//     destined for the robot have dst_robot in [0..LOGICAL_ROBOT_MAX-1].
//   - logical_robot_post() is multi-producer-safe (the manager and
//     the internal_bus thread can both post; bus_msgq_put serialises).
//   - logical_robot_shutdown() is one-shot: post a shutdown sentinel,
//     join the thread, free resources.

#pragma once

#include <stdint.h>
#include <stddef.h>

#include "bus_kernel.h"
#include "bus_msg.h"

#ifdef __cplusplus
extern "C" {
#endif

// ============ VTABLE ============

typedef struct {
    void     (*init)    (void *self);                         // called once before the loop starts
    void     (*on_msg)  (void *self, const bus_msg_t *m);     // called for every non-sentinel inbox message
    void     (*tick)    (void *self, uint32_t now_ms);        // called when timer fires; NULL-able if tick_period_ms == 0
    void     (*shutdown)(void *self);                         // called once after loop exits
    uint32_t   tick_period_ms;                                // 0 = no periodic tick
} logical_robot_vtable_t;

// ============ HANDLE ============
// Caller allocates by value (typically as a static or as a member of a
// dongle context struct). All threading state lives inside.

typedef struct {
    bus_thread_t                   thread;
    bus_msgq_t                     inbox;
    bus_msg_t                     *inbox_buf;        // backing storage; caller-owned
    uint16_t                       inbox_depth;
    uint16_t                       _pad;
    bus_timer_t                    tick_timer;
    int                            tick_running;     // 1 if timer was started
    const logical_robot_vtable_t  *vtable;
    void                          *self;
    char                           name[16];
} logical_robot_t;

// ============ API ============

// Start the robot. Caller must keep `vtable` and `inbox_buf` alive for
// the lifetime of the robot. `inbox_buf` must hold inbox_depth slots
// of sizeof(bus_msg_t) each. Returns BUS_OK on success.
//
// Concurrency: must be called from a single thread; not safe to call
// concurrently with logical_robot_post or logical_robot_shutdown for
// the same handle.
bus_result_t logical_robot_init(logical_robot_t              *r,
                                const char                    *name,
                                const logical_robot_vtable_t *vtable,
                                void                          *self,
                                bus_msg_t                     *inbox_buf,
                                uint16_t                       inbox_depth);

// Post a non-sentinel message to the robot's inbox. The msg is copied
// (bus_msgq_put semantics). Returns BUS_ERR_FULL if the inbox is full
// — per Track C Q1, callers should NAK and not retry.
//
// Refuses sentinel-shaped messages (dst_robot == 0xFF) with
// BUS_ERR_BAD_ARG; sentinels are only ever produced by the robot's
// own timer and by logical_robot_shutdown.
bus_result_t logical_robot_post(logical_robot_t *r, const bus_msg_t *m);

// Cooperatively stop the robot. Posts a shutdown sentinel, joins the
// thread, stops the timer, calls vtable->shutdown. Idempotent: a
// second call returns BUS_OK and does nothing.
bus_result_t logical_robot_shutdown(logical_robot_t *r);

// Diagnostic: number of messages currently queued in the inbox.
uint16_t logical_robot_inbox_count(const logical_robot_t *r);

#ifdef __cplusplus
}
#endif
