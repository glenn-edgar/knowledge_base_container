// libcomm/bus_msg.h
// Internal-bus message envelope for the dongle decomposition. Locked
// at Track C Q1 (2026-05-01): 40-byte slot, 32-byte inline payload,
// same shape both directions (manager↔robot, robot→manager events).
// One bus_msg_t fills exactly one bus_msgq slot.
//
// The dongle_manager owns the only translators between this shape and
// the on-the-wire frame_meta_t shape (see libcomm/frame.h). Anywhere
// else in the dongle, code traffics in bus_msg_t — frame_meta_t never
// crosses the manager boundary inward.
//
// Sentinel reservation: dst_robot = 0xFF is never a valid logical_robot
// index (LOGICAL_ROBOT_MAX is at most 8). The generic logical_robot
// loop uses 0xFF + cmd_lo to distinguish between timer ticks
// (cmd_lo = BUS_MSG_SENTINEL_TICK) and cooperative shutdown
// (cmd_lo = BUS_MSG_SENTINEL_SHUTDOWN). External-bus frames never
// produce a sentinel because the manager validates the address before
// emitting bus_msg_t. The sentinel space is therefore entirely
// internal — losing the value never appears in catalogue traffic.

#pragma once

#include <stdint.h>
#include <stddef.h>

#include "bus_config.h"

#ifdef __cplusplus
extern "C" {
#endif

// ============ THE ENVELOPE ============

typedef struct {
    uint8_t  dst_robot;        // 0..LOGICAL_ROBOT_MAX-1; 0xFF = sentinel
    uint8_t  cmd_lo;           // wire cmd low byte (or sentinel kind for dst=0xFF)
    uint8_t  cmd_hi;           // wire cmd high byte (0 for sentinels)
    uint8_t  seq;              // wire seq (0 for sentinels)
    uint8_t  ack_status;       // s2m only; 0 on m2s and sentinels
    uint8_t  src_addr;         // bus address of originator (0 for sentinels)
    uint8_t  payload_len;      // 0..BUS_MSG_INLINE_PAYLOAD_MAX
    uint8_t  _pad;
    uint8_t  payload[BUS_MSG_INLINE_PAYLOAD_MAX];
} bus_msg_t;

// ============ INVARIANTS (compile-time checks) ============

#define BK_STATIC_ASSERT(cond, msg) typedef char bk_msg_assert_##msg[(cond) ? 1 : -1]
BK_STATIC_ASSERT(sizeof(bus_msg_t) == 40,                       size_locked_at_40);
BK_STATIC_ASSERT(BUS_MSG_INLINE_PAYLOAD_MAX == 32,              payload_locked_at_32);
BK_STATIC_ASSERT(LOGICAL_ROBOT_MAX < BUS_MSG_DST_SENTINEL,      sentinel_outside_robot_space);
BK_STATIC_ASSERT(offsetof(bus_msg_t, payload) == 8,             header_locked_at_8);
#undef BK_STATIC_ASSERT

// ============ HELPERS ============
// Pure header-only; no .c file required. Simple field-shuffle, the
// compiler folds them down to the same memory writes as inlining
// would. Keeps call sites readable.

static inline void bus_msg_make_tick(bus_msg_t *m)
{
    m->dst_robot   = BUS_MSG_DST_SENTINEL;
    m->cmd_lo      = BUS_MSG_SENTINEL_TICK;
    m->cmd_hi      = 0;
    m->seq         = 0;
    m->ack_status  = 0;
    m->src_addr    = 0;
    m->payload_len = 0;
    m->_pad        = 0;
    // payload bytes intentionally untouched — irrelevant for sentinel.
}

static inline void bus_msg_make_shutdown(bus_msg_t *m)
{
    m->dst_robot   = BUS_MSG_DST_SENTINEL;
    m->cmd_lo      = BUS_MSG_SENTINEL_SHUTDOWN;
    m->cmd_hi      = 0;
    m->seq         = 0;
    m->ack_status  = 0;
    m->src_addr    = 0;
    m->payload_len = 0;
    m->_pad        = 0;
}

static inline int bus_msg_is_sentinel(const bus_msg_t *m)
{
    return m->dst_robot == BUS_MSG_DST_SENTINEL;
}

// Returns the sentinel kind (BUS_MSG_SENTINEL_TICK / _SHUTDOWN) when
// bus_msg_is_sentinel(m) is true, otherwise 0xFF (never a valid kind).
static inline uint8_t bus_msg_sentinel_kind(const bus_msg_t *m)
{
    return bus_msg_is_sentinel(m) ? m->cmd_lo : 0xFFu;
}

#ifdef __cplusplus
}
#endif
