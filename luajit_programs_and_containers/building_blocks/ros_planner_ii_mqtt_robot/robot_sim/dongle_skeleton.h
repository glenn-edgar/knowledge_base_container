// robot_sim/dongle_skeleton.h
// Slice L4a — four-thread dongle decomposition on the Linux waypoint.
// Mirrors the embedded shape locked in continue.md / Track C: ext_bus
// thread (HIGH), dongle_manager (MED), internal_bus (MED), and (when
// L4b lands) logical_robot threads (LOW). The application above this
// header sees a single dongle_ctx_t holding all queues, the pty, and
// identity.
//
// L4a scope: skeleton only. internal_bus_thread is wired but idle —
// nothing posts to int_bus_q yet because catalogue commands aren't
// routed through to logical robots until L4b. The HELLO/IDENT + PING
// handshake path runs through the new ext_bus → manager → ext_bus
// flow and must remain bit-for-bit compatible with the existing pty
// multi-dongle test.

#pragma once

#include <stdint.h>

#include "bus_kernel.h"
#include "bus_msg.h"
#include "frame.h"
#include "logical_robot.h"
#include "drive_base_robot.h"

#ifdef __cplusplus
extern "C" {
#endif

// ============ INTER-THREAD QUEUE SIZING ============

#define DONGLE_QUEUE_DEPTH       16    // per Track C Q1 default

// ============ MESSAGE-CONVENTION NOTES ============
// Inter-thread queues all use bus_msg_t (40-byte envelope, Track C Q1).
// Direction encoding:
//   - mgr_in_q (ext_bus -> manager): bus_msg_t with
//        src_addr      = frame.addr          (master-bus address used)
//        cmd_lo/hi     = frame.cmd
//        seq           = frame.seq           (master's m2s seq)
//        ack_status    = 0
//        dst_robot     = 0 (manager resolves to a real index for L4b)
//        payload[]     = first payload_len bytes of the m2s frame
//   - ext_bus_tx_q (manager -> ext_bus, also robots -> ext_bus in L4b):
//        src_addr      = which OUR address to put on the wire
//                        (e.g. COMM_ADDR_DONGLE_SELF for IDENT,
//                        slave_addr for ACK_BARE/NAK)
//        cmd_lo/hi     = wire cmd
//        seq           = wire ack_seq (echoes master's seq for ACKs;
//                        0 for spontaneous events)
//        ack_status    = wire ack_status byte
//        dst_robot     = ignored on the wire path (kept 0)
//        payload[]     = up to BUS_MSG_INLINE_PAYLOAD_MAX bytes
//   - int_bus_q (manager -> internal_bus, m2s for routing): same as
//        mgr_in_q except dst_robot is now resolved.
//
// All bus_msg_t for the wire path obey BUS_MSG_INLINE_PAYLOAD_MAX = 32.
// IDENT's 33-byte payload is the one historical exception; for L4a
// the manager builds it directly without a bus_msg_t intermediate
// (see manager_emit_ident_inline below). L4b will refactor this.

// ============ DONGLE CONTEXT ============
// One per robot_sim process. Owned by main.c, shared by all four
// threads via pointer.

#define DONGLE_TX_BUFFER_BYTES   2048    // worst-case s2m frame staged for write
#define DONGLE_RX_BUFFER_BYTES   2048    // pty read scratch + decoder feed

typedef struct {
    int                  master_fd;        // pty master FD owned by ext_bus
    uint16_t             dongle_type;
    uint16_t             dongle_instance;
    uint8_t              slave_addr;       // L4a: single slave addr for PING

    // Threads
    bus_thread_t         ext_bus_th;
    bus_thread_t         manager_th;
    bus_thread_t         internal_bus_th;

    // Queues
    bus_msgq_t           mgr_in_q;
    bus_msg_t            mgr_in_buf [DONGLE_QUEUE_DEPTH];
    bus_msgq_t           int_bus_q;
    bus_msg_t            int_bus_buf[DONGLE_QUEUE_DEPTH];
    bus_msgq_t           ext_tx_q;
    bus_msg_t            ext_tx_buf [DONGLE_QUEUE_DEPTH];

    // pty write serialisation. Two paths can write to the pty:
    //   (a) ext_bus_thread draining ext_tx_q as bus_msg_t (≤32-byte
    //       inline payload, Q1 lock).
    //   (b) dongle_manager_thread emitting IDENT directly with its
    //       33-byte payload (the one historical outlier above the
    //       inline limit). Manager grabs the mutex, builds + encodes
    //       the s2m frame, writes the bytes, releases.
    bus_mutex_t          pty_write_mu;

    // Logical-robot population (Track C Q5 ceiling = 8). L4b hosts
    // exactly one: drive_base at slot 0. Empty slots are NULL pointers
    // and any inbound msg whose dst_robot indexes one of them gets
    // dropped by internal_bus.
    drive_base_t         drive_base;
    bus_msg_t            drive_base_inbox_buf[DONGLE_QUEUE_DEPTH];
    logical_robot_t      drive_base_handle;
    logical_robot_t     *robots[LOGICAL_ROBOT_MAX];      // routing table

    // Cooperative shutdown — set by signal handler in main, observed
    // by every thread loop.
    volatile int         should_exit;
} dongle_ctx_t;

// ============ THREAD ENTRY POINTS ============

void ext_bus_entry      (void *arg);
void dongle_manager_entry(void *arg);
void internal_bus_entry (void *arg);

// Diagnostic trace toggle: ROBOT_SIM_TRACE env var enables per-byte
// hex dumps the same way the old single-thread watcher did.
int  dongle_trace_enabled(void);

#ifdef __cplusplus
}
#endif
