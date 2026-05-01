// robot_sim/dongle_threads.c
// Slice L4a — three infrastructure-thread bodies for the four-thread
// dongle decomposition. The fourth role (logical_robot threads) is
// hosted starting in L4b. See dongle_skeleton.h for the contract.
//
// Implementation discipline:
//   - All three threads are started via bus_thread_start; they own
//     no global state. Each function takes dongle_ctx_t* via void*.
//   - Wire encode/decode uses libcomm/frame.c (frame_encode_s2m,
//     frame_decoder_*). Same byte format the existing pty test
//     pins against — no protocol changes in L4a.
//   - HELLO/IDENT path is inlined in dongle_manager_entry exactly as
//     the old single-threaded watcher_thread did, so the existing
//     pty multi-dongle test (18 checks) is the regression gate.
//   - PING handling (legacy compat for L4a) is also in the manager,
//     replying ACK_BARE via the ext_tx_q. L4b will refactor this to
//     run through internal_bus + a logical_robot.

#define _XOPEN_SOURCE 700
#define _DEFAULT_SOURCE
#define _POSIX_C_SOURCE 200809L

#include "dongle_skeleton.h"

#include "comm.h"
#include "frame.h"
#include "bus_kernel.h"
#include "bus_msg.h"

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

int dongle_trace_enabled(void)
{
    static int cached = -1;
    if (cached < 0) cached = getenv("ROBOT_SIM_TRACE") ? 1 : 0;
    return cached;
}

// ============ shared frame encode/write helpers ============

// Encode an s2m frame and write the bytes to the pty under the write
// mutex. Used by both ext_bus_entry (draining ext_tx_q) and
// dongle_manager_entry (sending IDENT). Returns 0 on success, -1 on
// I/O error.
static int write_s2m_frame(dongle_ctx_t   *ctx,
                           uint8_t         addr,
                           comm_cmd_t      cmd,
                           uint8_t         ack_seq,
                           uint8_t         ack_status,
                           const uint8_t  *payload,
                           uint8_t         payload_len)
{
    // Worst-case SLIP-escaped frame: ~2 * FRAME_BUFFER_MAX + 3.
    uint8_t      ring_buf[512];
    frame_ring_t ring;
    frame_ring_init(&ring, ring_buf, sizeof(ring_buf));

    frame_meta_t meta;
    memset(&meta, 0, sizeof(meta));
    meta.addr        = addr;
    meta.cmd         = cmd;
    meta.seq         = 0;
    meta.ack_seq     = ack_seq;
    meta.ack_status  = ack_status;
    meta.payload_len = payload_len;

    if (frame_encode_s2m(&meta, payload, &ring) != 0) return -1;

    uint8_t  scratch[FRAME_BUFFER_MAX * 2];
    uint32_t n = frame_ring_read_drain(&ring, scratch, sizeof(scratch));

    bus_mutex_lock(&ctx->pty_write_mu, UINT32_MAX);
    int rc = 0;
    size_t off = 0;
    while (off < n) {
        ssize_t w = write(ctx->master_fd, scratch + off, n - off);
        if (w < 0) {
            if (errno == EINTR) continue;
            rc = -1; break;
        }
        off += (size_t)w;
    }
    bus_mutex_unlock(&ctx->pty_write_mu);

    if (dongle_trace_enabled() && rc == 0) {
        fprintf(stderr, "[robot_sim] writing %u bytes:", n);
        for (uint32_t i = 0; i < n; i++) fprintf(stderr, " %02x", scratch[i]);
        fprintf(stderr, "\n");
    }
    return rc;
}

// ============ ext_bus_thread ============
// HIGH priority on Zephyr. Owns the pty fd. Two responsibilities per
// loop: drain ext_tx_q (s2m bus_msg_t → encoded wire bytes) and read
// pty bytes (decode → m2s bus_msg_t → mgr_in_q).
//
// Uses poll(pty_fd, 5ms) so the loop wakes either on incoming bytes
// or the timeout, then non-blocking-drains ext_tx_q before next poll.

void ext_bus_entry(void *arg)
{
    dongle_ctx_t *ctx = (dongle_ctx_t *)arg;

    frame_decoder_t dec;
    frame_decoder_init(&dec, FRAME_DIR_M2S);

    uint8_t  rx_buf[256];
    int      trace = dongle_trace_enabled();

    while (!ctx->should_exit) {
        // 1) Drain ext_tx_q (non-blocking). Multiple producers may
        //    have stacked s2m frames since the last loop iteration.
        for (;;) {
            bus_msg_t out;
            bus_result_t rc = bus_msgq_get(&ctx->ext_tx_q, &out, 0);
            if (rc != BUS_OK) break;
            if (out.payload_len > BUS_MSG_INLINE_PAYLOAD_MAX) continue;  // defensive
            comm_cmd_t cmd = (comm_cmd_t)((uint16_t)out.cmd_lo
                                       | ((uint16_t)out.cmd_hi << 8));
            (void)write_s2m_frame(ctx,
                                  out.src_addr,
                                  cmd,
                                  out.seq,            // ack_seq (s2m)
                                  out.ack_status,
                                  out.payload,
                                  out.payload_len);
        }

        // 2) Poll pty for incoming bytes. 5 ms timeout balances
        //    outbound latency vs. wakeup overhead. Inbound bursts of
        //    25-byte frames take ~270 µs at 921.6 kbps so we never
        //    miss a frame within one poll cycle.
        struct pollfd pfd = { .fd = ctx->master_fd, .events = POLLIN };
        int p = poll(&pfd, 1, 5);
        if (p < 0) {
            if (errno == EINTR) continue;
            break;
        }
        if (p == 0) continue;
        if (pfd.revents & (POLLERR | POLLHUP | POLLNVAL)) {
            // Peer closed; let the read below return 0/EIO and exit.
        }
        if (!(pfd.revents & POLLIN)) continue;

        ssize_t r = read(ctx->master_fd, rx_buf, sizeof(rx_buf));
        if (r < 0) {
            if (errno == EINTR) continue;
            if (errno != EIO) {
                fprintf(stderr, "robot_sim: pty read errno=%d (%s)\n",
                        errno, strerror(errno));
            }
            break;
        }
        if (r == 0) break;
        if (trace) {
            fprintf(stderr, "[robot_sim] read %ld bytes:", (long)r);
            for (ssize_t i = 0; i < r; i++) fprintf(stderr, " %02x", rx_buf[i]);
            fprintf(stderr, "\n");
        }

        for (ssize_t i = 0; i < r; i++) {
            frame_meta_t fm;
            uint8_t      fp[COMM_PAYLOAD_MAX];
            frame_decode_result_t dr =
                frame_decoder_feed(&dec, rx_buf[i], &fm, fp);
            if (dr != FRAME_DECODE_FRAME_READY) {
                if (dr != FRAME_DECODE_NEED_MORE && trace) {
                    fprintf(stderr, "[robot_sim] decode error %d on byte %02x\n",
                            (int)dr, rx_buf[i]);
                }
                continue;
            }
            if (trace) {
                fprintf(stderr,
                        "[robot_sim] frame addr=%02x cmd=%04x seq=%02x len=%u\n",
                        fm.addr, fm.cmd, fm.seq, fm.payload_len);
            }

            // Translate frame_meta_t → bus_msg_t for manager.
            // payload_len is bounded by COMM_PAYLOAD_MAX (128) on the
            // wire but our inter-thread bus_msg_t caps at 32 bytes.
            // For L4a, every catalogue frame we care about (HELLO 0 B,
            // PING 0 B, NAK 1 B) fits. Larger payloads trigger Q1's
            // escape hatch — defer to L4b.
            bus_msg_t in;
            memset(&in, 0, sizeof(in));
            in.dst_robot   = 0;                    // unresolved; manager sets
            in.cmd_lo      = (uint8_t)(fm.cmd & 0xFF);
            in.cmd_hi      = (uint8_t)(fm.cmd >> 8);
            in.seq         = fm.seq;
            in.ack_status  = 0;                    // m2s
            in.src_addr    = fm.addr;
            uint8_t pl = fm.payload_len;
            if (pl > BUS_MSG_INLINE_PAYLOAD_MAX) pl = BUS_MSG_INLINE_PAYLOAD_MAX;
            in.payload_len = pl;
            if (pl > 0) memcpy(in.payload, fp, pl);

            // If queue is full we have an architectural problem
            // (manager not draining). Per feedback_no_soft_faults
            // we don't retry — drop and trace.
            bus_result_t put_rc = bus_msgq_put(&ctx->mgr_in_q, &in);
            if (put_rc != BUS_OK && trace) {
                fprintf(stderr, "[robot_sim] mgr_in_q full, dropped frame\n");
            }
        }
    }

    ctx->should_exit = 1;
}

// ============ dongle_manager_thread ============
// MED priority. Inbox: mgr_in_q. Handles HELLO/IDENT inline (with the
// 33-byte payload, bypassing the bus_msg_t inline limit) and answers
// PING with ACK_BARE via the ext_tx_q. Anything else gets a NAK.
// L4b will route non-handshake frames into int_bus_q.

// IDENT payload format (Phase B): 33 bytes total.
//   uuid[16]                   bytes 0-1 = type LE, 2-3 = instance LE, rest zero
//   fw_ver  u32                fixed at 0x00010000 for Phase B
//   bus_count u8               1
//   bus_local_ids[8]           [0]=0, rest 0
//   capabilities u32           0
static int manager_emit_ident(dongle_ctx_t *ctx, uint8_t in_seq)
{
    uint8_t payload[33];
    memset(payload, 0, sizeof(payload));
    comm_dongle_set_type    (payload + 0, ctx->dongle_type);
    comm_dongle_set_instance(payload + 0, ctx->dongle_instance);
    payload[16] = 0x00; payload[17] = 0x00;
    payload[18] = 0x01; payload[19] = 0x00;     // fw_ver = 0x00010000 LE
    payload[20] = 1;                            // bus_count
    payload[21] = 0;                            // bus_local_ids[0]
    return write_s2m_frame(ctx,
                           COMM_ADDR_DONGLE_SELF,
                           COMM_CMD_DONGLE_IDENT,
                           in_seq, 0,
                           payload, sizeof(payload));
}

static int manager_push_simple_ack(dongle_ctx_t *ctx,
                                   uint8_t       addr,
                                   comm_cmd_t    cmd,
                                   uint8_t       in_seq,
                                   uint8_t       payload_byte,
                                   int           has_payload)
{
    bus_msg_t out;
    memset(&out, 0, sizeof(out));
    out.dst_robot   = 0;
    out.cmd_lo      = (uint8_t)(cmd & 0xFF);
    out.cmd_hi      = (uint8_t)(cmd >> 8);
    out.seq         = in_seq;          // ack_seq for s2m
    out.ack_status  = 0;
    out.src_addr    = addr;
    if (has_payload) {
        out.payload_len = 1;
        out.payload[0]  = payload_byte;
    } else {
        out.payload_len = 0;
    }
    bus_result_t rc = bus_msgq_put(&ctx->ext_tx_q, &out);
    return (rc == BUS_OK) ? 0 : -1;
}

void dongle_manager_entry(void *arg)
{
    dongle_ctx_t *ctx = (dongle_ctx_t *)arg;

    while (!ctx->should_exit) {
        bus_msg_t in;
        bus_result_t rc = bus_msgq_get(&ctx->mgr_in_q, &in, 50);
        if (rc == BUS_ERR_TIMEOUT) continue;
        if (rc != BUS_OK) continue;
        if (ctx->should_exit) break;

        comm_cmd_t cmd = (comm_cmd_t)((uint16_t)in.cmd_lo
                                   | ((uint16_t)in.cmd_hi << 8));

        // ---- Dongle-self handshake (addr 0xFE) ----
        if (in.src_addr == COMM_ADDR_DONGLE_SELF) {
            if (cmd == COMM_CMD_DONGLE_HELLO) {
                (void)manager_emit_ident(ctx, in.seq);
            } else {
                (void)manager_push_simple_ack(ctx,
                    COMM_ADDR_DONGLE_SELF, COMM_CMD_NAK, in.seq,
                    COMM_NAK_REASON_UNKNOWN_CMD, 1);
            }
            continue;
        }

        // ---- Slave-bus traffic ----
        if (in.src_addr == ctx->slave_addr) {
            // Link-control space (cmd < 0x0100): handled inline by the
            // manager. PING is the only one we answer in L4a/L4b;
            // anything else gets a NAK.
            if (cmd < 0x0100u) {
                if (cmd == COMM_CMD_PING) {
                    (void)manager_push_simple_ack(ctx,
                        ctx->slave_addr, COMM_CMD_ACK_BARE, in.seq, 0, 0);
                } else {
                    (void)manager_push_simple_ack(ctx,
                        ctx->slave_addr, COMM_CMD_NAK, in.seq,
                        COMM_NAK_REASON_UNKNOWN_CMD, 1);
                }
                continue;
            }

            // Catalogue space (cmd ≥ 0x0100): route to a logical_robot
            // via internal_bus. addr → dst_robot mapping for L4b is
            // the trivial single-robot case: slave_addr → robots[0]
            // (drive_base).
            in.dst_robot = 0;

            // Two flavours of catalogue command:
            //  - "request-response" (GET_*): the target robot produces
            //    its own response (with seq=request.seq). Manager
            //    must NOT auto-ACK or libcomm's slot would close
            //    before the real response arrives.
            //  - "fire-and-forget" (PUSH_*, STOP/RESUME/ABORT,
            //    TELEMETRY_ON/OFF): manager auto-ACKs to confirm the
            //    command was queued (Q1 lock); robot reports
            //    completion via its own events later.
            int request_response = (cmd == 0x1031u);   // GET_TELEMETRY

            bus_result_t put_rc = bus_msgq_put(&ctx->int_bus_q, &in);
            if (put_rc == BUS_OK) {
                if (!request_response) {
                    (void)manager_push_simple_ack(ctx,
                        ctx->slave_addr, COMM_CMD_ACK_BARE, in.seq, 0, 0);
                }
            } else {
                // Fail-stop: queue full means the robot is backlogged.
                (void)manager_push_simple_ack(ctx,
                    ctx->slave_addr, COMM_CMD_NAK, in.seq,
                    COMM_NAK_REASON_NO_RESPONSE, 1);
            }
            continue;
        }

        // ---- Frames for other addresses ----
        // Multidrop: ours is one slave on a shared bus. The frame is
        // for someone else; drop it.
    }
}

// ============ internal_bus_thread ============
// MED priority. Pure fan-out worker. Consumes int_bus_q and posts to
// the matching robot's inbox via logical_robot_post (which copies the
// msg into the robot's inbox msgq).

void internal_bus_entry(void *arg)
{
    dongle_ctx_t *ctx = (dongle_ctx_t *)arg;

    while (!ctx->should_exit) {
        bus_msg_t msg;
        bus_result_t rc = bus_msgq_get(&ctx->int_bus_q, &msg, 50);
        if (rc == BUS_ERR_TIMEOUT) continue;
        if (rc != BUS_OK) continue;
        if (ctx->should_exit) break;

        if (msg.dst_robot >= LOGICAL_ROBOT_MAX) {
            if (dongle_trace_enabled()) {
                fprintf(stderr,
                        "[robot_sim] internal_bus dst_robot=%u out of range, drop\n",
                        msg.dst_robot);
            }
            continue;
        }
        logical_robot_t *r = ctx->robots[msg.dst_robot];
        if (!r) {
            if (dongle_trace_enabled()) {
                fprintf(stderr,
                        "[robot_sim] internal_bus dst_robot=%u unbound, drop\n",
                        msg.dst_robot);
            }
            continue;
        }
        bus_result_t put_rc = logical_robot_post(r, &msg);
        if (put_rc != BUS_OK && dongle_trace_enabled()) {
            fprintf(stderr,
                    "[robot_sim] internal_bus -> robot[%u] full, drop\n",
                    msg.dst_robot);
        }
    }
}
