// libcomm/test_dongle_catalogue.c
// Slice L4b — verifies the catalogue path inside a dongle:
//
//   external frame → mgr_in_q
//        → manager (translates, classifies cmd by space)
//        → int_bus_q (catalogue cmds only)
//        → internal_bus
//        → drive_base inbox
//        → drive_base.on_msg fires phys_push_line
//        → drive_base.tick produces DRV_EVT_SEG_DONE
//        → ext_tx_q
//
// Stays IN-PROCESS — does not spawn robot_sim or open a pty. The
// objective is to prove the routing topology end-to-end, including
// that link-control cmds (PING) bypass internal_bus while catalogue
// cmds (DRV_CMD_*) are routed through it. Wire-level encoding is
// already covered by the pty multi-dongle test.

#define _XOPEN_SOURCE 700
#define _DEFAULT_SOURCE
#define _POSIX_C_SOURCE 200809L

#include "bus_kernel.h"
#include "bus_msg.h"
#include "logical_robot.h"
#include "drive_base_robot.h"
#include "comm.h"

#include <stdint.h>
#include <stdio.h>
#include <string.h>

// ============ test harness reuses the dongle thread bodies ============
// Pulls in the same dongle_threads.c the production robot_sim uses,
// so when those bodies change this test exercises the new shape.

#include "../robot_sim/dongle_skeleton.h"

extern void ext_bus_entry      (void *arg);
extern void dongle_manager_entry(void *arg);
extern void internal_bus_entry (void *arg);

// We don't want ext_bus_thread for this test (no pty involvement).
// Instead: feed mgr_in_q directly with synthesised inbound bus_msg_t
// and read ext_tx_q to capture outbound responses.

static int g_pass, g_fail;
#define CHECK(cond, msg) do {                                              \
    if (cond) { g_pass++; printf("  PASS  %s\n", msg); }                   \
    else      { g_fail++; printf("  FAIL  %s\n", msg); }                   \
} while (0)

// ============ default drive_base tunables ============

static drive_base_tunables_t default_tunables(void)
{
    drive_base_tunables_t t = {0};
    t.schema_version       = DRV_TUNABLES_SCHEMA_VERSION;
    t.wheelbase_m          = 0.30f;
    t.wheel_radius_m       = 0.04f;
    t.mass_kg              = 8.0f;
    t.inertia_kg_m2        = 0.12f;
    t.lin_friction         = 0.8f;
    t.ang_friction         = 0.4f;
    t.max_torque_nm        = 1.5f;
    t.max_wheel_rad_s      = 30.0f;
    t.pid_kp               = 8.0f;
    t.pid_ki               = 4.0f;
    t.pid_kd               = 0.05f;
    t.energy_k             = 1.0f;
    t.lookahead_min_m      = 0.20f;
    t.lookahead_k_v        = 0.40f;
    t.arrival_tol_m        = 0.05f;
    t.heading_tol_rad      = 0.05f;
    t.max_linear_accel     = 1.0f;
    t.max_angular_accel    = 3.0f;
    t.cross_track_abort_m  = 20.0f;
    t.inner_dt_s           = 0.005f;
    t.battery_capacity_j   = 100000.0f;
    t.battery_initial_j    = 100000.0f;
    t.seed                 = 0xC0FFEE42ULL;
    return t;
}

// ============ helper: post a synthetic m2s bus_msg_t to mgr_in_q ============

static void post_to_manager(dongle_ctx_t *ctx,
                            uint8_t       src_addr,
                            uint16_t      cmd,
                            uint8_t       seq,
                            const uint8_t *payload,
                            uint8_t       payload_len)
{
    bus_msg_t in;
    memset(&in, 0, sizeof(in));
    in.dst_robot   = 0;
    in.cmd_lo      = (uint8_t)(cmd & 0xFF);
    in.cmd_hi      = (uint8_t)(cmd >> 8);
    in.seq         = seq;
    in.ack_status  = 0;
    in.src_addr    = src_addr;
    in.payload_len = payload_len;
    if (payload_len > 0) memcpy(in.payload, payload, payload_len);
    bus_msgq_put(&ctx->mgr_in_q, &in);
}

// ============ test bring-up (manager + internal_bus + drive_base) ============
// Skips ext_bus_thread; we don't have a real pty. Manager and
// internal_bus run normally; we feed mgr_in_q from the test.

static void start_dongle_no_extbus(dongle_ctx_t *ctx)
{
    memset(ctx, 0, sizeof(*ctx));
    ctx->master_fd       = -1;          // no pty — ext_bus not started
    ctx->dongle_type     = 1;           // drive_base
    ctx->dongle_instance = 1;
    ctx->slave_addr      = 1;
    ctx->should_exit     = 0;

    bus_mutex_init(&ctx->pty_write_mu);
    bus_msgq_init (&ctx->mgr_in_q,  ctx->mgr_in_buf,
                   (uint16_t)sizeof(bus_msg_t), DONGLE_QUEUE_DEPTH);
    bus_msgq_init (&ctx->int_bus_q, ctx->int_bus_buf,
                   (uint16_t)sizeof(bus_msg_t), DONGLE_QUEUE_DEPTH);
    bus_msgq_init (&ctx->ext_tx_q,  ctx->ext_tx_buf,
                   (uint16_t)sizeof(bus_msg_t), DONGLE_QUEUE_DEPTH);

    ctx->drive_base.tun               = default_tunables();
    ctx->drive_base.outbound          = &ctx->ext_tx_q;
    ctx->drive_base.tick_period_ms    = drive_base_vtable.tick_period_ms;
    ctx->drive_base.bus_addr          = ctx->slave_addr;
    ctx->drive_base.telemetry_enabled = 0;     // SEG_DONE only

    logical_robot_init(&ctx->drive_base_handle, "drvbase",
                       &drive_base_vtable, &ctx->drive_base,
                       ctx->drive_base_inbox_buf, DONGLE_QUEUE_DEPTH);
    ctx->robots[0] = &ctx->drive_base_handle;

    bus_thread_start(&ctx->manager_th,      "mgr",
                     BUS_PRIO_MED, dongle_manager_entry, ctx);
    bus_thread_start(&ctx->internal_bus_th, "intbus",
                     BUS_PRIO_MED, internal_bus_entry, ctx);
}

static void stop_dongle_no_extbus(dongle_ctx_t *ctx)
{
    ctx->should_exit = 1;
    bus_thread_join(&ctx->manager_th,      UINT32_MAX);
    bus_thread_join(&ctx->internal_bus_th, UINT32_MAX);
    logical_robot_shutdown(&ctx->drive_base_handle);
}

// ============ TESTS ============

static int wait_for_cmd_in_ext_tx(dongle_ctx_t *ctx, uint16_t want_cmd,
                                  bus_msg_t *out, uint32_t deadline_ms)
{
    while (bus_now_ms() < deadline_ms) {
        bus_msg_t msg;
        bus_result_t rc = bus_msgq_get(&ctx->ext_tx_q, &msg, 20);
        if (rc != BUS_OK) continue;
        uint16_t cmd = (uint16_t)msg.cmd_lo | ((uint16_t)msg.cmd_hi << 8);
        if (cmd == want_cmd) {
            if (out) *out = msg;
            return 1;
        }
    }
    return 0;
}

static void test_ping_link_control_inline(void)
{
    static dongle_ctx_t ctx;
    start_dongle_no_extbus(&ctx);

    // PING is link-control (cmd < 0x0100). Manager handles inline,
    // posts ACK_BARE to ext_tx_q. internal_bus must NOT see this cmd.
    post_to_manager(&ctx, ctx.slave_addr, COMM_CMD_PING, 42, NULL, 0);

    bus_msg_t reply;
    int got = wait_for_cmd_in_ext_tx(&ctx, COMM_CMD_ACK_BARE, &reply,
                                     bus_now_ms() + 200);
    CHECK(got, "PING produced ACK_BARE on ext_tx_q");
    if (got) {
        CHECK(reply.seq        == 42,                "ACK_BARE echoes seq");
        CHECK(reply.src_addr   == ctx.slave_addr,    "ACK_BARE src_addr is slave");
        CHECK(reply.payload_len == 0,                 "ACK_BARE has no payload");
    }

    // int_bus_q should be empty — manager did not route this.
    CHECK(bus_msgq_count(&ctx.int_bus_q) == 0,        "int_bus_q never received PING");

    stop_dongle_no_extbus(&ctx);
}

static void test_unknown_link_control_naks(void)
{
    static dongle_ctx_t ctx;
    start_dongle_no_extbus(&ctx);

    // Some bogus link-control cmd (still in 0x0000-0x00FF space).
    post_to_manager(&ctx, ctx.slave_addr, 0x0099u, 7, NULL, 0);

    bus_msg_t reply;
    int got = wait_for_cmd_in_ext_tx(&ctx, COMM_CMD_NAK, &reply,
                                     bus_now_ms() + 200);
    CHECK(got, "unknown link-ctrl cmd produced NAK");
    if (got) {
        CHECK(reply.seq         == 7,    "NAK echoes seq");
        CHECK(reply.payload_len == 1,    "NAK carries 1-byte reason");
        CHECK(reply.payload[0]  == COMM_NAK_REASON_UNKNOWN_CMD,
                                          "NAK reason is unknown_cmd");
    }
    stop_dongle_no_extbus(&ctx);
}

static void test_catalogue_routes_to_drive_base(void)
{
    static dongle_ctx_t ctx;
    start_dongle_no_extbus(&ctx);

    // Build DRV_CMD_PUSH_LINE payload via the helper.
    bus_msg_t cmd;
    drive_base_build_push_line(&cmd, /*dst*/0, /*seq*/3,
                               0.0f, 0.0f, 1.0f, 0.0f,
                               0.0f, 0.0f, 0.5f);
    // Simulate the wire-side translation: src_addr is set, dst_robot
    // unresolved (manager will resolve).
    cmd.src_addr = ctx.slave_addr;
    cmd.dst_robot = 0;
    bus_msgq_put(&ctx.mgr_in_q, &cmd);

    // Manager should immediately ACK_BARE this catalogue command.
    bus_msg_t ack;
    int got_ack = wait_for_cmd_in_ext_tx(&ctx, COMM_CMD_ACK_BARE, &ack,
                                         bus_now_ms() + 200);
    CHECK(got_ack,                              "PUSH_LINE got immediate ACK_BARE");
    if (got_ack) CHECK(ack.seq == 3,            "ACK_BARE echoes the inbound seq");

    // The line is 1 m at 0.5 m/s → ~2 s nominal. Wait up to 4 s for
    // SEG_DONE event flowing back through ext_tx_q.
    bus_msg_t done;
    int got_done = wait_for_cmd_in_ext_tx(&ctx, DRV_EVT_SEG_DONE, &done,
                                          bus_now_ms() + 4000);
    CHECK(got_done,                             "DRV_EVT_SEG_DONE arrived in ext_tx_q");
    if (got_done) {
        drive_base_seg_done_t info;
        int decoded = drive_base_decode_seg_done(&done, &info);
        CHECK(decoded == 0,                     "SEG_DONE decoded successfully");
        CHECK(done.src_addr == ctx.slave_addr,  "SEG_DONE src_addr is slave");
    }

    stop_dongle_no_extbus(&ctx);
}

int main(void)
{
    printf("[dongle_catalogue slice L4b]\n");
    test_ping_link_control_inline();
    test_unknown_link_control_naks();
    test_catalogue_routes_to_drive_base();
    printf("[summary] %d passed, %d failed\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
