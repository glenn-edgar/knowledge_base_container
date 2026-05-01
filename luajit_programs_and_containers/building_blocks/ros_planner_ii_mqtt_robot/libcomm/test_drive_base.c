// libcomm/test_drive_base.c
// Slice L3 unit test — drive_base logical_robot end-to-end:
//   bus_msg_t (DRV_CMD_*) → drive_base on_msg → libphysics → tick →
//   bus_msg_t (DRV_EVT_*) on a test outbox. Same path the real
//   dongle_manager will use, just with the test playing the manager.
//
// Loads tunables from physics_config.json's defaults so the simulated
// rover behaves like the existing rover_1 in MQTT scenarios.

#include "drive_base_robot.h"
#include "logical_robot.h"
#include "bus_kernel.h"
#include "bus_msg.h"

#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

static int g_pass, g_fail;
#define CHECK(cond, msg) do {                                              \
    if (cond) { g_pass++; printf("  PASS  %s\n", msg); }                   \
    else      { g_fail++; printf("  FAIL  %s\n", msg); }                   \
} while (0)

// ============ TUNABLES — match physics_config.json defaults ============

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

    t.gps_sigma_xy         = 0.0f;
    t.gps_sigma_h          = 0.0f;
    t.imu_sigma            = 0.0f;
    t.battery_noise_pct    = 0.0f;

    t.battery_capacity_j   = 100000.0f;
    t.battery_initial_j    = 100000.0f;

    t.init_x = 0.0f;  t.init_y = 0.0f;  t.init_heading = 0.0f;
    t.seed   = 0xC0FFEE42ULL;
    return t;
}

// ============ helpers ============

static int find_telemetry(bus_msgq_t *outbox, drive_base_telemetry_t *out,
                          uint32_t deadline_ms)
{
    while (bus_now_ms() < deadline_ms) {
        bus_msg_t evt;
        if (bus_msgq_get(outbox, &evt, 10) == BUS_OK) {
            if (drive_base_decode_telemetry(&evt, out) == 0) return 1;
        }
    }
    return 0;
}

// ============ TESTS ============

static void test_init_and_telemetry(void)
{
    bus_msg_t   inbox_buf[16];
    bus_msg_t   outbox_buf[64];
    bus_msgq_t  outbox;
    bus_msgq_init(&outbox, outbox_buf, sizeof(bus_msg_t), 64);

    drive_base_t db = {0};
    db.tun            = default_tunables();
    db.outbound       = &outbox;
    db.tick_period_ms = drive_base_vtable.tick_period_ms;

    logical_robot_t r;
    bus_result_t rc = logical_robot_init(&r, "drvbase", &drive_base_vtable,
                                         &db, inbox_buf, 16);
    CHECK(rc == BUS_OK, "logical_robot_init OK");

    // Wait for init + a few ticks so telemetry events appear in outbox.
    bus_thread_sleep_ms(60);
    CHECK(db.started == 1,    "phys_create + tunables applied");
    CHECK(db.phys != NULL,    "phys_t allocated");

    drive_base_telemetry_t tel;
    int got = find_telemetry(&outbox, &tel, bus_now_ms() + 50);
    CHECK(got, "telemetry event arrived in outbox");
    if (got) {
        CHECK(fabsf(tel.x) < 0.01f && fabsf(tel.y) < 0.01f,
              "telemetry shows initial pose at origin");
    }

    logical_robot_shutdown(&r);
    CHECK(db.phys == NULL,   "phys_destroy ran on shutdown");
}

static void test_push_line_advances_pose(void)
{
    bus_msg_t   inbox_buf[16];
    bus_msg_t   outbox_buf[256];
    bus_msgq_t  outbox;
    bus_msgq_init(&outbox, outbox_buf, sizeof(bus_msg_t), 256);

    drive_base_t db = {0};
    db.tun            = default_tunables();
    db.outbound       = &outbox;
    db.tick_period_ms = drive_base_vtable.tick_period_ms;

    logical_robot_t r;
    logical_robot_init(&r, "drvbase", &drive_base_vtable, &db, inbox_buf, 16);

    // Allow init to settle.
    bus_thread_sleep_ms(30);

    // Send a 1.0 m line command at 0.5 m/s.
    bus_msg_t cmd;
    drive_base_build_push_line(&cmd, /*dst*/0, /*seq*/1,
                               0.0f, 0.0f, 1.0f, 0.0f,
                               0.0f, 0.0f, 0.5f);
    bus_result_t rc = logical_robot_post(&r, &cmd);
    CHECK(rc == BUS_OK, "PUSH_LINE bus_msg accepted by inbox");

    // Run for ~3 seconds (1 m / 0.5 m/s = 2 s nominal + accel margin).
    // Drain the outbox periodically so it doesn't fill up.
    drive_base_seg_done_t done = {0};
    int seg_done_seen = 0;
    drive_base_telemetry_t last_tel = {0};
    uint32_t deadline = bus_now_ms() + 4000;
    while (bus_now_ms() < deadline && !seg_done_seen) {
        bus_msg_t evt;
        bus_result_t grc = bus_msgq_get(&outbox, &evt, 50);
        if (grc != BUS_OK) continue;

        drive_base_telemetry_t tel;
        if (drive_base_decode_telemetry(&evt, &tel) == 0) {
            last_tel = tel;
            continue;
        }
        if (drive_base_decode_seg_done(&evt, &done) == 0) {
            seg_done_seen = 1;
        }
    }

    CHECK(seg_done_seen,                       "SEG_DONE event delivered");
    CHECK(last_tel.x > 0.5f,                   "rover advanced past x=0.5 m");
    CHECK(fabsf(last_tel.y) < 0.10f,           "rover stayed near y axis");

    logical_robot_shutdown(&r);
}

static void test_stop_freezes_pose(void)
{
    bus_msg_t   inbox_buf[16];
    bus_msg_t   outbox_buf[256];
    bus_msgq_t  outbox;
    bus_msgq_init(&outbox, outbox_buf, sizeof(bus_msg_t), 256);

    drive_base_t db = {0};
    db.tun            = default_tunables();
    db.outbound       = &outbox;
    db.tick_period_ms = drive_base_vtable.tick_period_ms;

    logical_robot_t r;
    logical_robot_init(&r, "drvbase", &drive_base_vtable, &db, inbox_buf, 16);
    bus_thread_sleep_ms(30);

    bus_msg_t cmd;
    drive_base_build_push_line(&cmd, 0, 1,
                               0.0f, 0.0f, 5.0f, 0.0f,
                               0.0f, 0.0f, 0.5f);
    logical_robot_post(&r, &cmd);

    // Run 0.5 s, then issue STOP.
    bus_thread_sleep_ms(500);
    drive_base_build_simple(&cmd, 0, 2, DRV_CMD_STOP);
    logical_robot_post(&r, &cmd);

    // Drain a bit so stop takes effect, then capture pose.
    bus_thread_sleep_ms(300);
    // Drain queued events so we get a fresh telemetry sample.
    while (bus_msgq_count(&outbox) > 0) {
        bus_msg_t evt;
        bus_msgq_get(&outbox, &evt, 0);
    }
    drive_base_telemetry_t before, after;
    int g1 = find_telemetry(&outbox, &before, bus_now_ms() + 50);
    bus_thread_sleep_ms(300);
    while (bus_msgq_count(&outbox) > 0) {
        bus_msg_t evt;
        bus_msgq_get(&outbox, &evt, 0);
    }
    int g2 = find_telemetry(&outbox, &after, bus_now_ms() + 50);

    CHECK(g1 && g2, "captured before/after telemetry around STOP");
    if (g1 && g2) {
        // After STOP and a couple hundred ms, pose should be ~stable.
        // Allow up to 5 cm drift (PID windup + inertia).
        CHECK(fabsf(after.x - before.x) < 0.05f,
              "pose stable after STOP");
        CHECK(fabsf(after.v) < 0.05f,
              "velocity decayed to ~0 after STOP");
    }

    logical_robot_shutdown(&r);
}

static void test_unknown_cmd_is_noop(void)
{
    bus_msg_t   inbox_buf[8];
    bus_msg_t   outbox_buf[64];
    bus_msgq_t  outbox;
    bus_msgq_init(&outbox, outbox_buf, sizeof(bus_msg_t), 64);

    drive_base_t db = {0};
    db.tun            = default_tunables();
    db.outbound       = &outbox;
    db.tick_period_ms = drive_base_vtable.tick_period_ms;

    logical_robot_t r;
    logical_robot_init(&r, "drvbase", &drive_base_vtable, &db, inbox_buf, 8);
    bus_thread_sleep_ms(30);

    // Send a bus_msg with a junk command code.
    bus_msg_t cmd = {0};
    cmd.dst_robot   = 0;
    cmd.cmd_lo      = 0xAB;
    cmd.cmd_hi      = 0xCD;
    cmd.seq         = 99;
    cmd.payload_len = 0;
    bus_result_t rc = logical_robot_post(&r, &cmd);
    CHECK(rc == BUS_OK, "unknown cmd accepted by inbox");

    // Run a moment; verify pose still at origin.
    bus_thread_sleep_ms(100);
    drive_base_telemetry_t tel;
    int got = 0;
    while (bus_msgq_count(&outbox) > 0) {
        bus_msg_t evt;
        bus_msgq_get(&outbox, &evt, 0);
        if (drive_base_decode_telemetry(&evt, &tel) == 0) got = 1;
    }
    CHECK(got && fabsf(tel.x) < 0.01f && fabsf(tel.y) < 0.01f,
          "unknown cmd was a no-op (pose still ~origin)");

    logical_robot_shutdown(&r);
}

int main(void)
{
    printf("[drive_base slice L3]\n");
    test_init_and_telemetry();
    test_push_line_advances_pose();
    test_stop_freezes_pose();
    test_unknown_cmd_is_noop();
    printf("[summary] %d passed, %d failed\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
