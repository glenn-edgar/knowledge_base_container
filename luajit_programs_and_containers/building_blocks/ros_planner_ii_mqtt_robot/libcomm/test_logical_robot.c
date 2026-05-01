// libcomm/test_logical_robot.c
// Slice L2 unit test — generic logical_robot lifecycle.
// Verifies init, on_msg dispatch, periodic tick, cooperative shutdown,
// inbox-full backpressure, idempotent shutdown.
//
// Strategy: an "echo robot" whose vtable methods all just record what
// they saw into a test-controlled struct (with a mutex for safety).
// We then poke the API and assert the recorded state matches.

#include "logical_robot.h"
#include "bus_kernel.h"
#include "bus_msg.h"

#include <stdint.h>
#include <stdio.h>
#include <string.h>

static int g_pass, g_fail;
#define CHECK(cond, msg) do {                                              \
    if (cond) { g_pass++; printf("  PASS  %s\n", msg); }                   \
    else      { g_fail++; printf("  FAIL  %s\n", msg); }                   \
} while (0)

// ============ ECHO-ROBOT FIXTURE ============

#define MAX_RECORDED 32

typedef struct {
    bus_mutex_t mu;
    int         init_called;
    int         shutdown_called;
    int         on_msg_count;
    int         tick_count;
    bus_msg_t   recorded[MAX_RECORDED];   // last MAX_RECORDED on_msg messages
    int         recorded_n;
} echo_state_t;

static void echo_init(void *self)
{
    echo_state_t *e = (echo_state_t *)self;
    bus_mutex_lock(&e->mu, UINT32_MAX);
    e->init_called++;
    bus_mutex_unlock(&e->mu);
}

static void echo_on_msg(void *self, const bus_msg_t *m)
{
    echo_state_t *e = (echo_state_t *)self;
    bus_mutex_lock(&e->mu, UINT32_MAX);
    e->on_msg_count++;
    if (e->recorded_n < MAX_RECORDED) {
        e->recorded[e->recorded_n++] = *m;
    }
    bus_mutex_unlock(&e->mu);
}

static void echo_tick(void *self, uint32_t now_ms)
{
    (void)now_ms;
    echo_state_t *e = (echo_state_t *)self;
    bus_mutex_lock(&e->mu, UINT32_MAX);
    e->tick_count++;
    bus_mutex_unlock(&e->mu);
}

static void echo_shutdown(void *self)
{
    echo_state_t *e = (echo_state_t *)self;
    bus_mutex_lock(&e->mu, UINT32_MAX);
    e->shutdown_called++;
    bus_mutex_unlock(&e->mu);
}

static const logical_robot_vtable_t echo_vtable_with_tick = {
    .init           = echo_init,
    .on_msg         = echo_on_msg,
    .tick           = echo_tick,
    .shutdown       = echo_shutdown,
    .tick_period_ms = 20,    // 50 Hz
};

static const logical_robot_vtable_t echo_vtable_no_tick = {
    .init           = echo_init,
    .on_msg         = echo_on_msg,
    .tick           = NULL,
    .shutdown       = echo_shutdown,
    .tick_period_ms = 0,
};

// "Slow" robot: on_msg sleeps so the inbox fills under sustained
// post pressure. Used by test_inbox_full_returns_full.
static int slow_on_msg_count = 0;
static void slow_on_msg(void *self, const bus_msg_t *m)
{
    (void)self; (void)m;
    bus_thread_sleep_ms(20);
    slow_on_msg_count++;
}
static const logical_robot_vtable_t slow_vtable = {
    .init           = NULL,
    .on_msg         = slow_on_msg,
    .tick           = NULL,
    .shutdown       = NULL,
    .tick_period_ms = 0,
};

// ============ TESTS ============

static void test_basic_lifecycle(void)
{
    echo_state_t st = {0};
    bus_mutex_init(&st.mu);

    bus_msg_t inbox_buf[8];
    logical_robot_t r;
    bus_result_t rc = logical_robot_init(&r, "echo", &echo_vtable_no_tick,
                                         &st, inbox_buf, 8);
    CHECK(rc == BUS_OK, "logical_robot_init OK");

    // Allow the thread to start and run init.
    bus_thread_sleep_ms(20);

    bus_mutex_lock(&st.mu, UINT32_MAX);
    int init_seen = st.init_called;
    bus_mutex_unlock(&st.mu);
    CHECK(init_seen == 1, "init called exactly once");

    rc = logical_robot_shutdown(&r);
    CHECK(rc == BUS_OK, "logical_robot_shutdown OK");

    bus_mutex_lock(&st.mu, UINT32_MAX);
    int shut_seen = st.shutdown_called;
    bus_mutex_unlock(&st.mu);
    CHECK(shut_seen == 1, "shutdown called exactly once");

    // Idempotent — second shutdown is a no-op, doesn't crash.
    rc = logical_robot_shutdown(&r);
    CHECK(rc == BUS_OK, "second shutdown is no-op");
}

static void test_on_msg_dispatch(void)
{
    echo_state_t st = {0};
    bus_mutex_init(&st.mu);

    bus_msg_t inbox_buf[8];
    logical_robot_t r;
    logical_robot_init(&r, "echo", &echo_vtable_no_tick, &st, inbox_buf, 8);

    // Post 5 distinct messages.
    for (int i = 0; i < 5; i++) {
        bus_msg_t m = {0};
        m.dst_robot   = 0;
        m.cmd_lo      = (uint8_t)(0x10 + i);
        m.seq         = (uint8_t)i;
        m.payload_len = 1;
        m.payload[0]  = (uint8_t)(0xA0 + i);
        CHECK(logical_robot_post(&r, &m) == BUS_OK, "post message");
    }

    // Allow time for all to drain.
    bus_thread_sleep_ms(50);

    bus_mutex_lock(&st.mu, UINT32_MAX);
    int seen = st.on_msg_count;
    int recorded_ok = (st.recorded_n == 5);
    for (int i = 0; i < 5 && recorded_ok; i++) {
        if (st.recorded[i].cmd_lo != (uint8_t)(0x10 + i)
         || st.recorded[i].payload[0] != (uint8_t)(0xA0 + i)) recorded_ok = 0;
    }
    bus_mutex_unlock(&st.mu);

    CHECK(seen == 5,        "on_msg fired 5 times");
    CHECK(recorded_ok,      "messages received in order with correct payload");

    logical_robot_shutdown(&r);
}

static void test_post_rejects_sentinel(void)
{
    echo_state_t st = {0};
    bus_mutex_init(&st.mu);
    bus_msg_t inbox_buf[4];
    logical_robot_t r;
    logical_robot_init(&r, "echo", &echo_vtable_no_tick, &st, inbox_buf, 4);

    bus_msg_t fake_shutdown;
    bus_msg_make_shutdown(&fake_shutdown);
    bus_result_t rc = logical_robot_post(&r, &fake_shutdown);
    CHECK(rc == BUS_ERR_BAD_ARG, "post rejects sentinel-shaped msg");

    logical_robot_shutdown(&r);
    bus_mutex_lock(&st.mu, UINT32_MAX);
    int normal_shut = (st.shutdown_called == 1);
    bus_mutex_unlock(&st.mu);
    CHECK(normal_shut, "real shutdown still works after rejected fake");
}

static void test_tick_fires(void)
{
    echo_state_t st = {0};
    bus_mutex_init(&st.mu);
    bus_msg_t inbox_buf[8];
    logical_robot_t r;
    logical_robot_init(&r, "echotk", &echo_vtable_with_tick, &st, inbox_buf, 8);

    // 20 ms tick period; let it run for ~110 ms so we expect ~5 ticks.
    bus_thread_sleep_ms(110);

    bus_mutex_lock(&st.mu, UINT32_MAX);
    int ticks = st.tick_count;
    bus_mutex_unlock(&st.mu);

    // Allow some slack for scheduler timing on a busy host.
    CHECK(ticks >= 4 && ticks <= 8, "tick fired ~5 times in 110ms");

    logical_robot_shutdown(&r);
}

static void test_inbox_full_returns_full(void)
{
    echo_state_t st = {0};
    bus_mutex_init(&st.mu);
    bus_msg_t inbox_buf[2];   // depth 2 — small so we can fill it
    logical_robot_t r;

    slow_on_msg_count = 0;
    logical_robot_init(&r, "slow", &slow_vtable, NULL, inbox_buf, 2);

    bus_msg_t m = {0};
    m.dst_robot   = 0;
    m.payload_len = 0;

    // Fill the inbox: first one might be picked up by the thread
    // immediately, but the next 2 fill before slow_on_msg returns.
    int full_seen = 0;
    for (int i = 0; i < 5; i++) {
        bus_result_t rc = logical_robot_post(&r, &m);
        if (rc == BUS_ERR_FULL) { full_seen = 1; break; }
    }
    CHECK(full_seen, "post returns BUS_ERR_FULL when inbox saturates");

    // Let the slow thread drain so shutdown completes promptly.
    bus_thread_sleep_ms(150);
    logical_robot_shutdown(&r);
}

int main(void)
{
    printf("[logical_robot slice L2]\n");
    test_basic_lifecycle();
    test_on_msg_dispatch();
    test_post_rejects_sentinel();
    test_tick_fires();
    test_inbox_full_returns_full();
    printf("[summary] %d passed, %d failed\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
