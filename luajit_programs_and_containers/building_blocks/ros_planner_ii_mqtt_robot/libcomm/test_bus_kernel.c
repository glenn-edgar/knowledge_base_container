// libcomm/test_bus_kernel.c
// Standalone smoke test for bus_kernel.h. Exercises every primitive
// (thread, msgq, timer, mutex, clock) once. Same source compiles
// against bus_kernel_linux.c today and against bus_kernel_zephyr.c on
// hardware once that backend lands — it's the boundary's portable
// regression gate.
//
// Build (linux): see Makefile target `test_bus_kernel`.
// Run: ./test_bus_kernel ; echo "exit=$?"

#include "bus_kernel.h"

#include <stdint.h>
#include <stdio.h>
#include <string.h>

static int g_pass, g_fail;
#define CHECK(cond, msg) do {                                              \
    if (cond) { g_pass++; printf("  PASS  %s\n", msg); }                   \
    else      { g_fail++; printf("  FAIL  %s\n", msg); }                   \
} while (0)

// ---------- thread test ----------

typedef struct { int counter; bus_msgq_t *q; } worker_ctx_t;

static void worker_entry(void *arg)
{
    worker_ctx_t *c = (worker_ctx_t *)arg;
    for (int i = 0; i < 5; i++) {
        c->counter++;
        uint32_t msg = (uint32_t)i;
        bus_msgq_put(c->q, &msg);
    }
}

static void test_thread_and_msgq(void)
{
    bus_msgq_t q;
    uint32_t   buf[8];
    bus_result_t rc = bus_msgq_init(&q, buf, sizeof(uint32_t), 8);
    CHECK(rc == BUS_OK, "msgq_init ok");

    worker_ctx_t ctx = { 0, &q };
    bus_thread_t th;
    rc = bus_thread_start(&th, "worker", BUS_PRIO_MED, worker_entry, &ctx);
    CHECK(rc == BUS_OK, "thread_start ok");

    // Drain 5 messages with timeout.
    int got = 0;
    for (int i = 0; i < 5; i++) {
        uint32_t msg = 0xFFFFFFFFu;
        if (bus_msgq_get(&q, &msg, 1000) == BUS_OK && (int)msg == i) got++;
    }
    CHECK(got == 5, "msgq drained 5 in order");

    rc = bus_thread_join(&th, 1000);
    CHECK(rc == BUS_OK, "thread_join ok");
    CHECK(ctx.counter == 5, "worker ran 5 iterations");
}

// ---------- msgq edge cases ----------

static void test_msgq_full_empty(void)
{
    bus_msgq_t q;
    uint8_t    buf[3];   // depth 3, msg_size 1
    CHECK(bus_msgq_init(&q, buf, 1, 3) == BUS_OK, "small msgq init");

    uint8_t v;
    v = 1; CHECK(bus_msgq_put(&q, &v) == BUS_OK,       "put 1");
    v = 2; CHECK(bus_msgq_put(&q, &v) == BUS_OK,       "put 2");
    v = 3; CHECK(bus_msgq_put(&q, &v) == BUS_OK,       "put 3");
    v = 4; CHECK(bus_msgq_put(&q, &v) == BUS_ERR_FULL, "put on full → FULL");

    CHECK(bus_msgq_count(&q) == 3, "count == 3");

    CHECK(bus_msgq_get(&q, &v, 0) == BUS_OK && v == 1, "get 1 (FIFO)");
    CHECK(bus_msgq_get(&q, &v, 0) == BUS_OK && v == 2, "get 2");
    CHECK(bus_msgq_get(&q, &v, 0) == BUS_OK && v == 3, "get 3");
    CHECK(bus_msgq_get(&q, &v, 0) == BUS_ERR_EMPTY,    "get poll on empty → EMPTY");

    // Timeout path — should return TIMEOUT, not EMPTY.
    uint32_t t0 = bus_now_ms();
    bus_result_t rc = bus_msgq_get(&q, &v, 50);
    uint32_t dt = bus_now_ms() - t0;
    CHECK(rc == BUS_ERR_TIMEOUT, "get with 50ms timeout → TIMEOUT");
    CHECK(dt >= 40 && dt < 200,  "timeout fired in ~50ms");
}

// ---------- timer test ----------

static volatile int g_tick_count = 0;
static void timer_cb(void *arg) { (void)arg; g_tick_count++; }

static void test_timer(void)
{
    g_tick_count = 0;
    bus_timer_t tm;
    CHECK(bus_timer_init(&tm, timer_cb, NULL) == BUS_OK, "timer_init ok");
    CHECK(bus_timer_start(&tm, 20, 20) == BUS_OK,        "timer_start 20ms ok");

    bus_thread_sleep_ms(110);   // expect ~5 ticks
    bus_timer_stop(&tm);

    CHECK(g_tick_count >= 4 && g_tick_count <= 7,
          "timer fired ~5 times in 110ms");
}

// ---------- mutex test ----------

static void test_mutex(void)
{
    bus_mutex_t m;
    CHECK(bus_mutex_init(&m) == BUS_OK,           "mutex_init ok");
    CHECK(bus_mutex_lock(&m, 0) == BUS_OK,        "trylock unlocked → OK");
    CHECK(bus_mutex_lock(&m, 0) == BUS_ERR_TIMEOUT, "trylock locked → TIMEOUT");

    uint32_t t0 = bus_now_ms();
    CHECK(bus_mutex_lock(&m, 30) == BUS_ERR_TIMEOUT, "timed-lock locked → TIMEOUT");
    uint32_t dt = bus_now_ms() - t0;
    CHECK(dt >= 20 && dt < 200, "timed-lock waited ~30ms");

    CHECK(bus_mutex_unlock(&m) == BUS_OK,         "unlock ok");
    CHECK(bus_mutex_lock(&m, 0) == BUS_OK,        "trylock after unlock ok");
    CHECK(bus_mutex_unlock(&m) == BUS_OK,         "unlock #2 ok");
}

// ---------- clock test ----------

static void test_clock(void)
{
    uint32_t t0 = bus_now_ms();
    bus_thread_sleep_ms(50);
    uint32_t dt = bus_now_ms() - t0;
    CHECK(dt >= 40 && dt < 200, "now_ms + sleep_ms agree to ~50ms");
}

int main(void)
{
    printf("[bus_kernel smoke]\n");
    test_thread_and_msgq();
    test_msgq_full_empty();
    test_timer();
    test_mutex();
    test_clock();
    printf("[summary] %d passed, %d failed\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
