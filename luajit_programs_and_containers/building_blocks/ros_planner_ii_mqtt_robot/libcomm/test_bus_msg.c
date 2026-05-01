// libcomm/test_bus_msg.c
// Slice L1 unit test — bus_msg_t + sentinels + msgq round trip.
// Foundation gate for the Linux logical_robot work (Track C → L2-L5).

#include "bus_msg.h"
#include "bus_kernel.h"

#include <stdint.h>
#include <stdio.h>
#include <string.h>

static int g_pass, g_fail;
#define CHECK(cond, msg) do {                                              \
    if (cond) { g_pass++; printf("  PASS  %s\n", msg); }                   \
    else      { g_fail++; printf("  FAIL  %s\n", msg); }                   \
} while (0)

static void test_size_invariants(void)
{
    CHECK(sizeof(bus_msg_t) == 40,                   "sizeof(bus_msg_t) == 40");
    CHECK(BUS_MSG_INLINE_PAYLOAD_MAX == 32,          "inline payload locked at 32");
    CHECK(BUS_MSG_DST_SENTINEL == 0xFFu,             "sentinel marker is 0xFF");
    CHECK(LOGICAL_ROBOT_MAX <= 0xFE,                 "robot index space leaves room for sentinel");
}

static void test_sentinel_helpers(void)
{
    bus_msg_t m;
    memset(&m, 0xAA, sizeof(m));    // pre-fill so we can see field clearing

    bus_msg_make_tick(&m);
    CHECK(bus_msg_is_sentinel(&m),                    "tick is sentinel");
    CHECK(bus_msg_sentinel_kind(&m) == BUS_MSG_SENTINEL_TICK,
                                                       "tick sentinel kind");
    CHECK(m.cmd_hi == 0 && m.seq == 0 && m.ack_status == 0 && m.src_addr == 0,
                                                       "tick clears non-payload fields");
    CHECK(m.payload_len == 0,                          "tick payload_len is 0");

    bus_msg_make_shutdown(&m);
    CHECK(bus_msg_is_sentinel(&m),                    "shutdown is sentinel");
    CHECK(bus_msg_sentinel_kind(&m) == BUS_MSG_SENTINEL_SHUTDOWN,
                                                       "shutdown sentinel kind");
}

static void test_real_msg_not_sentinel(void)
{
    bus_msg_t m = {0};
    m.dst_robot   = 3;            // some valid logical_robot index
    m.cmd_lo      = 0x55;
    m.cmd_hi      = 0x01;
    m.seq         = 7;
    m.payload_len = 4;
    m.payload[0]  = 0xDE;
    m.payload[1]  = 0xAD;
    m.payload[2]  = 0xBE;
    m.payload[3]  = 0xEF;

    CHECK(!bus_msg_is_sentinel(&m),                   "real msg is NOT sentinel");
    CHECK(bus_msg_sentinel_kind(&m) == 0xFFu,         "non-sentinel kind reads as 0xFF");
}

static void test_msgq_round_trip(void)
{
    bus_msgq_t q;
    bus_msg_t  backing[4];     // depth 4 inbox
    bus_result_t rc = bus_msgq_init(&q, backing, sizeof(bus_msg_t), 4);
    CHECK(rc == BUS_OK,                               "bus_msgq_init for bus_msg_t");

    // Post 3 distinct messages: one sentinel + two real, with payloads.
    bus_msg_t in[3] = {0};
    bus_msg_make_tick(&in[0]);

    in[1].dst_robot   = 1;
    in[1].cmd_lo      = 0x10;
    in[1].cmd_hi      = 0x00;
    in[1].seq         = 42;
    in[1].payload_len = 5;
    memcpy(in[1].payload, "hello", 5);

    in[2].dst_robot   = 2;
    in[2].cmd_lo      = 0x11;
    in[2].cmd_hi      = 0x00;
    in[2].seq         = 43;
    in[2].payload_len = 32;             // fully fill payload tail
    for (int i = 0; i < 32; i++) in[2].payload[i] = (uint8_t)(i ^ 0x5A);

    CHECK(bus_msgq_put(&q, &in[0]) == BUS_OK,         "put #0 (tick sentinel)");
    CHECK(bus_msgq_put(&q, &in[1]) == BUS_OK,         "put #1 (5-byte payload)");
    CHECK(bus_msgq_put(&q, &in[2]) == BUS_OK,         "put #2 (full 32-byte payload)");

    bus_msg_t out;

    CHECK(bus_msgq_get(&q, &out, 0) == BUS_OK,        "get #0");
    CHECK(bus_msg_is_sentinel(&out)
       && bus_msg_sentinel_kind(&out) == BUS_MSG_SENTINEL_TICK,
                                                       "get #0 round-trips as tick sentinel");

    CHECK(bus_msgq_get(&q, &out, 0) == BUS_OK,        "get #1");
    CHECK(out.dst_robot == 1 && out.seq == 42
       && out.payload_len == 5
       && memcmp(out.payload, "hello", 5) == 0,
                                                       "get #1 round-trips byte-for-byte");

    CHECK(bus_msgq_get(&q, &out, 0) == BUS_OK,        "get #2");
    int ok = (out.dst_robot == 2 && out.payload_len == 32);
    for (int i = 0; i < 32 && ok; i++) {
        if (out.payload[i] != (uint8_t)(i ^ 0x5A)) ok = 0;
    }
    CHECK(ok,                                          "get #2 round-trips full 32-byte payload");

    CHECK(bus_msgq_get(&q, &out, 0) == BUS_ERR_EMPTY, "get on drained queue → EMPTY");
}

int main(void)
{
    printf("[bus_msg slice L1]\n");
    test_size_invariants();
    test_sentinel_helpers();
    test_real_msg_not_sentinel();
    test_msgq_round_trip();
    printf("[summary] %d passed, %d failed\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
