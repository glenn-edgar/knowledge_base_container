/**
 * @file test_nats_pubsub.c
 * @brief Test driver for nats_pubsub library.
 *
 * Requires a running NATS server at 127.0.0.1:4222:
 *   docker run -p 4222:4222 nats:latest
 *
 * Usage:
 *   ./test_nats_pubsub              # run all tests
 *   ./test_nats_pubsub tests        # tests only
 *   ./test_nats_pubsub demo         # interactive demo
 */

#define _GNU_SOURCE

#include <inttypes.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>

#include "nats_pubsub.h"

/* ------------------------------------------------------------------ */
/*  Minimal test framework                                             */
/* ------------------------------------------------------------------ */

static int tests_run    = 0;
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST_SERVER "nats://127.0.0.1:4222"

#define RUN_TEST(fn)                                               \
    do {                                                           \
        tests_run++;                                               \
        printf("  %-50s ", #fn);                                   \
        fflush(stdout);                                            \
        if (fn()) {                                                \
            tests_passed++;                                        \
            printf("[PASS]\n");                                    \
        } else {                                                   \
            tests_failed++;                                        \
            printf("[FAIL]\n");                                    \
        }                                                          \
    } while (0)

#define EXPECT(cond)                                               \
    do {                                                           \
        if (!(cond)) {                                             \
            fprintf(stderr, "    FAIL at %s:%d: %s\n",             \
                    __FILE__, __LINE__, #cond);                    \
            return false;                                          \
        }                                                          \
    } while (0)

#define EXPECT_OK(st) EXPECT((st) == PS_OK)

/* ------------------------------------------------------------------ */
/*  Callback helpers                                                   */
/* ------------------------------------------------------------------ */

typedef struct {
    char            last_data[1024];
    int             last_len;
    char            last_subject[256];
    char            last_original[256];
    char            last_reply[256];
    int             count;
    pthread_mutex_t mu;
    pthread_cond_t  cond;
} CallbackState;

static void cb_state_init(CallbackState *s)
{
    memset(s, 0, sizeof(*s));
    pthread_mutex_init(&s->mu, NULL);
    pthread_cond_init(&s->cond, NULL);
}

static void cb_state_destroy(CallbackState *s)
{
    pthread_mutex_destroy(&s->mu);
    pthread_cond_destroy(&s->cond);
}

static void cb_state_wait(CallbackState *s, int expected, int timeout_ms)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_nsec += (long)timeout_ms * 1000000L;
    ts.tv_sec  += ts.tv_nsec / 1000000000L;
    ts.tv_nsec %= 1000000000L;

    pthread_mutex_lock(&s->mu);
    while (s->count < expected)
        if (pthread_cond_timedwait(&s->cond, &s->mu, &ts) != 0) break;
    pthread_mutex_unlock(&s->mu);
}

static void generic_callback(const PubSubMsg *msg, void *user_data)
{
    CallbackState *s = user_data;
    pthread_mutex_lock(&s->mu);

    if (msg->data && msg->data_len > 0 &&
        msg->data_len < (int)sizeof(s->last_data)) {
        memcpy(s->last_data, msg->data, (size_t)msg->data_len);
        s->last_data[msg->data_len] = '\0';
    }
    s->last_len = msg->data_len;

    if (msg->subject)
        snprintf(s->last_subject, sizeof(s->last_subject), "%s", msg->subject);
    if (msg->original_subject)
        snprintf(s->last_original, sizeof(s->last_original), "%s", msg->original_subject);
    if (msg->reply_to)
        snprintf(s->last_reply, sizeof(s->last_reply), "%s", msg->reply_to);
    else
        s->last_reply[0] = '\0';

    s->count++;
    pthread_cond_signal(&s->cond);
    pthread_mutex_unlock(&s->mu);
}

/* Responder callback for request/reply tests */
static void echo_responder(const PubSubMsg *msg, void *user_data)
{
    PubSub *ps = user_data;
    if (!msg->reply_to) return;

    char reply[256];
    snprintf(reply, sizeof(reply), "echo: %.*s", msg->data_len, msg->data);
    pubsub_reply_str(ps, msg->reply_to, reply);
}

/* Helper */
static PubSub *make_ps(const char *ns, const char *name)
{
    PubSubConfig cfg;
    pubsub_config_defaults(&cfg);
    cfg.server      = TEST_SERVER;
    cfg.namespace_  = ns;
    cfg.client_name = name;
    PubSub *ps = NULL;
    pubsub_create(&ps, &cfg);
    return ps;
}

/* ================================================================== */
/*  Tests                                                              */
/* ================================================================== */

static bool test_status_strings(void)
{
    EXPECT(strcmp(ps_status_str(PS_OK), "ok") == 0);
    EXPECT(strcmp(ps_status_str(PS_ERR_TIMEOUT), "timeout") == 0);
    EXPECT(strcmp(ps_status_str(PS_ERR_NOT_CONNECTED), "not_connected") == 0);
    EXPECT(strcmp(ps_status_str(PS_ERR_NATS), "nats_error") == 0);
    return true;
}

static bool test_config_defaults(void)
{
    PubSubConfig cfg;
    pubsub_config_defaults(&cfg);
    EXPECT(cfg.server != NULL);
    EXPECT(strcmp(cfg.namespace_, "default") == 0);
    EXPECT(cfg.client_name == NULL);
    return true;
}

static bool test_connect_disconnect(void)
{
    PubSub *ps = make_ps("test_conn", "conn_test");
    EXPECT(ps != NULL);
    EXPECT(!pubsub_is_connected(ps));

    EXPECT_OK(pubsub_connect(ps));
    EXPECT(pubsub_is_connected(ps));
    EXPECT(strcmp(pubsub_namespace(ps), "test_conn") == 0);
    EXPECT(strcmp(pubsub_client_name(ps), "conn_test") == 0);

    EXPECT_OK(pubsub_disconnect(ps));
    EXPECT(!pubsub_is_connected(ps));

    pubsub_destroy(ps);
    return true;
}

static bool test_basic_pubsub(void)
{
    PubSub *ps = make_ps("test_basic", "basic_test");
    EXPECT_OK(pubsub_connect(ps));

    CallbackState state;
    cb_state_init(&state);

    PubSubSub *sub = NULL;
    EXPECT_OK(pubsub_subscribe(ps, "hello", generic_callback, &state,
                               NULL, &sub));

    EXPECT_OK(pubsub_publish_str(ps, "hello", "world"));
    cb_state_wait(&state, 1, 1000);

    EXPECT(state.count == 1);
    EXPECT(strcmp(state.last_data, "world") == 0);
    EXPECT(strcmp(state.last_original, "hello") == 0);
    EXPECT(strstr(state.last_subject, "test_basic.hello") != NULL);

    pubsub_unsubscribe(ps, sub);
    cb_state_destroy(&state);
    pubsub_destroy(ps);
    return true;
}

static bool test_binary_payload(void)
{
    PubSub *ps = make_ps("test_bin", "bin_test");
    EXPECT_OK(pubsub_connect(ps));

    CallbackState state;
    cb_state_init(&state);

    PubSubSub *sub = NULL;
    EXPECT_OK(pubsub_subscribe(ps, "binary", generic_callback, &state,
                               NULL, &sub));

    const unsigned char payload[] = {0x00, 0x01, 0x02, 0xFF, 0xFE};
    EXPECT_OK(pubsub_publish(ps, "binary", payload, sizeof(payload)));
    cb_state_wait(&state, 1, 1000);

    EXPECT(state.count == 1);
    EXPECT(state.last_len == (int)sizeof(payload));
    EXPECT(memcmp(state.last_data, payload, sizeof(payload)) == 0);

    pubsub_unsubscribe(ps, sub);
    cb_state_destroy(&state);
    pubsub_destroy(ps);
    return true;
}

static bool test_multiple_messages(void)
{
    PubSub *ps = make_ps("test_mm", "mm_test");
    EXPECT_OK(pubsub_connect(ps));

    CallbackState state;
    cb_state_init(&state);

    PubSubSub *sub = NULL;
    EXPECT_OK(pubsub_subscribe(ps, "counter", generic_callback, &state,
                               NULL, &sub));

    for (int i = 0; i < 50; i++) {
        char buf[32];
        snprintf(buf, sizeof(buf), "msg_%d", i);
        EXPECT_OK(pubsub_publish_str(ps, "counter", buf));
    }

    cb_state_wait(&state, 50, 2000);
    EXPECT(state.count == 50);

    pubsub_unsubscribe(ps, sub);
    cb_state_destroy(&state);
    pubsub_destroy(ps);
    return true;
}

static bool test_multiple_subscribers(void)
{
    PubSub *ps = make_ps("test_msub", "msub_test");
    EXPECT_OK(pubsub_connect(ps));

    CallbackState s1, s2;
    cb_state_init(&s1);
    cb_state_init(&s2);

    PubSubSub *sub1 = NULL, *sub2 = NULL;
    EXPECT_OK(pubsub_subscribe(ps, "topic", generic_callback, &s1,
                               NULL, &sub1));
    EXPECT_OK(pubsub_subscribe(ps, "topic", generic_callback, &s2,
                               NULL, &sub2));

    EXPECT_OK(pubsub_publish_str(ps, "topic", "broadcast"));
    cb_state_wait(&s1, 1, 1000);
    cb_state_wait(&s2, 1, 1000);

    EXPECT(s1.count == 1);
    EXPECT(s2.count == 1);
    EXPECT(strcmp(s1.last_data, "broadcast") == 0);
    EXPECT(strcmp(s2.last_data, "broadcast") == 0);

    pubsub_unsubscribe(ps, sub1);
    pubsub_unsubscribe(ps, sub2);
    cb_state_destroy(&s1);
    cb_state_destroy(&s2);
    pubsub_destroy(ps);
    return true;
}

static bool test_wildcard_star(void)
{
    PubSub *ps = make_ps("test_wc", "wc_test");
    EXPECT_OK(pubsub_connect(ps));

    CallbackState state;
    cb_state_init(&state);

    PubSubSub *sub = NULL;
    EXPECT_OK(pubsub_subscribe(ps, "sensor.*", generic_callback, &state,
                               NULL, &sub));

    EXPECT_OK(pubsub_publish_str(ps, "sensor.temp", "23.5"));
    cb_state_wait(&state, 1, 1000);
    EXPECT(state.count == 1);
    EXPECT(strcmp(state.last_data, "23.5") == 0);

    EXPECT_OK(pubsub_publish_str(ps, "sensor.humidity", "65"));
    cb_state_wait(&state, 2, 1000);
    EXPECT(state.count == 2);

    /* Two levels deep should NOT match * */
    EXPECT_OK(pubsub_publish_str(ps, "sensor.temp.celsius", "23.5"));
    usleep(200000);
    EXPECT(state.count == 2);

    pubsub_unsubscribe(ps, sub);
    cb_state_destroy(&state);
    pubsub_destroy(ps);
    return true;
}

static bool test_wildcard_gt(void)
{
    PubSub *ps = make_ps("test_gt", "gt_test");
    EXPECT_OK(pubsub_connect(ps));

    CallbackState state;
    cb_state_init(&state);

    PubSubSub *sub = NULL;
    EXPECT_OK(pubsub_subscribe(ps, "sensor.>", generic_callback, &state,
                               NULL, &sub));

    EXPECT_OK(pubsub_publish_str(ps, "sensor.temp", "23.5"));
    cb_state_wait(&state, 1, 1000);
    EXPECT(state.count == 1);

    EXPECT_OK(pubsub_publish_str(ps, "sensor.temp.celsius", "23.5"));
    cb_state_wait(&state, 2, 1000);
    EXPECT(state.count == 2);

    EXPECT_OK(pubsub_publish_str(ps, "sensor.a.b.c.d", "deep"));
    cb_state_wait(&state, 3, 1000);
    EXPECT(state.count == 3);

    pubsub_unsubscribe(ps, sub);
    cb_state_destroy(&state);
    pubsub_destroy(ps);
    return true;
}

static bool test_queue_group(void)
{
    PubSub *ps = make_ps("test_q", "q_test");
    EXPECT_OK(pubsub_connect(ps));

    CallbackState s1, s2;
    cb_state_init(&s1);
    cb_state_init(&s2);

    PubSubSub *sub1 = NULL, *sub2 = NULL;
    EXPECT_OK(pubsub_subscribe(ps, "work", generic_callback, &s1,
                               "workers", &sub1));
    EXPECT_OK(pubsub_subscribe(ps, "work", generic_callback, &s2,
                               "workers", &sub2));

    for (int i = 0; i < 10; i++) {
        char buf[32];
        snprintf(buf, sizeof(buf), "msg_%d", i);
        EXPECT_OK(pubsub_publish_str(ps, "work", buf));
    }

    usleep(500000);

    int total = s1.count + s2.count;
    EXPECT(total == 10);

    pubsub_unsubscribe(ps, sub1);
    pubsub_unsubscribe(ps, sub2);
    cb_state_destroy(&s1);
    cb_state_destroy(&s2);
    pubsub_destroy(ps);
    return true;
}

static bool test_unsubscribe(void)
{
    PubSub *ps = make_ps("test_unsub", "unsub_test");
    EXPECT_OK(pubsub_connect(ps));

    CallbackState state;
    cb_state_init(&state);

    PubSubSub *sub = NULL;
    EXPECT_OK(pubsub_subscribe(ps, "topic", generic_callback, &state,
                               NULL, &sub));

    EXPECT_OK(pubsub_publish_str(ps, "topic", "before"));
    cb_state_wait(&state, 1, 1000);
    EXPECT(state.count == 1);

    EXPECT_OK(pubsub_unsubscribe(ps, sub));
    usleep(100000);

    EXPECT_OK(pubsub_publish_str(ps, "topic", "after"));
    usleep(300000);
    EXPECT(state.count == 1);  /* should NOT receive after unsub */

    cb_state_destroy(&state);
    pubsub_destroy(ps);
    return true;
}

static bool test_namespace_isolation(void)
{
    PubSub *ps1 = make_ps("ns_one", "ns1_test");
    PubSub *ps2 = make_ps("ns_two", "ns2_test");
    EXPECT_OK(pubsub_connect(ps1));
    EXPECT_OK(pubsub_connect(ps2));

    CallbackState s1, s2;
    cb_state_init(&s1);
    cb_state_init(&s2);

    PubSubSub *sub1 = NULL, *sub2 = NULL;
    EXPECT_OK(pubsub_subscribe(ps1, "topic", generic_callback, &s1,
                               NULL, &sub1));
    EXPECT_OK(pubsub_subscribe(ps2, "topic", generic_callback, &s2,
                               NULL, &sub2));

    /* Publish on ns_one — only s1 should receive */
    EXPECT_OK(pubsub_publish_str(ps1, "topic", "from_ns1"));
    usleep(300000);
    EXPECT(s1.count == 1);
    EXPECT(s2.count == 0);

    /* Publish on ns_two — only s2 should receive */
    EXPECT_OK(pubsub_publish_str(ps2, "topic", "from_ns2"));
    usleep(300000);
    EXPECT(s1.count == 1);
    EXPECT(s2.count == 1);

    pubsub_unsubscribe(ps1, sub1);
    pubsub_unsubscribe(ps2, sub2);
    cb_state_destroy(&s1);
    cb_state_destroy(&s2);
    pubsub_destroy(ps1);
    pubsub_destroy(ps2);
    return true;
}

static bool test_request_reply(void)
{
    PubSub *ps = make_ps("test_rr", "rr_test");
    EXPECT_OK(pubsub_connect(ps));

    /* Set up echo responder */
    PubSubSub *sub = NULL;
    EXPECT_OK(pubsub_subscribe(ps, "service.echo", echo_responder, ps,
                               NULL, &sub));

    char *reply = NULL;
    int reply_len = 0;
    EXPECT_OK(pubsub_request(ps, "service.echo", "ping", 4, 5.0,
                             &reply, &reply_len));
    EXPECT(reply != NULL);
    EXPECT(reply_len > 0);
    EXPECT(strstr(reply, "ping") != NULL);

    free(reply);
    pubsub_unsubscribe(ps, sub);
    pubsub_destroy(ps);
    return true;
}

static bool test_request_str(void)
{
    PubSub *ps = make_ps("test_rrs", "rrs_test");
    EXPECT_OK(pubsub_connect(ps));

    PubSubSub *sub = NULL;
    EXPECT_OK(pubsub_subscribe(ps, "service.echo", echo_responder, ps,
                               NULL, &sub));

    char *reply = NULL;
    EXPECT_OK(pubsub_request_str(ps, "service.echo", "hello world",
                                 5.0, &reply));
    EXPECT(reply != NULL);
    EXPECT(strstr(reply, "hello world") != NULL);

    free(reply);
    pubsub_unsubscribe(ps, sub);
    pubsub_destroy(ps);
    return true;
}

static bool test_request_timeout(void)
{
    PubSub *ps = make_ps("test_rrt", "rrt_test");
    EXPECT_OK(pubsub_connect(ps));

    /* No responder — should timeout */
    char *reply = NULL;
    int reply_len = 0;
    ps_status_t st = pubsub_request(ps, "nobody.home", "hello", 5,
                                    0.5, &reply, &reply_len);
    EXPECT(st == PS_ERR_TIMEOUT);
    free(reply);

    pubsub_destroy(ps);
    return true;
}

static bool test_stats(void)
{
    PubSub *ps = make_ps("test_stats", "stats_test");
    EXPECT_OK(pubsub_connect(ps));

    CallbackState state;
    cb_state_init(&state);

    PubSubSub *sub = NULL;
    EXPECT_OK(pubsub_subscribe(ps, "stat", generic_callback, &state,
                               NULL, &sub));

    for (int i = 0; i < 5; i++)
        EXPECT_OK(pubsub_publish_str(ps, "stat", "x"));

    cb_state_wait(&state, 5, 1000);

    PubSubStats stats;
    EXPECT_OK(pubsub_get_stats(ps, &stats));
    EXPECT(stats.msgs_published == 5);
    EXPECT(stats.msgs_received == 5);
    EXPECT(stats.active_subscriptions == 1);

    pubsub_unsubscribe(ps, sub);
    cb_state_destroy(&state);
    pubsub_destroy(ps);
    return true;
}

static double monotonic_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec * 1000.0 + (double)ts.tv_nsec / 1e6;
}

static bool test_performance(void)
{
    PubSub *ps = make_ps("test_perf", "perf_test");
    EXPECT_OK(pubsub_connect(ps));

    CallbackState state;
    cb_state_init(&state);

    PubSubSub *sub = NULL;
    EXPECT_OK(pubsub_subscribe(ps, "perf", generic_callback, &state,
                               NULL, &sub));

    int n = 1000;
    double t0 = monotonic_ms();

    for (int i = 0; i < n; i++)
        pubsub_publish_str(ps, "perf", "test");

    cb_state_wait(&state, n, 5000);
    double elapsed = monotonic_ms() - t0;

    printf("\n    %d msgs in %.1f ms (%.0f msgs/sec) ",
           n, elapsed, n / (elapsed / 1000.0));

    EXPECT(state.count == n);

    pubsub_unsubscribe(ps, sub);
    cb_state_destroy(&state);
    pubsub_destroy(ps);
    return true;
}

/* ================================================================== */
/*  Demo                                                               */
/* ================================================================== */

static void demo_callback(const PubSubMsg *msg, void *user_data)
{
    const char *label = user_data;
    printf("   [%s] subject=%s original=%s data=%.*s\n",
           label, msg->subject, msg->original_subject,
           msg->data_len, msg->data);
}

static void run_demo(void)
{
    printf("\n");
    printf("====================================================\n");
    printf("  NATS PubSub Demo (C)\n");
    printf("====================================================\n");

    PubSub *ps = make_ps("demo", "demo_client");
    if (pubsub_connect(ps) != PS_OK) {
        fprintf(stderr, "Failed to connect\n");
        pubsub_destroy(ps);
        return;
    }

    /* 1. Basic pub/sub */
    printf("\n1. Basic publish/subscribe:\n");
    PubSubSub *sub1 = NULL;
    pubsub_subscribe(ps, "greet", demo_callback, "sub1", NULL, &sub1);
    pubsub_publish_str(ps, "greet", "Hello, NATS!");
    pubsub_publish_str(ps, "greet", "Hello again!");
    usleep(300000);

    /* 2. Wildcards */
    printf("\n2. Wildcard subscriptions:\n");
    PubSubSub *sub_star = NULL, *sub_gt = NULL;
    pubsub_subscribe(ps, "sensor.*", demo_callback, "star", NULL, &sub_star);
    pubsub_subscribe(ps, "log.>", demo_callback, "gt", NULL, &sub_gt);

    pubsub_publish_str(ps, "sensor.temp", "23.5C");
    pubsub_publish_str(ps, "sensor.humidity", "65%");
    pubsub_publish_str(ps, "log.app.info", "Server started");
    pubsub_publish_str(ps, "log.app.error.db", "Connection timeout");
    usleep(300000);

    /* 3. Queue groups */
    printf("\n3. Queue group (load balancing):\n");
    CallbackState qs1, qs2;
    cb_state_init(&qs1);
    cb_state_init(&qs2);

    PubSubSub *qsub1 = NULL, *qsub2 = NULL;
    pubsub_subscribe(ps, "tasks", generic_callback, &qs1, "workers", &qsub1);
    pubsub_subscribe(ps, "tasks", generic_callback, &qs2, "workers", &qsub2);

    for (int i = 0; i < 10; i++) {
        char buf[32];
        snprintf(buf, sizeof(buf), "task_%d", i);
        pubsub_publish_str(ps, "tasks", buf);
    }
    usleep(500000);
    printf("   Worker 1 received: %d msgs\n", qs1.count);
    printf("   Worker 2 received: %d msgs\n", qs2.count);
    printf("   Total: %d (expected 10)\n", qs1.count + qs2.count);

    /* 4. Request/reply */
    printf("\n4. Request/reply:\n");
    PubSubSub *echo_sub = NULL;
    pubsub_subscribe(ps, "echo", echo_responder, ps, NULL, &echo_sub);

    char *reply = NULL;
    pubsub_request_str(ps, "echo", "Hello service!", 5.0, &reply);
    if (reply) {
        printf("   Reply: %s\n", reply);
        free(reply);
    }

    /* 5. Namespace isolation */
    printf("\n5. Namespace isolation:\n");
    PubSub *ps2 = make_ps("other_ns", "other_client");
    pubsub_connect(ps2);

    CallbackState iso;
    cb_state_init(&iso);
    PubSubSub *iso_sub = NULL;
    pubsub_subscribe(ps2, "greet", generic_callback, &iso, NULL, &iso_sub);

    pubsub_publish_str(ps, "greet", "From demo namespace");
    usleep(300000);
    printf("   Published on 'demo' namespace\n");
    printf("   'other_ns' subscriber received: %d msgs (expected 0)\n", iso.count);

    /* 6. Stats */
    printf("\n6. Statistics:\n");
    PubSubStats stats;
    pubsub_get_stats(ps, &stats);
    printf("   Published: %" PRId64 "\n", stats.msgs_published);
    printf("   Received:  %" PRId64 "\n", stats.msgs_received);
    printf("   Active subscriptions: %d\n", stats.active_subscriptions);

    /* Cleanup */
    pubsub_unsubscribe(ps, sub1);
    pubsub_unsubscribe(ps, sub_star);
    pubsub_unsubscribe(ps, sub_gt);
    pubsub_unsubscribe(ps, qsub1);
    pubsub_unsubscribe(ps, qsub2);
    pubsub_unsubscribe(ps, echo_sub);
    pubsub_unsubscribe(ps2, iso_sub);
    cb_state_destroy(&qs1);
    cb_state_destroy(&qs2);
    cb_state_destroy(&iso);

    pubsub_destroy(ps2);
    pubsub_destroy(ps);

    printf("\n====================================================\n");
}

/* ================================================================== */
/*  Main                                                               */
/* ================================================================== */

int main(int argc, char **argv)
{
    const char *mode = (argc > 1) ? argv[1] : "all";

    printf("\n======================================================================\n");
    printf("  NATS PubSub Test Suite (C)\n");
    printf("  Server: %s\n", TEST_SERVER);
    printf("======================================================================\n");

    if (strcmp(mode, "demo") == 0) {
        run_demo();
        return 0;
    }

    if (strcmp(mode, "all") == 0 || strcmp(mode, "tests") == 0) {
        printf("\n--- Basic ---\n");
        RUN_TEST(test_status_strings);
        RUN_TEST(test_config_defaults);
        RUN_TEST(test_connect_disconnect);

        printf("\n--- Publish / Subscribe ---\n");
        RUN_TEST(test_basic_pubsub);
        RUN_TEST(test_binary_payload);
        RUN_TEST(test_multiple_messages);
        RUN_TEST(test_multiple_subscribers);
        RUN_TEST(test_unsubscribe);
        RUN_TEST(test_namespace_isolation);

        printf("\n--- Wildcards ---\n");
        RUN_TEST(test_wildcard_star);
        RUN_TEST(test_wildcard_gt);

        printf("\n--- Queue Groups ---\n");
        RUN_TEST(test_queue_group);

        printf("\n--- Request / Reply ---\n");
        RUN_TEST(test_request_reply);
        RUN_TEST(test_request_str);
        RUN_TEST(test_request_timeout);

        printf("\n--- Statistics & Performance ---\n");
        RUN_TEST(test_stats);
        RUN_TEST(test_performance);
    }

    printf("\n======================================================================\n");
    printf("  Results: %d run, %d passed, %d failed (%.1f%%)\n",
           tests_run, tests_passed, tests_failed,
           tests_run > 0 ? (100.0 * tests_passed / tests_run) : 0.0);
    printf("======================================================================\n\n");

    return tests_failed > 0 ? 1 : 0;
}

