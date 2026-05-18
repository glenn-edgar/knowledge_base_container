/**
 * @file test_zenoh_pubsub.c
 * @brief Unit + end-to-end tests for zenoh_pubsub.
 *
 * Modes:
 *   - API-surface tests always run (no external dependencies).
 *   - When --transport=udp|tcp is given, performs a round-trip test against
 *     a zenohd reachable at the locator (default: 127.0.0.1:17447).
 *   - --transport=serial is documented but not exercised here (PTY loopback
 *     test is a separate fixture).
 */

#define _DEFAULT_SOURCE
#define _POSIX_C_SOURCE 200809L

#include "zenoh_pubsub.h"
#include "zenoh_token.h"

#include <getopt.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

static int pass = 0;
static int fail = 0;

#define CHECK(cond, msg) do {                                              \
    if (cond) { ++pass; }                                                  \
    else {                                                                 \
        ++fail;                                                            \
        fprintf(stderr, "FAIL: %s (%s:%d)\n", msg, __FILE__, __LINE__);    \
    }                                                                      \
} while (0)

/* ------------------------------------------------------------------ */
/*  API surface (always run)                                           */
/* ------------------------------------------------------------------ */

static void test_status_strings(void) {
    CHECK(strcmp(zps_status_str(ZPS_OK), "OK") == 0, "status string OK");
    CHECK(zps_status_str(ZPS_ERR_INVALID_ARG) != NULL, "status string invalid arg");
}

static void test_config_defaults(void) {
    ZenohPubSubConfig cfg;
    zenoh_pubsub_config_defaults(&cfg);
    CHECK(cfg.locators == NULL,           "default locators NULL");
    CHECK(cfg.n_locators == 0,            "default n_locators 0");
    CHECK(strcmp(cfg.mode, "client") == 0,"default mode 'client'");
    CHECK(cfg.enable_scout == false,      "default scout disabled");
}

static void test_create_invalid_args(void) {
    ZenohPubSub *ps = NULL;
    ZenohPubSubConfig cfg;
    zenoh_pubsub_config_defaults(&cfg);
    CHECK(zenoh_pubsub_create(NULL, &cfg) == ZPS_ERR_INVALID_ARG, "create NULL out rejected");
    CHECK(zenoh_pubsub_create(&ps, NULL)  == ZPS_ERR_INVALID_ARG, "create NULL cfg rejected");
    CHECK(zenoh_pubsub_create(&ps, &cfg)  == ZPS_ERR_INVALID_ARG, "create with no locators rejected");
}

/* ------------------------------------------------------------------ */
/*  End-to-end round trip                                              */
/* ------------------------------------------------------------------ */

typedef struct {
    pthread_mutex_t  lock;
    pthread_cond_t   cond;
    int              received;
    uint32_t         got_token;
    uint8_t          got_payload[256];
    size_t           got_len;
} recv_ctx_t;

static void on_message(uint32_t token, const uint8_t *payload, size_t len, void *ctx) {
    recv_ctx_t *rc = (recv_ctx_t *)ctx;
    pthread_mutex_lock(&rc->lock);
    rc->got_token = token;
    if (len <= sizeof(rc->got_payload)) {
        memcpy(rc->got_payload, payload, len);
        rc->got_len = len;
    }
    rc->received++;
    pthread_cond_broadcast(&rc->cond);
    pthread_mutex_unlock(&rc->lock);
}

static int wait_for_message(recv_ctx_t *rc, int target_count, int timeout_ms) {
    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec  += timeout_ms / 1000;
    deadline.tv_nsec += (timeout_ms % 1000) * 1000000L;
    if (deadline.tv_nsec >= 1000000000L) {
        deadline.tv_nsec -= 1000000000L;
        deadline.tv_sec  += 1;
    }
    pthread_mutex_lock(&rc->lock);
    while (rc->received < target_count) {
        int rc_wait = pthread_cond_timedwait(&rc->cond, &rc->lock, &deadline);
        if (rc_wait != 0) {
            pthread_mutex_unlock(&rc->lock);
            return -1;
        }
    }
    pthread_mutex_unlock(&rc->lock);
    return 0;
}

static int run_e2e_test(const char *locator) {
    fprintf(stderr, "\n=== End-to-end test against %s ===\n", locator);

    recv_ctx_t rc = {0};
    pthread_mutex_init(&rc.lock, NULL);
    pthread_cond_init(&rc.cond, NULL);

    /* One session each for publisher and subscriber — exercises both
     * sides of the wire and forces messages through zenohd. */
    const char *locs[] = { locator };
    ZenohPubSubConfig cfg;
    zenoh_pubsub_config_defaults(&cfg);
    cfg.locators = locs;
    cfg.n_locators = 1;

    ZenohPubSub *sub_ps = NULL, *pub_ps = NULL;
    CHECK(zenoh_pubsub_create(&sub_ps, &cfg) == ZPS_OK, "create sub session");
    CHECK(zenoh_pubsub_create(&pub_ps, &cfg) == ZPS_OK, "create pub session");

    CHECK(zenoh_pubsub_connect(sub_ps) == ZPS_OK, "connect sub session");
    CHECK(zenoh_pubsub_connect(pub_ps) == ZPS_OK, "connect pub session");

    /* Declare subscriber before publishing. */
    uint32_t topic = zt_hash("e2e/test/round_trip");
    ZenohPubSubSub *sub = NULL;
    CHECK(zenoh_pubsub_subscribe(sub_ps, topic, on_message, &rc, &sub) == ZPS_OK,
          "subscribe OK");

    /* Give the subscription a moment to propagate through zenohd. */
    usleep(200000);   /* 200 ms */

    /* Publish three small messages. */
    const char *msgs[] = { "hello", "world", "zenoh" };
    for (int i = 0; i < 3; ++i) {
        CHECK(zenoh_pubsub_publish(pub_ps, topic,
                                   (const uint8_t *)msgs[i], strlen(msgs[i])) == ZPS_OK,
              "publish message");
        usleep(50000); /* small inter-message delay so test is deterministic */
    }

    /* Wait for all three. */
    int rc_wait = wait_for_message(&rc, 3, 3000);
    CHECK(rc_wait == 0, "received all 3 messages within 3 s");
    CHECK(rc.received == 3, "received count == 3");
    CHECK(rc.got_token == topic, "last message had correct token");

    /* Clean up. */
    CHECK(zenoh_pubsub_unsubscribe(sub_ps, sub) == ZPS_OK, "unsubscribe OK");
    zenoh_pubsub_disconnect(pub_ps);
    zenoh_pubsub_disconnect(sub_ps);
    zenoh_pubsub_destroy(pub_ps);
    zenoh_pubsub_destroy(sub_ps);

    pthread_cond_destroy(&rc.cond);
    pthread_mutex_destroy(&rc.lock);
    return 0;
}

/* ------------------------------------------------------------------ */
/*  main                                                               */
/* ------------------------------------------------------------------ */

int main(int argc, char **argv) {
    const char *transport = NULL;
    const char *locator   = "udp/127.0.0.1:17447";  /* default test locator */
    static struct option long_opts[] = {
        {"transport", required_argument, 0, 't'},
        {"locator",   required_argument, 0, 'l'},
        {0, 0, 0, 0}
    };
    int opt;
    while ((opt = getopt_long(argc, argv, "t:l:", long_opts, NULL)) != -1) {
        if (opt == 't') transport = optarg;
        if (opt == 'l') locator   = optarg;
    }

    /* API-surface tests — no zenohd needed. */
    test_status_strings();
    test_config_defaults();
    test_create_invalid_args();

    /* End-to-end if a transport is requested. */
    if (transport != NULL) {
        if (strcmp(transport, "serial") == 0) {
            fprintf(stderr, "\n[SKIP] serial transport e2e not implemented here\n"
                            "       (PTY loopback fixture is a separate target)\n");
        } else {
            char locbuf[128];
            if (strcmp(transport, "tcp") == 0) {
                snprintf(locbuf, sizeof(locbuf), "tcp/127.0.0.1:17447");
                locator = locbuf;
            } else if (strcmp(transport, "udp") == 0) {
                /* keep default udp/127.0.0.1:17447 */
            } else if (strncmp(transport, "unix", 4) != 0) {
                /* Otherwise use --locator as provided. */
            }
            run_e2e_test(locator);
        }
    }

    printf("\nzenoh_pubsub tests: %d passed, %d failed\n", pass, fail);
    return fail == 0 ? 0 : 1;
}
