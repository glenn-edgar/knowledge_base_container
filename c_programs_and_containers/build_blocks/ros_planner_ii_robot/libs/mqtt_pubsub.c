/**
 * @file mqtt_pubsub.c
 * @brief Streaming MQTT pub/sub with thread-safe ring buffer.
 *
 * mosquitto's on_message callback fires on its network thread.
 * We buffer messages in a lock-free(ish) ring and let the main
 * thread drain them via mqps_poll() / mqps_drain().
 *
 * Copied from knowledge_base/mqtt/ for standalone robot deployment.
 */

#include "mqtt_pubsub.h"
#include <mosquitto.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <stdatomic.h>

/* ================================================================== */
/*  Ring buffer (single producer / single consumer with mutex)         */
/* ================================================================== */

typedef struct {
    MqpsMsgSlot  slots[MQPS_RING_SIZE];
    atomic_uint  head;   /* next write position (producer: callback thread) */
    atomic_uint  tail;   /* next read position  (consumer: main thread)    */
    atomic_uint  dropped;
    pthread_mutex_t lock;
} MqpsRing;

static void ring_init(MqpsRing *r) {
    memset(r->slots, 0, sizeof(r->slots));
    atomic_store(&r->head, 0);
    atomic_store(&r->tail, 0);
    atomic_store(&r->dropped, 0);
    pthread_mutex_init(&r->lock, NULL);
}

static void ring_destroy(MqpsRing *r) {
    pthread_mutex_destroy(&r->lock);
}

static uint32_t ring_used(MqpsRing *r) {
    uint32_t h = atomic_load(&r->head);
    uint32_t t = atomic_load(&r->tail);
    return (h - t) & (MQPS_RING_SIZE - 1);
}

/* Push a message (called from mosquitto callback thread) */
static bool ring_push(MqpsRing *r, const char *topic,
                       const void *payload, int payload_len,
                       int qos, bool retain) {
    pthread_mutex_lock(&r->lock);

    uint32_t h = atomic_load(&r->head);
    uint32_t t = atomic_load(&r->tail);
    uint32_t next = (h + 1) & (MQPS_RING_SIZE - 1);

    if (next == t) {
        /* Ring full -- drop oldest */
        atomic_fetch_add(&r->dropped, 1);
        atomic_store(&r->tail, (t + 1) & (MQPS_RING_SIZE - 1));
    }

    MqpsMsgSlot *slot = &r->slots[h & (MQPS_RING_SIZE - 1)];

    /* Copy topic */
    if (topic) {
        strncpy(slot->topic, topic, MQPS_MAX_TOPIC - 1);
        slot->topic[MQPS_MAX_TOPIC - 1] = '\0';
    } else {
        slot->topic[0] = '\0';
    }

    /* Copy payload */
    int copy_len = payload_len;
    if (copy_len > MQPS_MAX_PAYLOAD - 1) copy_len = MQPS_MAX_PAYLOAD - 1;
    if (payload && copy_len > 0) {
        memcpy(slot->payload, payload, copy_len);
    }
    slot->payload[copy_len] = '\0';
    slot->payload_len = (uint32_t)copy_len;
    slot->qos = qos;
    slot->retain = retain;
    slot->valid = true;

    atomic_store(&r->head, next);
    pthread_mutex_unlock(&r->lock);
    return true;
}

/* Pop up to max messages (called from main thread) */
static int ring_pop(MqpsRing *r, MqpsMsgSlot *out, int max) {
    pthread_mutex_lock(&r->lock);

    int count = 0;
    while (count < max) {
        uint32_t h = atomic_load(&r->head);
        uint32_t t = atomic_load(&r->tail);
        if (h == t) break;  /* empty */

        MqpsMsgSlot *slot = &r->slots[t & (MQPS_RING_SIZE - 1)];
        if (slot->valid) {
            memcpy(&out[count], slot, sizeof(MqpsMsgSlot));
            slot->valid = false;
            count++;
        }
        atomic_store(&r->tail, (t + 1) & (MQPS_RING_SIZE - 1));
    }

    pthread_mutex_unlock(&r->lock);
    return count;
}

/* ================================================================== */
/*  MqttPubSubStream handle                                            */
/* ================================================================== */

struct MqttPubSubStream {
    struct mosquitto *mosq;
    char   host[256];
    int    port;
    char   client_id[128];
    bool   connected;
    MqpsRing ring;
    uint64_t published;
    uint64_t received;
    uint64_t polled;
};

/* ================================================================== */
/*  mosquitto callbacks                                                */
/* ================================================================== */

static void on_connect(struct mosquitto *mosq, void *userdata, int rc) {
    (void)mosq;
    MqttPubSubStream *ps = (MqttPubSubStream *)userdata;
    if (rc == 0) {
        ps->connected = true;
    } else {
        fprintf(stderr, "[mqps] connect failed: rc=%d\n", rc);
        ps->connected = false;
    }
}

static void on_disconnect(struct mosquitto *mosq, void *userdata, int rc) {
    (void)mosq;
    MqttPubSubStream *ps = (MqttPubSubStream *)userdata;
    ps->connected = false;
    if (rc != 0) {
        fprintf(stderr, "[mqps] unexpected disconnect: rc=%d\n", rc);
    }
}

static void on_message(struct mosquitto *mosq, void *userdata,
                        const struct mosquitto_message *msg) {
    (void)mosq;
    MqttPubSubStream *ps = (MqttPubSubStream *)userdata;
    ps->received++;
    ring_push(&ps->ring, msg->topic,
              msg->payload, msg->payloadlen,
              msg->qos, msg->retain);
}

/* ================================================================== */
/*  Public API                                                         */
/* ================================================================== */

MqttPubSubStream *mqps_create(const char *host, int port,
                               const char *client_id) {
    MqttPubSubStream *ps = calloc(1, sizeof(MqttPubSubStream));
    if (!ps) return NULL;

    strncpy(ps->host, host ? host : "localhost", sizeof(ps->host) - 1);
    ps->port = port > 0 ? port : 1883;
    strncpy(ps->client_id, client_id ? client_id : "mqps_client",
            sizeof(ps->client_id) - 1);

    ring_init(&ps->ring);

    mosquitto_lib_init();

    ps->mosq = mosquitto_new(ps->client_id, false, ps);  /* clean_session=false */
    if (!ps->mosq) {
        free(ps);
        return NULL;
    }

    mosquitto_connect_callback_set(ps->mosq, on_connect);
    mosquitto_disconnect_callback_set(ps->mosq, on_disconnect);
    mosquitto_message_callback_set(ps->mosq, on_message);

    return ps;
}

void mqps_destroy(MqttPubSubStream *ps) {
    if (!ps) return;
    if (ps->connected) {
        mosquitto_disconnect(ps->mosq);
    }
    mosquitto_destroy(ps->mosq);
    ring_destroy(&ps->ring);
    free(ps);
}

bool mqps_connect(MqttPubSubStream *ps, int timeout_ms) {
    if (!ps || !ps->mosq) return false;

    int rc = mosquitto_connect(ps->mosq, ps->host, ps->port, 60);
    if (rc != MOSQ_ERR_SUCCESS) {
        fprintf(stderr, "[mqps] connect error: %s\n", mosquitto_strerror(rc));
        return false;
    }

    /* Run network loop until connected or timeout */
    struct timespec start, now;
    clock_gettime(CLOCK_MONOTONIC, &start);

    while (!ps->connected) {
        mosquitto_loop(ps->mosq, 10, 1);  /* 10ms loop */
        clock_gettime(CLOCK_MONOTONIC, &now);
        int elapsed_ms = (int)((now.tv_sec - start.tv_sec) * 1000 +
                               (now.tv_nsec - start.tv_nsec) / 1000000);
        if (elapsed_ms >= timeout_ms) {
            fprintf(stderr, "[mqps] connect timeout after %dms\n", elapsed_ms);
            return false;
        }
    }

    return true;
}

void mqps_disconnect(MqttPubSubStream *ps) {
    if (!ps || !ps->mosq) return;
    mosquitto_disconnect(ps->mosq);
    mosquitto_loop(ps->mosq, 100, 1);  /* flush */
    ps->connected = false;
}

bool mqps_is_connected(MqttPubSubStream *ps) {
    return ps && ps->connected;
}

bool mqps_publish(MqttPubSubStream *ps, const char *topic,
                  const void *payload, int payload_len,
                  int qos, bool retain) {
    if (!ps || !ps->mosq || !ps->connected) return false;

    int rc = mosquitto_publish(ps->mosq, NULL, topic, payload_len, payload,
                                qos, retain);
    if (rc == MOSQ_ERR_SUCCESS) {
        ps->published++;
        /* Flush to ensure publish goes out */
        mosquitto_loop(ps->mosq, 0, 1);
        return true;
    }

    fprintf(stderr, "[mqps] publish error: %s\n", mosquitto_strerror(rc));
    return false;
}

bool mqps_subscribe(MqttPubSubStream *ps, const char *topic, int qos) {
    if (!ps || !ps->mosq || !ps->connected) return false;

    int rc = mosquitto_subscribe(ps->mosq, NULL, topic, qos);
    if (rc == MOSQ_ERR_SUCCESS) {
        /* Run loop briefly to process SUBACK */
        mosquitto_loop(ps->mosq, 100, 1);
        return true;
    }

    fprintf(stderr, "[mqps] subscribe error: %s\n", mosquitto_strerror(rc));
    return false;
}

bool mqps_unsubscribe(MqttPubSubStream *ps, const char *topic) {
    if (!ps || !ps->mosq) return false;

    int rc = mosquitto_unsubscribe(ps->mosq, NULL, topic);
    if (rc == MOSQ_ERR_SUCCESS) {
        mosquitto_loop(ps->mosq, 100, 1);
        return true;
    }
    return false;
}

int mqps_poll(MqttPubSubStream *ps, int timeout_ms,
              MqpsMsgSlot *out_msgs, int max_msgs) {
    if (!ps || !ps->mosq) return 0;

    /* Run mosquitto network loop -- triggers on_message callbacks
       which push into the ring buffer. */
    struct timespec start, now;
    clock_gettime(CLOCK_MONOTONIC, &start);

    do {
        mosquitto_loop(ps->mosq, 1, 1);  /* 1ms per iteration */
        clock_gettime(CLOCK_MONOTONIC, &now);
        int elapsed_ms = (int)((now.tv_sec - start.tv_sec) * 1000 +
                               (now.tv_nsec - start.tv_nsec) / 1000000);
        if (elapsed_ms >= timeout_ms) break;
    } while (1);

    /* Drain ring buffer into output */
    int count = ring_pop(&ps->ring, out_msgs, max_msgs);
    ps->polled += (uint64_t)count;
    return count;
}

int mqps_drain(MqttPubSubStream *ps,
               MqpsMsgSlot *out_msgs, int max_msgs) {
    if (!ps) return 0;
    int count = ring_pop(&ps->ring, out_msgs, max_msgs);
    ps->polled += (uint64_t)count;
    return count;
}

void mqps_get_stats(MqttPubSubStream *ps, MqpsStats *out) {
    if (!ps || !out) return;
    out->published = ps->published;
    out->received  = ps->received;
    out->dropped   = atomic_load(&ps->ring.dropped);
    out->polled    = ps->polled;
    out->ring_used = ring_used(&ps->ring);
}
