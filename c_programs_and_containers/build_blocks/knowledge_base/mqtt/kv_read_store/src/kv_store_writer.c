/**
 * kv_store_writer.c - Writes retained key/value messages to an MQTT broker.
 *
 * Translated from Python KVStoreWriter class.
 * Requires: libmosquitto (mosquitto-dev)
 *   apt install libmosquitto-dev
 *   Link with: -lmosquitto -lpthread
 */

#include "kv_store_writer.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <math.h>
#include <mosquitto.h>

/* ── Internal helpers ───────────────────────────────────────────────── */

static void kvw_now(struct timespec *ts)
{
    clock_gettime(CLOCK_REALTIME, ts);
}

/**
 * Build an absolute timespec that is `offset_sec` seconds from now.
 */
static void kvw_deadline(struct timespec *out, double offset_sec)
{
    kvw_now(out);
    long whole = (long)offset_sec;
    long frac  = (long)((offset_sec - (double)whole) * 1e9);
    out->tv_nsec += frac;
    out->tv_sec  += whole + out->tv_nsec / 1000000000L;
    out->tv_nsec  = out->tv_nsec % 1000000000L;
}

/* ── Pending-publish helpers (caller must hold pending_lock) ────────── */

/**
 * Register a new mid to track.  Returns slot index or -1 if full.
 */
static int kvw_pending_add(kvw_store_writer_t *kv, int mid)
{
    if (kv->pending_count >= KVW_MAX_PENDING) return -1;
    int idx = kv->pending_count++;
    kv->pending[idx].mid       = mid;
    kv->pending[idx].completed = false;
    kv->pending[idx].success   = false;
    return idx;
}

/**
 * Find a pending slot by mid. Returns index or -1.
 */
static int kvw_pending_find(kvw_store_writer_t *kv, int mid)
{
    for (int i = 0; i < kv->pending_count; i++) {
        if (kv->pending[i].mid == mid) return i;
    }
    return -1;
}

/**
 * Remove a pending slot by index (swap with last).
 */
static void kvw_pending_remove(kvw_store_writer_t *kv, int idx)
{
    if (idx < 0 || idx >= kv->pending_count) return;
    kv->pending[idx] = kv->pending[kv->pending_count - 1];
    kv->pending_count--;
}

/**
 * Wait for a specific mid to complete.
 * Caller must hold pending_lock on entry; lock is held on return.
 * Returns true if completed before deadline, false on timeout.
 */
static bool kvw_pending_wait(kvw_store_writer_t *kv, int mid,
                             const struct timespec *deadline)
{
    for (;;) {
        int idx = kvw_pending_find(kv, mid);
        if (idx < 0) return false;               /* was removed externally */
        if (kv->pending[idx].completed) return true;

        int rc = pthread_cond_timedwait(&kv->pending_cond,
                                        &kv->pending_lock, deadline);
        if (rc == ETIMEDOUT) return false;
    }
}

/* ── Mosquitto callbacks ────────────────────────────────────────────── */

static void on_connect_cb(struct mosquitto *mosq, void *obj, int reason_code)
{
    (void)mosq;
    kvw_store_writer_t *kv = (kvw_store_writer_t *)obj;

    pthread_mutex_lock(&kv->state_lock);
    kv->connected    = (reason_code == 0);
    kv->connect_done = true;
    pthread_cond_signal(&kv->connect_cond);
    pthread_mutex_unlock(&kv->state_lock);

    if (kv->connected) {
        printf("[Connected] Successfully connected to %s:%d\n",
               kv->config.host, kv->config.port);
    } else {
        printf("[Connection Failed] reason_code=%d\n", reason_code);
    }
}

static void on_disconnect_cb(struct mosquitto *mosq, void *obj, int reason_code)
{
    (void)mosq;
    kvw_store_writer_t *kv = (kvw_store_writer_t *)obj;

    pthread_mutex_lock(&kv->state_lock);
    kv->connected = false;
    pthread_mutex_unlock(&kv->state_lock);

    printf("[Disconnected] reason_code=%d\n", reason_code);
}

static void on_publish_cb(struct mosquitto *mosq, void *obj, int mid)
{
    (void)mosq;
    kvw_store_writer_t *kv = (kvw_store_writer_t *)obj;

    pthread_mutex_lock(&kv->pending_lock);
    int idx = kvw_pending_find(kv, mid);
    if (idx >= 0) {
        kv->pending[idx].completed = true;
        kv->pending[idx].success   = true;
    }
    pthread_cond_broadcast(&kv->pending_cond);
    pthread_mutex_unlock(&kv->pending_lock);
}

/* ── Public API ─────────────────────────────────────────────────────── */

void kvw_config_init(kvw_config_t *cfg)
{
    memset(cfg, 0, sizeof(*cfg));
    strncpy(cfg->host, "localhost", sizeof(cfg->host) - 1);
    cfg->port            = 1883;
    strncpy(cfg->client_id, "kv-writer", sizeof(cfg->client_id) - 1);
    cfg->keepalive       = 60;
    cfg->use_mqttv5      = false;
    cfg->has_credentials = false;
    cfg->clean_session   = true;
}

int kvw_init(kvw_store_writer_t *kv, const kvw_config_t *cfg)
{
    memset(kv, 0, sizeof(*kv));
    kv->config = *cfg;

    kv->mosq = mosquitto_new(cfg->client_id, cfg->clean_session, kv);
    if (!kv->mosq) {
        fprintf(stderr, "[Error] mosquitto_new failed: %s\n", strerror(errno));
        return -1;
    }

    if (cfg->has_credentials) {
        mosquitto_username_pw_set(kv->mosq, cfg->username, cfg->password);
    }

    if (cfg->use_mqttv5) {
        int ver = MQTT_PROTOCOL_V5;
        mosquitto_int_option(kv->mosq, MOSQ_OPT_PROTOCOL_VERSION, ver);
    }

    mosquitto_connect_callback_set(kv->mosq, on_connect_cb);
    mosquitto_disconnect_callback_set(kv->mosq, on_disconnect_cb);
    mosquitto_publish_callback_set(kv->mosq, on_publish_cb);

    pthread_mutex_init(&kv->pending_lock, NULL);
    pthread_cond_init(&kv->pending_cond, NULL);
    pthread_mutex_init(&kv->state_lock, NULL);
    pthread_cond_init(&kv->connect_cond, NULL);

    return 0;
}

bool kvw_connect(kvw_store_writer_t *kv, double timeout_sec)
{
    pthread_mutex_lock(&kv->state_lock);
    kv->connect_done = false;
    kv->running      = true;
    pthread_mutex_unlock(&kv->state_lock);

    int rc = mosquitto_connect(kv->mosq, kv->config.host,
                               kv->config.port, kv->config.keepalive);
    if (rc != MOSQ_ERR_SUCCESS) {
        fprintf(stderr, "[Error] Connection failed: %s\n", mosquitto_strerror(rc));
        kv->running = false;
        return false;
    }

    rc = mosquitto_loop_start(kv->mosq);
    if (rc != MOSQ_ERR_SUCCESS) {
        fprintf(stderr, "[Error] loop_start failed: %s\n", mosquitto_strerror(rc));
        kv->running = false;
        return false;
    }

    /* Wait for CONNACK */
    struct timespec deadline;
    kvw_deadline(&deadline, timeout_sec);

    pthread_mutex_lock(&kv->state_lock);
    while (!kv->connect_done) {
        int wait_rc = pthread_cond_timedwait(&kv->connect_cond,
                                             &kv->state_lock, &deadline);
        if (wait_rc == ETIMEDOUT) break;
    }
    bool ok = kv->connected;
    pthread_mutex_unlock(&kv->state_lock);

    if (!ok) {
        fprintf(stderr, "[Error] Connection timeout or refused (%s:%d)\n",
                kv->config.host, kv->config.port);
        kv->running = false;
        mosquitto_loop_stop(kv->mosq, true);
        return false;
    }

    return true;
}

void kvw_disconnect(kvw_store_writer_t *kv)
{
    kv->running = false;
    mosquitto_disconnect(kv->mosq);
    mosquitto_loop_stop(kv->mosq, false);

    pthread_mutex_lock(&kv->state_lock);
    kv->connected = false;
    pthread_mutex_unlock(&kv->state_lock);
}

void kvw_destroy(kvw_store_writer_t *kv)
{
    if (kv->running || kv->connected) {
        kvw_disconnect(kv);
    }
    if (kv->mosq) {
        mosquitto_destroy(kv->mosq);
        kv->mosq = NULL;
    }
    pthread_mutex_destroy(&kv->pending_lock);
    pthread_cond_destroy(&kv->pending_cond);
    pthread_mutex_destroy(&kv->state_lock);
    pthread_cond_destroy(&kv->connect_cond);
}

bool kvw_write_single(kvw_store_writer_t *kv,
                      const char *topic,
                      const char *value,
                      int qos,
                      bool retain,
                      double timeout_sec)
{
    if (!kv->connected) {
        fprintf(stderr, "[Error] Not connected. Call kvw_connect() first.\n");
        return false;
    }

    int mid = 0;
    int payloadlen = value ? (int)strlen(value) : 0;
    int rc = mosquitto_publish(kv->mosq, &mid, topic,
                               payloadlen, value, qos, retain);
    if (rc != MOSQ_ERR_SUCCESS) {
        fprintf(stderr, "[Error] Failed to queue message for %s: %s\n",
                topic, mosquitto_strerror(rc));
        return false;
    }

    /* Register mid and wait for on_publish */
    struct timespec deadline;
    kvw_deadline(&deadline, timeout_sec);

    pthread_mutex_lock(&kv->pending_lock);
    int idx = kvw_pending_add(kv, mid);
    if (idx < 0) {
        pthread_mutex_unlock(&kv->pending_lock);
        fprintf(stderr, "[Error] Too many pending publishes\n");
        return false;
    }

    bool completed = kvw_pending_wait(kv, mid, &deadline);
    bool success   = false;

    if (completed) {
        idx = kvw_pending_find(kv, mid);
        if (idx >= 0) {
            success = kv->pending[idx].success;
            kvw_pending_remove(kv, idx);
        }
    } else {
        fprintf(stderr, "[Timeout] Publish timeout for %s\n", topic);
        idx = kvw_pending_find(kv, mid);
        if (idx >= 0) kvw_pending_remove(kv, idx);
    }
    pthread_mutex_unlock(&kv->pending_lock);

    if (success) {
        /* Truncated preview */
        char preview[54];
        if (value) {
            int plen = (int)strlen(value);
            int clen = plen < 50 ? plen : 50;
            memcpy(preview, value, clen);
            preview[clen] = '\0';
            printf("[Written] %s => %s%s\n", topic, preview,
                   plen > 50 ? "..." : "");
        } else {
            printf("[Written] %s => (null)\n", topic);
        }
    }

    return success;
}

int kvw_write_batch(kvw_store_writer_t *kv,
                    int count,
                    const char *topics[],
                    const char *values[],
                    int qos,
                    bool retain,
                    double timeout_sec,
                    const char *failed_topics[])
{
    if (!kv->connected) {
        fprintf(stderr, "[Error] Not connected. Call kvw_connect() first.\n");
        return 0;
    }
    if (count <= 0) return 0;
    if (count > KVW_MAX_BATCH) count = KVW_MAX_BATCH;

    struct timespec deadline;
    kvw_deadline(&deadline, timeout_sec);

    /* Phase 1: publish all messages and register mids */
    int mids[KVW_MAX_BATCH];
    bool queued[KVW_MAX_BATCH];
    int fail_idx = 0;

    pthread_mutex_lock(&kv->pending_lock);
    for (int i = 0; i < count; i++) {
        mids[i]   = 0;
        queued[i] = false;

        const char *val = values[i] ? values[i] : "";
        int rc = mosquitto_publish(kv->mosq, &mids[i], topics[i],
                                   (int)strlen(val), val, qos, retain);
        if (rc != MOSQ_ERR_SUCCESS) {
            fprintf(stderr, "[Error] Failed to queue %s: %s\n",
                    topics[i], mosquitto_strerror(rc));
            if (failed_topics) failed_topics[fail_idx++] = topics[i];
            continue;
        }

        if (kvw_pending_add(kv, mids[i]) < 0) {
            fprintf(stderr, "[Error] Too many pending publishes\n");
            if (failed_topics) failed_topics[fail_idx++] = topics[i];
            continue;
        }
        queued[i] = true;
    }
    pthread_mutex_unlock(&kv->pending_lock);

    /* Phase 2: wait for each queued message */
    int success_count = 0;

    pthread_mutex_lock(&kv->pending_lock);
    for (int i = 0; i < count; i++) {
        if (!queued[i]) continue;

        bool completed = kvw_pending_wait(kv, mids[i], &deadline);
        int idx = kvw_pending_find(kv, mids[i]);

        if (completed && idx >= 0 && kv->pending[idx].success) {
            success_count++;
            printf("[Batch Written] %s\n", topics[i]);
        } else {
            if (!completed) {
                fprintf(stderr, "[Batch Timeout] %s\n", topics[i]);
            }
            if (failed_topics) failed_topics[fail_idx++] = topics[i];
        }

        if (idx >= 0) kvw_pending_remove(kv, idx);
    }
    pthread_mutex_unlock(&kv->pending_lock);

    return success_count;
}

bool kvw_delete_single(kvw_store_writer_t *kv,
                       const char *topic,
                       double timeout_sec)
{
    if (!kv->connected) {
        fprintf(stderr, "[Error] Not connected. Call kvw_connect() first.\n");
        return false;
    }

    /* Publish empty retained message to delete the key */
    int mid = 0;
    int rc = mosquitto_publish(kv->mosq, &mid, topic, 0, "", 1, true);
    if (rc != MOSQ_ERR_SUCCESS) {
        fprintf(stderr, "[Error] Failed to delete %s: %s\n",
                topic, mosquitto_strerror(rc));
        return false;
    }

    struct timespec deadline;
    kvw_deadline(&deadline, timeout_sec);

    pthread_mutex_lock(&kv->pending_lock);
    int idx = kvw_pending_add(kv, mid);
    if (idx < 0) {
        pthread_mutex_unlock(&kv->pending_lock);
        fprintf(stderr, "[Error] Too many pending publishes\n");
        return false;
    }

    bool completed = kvw_pending_wait(kv, mid, &deadline);
    bool success   = false;

    if (completed) {
        idx = kvw_pending_find(kv, mid);
        if (idx >= 0) {
            success = kv->pending[idx].success;
            kvw_pending_remove(kv, idx);
        }
    } else {
        fprintf(stderr, "[Timeout] Delete timeout for %s\n", topic);
        idx = kvw_pending_find(kv, mid);
        if (idx >= 0) kvw_pending_remove(kv, idx);
    }
    pthread_mutex_unlock(&kv->pending_lock);

    if (success) {
        printf("[Deleted] %s\n", topic);
    }

    return success;
}

int kvw_delete_batch(kvw_store_writer_t *kv,
                     int count,
                     const char *topics[],
                     double timeout_sec,
                     const char *failed_topics[])
{
    if (!kv->connected) {
        fprintf(stderr, "[Error] Not connected. Call kvw_connect() first.\n");
        return 0;
    }
    if (count <= 0) return 0;
    if (count > KVW_MAX_BATCH) count = KVW_MAX_BATCH;

    struct timespec deadline;
    kvw_deadline(&deadline, timeout_sec);

    /* Phase 1: publish all empty retained messages */
    int mids[KVW_MAX_BATCH];
    bool queued[KVW_MAX_BATCH];
    int fail_idx = 0;

    pthread_mutex_lock(&kv->pending_lock);
    for (int i = 0; i < count; i++) {
        mids[i]   = 0;
        queued[i] = false;

        int rc = mosquitto_publish(kv->mosq, &mids[i], topics[i],
                                   0, "", 1, true);
        if (rc != MOSQ_ERR_SUCCESS) {
            fprintf(stderr, "[Error] Failed to queue delete for %s: %s\n",
                    topics[i], mosquitto_strerror(rc));
            if (failed_topics) failed_topics[fail_idx++] = topics[i];
            continue;
        }

        if (kvw_pending_add(kv, mids[i]) < 0) {
            fprintf(stderr, "[Error] Too many pending publishes\n");
            if (failed_topics) failed_topics[fail_idx++] = topics[i];
            continue;
        }
        queued[i] = true;
    }
    pthread_mutex_unlock(&kv->pending_lock);

    /* Phase 2: wait for each */
    int success_count = 0;

    pthread_mutex_lock(&kv->pending_lock);
    for (int i = 0; i < count; i++) {
        if (!queued[i]) continue;

        bool completed = kvw_pending_wait(kv, mids[i], &deadline);
        int idx = kvw_pending_find(kv, mids[i]);

        if (completed && idx >= 0 && kv->pending[idx].success) {
            success_count++;
            printf("[Batch Deleted] %s\n", topics[i]);
        } else {
            if (!completed) {
                fprintf(stderr, "[Delete Timeout] %s\n", topics[i]);
            }
            if (failed_topics) failed_topics[fail_idx++] = topics[i];
        }

        if (idx >= 0) kvw_pending_remove(kv, idx);
    }
    pthread_mutex_unlock(&kv->pending_lock);

    return success_count;
}

bool kvw_update_single(kvw_store_writer_t *kv,
                       const char *topic,
                       const char *value,
                       int qos,
                       double timeout_sec)
{
    return kvw_write_single(kv, topic, value, qos, true, timeout_sec);
}

bool kvw_is_connected(kvw_store_writer_t *kv)
{
    pthread_mutex_lock(&kv->state_lock);
    bool c = kv->connected;
    pthread_mutex_unlock(&kv->state_lock);
    return c;
}

