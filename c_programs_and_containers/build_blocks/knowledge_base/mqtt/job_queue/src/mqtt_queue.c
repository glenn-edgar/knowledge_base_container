/**
 * mqtt_queue.c - MQTT Queue Publisher & Reader implementation
 *
 * Wraps libmosquitto with thread-safe connect/subscribe/publish operations
 * and a persistent-session reader for offline message queuing.
 */

#define _POSIX_C_SOURCE 200809L   /* clock_gettime, nanosleep, strdup */

#include "mqtt_queue.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <time.h>

/* Suppress unused-parameter warnings for mosquitto callbacks */
#define UNUSED(x) (void)(x)

/* ================================================================== */
/*  Helpers                                                            */
/* ================================================================== */

static char *safe_strdup(const char *s)
{
    return s ? strdup(s) : NULL;
}

/**
 * Wait on a condition variable with an absolute-time timeout.
 * Returns 0 if signalled, ETIMEDOUT on timeout, or other errno.
 */
static int cond_timedwait_ms(pthread_cond_t *cond, pthread_mutex_t *mtx, int ms)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec  += ms / 1000;
    ts.tv_nsec += (ms % 1000) * 1000000L;
    if (ts.tv_nsec >= 1000000000L) {
        ts.tv_sec  += 1;
        ts.tv_nsec -= 1000000000L;
    }
    return pthread_cond_timedwait(cond, mtx, &ts);
}

/* ================================================================== */
/*  Message list                                                       */
/* ================================================================== */

void mqtt_msg_list_free(mqtt_msg_t *head)
{
    while (head) {
        mqtt_msg_t *next = head->next;
        free(head->topic);
        free(head->payload);
        free(head);
        head = next;
    }
}

static mqtt_msg_t *mqtt_msg_new(const char *topic, const void *payload, int payloadlen)
{
    mqtt_msg_t *m = calloc(1, sizeof(*m));
    if (!m) return NULL;

    m->topic   = safe_strdup(topic);
    m->payload = malloc((size_t)payloadlen + 1);
    if (m->payload) {
        memcpy(m->payload, payload, (size_t)payloadlen);
        m->payload[payloadlen] = '\0';
    }
    m->next = NULL;
    return m;
}

/* ================================================================== */
/*  Library init / cleanup                                             */
/* ================================================================== */

void mqtt_queue_lib_init(void)
{
    mosquitto_lib_init();
}

void mqtt_queue_lib_cleanup(void)
{
    mosquitto_lib_cleanup();
}

/* ================================================================== */
/*  QueuePublisher                                                     */
/* ================================================================== */

/* ---- callbacks --------------------------------------------------- */

static void pub_on_connect(struct mosquitto *mosq, void *userdata, int rc)
{
    UNUSED(mosq);
    mqtt_publisher_t *pub = (mqtt_publisher_t *)userdata;
    pthread_mutex_lock(&pub->lock);
    pub->connected = (rc == 0);
    printf("[publisher] on_connect rc=%d (%s)\n", rc,
           rc == 0 ? "success" : mosquitto_connack_string(rc));
    pthread_cond_signal(&pub->connect_cond);
    pthread_mutex_unlock(&pub->lock);
}

static void pub_on_disconnect(struct mosquitto *mosq, void *userdata, int rc)
{
    UNUSED(mosq);
    mqtt_publisher_t *pub = (mqtt_publisher_t *)userdata;
    pthread_mutex_lock(&pub->lock);
    pub->connected = false;
    printf("[publisher] on_disconnect rc=%d\n", rc);
    pthread_cond_signal(&pub->connect_cond);
    pthread_mutex_unlock(&pub->lock);
}

/* ---- API --------------------------------------------------------- */

int mqtt_publisher_init(mqtt_publisher_t *pub, const mqtt_queue_config_t *cfg)
{
    memset(pub, 0, sizeof(*pub));
    pub->cfg = *cfg;

    pthread_mutex_init(&pub->lock, NULL);
    pthread_cond_init(&pub->connect_cond, NULL);

    pub->mosq = mosquitto_new(cfg->client_id, cfg->clean_session, pub);
    if (!pub->mosq) {
        fprintf(stderr, "[publisher] mosquitto_new failed: %s\n", strerror(errno));
        return -1;
    }

    if (cfg->username) {
        mosquitto_username_pw_set(pub->mosq, cfg->username, cfg->password);
    }

    mosquitto_connect_callback_set(pub->mosq, pub_on_connect);
    mosquitto_disconnect_callback_set(pub->mosq, pub_on_disconnect);

    return 0;
}

int mqtt_publisher_connect(mqtt_publisher_t *pub, int timeout_ms)
{
    int rc = mosquitto_connect(pub->mosq, pub->cfg.host, pub->cfg.port, pub->cfg.keepalive);
    if (rc != MOSQ_ERR_SUCCESS) {
        fprintf(stderr, "[publisher] mosquitto_connect: %s\n", mosquitto_strerror(rc));
        return -1;
    }

    rc = mosquitto_loop_start(pub->mosq);
    if (rc != MOSQ_ERR_SUCCESS) {
        fprintf(stderr, "[publisher] mosquitto_loop_start: %s\n", mosquitto_strerror(rc));
        return -1;
    }

    /* Wait for CONNACK */
    pthread_mutex_lock(&pub->lock);
    if (!pub->connected) {
        cond_timedwait_ms(&pub->connect_cond, &pub->lock, timeout_ms);
    }
    bool ok = pub->connected;
    pthread_mutex_unlock(&pub->lock);

    if (!ok) {
        fprintf(stderr, "[publisher] connect timeout\n");
        mosquitto_loop_stop(pub->mosq, true);
        return -1;
    }
    return 0;
}

void mqtt_publisher_disconnect(mqtt_publisher_t *pub)
{
    mosquitto_disconnect(pub->mosq);
    mosquitto_loop_stop(pub->mosq, false);
    pthread_mutex_lock(&pub->lock);
    pub->connected = false;
    pthread_mutex_unlock(&pub->lock);
}

void mqtt_publisher_destroy(mqtt_publisher_t *pub)
{
    if (pub->mosq) {
        mosquitto_destroy(pub->mosq);
        pub->mosq = NULL;
    }
    pthread_mutex_destroy(&pub->lock);
    pthread_cond_destroy(&pub->connect_cond);
}

int mqtt_publisher_publish(mqtt_publisher_t *pub,
                           const char *topic,
                           const char *json_payload,
                           int qos,
                           bool retain)
{
    int mid = 0;
    int rc = mosquitto_publish(pub->mosq, &mid, topic,
                               (int)strlen(json_payload), json_payload,
                               qos, retain);
    if (rc != MOSQ_ERR_SUCCESS) {
        fprintf(stderr, "[publisher] publish failed: %s\n", mosquitto_strerror(rc));
        return -1;
    }
    return 0;
}

int mqtt_publisher_publish_batch(mqtt_publisher_t *pub,
                                 const char *topic,
                                 const char **json_payloads,
                                 int count,
                                 int qos,
                                 bool retain,
                                 int delay_between_ms)
{
    int ok = 0;
    for (int i = 0; i < count; i++) {
        if (mqtt_publisher_publish(pub, topic, json_payloads[i], qos, retain) == 0) {
            ok++;
            printf("  [batch] published %d/%d\n", i + 1, count);
        } else {
            fprintf(stderr, "  [batch] failed %d/%d\n", i + 1, count);
        }
        if (delay_between_ms > 0 && i < count - 1) {
            struct timespec ts = {
                .tv_sec  = delay_between_ms / 1000,
                .tv_nsec = (delay_between_ms % 1000) * 1000000L
            };
            nanosleep(&ts, NULL);
        }
    }
    return ok;
}

bool mqtt_publisher_is_connected(const mqtt_publisher_t *pub)
{
    return pub->connected;
}

/* ================================================================== */
/*  QueueReader                                                        */
/* ================================================================== */

/* ---- callbacks --------------------------------------------------- */

static void rdr_on_connect(struct mosquitto *mosq, void *userdata, int rc)
{
    UNUSED(mosq);
    mqtt_reader_t *rdr = (mqtt_reader_t *)userdata;
    pthread_mutex_lock(&rdr->lock);

    rdr->connected = (rc == 0);

    printf("[reader] on_connect rc=%d (%s)\n", rc,
           rc == 0 ? "success" : mosquitto_connack_string(rc));

    pthread_cond_signal(&rdr->connect_cond);
    pthread_mutex_unlock(&rdr->lock);
}

static void rdr_on_disconnect(struct mosquitto *mosq, void *userdata, int rc)
{
    UNUSED(mosq);
    mqtt_reader_t *rdr = (mqtt_reader_t *)userdata;
    pthread_mutex_lock(&rdr->lock);
    rdr->connected = false;
    printf("[reader] on_disconnect rc=%d\n", rc);
    pthread_cond_signal(&rdr->connect_cond);
    pthread_mutex_unlock(&rdr->lock);
}

static void rdr_on_message(struct mosquitto *mosq, void *userdata,
                           const struct mosquitto_message *msg)
{
    UNUSED(mosq);
    mqtt_reader_t *rdr = (mqtt_reader_t *)userdata;
    mqtt_msg_t *m = mqtt_msg_new(msg->topic, msg->payload, msg->payloadlen);
    if (!m) return;

    pthread_mutex_lock(&rdr->lock);
    if (rdr->msg_tail) {
        rdr->msg_tail->next = m;
    } else {
        rdr->msg_head = m;
    }
    rdr->msg_tail = m;
    rdr->msg_count++;
    pthread_mutex_unlock(&rdr->lock);
}

static void rdr_on_subscribe(struct mosquitto *mosq, void *userdata,
                             int mid, int qos_count, const int *granted_qos)
{
    UNUSED(mosq); UNUSED(granted_qos);
    mqtt_reader_t *rdr = (mqtt_reader_t *)userdata;
    printf("[reader] on_subscribe mid=%d qos_count=%d\n", mid, qos_count);

    pthread_mutex_lock(&rdr->lock);
    if (mid == rdr->subscribe_mid) {
        rdr->subscribe_ack = true;
        pthread_cond_signal(&rdr->subscribe_cond);
    }
    pthread_mutex_unlock(&rdr->lock);
}

/* ---- API --------------------------------------------------------- */

int mqtt_reader_init(mqtt_reader_t *rdr, const mqtt_queue_config_t *cfg)
{
    memset(rdr, 0, sizeof(*rdr));
    rdr->cfg = *cfg;

    pthread_mutex_init(&rdr->lock, NULL);
    pthread_cond_init(&rdr->connect_cond, NULL);
    pthread_cond_init(&rdr->subscribe_cond, NULL);

    rdr->mosq = mosquitto_new(cfg->client_id, cfg->clean_session, rdr);
    if (!rdr->mosq) {
        fprintf(stderr, "[reader] mosquitto_new failed: %s\n", strerror(errno));
        return -1;
    }

    if (cfg->username) {
        mosquitto_username_pw_set(rdr->mosq, cfg->username, cfg->password);
    }

    mosquitto_connect_callback_set(rdr->mosq, rdr_on_connect);
    mosquitto_disconnect_callback_set(rdr->mosq, rdr_on_disconnect);
    mosquitto_message_callback_set(rdr->mosq, rdr_on_message);
    mosquitto_subscribe_callback_set(rdr->mosq, rdr_on_subscribe);

    return 0;
}

int mqtt_reader_connect(mqtt_reader_t *rdr, int timeout_ms)
{
    int rc = mosquitto_connect(rdr->mosq, rdr->cfg.host, rdr->cfg.port, rdr->cfg.keepalive);
    if (rc != MOSQ_ERR_SUCCESS) {
        fprintf(stderr, "[reader] mosquitto_connect: %s\n", mosquitto_strerror(rc));
        return -1;
    }

    rc = mosquitto_loop_start(rdr->mosq);
    if (rc != MOSQ_ERR_SUCCESS) {
        fprintf(stderr, "[reader] mosquitto_loop_start: %s\n", mosquitto_strerror(rc));
        return -1;
    }

    pthread_mutex_lock(&rdr->lock);
    if (!rdr->connected) {
        cond_timedwait_ms(&rdr->connect_cond, &rdr->lock, timeout_ms);
    }
    bool ok = rdr->connected;
    pthread_mutex_unlock(&rdr->lock);

    if (!ok) {
        fprintf(stderr, "[reader] connect timeout\n");
        mosquitto_loop_stop(rdr->mosq, true);
        return -1;
    }
    return 0;
}

void mqtt_reader_disconnect(mqtt_reader_t *rdr)
{
    mosquitto_disconnect(rdr->mosq);
    mosquitto_loop_stop(rdr->mosq, false);
    pthread_mutex_lock(&rdr->lock);
    rdr->connected = false;
    pthread_mutex_unlock(&rdr->lock);
}

void mqtt_reader_destroy(mqtt_reader_t *rdr)
{
    /* Free any remaining messages */
    mqtt_msg_list_free(rdr->msg_head);
    rdr->msg_head = rdr->msg_tail = NULL;
    rdr->msg_count = 0;

    if (rdr->mosq) {
        mosquitto_destroy(rdr->mosq);
        rdr->mosq = NULL;
    }
    pthread_mutex_destroy(&rdr->lock);
    pthread_cond_destroy(&rdr->connect_cond);
    pthread_cond_destroy(&rdr->subscribe_cond);
}

int mqtt_reader_subscribe(mqtt_reader_t *rdr, const char *topic, int qos, int timeout_ms)
{
    pthread_mutex_lock(&rdr->lock);
    if (!rdr->connected) {
        pthread_mutex_unlock(&rdr->lock);
        fprintf(stderr, "[reader] subscribe: not connected\n");
        return -1;
    }

    rdr->subscribe_ack = false;
    int mid = 0;
    int rc = mosquitto_subscribe(rdr->mosq, &mid, topic, qos);
    if (rc != MOSQ_ERR_SUCCESS) {
        pthread_mutex_unlock(&rdr->lock);
        fprintf(stderr, "[reader] subscribe failed: %s\n", mosquitto_strerror(rc));
        return -1;
    }
    rdr->subscribe_mid = mid;

    /* Wait for SUBACK */
    while (!rdr->subscribe_ack) {
        int err = cond_timedwait_ms(&rdr->subscribe_cond, &rdr->lock, timeout_ms);
        if (err == ETIMEDOUT) {
            pthread_mutex_unlock(&rdr->lock);
            fprintf(stderr, "[reader] subscribe timeout waiting for SUBACK\n");
            return -1;
        }
    }
    pthread_mutex_unlock(&rdr->lock);
    return 0;
}

mqtt_msg_t *mqtt_reader_read_queue(mqtt_reader_t *rdr,
                                   const char *topic,
                                   int qos,
                                   int timeout_ms,
                                   int *out_count)
{
    /* Clear any old messages */
    mqtt_reader_clear(rdr);

    /* Subscribe if this isn't a resumed persistent session */
    if (!rdr->session_present) {
        if (mqtt_reader_subscribe(rdr, topic, qos, 2000) != 0) {
            if (out_count) *out_count = 0;
            return NULL;
        }
    } else {
        printf("[reader] using existing subscription from persistent session\n");
    }

    /* Collect messages for the specified timeout */
    struct timespec ts = {
        .tv_sec  = timeout_ms / 1000,
        .tv_nsec = (timeout_ms % 1000) * 1000000L
    };
    nanosleep(&ts, NULL);

    /* Detach the message list */
    pthread_mutex_lock(&rdr->lock);
    mqtt_msg_t *head = rdr->msg_head;
    int count = rdr->msg_count;
    rdr->msg_head = rdr->msg_tail = NULL;
    rdr->msg_count = 0;
    pthread_mutex_unlock(&rdr->lock);

    if (out_count) *out_count = count;
    return head;
}

void mqtt_reader_clear(mqtt_reader_t *rdr)
{
    pthread_mutex_lock(&rdr->lock);
    mqtt_msg_list_free(rdr->msg_head);
    rdr->msg_head = rdr->msg_tail = NULL;
    rdr->msg_count = 0;
    pthread_mutex_unlock(&rdr->lock);
}

bool mqtt_reader_is_connected(const mqtt_reader_t *rdr)
{
    return rdr->connected;
}

