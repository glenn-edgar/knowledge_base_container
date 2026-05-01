/**
 * kv_store_writer.h - Writes retained key/value messages to an MQTT broker.
 *
 * Translated from Python KVStoreWriter class.
 * Requires: libmosquitto (mosquitto-dev)
 *   apt install libmosquitto-dev
 *   Link with: -lmosquitto -lpthread
 */

#ifndef KV_STORE_WRITER_H
#define KV_STORE_WRITER_H

#include <stdbool.h>
#include <stdint.h>
#include <time.h>
#include <pthread.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ── Limits ─────────────────────────────────────────────────────────── */

#define KVW_MAX_TOPIC_LEN       256
#define KVW_MAX_VALUE_LEN       4096
#define KVW_MAX_CLIENT_ID_LEN   128
#define KVW_MAX_PENDING         128     /* max in-flight publishes tracked */
#define KVW_MAX_BATCH           64      /* max topics in one batch call */

/* ── Publish result tracking ────────────────────────────────────────── */

typedef struct {
    int     mid;            /* message id from mosquitto_publish */
    bool    completed;      /* on_publish fired */
    bool    success;        /* publish succeeded */
} kvw_publish_result_t;

/* ── Configuration ──────────────────────────────────────────────────── */

typedef struct {
    char    host[256];
    int     port;
    char    client_id[KVW_MAX_CLIENT_ID_LEN];
    int     keepalive;
    bool    use_mqttv5;
    char    username[128];
    char    password[128];
    bool    has_credentials;
    bool    clean_session;
} kvw_config_t;

/* ── KVStoreWriter Handle ───────────────────────────────────────────── */

typedef struct {
    /* Configuration */
    kvw_config_t            config;

    /* Mosquitto client */
    struct mosquitto       *mosq;

    /* Publish tracking */
    kvw_publish_result_t    pending[KVW_MAX_PENDING];
    int                     pending_count;
    pthread_mutex_t         pending_lock;
    pthread_cond_t          pending_cond;   /* signalled when any mid completes */

    /* Connection state */
    volatile bool           connected;
    volatile bool           running;
    pthread_mutex_t         state_lock;
    pthread_cond_t          connect_cond;
    bool                    connect_done;
} kvw_store_writer_t;

/* ── API ────────────────────────────────────────────────────────────── */

/**
 * Initialise a default config struct.
 */
void kvw_config_init(kvw_config_t *cfg);

/**
 * Create and initialise a KVStoreWriter.
 * Returns 0 on success, -1 on failure.
 */
int kvw_init(kvw_store_writer_t *kv, const kvw_config_t *cfg);

/**
 * Connect to the MQTT broker.
 * Blocks up to `timeout_sec` seconds waiting for CONNACK.
 * Returns true on success.
 */
bool kvw_connect(kvw_store_writer_t *kv, double timeout_sec);

/**
 * Disconnect from the broker and stop the network loop.
 */
void kvw_disconnect(kvw_store_writer_t *kv);

/**
 * Destroy the writer (disconnect if needed, free resources).
 */
void kvw_destroy(kvw_store_writer_t *kv);

/**
 * Write a single key/value pair as a retained MQTT message.
 *
 * @param topic   MQTT topic (the "key")
 * @param value   Value string to store
 * @param qos     Quality of Service (0, 1, or 2)
 * @param retain  Whether to retain (should be true for KV store)
 * @param timeout_sec  Seconds to wait for publish confirmation
 * @return true if published successfully
 */
bool kvw_write_single(kvw_store_writer_t *kv,
                      const char *topic,
                      const char *value,
                      int qos,
                      bool retain,
                      double timeout_sec);

/**
 * Write multiple key/value pairs.
 *
 * All messages are published first (parallel), then confirmations are awaited.
 *
 * @param kv            Writer handle
 * @param count         Number of pairs
 * @param topics        Array of topic strings
 * @param values        Array of value strings
 * @param qos           QoS for all messages
 * @param retain        Retain flag for all messages
 * @param timeout_sec   Total timeout for all publishes
 * @param failed_topics If not NULL, receives pointers to failed topic strings
 * @return Number of successfully published messages
 */
int kvw_write_batch(kvw_store_writer_t *kv,
                    int count,
                    const char *topics[],
                    const char *values[],
                    int qos,
                    bool retain,
                    double timeout_sec,
                    const char *failed_topics[]);

/**
 * Delete a single key by publishing an empty retained message.
 */
bool kvw_delete_single(kvw_store_writer_t *kv,
                       const char *topic,
                       double timeout_sec);

/**
 * Delete multiple keys by publishing empty retained messages.
 *
 * @param kv            Writer handle
 * @param count         Number of topics to delete
 * @param topics        Array of topic strings
 * @param timeout_sec   Total timeout
 * @param failed_topics If not NULL, receives pointers to failed topic strings
 * @return Number of successfully deleted keys
 */
int kvw_delete_batch(kvw_store_writer_t *kv,
                     int count,
                     const char *topics[],
                     double timeout_sec,
                     const char *failed_topics[]);

/**
 * Update a key's value (convenience wrapper: write_single with retain=true).
 */
bool kvw_update_single(kvw_store_writer_t *kv,
                       const char *topic,
                       const char *value,
                       int qos,
                       double timeout_sec);

/**
 * Check if currently connected to the broker.
 */
bool kvw_is_connected(kvw_store_writer_t *kv);

#ifdef __cplusplus
}
#endif

#endif /* KV_STORE_WRITER_H */

