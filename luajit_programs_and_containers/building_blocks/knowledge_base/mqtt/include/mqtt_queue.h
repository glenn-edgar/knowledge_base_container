/**
 * mqtt_queue.h - MQTT Queue Publisher & Reader Library
 *
 * Reliable queued messaging over MQTT v3.1.1 using persistent sessions.
 * Wraps the Mosquitto C client library (libmosquitto).
 *
 * Features:
 *   - QueuePublisher: publish JSON messages (single + batch) with QoS 1/2
 *   - QueueReader:    persistent-session consumer with offline message queuing
 *
 * Requires: libmosquitto  (apt install libmosquitto-dev)
 *
 * Thread safety: each instance is internally synchronised via pthread mutexes.
 */

#ifndef MQTT_QUEUE_H
#define MQTT_QUEUE_H

#include <stdbool.h>
#include <stddef.h>
#include <pthread.h>
#include <mosquitto.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ------------------------------------------------------------------ */
/*  Common configuration                                               */
/* ------------------------------------------------------------------ */

typedef struct {
    const char *host;       /* broker hostname, default "localhost"    */
    int         port;       /* broker port,     default 1883          */
    const char *client_id;  /* MQTT client id (must be fixed for persistent sessions) */
    int         keepalive;  /* keepalive seconds, default 60          */
    const char *username;   /* optional auth username (NULL to skip)  */
    const char *password;   /* optional auth password                 */
    bool        clean_session; /* false for persistent sessions       */
} mqtt_queue_config_t;

/* Sensible defaults (clean_session = true for publisher, false for reader) */
#define MQTT_QUEUE_CONFIG_DEFAULTS { \
    .host = "localhost", .port = 1883, .client_id = NULL, \
    .keepalive = 60, .username = NULL, .password = NULL, \
    .clean_session = true }

/* ------------------------------------------------------------------ */
/*  Received message (reader side)                                     */
/* ------------------------------------------------------------------ */

typedef struct mqtt_msg {
    char *topic;
    char *payload;
    struct mqtt_msg *next;   /* singly-linked list */
} mqtt_msg_t;

/* Free a linked list of messages */
void mqtt_msg_list_free(mqtt_msg_t *head);

/* ------------------------------------------------------------------ */
/*  QueuePublisher                                                     */
/* ------------------------------------------------------------------ */

typedef struct {
    struct mosquitto *mosq;
    mqtt_queue_config_t cfg;
    bool        connected;
    pthread_mutex_t lock;
    pthread_cond_t  connect_cond;
} mqtt_publisher_t;

/** Initialise a publisher (call once). Returns 0 on success. */
int  mqtt_publisher_init(mqtt_publisher_t *pub, const mqtt_queue_config_t *cfg);

/** Connect to broker. Returns 0 on success. timeout_ms = max wait. */
int  mqtt_publisher_connect(mqtt_publisher_t *pub, int timeout_ms);

/** Disconnect from broker. */
void mqtt_publisher_disconnect(mqtt_publisher_t *pub);

/** Destroy publisher and free resources. */
void mqtt_publisher_destroy(mqtt_publisher_t *pub);

/**
 * Publish a single JSON string to 'topic'.
 * The caller is responsible for serialising JSON; this function publishes
 * the raw payload bytes.  Returns 0 on success.
 */
int  mqtt_publisher_publish(mqtt_publisher_t *pub,
                            const char *topic,
                            const char *json_payload,
                            int qos,
                            bool retain);

/**
 * Publish an array of JSON strings (batch).
 * Returns the number of successfully published messages.
 */
int  mqtt_publisher_publish_batch(mqtt_publisher_t *pub,
                                  const char *topic,
                                  const char **json_payloads,
                                  int count,
                                  int qos,
                                  bool retain,
                                  int delay_between_ms);

bool mqtt_publisher_is_connected(const mqtt_publisher_t *pub);

/* ------------------------------------------------------------------ */
/*  QueueReader                                                        */
/* ------------------------------------------------------------------ */

typedef struct {
    struct mosquitto *mosq;
    mqtt_queue_config_t cfg;
    bool        connected;
    bool        session_present;

    /* Incoming message buffer (linked list, guarded by lock) */
    mqtt_msg_t *msg_head;
    mqtt_msg_t *msg_tail;
    int         msg_count;

    /* Synchronisation */
    pthread_mutex_t lock;
    pthread_cond_t  connect_cond;
    pthread_cond_t  subscribe_cond;
    bool            subscribe_ack;
    int             subscribe_mid;  /* mid we're waiting for */
} mqtt_reader_t;

/** Initialise a reader (call once). Returns 0 on success. */
int  mqtt_reader_init(mqtt_reader_t *rdr, const mqtt_queue_config_t *cfg);

/** Connect to broker. Returns 0 on success. */
int  mqtt_reader_connect(mqtt_reader_t *rdr, int timeout_ms);

/** Disconnect from broker. */
void mqtt_reader_disconnect(mqtt_reader_t *rdr);

/** Destroy reader and free resources. */
void mqtt_reader_destroy(mqtt_reader_t *rdr);

/**
 * Subscribe and block until SUBACK or timeout.
 * Returns 0 on success.
 */
int  mqtt_reader_subscribe(mqtt_reader_t *rdr,
                           const char *topic,
                           int qos,
                           int timeout_ms);

/**
 * Collect messages for up to timeout_ms milliseconds.
 * If the session was already present, skips re-subscribing.
 * Returns a linked list of messages (caller must free with mqtt_msg_list_free).
 * *out_count receives the number of messages.
 */
mqtt_msg_t *mqtt_reader_read_queue(mqtt_reader_t *rdr,
                                   const char *topic,
                                   int qos,
                                   int timeout_ms,
                                   int *out_count);

/** Clear any buffered messages. */
void mqtt_reader_clear(mqtt_reader_t *rdr);

bool mqtt_reader_is_connected(const mqtt_reader_t *rdr);

/* ------------------------------------------------------------------ */
/*  Library-level init / cleanup (call once per process)               */
/* ------------------------------------------------------------------ */

/** Call before any other mqtt_queue function. */
void mqtt_queue_lib_init(void);

/** Call at program exit. */
void mqtt_queue_lib_cleanup(void);

#ifdef __cplusplus
}
#endif

#endif /* MQTT_QUEUE_H */

