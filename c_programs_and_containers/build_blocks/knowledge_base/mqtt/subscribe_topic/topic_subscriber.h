/**
 * topic_subscriber.h - Subscribes to MQTT topics and issues callbacks for received messages.
 *
 * Translated from Python TopicSubscriber class.
 * Requires: libmosquitto (mosquitto-dev)
 *   apt install libmosquitto-dev
 *   Link with: -lmosquitto -lpthread
 */

 #ifndef TOPIC_SUBSCRIBER_H
 #define TOPIC_SUBSCRIBER_H
 
 #include <stdbool.h>
 #include <stdint.h>
 #include <time.h>
 #include <pthread.h>
 
 #ifdef __cplusplus
 extern "C" {
 #endif
 
 /* ── Limits ─────────────────────────────────────────────────────────── */
 
 #define TS_MAX_TOPIC_LEN        256
 #define TS_MAX_PAYLOAD_LEN      65536
 #define TS_MAX_SUBSCRIPTIONS    64
 #define TS_MAX_CALLBACKS_PER_TOPIC 8
 #define TS_MAX_CLIENT_ID_LEN    128
 
 /* ── Message Info ───────────────────────────────────────────────────── */
 
 typedef struct {
     char        topic[TS_MAX_TOPIC_LEN];
     char        payload[TS_MAX_PAYLOAD_LEN];    /* Null-terminated string copy */
     uint8_t    *raw_payload;                     /* Raw bytes (caller must NOT free) */
     int         raw_payload_len;
     int         qos;
     bool        retain;
     struct timespec timestamp;
 } ts_message_info_t;
 
 /* Callback signature */
 typedef void (*ts_message_callback_t)(const ts_message_info_t *msg, void *user_data);
 
 /* ── Callback Entry ─────────────────────────────────────────────────── */
 
 typedef struct {
     ts_message_callback_t fn;
     void                 *user_data;
 } ts_callback_entry_t;
 
 /* ── Subscription Entry ─────────────────────────────────────────────── */
 
 typedef struct {
     char                topic[TS_MAX_TOPIC_LEN];
     int                 qos;
     ts_callback_entry_t callbacks[TS_MAX_CALLBACKS_PER_TOPIC];
     int                 callback_count;
     bool                active;
 } ts_subscription_t;
 
 /* ── Statistics ──────────────────────────────────────────────────────── */
 
 typedef struct {
     uint64_t        messages_received;
     uint64_t        callbacks_executed;
     uint64_t        errors;
     struct timespec  last_message_time;
     bool            has_last_message;
 } ts_stats_t;
 
 /* ── Configuration ──────────────────────────────────────────────────── */
 
 typedef struct {
     char    host[256];
     int     port;
     char    client_id[TS_MAX_CLIENT_ID_LEN];
     int     keepalive;
     bool    use_mqttv5;
     char    username[128];
     char    password[128];
     bool    has_credentials;
     bool    auto_reconnect;
     double  reconnect_delay;    /* seconds */
     bool    clean_session;
 } ts_config_t;
 
 /* ── TopicSubscriber Handle ─────────────────────────────────────────── */
 
 typedef struct {
     /* Configuration */
     ts_config_t         config;
 
     /* Mosquitto client */
     struct mosquitto   *mosq;
 
     /* Subscriptions */
     ts_subscription_t   subscriptions[TS_MAX_SUBSCRIPTIONS];
     int                 subscription_count;
     pthread_mutex_t     subscriptions_lock;
 
     /* State */
     volatile bool       connected;
     volatile bool       running;
     pthread_mutex_t     state_lock;
     pthread_cond_t      connect_cond;
     bool                connect_done;       /* signalled by on_connect */
 
     /* Statistics */
     ts_stats_t          stats;
     pthread_mutex_t     stats_lock;
 
     /* Reconnect timer thread */
     pthread_t           reconnect_thread;
     bool                reconnect_thread_active;
 } topic_subscriber_t;
 
 /* ── API ────────────────────────────────────────────────────────────── */
 
 /**
  * Initialise a default config struct.
  */
 void ts_config_init(ts_config_t *cfg);
 
 /**
  * Create and initialise a TopicSubscriber.
  * Returns 0 on success, -1 on failure.
  */
 int ts_init(topic_subscriber_t *ts, const ts_config_t *cfg);
 
 /**
  * Connect to the MQTT broker.
  * Blocks up to `timeout_sec` seconds waiting for CONNACK.
  * Returns true on success.
  */
 bool ts_connect(topic_subscriber_t *ts, double timeout_sec);
 
 /**
  * Disconnect from the broker and clean up the network loop.
  */
 void ts_disconnect(topic_subscriber_t *ts);
 
 /**
  * Destroy the subscriber (disconnect if needed, free resources).
  */
 void ts_destroy(topic_subscriber_t *ts);
 
 /**
  * Subscribe to a topic with a callback.
  * If `replace` is true, replaces all existing callbacks for that topic.
  * Returns true on success.
  */
 bool ts_subscribe(topic_subscriber_t *ts,
                   const char *topic,
                   ts_message_callback_t callback,
                   void *user_data,
                   int qos,
                   bool replace);
 
 /**
  * Subscribe to multiple topics at once.
  * `topics`, `callbacks`, `user_datas`, `qos_values` are parallel arrays of length `count`.
  * Returns the number of successful subscriptions.
  * `failed_topics` (if not NULL) receives the names of failed topics (caller provides array of `count` char pointers).
  */
 int ts_subscribe_many(topic_subscriber_t *ts,
                       int count,
                       const char *topics[],
                       ts_message_callback_t callbacks[],
                       void *user_datas[],
                       const int qos_values[],
                       bool replace,
                       const char *failed_topics[]);
 
 /**
  * Unsubscribe from a topic and remove all its callbacks.
  */
 bool ts_unsubscribe(topic_subscriber_t *ts, const char *topic);
 
 /**
  * Unsubscribe from all topics. Returns the number of topics unsubscribed.
  */
 int ts_unsubscribe_all(topic_subscriber_t *ts);
 
 /**
  * Add an additional callback to an existing subscription.
  */
 bool ts_add_callback(topic_subscriber_t *ts,
                      const char *topic,
                      ts_message_callback_t callback,
                      void *user_data);
 
 /**
  * Remove a specific callback from a topic (matched by function pointer).
  */
 bool ts_remove_callback(topic_subscriber_t *ts,
                         const char *topic,
                         ts_message_callback_t callback);
 
 /**
  * Get a snapshot of current subscriptions.
  * Fills `topics` and `qos_values` arrays (caller provides, up to `max_count`).
  * Returns the number of active subscriptions written.
  */
 int ts_get_subscriptions(topic_subscriber_t *ts,
                          char topics[][TS_MAX_TOPIC_LEN],
                          int qos_values[],
                          int max_count);
 
 /**
  * Get a copy of the current statistics.
  */
 void ts_get_statistics(topic_subscriber_t *ts, ts_stats_t *out);
 
 /**
  * Check if currently connected to the broker.
  */
 bool ts_is_connected(topic_subscriber_t *ts);
 
 /**
  * Block and wait for messages.
  * If `timeout_sec` > 0, waits for that many seconds then returns.
  * If `timeout_sec` <= 0, blocks until `ts_disconnect()` is called or SIGINT.
  */
 void ts_wait_for_messages(topic_subscriber_t *ts, double timeout_sec);
 
 #ifdef __cplusplus
 }
 #endif
 
 #endif /* TOPIC_SUBSCRIBER_H */