/**
 * kv_store_reader.h - Reads retained key/value messages from an MQTT broker.
 *
 * Translated from Python KVStoreReader class.
 * Requires: libmosquitto (mosquitto-dev)
 *   apt install libmosquitto-dev
 *   Link with: -lmosquitto -lpthread
 */

 #ifndef KV_STORE_READER_H
 #define KV_STORE_READER_H
 
 #include <stdbool.h>
 #include <stdint.h>
 #include <time.h>
 #include <pthread.h>
 
 #ifdef __cplusplus
 extern "C" {
 #endif
 
 /* ── Limits ─────────────────────────────────────────────────────────── */
 
 #define KVR_MAX_TOPIC_LEN       256
 #define KVR_MAX_VALUE_LEN       4096
 #define KVR_MAX_ENTRIES         256     /* max retained KV pairs collected */
 #define KVR_MAX_SENTINELS       8       /* max sentinel topics per read */
 #define KVR_MAX_CLIENT_ID_LEN   128
 
 /* ── KV Entry ───────────────────────────────────────────────────────── */
 
 typedef struct {
     char    topic[KVR_MAX_TOPIC_LEN];
     char    value[KVR_MAX_VALUE_LEN];
     bool    active;
 } kvr_entry_t;
 
 /* ── Configuration ──────────────────────────────────────────────────── */
 
 typedef struct {
     char    host[256];
     int     port;
     char    client_id[KVR_MAX_CLIENT_ID_LEN];
     int     keepalive;
     bool    use_mqttv5;
     char    username[128];
     char    password[128];
     bool    has_credentials;
     bool    clean_session;
 } kvr_config_t;
 
 /* ── KVStoreReader Handle ───────────────────────────────────────────── */
 
 typedef struct {
     /* Configuration */
     kvr_config_t            config;
 
     /* Mosquitto client */
     struct mosquitto       *mosq;
 
     /* Collected KV entries (populated during read_pattern) */
     kvr_entry_t             entries[KVR_MAX_ENTRIES];
     int                     entry_count;
     pthread_mutex_t         entries_lock;
 
     /* Sentinel coordination */
     char                    sentinels[KVR_MAX_SENTINELS][KVR_MAX_TOPIC_LEN];
     int                     sentinel_count;
     pthread_mutex_t         sentinel_lock;
     pthread_cond_t          sentinel_cond;
     bool                    sentinel_fired;
 
     /* Subscribe acknowledgement */
     pthread_mutex_t         sub_lock;
     pthread_cond_t          sub_cond;
     bool                    sub_acked;
 
     /* Connection state */
     volatile bool           connected;
     volatile bool           running;
     pthread_mutex_t         state_lock;
     pthread_cond_t          connect_cond;
     bool                    connect_done;
 } kvr_store_reader_t;
 
 /* ── API ────────────────────────────────────────────────────────────── */
 
 /**
  * Initialise a default config struct.
  */
 void kvr_config_init(kvr_config_t *cfg);
 
 /**
  * Create and initialise a KVStoreReader.
  * Returns 0 on success, -1 on failure.
  */
 int kvr_init(kvr_store_reader_t *kv, const kvr_config_t *cfg);
 
 /**
  * Connect to the MQTT broker.
  * Blocks up to `timeout_sec` seconds waiting for CONNACK.
  */
 bool kvr_connect(kvr_store_reader_t *kv, double timeout_sec);
 
 /**
  * Disconnect from the broker and stop the network loop.
  */
 void kvr_disconnect(kvr_store_reader_t *kv);
 
 /**
  * Destroy the reader (disconnect if needed, free resources).
  */
 void kvr_destroy(kvr_store_reader_t *kv);
 
 /**
  * Read retained messages matching a topic pattern.
  *
  * Subscribes to `pattern`, collects retained messages until either a sentinel
  * topic is received or `timeout_sec` elapses.
  *
  * @param kv                Reader handle
  * @param pattern           MQTT topic pattern (may include + and # wildcards)
  * @param qos               QoS for subscription
  * @param timeout_sec       Seconds to wait for retained messages
  * @param sentinel_topics   NULL-terminated array of sentinel topic strings, or NULL
  * @param wait_for_sentinel If true and sentinels given, return early on sentinel
  * @param out_entries       Caller-provided array to receive results
  * @param max_entries       Size of out_entries array
  * @return Number of entries written to out_entries
  */
 int kvr_read_pattern(kvr_store_reader_t *kv,
                      const char *pattern,
                      int qos,
                      double timeout_sec,
                      const char *sentinel_topics[],
                      bool wait_for_sentinel,
                      kvr_entry_t *out_entries,
                      int max_entries);
 
 /**
  * Read a single retained value for an exact topic.
  *
  * @param kv            Reader handle
  * @param topic         Exact MQTT topic (no wildcards)
  * @param timeout_sec   Seconds to wait
  * @param out_value     Buffer to receive the value
  * @param out_value_len Size of out_value buffer
  * @return true if a value was found, false otherwise
  */
 bool kvr_read_single(kvr_store_reader_t *kv,
                      const char *topic,
                      double timeout_sec,
                      char *out_value,
                      int out_value_len);
 
 /**
  * Read all retained messages under a base topic (default "#").
  * Convenience wrapper around kvr_read_pattern.
  */
 int kvr_read_all(kvr_store_reader_t *kv,
                  const char *base_topic,
                  double timeout_sec,
                  const char *sentinel_topics[],
                  bool wait_for_sentinel,
                  kvr_entry_t *out_entries,
                  int max_entries);
 
 /**
  * Check if currently connected to the broker.
  */
 bool kvr_is_connected(kvr_store_reader_t *kv);
 
 #ifdef __cplusplus
 }
 #endif
 
 #endif /* KV_STORE_READER_H */