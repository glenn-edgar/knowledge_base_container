/**
 * mqtt_pubsub.h - JSON-pubsub 2.0 over MQTT (v3.1.1)
 *
 * Provides an pubsub server and client that exchange JSON-pubsub 2.0 messages
 * over MQTT topics.  Wraps libmosquitto + cJSON.
 *
 * Topic layout:
 *   request:  pubsub/{service}/request/{caller_client_id}
 *   response: pubsub/{service}/response/{caller_client_id}
 *
 * Requires: libmosquitto  (apt install libmosquitto-dev)
 *           cJSON          (apt install libcjson-dev)
 *
 * Thread safety: each instance is internally synchronised via pthreads.
 */

 #ifndef MQTT_PUBSUB_H
 #define MQTT_PUBSUB_H
 
 #include <stdbool.h>
 #include <stddef.h>
 #include <pthread.h>
 #include <mosquitto.h>
 #include <cjson/cJSON.h>
 
 #ifdef __cplusplus
 extern "C" {
 #endif
 
 /* ------------------------------------------------------------------ */
 /*  Configuration (shared by server and client)                        */
 /* ------------------------------------------------------------------ */
 
 typedef struct {
     const char *host;           /* broker hostname  (default "localhost") */
     int         port;           /* broker port      (default 1883)       */
     const char *client_id;      /* NULL = auto-generate                  */
     const char *service_name;   /* pubsub namespace    (default "pubsub_service") */
     int         keepalive;      /* seconds          (default 60)         */
     const char *username;       /* optional auth    (NULL to skip)       */
     const char *password;
     int         qos;            /* subscribe/publish QoS (default 1)     */
 } mqtt_pubsub_config_t;
 
 #define MQTT_PUBSUB_CONFIG_DEFAULTS { \
     .host = "localhost", .port = 1883, .client_id = NULL, \
     .service_name = "pubsub_service", .keepalive = 60, \
     .username = NULL, .password = NULL, .qos = 1 }
 
 /* ------------------------------------------------------------------ */
 /*  JSON-pubsub error codes                                               */
 /* ------------------------------------------------------------------ */
 
 #define JSONPUBSUB_PARSE_ERROR      -32700
 #define JSONPUBSUB_INVALID_REQUEST  -32600
 #define JSONPUBSUB_METHOD_NOT_FOUND -32601
 #define JSONPUBSUB_INVALID_PARAMS   -32602
 #define JSONPUBSUB_INTERNAL_ERROR   -32603
 
 /* ------------------------------------------------------------------ */
 /*  pubsub Server                                                         */
 /* ------------------------------------------------------------------ */
 
 /**
  * Method handler signature.
  *
  * @param params   cJSON object (object or array) — borrowed, do NOT free.
  *                 May be NULL if the caller sent no params.
  * @param userdata Opaque pointer registered alongside the method.
  * @return         A *new* cJSON value to be used as "result" in the
  *                 JSON-pubsub response.  The library will cJSON_Delete it
  *                 after serialising.  Return NULL to indicate an error.
  */
 typedef cJSON *(*pubsub_method_fn)(const cJSON *params, void *userdata);
 
 /** Single registered method entry. */
 typedef struct pubsub_method_entry {
     char                    *name;
     pubsub_method_fn            fn;
     void                    *userdata;
     struct pubsub_method_entry *next;
 } pubsub_method_entry_t;
 
 /** pubsub Server handle. */
 typedef struct {
     struct mosquitto   *mosq;
     mqtt_pubsub_config_t   cfg;
     char               *client_id;       /* owned copy (may be auto-generated) */
     char               *request_topic;   /* "pubsub/{svc}/request/+" */
     char               *response_topic_base; /* "pubsub/{svc}/response" */
 
     pubsub_method_entry_t *methods;         /* linked list of registered methods */
 
     bool                connected;
     pthread_mutex_t     lock;
     pthread_cond_t      connect_cond;
     pthread_cond_t      subscribe_cond;
     bool                subscribed;
     bool                stop_flag;
 } pubsub_server_t;
 
 /** Initialise a server. Returns 0 on success. */
 int  pubsub_server_init(pubsub_server_t *srv, const mqtt_pubsub_config_t *cfg);
 
 /** Register a single method. */
 void pubsub_server_register(pubsub_server_t *srv, const char *name,
                          pubsub_method_fn fn, void *userdata);
 
 /**
  * Connect, subscribe, and start the network loop.
  * wait_for_suback: if true, blocks until SUBACK or sub_timeout_ms.
  * Returns 0 on success.
  */
 int  pubsub_server_start(pubsub_server_t *srv, bool wait_for_suback, int sub_timeout_ms);
 
 /** Block the calling thread until pubsub_server_stop() is called. */
 void pubsub_server_wait(pubsub_server_t *srv);
 
 /** Signal the server to stop and disconnect. */
 void pubsub_server_stop(pubsub_server_t *srv);
 
 /** Destroy server and free all resources. */
 void pubsub_server_destroy(pubsub_server_t *srv);
 
 /* ------------------------------------------------------------------ */
 /*  pubsub Client                                                         */
 /* ------------------------------------------------------------------ */
 
 /** Pending-request slot. */
 typedef struct pubsub_pending {
     char               *id;          /* request id string */
     cJSON              *response;    /* filled by on_message */
     pthread_cond_t      cond;
     bool                done;
     struct pubsub_pending  *next;
 } pubsub_pending_t;
 
 /** Async callback signature: (error_json_or_NULL, result_json_or_NULL, userdata). */
 typedef void (*pubsub_async_cb)(const cJSON *error, const cJSON *result, void *userdata);
 
 /** pubsub Client handle. */
 typedef struct {
     struct mosquitto   *mosq;
     mqtt_pubsub_config_t   cfg;
     char               *client_id;          /* owned */
     char               *request_topic;      /* "pubsub/{svc}/request/{client_id}" */
     char               *response_topic;     /* "pubsub/{svc}/response/{client_id}" */
 
     pubsub_pending_t      *pending_head;       /* linked list of pending requests */
     int                 request_counter;
 
     bool                connected;
     pthread_mutex_t     lock;
     pthread_cond_t      connect_cond;
     pthread_cond_t      subscribe_cond;
     bool                subscribed;
     float               default_timeout_s;
 } pubsub_client_t;
 
 /**
  * Initialise a client. default_timeout_s is used by pubsub_client_call
  * when no per-call timeout is given (pass 0 → 30 s).
  * Returns 0 on success.
  */
 int  pubsub_client_init(pubsub_client_t *cli, const mqtt_pubsub_config_t *cfg,
                      float default_timeout_s);
 
 /**
  * Connect, subscribe to response topic, start network loop.
  * Returns 0 on success.
  */
 int  pubsub_client_connect(pubsub_client_t *cli, int timeout_ms);
 
 /** Disconnect and stop loop. */
 void pubsub_client_disconnect(pubsub_client_t *cli);
 
 /** Destroy client and free resources (also frees any pending requests). */
 void pubsub_client_destroy(pubsub_client_t *cli);
 
 /**
  * Synchronous pubsub call.
  *
  * @param method     Method name.
  * @param params     cJSON params (object or array). Borrowed; caller keeps
  *                   ownership.  May be NULL.
  * @param timeout_s  Per-call timeout (0 = use default).
  * @param out_result Receives the "result" cJSON value on success.
  *                   Caller must cJSON_Delete when done.
  * @param out_error  Receives the "error" cJSON value on pubsub error.
  *                   Caller must cJSON_Delete when done.
  *
  * @return  0  on success (*out_result set, *out_error = NULL)
  *         -1  on transport/timeout error (neither set)
  *         -2  on JSON-pubsub error (*out_error set, *out_result = NULL)
  */
 int  pubsub_client_call(pubsub_client_t *cli,
                      const char *method,
                      const cJSON *params,
                      float timeout_s,
                      cJSON **out_result,
                      cJSON **out_error);
 
 /**
  * Asynchronous pubsub call — fires a request, invokes callback from a
  * background thread when the response arrives (or on timeout).
  * Returns the request-id string (caller must free).
  */
 char *pubsub_client_call_async(pubsub_client_t *cli,
                             const char *method,
                             const cJSON *params,
                             float timeout_s,
                             pubsub_async_cb cb,
                             void *cb_userdata);
 
 /* ------------------------------------------------------------------ */
 /*  Library-level init / cleanup  (call once per process)              */
 /* ------------------------------------------------------------------ */
 
 void mqtt_pubsub_lib_init(void);
 void mqtt_pubsub_lib_cleanup(void);
 
 #ifdef __cplusplus
 }
 #endif
 
 #endif /* MQTT_pubsub_H */