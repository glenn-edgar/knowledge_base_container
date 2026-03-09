/**
 * mqtt_rpc.h - JSON-RPC 2.0 over MQTT (v3.1.1)
 *
 * Provides an RPC server and client that exchange JSON-RPC 2.0 messages
 * over MQTT topics.  Wraps libmosquitto + cJSON.
 *
 * Topic layout:
 *   request:  rpc/{service}/request/{caller_client_id}
 *   response: rpc/{service}/response/{caller_client_id}
 *
 * Requires: libmosquitto  (apt install libmosquitto-dev)
 *           cJSON          (apt install libcjson-dev)
 *
 * Thread safety: each instance is internally synchronised via pthreads.
 */

 #ifndef MQTT_RPC_H
 #define MQTT_RPC_H
 
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
     const char *service_name;   /* RPC namespace    (default "rpc_service") */
     int         keepalive;      /* seconds          (default 60)         */
     const char *username;       /* optional auth    (NULL to skip)       */
     const char *password;
     int         qos;            /* subscribe/publish QoS (default 1)     */
 } mqtt_rpc_config_t;
 
 #define MQTT_RPC_CONFIG_DEFAULTS { \
     .host = "localhost", .port = 1883, .client_id = NULL, \
     .service_name = "rpc_service", .keepalive = 60, \
     .username = NULL, .password = NULL, .qos = 1 }
 
 /* ------------------------------------------------------------------ */
 /*  JSON-RPC error codes                                               */
 /* ------------------------------------------------------------------ */
 
 #define JSONRPC_PARSE_ERROR      -32700
 #define JSONRPC_INVALID_REQUEST  -32600
 #define JSONRPC_METHOD_NOT_FOUND -32601
 #define JSONRPC_INVALID_PARAMS   -32602
 #define JSONRPC_INTERNAL_ERROR   -32603
 
 /* ------------------------------------------------------------------ */
 /*  RPC Server                                                         */
 /* ------------------------------------------------------------------ */
 
 /**
  * Method handler signature.
  *
  * @param params   cJSON object (object or array) — borrowed, do NOT free.
  *                 May be NULL if the caller sent no params.
  * @param userdata Opaque pointer registered alongside the method.
  * @return         A *new* cJSON value to be used as "result" in the
  *                 JSON-RPC response.  The library will cJSON_Delete it
  *                 after serialising.  Return NULL to indicate an error
  *                 (set *err_code / *err_msg via the alternate error-
  *                 returning signature — or just return NULL and the
  *                 library will send a generic internal-error).
  */
 typedef cJSON *(*rpc_method_fn)(const cJSON *params, void *userdata);
 
 /** Single registered method entry. */
 typedef struct rpc_method_entry {
     char                    *name;
     rpc_method_fn            fn;
     void                    *userdata;
     struct rpc_method_entry *next;
 } rpc_method_entry_t;
 
 /** RPC Server handle. */
 typedef struct {
     struct mosquitto   *mosq;
     mqtt_rpc_config_t   cfg;
     char               *client_id;       /* owned copy (may be auto-generated) */
     char               *request_topic;   /* "rpc/{svc}/request/+" */
     char               *response_topic_base; /* "rpc/{svc}/response" */
 
     rpc_method_entry_t *methods;         /* linked list of registered methods */
 
     bool                connected;
     pthread_mutex_t     lock;
     pthread_cond_t      connect_cond;
     pthread_cond_t      subscribe_cond;
     bool                subscribed;
     bool                stop_flag;
 } rpc_server_t;
 
 /** Initialise a server. Returns 0 on success. */
 int  rpc_server_init(rpc_server_t *srv, const mqtt_rpc_config_t *cfg);
 
 /** Register a single method. */
 void rpc_server_register(rpc_server_t *srv, const char *name,
                          rpc_method_fn fn, void *userdata);
 
 /**
  * Connect, subscribe, and start the network loop.
  * wait_for_suback: if true, blocks until SUBACK or sub_timeout_ms.
  * Returns 0 on success.
  */
 int  rpc_server_start(rpc_server_t *srv, bool wait_for_suback, int sub_timeout_ms);
 
 /** Block the calling thread until rpc_server_stop() is called or SIGINT. */
 void rpc_server_wait(rpc_server_t *srv);
 
 /** Signal the server to stop and disconnect. */
 void rpc_server_stop(rpc_server_t *srv);
 
 /** Destroy server and free all resources. */
 void rpc_server_destroy(rpc_server_t *srv);
 
 /* ------------------------------------------------------------------ */
 /*  RPC Client                                                         */
 /* ------------------------------------------------------------------ */
 
 /** Pending-request slot. */
 typedef struct rpc_pending {
     char               *id;          /* request id string */
     cJSON              *response;    /* filled by on_message */
     pthread_cond_t      cond;
     bool                done;
     struct rpc_pending  *next;
 } rpc_pending_t;
 
 /** Async callback signature: (error_json_or_NULL, result_json_or_NULL, userdata). */
 typedef void (*rpc_async_cb)(const cJSON *error, const cJSON *result, void *userdata);
 
 /** RPC Client handle. */
 typedef struct {
     struct mosquitto   *mosq;
     mqtt_rpc_config_t   cfg;
     char               *client_id;          /* owned */
     char               *request_topic;      /* "rpc/{svc}/request/{client_id}" */
     char               *response_topic;     /* "rpc/{svc}/response/{client_id}" */
 
     rpc_pending_t      *pending_head;       /* linked list of pending requests */
     int                 request_counter;
 
     bool                connected;
     pthread_mutex_t     lock;
     pthread_cond_t      connect_cond;
     pthread_cond_t      subscribe_cond;
     bool                subscribed;
     float               default_timeout_s;
 } rpc_client_t;
 
 /**
  * Initialise a client. default_timeout_s is used by rpc_client_call
  * when no per-call timeout is given (pass 0 → 30 s).
  * Returns 0 on success.
  */
 int  rpc_client_init(rpc_client_t *cli, const mqtt_rpc_config_t *cfg,
                      float default_timeout_s);
 
 /**
  * Connect, subscribe to response topic, start network loop.
  * Returns 0 on success.
  */
 int  rpc_client_connect(rpc_client_t *cli, int timeout_ms);
 
 /** Disconnect and stop loop. */
 void rpc_client_disconnect(rpc_client_t *cli);
 
 /** Destroy client and free resources (also frees any pending requests). */
 void rpc_client_destroy(rpc_client_t *cli);
 
 /**
  * Synchronous RPC call.
  *
  * @param method     Method name.
  * @param params     cJSON params (object or array). Borrowed; caller keeps
  *                   ownership.  May be NULL.
  * @param timeout_s  Per-call timeout (0 = use default).
  * @param out_result Receives the "result" cJSON value on success.
  *                   Caller must cJSON_Delete when done.
  * @param out_error  Receives the "error" cJSON value on RPC error.
  *                   Caller must cJSON_Delete when done.
  *
  * @return  0  on success (*out_result set, *out_error = NULL)
  *         -1  on transport/timeout error (neither set)
  *         -2  on JSON-RPC error (*out_error set, *out_result = NULL)
  */
 int  rpc_client_call(rpc_client_t *cli,
                      const char *method,
                      const cJSON *params,
                      float timeout_s,
                      cJSON **out_result,
                      cJSON **out_error);
 
 /**
  * Asynchronous RPC call — fires a request, invokes callback from a
  * background thread when the response arrives (or on timeout).
  * Returns the request-id string (caller must free).
  */
 char *rpc_client_call_async(rpc_client_t *cli,
                             const char *method,
                             const cJSON *params,
                             float timeout_s,
                             rpc_async_cb cb,
                             void *cb_userdata);
 
 /* ------------------------------------------------------------------ */
 /*  Library-level init / cleanup  (call once per process)              */
 /* ------------------------------------------------------------------ */
 
 void mqtt_rpc_lib_init(void);
 void mqtt_rpc_lib_cleanup(void);
 
 #ifdef __cplusplus
 }
 #endif
 
 #endif /* MQTT_RPC_H */
 
 