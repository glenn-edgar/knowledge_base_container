/**
 * @file nats_rpc.h
 * @brief NATS-based Remote Procedure Call (RPC) library — C API
 *
 * Translated from Python NAS_RPC.  Uses the nats.c library for transport.
 *
 * Features:
 *   - Request / response RPC with JSON payloads (cJSON)
 *   - Synchronous API (nats.c handles threading internally)
 *   - Namespace-prefixed subjects
 *   - Instance-specific and load-balanced method routing
 *   - Built-in health-check endpoint
 *   - Batch (parallel) calls
 *   - Per-handler call / error counters
 *   - Configurable timeout
 *
 * Typical usage — server side:
 *   RpcServer *srv;
 *   rpc_server_create(&srv, &cfg);
 *   rpc_server_register(srv, "math.add", add_handler, NULL);
 *   rpc_server_start(srv, "rpc");          // blocks or call in a thread
 *   ...
 *   rpc_server_stop(srv);
 *   rpc_server_destroy(srv);
 *
 * Typical usage — client side:
 *   RpcClient *cli;
 *   rpc_client_create(&cli, &cfg);
 *   rpc_client_connect(cli);
 *   char *result = NULL;
 *   rpc_client_call(cli, "rpc.math.add", "{\"a\":5,\"b\":3}", 5.0, &result);
 *   free(result);
 *   rpc_client_disconnect(cli);
 *   rpc_client_destroy(cli);
 */

#ifndef NATS_RPC_H
#define NATS_RPC_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include <nats/nats.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ------------------------------------------------------------------ */
/*  Status codes                                                       */
/* ------------------------------------------------------------------ */

typedef enum {
    RPC_OK = 0,
    RPC_ERR_INVALID_ARG,
    RPC_ERR_CONNECTION,
    RPC_ERR_TIMEOUT,
    RPC_ERR_ENCODE,
    RPC_ERR_DECODE,
    RPC_ERR_MEMORY,
    RPC_ERR_HANDLER,        /**< Handler returned an error            */
    RPC_ERR_NOT_FOUND,      /**< No pending request / no handler      */
    RPC_ERR_NATS,           /**< Generic nats.c error                 */
} rpc_status_t;

const char *rpc_status_str(rpc_status_t st);

/* ------------------------------------------------------------------ */
/*  Configuration (shared by server and client)                        */
/* ------------------------------------------------------------------ */

typedef struct {
    const char *server;          /**< e.g. "nats://127.0.0.1:4222"    */
    const char *namespace_;      /**< Subject prefix (default "default")*/
    const char *instance_id;     /**< Unique id (auto-generated if NULL)*/
    bool        enable_health;   /**< Register _health handler (server)*/
} RpcConfig;

/** Fill cfg with sensible defaults. */
void rpc_config_defaults(RpcConfig *cfg);

/* ------------------------------------------------------------------ */
/*  RPC Handler callback                                               */
/* ------------------------------------------------------------------ */

/**
 * User-supplied RPC handler function.
 *
 * @param params_json  JSON string of parameters (never NULL, may be "{}").
 * @param user_data    Opaque pointer registered with the handler.
 * @param[out] result_json  On success, set to a malloc'd JSON string.
 *                          On error, set to a malloc'd error message string.
 * @return RPC_OK on success.  Any other value sends an error response.
 */
typedef rpc_status_t (*rpc_handler_fn)(const char *params_json,
                                       void       *user_data,
                                       char      **result_json);

/* ------------------------------------------------------------------ */
/*  RPC Server                                                         */
/* ------------------------------------------------------------------ */

typedef struct RpcServer RpcServer;

/** Create an RPC server (does NOT connect). */
rpc_status_t rpc_server_create(RpcServer **out, const RpcConfig *cfg);

/** Destroy the server and free all resources. */
void rpc_server_destroy(RpcServer *srv);

/**
 * Register a method handler.
 *
 * @param method            Method name (e.g. "math.add").
 * @param handler           Callback invoked on each request.
 * @param user_data         Passed to handler on each call.
 * @param instance_specific If true, only this instance receives calls.
 */
rpc_status_t rpc_server_register(RpcServer    *srv,
                                 const char   *method,
                                 rpc_handler_fn handler,
                                 void         *user_data,
                                 bool          instance_specific);

/**
 * Start the server: connect, subscribe to all registered methods,
 * and begin handling requests.
 *
 * @param prefix  Optional subject prefix prepended to method names.
 *                Pass NULL or "" for no prefix.
 *
 * This function returns immediately; requests are handled on nats.c's
 * internal thread pool.  Call rpc_server_wait() to block until stopped.
 */
rpc_status_t rpc_server_start(RpcServer *srv, const char *prefix);

/**
 * Block until rpc_server_stop() is called (from another thread or
 * a signal handler).
 */
void rpc_server_wait(RpcServer *srv);

/** Stop the server and unsubscribe. */
rpc_status_t rpc_server_stop(RpcServer *srv);

/** Get the server's instance_id. */
const char *rpc_server_instance_id(const RpcServer *srv);

/** Check whether the server is running. */
bool rpc_server_is_running(const RpcServer *srv);

/* ------------------------------------------------------------------ */
/*  Server statistics                                                  */
/* ------------------------------------------------------------------ */

typedef struct {
    const char *method;
    int64_t     call_count;
    int64_t     error_count;
    bool        instance_specific;
} RpcHandlerStats;

/**
 * Get per-handler statistics.
 *
 * @param[out] stats  Caller must free() the array.  Strings inside
 *                    point into the server and are valid until the
 *                    server is destroyed.
 * @param[out] count  Number of entries.
 */
rpc_status_t rpc_server_get_stats(const RpcServer *srv,
                                  RpcHandlerStats **stats,
                                  size_t *count);

/* ------------------------------------------------------------------ */
/*  RPC Client                                                         */
/* ------------------------------------------------------------------ */

typedef struct RpcClient RpcClient;

/** Create an RPC client (does NOT connect). */
rpc_status_t rpc_client_create(RpcClient **out, const RpcConfig *cfg);

/** Destroy the client. */
void rpc_client_destroy(RpcClient *cli);

/** Connect to the NATS server. */
rpc_status_t rpc_client_connect(RpcClient *cli);

/** Disconnect from the NATS server. */
rpc_status_t rpc_client_disconnect(RpcClient *cli);

/** Check whether the client is connected. */
bool rpc_client_is_connected(const RpcClient *cli);

/** Get the client's instance_id. */
const char *rpc_client_instance_id(const RpcClient *cli);

/**
 * Make a synchronous RPC call.
 *
 * @param method          Fully-qualified method (namespace is prepended
 *                        automatically).  e.g. "rpc.math.add"
 * @param params_json     JSON parameters string (may be NULL → "{}").
 * @param timeout_sec     Seconds to wait for a response.
 * @param[out] result_json  On success, receives a malloc'd JSON string
 *                          containing the result.  Caller must free().
 *                          On RPC_ERR_HANDLER, receives the error message.
 *
 * @return RPC_OK on success.
 */
rpc_status_t rpc_client_call(RpcClient  *cli,
                             const char *method,
                             const char *params_json,
                             double      timeout_sec,
                             char      **result_json);

/**
 * Call targeting a specific server instance.
 *
 * @param target_instance  The instance_id of the target server.
 *                         The method must have been registered with
 *                         instance_specific = true on that server.
 */
rpc_status_t rpc_client_call_instance(RpcClient  *cli,
                                      const char *method,
                                      const char *params_json,
                                      double      timeout_sec,
                                      const char *target_instance,
                                      char      **result_json);

/* ------------------------------------------------------------------ */
/*  Batch calls                                                        */
/* ------------------------------------------------------------------ */

typedef struct {
    const char *method;          /**< Method name                      */
    const char *params_json;     /**< JSON params (NULL → "{}")        */
    const char *target_instance; /**< NULL for load-balanced           */
} RpcBatchEntry;

typedef struct {
    rpc_status_t  status;        /**< Per-call status                  */
    char         *result_json;   /**< malloc'd result (caller frees)   */
} RpcBatchResult;

/**
 * Execute multiple RPC calls in parallel.
 *
 * @param entries     Array of call descriptors.
 * @param count       Number of entries.
 * @param timeout_sec Timeout for the entire batch.
 * @param[out] results  Caller-allocated array of count RpcBatchResult.
 *                      Each result_json that is non-NULL must be free()'d.
 */
rpc_status_t rpc_client_call_batch(RpcClient        *cli,
                                   const RpcBatchEntry *entries,
                                   size_t              count,
                                   double              timeout_sec,
                                   RpcBatchResult     *results);

#ifdef __cplusplus
}
#endif

#endif /* NATS_RPC_H */

