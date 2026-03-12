/**
 * @file nats_rpc.c
 * @brief NATS-based RPC library — implementation
 *
 * Translated from Python NAS_RPC.  Uses nats.c for transport and
 * cJSON for JSON encoding/decoding.
 */

 #define _GNU_SOURCE   /* strdup, clock_gettime */

 #include "nats_rpc.h"
 
 #include <cjson/cJSON.h>
 #include <inttypes.h>
 #include <stdio.h>
 #include <stdlib.h>
 #include <string.h>
 #include <time.h>
 #include <unistd.h>
 #include <pthread.h>
 
 /* ------------------------------------------------------------------ */
 /*  Constants                                                          */
 /* ------------------------------------------------------------------ */
 
 #define RPC_MAX_HANDLERS   128
 #define RPC_INSTANCE_ID_LEN 128
 #define RPC_SUBJECT_LEN     256
 
 /* ------------------------------------------------------------------ */
 /*  Status strings                                                     */
 /* ------------------------------------------------------------------ */
 
 static const char *status_strings[] = {
     "ok", "invalid_arg", "connection", "timeout", "encode",
     "decode", "memory", "handler_error", "not_found", "nats_error"
 };
 
 const char *rpc_status_str(rpc_status_t st)
 {
     if (st >= 0 && st <= RPC_ERR_NATS)
         return status_strings[st];
     return "unknown";
 }
 
 /* ------------------------------------------------------------------ */
 /*  Utility: timestamp                                                 */
 /* ------------------------------------------------------------------ */
 
 static void iso_now(char *buf, size_t len)
 {
     time_t t = time(NULL);
     struct tm tm;
     gmtime_r(&t, &tm);
     strftime(buf, len, "%Y-%m-%dT%H:%M:%SZ", &tm);
 }
 
 static double monotonic_ms(void)
 {
     struct timespec ts;
     clock_gettime(CLOCK_MONOTONIC, &ts);
     return (double)ts.tv_sec * 1000.0 + (double)ts.tv_nsec / 1e6;
 }
 
 /* ------------------------------------------------------------------ */
 /*  Utility: generate instance id                                      */
 /* ------------------------------------------------------------------ */
 
 static void gen_instance_id(char *buf, size_t len)
 {
     char hostname[64] = {0};
     gethostname(hostname, sizeof(hostname) - 1);
     pid_t pid = getpid();
     unsigned r = (unsigned)time(NULL) ^ (unsigned)clock();
     snprintf(buf, len, "%s_%d_%08x", hostname, (int)pid, r);
 }
 
 /* ------------------------------------------------------------------ */
 /*  Utility: build namespaced subject                                  */
 /* ------------------------------------------------------------------ */
 
 static void build_subject(char *buf, size_t len,
                           const char *ns, const char *prefix,
                           const char *method, const char *suffix)
 {
     /* _health and similar internal subjects skip namespace */
     if (method[0] == '_') {
         if (prefix && prefix[0])
             snprintf(buf, len, "%s.%s%s%s", prefix, method,
                      suffix ? "." : "", suffix ? suffix : "");
         else
             snprintf(buf, len, "%s%s%s", method,
                      suffix ? "." : "", suffix ? suffix : "");
     } else {
         if (prefix && prefix[0])
             snprintf(buf, len, "%s.%s.%s%s%s", ns, prefix, method,
                      suffix ? "." : "", suffix ? suffix : "");
         else
             snprintf(buf, len, "%s.%s%s%s", ns, method,
                      suffix ? "." : "", suffix ? suffix : "");
     }
 }
 
 /* ------------------------------------------------------------------ */
 /*  Handler registration entry                                         */
 /* ------------------------------------------------------------------ */
 
 typedef struct {
     char            method[128];
     rpc_handler_fn  handler;
     void           *user_data;
     bool            instance_specific;
     int64_t         call_count;
     int64_t         error_count;
     natsSubscription *sub;         /* nats subscription handle */
 } HandlerEntry;
 
 /* ================================================================== */
 /*  RPC Server                                                         */
 /* ================================================================== */
 
 /* Forward declare for health context */
 typedef struct RpcServer RpcServer;
 typedef struct { RpcServer *srv; } HealthCtx;
 
 struct RpcServer {
     /* Config */
     char              server[256];
     char              namespace_[128];
     char              instance_id[RPC_INSTANCE_ID_LEN];
     bool              enable_health;
 
     /* NATS connection */
     natsConnection   *conn;
     natsOptions      *opts;
 
     /* Handlers */
     HandlerEntry      handlers[RPC_MAX_HANDLERS];
     int               handler_count;
 
     /* State */
     bool              running;
     time_t            start_time;
     int64_t           total_requests;
     int64_t           total_errors;
 
     /* Prefix used in start */
     char              prefix[128];
 
     /* Mutex for counters */
     pthread_mutex_t   mu;
 
     /* Condition for wait/stop */
     pthread_mutex_t   wait_mu;
     pthread_cond_t    wait_cond;
 
     /* Health check context (embedded, not static) */
     HealthCtx         health_ctx;
 };
 
 /* Forward declaration */
 static void server_msg_handler(natsConnection *nc, natsSubscription *sub,
                                natsMsg *msg, void *closure);
 
 /* ---- Config defaults ---- */
 
 void rpc_config_defaults(RpcConfig *cfg)
 {
     if (!cfg) return;
     memset(cfg, 0, sizeof(*cfg));
     cfg->server       = "nats://127.0.0.1:4222";
     cfg->namespace_   = "default";
     cfg->instance_id  = NULL;
     cfg->enable_health = true;
 }
 
 /* ---- Create / Destroy ---- */
 
 rpc_status_t rpc_server_create(RpcServer **out, const RpcConfig *cfg)
 {
     if (!out || !cfg) return RPC_ERR_INVALID_ARG;
 
     RpcServer *srv = calloc(1, sizeof(*srv));
     if (!srv) return RPC_ERR_MEMORY;
 
     snprintf(srv->server, sizeof(srv->server), "%s",
              cfg->server ? cfg->server : "nats://127.0.0.1:4222");
     snprintf(srv->namespace_, sizeof(srv->namespace_), "%s",
              cfg->namespace_ ? cfg->namespace_ : "default");
 
     if (cfg->instance_id && cfg->instance_id[0]) {
         snprintf(srv->instance_id, sizeof(srv->instance_id), "%s",
                  cfg->instance_id);
     } else {
         gen_instance_id(srv->instance_id, sizeof(srv->instance_id));
     }
 
     srv->enable_health = cfg->enable_health;
     srv->start_time = time(NULL);
 
     pthread_mutex_init(&srv->mu, NULL);
     pthread_mutex_init(&srv->wait_mu, NULL);
     pthread_cond_init(&srv->wait_cond, NULL);
 
     *out = srv;
     return RPC_OK;
 }
 
 void rpc_server_destroy(RpcServer *srv)
 {
     if (!srv) return;
     if (srv->running) rpc_server_stop(srv);
     if (srv->conn) {
         natsConnection_Destroy(srv->conn);
         srv->conn = NULL;
     }
     if (srv->opts) {
         natsOptions_Destroy(srv->opts);
         srv->opts = NULL;
     }
     pthread_mutex_destroy(&srv->mu);
     pthread_mutex_destroy(&srv->wait_mu);
     pthread_cond_destroy(&srv->wait_cond);
     free(srv);
 }
 
 /* ---- Register ---- */
 
 rpc_status_t rpc_server_register(RpcServer *srv, const char *method,
                                  rpc_handler_fn handler, void *user_data,
                                  bool instance_specific)
 {
     if (!srv || !method || !handler) return RPC_ERR_INVALID_ARG;
     if (srv->handler_count >= RPC_MAX_HANDLERS) return RPC_ERR_MEMORY;
 
     HandlerEntry *e = &srv->handlers[srv->handler_count++];
     memset(e, 0, sizeof(*e));
     snprintf(e->method, sizeof(e->method), "%s", method);
     e->handler           = handler;
     e->user_data         = user_data;
     e->instance_specific = instance_specific;
 
     return RPC_OK;
 }
 
 /* ---- Built-in health check handler ---- */
 
 static rpc_status_t health_handler(const char *params_json,
                                    void *user_data, char **result_json)
 {
     (void)params_json;
     HealthCtx *ctx = user_data;
     RpcServer *srv = ctx->srv;
 
     time_t uptime = time(NULL) - srv->start_time;
 
     cJSON *obj = cJSON_CreateObject();
     if (!obj) return RPC_ERR_MEMORY;
 
     cJSON_AddStringToObject(obj, "status", "healthy");
     cJSON_AddStringToObject(obj, "instance_id", srv->instance_id);
     cJSON_AddStringToObject(obj, "namespace", srv->namespace_);
     cJSON_AddNumberToObject(obj, "uptime_seconds", (double)uptime);
     cJSON_AddNumberToObject(obj, "total_requests", (double)srv->total_requests);
     cJSON_AddNumberToObject(obj, "total_errors", (double)srv->total_errors);
     cJSON_AddNumberToObject(obj, "handler_count", srv->handler_count);
 
     char ts[32];
     iso_now(ts, sizeof(ts));
     cJSON_AddStringToObject(obj, "timestamp", ts);
 
     /* List registered methods */
     cJSON *methods = cJSON_AddArrayToObject(obj, "handlers");
     for (int i = 0; i < srv->handler_count; i++)
         cJSON_AddItemToArray(methods,
             cJSON_CreateString(srv->handlers[i].method));
 
     *result_json = cJSON_PrintUnformatted(obj);
     cJSON_Delete(obj);
     return *result_json ? RPC_OK : RPC_ERR_MEMORY;
 }
 
 /* ---- NATS message callback (runs on nats.c's thread) ---- */
 
 typedef struct {
     RpcServer    *srv;
     HandlerEntry *entry;
 } SubClosure;
 
 static void server_msg_handler(natsConnection *nc, natsSubscription *sub,
                                natsMsg *msg, void *closure)
 {
     (void)sub;
     SubClosure *sc = closure;
     RpcServer  *srv = sc->srv;
     HandlerEntry *entry = sc->entry;
 
     const char *reply = natsMsg_GetReply(msg);
     const char *data  = natsMsg_GetData(msg);
     int         dlen  = natsMsg_GetDataLength(msg);
 
     if (!reply || !data || dlen <= 0) {
         natsMsg_Destroy(msg);
         return;
     }
 
     /* Parse request JSON */
     cJSON *req = cJSON_ParseWithLength(data, (size_t)dlen);
     if (!req) {
         /* Send parse error */
         const char *err = "{\"id\":null,\"result\":null,"
                           "\"error\":{\"code\":-32700,"
                           "\"message\":\"Parse error\"}}";
         natsConnection_Publish(nc, reply, err, (int)strlen(err));
         natsMsg_Destroy(msg);
         return;
     }
 
     /* Extract request id */
     cJSON *id_item = cJSON_GetObjectItem(req, "id");
     const char *req_id = (id_item && cJSON_IsString(id_item))
                          ? id_item->valuestring : NULL;
 
     /* Extract params */
     cJSON *params = cJSON_GetObjectItem(req, "params");
     char *params_str = NULL;
     if (params)
         params_str = cJSON_PrintUnformatted(params);
     if (!params_str)
         params_str = strdup("{}");
 
     /* Call user handler */
     double t0 = monotonic_ms();
 
     char *result_str = NULL;
     rpc_status_t hst = entry->handler(params_str, entry->user_data, &result_str);
 
     double elapsed_ms = monotonic_ms() - t0;
 
     pthread_mutex_lock(&srv->mu);
     srv->total_requests++;
     entry->call_count++;
     if (hst != RPC_OK) {
         srv->total_errors++;
         entry->error_count++;
     }
     pthread_mutex_unlock(&srv->mu);
 
     /* Build response JSON */
     cJSON *resp = cJSON_CreateObject();
     if (req_id)
         cJSON_AddStringToObject(resp, "id", req_id);
     else
         cJSON_AddNullToObject(resp, "id");
 
     if (hst == RPC_OK) {
         /* Try to embed result as JSON object; fall back to string */
         cJSON *robj = result_str ? cJSON_Parse(result_str) : NULL;
         if (robj)
             cJSON_AddItemToObject(resp, "result", robj);
         else if (result_str)
             cJSON_AddStringToObject(resp, "result", result_str);
         else
             cJSON_AddNullToObject(resp, "result");
         cJSON_AddNullToObject(resp, "error");
     } else {
         cJSON_AddNullToObject(resp, "result");
         cJSON *err = cJSON_CreateObject();
         cJSON_AddNumberToObject(err, "code", -32603);
         cJSON_AddStringToObject(err, "message",
                                 result_str ? result_str : "Handler error");
         cJSON_AddItemToObject(resp, "error", err);
     }
 
     cJSON_AddStringToObject(resp, "instance_id", srv->instance_id);
     cJSON_AddNumberToObject(resp, "processing_time_ms", elapsed_ms);
 
     char ts[32];
     iso_now(ts, sizeof(ts));
     cJSON_AddStringToObject(resp, "timestamp", ts);
 
     char *resp_str = cJSON_PrintUnformatted(resp);
     if (resp_str) {
         natsConnection_Publish(nc, reply, resp_str, (int)strlen(resp_str));
         free(resp_str);
     }
 
     cJSON_Delete(resp);
     cJSON_Delete(req);
     free(params_str);
     free(result_str);
     natsMsg_Destroy(msg);
 }
 
 /* ---- Start / Stop / Wait ---- */
 
 /* ---- Start / Stop / Wait ---- */
 
 rpc_status_t rpc_server_start(RpcServer *srv, const char *prefix)
 {
     if (!srv) return RPC_ERR_INVALID_ARG;
     if (srv->running) return RPC_OK;
 
     snprintf(srv->prefix, sizeof(srv->prefix), "%s", prefix ? prefix : "");
 
     /* Create NATS options */
     natsStatus ns = natsOptions_Create(&srv->opts);
     if (ns != NATS_OK) return RPC_ERR_NATS;
 
     natsOptions_SetURL(srv->opts, srv->server);
 
     char client_name[256];
     snprintf(client_name, sizeof(client_name), "rpc_server_%s", srv->instance_id);
     natsOptions_SetName(srv->opts, client_name);
 
     /* Connect */
     ns = natsConnection_Connect(&srv->conn, srv->opts);
     if (ns != NATS_OK) {
         fprintf(stderr, "RPC server: failed to connect to %s: %s\n",
                 srv->server, natsStatus_GetText(ns));
         return RPC_ERR_CONNECTION;
     }
 
     printf("RPC server connected to %s\n", srv->server);
     printf("Instance: %s, Namespace: %s\n", srv->instance_id, srv->namespace_);
 
     /* Register health check if enabled */
     if (srv->enable_health) {
         srv->health_ctx.srv = srv;
         rpc_server_register(srv, "_health", health_handler,
                             &srv->health_ctx, true);
     }
 
     /* Subscribe to each handler */
     for (int i = 0; i < srv->handler_count; i++) {
         HandlerEntry *e = &srv->handlers[i];
 
         char subject[RPC_SUBJECT_LEN];
         build_subject(subject, sizeof(subject),
                       srv->namespace_, srv->prefix, e->method,
                       e->instance_specific ? srv->instance_id : NULL);
 
         /* Allocate closure (leaked intentionally — freed in destroy) */
         SubClosure *sc = malloc(sizeof(*sc));
         if (!sc) return RPC_ERR_MEMORY;
         sc->srv   = srv;
         sc->entry = e;
 
         ns = natsConnection_Subscribe(&e->sub, srv->conn, subject,
                                       server_msg_handler, sc);
         if (ns != NATS_OK) {
             fprintf(stderr, "RPC server: subscribe failed for %s: %s\n",
                     subject, natsStatus_GetText(ns));
             free(sc);
             return RPC_ERR_NATS;
         }
 
         const char *tag = e->instance_specific ? " (instance-specific)" : "";
         printf("RPC method registered: %s%s\n", subject, tag);
     }
 
     natsConnection_Flush(srv->conn);
     srv->running = true;
 
     printf("RPC server started with %d methods\n", srv->handler_count);
     return RPC_OK;
 }
 
 void rpc_server_wait(RpcServer *srv)
 {
     if (!srv) return;
     pthread_mutex_lock(&srv->wait_mu);
     while (srv->running)
         pthread_cond_wait(&srv->wait_cond, &srv->wait_mu);
     pthread_mutex_unlock(&srv->wait_mu);
 }
 
 rpc_status_t rpc_server_stop(RpcServer *srv)
 {
     if (!srv) return RPC_ERR_INVALID_ARG;
     if (!srv->running) return RPC_OK;
 
     /* Unsubscribe all */
     for (int i = 0; i < srv->handler_count; i++) {
         HandlerEntry *e = &srv->handlers[i];
         if (e->sub) {
             natsSubscription_Unsubscribe(e->sub);
             natsSubscription_Destroy(e->sub);
             e->sub = NULL;
         }
     }
 
     if (srv->conn) {
         natsConnection_Flush(srv->conn);
         natsConnection_Close(srv->conn);
     }
 
     srv->running = false;
 
     /* Signal waiters */
     pthread_mutex_lock(&srv->wait_mu);
     pthread_cond_broadcast(&srv->wait_cond);
     pthread_mutex_unlock(&srv->wait_mu);
 
     printf("RPC server stopped (instance: %s)\n", srv->instance_id);
     return RPC_OK;
 }
 
 const char *rpc_server_instance_id(const RpcServer *srv)
 {
     return srv ? srv->instance_id : NULL;
 }
 
 bool rpc_server_is_running(const RpcServer *srv)
 {
     return srv ? srv->running : false;
 }
 
 /* ---- Statistics ---- */
 
 rpc_status_t rpc_server_get_stats(const RpcServer *srv,
                                   RpcHandlerStats **stats, size_t *count)
 {
     if (!srv || !stats || !count) return RPC_ERR_INVALID_ARG;
 
     *count = (size_t)srv->handler_count;
     *stats = calloc(*count, sizeof(RpcHandlerStats));
     if (!*stats) return RPC_ERR_MEMORY;
 
     for (int i = 0; i < srv->handler_count; i++) {
         (*stats)[i].method            = srv->handlers[i].method;
         (*stats)[i].call_count        = srv->handlers[i].call_count;
         (*stats)[i].error_count       = srv->handlers[i].error_count;
         (*stats)[i].instance_specific = srv->handlers[i].instance_specific;
     }
     return RPC_OK;
 }
 
 /* ================================================================== */
 /*  RPC Client                                                         */
 /* ================================================================== */
 
 struct RpcClient {
     char              server[256];
     char              namespace_[128];
     char              instance_id[RPC_INSTANCE_ID_LEN];
 
     natsConnection   *conn;
     natsOptions      *opts;
     bool              connected;
 
     int64_t           request_count;
 };
 
 /* ---- Create / Destroy ---- */
 
 rpc_status_t rpc_client_create(RpcClient **out, const RpcConfig *cfg)
 {
     if (!out || !cfg) return RPC_ERR_INVALID_ARG;
 
     RpcClient *cli = calloc(1, sizeof(*cli));
     if (!cli) return RPC_ERR_MEMORY;
 
     snprintf(cli->server, sizeof(cli->server), "%s",
              cfg->server ? cfg->server : "nats://127.0.0.1:4222");
     snprintf(cli->namespace_, sizeof(cli->namespace_), "%s",
              cfg->namespace_ ? cfg->namespace_ : "default");
 
     if (cfg->instance_id && cfg->instance_id[0]) {
         snprintf(cli->instance_id, sizeof(cli->instance_id), "%s",
                  cfg->instance_id);
     } else {
         gen_instance_id(cli->instance_id, sizeof(cli->instance_id));
     }
 
     *out = cli;
     return RPC_OK;
 }
 
 void rpc_client_destroy(RpcClient *cli)
 {
     if (!cli) return;
     rpc_client_disconnect(cli);
     if (cli->opts) {
         natsOptions_Destroy(cli->opts);
         cli->opts = NULL;
     }
     free(cli);
 }
 
 /* ---- Connect / Disconnect ---- */
 
 rpc_status_t rpc_client_connect(RpcClient *cli)
 {
     if (!cli) return RPC_ERR_INVALID_ARG;
     if (cli->connected) return RPC_OK;
 
     natsStatus ns = natsOptions_Create(&cli->opts);
     if (ns != NATS_OK) return RPC_ERR_NATS;
 
     natsOptions_SetURL(cli->opts, cli->server);
 
     char name[256];
     snprintf(name, sizeof(name), "rpc_client_%s", cli->instance_id);
     natsOptions_SetName(cli->opts, name);
 
     ns = natsConnection_Connect(&cli->conn, cli->opts);
     if (ns != NATS_OK) {
         fprintf(stderr, "RPC client: failed to connect to %s: %s\n",
                 cli->server, natsStatus_GetText(ns));
         return RPC_ERR_CONNECTION;
     }
 
     cli->connected = true;
     printf("RPC client connected to %s (instance: %s)\n",
            cli->server, cli->instance_id);
     return RPC_OK;
 }
 
 rpc_status_t rpc_client_disconnect(RpcClient *cli)
 {
     if (!cli) return RPC_ERR_INVALID_ARG;
     if (!cli->connected) return RPC_OK;
 
     if (cli->conn) {
         natsConnection_Flush(cli->conn);
         natsConnection_Close(cli->conn);
         natsConnection_Destroy(cli->conn);
         cli->conn = NULL;
     }
 
     cli->connected = false;
     printf("RPC client disconnected (instance: %s)\n", cli->instance_id);
     return RPC_OK;
 }
 
 bool rpc_client_is_connected(const RpcClient *cli)
 {
     return cli ? cli->connected : false;
 }
 
 const char *rpc_client_instance_id(const RpcClient *cli)
 {
     return cli ? cli->instance_id : NULL;
 }
 
 /* ---- Internal: build and send a request, wait for reply ---- */
 
 static rpc_status_t do_call(RpcClient *cli, const char *subject,
                             const char *params_json, double timeout_sec,
                             char **result_json)
 {
     if (!cli || !subject || !result_json)
         return RPC_ERR_INVALID_ARG;
     if (!cli->connected)
         return RPC_ERR_CONNECTION;
 
     *result_json = NULL;
 
     /* Build request JSON */
     cli->request_count++;
 
     char req_id[128];
     snprintf(req_id, sizeof(req_id), "%s_%" PRId64 "_%lx",
              cli->instance_id, cli->request_count,
              (unsigned long)time(NULL));
 
     cJSON *req = cJSON_CreateObject();
     if (!req) return RPC_ERR_MEMORY;
 
     cJSON_AddStringToObject(req, "id", req_id);
     cJSON_AddStringToObject(req, "method", subject);
 
     /* Embed params as object if valid JSON, otherwise as string */
     if (params_json && params_json[0]) {
         cJSON *p = cJSON_Parse(params_json);
         if (p)
             cJSON_AddItemToObject(req, "params", p);
         else
             cJSON_AddStringToObject(req, "params", params_json);
     } else {
         cJSON_AddObjectToObject(req, "params");
     }
 
     cJSON_AddStringToObject(req, "caller_instance", cli->instance_id);
 
     char ts[32];
     iso_now(ts, sizeof(ts));
     cJSON_AddStringToObject(req, "timestamp", ts);
 
     char *req_str = cJSON_PrintUnformatted(req);
     cJSON_Delete(req);
     if (!req_str) return RPC_ERR_ENCODE;
 
     /* Use nats.c's built-in request/reply (handles inbox automatically) */
     natsMsg *reply = NULL;
     int64_t timeout_ms = (int64_t)(timeout_sec * 1000.0);
     if (timeout_ms < 100) timeout_ms = 100;
 
     natsStatus ns = natsConnection_Request(&reply, cli->conn, subject,
                                            req_str, (int)strlen(req_str),
                                            timeout_ms);
     free(req_str);
 
     if (ns == NATS_TIMEOUT || ns == NATS_NO_RESPONDERS) {
         return RPC_ERR_TIMEOUT;
     }
     if (ns != NATS_OK) {
         return RPC_ERR_NATS;
     }
 
     /* Parse response */
     const char *rdata = natsMsg_GetData(reply);
     int rlen = natsMsg_GetDataLength(reply);
 
     if (!rdata || rlen <= 0) {
         natsMsg_Destroy(reply);
         return RPC_ERR_DECODE;
     }
 
     cJSON *resp = cJSON_ParseWithLength(rdata, (size_t)rlen);
     natsMsg_Destroy(reply);
 
     if (!resp) return RPC_ERR_DECODE;
 
     /* Check for error */
     cJSON *err = cJSON_GetObjectItem(resp, "error");
     if (err && !cJSON_IsNull(err)) {
         cJSON *emsg = cJSON_GetObjectItem(err, "message");
         if (emsg && cJSON_IsString(emsg))
             *result_json = strdup(emsg->valuestring);
         else
             *result_json = strdup("Unknown RPC error");
         cJSON_Delete(resp);
         return RPC_ERR_HANDLER;
     }
 
     /* Extract result */
     cJSON *res = cJSON_GetObjectItem(resp, "result");
     if (res && !cJSON_IsNull(res))
         *result_json = cJSON_PrintUnformatted(res);
     else
         *result_json = strdup("null");
 
     cJSON_Delete(resp);
     return RPC_OK;
 }
 
 /* ---- Public call functions ---- */
 
 /**
  * Check if the method contains an internal subject starting with '_'.
  * E.g. "rpc._health" → the "_health" part should skip namespace.
  * Returns true if namespace should be skipped.
  */
 static bool method_is_internal(const char *method)
 {
     if (!method) return false;
     if (method[0] == '_') return true;
     /* Check for "prefix._method" pattern */
     const char *p = method;
     while ((p = strchr(p, '.')) != NULL) {
         p++;
         if (*p == '_') return true;
     }
     return false;
 }
 
 rpc_status_t rpc_client_call(RpcClient *cli, const char *method,
                              const char *params_json, double timeout_sec,
                              char **result_json)
 {
     if (!cli || !method) return RPC_ERR_INVALID_ARG;
 
     char subject[RPC_SUBJECT_LEN];
     if (method_is_internal(method)) {
         snprintf(subject, sizeof(subject), "%s", method);
     } else {
         snprintf(subject, sizeof(subject), "%s.%s", cli->namespace_, method);
     }
 
     return do_call(cli, subject, params_json, timeout_sec, result_json);
 }
 
 rpc_status_t rpc_client_call_instance(RpcClient *cli, const char *method,
                                       const char *params_json,
                                       double timeout_sec,
                                       const char *target_instance,
                                       char **result_json)
 {
     if (!cli || !method || !target_instance)
         return RPC_ERR_INVALID_ARG;
 
     char subject[RPC_SUBJECT_LEN];
     if (method_is_internal(method)) {
         snprintf(subject, sizeof(subject), "%s.%s", method, target_instance);
     } else {
         snprintf(subject, sizeof(subject), "%s.%s.%s",
                  cli->namespace_, method, target_instance);
     }
 
     return do_call(cli, subject, params_json, timeout_sec, result_json);
 }
 
 /* ---- Batch calls ---- */
 
 rpc_status_t rpc_client_call_batch(RpcClient *cli,
                                    const RpcBatchEntry *entries,
                                    size_t count, double timeout_sec,
                                    RpcBatchResult *results)
 {
     if (!cli || !entries || !results || count == 0)
         return RPC_ERR_INVALID_ARG;
 
     /* Simple sequential implementation.
      * For true parallelism we'd use nats async subscriptions + inbox,
      * but sequential is correct and simpler for C. */
     for (size_t i = 0; i < count; i++) {
         const RpcBatchEntry *e = &entries[i];
         results[i].result_json = NULL;
 
         if (e->target_instance && e->target_instance[0]) {
             results[i].status = rpc_client_call_instance(
                 cli, e->method, e->params_json, timeout_sec,
                 e->target_instance, &results[i].result_json);
         } else {
             results[i].status = rpc_client_call(
                 cli, e->method, e->params_json, timeout_sec,
                 &results[i].result_json);
         }
     }
 
     return RPC_OK;
 }