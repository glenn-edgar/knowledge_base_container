/**
 * mqtt_rpc.c - JSON-RPC 2.0 over MQTT implementation
 *
 * Server listens on  rpc/{service}/request/+
 * Client publishes to rpc/{service}/request/{client_id}
 * Server replies to   rpc/{service}/response/{client_id}
 * Client subscribes   rpc/{service}/response/{client_id}
 */

#define _DEFAULT_SOURCE
#define _POSIX_C_SOURCE 200809L

#include "mqtt_rpc.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <unistd.h>

#define UNUSED(x) (void)(x)

/* ================================================================== */
/*  Helpers                                                            */
/* ================================================================== */

/** printf into a newly malloc'd string. */
static char *aprintf(const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(NULL, 0, fmt, ap);
    va_end(ap);
    if (n < 0) return NULL;

    char *buf = malloc((size_t)n + 1);
    if (!buf) return NULL;

    va_start(ap, fmt);
    vsnprintf(buf, (size_t)n + 1, fmt, ap);
    va_end(ap);
    return buf;
}

/** Absolute-time condvar wait with millisecond timeout. */
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

/** Generate a simple pseudo-unique id: "prefix_COUNTER". */
static char *make_id(const char *prefix, int counter)
{
    return aprintf("%s_%d", prefix, counter);
}

/** Generate an auto client_id with a small random suffix. */
static char *auto_client_id(const char *prefix)
{
    unsigned int seed = (unsigned int)(time(NULL) ^ getpid());
    return aprintf("%s_%08x", prefix, rand_r(&seed) & 0xFFFFFFFF);
}

/* ================================================================== */
/*  Library init / cleanup                                             */
/* ================================================================== */

void mqtt_rpc_lib_init(void)
{
    mosquitto_lib_init();
}

void mqtt_rpc_lib_cleanup(void)
{
    mosquitto_lib_cleanup();
}

/* ================================================================== */
/*  RPC Server — callbacks                                             */
/* ================================================================== */

static void srv_on_connect(struct mosquitto *mosq, void *userdata, int rc)
{
    UNUSED(mosq);
    rpc_server_t *srv = (rpc_server_t *)userdata;

    pthread_mutex_lock(&srv->lock);
    srv->connected = (rc == 0);
    printf("[rpc_server] on_connect rc=%d (%s)\n", rc,
           rc == 0 ? "success" : mosquitto_connack_string(rc));

    if (srv->connected) {
        int mid = 0;
        int res = mosquitto_subscribe(srv->mosq, &mid, srv->request_topic, srv->cfg.qos);
        if (res != MOSQ_ERR_SUCCESS)
            fprintf(stderr, "[rpc_server] subscribe error: %s\n", mosquitto_strerror(res));
    }
    pthread_cond_signal(&srv->connect_cond);
    pthread_mutex_unlock(&srv->lock);
}

static void srv_on_disconnect(struct mosquitto *mosq, void *userdata, int rc)
{
    UNUSED(mosq);
    rpc_server_t *srv = (rpc_server_t *)userdata;
    pthread_mutex_lock(&srv->lock);
    srv->connected = false;
    printf("[rpc_server] on_disconnect rc=%d\n", rc);
    pthread_cond_signal(&srv->connect_cond);
    pthread_mutex_unlock(&srv->lock);
}

static void srv_on_subscribe(struct mosquitto *mosq, void *userdata,
                             int mid, int qos_count, const int *granted_qos)
{
    UNUSED(mosq); UNUSED(qos_count); UNUSED(granted_qos);
    rpc_server_t *srv = (rpc_server_t *)userdata;
    printf("[rpc_server] on_subscribe mid=%d\n", mid);
    pthread_mutex_lock(&srv->lock);
    srv->subscribed = true;
    pthread_cond_signal(&srv->subscribe_cond);
    pthread_mutex_unlock(&srv->lock);
}

/* ---- request processing (runs in a detached thread) --------------- */

typedef struct {
    rpc_server_t *srv;
    cJSON        *request;
    char         *caller_id;
} srv_request_ctx_t;

static void *srv_process_thread(void *arg)
{
    srv_request_ctx_t *ctx = (srv_request_ctx_t *)arg;
    rpc_server_t *srv = ctx->srv;
    cJSON *req = ctx->request;
    const char *caller_id = ctx->caller_id;

    /* Build response envelope */
    cJSON *resp = cJSON_CreateObject();
    cJSON *id_val = cJSON_GetObjectItem(req, "id");
    if (id_val)
        cJSON_AddItemToObject(resp, "id", cJSON_Duplicate(id_val, true));
    cJSON_AddStringToObject(resp, "jsonrpc", "2.0");

    /* Validate JSON-RPC shape */
    cJSON *ver = cJSON_GetObjectItem(req, "jsonrpc");
    cJSON *method_j = cJSON_GetObjectItem(req, "method");

    if (!ver || !cJSON_IsString(ver) || strcmp(ver->valuestring, "2.0") != 0 ||
        !method_j || !cJSON_IsString(method_j)) {
        cJSON *err = cJSON_CreateObject();
        cJSON_AddNumberToObject(err, "code", JSONRPC_INVALID_REQUEST);
        cJSON_AddStringToObject(err, "message", "Invalid Request");
        cJSON_AddItemToObject(resp, "error", err);
    } else {
        const char *method_name = method_j->valuestring;
        cJSON *params = cJSON_GetObjectItem(req, "params"); /* may be NULL */

        /* Look up method */
        rpc_method_entry_t *entry = NULL;
        pthread_mutex_lock(&srv->lock);
        for (rpc_method_entry_t *e = srv->methods; e; e = e->next) {
            if (strcmp(e->name, method_name) == 0) { entry = e; break; }
        }
        pthread_mutex_unlock(&srv->lock);

        if (!entry) {
            cJSON *err = cJSON_CreateObject();
            cJSON_AddNumberToObject(err, "code", JSONRPC_METHOD_NOT_FOUND);
            char msg[128];
            snprintf(msg, sizeof(msg), "Method '%s' not found", method_name);
            cJSON_AddStringToObject(err, "message", msg);
            cJSON_AddItemToObject(resp, "error", err);
        } else {
            cJSON *result = entry->fn(params, entry->userdata);
            if (result) {
                cJSON_AddItemToObject(resp, "result", result);
            } else {
                cJSON *err = cJSON_CreateObject();
                cJSON_AddNumberToObject(err, "code", JSONRPC_INTERNAL_ERROR);
                cJSON_AddStringToObject(err, "message", "Method returned NULL");
                cJSON_AddItemToObject(resp, "error", err);
            }
        }
    }

    /* Publish response */
    char *resp_topic = aprintf("%s/%s", srv->response_topic_base, caller_id);
    char *payload = cJSON_PrintUnformatted(resp);
    if (resp_topic && payload) {
        mosquitto_publish(srv->mosq, NULL, resp_topic,
                          (int)strlen(payload), payload, srv->cfg.qos, false);
    }
    free(resp_topic);
    free(payload);
    cJSON_Delete(resp);
    cJSON_Delete(req);
    free(ctx->caller_id);
    free(ctx);
    return NULL;
}

static void srv_on_message(struct mosquitto *mosq, void *userdata,
                           const struct mosquitto_message *msg)
{
    UNUSED(mosq);
    rpc_server_t *srv = (rpc_server_t *)userdata;

    /*
     * Expected topic: rpc/{service}/request/{caller_client_id}
     * We need to extract the caller_client_id (everything after the third '/').
     */
    const char *topic = msg->topic;
    int slash_count = 0;
    const char *caller_start = NULL;
    for (const char *p = topic; *p; p++) {
        if (*p == '/') {
            slash_count++;
            if (slash_count == 3) { caller_start = p + 1; break; }
        }
    }
    if (!caller_start || *caller_start == '\0') return;

    cJSON *req = cJSON_ParseWithLength((const char *)msg->payload, (size_t)msg->payloadlen);
    if (!req) {
        fprintf(stderr, "[rpc_server] failed to parse JSON request\n");
        return;
    }

    /* Dispatch to a thread (mirrors the Python version) */
    srv_request_ctx_t *ctx = malloc(sizeof(*ctx));
    if (!ctx) { cJSON_Delete(req); return; }
    ctx->srv       = srv;
    ctx->request   = req;
    ctx->caller_id = strdup(caller_start);

    pthread_t tid;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    if (pthread_create(&tid, &attr, srv_process_thread, ctx) != 0) {
        fprintf(stderr, "[rpc_server] pthread_create failed\n");
        cJSON_Delete(req);
        free(ctx->caller_id);
        free(ctx);
    }
    pthread_attr_destroy(&attr);
}

/* ================================================================== */
/*  RPC Server — public API                                            */
/* ================================================================== */

int rpc_server_init(rpc_server_t *srv, const mqtt_rpc_config_t *cfg)
{
    memset(srv, 0, sizeof(*srv));
    srv->cfg = *cfg;

    /* Auto-generate client_id if needed */
    srv->client_id = cfg->client_id ? strdup(cfg->client_id)
                                    : auto_client_id("rpc_server");

    const char *svc = cfg->service_name ? cfg->service_name : "rpc_service";
    srv->request_topic       = aprintf("rpc/%s/request/+", svc);
    srv->response_topic_base = aprintf("rpc/%s/response", svc);

    pthread_mutex_init(&srv->lock, NULL);
    pthread_cond_init(&srv->connect_cond, NULL);
    pthread_cond_init(&srv->subscribe_cond, NULL);

    srv->mosq = mosquitto_new(srv->client_id, true /* clean_session */, srv);
    if (!srv->mosq) {
        fprintf(stderr, "[rpc_server] mosquitto_new: %s\n", strerror(errno));
        return -1;
    }

    if (cfg->username)
        mosquitto_username_pw_set(srv->mosq, cfg->username, cfg->password);

    mosquitto_connect_callback_set(srv->mosq, srv_on_connect);
    mosquitto_disconnect_callback_set(srv->mosq, srv_on_disconnect);
    mosquitto_subscribe_callback_set(srv->mosq, srv_on_subscribe);
    mosquitto_message_callback_set(srv->mosq, srv_on_message);

    return 0;
}

void rpc_server_register(rpc_server_t *srv, const char *name,
                         rpc_method_fn fn, void *userdata)
{
    rpc_method_entry_t *e = calloc(1, sizeof(*e));
    if (!e) return;
    e->name     = strdup(name);
    e->fn       = fn;
    e->userdata = userdata;

    pthread_mutex_lock(&srv->lock);
    e->next = srv->methods;
    srv->methods = e;
    pthread_mutex_unlock(&srv->lock);

    printf("[rpc_server] registered method: %s\n", name);
}

int rpc_server_start(rpc_server_t *srv, bool wait_for_suback, int sub_timeout_ms)
{
    int rc = mosquitto_connect(srv->mosq, srv->cfg.host, srv->cfg.port, srv->cfg.keepalive);
    if (rc != MOSQ_ERR_SUCCESS) {
        fprintf(stderr, "[rpc_server] connect: %s\n", mosquitto_strerror(rc));
        return -1;
    }

    rc = mosquitto_loop_start(srv->mosq);
    if (rc != MOSQ_ERR_SUCCESS) {
        fprintf(stderr, "[rpc_server] loop_start: %s\n", mosquitto_strerror(rc));
        return -1;
    }

    /* Wait for CONNACK */
    pthread_mutex_lock(&srv->lock);
    if (!srv->connected)
        cond_timedwait_ms(&srv->connect_cond, &srv->lock, 5000);
    bool ok = srv->connected;
    pthread_mutex_unlock(&srv->lock);
    if (!ok) { fprintf(stderr, "[rpc_server] connect timeout\n"); return -1; }

    /* Optionally wait for SUBACK */
    if (wait_for_suback) {
        pthread_mutex_lock(&srv->lock);
        if (!srv->subscribed)
            cond_timedwait_ms(&srv->subscribe_cond, &srv->lock, sub_timeout_ms);
        if (!srv->subscribed)
            fprintf(stderr, "[rpc_server] warning: SUBACK not received yet\n");
        pthread_mutex_unlock(&srv->lock);
    }

    printf("[rpc_server] started (service=%s)\n", srv->cfg.service_name);
    return 0;
}

void rpc_server_wait(rpc_server_t *srv)
{
    while (!srv->stop_flag) {
        usleep(250000); /* 250 ms */
    }
}

void rpc_server_stop(rpc_server_t *srv)
{
    printf("[rpc_server] stopping...\n");
    srv->stop_flag = true;
    mosquitto_disconnect(srv->mosq);
    mosquitto_loop_stop(srv->mosq, false);
    pthread_mutex_lock(&srv->lock);
    srv->connected = false;
    pthread_mutex_unlock(&srv->lock);
}

void rpc_server_destroy(rpc_server_t *srv)
{
    /* Free method list */
    rpc_method_entry_t *e = srv->methods;
    while (e) {
        rpc_method_entry_t *next = e->next;
        free(e->name);
        free(e);
        e = next;
    }
    srv->methods = NULL;

    free(srv->client_id);
    free(srv->request_topic);
    free(srv->response_topic_base);

    if (srv->mosq) {
        mosquitto_destroy(srv->mosq);
        srv->mosq = NULL;
    }
    pthread_mutex_destroy(&srv->lock);
    pthread_cond_destroy(&srv->connect_cond);
    pthread_cond_destroy(&srv->subscribe_cond);
}

/* ================================================================== */
/*  RPC Client — callbacks                                             */
/* ================================================================== */

static void cli_on_connect(struct mosquitto *mosq, void *userdata, int rc)
{
    UNUSED(mosq);
    rpc_client_t *cli = (rpc_client_t *)userdata;

    pthread_mutex_lock(&cli->lock);
    cli->connected = (rc == 0);
    printf("[rpc_client] on_connect rc=%d (%s)\n", rc,
           rc == 0 ? "success" : mosquitto_connack_string(rc));

    if (cli->connected) {
        int mid = 0;
        int res = mosquitto_subscribe(cli->mosq, &mid, cli->response_topic, cli->cfg.qos);
        if (res != MOSQ_ERR_SUCCESS)
            fprintf(stderr, "[rpc_client] subscribe error: %s\n", mosquitto_strerror(res));
    }
    pthread_cond_signal(&cli->connect_cond);
    pthread_mutex_unlock(&cli->lock);
}

static void cli_on_disconnect(struct mosquitto *mosq, void *userdata, int rc)
{
    UNUSED(mosq);
    rpc_client_t *cli = (rpc_client_t *)userdata;
    pthread_mutex_lock(&cli->lock);
    cli->connected = false;
    printf("[rpc_client] on_disconnect rc=%d\n", rc);
    pthread_cond_signal(&cli->connect_cond);
    pthread_mutex_unlock(&cli->lock);
}

static void cli_on_subscribe(struct mosquitto *mosq, void *userdata,
                             int mid, int qos_count, const int *granted_qos)
{
    UNUSED(mosq); UNUSED(qos_count); UNUSED(granted_qos);
    rpc_client_t *cli = (rpc_client_t *)userdata;
    printf("[rpc_client] on_subscribe mid=%d\n", mid);
    pthread_mutex_lock(&cli->lock);
    cli->subscribed = true;
    pthread_cond_signal(&cli->subscribe_cond);
    pthread_mutex_unlock(&cli->lock);
}

static void cli_on_message(struct mosquitto *mosq, void *userdata,
                           const struct mosquitto_message *msg)
{
    UNUSED(mosq);
    rpc_client_t *cli = (rpc_client_t *)userdata;

    cJSON *resp = cJSON_ParseWithLength((const char *)msg->payload, (size_t)msg->payloadlen);
    if (!resp) return;

    cJSON *id_j = cJSON_GetObjectItem(resp, "id");
    if (!id_j || !cJSON_IsString(id_j)) {
        cJSON_Delete(resp);
        return;
    }
    const char *id_str = id_j->valuestring;

    pthread_mutex_lock(&cli->lock);
    for (rpc_pending_t *p = cli->pending_head; p; p = p->next) {
        if (strcmp(p->id, id_str) == 0) {
            p->response = resp;   /* hand ownership to waiter */
            resp = NULL;          /* don't delete below */
            p->done = true;
            pthread_cond_signal(&p->cond);
            break;
        }
    }
    pthread_mutex_unlock(&cli->lock);

    if (resp) cJSON_Delete(resp);  /* unmatched response */
}

/* ================================================================== */
/*  RPC Client — public API                                            */
/* ================================================================== */

int rpc_client_init(rpc_client_t *cli, const mqtt_rpc_config_t *cfg,
                    float default_timeout_s)
{
    memset(cli, 0, sizeof(*cli));
    cli->cfg = *cfg;
    cli->default_timeout_s = (default_timeout_s > 0) ? default_timeout_s : 30.0f;

    cli->client_id = cfg->client_id ? strdup(cfg->client_id)
                                    : auto_client_id("rpc_client");

    const char *svc = cfg->service_name ? cfg->service_name : "rpc_service";
    cli->request_topic  = aprintf("rpc/%s/request/%s", svc, cli->client_id);
    cli->response_topic = aprintf("rpc/%s/response/%s", svc, cli->client_id);

    pthread_mutex_init(&cli->lock, NULL);
    pthread_cond_init(&cli->connect_cond, NULL);
    pthread_cond_init(&cli->subscribe_cond, NULL);

    cli->mosq = mosquitto_new(cli->client_id, true, cli);
    if (!cli->mosq) {
        fprintf(stderr, "[rpc_client] mosquitto_new: %s\n", strerror(errno));
        return -1;
    }

    if (cfg->username)
        mosquitto_username_pw_set(cli->mosq, cfg->username, cfg->password);

    mosquitto_connect_callback_set(cli->mosq, cli_on_connect);
    mosquitto_disconnect_callback_set(cli->mosq, cli_on_disconnect);
    mosquitto_subscribe_callback_set(cli->mosq, cli_on_subscribe);
    mosquitto_message_callback_set(cli->mosq, cli_on_message);

    return 0;
}

int rpc_client_connect(rpc_client_t *cli, int timeout_ms)
{
    int rc = mosquitto_connect(cli->mosq, cli->cfg.host, cli->cfg.port, cli->cfg.keepalive);
    if (rc != MOSQ_ERR_SUCCESS) {
        fprintf(stderr, "[rpc_client] connect: %s\n", mosquitto_strerror(rc));
        return -1;
    }

    rc = mosquitto_loop_start(cli->mosq);
    if (rc != MOSQ_ERR_SUCCESS) {
        fprintf(stderr, "[rpc_client] loop_start: %s\n", mosquitto_strerror(rc));
        return -1;
    }

    /* Wait for CONNACK */
    pthread_mutex_lock(&cli->lock);
    if (!cli->connected)
        cond_timedwait_ms(&cli->connect_cond, &cli->lock, timeout_ms);
    bool ok = cli->connected;
    pthread_mutex_unlock(&cli->lock);

    if (!ok) {
        fprintf(stderr, "[rpc_client] connect timeout\n");
        mosquitto_loop_stop(cli->mosq, true);
        return -1;
    }

    /* Wait for SUBACK */
    pthread_mutex_lock(&cli->lock);
    if (!cli->subscribed)
        cond_timedwait_ms(&cli->subscribe_cond, &cli->lock, timeout_ms);
    if (!cli->subscribed)
        fprintf(stderr, "[rpc_client] warning: SUBACK not received yet\n");
    pthread_mutex_unlock(&cli->lock);

    return 0;
}

void rpc_client_disconnect(rpc_client_t *cli)
{
    printf("[rpc_client] disconnecting...\n");
    mosquitto_disconnect(cli->mosq);
    mosquitto_loop_stop(cli->mosq, false);
    pthread_mutex_lock(&cli->lock);
    cli->connected = false;
    pthread_mutex_unlock(&cli->lock);
}

void rpc_client_destroy(rpc_client_t *cli)
{
    /* Free any lingering pending requests */
    rpc_pending_t *p = cli->pending_head;
    while (p) {
        rpc_pending_t *next = p->next;
        free(p->id);
        if (p->response) cJSON_Delete(p->response);
        pthread_cond_destroy(&p->cond);
        free(p);
        p = next;
    }
    cli->pending_head = NULL;

    free(cli->client_id);
    free(cli->request_topic);
    free(cli->response_topic);

    if (cli->mosq) {
        mosquitto_destroy(cli->mosq);
        cli->mosq = NULL;
    }
    pthread_mutex_destroy(&cli->lock);
    pthread_cond_destroy(&cli->connect_cond);
    pthread_cond_destroy(&cli->subscribe_cond);
}

int rpc_client_call(rpc_client_t *cli,
                    const char *method,
                    const cJSON *params,
                    float timeout_s,
                    cJSON **out_result,
                    cJSON **out_error)
{
    if (out_result) *out_result = NULL;
    if (out_error)  *out_error  = NULL;

    if (!cli->connected) {
        fprintf(stderr, "[rpc_client] call: not connected\n");
        return -1;
    }

    float tout = (timeout_s > 0) ? timeout_s : cli->default_timeout_s;

    /* Build request id */
    pthread_mutex_lock(&cli->lock);
    cli->request_counter++;
    char *req_id = make_id(cli->client_id, cli->request_counter);
    pthread_mutex_unlock(&cli->lock);

    /* Build JSON-RPC request */
    cJSON *req = cJSON_CreateObject();
    cJSON_AddStringToObject(req, "jsonrpc", "2.0");
    cJSON_AddStringToObject(req, "method", method);
    cJSON_AddStringToObject(req, "id", req_id);
    if (params)
        cJSON_AddItemToObject(req, "params", cJSON_Duplicate(params, true));

    char *payload = cJSON_PrintUnformatted(req);
    cJSON_Delete(req);

    if (!payload) {
        free(req_id);
        return -1;
    }

    /* Register pending slot */
    rpc_pending_t *pend = calloc(1, sizeof(*pend));
    pend->id   = req_id;
    pend->done = false;
    pthread_cond_init(&pend->cond, NULL);

    pthread_mutex_lock(&cli->lock);
    pend->next = cli->pending_head;
    cli->pending_head = pend;
    pthread_mutex_unlock(&cli->lock);

    /* Publish */
    int rc = mosquitto_publish(cli->mosq, NULL, cli->request_topic,
                               (int)strlen(payload), payload, cli->cfg.qos, false);
    free(payload);

    if (rc != MOSQ_ERR_SUCCESS) {
        fprintf(stderr, "[rpc_client] publish failed: %s\n", mosquitto_strerror(rc));
        /* Remove pending */
        pthread_mutex_lock(&cli->lock);
        rpc_pending_t **pp = &cli->pending_head;
        while (*pp) {
            if (*pp == pend) { *pp = pend->next; break; }
            pp = &(*pp)->next;
        }
        pthread_mutex_unlock(&cli->lock);
        free(pend->id);
        pthread_cond_destroy(&pend->cond);
        free(pend);
        return -1;
    }

    /* Wait for response */
    int timeout_ms = (int)(tout * 1000);
    pthread_mutex_lock(&cli->lock);
    while (!pend->done) {
        int err = cond_timedwait_ms(&pend->cond, &cli->lock, timeout_ms);
        if (err == ETIMEDOUT) break;
    }
    /* Unlink from pending list */
    rpc_pending_t **pp = &cli->pending_head;
    while (*pp) {
        if (*pp == pend) { *pp = pend->next; break; }
        pp = &(*pp)->next;
    }
    pthread_mutex_unlock(&cli->lock);

    if (!pend->done) {
        fprintf(stderr, "[rpc_client] call '%s' timed out after %.1fs\n", method, tout);
        free(pend->id);
        pthread_cond_destroy(&pend->cond);
        free(pend);
        return -1;
    }

    /* Extract result or error from response */
    cJSON *response = pend->response;
    free(pend->id);
    pthread_cond_destroy(&pend->cond);
    free(pend);

    if (!response) return -1;

    cJSON *err_j = cJSON_DetachItemFromObject(response, "error");
    cJSON *res_j = cJSON_DetachItemFromObject(response, "result");
    cJSON_Delete(response);

    if (err_j) {
        if (out_error) *out_error = err_j;
        else cJSON_Delete(err_j);
        if (res_j) cJSON_Delete(res_j);
        return -2;
    }

    if (out_result) *out_result = res_j;
    else if (res_j) cJSON_Delete(res_j);
    return 0;
}

/* ---- async call -------------------------------------------------- */

typedef struct {
    rpc_client_t  *cli;
    char          *method;
    cJSON         *params;      /* owned copy */
    float          timeout_s;
    rpc_async_cb   cb;
    void          *cb_userdata;
} async_ctx_t;

static void *async_thread(void *arg)
{
    async_ctx_t *ctx = (async_ctx_t *)arg;

    cJSON *result = NULL, *error = NULL;
    int rc = rpc_client_call(ctx->cli, ctx->method, ctx->params,
                             ctx->timeout_s, &result, &error);

    if (ctx->cb) {
        if (rc == 0) {
            ctx->cb(NULL, result, ctx->cb_userdata);
        } else if (rc == -2) {
            ctx->cb(error, NULL, ctx->cb_userdata);
        } else {
            /* transport error — manufacture an error object */
            cJSON *terr = cJSON_CreateObject();
            cJSON_AddNumberToObject(terr, "code", -1);
            cJSON_AddStringToObject(terr, "message", "Transport/timeout error");
            ctx->cb(terr, NULL, ctx->cb_userdata);
            cJSON_Delete(terr);
        }
    }

    if (result) cJSON_Delete(result);
    if (error)  cJSON_Delete(error);
    free(ctx->method);
    if (ctx->params) cJSON_Delete(ctx->params);
    free(ctx);
    return NULL;
}

char *rpc_client_call_async(rpc_client_t *cli,
                            const char *method,
                            const cJSON *params,
                            float timeout_s,
                            rpc_async_cb cb,
                            void *cb_userdata)
{
    async_ctx_t *ctx = calloc(1, sizeof(*ctx));
    if (!ctx) return NULL;

    ctx->cli         = cli;
    ctx->method      = strdup(method);
    ctx->params      = params ? cJSON_Duplicate(params, true) : NULL;
    ctx->timeout_s   = (timeout_s > 0) ? timeout_s : cli->default_timeout_s;
    ctx->cb          = cb;
    ctx->cb_userdata = cb_userdata;

    /* Generate the id that rpc_client_call will use (peek at next counter) */
    pthread_mutex_lock(&cli->lock);
    int next = cli->request_counter + 1;
    pthread_mutex_unlock(&cli->lock);
    char *future_id = make_id(cli->client_id, next);

    pthread_t tid;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    if (pthread_create(&tid, &attr, async_thread, ctx) != 0) {
        free(ctx->method);
        if (ctx->params) cJSON_Delete(ctx->params);
        free(ctx);
        free(future_id);
        future_id = NULL;
    }
    pthread_attr_destroy(&attr);
    return future_id;
}
