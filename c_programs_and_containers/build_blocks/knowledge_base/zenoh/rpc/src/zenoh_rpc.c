/**
 * @file zenoh_rpc.c
 * @brief zenoh-pico-backed request/response RPC wrapper.
 *
 * Server side wraps z_declare_queryable; handler is invoked from the
 * zenoh-pico read thread when a query arrives, and its returned payload
 * is sent back via z_query_reply.
 *
 * Client side wraps z_get; the reply closure stores the first reply and
 * signals a pthread_cond; the call blocks on pthread_cond_timedwait with
 * the caller-provided timeout.
 */

#define _POSIX_C_SOURCE 200809L

#include "zenoh_rpc.h"

#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <zenoh-pico.h>

/* ------------------------------------------------------------------ */
/*  Status                                                             */
/* ------------------------------------------------------------------ */

const char *zrpc_status_str(zrpc_status_t st) {
    switch (st) {
        case ZRPC_OK:                return "OK";
        case ZRPC_ERR_INVALID_ARG:   return "invalid argument";
        case ZRPC_ERR_CONNECTION:    return "connection error";
        case ZRPC_ERR_TIMEOUT:       return "timeout";
        case ZRPC_ERR_MEMORY:        return "out of memory";
        case ZRPC_ERR_NOT_CONNECTED: return "not connected";
        case ZRPC_ERR_NO_REPLY:      return "no reply received";
        case ZRPC_ERR_HANDLER:       return "handler error";
        case ZRPC_ERR_ZENOH:         return "zenoh-pico error";
    }
    return "unknown";
}

void zenoh_rpc_config_defaults(ZenohRpcConfig *cfg) {
    if (!cfg) return;
    cfg->locators     = NULL;
    cfg->n_locators   = 0;
    cfg->mode         = "client";
    cfg->enable_scout = false;
    cfg->client_name  = NULL;
}

/* ------------------------------------------------------------------ */
/*  Helpers                                                            */
/* ------------------------------------------------------------------ */

static void token_to_keyexpr(uint32_t token, char *buf, size_t n) {
    snprintf(buf, n, "tok/%08x", token);
}

static z_result_t open_session_from_cfg(z_owned_session_t *s, const ZenohRpcConfig *cfg) {
    z_owned_config_t zcfg;
    z_config_default(&zcfg);
    zp_config_insert(z_loan_mut(zcfg), Z_CONFIG_MODE_KEY, cfg->mode);
    for (size_t i = 0; i < cfg->n_locators; ++i) {
        zp_config_insert(z_loan_mut(zcfg), Z_CONFIG_CONNECT_KEY, cfg->locators[i]);
    }
    return z_open(s, z_move(zcfg), NULL);
}

/* ------------------------------------------------------------------ */
/*  Server                                                             */
/* ------------------------------------------------------------------ */

typedef struct handler_entry {
    uint32_t                  token;
    zenoh_rpc_handler_t       fn;
    void                     *ctx;
    z_owned_queryable_t       z_qable;
    int                       declared;
    struct handler_entry     *next;
} handler_entry_t;

struct ZenohRpcServer {
    ZenohRpcConfig    cfg_copy;
    int               running;
    pthread_mutex_t   lock;
    z_owned_session_t session;
    handler_entry_t  *handlers;
};

/* zenoh-pico query closure adapter — invoked on the read thread. */
static void server_query_cb(z_loaned_query_t *query, void *ctx) {
    handler_entry_t *h = (handler_entry_t *)ctx;
    if (!h || !h->fn) return;

    /* Extract request payload (may be empty). */
    const z_loaned_bytes_t *zreq = z_query_payload(query);
    z_owned_slice_t reqslice;
    z_bytes_to_slice(zreq, &reqslice);
    const uint8_t *req_data = z_slice_data(z_loan(reqslice));
    size_t         req_len  = z_slice_len(z_loan(reqslice));

    /* Invoke user handler. */
    uint8_t      *resp     = NULL;
    size_t        resp_len = 0;
    zrpc_status_t st       = h->fn(h->token, req_data, req_len, &resp, &resp_len, h->ctx);

    z_drop(z_move(reqslice));

    if (st == ZRPC_OK) {
        /* Send reply (even if zero-length). */
        z_owned_bytes_t reply_body;
        z_bytes_copy_from_buf(&reply_body, resp ? resp : (const uint8_t *)"", resp_len);
        z_query_reply(query, z_query_keyexpr(query), z_move(reply_body), NULL);
        free(resp);
    } else {
        /* Send reply_err with status string as payload. */
        const char *errmsg = zrpc_status_str(st);
        z_owned_bytes_t err_body;
        z_bytes_copy_from_buf(&err_body, (const uint8_t *)errmsg, strlen(errmsg));
        z_query_reply_err(query, z_move(err_body), NULL);
        free(resp);
    }
}

zrpc_status_t zenoh_rpc_server_create(ZenohRpcServer **out, const ZenohRpcConfig *cfg) {
    if (!out || !cfg) return ZRPC_ERR_INVALID_ARG;
    if (cfg->n_locators == 0 || cfg->locators == NULL) return ZRPC_ERR_INVALID_ARG;
    ZenohRpcServer *srv = calloc(1, sizeof(*srv));
    if (!srv) return ZRPC_ERR_MEMORY;
    srv->cfg_copy = *cfg;
    pthread_mutex_init(&srv->lock, NULL);
    *out = srv;
    return ZRPC_OK;
}

void zenoh_rpc_server_destroy(ZenohRpcServer *srv) {
    if (!srv) return;
    if (srv->running) zenoh_rpc_server_stop(srv);
    pthread_mutex_lock(&srv->lock);
    handler_entry_t *h = srv->handlers;
    while (h) {
        handler_entry_t *next = h->next;
        free(h);
        h = next;
    }
    pthread_mutex_unlock(&srv->lock);
    pthread_mutex_destroy(&srv->lock);
    free(srv);
}

zrpc_status_t zenoh_rpc_server_register(ZenohRpcServer *srv,
                                        uint32_t token,
                                        zenoh_rpc_handler_t handler,
                                        void *ctx) {
    if (!srv || !handler) return ZRPC_ERR_INVALID_ARG;

    handler_entry_t *h = calloc(1, sizeof(*h));
    if (!h) return ZRPC_ERR_MEMORY;
    h->token = token;
    h->fn    = handler;
    h->ctx   = ctx;

    pthread_mutex_lock(&srv->lock);
    h->next = srv->handlers;
    srv->handlers = h;

    /* If we're already running, declare the queryable now; otherwise
     * declare it later in start(). */
    if (srv->running) {
        char keystr[32];
        token_to_keyexpr(token, keystr, sizeof(keystr));
        z_view_keyexpr_t ke;
        if (z_view_keyexpr_from_str(&ke, keystr) >= 0) {
            z_owned_closure_query_t closure;
            z_closure(&closure, server_query_cb, NULL, h);
            if (z_declare_queryable(z_loan(srv->session), &h->z_qable,
                                    z_loan(ke), z_move(closure), NULL) >= 0) {
                h->declared = 1;
            }
        }
    }
    pthread_mutex_unlock(&srv->lock);
    return ZRPC_OK;
}

zrpc_status_t zenoh_rpc_server_start(ZenohRpcServer *srv) {
    if (!srv) return ZRPC_ERR_INVALID_ARG;
    if (open_session_from_cfg(&srv->session, &srv->cfg_copy) < 0) {
        return ZRPC_ERR_CONNECTION;
    }
    pthread_mutex_lock(&srv->lock);
    srv->running = 1;

    /* Declare queryables for all already-registered handlers. */
    for (handler_entry_t *h = srv->handlers; h != NULL; h = h->next) {
        if (h->declared) continue;
        char keystr[32];
        token_to_keyexpr(h->token, keystr, sizeof(keystr));
        z_view_keyexpr_t ke;
        if (z_view_keyexpr_from_str(&ke, keystr) < 0) continue;
        z_owned_closure_query_t closure;
        z_closure(&closure, server_query_cb, NULL, h);
        if (z_declare_queryable(z_loan(srv->session), &h->z_qable,
                                z_loan(ke), z_move(closure), NULL) >= 0) {
            h->declared = 1;
        }
    }
    pthread_mutex_unlock(&srv->lock);
    return ZRPC_OK;
}

zrpc_status_t zenoh_rpc_server_stop(ZenohRpcServer *srv) {
    if (!srv) return ZRPC_ERR_INVALID_ARG;
    pthread_mutex_lock(&srv->lock);
    int was_running = srv->running;
    srv->running = 0;
    if (was_running) {
        for (handler_entry_t *h = srv->handlers; h != NULL; h = h->next) {
            if (h->declared) {
                z_drop(z_move(h->z_qable));
                h->declared = 0;
            }
        }
    }
    pthread_mutex_unlock(&srv->lock);
    if (was_running) z_drop(z_move(srv->session));
    return ZRPC_OK;
}

/* ------------------------------------------------------------------ */
/*  Client                                                             */
/* ------------------------------------------------------------------ */

struct ZenohRpcClient {
    ZenohRpcConfig    cfg_copy;
    int               connected;
    pthread_mutex_t   lock;
    z_owned_session_t session;
};

/* Per-call reply collector — heap-allocated so it survives the call
 * frame across the timedwait deadline path. */
typedef struct {
    pthread_mutex_t lock;
    pthread_cond_t  cond;
    int             reply_seen;
    int             final_seen;
    int             is_err;
    uint8_t        *payload;
    size_t          payload_len;
} reply_collector_t;

static reply_collector_t *collector_new(void) {
    reply_collector_t *c = calloc(1, sizeof(*c));
    if (!c) return NULL;
    pthread_mutex_init(&c->lock, NULL);
    pthread_cond_init(&c->cond, NULL);
    return c;
}

static void collector_free(reply_collector_t *c) {
    if (!c) return;
    pthread_cond_destroy(&c->cond);
    pthread_mutex_destroy(&c->lock);
    free(c->payload);
    free(c);
}

static void client_reply_cb(z_loaned_reply_t *reply, void *ctx) {
    reply_collector_t *c = (reply_collector_t *)ctx;
    pthread_mutex_lock(&c->lock);
    /* Only capture the first reply (matches synchronous _call() semantics). */
    if (!c->reply_seen) {
        if (z_reply_is_ok(reply)) {
            const z_loaned_sample_t *sample = z_reply_ok(reply);
            const z_loaned_bytes_t  *bytes  = z_sample_payload(sample);
            z_owned_slice_t slice;
            if (z_bytes_to_slice(bytes, &slice) >= 0) {
                size_t n = z_slice_len(z_loan(slice));
                c->payload     = malloc(n);
                if (c->payload) {
                    memcpy(c->payload, z_slice_data(z_loan(slice)), n);
                    c->payload_len = n;
                }
                z_drop(z_move(slice));
            }
            c->is_err = 0;
        } else {
            const z_loaned_reply_err_t *err   = z_reply_err(reply);
            const z_loaned_bytes_t     *bytes = z_reply_err_payload(err);
            z_owned_slice_t slice;
            if (z_bytes_to_slice(bytes, &slice) >= 0) {
                size_t n = z_slice_len(z_loan(slice));
                c->payload     = malloc(n);
                if (c->payload) {
                    memcpy(c->payload, z_slice_data(z_loan(slice)), n);
                    c->payload_len = n;
                }
                z_drop(z_move(slice));
            }
            c->is_err = 1;
        }
        c->reply_seen = 1;
        pthread_cond_broadcast(&c->cond);
    }
    pthread_mutex_unlock(&c->lock);
}

static void client_reply_dropper(void *ctx) {
    reply_collector_t *c = (reply_collector_t *)ctx;
    pthread_mutex_lock(&c->lock);
    c->final_seen = 1;
    pthread_cond_broadcast(&c->cond);
    pthread_mutex_unlock(&c->lock);
    /* Don't free here — the caller (_call) frees when done. */
}

zrpc_status_t zenoh_rpc_client_create(ZenohRpcClient **out, const ZenohRpcConfig *cfg) {
    if (!out || !cfg) return ZRPC_ERR_INVALID_ARG;
    if (cfg->n_locators == 0 || cfg->locators == NULL) return ZRPC_ERR_INVALID_ARG;
    ZenohRpcClient *cli = calloc(1, sizeof(*cli));
    if (!cli) return ZRPC_ERR_MEMORY;
    cli->cfg_copy = *cfg;
    pthread_mutex_init(&cli->lock, NULL);
    *out = cli;
    return ZRPC_OK;
}

void zenoh_rpc_client_destroy(ZenohRpcClient *cli) {
    if (!cli) return;
    if (cli->connected) zenoh_rpc_client_disconnect(cli);
    pthread_mutex_destroy(&cli->lock);
    free(cli);
}

zrpc_status_t zenoh_rpc_client_connect(ZenohRpcClient *cli) {
    if (!cli) return ZRPC_ERR_INVALID_ARG;
    if (open_session_from_cfg(&cli->session, &cli->cfg_copy) < 0) {
        return ZRPC_ERR_CONNECTION;
    }
    pthread_mutex_lock(&cli->lock);
    cli->connected = 1;
    pthread_mutex_unlock(&cli->lock);
    return ZRPC_OK;
}

zrpc_status_t zenoh_rpc_client_disconnect(ZenohRpcClient *cli) {
    if (!cli) return ZRPC_ERR_INVALID_ARG;
    pthread_mutex_lock(&cli->lock);
    int was_connected = cli->connected;
    cli->connected = 0;
    pthread_mutex_unlock(&cli->lock);
    if (was_connected) z_drop(z_move(cli->session));
    return ZRPC_OK;
}

zrpc_status_t zenoh_rpc_client_call(ZenohRpcClient *cli,
                                    uint32_t token,
                                    const uint8_t *req,
                                    size_t req_len,
                                    uint32_t timeout_ms,
                                    uint8_t **resp,
                                    size_t *resp_len) {
    if (!cli || !resp || !resp_len) return ZRPC_ERR_INVALID_ARG;
    if (req_len > 0 && req == NULL) return ZRPC_ERR_INVALID_ARG;
    if (!cli->connected) return ZRPC_ERR_NOT_CONNECTED;

    char keystr[32];
    token_to_keyexpr(token, keystr, sizeof(keystr));

    z_view_keyexpr_t ke;
    if (z_view_keyexpr_from_str(&ke, keystr) < 0) return ZRPC_ERR_INVALID_ARG;

    reply_collector_t *col = collector_new();
    if (!col) return ZRPC_ERR_MEMORY;

    z_get_options_t opts;
    z_get_options_default(&opts);
    if (timeout_ms > 0) opts.timeout_ms = timeout_ms;

    z_owned_bytes_t reqbody;
    if (req_len > 0) {
        if (z_bytes_copy_from_buf(&reqbody, req, req_len) < 0) {
            collector_free(col);
            return ZRPC_ERR_MEMORY;
        }
        opts.payload = z_move(reqbody);
    }

    z_owned_closure_reply_t closure;
    z_closure(&closure, client_reply_cb, client_reply_dropper, col);
    if (z_get(z_loan(cli->session), z_loan(ke), "", z_move(closure), &opts) < 0) {
        collector_free(col);
        return ZRPC_ERR_ZENOH;
    }

    /* Wait for first reply or final marker, with the timeout deadline. */
    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec  += timeout_ms / 1000;
    deadline.tv_nsec += (timeout_ms % 1000) * 1000000L;
    if (deadline.tv_nsec >= 1000000000L) {
        deadline.tv_nsec -= 1000000000L;
        deadline.tv_sec  += 1;
    }

    pthread_mutex_lock(&col->lock);
    while (!col->reply_seen && !col->final_seen) {
        int rc = pthread_cond_timedwait(&col->cond, &col->lock, &deadline);
        if (rc == ETIMEDOUT) {
            pthread_mutex_unlock(&col->lock);
            collector_free(col);
            return ZRPC_ERR_TIMEOUT;
        }
    }

    zrpc_status_t result;
    if (col->reply_seen) {
        if (col->is_err) {
            result = ZRPC_ERR_HANDLER;
            *resp     = col->payload;   /* hand error message to caller */
            *resp_len = col->payload_len;
            col->payload = NULL;        /* prevent free in collector_free */
        } else {
            result = ZRPC_OK;
            *resp     = col->payload;
            *resp_len = col->payload_len;
            col->payload = NULL;
        }
    } else {
        result    = ZRPC_ERR_NO_REPLY;
        *resp     = NULL;
        *resp_len = 0;
    }
    pthread_mutex_unlock(&col->lock);

    collector_free(col);
    return result;
}
