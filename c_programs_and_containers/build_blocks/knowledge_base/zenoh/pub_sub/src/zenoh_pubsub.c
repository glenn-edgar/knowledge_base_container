/**
 * @file zenoh_pubsub.c
 * @brief zenoh-pico-backed publish/subscribe wrapper.
 *
 * Wire keyexpr is "tok/%08x" where %08x is the FNV1a-32 token in lowercase
 * hex. This keeps the on-the-wire keys compact (12 ASCII chars) and human-
 * readable in zenoh-pico traces.
 *
 * Session lifecycle uses z_open / z_drop. Publish uses z_put (one-shot, no
 * publisher caching). Subscribe uses z_declare_subscriber with a closure
 * that adapts zenoh-pico's z_loaned_sample_t to the caller's
 * zenoh_pubsub_callback_t.
 */

#define _POSIX_C_SOURCE 200809L

#include "zenoh_pubsub.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <zenoh-pico.h>

/* ------------------------------------------------------------------ */
/*  Status                                                             */
/* ------------------------------------------------------------------ */

const char *zps_status_str(zps_status_t st) {
    switch (st) {
        case ZPS_OK:                return "OK";
        case ZPS_ERR_INVALID_ARG:   return "invalid argument";
        case ZPS_ERR_CONNECTION:    return "connection error";
        case ZPS_ERR_TIMEOUT:       return "timeout";
        case ZPS_ERR_MEMORY:        return "out of memory";
        case ZPS_ERR_NOT_CONNECTED: return "not connected";
        case ZPS_ERR_ZENOH:         return "zenoh-pico error";
    }
    return "unknown";
}

void zenoh_pubsub_config_defaults(ZenohPubSubConfig *cfg) {
    if (!cfg) return;
    cfg->locators        = NULL;
    cfg->n_locators      = 0;
    cfg->listen_locators = NULL;
    cfg->n_listen        = 0;
    cfg->mode            = "client";
    cfg->enable_scout    = false;
    cfg->client_name     = NULL;
}

/* ------------------------------------------------------------------ */
/*  Handles                                                            */
/* ------------------------------------------------------------------ */

struct ZenohPubSub {
    ZenohPubSubConfig cfg_copy;
    int               connected;
    pthread_mutex_t   lock;
    z_owned_session_t session;
};

/* Adapter that bridges zenoh-pico's closure callback to user's typed callback. */
typedef struct {
    uint32_t                 token;
    zenoh_pubsub_callback_t  user_cb;
    void                    *user_ctx;
} sub_adapter_t;

struct ZenohPubSubSub {
    z_owned_subscriber_t  z_sub;
    sub_adapter_t        *adapter;   /* lifetime tied to closure's drop */
};

/* ------------------------------------------------------------------ */
/*  Helpers                                                            */
/* ------------------------------------------------------------------ */

/* Render an FNV1a token as a stable keyexpr: "tok/cafebabe". */
static void token_to_keyexpr(uint32_t token, char *buf, size_t n) {
    snprintf(buf, n, "tok/%08x", token);
}

/* Closure callback: zenoh-pico delivers a sample, we hand bytes to user_cb. */
static void sample_handler(z_loaned_sample_t *sample, void *ctx) {
    sub_adapter_t *ad = (sub_adapter_t *)ctx;
    if (!ad || !ad->user_cb) return;

    const z_loaned_bytes_t *bytes = z_sample_payload(sample);
    z_owned_slice_t slice;
    if (z_bytes_to_slice(bytes, &slice) < 0) return;
    const uint8_t *data = z_slice_data(z_loan(slice));
    size_t         len  = z_slice_len(z_loan(slice));

    ad->user_cb(ad->token, data, len, ad->user_ctx);
    z_drop(z_move(slice));
}

/* Closure drop: free the adapter when the subscriber is undeclared. */
static void sample_dropper(void *ctx) {
    free(ctx);
}

/* ------------------------------------------------------------------ */
/*  Lifecycle                                                          */
/* ------------------------------------------------------------------ */

zps_status_t zenoh_pubsub_create(ZenohPubSub **out, const ZenohPubSubConfig *cfg) {
    if (!out || !cfg) return ZPS_ERR_INVALID_ARG;
    /* Require at least one connect OR listen locator. */
    int has_connect = (cfg->n_locators > 0 && cfg->locators != NULL);
    int has_listen  = (cfg->n_listen   > 0 && cfg->listen_locators != NULL);
    if (!has_connect && !has_listen) return ZPS_ERR_INVALID_ARG;

    ZenohPubSub *ps = calloc(1, sizeof(*ps));
    if (!ps) return ZPS_ERR_MEMORY;
    ps->cfg_copy = *cfg;
    pthread_mutex_init(&ps->lock, NULL);
    *out = ps;
    return ZPS_OK;
}

void zenoh_pubsub_destroy(ZenohPubSub *ps) {
    if (!ps) return;
    if (ps->connected) zenoh_pubsub_disconnect(ps);
    pthread_mutex_destroy(&ps->lock);
    free(ps);
}

zps_status_t zenoh_pubsub_connect(ZenohPubSub *ps) {
    if (!ps) return ZPS_ERR_INVALID_ARG;

    z_owned_config_t cfg;
    z_config_default(&cfg);
    zp_config_insert(z_loan_mut(cfg), Z_CONFIG_MODE_KEY, ps->cfg_copy.mode);
    for (size_t i = 0; i < ps->cfg_copy.n_locators; ++i) {
        zp_config_insert(z_loan_mut(cfg), Z_CONFIG_CONNECT_KEY, ps->cfg_copy.locators[i]);
    }
    for (size_t i = 0; i < ps->cfg_copy.n_listen; ++i) {
        zp_config_insert(z_loan_mut(cfg), Z_CONFIG_LISTEN_KEY, ps->cfg_copy.listen_locators[i]);
    }
    /* Scouting: zenoh-pico defaults to no multicast scouting on TCP/UDP unicast,
     * so leaving enable_scout as false requires no extra config. */

    if (z_open(&ps->session, z_move(cfg), NULL) < 0) {
        return ZPS_ERR_CONNECTION;
    }

    pthread_mutex_lock(&ps->lock);
    ps->connected = 1;
    pthread_mutex_unlock(&ps->lock);
    return ZPS_OK;
}

zps_status_t zenoh_pubsub_disconnect(ZenohPubSub *ps) {
    if (!ps) return ZPS_ERR_INVALID_ARG;

    pthread_mutex_lock(&ps->lock);
    int was_connected = ps->connected;
    ps->connected = 0;
    pthread_mutex_unlock(&ps->lock);

    if (was_connected) {
        z_drop(z_move(ps->session));
    }
    return ZPS_OK;
}

/* ------------------------------------------------------------------ */
/*  Publish                                                            */
/* ------------------------------------------------------------------ */

zps_status_t zenoh_pubsub_publish(ZenohPubSub *ps,
                                  uint32_t token,
                                  const uint8_t *payload,
                                  size_t len) {
    if (!ps) return ZPS_ERR_INVALID_ARG;
    if (len > 0 && payload == NULL) return ZPS_ERR_INVALID_ARG;
    if (!ps->connected) return ZPS_ERR_NOT_CONNECTED;

    char keystr[32];
    token_to_keyexpr(token, keystr, sizeof(keystr));

    z_view_keyexpr_t ke;
    if (z_view_keyexpr_from_str(&ke, keystr) < 0) return ZPS_ERR_INVALID_ARG;

    z_owned_bytes_t body;
    if (z_bytes_copy_from_buf(&body, payload, len) < 0) return ZPS_ERR_MEMORY;

    if (z_put(z_loan(ps->session), z_loan(ke), z_move(body), NULL) < 0) {
        return ZPS_ERR_ZENOH;
    }
    return ZPS_OK;
}

/* ------------------------------------------------------------------ */
/*  Subscribe                                                          */
/* ------------------------------------------------------------------ */

zps_status_t zenoh_pubsub_subscribe(ZenohPubSub *ps,
                                    uint32_t token,
                                    zenoh_pubsub_callback_t cb,
                                    void *ctx,
                                    ZenohPubSubSub **out) {
    if (!ps || !cb || !out) return ZPS_ERR_INVALID_ARG;
    if (!ps->connected) return ZPS_ERR_NOT_CONNECTED;

    char keystr[32];
    token_to_keyexpr(token, keystr, sizeof(keystr));

    z_view_keyexpr_t ke;
    if (z_view_keyexpr_from_str(&ke, keystr) < 0) return ZPS_ERR_INVALID_ARG;

    ZenohPubSubSub *sub = calloc(1, sizeof(*sub));
    if (!sub) return ZPS_ERR_MEMORY;

    sub_adapter_t *ad = calloc(1, sizeof(*ad));
    if (!ad) { free(sub); return ZPS_ERR_MEMORY; }
    ad->token    = token;
    ad->user_cb  = cb;
    ad->user_ctx = ctx;

    z_owned_closure_sample_t closure;
    z_closure(&closure, sample_handler, sample_dropper, ad);
    sub->adapter = ad;

    if (z_declare_subscriber(z_loan(ps->session),
                             &sub->z_sub,
                             z_loan(ke),
                             z_move(closure),
                             NULL) < 0) {
        /* zenoh-pico drops the closure (and the adapter via sample_dropper) on
         * failure, so don't free(ad) here. */
        free(sub);
        return ZPS_ERR_ZENOH;
    }
    *out = sub;
    return ZPS_OK;
}

zps_status_t zenoh_pubsub_unsubscribe(ZenohPubSub *ps, ZenohPubSubSub *sub) {
    if (!ps || !sub) return ZPS_ERR_INVALID_ARG;
    z_drop(z_move(sub->z_sub));   /* sample_dropper fires here, freeing adapter */
    free(sub);
    return ZPS_OK;
}
