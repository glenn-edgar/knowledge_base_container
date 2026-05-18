/**
 * @file nats_pubsub.c
 * @brief NATS Publish/Subscribe library — implementation
 *
 * Translated from Python NatsPubSub.  Uses nats.c for transport.
 */

#define _GNU_SOURCE   /* strdup */

#include "nats_pubsub.h"

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

#define PS_MAX_SUBS      256
#define PS_SUBJECT_LEN   256
#define PS_NAME_LEN      128

/* ------------------------------------------------------------------ */
/*  Status strings                                                     */
/* ------------------------------------------------------------------ */

static const char *status_strings[] = {
    "ok", "invalid_arg", "connection", "timeout",
    "memory", "not_connected", "nats_error"
};

const char *ps_status_str(ps_status_t st)
{
    if (st >= 0 && st <= PS_ERR_NATS)
        return status_strings[st];
    return "unknown";
}

/* ------------------------------------------------------------------ */
/*  Subscription handle                                                */
/* ------------------------------------------------------------------ */

/* Queue mode: incoming messages are heap-copied into ring buffer slots. */
typedef struct {
    char *subject;          /* malloc'd */
    char *original_subject;
    uint8_t *data;
    int      data_len;
    char *reply_to;
} qmsg_t;

typedef struct {
    pthread_mutex_t lock;
    qmsg_t  *ring;          /* depth slots */
    size_t   depth;         /* power of two */
    size_t   mask;
    size_t   head;
    size_t   tail;
    size_t   dropped;
} sub_queue_t;

struct PubSubSub {
    natsSubscription *nsub;            /* nats.c subscription */
    char              original_subject[PS_SUBJECT_LEN]; /* without namespace */
    char              full_subject[PS_SUBJECT_LEN];     /* with namespace */
    pubsub_msg_cb     cb;
    void             *user_data;
    PubSub           *ps;              /* back-pointer */
    int64_t           msgs_received;
    sub_queue_t      *queue;           /* non-NULL = queue mode (no cb) */
};

/* ------------------------------------------------------------------ */
/*  PubSub handle                                                      */
/* ------------------------------------------------------------------ */

struct PubSub {
    /* Config */
    char              server[256];
    char              namespace_[PS_NAME_LEN];
    char              client_name[PS_NAME_LEN];

    /* NATS connection */
    natsConnection   *conn;
    natsOptions      *opts;
    bool              connected;

    /* Subscriptions */
    PubSubSub        *subs[PS_MAX_SUBS];
    int               sub_count;

    /* Stats */
    int64_t           msgs_published;
    int64_t           msgs_received;

    /* Mutex for stats and sub list */
    pthread_mutex_t   mu;
};

/* ------------------------------------------------------------------ */
/*  Utility: namespace helpers                                         */
/* ------------------------------------------------------------------ */

static void add_namespace(char *buf, size_t len,
                          const char *ns, const char *subject)
{
    if (!subject || !subject[0]) {
        buf[0] = '\0';
        return;
    }
    /* Internal subjects (starting with _) skip namespace */
    if (subject[0] == '_') {
        snprintf(buf, len, "%s", subject);
    } else {
        snprintf(buf, len, "%s.%s", ns, subject);
    }
}

static void remove_namespace(char *buf, size_t len,
                             const char *ns, const char *subject)
{
    size_t ns_len = strlen(ns);
    if (strncmp(subject, ns, ns_len) == 0 && subject[ns_len] == '.') {
        snprintf(buf, len, "%s", subject + ns_len + 1);
    } else {
        snprintf(buf, len, "%s", subject);
    }
}

/* ------------------------------------------------------------------ */
/*  Utility: generate client name                                      */
/* ------------------------------------------------------------------ */

static void gen_client_name(char *buf, size_t len)
{
    unsigned r = (unsigned)time(NULL) ^ (unsigned)clock();
    snprintf(buf, len, "pubsub_%08x", r);
}

/* ------------------------------------------------------------------ */
/*  Config defaults                                                    */
/* ------------------------------------------------------------------ */

void pubsub_config_defaults(PubSubConfig *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->server      = "nats://127.0.0.1:4222";
    cfg->namespace_  = "default";
    cfg->client_name = NULL;
}

/* ------------------------------------------------------------------ */
/*  Create / Destroy                                                   */
/* ------------------------------------------------------------------ */

ps_status_t pubsub_create(PubSub **out, const PubSubConfig *cfg)
{
    if (!out || !cfg) return PS_ERR_INVALID_ARG;

    PubSub *ps = calloc(1, sizeof(*ps));
    if (!ps) return PS_ERR_MEMORY;

    snprintf(ps->server, sizeof(ps->server), "%s",
             cfg->server ? cfg->server : "nats://127.0.0.1:4222");
    snprintf(ps->namespace_, sizeof(ps->namespace_), "%s",
             cfg->namespace_ ? cfg->namespace_ : "default");

    if (cfg->client_name && cfg->client_name[0]) {
        snprintf(ps->client_name, sizeof(ps->client_name), "%s",
                 cfg->client_name);
    } else {
        gen_client_name(ps->client_name, sizeof(ps->client_name));
    }

    pthread_mutex_init(&ps->mu, NULL);

    *out = ps;
    return PS_OK;
}

void pubsub_destroy(PubSub *ps)
{
    if (!ps) return;

    /* Unsubscribe all */
    for (int i = 0; i < ps->sub_count; i++) {
        if (ps->subs[i]) {
            if (ps->subs[i]->nsub) {
                natsSubscription_Unsubscribe(ps->subs[i]->nsub);
                natsSubscription_Destroy(ps->subs[i]->nsub);
            }
            free(ps->subs[i]);
            ps->subs[i] = NULL;
        }
    }

    if (ps->connected) pubsub_disconnect(ps);

    if (ps->conn) {
        natsConnection_Destroy(ps->conn);
        ps->conn = NULL;
    }
    if (ps->opts) {
        natsOptions_Destroy(ps->opts);
        ps->opts = NULL;
    }

    pthread_mutex_destroy(&ps->mu);
    free(ps);
}

/* ------------------------------------------------------------------ */
/*  Connect / Disconnect                                               */
/* ------------------------------------------------------------------ */

ps_status_t pubsub_connect(PubSub *ps)
{
    if (!ps) return PS_ERR_INVALID_ARG;
    if (ps->connected) return PS_OK;

    natsStatus ns = natsOptions_Create(&ps->opts);
    if (ns != NATS_OK) return PS_ERR_NATS;

    natsOptions_SetURL(ps->opts, ps->server);
    natsOptions_SetName(ps->opts, ps->client_name);

    ns = natsConnection_Connect(&ps->conn, ps->opts);
    if (ns != NATS_OK) {
        fprintf(stderr, "PubSub: failed to connect to %s: %s\n",
                ps->server, natsStatus_GetText(ns));
        return PS_ERR_CONNECTION;
    }

    ps->connected = true;
    printf("PubSub connected to %s (namespace: %s, client: %s)\n",
           ps->server, ps->namespace_, ps->client_name);
    return PS_OK;
}

ps_status_t pubsub_disconnect(PubSub *ps)
{
    if (!ps) return PS_ERR_INVALID_ARG;
    if (!ps->connected) return PS_OK;

    /* Unsubscribe all active subscriptions */
    for (int i = 0; i < ps->sub_count; i++) {
        if (ps->subs[i] && ps->subs[i]->nsub) {
            natsSubscription_Unsubscribe(ps->subs[i]->nsub);
            natsSubscription_Destroy(ps->subs[i]->nsub);
            ps->subs[i]->nsub = NULL;
        }
    }

    if (ps->conn) {
        natsConnection_Flush(ps->conn);
        natsConnection_Close(ps->conn);
        natsConnection_Destroy(ps->conn);
        ps->conn = NULL;
    }

    ps->connected = false;
    printf("PubSub disconnected (namespace: %s)\n", ps->namespace_);
    return PS_OK;
}

bool pubsub_is_connected(const PubSub *ps)
{
    return ps ? ps->connected : false;
}

const char *pubsub_namespace(const PubSub *ps)
{
    return ps ? ps->namespace_ : NULL;
}

const char *pubsub_client_name(const PubSub *ps)
{
    return ps ? ps->client_name : NULL;
}

/* ------------------------------------------------------------------ */
/*  Publish                                                            */
/* ------------------------------------------------------------------ */

ps_status_t pubsub_publish(PubSub *ps, const char *subject,
                           const void *data, int data_len)
{
    if (!ps || !subject) return PS_ERR_INVALID_ARG;
    if (!ps->connected)  return PS_ERR_NOT_CONNECTED;

    char full_subject[PS_SUBJECT_LEN];
    add_namespace(full_subject, sizeof(full_subject),
                  ps->namespace_, subject);

    natsStatus ns = natsConnection_Publish(ps->conn, full_subject,
                                           data, data_len);
    if (ns != NATS_OK) return PS_ERR_NATS;

    pthread_mutex_lock(&ps->mu);
    ps->msgs_published++;
    pthread_mutex_unlock(&ps->mu);

    return PS_OK;
}

ps_status_t pubsub_publish_str(PubSub *ps, const char *subject,
                               const char *str)
{
    if (!str) return PS_ERR_INVALID_ARG;
    return pubsub_publish(ps, subject, str, (int)strlen(str));
}

/* ------------------------------------------------------------------ */
/*  Subscribe — internal nats.c callback adapter                       */
/* ------------------------------------------------------------------ */

static void nats_msg_callback(natsConnection *nc, natsSubscription *nsub,
                              natsMsg *msg, void *closure)
{
    (void)nc;
    (void)nsub;

    PubSubSub *sub = closure;
    if (!sub) { natsMsg_Destroy(msg); return; }

    char orig[PS_SUBJECT_LEN];
    remove_namespace(orig, sizeof(orig),
                     sub->ps->namespace_, natsMsg_GetSubject(msg));

    if (sub->queue) {
        /* Queue mode: heap-copy the message into a ring slot. */
        sub_queue_t *q = sub->queue;
        const char *subj = natsMsg_GetSubject(msg);
        const char *reply = natsMsg_GetReply(msg);
        const char *data  = natsMsg_GetData(msg);
        int         dlen  = natsMsg_GetDataLength(msg);

        qmsg_t copy = {0};
        copy.subject          = subj   ? strdup(subj)  : NULL;
        copy.original_subject = strdup(orig);
        copy.reply_to         = reply  ? strdup(reply) : NULL;
        copy.data_len         = dlen;
        if (dlen > 0) {
            copy.data = malloc(dlen);
            if (copy.data) memcpy(copy.data, data, dlen);
        }

        pthread_mutex_lock(&q->lock);
        size_t used = q->head - q->tail;
        if (used >= q->depth) {
            /* Overflow: drop oldest. */
            qmsg_t *old = &q->ring[q->tail & q->mask];
            free(old->subject);
            free(old->original_subject);
            free(old->data);
            free(old->reply_to);
            memset(old, 0, sizeof(*old));
            q->tail++;
            q->dropped++;
        }
        q->ring[q->head & q->mask] = copy;
        q->head++;
        pthread_mutex_unlock(&q->lock);
    } else if (sub->cb) {
        /* Callback mode (original): invoke on this dispatch thread. */
        PubSubMsg pmsg = {
            .subject          = natsMsg_GetSubject(msg),
            .original_subject = orig,
            .data             = natsMsg_GetData(msg),
            .data_len         = natsMsg_GetDataLength(msg),
            .reply_to         = natsMsg_GetReply(msg),
        };
        sub->cb(&pmsg, sub->user_data);
    }

    pthread_mutex_lock(&sub->ps->mu);
    sub->msgs_received++;
    sub->ps->msgs_received++;
    pthread_mutex_unlock(&sub->ps->mu);

    natsMsg_Destroy(msg);
}

/* ------------------------------------------------------------------ */
/*  Subscribe — internal helper                                        */
/* ------------------------------------------------------------------ */

static ps_status_t do_subscribe(PubSub *ps, const char *full_subject,
                                const char *original_subject,
                                pubsub_msg_cb cb, void *user_data,
                                const char *queue, sub_queue_t *q_attach,
                                PubSubSub **out)
{
    if (!ps || !full_subject || !out)
        return PS_ERR_INVALID_ARG;
    /* Either cb (callback mode) or q_attach (queue mode) must be set */
    if (!cb && !q_attach)
        return PS_ERR_INVALID_ARG;
    if (!ps->connected)
        return PS_ERR_NOT_CONNECTED;
    if (ps->sub_count >= PS_MAX_SUBS)
        return PS_ERR_MEMORY;

    PubSubSub *sub = calloc(1, sizeof(*sub));
    if (!sub) return PS_ERR_MEMORY;

    snprintf(sub->original_subject, sizeof(sub->original_subject),
             "%s", original_subject);
    snprintf(sub->full_subject, sizeof(sub->full_subject),
             "%s", full_subject);
    sub->cb        = cb;
    sub->user_data = user_data;
    sub->ps        = ps;
    sub->queue     = q_attach;

    natsStatus ns;
    if (queue && queue[0]) {
        ns = natsConnection_QueueSubscribe(&sub->nsub, ps->conn,
                                           full_subject, queue,
                                           nats_msg_callback, sub);
    } else {
        ns = natsConnection_Subscribe(&sub->nsub, ps->conn,
                                      full_subject,
                                      nats_msg_callback, sub);
    }

    if (ns != NATS_OK) {
        fprintf(stderr, "PubSub: subscribe failed for %s: %s\n",
                full_subject, natsStatus_GetText(ns));
        free(sub);
        return PS_ERR_NATS;
    }

    natsConnection_Flush(ps->conn);

    pthread_mutex_lock(&ps->mu);
    ps->subs[ps->sub_count++] = sub;
    pthread_mutex_unlock(&ps->mu);

    *out = sub;
    return PS_OK;
}

/* ------------------------------------------------------------------ */
/*  Subscribe — public API                                             */
/* ------------------------------------------------------------------ */

ps_status_t pubsub_subscribe(PubSub *ps, const char *subject,
                             pubsub_msg_cb cb, void *user_data,
                             const char *queue, PubSubSub **sub)
{
    if (!ps || !subject) return PS_ERR_INVALID_ARG;

    char full_subject[PS_SUBJECT_LEN];
    add_namespace(full_subject, sizeof(full_subject),
                  ps->namespace_, subject);

    return do_subscribe(ps, full_subject, subject, cb, user_data, queue, NULL, sub);
}

ps_status_t pubsub_subscribe_raw(PubSub *ps, const char *subject,
                                 pubsub_msg_cb cb, void *user_data,
                                 const char *queue, PubSubSub **sub)
{
    if (!ps || !subject) return PS_ERR_INVALID_ARG;
    return do_subscribe(ps, subject, subject, cb, user_data, queue, NULL, sub);
}

/* ------------------------------------------------------------------ */
/*  Queue + poll subscribe (LuaJIT-safe)                               */
/* ------------------------------------------------------------------ */

static size_t round_up_pow2(size_t v) {
    if (v <= 1) return 1;
    size_t p = 1;
    while (p < v) p <<= 1;
    return p;
}

ps_status_t pubsub_subscribe_queue(PubSub *ps, const char *subject,
                                   size_t queue_depth, const char *queue_group,
                                   bool raw, PubSubSub **out)
{
    if (!ps || !subject || !out) return PS_ERR_INVALID_ARG;

    size_t depth = round_up_pow2(queue_depth == 0 ? 64 : queue_depth);

    sub_queue_t *q = calloc(1, sizeof(*q));
    if (!q) return PS_ERR_MEMORY;
    q->ring = calloc(depth, sizeof(qmsg_t));
    if (!q->ring) { free(q); return PS_ERR_MEMORY; }
    q->depth = depth;
    q->mask  = depth - 1;
    pthread_mutex_init(&q->lock, NULL);

    char full_subject[PS_SUBJECT_LEN];
    if (raw) {
        snprintf(full_subject, sizeof(full_subject), "%s", subject);
    } else {
        add_namespace(full_subject, sizeof(full_subject),
                      ps->namespace_, subject);
    }
    ps_status_t st = do_subscribe(ps, full_subject, subject,
                                  NULL, NULL, queue_group, q, out);
    if (st != PS_OK) {
        free(q->ring);
        pthread_mutex_destroy(&q->lock);
        free(q);
    }
    return st;
}

ps_status_t pubsub_poll(PubSubSub *sub, PubSubOwnedMsg *out_msg)
{
    if (!sub || !out_msg) return PS_ERR_INVALID_ARG;
    if (!sub->queue) return PS_ERR_INVALID_ARG;
    sub_queue_t *q = sub->queue;

    pthread_mutex_lock(&q->lock);
    if (q->head == q->tail) {
        pthread_mutex_unlock(&q->lock);
        memset(out_msg, 0, sizeof(*out_msg));
        return PS_EMPTY;
    }
    qmsg_t *slot = &q->ring[q->tail & q->mask];
    out_msg->subject          = slot->subject;
    out_msg->original_subject = slot->original_subject;
    out_msg->data             = slot->data;
    out_msg->data_len         = slot->data_len;
    out_msg->reply_to         = slot->reply_to;
    memset(slot, 0, sizeof(*slot));
    q->tail++;
    pthread_mutex_unlock(&q->lock);
    return PS_OK;
}

void pubsub_msg_free(PubSubOwnedMsg *msg)
{
    if (!msg) return;
    free(msg->subject);
    free(msg->original_subject);
    free(msg->data);
    free(msg->reply_to);
    memset(msg, 0, sizeof(*msg));
}

size_t pubsub_pending(PubSubSub *sub)
{
    if (!sub || !sub->queue) return 0;
    sub_queue_t *q = sub->queue;
    pthread_mutex_lock(&q->lock);
    size_t n = q->head - q->tail;
    pthread_mutex_unlock(&q->lock);
    return n;
}

size_t pubsub_dropped(PubSubSub *sub)
{
    if (!sub || !sub->queue) return 0;
    sub_queue_t *q = sub->queue;
    pthread_mutex_lock(&q->lock);
    size_t n = q->dropped;
    pthread_mutex_unlock(&q->lock);
    return n;
}

void pubsub_reset_dropped(PubSubSub *sub)
{
    if (!sub || !sub->queue) return;
    sub_queue_t *q = sub->queue;
    pthread_mutex_lock(&q->lock);
    q->dropped = 0;
    pthread_mutex_unlock(&q->lock);
}

/* ------------------------------------------------------------------ */
/*  Unsubscribe                                                        */
/* ------------------------------------------------------------------ */

ps_status_t pubsub_unsubscribe(PubSub *ps, PubSubSub *sub)
{
    if (!ps || !sub) return PS_ERR_INVALID_ARG;

    if (sub->nsub) {
        natsSubscription_Unsubscribe(sub->nsub);
        natsSubscription_Destroy(sub->nsub);
        sub->nsub = NULL;
    }

    /* Remove from array */
    pthread_mutex_lock(&ps->mu);
    for (int i = 0; i < ps->sub_count; i++) {
        if (ps->subs[i] == sub) {
            ps->subs[i] = ps->subs[ps->sub_count - 1];
            ps->subs[ps->sub_count - 1] = NULL;
            ps->sub_count--;
            break;
        }
    }
    pthread_mutex_unlock(&ps->mu);

    /* Drain + free queue if queue-mode. */
    if (sub->queue) {
        sub_queue_t *q = sub->queue;
        pthread_mutex_lock(&q->lock);
        while (q->tail != q->head) {
            qmsg_t *slot = &q->ring[q->tail & q->mask];
            free(slot->subject);
            free(slot->original_subject);
            free(slot->data);
            free(slot->reply_to);
            q->tail++;
        }
        pthread_mutex_unlock(&q->lock);
        pthread_mutex_destroy(&q->lock);
        free(q->ring);
        free(q);
    }

    free(sub);
    return PS_OK;
}

ps_status_t pubsub_auto_unsubscribe(PubSubSub *sub, int max_msgs)
{
    if (!sub || !sub->nsub) return PS_ERR_INVALID_ARG;

    natsStatus ns = natsSubscription_AutoUnsubscribe(sub->nsub, max_msgs);
    if (ns != NATS_OK) return PS_ERR_NATS;
    return PS_OK;
}

const char *pubsub_sub_subject(const PubSubSub *sub)
{
    return sub ? sub->original_subject : NULL;
}

/* ------------------------------------------------------------------ */
/*  Request / Reply                                                    */
/* ------------------------------------------------------------------ */

ps_status_t pubsub_request(PubSub *ps, const char *subject,
                           const void *data, int data_len,
                           double timeout_sec,
                           char **reply_data, int *reply_len)
{
    if (!ps || !subject || !reply_data || !reply_len)
        return PS_ERR_INVALID_ARG;
    if (!ps->connected) return PS_ERR_NOT_CONNECTED;

    *reply_data = NULL;
    *reply_len  = 0;

    char full_subject[PS_SUBJECT_LEN];
    add_namespace(full_subject, sizeof(full_subject),
                  ps->namespace_, subject);

    int64_t timeout_ms = (int64_t)(timeout_sec * 1000.0);
    if (timeout_ms < 100) timeout_ms = 100;

    natsMsg *reply = NULL;
    natsStatus ns = natsConnection_Request(&reply, ps->conn, full_subject,
                                           data, data_len, timeout_ms);

    pthread_mutex_lock(&ps->mu);
    ps->msgs_published++;
    pthread_mutex_unlock(&ps->mu);

    if (ns == NATS_TIMEOUT || ns == NATS_NO_RESPONDERS) {
        return PS_ERR_TIMEOUT;
    }
    if (ns != NATS_OK) {
        return PS_ERR_NATS;
    }

    const char *rdata = natsMsg_GetData(reply);
    int rlen = natsMsg_GetDataLength(reply);

    if (rdata && rlen > 0) {
        *reply_data = malloc((size_t)rlen + 1);
        if (!*reply_data) {
            natsMsg_Destroy(reply);
            return PS_ERR_MEMORY;
        }
        memcpy(*reply_data, rdata, (size_t)rlen);
        (*reply_data)[rlen] = '\0';   /* NUL-terminate for convenience */
        *reply_len = rlen;
    }

    natsMsg_Destroy(reply);
    return PS_OK;
}

ps_status_t pubsub_request_str(PubSub *ps, const char *subject,
                               const char *str, double timeout_sec,
                               char **reply_str)
{
    if (!str || !reply_str) return PS_ERR_INVALID_ARG;
    int reply_len = 0;
    return pubsub_request(ps, subject, str, (int)strlen(str),
                          timeout_sec, reply_str, &reply_len);
}

/* ------------------------------------------------------------------ */
/*  Reply                                                              */
/* ------------------------------------------------------------------ */

ps_status_t pubsub_reply(PubSub *ps, const char *reply_to,
                         const void *data, int data_len)
{
    if (!ps || !reply_to) return PS_ERR_INVALID_ARG;
    if (!ps->connected)   return PS_ERR_NOT_CONNECTED;

    natsStatus ns = natsConnection_Publish(ps->conn, reply_to, data, data_len);
    return (ns == NATS_OK) ? PS_OK : PS_ERR_NATS;
}

ps_status_t pubsub_reply_str(PubSub *ps, const char *reply_to,
                             const char *str)
{
    if (!str) return PS_ERR_INVALID_ARG;
    return pubsub_reply(ps, reply_to, str, (int)strlen(str));
}

/* ------------------------------------------------------------------ */
/*  Statistics                                                         */
/* ------------------------------------------------------------------ */

ps_status_t pubsub_get_stats(const PubSub *ps, PubSubStats *stats)
{
    if (!ps || !stats) return PS_ERR_INVALID_ARG;

    stats->msgs_published      = ps->msgs_published;
    stats->msgs_received       = ps->msgs_received;
    stats->active_subscriptions = ps->sub_count;
    return PS_OK;
}
