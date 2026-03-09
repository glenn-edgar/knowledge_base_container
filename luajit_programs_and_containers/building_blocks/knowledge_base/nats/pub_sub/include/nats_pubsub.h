/**
 * @file nats_pubsub.h
 * @brief NATS Publish/Subscribe library — C API
 *
 * Translated from Python NatsPubSub.  Uses the nats.c library for transport.
 *
 * Features:
 *   - Publish messages to subjects
 *   - Subscribe with callbacks (sync, runs on nats.c internal threads)
 *   - Wildcard subscriptions (* and >)
 *   - Queue group subscriptions (load balancing)
 *   - Request/reply pattern
 *   - Namespace-prefixed subjects
 *   - Pattern subscriptions with namespace
 *
 * Typical usage:
 *
 *   PubSub *ps;
 *   pubsub_create(&ps, &cfg);
 *   pubsub_connect(ps);
 *
 *   // Subscribe
 *   PubSubSub *sub;
 *   pubsub_subscribe(ps, "sensor.temp", my_callback, ctx, NULL, &sub);
 *
 *   // Publish
 *   pubsub_publish(ps, "sensor.temp", "{\"value\":23.5}", 16);
 *
 *   // Request/reply
 *   char *reply = NULL;
 *   int reply_len = 0;
 *   pubsub_request(ps, "service.echo", "hello", 5, 5.0, &reply, &reply_len);
 *   free(reply);
 *
 *   // Cleanup
 *   pubsub_unsubscribe(ps, sub);
 *   pubsub_disconnect(ps);
 *   pubsub_destroy(ps);
 */

#ifndef NATS_PUBSUB_H
#define NATS_PUBSUB_H

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
    PS_OK = 0,
    PS_ERR_INVALID_ARG,
    PS_ERR_CONNECTION,
    PS_ERR_TIMEOUT,
    PS_ERR_MEMORY,
    PS_ERR_NOT_CONNECTED,
    PS_ERR_NATS,            /**< Generic nats.c error                 */
} ps_status_t;

const char *ps_status_str(ps_status_t st);

/* ------------------------------------------------------------------ */
/*  Configuration                                                      */
/* ------------------------------------------------------------------ */

typedef struct {
    const char *server;          /**< e.g. "nats://127.0.0.1:4222"    */
    const char *namespace_;      /**< Subject prefix (default "default")*/
    const char *client_name;     /**< Client name (auto-generated if NULL)*/
} PubSubConfig;

/** Fill cfg with sensible defaults. */
void pubsub_config_defaults(PubSubConfig *cfg);

/* ------------------------------------------------------------------ */
/*  Message delivered to callbacks                                     */
/* ------------------------------------------------------------------ */

typedef struct {
    const char *subject;         /**< Full subject including namespace  */
    const char *original_subject;/**< Subject without namespace prefix  */
    const char *data;            /**< Payload bytes                     */
    int         data_len;        /**< Payload length                    */
    const char *reply_to;        /**< Reply subject, or NULL            */
} PubSubMsg;

/* ------------------------------------------------------------------ */
/*  Callback signature                                                 */
/* ------------------------------------------------------------------ */

/**
 * User-supplied message callback.
 *
 * Called on nats.c's internal dispatch thread.  Keep it short or
 * dispatch work elsewhere.
 *
 * @param msg        Message data (valid only for the duration of the call).
 * @param user_data  Opaque pointer registered with the subscription.
 */
typedef void (*pubsub_msg_cb)(const PubSubMsg *msg, void *user_data);

/* ------------------------------------------------------------------ */
/*  PubSub handle                                                      */
/* ------------------------------------------------------------------ */

typedef struct PubSub PubSub;

/** Create a PubSub client (does NOT connect). */
ps_status_t pubsub_create(PubSub **out, const PubSubConfig *cfg);

/** Destroy the client and free all resources. */
void pubsub_destroy(PubSub *ps);

/** Connect to the NATS server. */
ps_status_t pubsub_connect(PubSub *ps);

/** Disconnect from the NATS server. */
ps_status_t pubsub_disconnect(PubSub *ps);

/** Check whether the client is connected. */
bool pubsub_is_connected(const PubSub *ps);

/** Get the namespace. */
const char *pubsub_namespace(const PubSub *ps);

/** Get the client name. */
const char *pubsub_client_name(const PubSub *ps);

/* ------------------------------------------------------------------ */
/*  Publish                                                            */
/* ------------------------------------------------------------------ */

/**
 * Publish a message.
 *
 * The namespace is automatically prepended to the subject.
 *
 * @param subject   Subject name (without namespace).
 * @param data      Payload bytes.
 * @param data_len  Payload length.
 */
ps_status_t pubsub_publish(PubSub *ps, const char *subject,
                           const void *data, int data_len);

/**
 * Publish a NUL-terminated string (convenience).
 */
ps_status_t pubsub_publish_str(PubSub *ps, const char *subject,
                               const char *str);

/* ------------------------------------------------------------------ */
/*  Subscribe                                                          */
/* ------------------------------------------------------------------ */

/** Opaque subscription handle. */
typedef struct PubSubSub PubSubSub;

/**
 * Subscribe to a subject.
 *
 * Namespace is prepended automatically unless the subject starts with '_'.
 *
 * @param subject    Subject (supports NATS wildcards * and >).
 * @param cb         Callback invoked on each message.
 * @param user_data  Passed to callback.
 * @param queue      Queue group name, or NULL for no queue group.
 * @param[out] sub   Receives subscription handle.  Caller must
 *                   eventually call pubsub_unsubscribe().
 */
ps_status_t pubsub_subscribe(PubSub       *ps,
                             const char   *subject,
                             pubsub_msg_cb cb,
                             void         *user_data,
                             const char   *queue,
                             PubSubSub   **sub);

/**
 * Subscribe to a raw subject (no namespace prepended).
 */
ps_status_t pubsub_subscribe_raw(PubSub       *ps,
                                 const char   *subject,
                                 pubsub_msg_cb cb,
                                 void         *user_data,
                                 const char   *queue,
                                 PubSubSub   **sub);

/**
 * Unsubscribe and free subscription handle.
 */
ps_status_t pubsub_unsubscribe(PubSub *ps, PubSubSub *sub);

/**
 * Auto-unsubscribe after max_msgs messages.
 */
ps_status_t pubsub_auto_unsubscribe(PubSubSub *sub, int max_msgs);

/**
 * Get the original (un-namespaced) subject of a subscription.
 */
const char *pubsub_sub_subject(const PubSubSub *sub);

/* ------------------------------------------------------------------ */
/*  Request / Reply                                                    */
/* ------------------------------------------------------------------ */

/**
 * Send a request and wait for a reply (synchronous).
 *
 * Namespace is prepended to the subject.
 *
 * @param subject      Subject to send request to.
 * @param data         Request payload.
 * @param data_len     Request payload length.
 * @param timeout_sec  Timeout in seconds.
 * @param[out] reply_data  Receives malloc'd reply payload.  Caller frees.
 * @param[out] reply_len   Length of reply payload.
 *
 * @return PS_OK on success, PS_ERR_TIMEOUT if no reply.
 */
ps_status_t pubsub_request(PubSub     *ps,
                           const char *subject,
                           const void *data,
                           int         data_len,
                           double      timeout_sec,
                           char      **reply_data,
                           int        *reply_len);

/**
 * Request with a NUL-terminated string (convenience).
 * reply_data will be a malloc'd NUL-terminated string.
 */
ps_status_t pubsub_request_str(PubSub     *ps,
                               const char *subject,
                               const char *str,
                               double      timeout_sec,
                               char      **reply_str);

/**
 * Publish a reply to a message's reply_to subject.
 *
 * Use this inside a subscription callback to respond to a request.
 * No namespace is prepended (reply_to subjects are internal).
 *
 * @param reply_to  The reply_to field from the received PubSubMsg.
 * @param data      Reply payload.
 * @param data_len  Reply payload length.
 */
ps_status_t pubsub_reply(PubSub     *ps,
                         const char *reply_to,
                         const void *data,
                         int         data_len);

/** Reply with a NUL-terminated string (convenience). */
ps_status_t pubsub_reply_str(PubSub     *ps,
                             const char *reply_to,
                             const char *str);

/* ------------------------------------------------------------------ */
/*  Statistics                                                         */
/* ------------------------------------------------------------------ */

typedef struct {
    int64_t  msgs_published;
    int64_t  msgs_received;
    int      active_subscriptions;
} PubSubStats;

/** Get client statistics. */
ps_status_t pubsub_get_stats(const PubSub *ps, PubSubStats *stats);

#ifdef __cplusplus
}
#endif

#endif /* NATS_PUBSUB_H */

