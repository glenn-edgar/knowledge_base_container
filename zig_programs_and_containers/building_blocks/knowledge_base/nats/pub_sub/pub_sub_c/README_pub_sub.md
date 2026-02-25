# NATS PubSub — C Library

C translation of the Python `NatsPubSub` module. Standalone publish/subscribe library over NATS messaging.

## Structure

```
nats_pubsub/
├── include/
│   └── nats_pubsub.h       # Public API
├── src/
│   └── nats_pubsub.c       # Implementation
├── test/
│   └── test_nats_pubsub.c  # 17 tests + interactive demo
├── Makefile
└── README.md
```

## Prerequisites

### 1. Install nats.c (build from source)

```bash
git clone https://github.com/nats-io/nats.c.git
cd nats.c && mkdir build && cd build
cmake .. -DNATS_BUILD_STREAMING=OFF
make && sudo make install && sudo ldconfig
```

### 2. Start a NATS server

```bash
docker run -d -p 4222:4222 nats:latest
```

Note: JetStream is NOT required. cJSON is NOT required (PubSub works with raw bytes).

## Build & Run

```bash
make
make run-test       # tests
make run-demo       # interactive demo
```

## API Overview

### Connect / Disconnect

```c
PubSubConfig cfg;
pubsub_config_defaults(&cfg);
cfg.server     = "nats://127.0.0.1:4222";
cfg.namespace_ = "myapp";

PubSub *ps;
pubsub_create(&ps, &cfg);
pubsub_connect(ps);
// ... use ...
pubsub_disconnect(ps);
pubsub_destroy(ps);
```

### Publish

```c
pubsub_publish_str(ps, "sensor.temp", "23.5");
pubsub_publish(ps, "binary.topic", data, data_len);
```

### Subscribe

```c
void my_callback(const PubSubMsg *msg, void *ctx) {
    printf("Got: %.*s on %s\n", msg->data_len, msg->data, msg->original_subject);
}

PubSubSub *sub;
pubsub_subscribe(ps, "sensor.*", my_callback, ctx, NULL, &sub);
// Wildcards: * = single token, > = multi-level
pubsub_unsubscribe(ps, sub);
```

### Queue Groups (load balancing)

```c
pubsub_subscribe(ps, "tasks", handler, ctx, "workers", &sub);
```

### Request / Reply

```c
// Responder
void echo_handler(const PubSubMsg *msg, void *ctx) {
    PubSub *ps = ctx;
    pubsub_reply_str(ps, msg->reply_to, "pong");
}
pubsub_subscribe(ps, "echo", echo_handler, ps, NULL, &sub);

// Requester
char *reply;
pubsub_request_str(ps, "echo", "ping", 5.0, &reply);
printf("Reply: %s\n", reply);
free(reply);
```

### Namespace Isolation

Different namespaces are completely isolated — messages on "topic" in
namespace "app_a" don't reach subscribers in namespace "app_b".

## Translation Notes

| Python | C |
|--------|---|
| `async/await` + raw TCP | Synchronous via nats.c library |
| `asyncio` event loop + threads | nats.c internal thread pool |
| `Message` dataclass | `PubSubMsg` struct (stack-allocated in callback) |
| `NatsPubSub` class | `PubSub` handle + `PubSubSub` subscriptions |
| `subscribe()` returns sid string | `pubsub_subscribe()` returns `PubSubSub*` handle |
| `request()` with Future + inbox | `pubsub_request()` via nats.c built-in request/reply |
| Callback receives `Message` object | Callback receives `const PubSubMsg*` (valid during call only) |

The C version does not need cJSON since PubSub works with raw bytes — the
caller is responsible for any JSON encoding/decoding.

