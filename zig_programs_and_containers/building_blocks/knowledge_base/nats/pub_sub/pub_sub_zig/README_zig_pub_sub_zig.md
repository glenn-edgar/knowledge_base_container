# pub_sub_zig

Idiomatic Zig bindings for the NATS PubSub C library (`pub_sub_c`).

Wraps the C publish/subscribe library with Zig-native types: slices instead of raw pointers, error unions instead of status codes, and a callback trampoline that delivers proper Zig `Message` structs to user callbacks.

## Directory layout

```
pub_sub/
├── pub_sub_c/          # C library (unchanged)
│   ├── include/
│   │   └── nats_pubsub.h
│   ├── build/
│   │   ├── libnats_pubsub.a
│   │   └── libnats_pubsub.so
│   ├── src/
│   │   └── nats_pubsub.c
│   └── test/
│       └── test_nats_pubsub.c
└── pub_sub_zig/        # Zig wrapper
    ├── build.zig
    ├── build.zig.zon
    ├── src/
    │   ├── root.zig        # Public API re-exports
    │   ├── c_api.zig       # Manual extern declarations
    │   ├── status.zig      # ps_status_t → Zig error mapping
    │   └── pubsub.zig      # PubSub, Subscription, Message
    └── test/
        ├── integration_test.zig
        └── example.zig
```

## Prerequisites

- Zig 0.13+
- `pub_sub_c` built (libraries in `pub_sub_c/build/`)
- `libnats` installed (the nats.c library)
- NATS server for integration tests and examples

## Building

From `pub_sub_zig/`:

```bash
zig build                # Build static and shared libraries
zig build test           # Unit tests (no NATS server needed)
zig build integration    # Integration tests (needs NATS server)
zig build example        # Build and run example program
```

The build expects `pub_sub_c/build/` at `../pub_sub_c/build/` by default. Override with:

```bash
zig build -Dc-lib=/path/to/pub_sub_c/build
```

### Starting a NATS server

```bash
docker run -p 4222:4222 nats:latest
```

## Quick start

```zig
const ps = @import("pub_sub_zig");

// Create and connect
var client = try ps.PubSub.init(.{
    .server = "nats://127.0.0.1:4222",
    .namespace = "myapp",
    .client_name = "sensor-reader",
});
defer client.deinit();
try client.connect();

// Subscribe
var sub = try client.subscribe("sensor.*", myCallback, null, null);

// Publish
try client.publishStr("sensor.temp", "{\"value\":23.5}");

// Request/reply
const reply = try client.requestStr("service.echo", "ping", 5.0);
defer ps.PubSub.freeReply(reply);

// Cleanup
try client.unsubscribe(&sub);
```

### Callback signature

```zig
fn myCallback(msg: *const ps.Message, user_data: ?*anyopaque) void {
    std.debug.print("subject={s} data={s}\n", .{
        msg.original_subject,
        msg.data,
    });
}
```

## API reference

### Config

| Field         | Type              | Default                        |
|---------------|-------------------|--------------------------------|
| `server`      | `[:0]const u8`    | `"nats://127.0.0.1:4222"`     |
| `namespace`   | `[:0]const u8`    | `"default"`                    |
| `client_name` | `?[:0]const u8`   | `null` (auto-generated)        |

### PubSub

| Method                     | Description                                        |
|----------------------------|----------------------------------------------------|
| `init(Config)`             | Create client (does not connect)                   |
| `deinit()`                 | Destroy client and free all resources               |
| `connect()`                | Connect to NATS server                             |
| `disconnect()`             | Disconnect from server                             |
| `isConnected()`            | Connection status                                  |
| `getNamespace()`           | Current namespace prefix                           |
| `clientName()`             | Client name                                        |
| `publish(subject, data)`   | Publish raw bytes                                  |
| `publishStr(subject, str)` | Publish a string                                   |
| `subscribe(...)`           | Subscribe with namespace prefix                    |
| `subscribeRaw(...)`        | Subscribe without namespace prefix                 |
| `unsubscribe(sub)`         | Unsubscribe and free handle                        |
| `request(subject, data, timeout)` | Synchronous request, returns raw reply       |
| `requestStr(subject, str, timeout)` | Synchronous request, returns string reply  |
| `freeReply(reply)`         | Free malloc'd reply data                           |
| `reply(reply_to, data)`    | Send reply inside a callback                       |
| `replyStr(reply_to, str)`  | Send string reply inside a callback                |
| `getStats()`               | Published/received counts, active subscriptions    |

### Message

Delivered to callbacks with these fields:

| Field              | Type            | Description                          |
|--------------------|-----------------|--------------------------------------|
| `subject`          | `[]const u8`    | Full subject including namespace     |
| `original_subject` | `[]const u8`    | Subject without namespace prefix     |
| `data`             | `[]const u8`    | Payload bytes                        |
| `reply_to`         | `?[]const u8`   | Reply subject (null if not a request)|

### Errors

All methods return `Error` which maps from C status codes:

| Zig error          | C status             |
|--------------------|----------------------|
| `InvalidArg`       | `PS_ERR_INVALID_ARG` |
| `ConnectionFailed` | `PS_ERR_CONNECTION`  |
| `Timeout`          | `PS_ERR_TIMEOUT`     |
| `OutOfMemory`      | `PS_ERR_MEMORY`      |
| `NotConnected`     | `PS_ERR_NOT_CONNECTED` |
| `NatsError`        | `PS_ERR_NATS`        |

## Design notes

**Manual extern declarations** — `c_api.zig` declares all C types and functions explicitly rather than using `@cImport`. This avoids the include-path propagation issues in Zig 0.13's module system that were encountered in the `key_store_zig` wrapper.

**Callback trampoline** — The C library delivers messages on nats.c's internal dispatch thread via a C-ABI callback. A trampoline function converts the C `PubSubMsg` (raw pointers, lengths) into a Zig `Message` (slices) before calling the user's Zig callback. Each subscription allocates a `CallbackContext` on the heap, which is freed on unsubscribe or destroy.

**Namespace transparency** — The C library handles namespace prefixing internally. The Zig wrapper passes subjects through without modification. Subjects starting with `_` bypass namespace prefixing.

**Memory ownership** — `request`/`requestStr` return malloc'd data. Call `PubSub.freeReply()` to free. All other methods use stack or C-library-owned memory with no caller allocation needed.

## License

MIT