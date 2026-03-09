# MQTT KV Store Library

A C library that turns an MQTT broker's retained-message store into a lightweight key/value database. Topics serve as keys and retained payloads serve as values. The library provides two independent modules — a **writer** for publishing, updating, and deleting keys, and a **reader** for querying keys back using MQTT wildcard patterns and sentinel-based completion detection.

## Prerequisites

**Operating System:** Linux (tested on Ubuntu 22.04 / 24.04). Other POSIX systems should work with minor adjustments.

**MQTT Broker:** A running Mosquitto instance (or any MQTT 3.1.1 / 5.0 broker) is required at runtime. The default configuration connects to `localhost:1883`.

### Install Dependencies

```bash
# Ubuntu / Debian
sudo apt update
sudo apt install -y build-essential libmosquitto-dev mosquitto mosquitto-clients

# Fedora / RHEL
sudo dnf install -y gcc make mosquitto-devel mosquitto

# Arch
sudo pacman -S mosquitto
```

The library links against:

- **libmosquitto** — MQTT client library (`-lmosquitto`)
- **libpthread** — POSIX threads (`-lpthread`)

### Start the Broker

```bash
# Start Mosquitto with default settings (listens on localhost:1883)
sudo systemctl start mosquitto

# Or run in the foreground for debugging
mosquitto -v
```

Verify the broker is reachable:

```bash
mosquitto_pub -t "test/ping" -m "hello" -r
mosquitto_sub -t "test/ping" -C 1
# Should print: hello
```

## Project Structure

```
mqtt_kv_store/
├── include/
│   ├── kv_store_reader.h
│   └── kv_store_writer.h
├── src/
│   ├── kv_store_reader.c
│   └── kv_store_writer.c
├── test/
│   ├── kv_store_test.c        # Full read/write integration test
│   └── kv_writer_demo.c       # Writer-only demo
├── Makefile
└── README.md
```

## Building

```bash
# Build the library and tests
make

# Or compile manually
gcc -Wall -Wextra -std=c11 -O2 -D_POSIX_C_SOURCE=199309L \
    -I include -o kv_store_test \
    test/kv_store_test.c src/kv_store_reader.c src/kv_store_writer.c \
    -lmosquitto -lpthread

gcc -Wall -Wextra -std=c11 -O2 -D_POSIX_C_SOURCE=199309L \
    -I include -o kv_writer_demo \
    test/kv_writer_demo.c src/kv_store_writer.c \
    -lmosquitto -lpthread
```

## Writer API

The writer module (`kv_store_writer.h`) publishes, updates, and deletes retained messages on the broker. All write operations are synchronous — each call blocks until the broker acknowledges the publish or a timeout expires.

### Lifecycle

```c
#include "kv_store_writer.h"

kvw_config_t cfg;
kvw_config_init(&cfg);                              // defaults: localhost:1883
strncpy(cfg.client_id, "my-writer", sizeof(cfg.client_id) - 1);

kvw_store_writer_t writer;
kvw_init(&writer, &cfg);                            // create instance
kvw_connect(&writer, 5.0);                          // connect (5 s timeout)

// ... write operations ...

kvw_disconnect(&writer);                            // graceful disconnect
kvw_destroy(&writer);                               // free resources
```

### Write Operations

**Single write** — publish one retained key/value pair:

```c
kvw_write_single(&writer, "kv/config/host", "192.168.1.1",
                 1,       // QoS
                 true,    // retain
                 2.0);    // timeout (seconds)
```

**Batch write** — publish multiple keys in one call. Failed topics are reported through the `failed` array:

```c
const char *topics[] = {"kv/a", "kv/b", "kv/c"};
const char *values[] = {"1",    "2",    "3"};
const char *failed[3] = {NULL};

int ok = kvw_write_batch(&writer, 3, topics, values,
                         1, true, 10.0, failed);
printf("%d of 3 succeeded\n", ok);
```

**Update** — shorthand for writing a retained message to an existing key:

```c
kvw_update_single(&writer, "kv/config/port", "9090", 1, 2.0);
```

**Delete single** — publishes an empty retained message, which the broker interprets as clearing the key:

```c
kvw_delete_single(&writer, "kv/config/host", 2.0);
```

**Delete batch** — clears multiple keys:

```c
const char *keys[] = {"kv/a", "kv/b"};
const char *failed[2] = {NULL};
int ok = kvw_delete_batch(&writer, 2, keys, 10.0, failed);
```

## Reader API

The reader module (`kv_store_reader.h`) subscribes to MQTT topic patterns, collects the retained messages the broker replays, and returns them as an array of key/value entries. Two completion strategies are supported: a simple timeout, or a **sentinel** topic whose arrival signals that all retained messages have been delivered.

### Sentinel Topics

Because MQTT does not tell subscribers "all retained messages have been sent", the library uses sentinel topics as end-of-data markers. A sentinel is a retained message published after all regular data keys. The reader subscribes to the data pattern, and when the sentinel message arrives it knows the snapshot is complete. Sentinels are filtered out of the returned results.

Example layout on the broker:

```
kv/sensors/temperature/living_room  → "22.5"
kv/sensors/temperature/bedroom      → "20.1"
kv/sensors/humidity/living_room     → "45"
kv/sensors/.sentinel/1              → "done"    ← sentinel
```

### Lifecycle

```c
#include "kv_store_reader.h"

kvr_config_t cfg;
kvr_config_init(&cfg);
strncpy(cfg.client_id, "my-reader", sizeof(cfg.client_id) - 1);

kvr_store_reader_t reader;
kvr_init(&reader, &cfg);
kvr_connect(&reader, 5.0);

// ... read operations ...

kvr_disconnect(&reader);
kvr_destroy(&reader);
```

### Read Operations

**Pattern read with sentinel** — subscribe to a wildcard pattern and wait for a sentinel before returning:

```c
kvr_entry_t entries[KVR_MAX_ENTRIES];
const char *sentinels[] = {"kv/sensors/.sentinel/1", NULL};

int n = kvr_read_pattern(&reader,
                         "kv/sensors/+/+",    // MQTT wildcard pattern
                         1,                   // QoS
                         2.0,                 // timeout (seconds)
                         sentinels,           // sentinel topic list (NULL-terminated)
                         true,                // wait for sentinel
                         entries,             // output buffer
                         KVR_MAX_ENTRIES);     // buffer capacity

for (int i = 0; i < n; i++) {
    printf("%s = %s\n", entries[i].topic, entries[i].value);
}
```

**Single value read** — fetch exactly one key by its exact topic:

```c
char value[KVR_MAX_VALUE_LEN];
if (kvr_read_single(&reader, "kv/system/version", 1.0,
                    value, sizeof(value))) {
    printf("version = %s\n", value);
}
```

**Read all** — subscribe to `#` (or a base topic) and collect everything:

```c
const char *sentinels[] = {"kv/.sentinel", NULL};
int n = kvr_read_all(&reader, "#", 2.0, sentinels, true,
                     entries, KVR_MAX_ENTRIES);
```

### MQTT Wildcard Patterns

The reader accepts standard MQTT wildcards:

| Pattern | Matches |
|---------|---------|
| `kv/example/#` | All topics under `kv/example/` at any depth |
| `kv/example/config/+` | Single level: `kv/example/config/host`, `kv/example/config/port`, etc. |
| `kv/sensors/+/+` | Two variable levels: `kv/sensors/temperature/bedroom`, etc. |
| `#` | Every topic on the broker |

## Integration Test (`kv_store_test`)

The `kv_store_test` program exercises both the writer and reader APIs end-to-end against a live broker. It runs in two phases.

### Phase 1 — Write

The test populates the broker with 27 retained messages spanning five topic hierarchies, plus five sentinel markers:

| Prefix | Keys | Example |
|--------|------|---------|
| `kv/example/config/` | 5 | `host`, `port`, `enabled`, `timeout`, `retry_count` |
| `kv/example/status/` | 4 | `uptime`, `connections`, `last_error`, `cpu_usage` |
| `kv/system/` | 4 | `version`, `build`, `hostname`, `os` |
| `kv/app/` | 4 | `users/count`, `users/active`, `database/connected`, `database/pool_size` |
| `kv/sensors/` | 4 | `temperature/living_room`, `humidity/bedroom`, etc. |
| Sentinels | 5 | `kv/example/.sentinel`, `kv/.sentinel`, etc. |

All messages are published as QoS 1 with the retain flag set. A one-second pause follows to let the broker process everything.

### Phase 2 — Read

The reader connects with a separate client ID and runs six query modes:

1. **Multi-level wildcard** (`kv/example/#`) — retrieves all config and status keys under `kv/example/`, terminated by the `kv/example/.sentinel` sentinel.
2. **Single-level wildcard** (`kv/example/config/+`) — retrieves only the five config parameters, terminated by `kv/example/config/.sentinel`.
3. **Exact topic** (`kv/system/version`, `kv/system/build`, `kv/system/hostname`) — three individual reads using `kvr_read_single`.
4. **Double wildcard** (`kv/sensors/+/+`) — retrieves temperature and humidity readings across rooms, using `kv/sensors/.sentinel/1`.
5. **Global read** (`#`) — retrieves every retained message on the broker, printing a summary grouped by top-level prefix.
6. **Application metrics** (`kv/app/+/+`) — retrieves user and database stats, using `kv/app/.sentinel/1`.

### Running

```bash
# Ensure broker is running
sudo systemctl start mosquitto

# Run the test
./kv_store_test
```

Expected output confirms all 27 messages were published and each read mode returns the correct entries. The exit code is `0` on success, `1` on any failure.

### Writer Demo (`kv_writer_demo`)

A standalone demo that exercises only the writer API — single writes, batch writes, updates, single deletes, and batch deletes. It does not require sentinels or the reader module.

```bash
./kv_writer_demo
```

## Configuration

Both `kvw_config_t` and `kvr_config_t` share the same fields:

| Field | Default | Description |
|-------|---------|-------------|
| `host` | `"localhost"` | Broker hostname or IP |
| `port` | `1883` | Broker port |
| `client_id` | `"kv-writer"` / `"kv-reader"` | MQTT client identifier |
| `keepalive` | `60` | Keep-alive interval (seconds) |
| `clean_session` | `true` | Start with a clean session |
| `use_mqttv5` | `false` | Use MQTT v5 protocol |
| `has_credentials` | `false` | Enable username/password auth |
| `username` | `""` | Broker username (if `has_credentials`) |
| `password` | `""` | Broker password (if `has_credentials`) |

## Limits

Compile-time constants defined in the headers:

| Constant | Default | Description |
|----------|---------|-------------|
| `KVR_MAX_ENTRIES` | 256 | Maximum entries returned per read |
| `KVR_MAX_TOPIC_LEN` | 256 | Maximum topic string length |
| `KVR_MAX_VALUE_LEN` | 1024 | Maximum value string length |
| `KVR_MAX_SENTINELS` | 16 | Maximum sentinel topics per read |
| `KVW_MAX_PENDING` | 64 | Maximum in-flight publishes |
| `KVW_MAX_BATCH` | 128 | Maximum topics per batch operation |

## License

MIT