# MQTT RPC Library

A C library implementing JSON-RPC 2.0 over MQTT v3.1.1. Provides both server and client APIs for remote procedure calls routed through an MQTT broker. Wraps libmosquitto for MQTT transport and cJSON for JSON serialization.

The server registers named methods and dispatches incoming requests on detached threads. The client supports synchronous calls with per-call timeouts and asynchronous calls with callbacks.

## Topic Layout

```
Request:  rpc/{service}/request/{caller_client_id}
Response: rpc/{service}/response/{caller_client_id}
```

The server subscribes to `rpc/{service}/request/+` (single-level wildcard) and extracts the caller's client ID from the topic to route the response back.

## Project Structure

```
pubsub/
├── include/
│   └── mqtt_pubsub.h          # Public API header
├── src/
│   └── mqtt_pubsub.c          # Implementation
├── test/
│   └── mqtt_pubsub_test.c     # Test program (server + client in one process)
├── Makefile
└── README.md
```

## Prerequisites

### Ubuntu / Debian

```bash
# Mosquitto broker and C client development library
sudo apt update
sudo apt install -y mosquitto libmosquitto-dev

# cJSON development library
sudo apt install -y libcjson-dev

# Build tools
sudo apt install -y build-essential

# Verify broker is running
sudo systemctl status mosquitto

# Start if needed
sudo systemctl start mosquitto
sudo systemctl enable mosquitto
```

### Fedora / RHEL

```bash
sudo dnf install mosquitto mosquitto-devel cjson-devel gcc make
sudo systemctl start mosquitto
```

### Arch Linux

```bash
sudo pacman -S mosquitto cjson
sudo systemctl start mosquitto
```

## Building

```bash
make            # builds static library and test binary
make lib        # builds only libmqtt_rpc.a
make test       # builds only the test binary
make clean      # removes build artifacts
```

Output in `build/`:

- `build/libmqtt_rpc.a` — static library
- `build/mqtt_rpc_test` — test binary

## Running the Test

```bash
make run
```

The test program starts a server and client in the same process. The server registers four methods (`add`, `multiply`, `get_time`, `echo`), then the client exercises them with synchronous calls, tests an unknown-method error case, and fires an asynchronous call.

## Using the Library

### Linking

```bash
gcc -Iinclude -o my_app my_app.c -Lbuild -lmqtt_rpc -lmosquitto -lcjson -lpthread
```

### Server Example

```c
#include "mqtt_rpc.h"

static cJSON *my_add(const cJSON *params, void *ud) {
    (void)ud;
    cJSON *a = cJSON_GetObjectItem(params, "a");
    cJSON *b = cJSON_GetObjectItem(params, "b");
    return cJSON_CreateNumber(a->valuedouble + b->valuedouble);
}

int main(void) {
    mqtt_rpc_lib_init();

    mqtt_rpc_config_t cfg = {
        .host = "localhost", .port = 1883,
        .client_id = "my-server",
        .service_name = "calc",
        .keepalive = 60, .qos = 1,
    };

    pubsub_server_t srv;
    pubsub_server_init(&srv, &cfg);
    pubsub_server_register(&srv, "add", my_add, NULL);
    pubsub_server_start(&srv, true, 3000);

    pubsub_server_wait(&srv);  // blocks until pubsub_server_stop()

    pubsub_server_stop(&srv);
    pubsub_server_destroy(&srv);
    mqtt_rpc_lib_cleanup();
}
```

### Client Example

```c
#include "mqtt_rpc.h"

int main(void) {
    mqtt_rpc_lib_init();

    mqtt_rpc_config_t cfg = {
        .host = "localhost", .port = 1883,
        .client_id = "my-client",
        .service_name = "calc",
        .keepalive = 60, .qos = 1,
    };

    pubsub_client_t cli;
    pubsub_client_init(&cli, &cfg, 10.0f);
    pubsub_client_connect(&cli, 5000);

    // Synchronous call
    cJSON *params = cJSON_CreateObject();
    cJSON_AddNumberToObject(params, "a", 5);
    cJSON_AddNumberToObject(params, "b", 3);

    cJSON *result = NULL, *error = NULL;
    int rc = pubsub_client_call(&cli, "add", params, 0, &result, &error);

    if (rc == 0 && result) {
        printf("Result: %.0f\n", result->valuedouble);
        cJSON_Delete(result);
    } else if (rc == -2 && error) {
        // JSON-RPC error
        cJSON_Delete(error);
    }
    cJSON_Delete(params);

    pubsub_client_disconnect(&cli);
    pubsub_client_destroy(&cli);
    mqtt_rpc_lib_cleanup();
}
```

### Async Calls

```c
void on_result(const cJSON *error, const cJSON *result, void *ud) {
    if (result) {
        char *s = cJSON_PrintUnformatted(result);
        printf("Got: %s\n", s);
        free(s);
    }
}

char *id = pubsub_client_call_async(&cli, "add", params, 0, on_result, NULL);
free(id);
```

### Return Codes for `pubsub_client_call`

| Return | Meaning |
|--------|---------|
| `0`    | Success — `*out_result` is set |
| `-1`   | Transport or timeout error — neither output is set |
| `-2`   | JSON-RPC error — `*out_error` is set |

### Standard JSON-RPC Error Codes

| Code | Name |
|------|------|
| `-32700` | Parse Error |
| `-32600` | Invalid Request |
| `-32601` | Method Not Found |
| `-32602` | Invalid Params |
| `-32603` | Internal Error |

## License

MIT