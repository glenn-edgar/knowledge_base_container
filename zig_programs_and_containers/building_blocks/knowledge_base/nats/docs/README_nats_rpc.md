# NATS RPC — C Library

C translation of the Python `NAS_RPC` module. Standalone library for
request/response RPC over NATS messaging.

## Structure

```
nats_rpc/
├── include/
│   └── nats_rpc.h          # Public API
├── src/
│   └── nats_rpc.c          # Implementation
├── test/
│   └── test_nats_rpc.c     # 16 tests + interactive demo
├── Makefile
└── README.md
```

## Prerequisites

### 1. Install cJSON

```bash
sudo apt-get update
sudo apt-get install libcjson-dev
```

### 2. Install nats.c (build from source)

```bash
git clone https://github.com/nats-io/nats.c.git
cd nats.c
mkdir build && cd build
cmake .. -DNATS_BUILD_STREAMING=OFF
make
sudo make install
sudo ldconfig
```

### 3. Start a NATS server

```bash
docker run -d -p 4222:4222 nats:latest
```

Note: JetStream is NOT required for RPC (unlike the KeyStore/KbStore libraries).

## Build

```bash
cd nats_rpc
make
```

## Run Tests

```bash
LD_LIBRARY_PATH=./build ./build/test_nats_rpc
```

Or with make:

```bash
make run-test       # run tests
make run-demo       # run interactive demo
```

## Quick Start

```bash
sudo apt-get update
sudo apt-get install -y libcjson-dev build-essential cmake git docker.io

git clone https://github.com/nats-io/nats.c.git
cd nats.c && mkdir build && cd build
cmake .. -DNATS_BUILD_STREAMING=OFF && make && sudo make install && sudo ldconfig
cd ../..

docker run -d -p 4222:4222 nats:latest

cd nats_rpc && make && make run-test
```

## API Overview

### Server Side

```c
#include "nats_rpc.h"

/* Handler function signature */
rpc_status_t my_handler(const char *params_json,
                        void *user_data,
                        char **result_json)
{
    /* Parse params_json with cJSON, compute result */
    *result_json = strdup("42");
    return RPC_OK;          /* or RPC_ERR_HANDLER with error message */
}

RpcConfig cfg;
rpc_config_defaults(&cfg);
cfg.server      = "nats://127.0.0.1:4222";
cfg.namespace_  = "myapp";
cfg.instance_id = "server1";

RpcServer *srv;
rpc_server_create(&srv, &cfg);

/* Register methods */
rpc_server_register(srv, "math.add", add_handler, NULL, false);
rpc_server_register(srv, "private",  priv_handler, NULL, true);  /* instance-specific */

/* Start (subscribes and returns immediately) */
rpc_server_start(srv, "rpc");

/* Block until rpc_server_stop() is called */
rpc_server_wait(srv);

rpc_server_destroy(srv);
```

### Client Side

```c
RpcConfig cfg;
rpc_config_defaults(&cfg);
cfg.server     = "nats://127.0.0.1:4222";
cfg.namespace_ = "myapp";

RpcClient *cli;
rpc_client_create(&cli, &cfg);
rpc_client_connect(cli);

/* Simple call (load-balanced if multiple servers) */
char *result = NULL;
rpc_client_call(cli, "rpc.math.add", "{\"a\":5,\"b\":3}", 5.0, &result);
printf("Result: %s\n", result);   /* "8" */
free(result);

/* Target a specific instance */
rpc_client_call_instance(cli, "rpc.private", "{}",
                         5.0, "server1", &result);
free(result);

/* Batch calls */
RpcBatchEntry entries[] = {
    { "rpc.math.add", "{\"a\":1,\"b\":2}", NULL },
    { "rpc.math.add", "{\"a\":3,\"b\":4}", NULL },
};
RpcBatchResult results[2] = {0};
rpc_client_call_batch(cli, entries, 2, 5.0, results);

rpc_client_destroy(cli);
```

### Built-in Health Check

When `enable_health = true` (default), the server registers an `_health`
endpoint.  Query it with instance targeting:

```c
rpc_client_call_instance(cli, "rpc._health", "{}",
                         5.0, "server1", &result);
/* Returns: {"status":"healthy","instance_id":"server1",
             "uptime_seconds":42,"handlers":["math.add",...]} */
```

## Translation Notes

| Python | C |
|--------|---|
| `async/await` + raw TCP | Synchronous via nats.c library |
| `asyncio.Future` pending requests | nats.c `natsConnection_Request` (built-in inbox) |
| `json.loads/dumps` | cJSON parse/print |
| `threading.Event` + background loop | nats.c internal thread pool |
| `NAS_RPC` class (server+client) | Separate `RpcServer` / `RpcClient` types |
| `register_handler` decorator | `rpc_server_register()` with function pointer + user_data |
| `call_sync` / `call` | `rpc_client_call()` (synchronous only) |
| `call_batch` | `rpc_client_call_batch()` (sequential) |
| Service discovery | Omitted (not needed for single NATS server) |
| `call_async` / `get_response` | Omitted (use nats.c async API directly if needed) |

### Key Design Differences

The Python version implements the NATS wire protocol from scratch over raw TCP
sockets and manages its own async event loop.  The C version delegates all
protocol handling to nats.c, which provides connection management, automatic
reconnect, subscription handling, and built-in request/reply with inbox subjects.
This makes the C implementation significantly simpler and more robust.

The Python version combines server and client in one class.  The C version
separates them into `RpcServer` and `RpcClient` for clearer ownership and
lifecycle management.

