# NATS Libraries — C and Zig

A collection of standalone libraries for building distributed systems on [NATS](https://nats.io/) messaging. Each module provides a battle-tested C implementation with an idiomatic Zig wrapper on top.

The C libraries share a common design: `_create/_destroy` lifecycle, status-code returns, caller-frees-strings ownership, and nats.c for transport. The Zig wrappers provide error unions, slices, tagged unions, and callback trampolines while linking directly to the C libraries underneath.

All C libraries were translated from Python originals. All Zig wrappers use manual extern declarations (no `@cImport`) to avoid Zig 0.13 module system include-path propagation issues.

## Modules

### key_store/ — JetStream KV, Knowledge Base & Job Queue

Three C libraries built on NATS JetStream Key-Value, with a unified Zig wrapper:

| C Library | Zig Module | Description |
|-----------|------------|-------------|
| `libnats_key_store` | `key_store.zig` | Low-level JetStream KV — get, put, delete, key listing, prefix search, watch |
| `libnats_kb_store` | `kb_store.zig` | Hierarchical knowledge base with dot-delimited paths (ltree-style) — tree queries, subtree operations |
| `libnats_job_queue` | `job_queue.zig` | Priority job queue with retries — submit, claim, complete/fail, worker tracking |

KbStore and JobQueue build on KeyStore (borrow its connection handle). **Requires JetStream enabled** on the NATS server.

### rpc/ — Request/Response RPC

| C Library | Zig Modules | Description |
|-----------|-------------|-------------|
| `libnats_rpc` | `server.zig`, `client.zig` | JSON-based RPC — method registration with handler callbacks, instance-specific routing, batch calls, built-in health endpoint, per-handler statistics |

Server subscribes to method subjects; client uses nats.c's built-in request/reply. The Zig wrapper uses a `HandlerResult` tagged union (`.ok` / `.err`) instead of C's status-code-plus-out-param pattern. Requires cJSON. Does **not** require JetStream.

### pub_sub/ — Publish/Subscribe

| C Library | Zig Module | Description |
|-----------|------------|-------------|
| `libnats_pubsub` | `pubsub.zig` | Pub/sub with namespace isolation — wildcards (`*`, `>`), queue groups, request/reply, binary payloads, raw-subject subscriptions |

The Zig wrapper uses a callback trampoline that converts C `PubSubMsg` structs into Zig `Message` structs with proper slices. Pure message passing with no JSON dependency. Does **not** require JetStream.

## Directory Layout

Each module nests the C library alongside its Zig wrapper:

```
module/
├── module_c/             # C library
│   ├── include/          # Public headers
│   ├── src/              # Implementation (.c)
│   ├── test/             # C test drivers
│   ├── build/            # Compiled output (.a, .so, test binaries)
│   ├── Makefile
│   └── README_*.md
└── module_zig/           # Zig wrapper
    ├── build.zig
    ├── build.zig.zon
    ├── src/
    │   ├── root.zig      # Public API re-exports
    │   ├── c_api.zig     # Manual extern declarations
    │   ├── status.zig    # Status code → Zig error mapping
    │   └── *.zig         # Idiomatic wrapper types
    ├── test/
    │   ├── integration_test.zig
    │   └── example.zig
    └── README.md
```

Full tree:

```
nats/
├── key_store/
│   ├── key_store_c/
│   └── key_store_zig/
├── rpc/
│   ├── rpc_c/
│   └── rpc_zig/
├── pub_sub/
│   ├── pub_sub_c/
│   └── pub_sub_zig/
└── README.md
```

## Prerequisites

**Zig** (for Zig wrappers):
- Zig 0.13+

**nats.c** (all modules):
```bash
git clone https://github.com/nats-io/nats.c.git
cd nats.c && mkdir build && cd build
cmake .. -DNATS_BUILD_STREAMING=OFF
make && sudo make install && sudo ldconfig
```

**cJSON** (key_store and rpc only):
```bash
sudo apt-get install libcjson-dev
```

**NATS server:**
```bash
# Plain NATS (pub_sub, rpc):
docker run -d -p 4222:4222 nats:latest

# With JetStream (key_store):
docker run -d -p 4222:4222 nats:latest -js
```

## Build & Test

### C libraries

Each C module builds independently:

```bash
cd key_store/key_store_c && make && make run-all
cd pub_sub/pub_sub_c     && make && make run-test
cd rpc/rpc_c             && make && make run-test
```

### Zig wrappers

Each Zig wrapper links against its adjacent C library:

```bash
cd key_store/key_store_zig && zig build && zig build test
cd pub_sub/pub_sub_zig     && zig build && zig build test
cd rpc/rpc_zig             && zig build && zig build test
```

Integration tests require a running NATS server:

```bash
cd key_store/key_store_zig && zig build integration
cd pub_sub/pub_sub_zig     && zig build integration
cd rpc/rpc_zig             && zig build integration
```

Example programs:

```bash
cd pub_sub/pub_sub_zig && zig build example
cd rpc/rpc_zig         && zig build example
```

The Zig builds expect C libraries at `../module_c/build/` by default. Override with `-Dc-lib=/path/to/build`.

## Common API Patterns

### C layer

- **Lifecycle:** `xxx_create()` allocates, `xxx_destroy()` frees
- **Status codes:** Every function returns a `_status_t` enum; `_OK = 0`
- **String ownership:** Output strings are `malloc`'d by the library; caller calls `free()`
- **Opaque handles:** Internal structs hidden behind forward-declared typedefs
- **Thread safety:** `pthread_mutex_t` guards shared state; nats.c callbacks run on internal threads
- **Namespace prefixing:** Subjects are automatically prefixed with a configurable namespace

### Zig layer

- **Error unions:** C status codes map to Zig errors; use `try` / `catch`
- **Slices:** C `char*` / `char**` become `[:0]const u8` and `[][:0]const u8`
- **Optional returns:** Nullable C pointers become Zig optionals
- **Callback trampolines:** C-ABI adapters convert raw C structs to Zig types before calling user callbacks
- **Tagged unions:** RPC handler results use `.ok` / `.err` instead of status codes plus out-params
- **Manual externs:** `c_api.zig` declares all C types and functions explicitly — no `@cImport`, no include-path dependencies
- **Config structs:** Zig-native defaults with `[:0]const u8` fields, converted to C structs internally

## Dependencies Between Modules

```
pub_sub   ──▶ nats.c
rpc       ──▶ nats.c + cJSON
key_store ──▶ nats.c + cJSON
              ├── libnats_key_store  (standalone)
              ├── libnats_kb_store   (borrows KeyStore handle)
              └── libnats_job_queue  (borrows KeyStore handle)

Zig wrappers link against their corresponding C libraries:
pub_sub_zig   ──▶ libnats_pubsub + libnats
rpc_zig       ──▶ libnats_rpc    + libnats + libcjson
key_store_zig ──▶ libnats_key_store + libnats_kb_store + libnats_job_queue + libnats + libcjson
```

The three top-level modules (key_store, rpc, pub_sub) are independent of each other and can be used in any combination. The Zig wrappers are likewise independent — use any subset.

## License

MIT