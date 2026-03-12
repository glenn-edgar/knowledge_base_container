# NATS C Libraries

A collection of standalone C libraries for building distributed systems on [NATS](https://nats.io/) messaging. Each module is independently compiled and tested, sharing a common design: `_create/_destroy` lifecycle, status-code returns, caller-frees-strings ownership, and nats.c for transport.

All libraries were translated from Python originals into idiomatic C with full test coverage.

## Modules

### key_store/ — JetStream KV, Knowledge Base & Job Queue

Three libraries built on NATS JetStream Key-Value:

| Library | Header | Description |
|---------|--------|-------------|
| `libnats_key_store` | `nats_key_store.h` | Low-level JetStream KV operations — get, put, delete, key listing, prefix search, watch for changes |
| `libnats_kb_store` | `nats_kb_store.h` | Hierarchical knowledge base using dot-delimited paths (ltree-style) — tree queries, subtree operations, path-based get/set |
| `libnats_job_queue` | `nats_job_queue.h` | Priority job queue with retries — submit, claim, complete/fail, worker tracking, stale job cleanup |

KbStore and JobQueue build on KeyStore (borrow its connection handle). **Requires JetStream enabled** on the NATS server.

34 tests across two test drivers.

### rpc/ — Request/Response RPC

| Library | Header | Description |
|---------|--------|-------------|
| `libnats_rpc` | `nats_rpc.h` | JSON-based RPC with separate server/client types — method registration with callbacks, instance-specific routing, batch calls, built-in health endpoint, per-handler statistics |

Server subscribes to method subjects; client uses nats.c's built-in request/reply. Requires cJSON. Does **not** require JetStream.

16 tests plus interactive demo.

### pub_sub/ — Publish/Subscribe

| Library | Header | Description |
|---------|--------|-------------|
| `libnats_pubsub` | `nats_pubsub.h` | Pub/sub with namespace isolation — wildcards (`*`, `>`), queue groups for load balancing, request/reply, binary payloads, raw-subject subscriptions |

Pure message passing with no JSON dependency. Does **not** require JetStream.

17 tests plus interactive demo.

## Prerequisites

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

Each module builds independently:

```bash
cd key_store && make && make run-all
cd pub_sub   && make && make run-test
cd rpc       && make && make run-test
```

All 79 tests passing (34 + 16 + 17 + 12 = 79).

## Directory Layout

Each module follows the same structure:

```
module/
├── include/          # Public headers
├── src/              # Implementation (.c)
├── test/             # Test drivers
├── build/            # Compiled output (static .a, shared .so, test binaries)
├── Makefile
├── README_*.md       # Module-specific documentation
└── *.sh              # Helper scripts
```

## Common API Patterns

All libraries follow consistent conventions:

- **Lifecycle:** `xxx_create()` allocates, `xxx_destroy()` frees
- **Status codes:** Every function returns a `_status_t` enum; `_OK = 0`
- **String ownership:** Output strings are `malloc`'d by the library; caller calls `free()`
- **Opaque handles:** Internal structs hidden behind forward-declared typedefs
- **Thread safety:** `pthread_mutex_t` guards shared state; nats.c callbacks run on internal threads
- **Namespace prefixing:** Subjects are automatically prefixed with a configurable namespace for multi-tenant isolation

## Dependencies Between Modules

```
pub_sub   ──▶ nats.c
rpc       ──▶ nats.c + cJSON
key_store ──▶ nats.c + cJSON
              ├── libnats_key_store  (standalone)
              ├── libnats_kb_store   (borrows KeyStore handle)
              └── libnats_job_queue  (borrows KeyStore handle)
```

The three top-level modules (key_store, rpc, pub_sub) are independent of each other and can be used in any combination.

