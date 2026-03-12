# NATS KeyStore + KbStore + JobQueue — C Library

C translation of the Python `nats_key_store`, `nats_kb_store`, and `JobQueue` modules.

## Structure

```
nats_kv/
├── include/
│   ├── nats_key_store.h    # KeyStore API (KV operations, counters, sessions)
│   ├── nats_kb_store.h     # KbStore API  (label+node KB operations)
│   └── nats_job_queue.h    # JobQueue API (distributed job queue)
├── src/
│   ├── nats_key_store.c    # KeyStore implementation
│   ├── nats_kb_store.c     # KbStore implementation
│   └── nats_job_queue.c    # JobQueue implementation
├── test/
│   ├── test_nats_kv.c      # KeyStore + KbStore tests (22 tests + demo)
│   └── test_job_queue.c    # JobQueue tests (12 tests + demo)
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

### 3. Start a NATS server with JetStream

```bash
docker run -d -p 4222:4222 nats:latest -js
```

## Build

```bash
cd nats_kv
make
```

## Run Tests

```bash
# All tests
make run-all

# Individual
LD_LIBRARY_PATH=./build ./build/test_nats_kv           # KV + KB tests
LD_LIBRARY_PATH=./build ./build/test_job_queue          # JobQueue tests
LD_LIBRARY_PATH=./build ./build/test_job_queue demo     # JobQueue demo
```

Make shortcuts:

```bash
make run-test       # KeyStore + KbStore tests
make run-test-jq    # JobQueue tests
make run-demo       # KV + KB demo
make run-demo-jq    # JobQueue demo
```

## Quick Start (copy-paste)

```bash
sudo apt-get update
sudo apt-get install -y libcjson-dev build-essential cmake git docker.io

git clone https://github.com/nats-io/nats.c.git
cd nats.c && mkdir build && cd build
cmake .. -DNATS_BUILD_STREAMING=OFF && make && sudo make install && sudo ldconfig
cd ../..

docker run -d -p 4222:4222 nats:latest -js

cd nats_kv && make && make run-all
```

## API Overview

### KeyStore

```c
KeyStore *ks;
ks_create(&ks, &cfg);
ks_connect(ks);

ks_put(ks, "key", "{\"data\":1}", &rev);
ks_get(ks, "key", &value);
ks_delete(ks, "key");
ks_exists(ks, "key", &found);
ks_keys(ks, "prefix.*", &keys, &count);
ks_increment(ks, "counter", 1, &new_val);

ks_disconnect(ks);
ks_destroy(ks);
```

### KbStore

```c
KbStore *kb;
kb_create(&kb, server, "my_kb", "Description");
kb_store(kb, "topic", "label", "node", label_json, node_json, true, &key);

KbEntry entry;
kb_get(kb, key, &entry);
kb_entry_free(&entry);
```

### JobQueue

The JobQueue shares the KeyStore — no separate connection needed.

```c
KeyStore *ks;
ks_create(&ks, &cfg);
ks_connect(ks);

JobQueue *jq;
jq_create(&jq, ks, "my-worker");

// Submit
char *job_id;
jq_submit(jq, "{\"task\":\"process\"}", "myqueue", 5, 3, 300, &job_id);

// Claim next job (highest priority first)
Job job;
const char *queues[] = {"myqueue"};
jq_claim_job(jq, queues, 1, &job);

// Complete or fail
bool ok;
jq_complete_job(jq, job.id, "{\"result\":42}", &ok);
jq_fail_job(jq, job.id, "something broke", &ok);

// Stats
JqStats stats;
jq_get_stats(jq, "myqueue", &stats);

// Monitoring
JqWorkerInfo *workers;
size_t count;
jq_get_active_workers(jq, 30, &workers, &count);
free(workers);

// Cleanup
job_free(&job);
free(job_id);
jq_destroy(jq);
ks_destroy(ks);
```

## Libraries Produced

| Library | Contents | Depends on |
|---------|----------|------------|
| `libnats_key_store` | KeyStore (KV operations) | libnats, libcjson |
| `libnats_kb_store` | KbStore (knowledge base) | libnats_key_store |
| `libnats_job_queue` | JobQueue (distributed jobs) | libnats_key_store |

All three modules share the same `libnats_key_store` library.

## Translation Notes

| Python | C |
|--------|---|
| `async/await` + `asyncio` | Synchronous (nats.c is internally threaded) |
| `dict` payload | JSON string via cJSON |
| `list[dict, dict]` storage | JSON array `[{...},{...}]` |
| `KeyStore` class | `KeyStore` opaque handle + `ks_*` functions |
| `NatsKbStore(KeyStore)` inheritance | `KbStore` wrapping a `KeyStore` |
| `JobQueue(keystore)` | `JobQueue` borrowing a `KeyStore` pointer |
| `SyncSession` context manager | Manual `ks_connect`/`ks_disconnect` |
| Exception handling | `ks_status_t` return codes |
| `uuid.uuid4()` | Pseudo-random hex string |
| `asyncio.sleep` backoff | Omitted (synchronous retries are immediate) |