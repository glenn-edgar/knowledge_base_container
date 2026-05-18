# Zenoh build_block — Continue From Here

## Session Summary (2026-05-17)

Created `build_blocks/knowledge_base/zenoh/` from scratch. Three C client libraries
on top of zenoh-pico 1.9.0, fully built and tested end-to-end against a
prebuilt `eclipse/zenoh:latest` container. **No Rust in our build chain.**

### What landed

```
zenoh/
├── README_zenoh.md               # top-level overview
├── mkdocs.yml                    # mkdocs nav
├── docs/                         # collected READMEs for site build
├── continue.md                   # this file
├── token/    ── libzenoh_token.{a,so}   +  test_zenoh_token   (227 tests)
├── pub_sub/  ── libzenoh_pubsub.{a,so}  +  test_zenoh_pubsub  (21 tests)
└── rpc/      ── libzenoh_rpc.{a,so}     +  test_zenoh_rpc     (26 tests)
```

### Modules in detail

**token/ — FNV1a-32 topic-token utility.**
Pure C99, no zenoh dependency. Exports `zt_hash(s)`, `zt_hash_list(strs,n,out)`,
plus an optional `zt_register/zt_lookup` registry for reverse lookup (debug
trace tooling). Thread-safe via `pthread_mutex_t`. Wire keyexpr convention:
`tok/%08x` (12 ASCII chars, human-readable in zenoh traces).
**Validated against FNV-1a RFC test vectors.**

**pub_sub/ — wraps z_declare_publisher / z_declare_subscriber.**
- One-shot publish via `z_put` (no per-token publisher cache; keeps API simple)
- Subscriber via `z_declare_subscriber` + closure adapter (`sample_handler`,
  `sample_dropper`) that bridges zenoh-pico's `z_loaned_sample_t` to a
  caller-typed `zenoh_pubsub_callback_t`
- Adapter heap-allocated; lifetime tied to closure's drop callback

**rpc/ — wraps z_get (client) and z_declare_queryable (server).**
- Server: linked-list of registered handlers, each gets its own queryable.
  `register()` works before AND after `start()` (declares lazily if not yet
  started).
- Client: synchronous `_call()` blocks on a per-call `pthread_cond_t`. Closure
  collects first reply, signals condvar, drops on final marker. Timeout via
  `pthread_cond_timedwait`.
- Handler errors propagate as `z_query_reply_err` carrying status string.

### Test results (clean rebuild 2026-05-17)

| Test bin | API only | E2E UDP | E2E TCP |
|----------|----------|---------|---------|
| `test_zenoh_token`  | 227/227 | n/a | n/a |
| `test_zenoh_pubsub` | 9/9 | 21/21 | 21/21 |
| `test_zenoh_rpc`    | 12/12 | 26/26 | 26/26 |

### Gotchas discovered (now in memory)

1. **zenoh-pico requires platform defines at compile time:**
   `-DZENOH_LINUX -DZENOH_COMPILER_GCC -DZENOH_C_STANDARD=11`.
   Without them, internal socket types fail to resolve. CMakeLists silently
   sets these in zenoh-pico's own build, so it's invisible from header
   inspection.

2. **zenoh-pico's generated `config.h`** lives in `${ZENOH_PICO_HOME}/build/include`,
   not the source `include/`. Both directories must be on the include path.

3. **zenohd container defaults to TCP-only listen.** UDP must be explicitly
   added at runtime: `--listen tcp/0.0.0.0:7447 --listen udp/0.0.0.0:7447`.
   Default container start = no UDP = connection refused for UDP clients.

## Environment / dependencies

- **zenoh-pico 1.9.0** built locally at `~/src/zenoh-pico` (NOT system-installed;
  sudo was unavailable in this session). Shared+static libs at
  `~/src/zenoh-pico/lib-combined/`.
- **eclipse/zenoh:latest** Docker image pulled for testing.
- Each module's Makefile uses `ZENOH_PICO_HOME` env var (defaults to
  `$HOME/src/zenoh-pico`). To switch to a system install, override:
  `make ZENOH_PICO_HOME=/usr/local`.

### Running tests yourself

```bash
# Start the test fixture (one container, both TCP and UDP):
docker run -d --name zenoh-test \
    -p 17447:7447/tcp -p 17447:7447/udp \
    eclipse/zenoh:latest \
    --listen tcp/0.0.0.0:7447 \
    --listen udp/0.0.0.0:7447

# Module tests:
cd token   && make run-test
cd pub_sub && make run-test TEST_ARGS="--transport=udp"
cd pub_sub && make run-test TEST_ARGS="--transport=tcp"
cd rpc     && make run-test TEST_ARGS="--transport=udp"
cd rpc     && make run-test TEST_ARGS="--transport=tcp"

# Teardown:
docker rm -f zenoh-test
```

## What's NOT in this session

- **Serial SLIP transport e2e tests.** API surface accepts `--transport=serial`
  but the PTY-loopback fixture wasn't built. Skipped with a `[SKIP]` message.
- **Unix domain socket transport tests.** Same story — zenoh-pico supports it,
  Makefile/test driver don't exercise it yet.
- **MCU port.** The C source files compile against zenoh-pico, but the
  Makefiles are Linux-pthread. Pico 2 W port (pico-sdk + FreeRTOS) is the
  next major piece of work — see [[project-zenoh-substrate]] for the plan.
- **Install target.** `make install` is wired in each Makefile but untested
  (would need sudo and probably an actual PREFIX strategy).

## Next session candidates

1. **Serial SLIP test fixture** — PTY loopback (`openpty(3)`) to wire two
   sessions back-to-back through serial transport. ~half a day.
2. **MCU port to Pico 2 W** — separate target tree, pico-sdk CMake build,
   FreeRTOS task/mutex/cond instead of pthread. Probably reuse the source
   files mostly unchanged. Substantial.
3. **Robot controller scaffolding** — the downstream application that uses
   these drivers. Lives elsewhere in the repo; not part of this build_block.

## Related memory

- [[project-zenoh-substrate]] — design decisions, topology, MCU target
- [[project-thread-zenoh-bridge]] — alt MCU radio path via Thread + OTBR
- [[feedback-no-soft-faults]] — open question on WiFi reconnect semantics
