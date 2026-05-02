# `robot_controller_test_peer.lua`

In-process fixture for the future Phase-2 controller. Synchronous, no
broker, no transport — exposes the contract verbs as Lua methods.

The harness exists **before** the service. When Phase 2 implements the
real `robot_controller` + the real `upward_peer.lua` client, both get
graded against this fixture.

## See first

The contract specification lives at
[Controller / Contract spec](../controller/contract.md).

## Shape

```lua
local rc_peer = require("robot_controller_test_peer")

local ctrl = rc_peer.new{
    session_id_factory = function() return "01H..." end,  -- optional
    kb_root_for        = function(robot_id) return "rovers." .. robot_id end,
    policy             = { heartbeat_period_s = 10, drain_grace_s = 30, kb_read_timeout_s = 5 },
}

-- override defaults (any/all):
ctrl:on_register(function(req) ... return ack_table end)
ctrl:on_heartbeat(function(req, session) ... return ack end)
ctrl:on_exception(function(req, session) ... end)
ctrl:on_kb_read(function(req, session) ... return resp end)
ctrl:on_kb_write(function(req, session) ... return resp end)
ctrl:on_shutdown(function(req, session) ... end)

-- drive (rover-initiated verbs):
local resp = ctrl:handle{ type = "register", robot_id = "rover_1", ... }

-- introspect:
ctrl.registers_seen     -- list of register requests
ctrl.heartbeats_seen
ctrl.exceptions_seen    -- deduplicated by exception_id
ctrl.kb_reads_seen
ctrl.kb_writes_seen
ctrl.shutdowns_seen
ctrl.ack_log            -- list of {req, resp} pairs in order
ctrl:live_count()       -- number of currently-registered sessions
ctrl:reset()            -- clear all state

-- controller-initiated verbs (build, then push to your peer's mock client):
local drain = ctrl:make_drain(session_id, deadline)
local pause = ctrl:make_pause(session_id, request_id)
local inv   = ctrl:make_kb_invalidate(session_id, paths)
```

## Built-in defaults (override via hooks)

Without any hooks, `ctrl:handle` enforces the contract directly:

| Verb | Default behavior |
|---|---|
| `register` | Validates required fields, rejects duplicate `(class, dongle_instance)` with `dongle_collision`, allocates a session, returns ACK with `kb_root` + `policy` |
| `heartbeat` | Looks up session, increments `last_hb_seq`, returns ACK echoing `seq` |
| `exception` | Deduplicates by `exception_id`, ACKs |
| `kb_read` | Reads `ctrl.kb_store[path]`; returns `not_found` on miss |
| `kb_write` | Enforces `path` under `session.kb_root` (`denied` otherwise), supports `if_match` CAS, increments version |
| `shutdown_notice` | Marks session as not-live, frees the dongle slot |

## What the contract test asserts

`test_robot_controller_contract.lua` (37 checks): register happy path,
field validation, dongle_collision, heartbeat ack + bad-session,
exception idempotency, kb_read success/projection/not_found, kb_write
success/scope-denied/CAS-miss, shutdown frees slot,
controller-initiated verb shapes, unknown-verb handling.

## Wiring into Phase 2

When `upward_peer.lua` becomes a real client:

1. Test it against the in-process peer FIRST (loopback): wire
   `upward_peer:_send(verb)` to `ctrl:handle(verb)` in the test.
2. Add a transport adapter (NATS / MQTT / …) that sits between
   `upward_peer:_send` and the wire on one side, and between
   `ctrl:handle` and the wire on the other.
3. The same contract test runs unchanged against either configuration
   — only the transport differs.

## What's deliberately NOT in the fixture

- **Persistence** — the in-memory `kb_store` is volatile. The real
  controller persists to PG; the fixture lets tests pre-load
  `ctrl.kb_store` for read tests and inspect after writes.
- **Time-based behaviors** — heartbeat timeout, drain deadline.
  Fixture is synchronous so tests fast-forward by hand.
- **Concurrency** — single-threaded by design. Real controller
  concurrency lives in the transport adapter, not the verb logic.
