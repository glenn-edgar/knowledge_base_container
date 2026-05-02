# robot_controller contract (Phase 2 spec)

## Status

**Design only.** No service exists today. This document defines the
protocol so that:

- `robot_controller_test_peer.lua` can stand up an in-process fixture
  exposing exactly these verbs.
- `robot_base/supervisor/upward_peer.lua` (Phase 1 stub) can be
  upgraded to a real client that hits these verbs.
- The real `robot_controller` service, when written, is graded
  green-or-red against the harness rather than against an evolving
  whiteboard.

## Roles

```
   +--------------+               +-----------------+
   |   rover N    | <--- bus ---> | robot_controller|
   | (upward_peer)|               |   (Phase 2)     |
   +--------------+               +-----------------+
                                          |
                                  +-------+-------+
                                  |               |
                              +---v---+      +----v----+
                              |  PG   |      |  KB     |
                              +-------+      +---------+
                              exception      authoritative
                              sink           knowledge base
```

The rover is **untrusted** for KB consistency: it never writes the
authoritative KB directly. The controller is the broker.

## Lifecycle

```
   rover boot                  controller fleet
   ----------                  ----------------
        | REGISTER             |
        |--------------------->|
        |   REGISTER_ACK       |
        |<---------------------|
        |                      |
        |   HEARTBEAT (loop)   |
        |--------------------->|
        |   HEARTBEAT_ACK      |
        |<---------------------|
        |                      |
        |   EXCEPTION          |
        |--------------------->| -> persist to PG
        |   EXCEPTION_ACK      |
        |<---------------------|
        |                      |
        |   KB_READ            |
        |--------------------->| -> proxy to KB
        |   KB_READ_RESPONSE   |
        |<---------------------|
        |                      |
        |          ...         |
        |                      |
        |   DRAIN (controller→) |
        |<---------------------|
        |   SHUTDOWN_NOTICE    |
        |--------------------->|
        ✕                      ✕
```

Verbs are split by direction. **Rover-initiated** verbs always have a
matching ACK; the rover assumes the verb failed if no ACK arrives
within the bounded window. **Controller-initiated** verbs are best-
effort: the rover may decline (busy, draining) but must respond.

## Rover-initiated verbs

### REGISTER

Issued exactly once per rover boot, before any other verb. Identifies
the rover and declares capabilities.

```json
{
  "type":           "register",
  "robot_id":       "rover_1",
  "robot_class":    "lunar_rover",
  "dongle_instance": 1,
  "capabilities":   ["init_check", "path_line", "path_spline", "..."],
  "energy_max":     10000,
  "energy_remaining": 10000,
  "boot_ts":        "2026-05-02T17:36:19Z",
  "client_version": "robot_base:1.0+L6"
}
```

Controller responds:

```json
{
  "type":   "register_ack",
  "ack":    true,
  "session_id": "01H...",      // controller-assigned, opaque
  "fleet_seq":  42,             // controller-assigned membership index
  "kb_root":    "rovers.rover_1",  // KB path under which this rover writes
  "policy": {
    "heartbeat_period_s":  10,
    "drain_grace_s":       30,
    "kb_read_timeout_s":   5
  }
}
```

On `ack: false` the rover **fail-stops** (per `feedback_no_soft_faults`).
Reasons (`reason: "..."`):

- `class_unknown` — no factory for ROBOT_CLASS in the controller's catalog
- `dongle_collision` — another live session has the same `dongle_instance`
- `version_incompatible` — rover client_version too old/new
- `quota_exhausted` — fleet is at session cap
- `internal` — controller can't proceed

### HEARTBEAT

Periodic (default `policy.heartbeat_period_s`). Carries lightweight
runtime state.

```json
{
  "type":             "heartbeat",
  "session_id":       "01H...",
  "seq":              473,
  "energy_remaining": 9756,
  "fault_active":     false,
  "current_phase":    "monitor",      // robot_supervisor SM state
  "active_workers":   ["worker_path_line"],
  "ts":               "2026-05-02T17:38:00Z"
}
```

Controller responds with `heartbeat_ack` (same `seq`). Three missed
ACKs in a row → rover treats controller as **lost** and applies the
configured loss policy (default: continue current mission, refuse
new commands until reattached).

### EXCEPTION

Emit a fault for persistence to the PG exception sink. Idempotent on
`exception_id` — duplicate sends are deduped by the controller.

```json
{
  "type":         "exception",
  "session_id":   "01H...",
  "exception_id": "01H...-0042",       // monotonic per session
  "severity":     "ERROR",              // INFO | WARN | ERROR | FATAL
  "kind":         "tool_fault",
  "subject":      "worker_recharge",
  "details": {
    "fault_reason": "charger_fault",
    "energy_at":    7430
  },
  "ts": "2026-05-02T17:39:11Z"
}
```

Controller responds with `exception_ack` echoing `exception_id`. On
`FATAL` severity the rover supervisor MUST advance to teardown after
the ACK arrives (same fail-stop discipline).

### KB_READ

Proxy read from the authoritative KB. The rover never reads PG
directly.

```json
{
  "type":       "kb_read",
  "session_id": "01H...",
  "request_id": "01H...-r19",
  "path":       "rovers.rover_1.config.tunables_v2",
  "fields":     null              // null = whole record; else string[]
}
```

Controller responds with one of:

```json
{
  "type":       "kb_read_response",
  "request_id": "01H...-r19",
  "ok":         true,
  "value":      { ... },
  "version":    "ts-or-vector-here"
}
```

```json
{
  "type":       "kb_read_response",
  "request_id": "01H...-r19",
  "ok":         false,
  "error":      "not_found"      // not_found | timeout | denied | internal
}
```

### KB_WRITE

Proxy write to a path the rover owns (typically only paths under
`policy.kb_root`). The controller MUST reject writes outside that
subtree (`error: "denied"`).

```json
{
  "type":       "kb_write",
  "session_id": "01H...",
  "request_id": "01H...-w08",
  "path":       "rovers.rover_1.state.last_mission",
  "value":      { ... },
  "if_match":   "ts-or-vector"      // optional CAS; null = unconditional
}
```

Response:

```json
{
  "type":       "kb_write_response",
  "request_id": "01H...-w08",
  "ok":         true,
  "version":    "ts-or-vector-after-write"
}
```

On CAS miss: `ok: false, error: "version_mismatch", version: "current"`.

### SHUTDOWN_NOTICE

Issued from `request_shutdown` / `teardown` states. Best-effort; ACK
not required for the rover to proceed with teardown.

```json
{
  "type":       "shutdown_notice",
  "session_id": "01H...",
  "reason":     "sigterm"     // sigterm | child_died | drain | fatal_exception
}
```

## Controller-initiated verbs

### DRAIN

Ask the rover to wind down current mission and disconnect.

```json
{
  "type":       "drain",
  "session_id": "01H...",
  "deadline":   "2026-05-02T17:42:00Z"
}
```

Rover responds with `drain_ack` and transitions through
`request_shutdown → teardown` before `deadline`. Missing the deadline
is itself an exception (`kind: "drain_overrun"`).

### PAUSE / RESUME

Hold/release the rover's command intake. PAUSE does **not** affect
in-flight commands; it only blocks new dispatch.

```json
{ "type": "pause",  "session_id": "...", "request_id": "..." }
{ "type": "resume", "session_id": "...", "request_id": "..." }
```

Rover responds with `pause_ack` / `resume_ack` carrying `request_id`.

### KB_INVALIDATE

Tell the rover that a cached KB key is stale.

```json
{
  "type":       "kb_invalidate",
  "session_id": "...",
  "paths":      ["rovers.rover_1.config.tunables_v2", "..."]
}
```

Rover responds with `kb_invalidate_ack` (`paths` echoed). v1 rovers
without a cache simply ACK.

## Invariants

1. **No soft faults** (`feedback_no_soft_faults`). Any rejected
   REGISTER, FATAL exception, or expired DRAIN deadline halts the
   rover. The supervisor's chain-tree state machine handles the
   transition; `upward_peer:on_shutdown()` runs once before exit.

2. **One controller per rover at a time.** REGISTER includes
   `dongle_instance`; the controller refuses a second REGISTER with
   the same `(class, dongle_instance)` tuple while the first session
   is live.

3. **Idempotent ACKs.** Both ends MUST tolerate duplicate verbs
   carrying the same `request_id` / `exception_id` / `seq`.

4. **Heartbeat + ACK windows.** Default `heartbeat_period_s = 10s`.
   Three missed ACKs (= 30s of silence) = controller lost. The rover
   keeps running its current mission but stops accepting new RPC from
   the planner until reattached.

5. **KB writes are scoped.** The controller MUST deny any KB_WRITE
   whose `path` is not under the rover's `kb_root`. Cross-rover writes
   require an out-of-band privilege the rover never has.

6. **Transport-agnostic.** This document specifies verbs and payloads,
   not transport. Phase-2 implementation may use NATS subjects (one
   per session_id), MQTT topics (mirroring the planner's pattern), or
   an in-process channel for the test peer. The verb shape is what's
   normative.

## Test peer

`robot_controller_test_peer.lua` provides a fixture that exposes the
contract as a Lua object:

```lua
local ctrl = robot_controller_test_peer.new{
    session_id_factory = ...,
    kb_root_for        = function(robot_id) return "rovers." .. robot_id end,
}

ctrl:on_register(function(req) ... return ack_table end)
ctrl:on_heartbeat(function(req) ... return ack_table end)
ctrl:on_exception(function(req) ... end)
ctrl:on_kb_read(function(req) ... return response end)
ctrl:on_kb_write(function(req) ... return response end)

-- driven by the rover client (via in-process loopback or a real wire)
local ack = ctrl:handle(verb_table)

-- inject controller-initiated verbs:
local push = ctrl:make_drain(session_id, deadline)
```

The test peer is intentionally synchronous and in-process so contract
tests run without a broker. Real transport adapters (NATS, MQTT, …)
will sit between this peer's `:handle()` method and the wire on the
real service.

## Next steps before Phase 2 implementation

1. ✅ This contract document (`docs/controller/contract.md`).
2. ✅ `robot_controller_test_peer.lua` with the verb surface.
3. Stub-against-peer test (`test_upward_peer_contract.lua`): drive
   the current `upward_peer.lua` stub against the test peer and
   verify the harness rejects the stub's no-op responses with clear
   errors. This makes the harness self-validating.
4. Phase-2 implementation of `upward_peer.lua` (real client, real
   transport). Ship `robot_base:1.1`. Class images rebuild atop.
5. Phase-2 service (`robot_controller`) — separate container, not
   inside `robot_base`.
