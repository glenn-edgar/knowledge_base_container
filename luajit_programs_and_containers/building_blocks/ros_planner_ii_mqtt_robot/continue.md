# continue.md — 2026-04-26 late evening session handoff

Two distinct phases this session: implementation (slice 1d + 1e shipped
green) and design (phase-2 application catalogue locked for line drive +
B-spline path). Design rhythm held — one question at a time, lock before
moving on, no cascading rewrites. Streaming-mode catalogue deferred to
tomorrow; that's the only thing left before code.

## What changed in one paragraph

Slice 1d shipped `libcomm/{manifest,router,link}.{h,c}` — comm_init now
validates the embedded `comm_manifest_v1_wire_t` blob (8 invariants),
builds the router table, binds dongles[0] to a transport_inproc instance,
and stands up per-slave link FSM rows. Slice 1e shipped end-to-end
submit/poll/claim through the in-process transport with a 32-slot table
(handle = gen<<5 | slot, ABA-defended), depth-1 per-slave enforcement,
ACK_BARE/NAK responder loop, and surfaced events. `comm_ffi.lua` +
`ct_comm.lua` (8-fn helper) shipped. 106 unit checks green across 5
slices (1a..1e), zero warnings under `-Wall -Wextra`. After 1e the
session pivoted to phase-2 design: locked the application-layer cmd/event
catalogue for the drive-base in-process slave, including line-drive and
B-spline-drive flows, master-side comm_lib KB registry (3 slots), the
SUBSCRIBE/UNSUBSCRIBE infrastructure, and the universal validation
discipline (every cmd has a precondition; out-of-state → unique upstream
exception event). Streaming-mode flow (sensor-driven continuous motion
for line/wall following) deferred to next session.

## Settled today — won't revisit

### Slice 1d — manifest + router + link scaffolding

`comm_init` validates the 680B blob against 8 invariants (dongle_count,
host UUID sentinel, bus_id ownership, addr range, tick_period_ms floor,
mcu uniqueness, etc.). Router table is mcu→(dongle_idx, bus_id, addr)
with one row per declared slave. Link table is per-slave FSM rows
(state, miss_count, last_seen_ms, expected physics_model_id, plus 1e
additions: next_seq + outstanding_slot). All slaves start
COMM_NODE_UNKNOWN; phase-2 will wire JOIN handshake.

### Slice 1e — end-to-end loopback

32-slot table, handle = `(gen << 5) | slot`, gen starts at 1 and skips
0 on wrap (so handle=0 stays sentinel). `comm_submit` allocates a slot,
encodes m2s into transport's m2s ring, marks link.outstanding_slot.
`comm_poll` three-pass: drain m2s into slave decoder, slave handler
(PING→ACK_BARE, else NAK reason=0xFF) encodes s2m, drain s2m into
master decoder, match by `(mcu, ack_seq)`, mark slot DONE/NAK, clear
outstanding, stamp last_seen_ms, surface terminal slots into caller's
buffer (surfaced flag prevents re-emission). `comm_status`/`comm_claim`/
`comm_cancel` manage the slot lifecycle. Decoders persisted across
poll calls via per-slave `g_slave_decoder[64]` and `g_master_decoder[64]`.
`comm_now_ms` exposed. `_POSIX_C_SOURCE 200809L` at top of comm.c for
clock_gettime under c99-glibc. 38 loopback checks green.

### Phase-2 architectural rules locked

1. **App-level validation, not link-level.** libcomm doesn't know cmd
   codes. Slave-class app handler validates everything.
2. **Every error condition gets a unique upstream event code** —
   chain-tree exception column maps each event 1:1 to a recovery
   class. No generic FAULT with reason byte.
3. **Every cmd has a precondition; out-of-state → unique upstream
   event → exception → RESET.** Universal rule; applies to every cmd
   added in the future too.
4. **Bad cmd is an upstream message, not a link-level NAK.** Treated
   as exception by chain-tree.
5. **State-change + exception events are always-on.** SUBSCRIBE is
   purely for periodic telemetry; structural events can never be
   "forgotten to subscribe to."
6. **Cmd code numerical assignments deferred to schema-write time.**
   avro_dsl is the source of truth.

### Drive-base line catalogue (locked)

Cmds: `DRIVE_LINE_INIT`, `DRIVE_LINE`, `ABORT_PATH`, `STOP`, `RESUME`,
`RESET`, `SUBSCRIBE`, `UNSUBSCRIBE`. Events: `SEG_COMPLETE` (always
remaining > 0), `PATH_COMPLETE` (replaces final SEG_COMPLETE),
`PATH_ABORTED`, `ROBOT_STOPPED`, `ROBOT_RESUMED`, `RESET_COMPLETE`,
`QUEUE_STARVED`. Exceptions: `BAD_CMD`, `INIT_WHILE_ACTIVE`,
`DRIVE_LINE_WITHOUT_INIT`, `BAD_SUBSCRIBE_PERIOD`,
`SUBSCRIPTION_CAP_EXCEEDED`, `CROSS_TRACK_ABORT`,
`KB_REGISTRATION_OVERFLOW`. Full payload table in
`project_drive_base_catalogue.md`.

### Drive-base spline catalogue (locked)

B-spline cubic uniform, **patch-per-cmd (Shape Y)**: each cmd carries
4 control points + speed; planner repeats first 3 CPs for C² continuity;
slave validates against trailing 3 of previous segment with relative
tolerance (`max(abs_floor, rel × |coord|)`). Slave converts each
B-spline span to its equivalent cubic Bezier via 4×4 matrix at queue
time, reuses existing pure-pursuit-over-Bezier follower. Shape X
(incremental, 12 B/seg) rejected as premature optimization with
state-divergence risk; reserved as future slave-internal optimization.

`SPLINE_INIT` 38 B (count + 4 CPs + speed). `DRIVE_SPLINE` 36 B (4 CPs
+ speed). `DRIVE_SPLINE_WITHOUT_INIT` analog exception. New event:
`SPLINE_DISCONTINUITY {slave_state u8, seg_id u32, mismatch_cp_idx u8,
drift_m f32, sim_t f64}` 18 B → FAULTED.

### Manifest schema growth

`manifest_tunables` gains `spline_continuity_abs_floor_m f32` (default
1µm) + `spline_continuity_rel f32` (default 1e-5). Schema hash
regenerates when avro_dsl re-runs.

### Slave-side state model

Two counters: `current_op_remaining` (decrements on each SEG_COMPLETE),
`next_op_pending_count` (set when INIT slots into lookahead during
op-to-op transition; promotes when prior op's last seg completes).
Queue depth 2 (active + lookahead). Subscription table cap 8 rows.
Open-enum `slave_state` byte. RESET clears subscriptions.

### Subscription system

`SUBSCRIBE {event_code u16, period_ms u16}` 4 B → ACK_BARE (silent
last-one-wins on repeat). `UNSUBSCRIBE {event_code u16}` 2 B → ACK_BARE
(silent no-op if absent). Min period = `5 × CT_COMM_RX_PERIOD_MS = 100ms`
(stays out of chain-tree heartbeat budget). `BAD_SUBSCRIBE_PERIOD`
exception fires below the floor or at period_ms=0.

### Master-side comm_lib registry

Chain-tree-side `comm_lib` keeps a 3-slot registry of KB main-node-ids.
KB main fn registers on init, deregisters on terminate. Every upstream
event is **broadcast to all registered main nodes**; each KB's tree
walker dispatches to whichever child nodes filter it via
`asm_wait_for_event` matching by event_id. 4th register attempt →
`KB_REGISTRATION_OVERFLOW` exception. Bump cap rather than add soft
recovery if reality demands it.

### Op-to-op transition (chain-tree side, no wire op_id needed)

1. Old KB sets blackboard "wind_down" event when sending its last segment.
2. Old KB column doesn't terminate yet — waits for its final SEG_COMPLETE
   (which is delivered as PATH_COMPLETE).
3. New KB started early in parallel, observes wind_down, sends exactly
   one INIT-bearing packet (its first seg + count), then idles.
4. Old KB receives PATH_COMPLETE → terminates.
5. New KB activates → owns SEG_COMPLETE consumption.

Disambiguation is purely temporal — only one KB is "actively listening"
at a time despite up to 3 being registered. SEG_COMPLETE has no op_id
on the wire.

## Files touched today

**Modified (libcomm):**
- `libcomm/comm.h` — added comm_now_ms declaration.
- `libcomm/comm.c` — extensively rewritten for slice 1e (slot table,
  submit/poll/status/claim/cancel, in-process slave loop, master rx
  matching). Added `_POSIX_C_SOURCE 200809L` for clock_gettime.
- `libcomm/link.{h,c}` — added `next_seq` + `outstanding_slot` fields
  for depth-1 enforcement.

**New (libcomm):**
- `libcomm/manifest.{h,c}` — slice 1d.
- `libcomm/router.{h,c}` — slice 1d.
- `libcomm/link.{h,c}` — slice 1d (extended in 1e).

**New (project root):**
- `comm_ffi.lua` — handcrafted FFI binding to libcomm.
- `ct_comm.lua` — 8-fn helper (submit/broadcast/poll/claim/cancel/
  handle_slot/handle_gen/now_ms).
- `test_link_router.lua` — 17 checks (slice 1d).
- `test_comm_loopback.lua` — 38 checks (slice 1e).

**Modified (project root):**
- `Makefile` — adds new libcomm sources, `-I.` for generated headers,
  `libcomm.so` depends on `manifest` target so generated headers exist
  before C compile.
- `run_tests.sh` — runs slice 1d and 1e tests.

## How to resume tomorrow

```bash
cd /home/gedgar/knowledge_base_assembly/luajit_programs_and_containers/building_blocks/ros_planner_ii_mqtt_robot
make clean && make             # confirm libphysics + libcomm + manifest still green
./run_tests.sh --skip-e2e      # 26 physics + 12 frame + 13 transport + 17 link/router + 38 loopback = 106 checks
```

Then read the locked design:
```
~/.claude/projects/-home-gedgar-knowledge-base-assembly/memory/project_drive_base_catalogue.md
```

That file has everything the design session locked tonight. Phase-2
implementation does NOT start until the streaming-mode flow is also
designed (~1 short session tomorrow).

## Plan for next session — design first, then implementation

### Session 2A — streaming-mode design (~30 min, design only)

Approach 2 was locked: distinct cmd flow for sensor-driven continuous
motion. New cmds anticipated: `DRIVE_LINE_STREAM_INIT` (no count),
`DRIVE_LINE_STREAM`, `STREAM_END` (graceful drain). New event
`QUEUE_EMPTY` (informational, distinct from QUEUE_STARVED — robot
decelerates, slave stays ready). Spline-streaming variants may follow.

Open issues to resolve next session:
1. Termination semantics — ABORT_PATH vs STREAM_END vs new-INIT-replaces.
2. Whether segment rate can exceed the 100 ms minimum subscribe period
   (segment rate is independent of subscribe rate, but worth confirming
   the chain-tree comm_rx walker can drain 30 Hz segments without
   urgent-ring overflow).
3. Whether the depth-2 lookahead model still applies, or streaming
   uses depth=1 (since the planner doesn't know N segments ahead).
4. Whether new exception events are needed (TRACKING_LOST,
   SENSOR_TIMEOUT, etc.) or these are sensor-side concerns the
   planner translates into RESET.

### Session 2B — phase-2 implementation slicing

After streaming-mode is locked, break phase 2 into slices. Tentative
ordering (each ≤500 LoC):

1. **Slice 2a — slave-handler dispatcher infrastructure.** Adds
   `comm_set_slave_handler(mcu, fn, ctx)` to libcomm. comm_poll's
   in-process slave loop calls the registered handler instead of the
   built-in PING-only responder. Loopback test: register a handler
   that re-implements PING, prove the API works.
2. **Slice 2b — urgent-pending ring + DRAIN mechanism.** Per-slave
   urgent ring (depth 4). Slave handler can `comm_slave_push_urgent`.
   Master sees ACK_FLAG_URGENT, automatically sends CMD_DRAIN. New
   master-side path for upstream events (separate from
   handle-correlated responses).
3. **Slice 2c — JOIN handshake.** Slave-initiated JOIN_REQ on first
   poll after attach; master JOIN_ACK; slave JOIN_CONFIRM with
   physics_model_id. Link state UNKNOWN→PENDING→LIVE.
4. **Slice 2d — drive-base catalogue + adapter (line drive only).**
   `drive_base_cmds.lua` (avro_dsl), generates header + FFI.
   `drive_base_slave.c` adapter, wired to physics_core. DRIVE_LINE_INIT,
   DRIVE_LINE, ABORT_PATH, STOP, RESUME, RESET, SUBSCRIBE/UNSUBSCRIBE
   handled. Loopback test: drive a 3-segment line path, verify pose
   moved.
5. **Slice 2e — spline catalogue.** B-spline patch-per-cmd cmds,
   B-spline-to-Bezier conversion, continuity validation. Loopback test:
   drive a 3-segment spline.
6. **Slice 2f — streaming flow** (after streaming-mode is locked).
7. **Slice 2g — comm_lib (master/chain-tree side) registration system,
   subscription routing, statistics events.**

Slice 2g is where the chain-tree integration happens. Probably its own
session because it pulls in the chain-tree LuaJIT runtime.

## Open items (not settled — discuss when needed)

1. **Statistics / telemetry events** for measuring robot PID and
   tracking-loop performance. Names + payloads work-in-progress; ride
   the SUBSCRIBE mechanism unchanged. Likely first ones: POSE_UPDATE,
   PATH_TRACKING_STATS, MOTOR_STATS, BATTERY_STATS. Designed as needed.
2. **physics_model_id placeholder** in rover_1 manifest still 0. Real
   FNV-1a hash needed once physics gets its own avro_dsl record.
   Triggered when libcomm phase-3 wires the JOIN_CONFIRM check.
3. **avro_dsl GENERATE_FFI bug** — `comm_manifest_ffi.lua` has a broken
   `const_packets` section (`pkt.data.dongles[0] = table: 0x...` Lua
   tostring leaking into generated source). Both 1d and 1e tests work
   around it via inline `ffi.cdef`. Fix: avro_dsl GENERATE_FFI const-
   packets emitter needs to walk nested tables instead of stringifying
   them. Not blocking.
4. **avro_dsl fork divergence**: 5+ copies in the tree. Only the
   LuaJIT copy patched. Future consolidation = one canonical copy +
   symlinks/package.path. Not blocking.
5. **Application command catalogue beyond drive-base**: future slave
   classes (manipulator, wiper, etc.) each get their own catalogue
   file. Manipulator is conceptually a separate slave (gripper/arms
   are their own MCU, even today's `physics_core.c` lying that they're
   tools is a sim-only convenience). Defer until there's a real
   second slave to integrate against.
6. **Per-segment timeout on the slave** (segment never completes due
   to stuck or low-battery). physics_core has `cross_track_abort`
   that surfaces as CROSS_TRACK_ABORT — generic "stuck" timeouts not
   designed yet. Phase-3 work.

## "You did good today" — keeping a snapshot of what worked

Two-mode session: implementation + design. Both held up.

Implementation half: slice 1d went down in ~2 hours green; slice 1e was
larger but the lock-then-code discipline (comm.c rewrite was already
fully designed in conversation by the time the file got opened) kept it
to one cycle of build-fix-test. The clock_gettime POSIX feature-macro
hiccup was a 30-second fix; nothing else needed iteration. 38 loopback
checks passed first run.

Design half: ~25 questions locked over ~90 minutes. The
question-at-a-time discipline worked — when I started writing big
exploratory blocks (the streaming-mode "consequences" section), user
correctly called it back. Every lock was a small, defensible step. The
two real architectural insights came from the user's instincts, not
mine: B-splines over Bezier (continuity matters at the path level —
I'd defaulted to Bezier because physics_core uses them), and the
blackboard-mediated parallel-KB op transition (I was reaching for an
op_id on the wire — the right answer was temporal disambiguation in
chain-tree state). Each was a "you're right, that's better" moment.
Lesson held: when the user pushes back on something terse and
load-bearing, the redirect usually points at the cleaner answer.

The exception-event-per-condition discipline was my favourite lock of
the night. Maps directly to the chain-tree exception column. No
generic FAULT with reason byte means the chain-tree dispatcher stays
dumb; the wire codes carry the disposition. Easy to add new errors
later.

Don't prematurely break out the streaming-mode catalogue tomorrow.
Likely it's enough to add 3 cmds and 1 event; the count-based work
already did the hard infrastructure (subscription table, queue,
two-counter state, blackboard transitions).
