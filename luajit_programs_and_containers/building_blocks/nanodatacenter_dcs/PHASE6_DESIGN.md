# Phase 6 — Sync layer rewrite via RPC queues

**Status:** design, 2026-04-26 (post-evening strategic session). No
code yet.
**Audience:** future implementation session, post Phase 4 close-out.
**Companion docs:** `building_blocks/observability/continue.md`
(session handoff), `feedback_one_reset_path.md`,
`feedback_three_tier_config.md`, `project_v3_platform_roadmap.md`,
`project_file_store.md` (memories).

---

## 0. Strategic locks set 2026-04-26 evening session

These shape the implementation direction and override the original
draft body where they disagree. Body sections below have been updated
where most relevant; this section is the canonical summary.

### Hardware target

* CPU: Pi 4 or above (Pi 5, x86 mini-PC, industrial ARM SBC OK as long
  as Debian-based).
* OS: Debian Linux. **Native, not WSL2/Docker Desktop.** WSL2 is dev-
  only — no code paths for its quirks.
* Memory: 8GB+ per node.
* Storage: USB3 SSD or better. **No SD-card class.** Aggressive pg I/O
  is fine; no wear-leveling avoidance needed.

### Scale + topology

* 8 nodes per cluster v1; up to 16 v2.
* Beyond ~16 = federation/cloud-bridge, not in-cluster sharding.
  ("Cluster = site.")
* MQTT-registered devices (Pi Zero, sensors) are app-level, NOT
  platform-tier. Apps like `ros_mission_planner_ii` handle their own
  MQTT registration.
* **Strict all-N quorum.** No degraded operation. Any node loss =
  cluster fault.
* **Fixed master.** cpu_01 hosts pg + 4 infra containers + (planned)
  image registry. No leader election.
* Master loss = site outage; operator manually re-provisions
  (productization concern, post-v1).

### Maintenance discipline (one startup path)

* Planned shutdown = **whole-cluster shutdown.** DRAIN cascades site-
  wide. After all DRAIN_READY, dcs.lua processes exit. Operator does
  hardware work. Cluster brought back through the **only** startup
  path — same as fresh deploy, same as power-loss recovery.
* No "operate-degraded." No "pause-and-resume." Single startup path
  prevents startup-cycle errors (Bell/AT&T 1990).

### Three-tier configuration model (step 4)

* **JSON file** (read first, before pg available): site_id, cpu_id,
  master_host, pg/nats/mqtt URLs, secrets_path. Pattern is v2's
  `Get_site_data(file_name)` ported to Lua.
* **secrets.env file** (operator-managed): `POSTGRES_PASSWORD`. Never
  in KB.
* **KB** (read after pg connection up): tunables, catalog, runtime
  state. One-time reads at startup mostly.
* **Code** (compile-time): protocol verb names, magic strings, schema
  versions, image tag references in `definitions.lua`.

### File store (referenced from step 4 + step 5)

Content-addressable file/blob store in pg landed in commit
`6a63eec3`. Three tables (`<db>_doc_class`, `<db>_fs_blob` sha256-
keyed, `<db>_fs_node` path→blob). DSL is `kb:add_doc_class{...}`. As
of 2026-04-26 it is **schema-only, no live data, no automated test
coverage.** Smoke test queued before catalog migration. See
`project_file_store.md`.

### Cloud integration

* Cluster = site. Federation when too big.
* First touch point: tree-shaped observability data (step 6) — log/
  exception analysis runs cloud-side eventually.
* Second touch point: file store sha256-keyed blobs federate by
  construction (no conflict resolution needed for content-addressable
  storage).

### Verb set (8, not 7)

The original draft had 7 verbs. Strategic discussion added
**`DRAIN_READY`** as the container's response to `DRAIN`. Final
inter-CPU set:

`JOIN_REQ`, `JOIN_ACK`, `JOIN_CONFIRM`, `HEARTBEAT`, `HEARTBEAT_ACK`,
`RESET_HINT`, `DRAIN`, `DRAIN_READY`.

Container layer mirrors with the same 8 plus `PAUSE` / `RESUME` /
`CONTAINER_READY` (Phase 6.4).

### Catalog hydration in JOIN handshake (Q4a closure)

Bootstrap.db moves to live-from-pg. Slave's JOIN handshake includes
catalog hydration: master pushes the slave's catalog rows after JOIN_
ACK; slave reaches ACTIVE only after hydration completes. File-staged
bootstrap.db (today) goes away. Eliminates the file-staging step in
the deploy flow; slaves only need site.json + secrets.env + start.sh.

### Step ordering (the 7-step roadmap)

| Step | Theme |
|---|---|
| 1 | Solidify system/node-control RPC (Phase 6.1+6.2+6.3 + planned-DRAIN) |
| 2 | Container base + RPC methods (Phase 6.4) |
| 3 | Condense for build (manual deploy from master Pi) |
| 4 | KB-driven everything (file store, three-tier config, catalog hydration) |
| 5 | App-container build documentation |
| 6 | Log-analysis web UI by KB namespace tree (cloud-driven) |
| 7 | v1 done = soak-node deployed + 30-day adversarial soak running |

This roadmap supersedes the original §9 migration plan in this doc.

---

## 1. Principle

The sync layer in DCS — every place where a node decides "is my peer
alive, is the cluster healthy, can I do work" — will be rewritten
under one rule:

> **Exactly ONE reset path. Symmetric on master, slave, and infra.
> Joins are a 3-way handshake. Detection-of-loss is RPC timeout.
> The reset is process-restart via watchdog, not state-machine reset.**

Heterogeneous recovery paths cascade. The historical lesson is the
**AT&T January 15, 1990 long-distance collapse**: a recovering switch
sent a recovery message; receiving switches had a bug that crashed
*them*; their crashes propagated to neighbors. 60,000 calls/min lost,
9 hours. Root cause was not hardware — it was N different recovery
behaviors interacting pathologically. With one reset path, N=1 and the
N² interaction surface is zero.

This principle layers on top of two existing rules already in memory:

- `feedback_no_soft_faults` — fault paths halt; no retries, no
  recovery branches, no soft-fault flags.
- `feedback_coordinator_startup_wipe` — reset state must be scoped
  local-only. Never wipe state another participant owns.

Phase 6 makes both of those structural instead of disciplinary.

---

## 2. Today's violations (Phase 4 audit + 2026-04-26 empirical)

The audit run on 2026-04-26 found four major violations of the
principle in the existing `dcs_host/` and `chain_tree/` code, and the
session-end empirical bounce confirmed Violation #2 the hard way:
master alone could not converge after restart because slave's stale
sync bit was never re-asserted (master cleared its view, slave never
noticed master had restarted, and there is no ACK channel for slave to
verify master saw its bit).

Audit citations (kept short here; see audit transcript for full
detail):

1. **Multiple asymmetric reset paths.** Master has 4 failure modes
   converging on watchdog restart; slave has 2; slave cannot initiate
   master recovery. (`dcs_host/dcs.lua:310-313`,
   `start.sh.template:72-78`, `chain_tree/dcs_dsl.lua:158-160`,
   `chain_tree/dcs_dsl.lua:279-282`)

2. **Master-initiated 1-way assertion vs. 3-way handshake.** Slave
   writes its sync bit; master polls and never ACKs; slave waits for
   `cluster_go` (a GO signal, not an ACK of slave's bit).
   (`dcs_host/user_functions.lua:352-413`)

3. **Infra container loss → full system teardown.** Any infra loss
   cascades across all CPUs; no per-node restart-in-place attempt.
   (`dcs_host/user_functions.lua:594-642`,
   `chain_tree/dcs_dsl.lua:281-282`,
   `chain_tree/dcs_dsl.lua:311-326`)

4. **Slave cannot detect master loss.** Slave waits the full 60s
   `wait_go` timeout; no early detection. Passive, slow, asymmetric.
   (`chain_tree/dcs_dsl.lua:151-162`)

What today *does* match the principle (keep these):

- Sync state cleanly cleared on teardown (`chain_tree/dcs_dsl.lua:311-326`).
- Symmetric heartbeat publish at monitor state (master + slave both
  publish on 5s reset-loop).
- Patient-forever sync with no crash on transient infra delay during
  startup (`dcs_host/user_functions.lua:301-313`).

---

## 3. Architecture: RPC queues replace polled bits

**Today:** sync state is polled shared rows in
`knowledge_base_status` (`cluster_sync_bits`, `ready_bits`,
`cluster_go`, heartbeat counters). Master and slaves all read/write a
common mutable state. This is a shared-memory pattern. The
2026-04-26 master-bounce hang is a textbook stale-bit-in-shared-memory
failure: slave's pre-restart bit looked fresh to master (because
master cleared its view first, then awaited bits, but slave had no
trigger to re-write).

**Phase 6:** sync state is **message-passing** over per-node RPC
queues. The shared mutable state goes away.

### 3.1. Queue topology

One inbox per node — addressable mailboxes:

```
master_q          (slaves write JOIN_REQ / HEARTBEAT here; master reads)
cpu_02_q          (master writes JOIN_ACK / GO / RESET_HINT here; cpu_02 reads)
cpu_03_q          (...etc per slave)
```

Backed by the existing `rpc_client`/`rpc_server` tables in postgres
(per `project_dcs_data_model`: CPU-scoped RPC queues already exist).
No new infrastructure required; this is a usage rewrite of code that
has been writing/reading bit-mask rows.

### 3.2. RPC verb set (sync layer — 7 verbs)

| Verb | Direction | Payload | Purpose |
|---|---|---|---|
| `JOIN_REQ` | slave → master | `{cpu_id, epoch, bootstrap_hash}` | Slave declares intent to join. Epoch is slave's boot timestamp; bootstrap_hash detects catalog mismatch. |
| `JOIN_ACK` | master → slave | `{cpu_id, master_epoch, accepted_epoch}` | Master acknowledges a specific JOIN_REQ. Echoing slave's epoch lets slave detect a stale ACK. |
| `JOIN_CONFIRM` | slave → master | `{cpu_id, epoch}` | Slave's ACK-of-ACK. Master marks slave ACTIVE only after seeing this. |
| `HEARTBEAT` | slave → master | `{cpu_id, epoch, seq}` | Steady-state liveness. Slave-initiated; master-loss detected by slave's RPC timeout. |
| `HEARTBEAT_ACK` | master → slave | `{cpu_id, master_epoch, seq}` | Master confirms heartbeat received. |
| `RESET_HINT` | master → slave | `{cpu_id, reason}` | Polite "please reset." Slave then SIGTERMs itself; watchdog respawns. Cleaner than relying purely on heartbeat-timeout. |
| `DRAIN` | slave → master | `{cpu_id, epoch}` | Slave going down cleanly (operator action). Master ages slave out without alarming. |

The same shape extends to container ↔ dcs in §7.

### 3.3. State machines

**Slave states:**

```
DISCONNECTED → JOINING (sent JOIN_REQ, waiting for JOIN_ACK)
            → ACK_RECEIVED (sent JOIN_CONFIRM, waiting for HEARTBEAT_ACKs)
            → ACTIVE
```

Any of:
- HEARTBEAT_ACK timeout (≥3 missed) at any state ≥ JOINING
- RESET_HINT received
- own-side error (chain-tree exception, etc.)

→ **fail-stop reset** (process exit → watchdog respawn → DISCONNECTED).

**Master per-slave map:**

```
{cpu_id → state, epoch, last_heartbeat_at}

state ∈ { UNKNOWN, JOINING_SAW_REQ, ACTIVE, DRAINING }
```

State transitions on master:
- saw JOIN_REQ from cpu_X with new epoch → write JOIN_ACK to cpu_X_q,
  set state JOINING_SAW_REQ.
- saw JOIN_CONFIRM matching the ACK'd epoch → set ACTIVE.
- saw DRAIN → set DRAINING, age out cleanly.
- HEARTBEAT freshness window expired → write RESET_HINT, set UNKNOWN.

Master's per-slave state lives in master-owned KB rows
(`system.site.<site>.sync_control.peer_state.<cpu_id>` structured:
`{state, epoch, last_heartbeat_at}`) for **observability** — a
debugging tool can SELECT against it and see exactly what master
believes about each slave. Correctness does not depend on
persistence; on master restart, master starts with empty per-slave
state and slaves rejoin via retry.

### 3.4. Why this kills the 2026-04-26 bug class

Today's bug was: master cleared `cluster_sync_bits` on startup; slave's
bit-1 in the cleared mask vanished; slave never knew. Master waited
for quorum forever.

In the RPC model:

- There is no shared bit. Slave's "bit" is the absence/presence of a
  fresh JOIN_CONFIRM from slave with a current epoch.
- On master restart, master's per-slave map starts empty — that's
  fine, master's view is master-private. Master broadcasts nothing;
  slaves just notice their HEARTBEAT_ACK timeouts and reset.
- Slave's reset triggers a fresh JOIN_REQ with a new epoch.
- Master sees JOIN_REQ, ACKs, slave CONFIRMs, slave is ACTIVE.

No shared mutable state means no stale-shared-state failure.

---

## 4. Phase 6.1 — Inter-CPU sync via RPC queues

**Scope:** rewrite the master ↔ slave sync handshake using the §3 RPC
model. Replace `cluster_sync_bits`, `cluster_go`, and the
sync_control_master / sync_control_slave bit-bashing with the 7-verb
protocol. Keep `ready_bits` for the application-readiness signal (it
encodes a different invariant — "node has finished its setup phase"
— that is orthogonal to sync; can revisit in Phase 6.4).

**Code touch list (estimated):**

- `dcs_host/user_functions.lua` — replace `SET_OWN_SYNC_BIT`,
  `VERIFY_SYNC_QUORUM_OR_TIMEOUT`, `WAIT_GO`, `cluster_go` writers
  with RPC verbs. Add per-slave state-table writer.
- `chain_tree/dcs_dsl.lua` — sync_master and sync_slave columns get
  rewritten; await_quorum becomes "process JOIN_REQ queue"; wait_go
  becomes "await JOIN_ACK + send JOIN_CONFIRM".
- `chain_tree/dcs.json` — same chain-tree shape, new node bodies.
- New: RPC client/server helpers if they don't already exist as
  re-usable lua modules.
- `construct_kb/` — schema for `peer_state.<cpu_id>` + queue rows if
  not already covered by existing `rpc_client`/`rpc_server` schema.

**Acceptance test:**

1. Boot fresh cluster — master + slave reach ACTIVE within 15s.
2. SIGTERM master only — slave's HEARTBEAT_ACK times out within 30s
   (3 × 10s heartbeats), slave fails-stop and respawns. Master
   respawns from watchdog within 2s. Both reach ACTIVE within 30s
   total. **No bit-mask gymnastics required.** This is the failure
   mode that hung the cluster on 2026-04-26.
3. SIGTERM slave only — master ages slave out cleanly within 30s; on
   slave respawn, fresh JOIN_REQ, ACTIVE within 15s.
4. Both SIGTERMed simultaneously — both respawn, JOIN handshake
   converges within 30s.

---

## 5. Phase 6.2 — Per-node infra restart

**Scope:** replace the full-system teardown on infra loss with
per-node cooperative pause + infra-restart attempt. Only escalate to
system teardown after N retries.

**Today** (`chain_tree/dcs_dsl.lua:281-282` + `:311-326`): any infra
heartbeat staleness → ERR_MONITOR_TRIP → request_shutdown_st →
teardown_st (stop all 4 infra containers) → terminate_system → all
nodes reset. One infra container blip nukes the whole cluster.

**Phase 6.2** uses the §3 RPC verbs:

1. Master detects infra heartbeat staleness via broker snapshot.
2. Master writes `RESET_HINT` to its **own** master_q with reason=
   "infra restarting" — this is master telling itself "pause."
3. Master broadcasts `PAUSE` (new container-layer verb, see §7) to
   each app container's queue: "stop new work, finish in-flight,
   ack PAUSED."
4. Master via broker issues `docker restart <infra>` (Phase 2 broker
   mutation, already exists).
5. Master polls broker snapshot until infra is back; broker probe
   (Phase 4) is the canonical "ready" signal.
6. Master broadcasts `RESUME`. Containers ack.
7. Cluster operates normally.

Escalation: if the same infra container fails restart 3× consecutively
within 5 min, master writes `RESET_HINT` to all participants
(including itself) and the cluster goes through a full reset+rejoin.

**Why this is correct under one-reset-path:** the per-node infra
restart isn't a "soft recovery" branch — it's bounded
(N retries / time window) and falls back to the *same* reset path
(`RESET_HINT` → process exit → watchdog) on failure. There is still
exactly one terminal reset; we just attempt a cheaper cooperative
pause-and-resume first.

**Code touch list:**

- `dcs_host/user_functions.lua` — replace
  `VERIFY_SYSTEM_CONTAINERS_HEALTHY`'s ERR_MONITOR_TRIP fire with the
  pause/restart/resume sequence.
- `chain_tree/dcs_dsl.lua` — add `pausing` and `restarting_infra`
  states to the system_control state machine. Existing teardown
  becomes the escalation-only branch.

**Acceptance test:** `docker restart pg-vector` while cluster is at
sys_ready — cluster pauses, infra restarts, cluster resumes, no slave
or app container restart. Total downtime ≤30s.

---

## 6. Phase 6.3 — Bidirectional master-loss detection

**Scope:** **automatic fallout of 6.1.** Slave's HEARTBEAT_ACK timeout
*is* the master-loss detection signal. Slave doesn't need a separate
master-heartbeat poll; the round-trip on its own heartbeat is the
proof of life.

Specifically: 6.3 is implemented by `HEARTBEAT_ACK` in the verb set
(§3.2). With heartbeat at 5–10s and ≥3 misses to declare lost,
master-loss is detected in 15–30s, vs. 60s for today's `wait_go`
timeout. The rest is fallout.

**Acceptance test:** cover under 6.1 acceptance test #2 (master
SIGTERM → slave detects via heartbeat timeout, not wait_go).

---

## 7. Phase 6.4 — Container ↔ dcs RPC queues

**Scope:** apply the same RPC discipline to the container layer.
Closes the gap that 2026-04-24's walker-starvation cascade exploited:
today there is no in-band signal that an app container's chain-tree
controller is alive (only docker-level state via broker snapshot, and
HTTP responsiveness via Phase 4 probes — neither catches controller
starvation).

### 7.1. Topology

Each app container's controller speaks RPC to its CPU's queue:

```
container_<name>_q  (dcs writes PAUSE / RESUME / DRAIN / RESET_HINT)
master_q (or per-CPU queue) (container writes CONTAINER_READY / HEARTBEAT)
```

Infra containers (`pg-vector`, `nats-js-ram`, `mosquitto-ram-ws_main`,
`kv-bridge`) are off-the-shelf images with no RPC client — they stay
broker-probed. Clean split: app = RPC, infra = broker snapshot.

### 7.2. RPC verb set (container layer — 7 verbs, mirrored from §3)

| Verb | Direction | Payload | Purpose |
|---|---|---|---|
| `CONTAINER_READY` | container → dcs | `{name, slot, epoch}` | Controller chain-tree finished setup, ready to receive traffic. dcs marks ACTIVE only after this. Phase 4 HTTP probe still complements (HTTP-layer ready ≠ controller ready). |
| `HEARTBEAT` | container → dcs | `{name, epoch, seq}` | Controller liveness. Slow cadence (1 min) — see §8. |
| `HEARTBEAT_ACK` | dcs → container | `{name, dcs_epoch, seq}` | dcs confirms heartbeat received. |
| `PAUSE` | dcs → container | `{name, reason}` | Cooperative pause: stop accepting new work, finish in-flight, ack `PAUSED`. Used by 6.2 during infra restart. |
| `RESUME` | dcs → container | `{name}` | Resume normal operation. |
| `DRAIN` | dcs → container | `{name}` | Graceful shutdown — finish in-flight, then exit cleanly. dcs then `docker rm`s. **Implements the cooperative tear-down that `feedback_kill_non_infra_contract` flagged as missing.** |
| `RESET_HINT` | dcs → container | `{name, reason}` | Polite "please reset your controller chain-tree." Container's controller resets in-place. If 3× resets fail, dcs escalates to `docker rm` + run-fresh. |

### 7.3. Two-tier escalation

Container failure modes have two tiers:

- **Controller chain-tree reset** (in-place): container stays alive,
  controller re-runs its chain-tree from scratch. Triggered by missed
  HEARTBEAT_ACK or explicit RESET_HINT. Recovery time: a few seconds.
- **Container restart** (`docker rm` + node_control reconcile):
  container goes away, broker spawns fresh. Triggered by 3× failed
  in-place resets, OR by docker-level state (broker snapshot says
  container exited).

The two tiers map to the same single reset *discipline* (fail-stop +
clean rejoin), with different blast radius. Heartbeat is the trigger;
everything else is bounded escalation.

### 7.4. What this subsumes

Phase 6.4 rewrites the previously-queued "luajit_base controller
hardening" work item from the prior `continue.md`. The original list
was:

- Add a "sync lost" state to the luajit-base controller chain-tree.
  → becomes: controller's RPC client manages JOINING / ACTIVE /
  RESETTING states.
- Add a worker-pause primitive. → becomes: PAUSE verb handler.
- Add a per-app heartbeat from app workers to controller, so the
  controller knows when a worker has acked the pause. → becomes:
  intra-container concern, controller-internal.
- Define recovery semantics. → becomes: covered by RPC state machine
  + heartbeat-ACK timeout.
- WATCHDOG strobing cadence. → becomes: §8 cadence table answers it.

### 7.5. Acceptance test

1. Start `test_app_01`. CONTAINER_READY arrives within 5s of docker
   "running"; dcs marks ACTIVE.
2. dcs sends `PAUSE` (e.g., during infra restart). Container ACKs
   PAUSED within 1s.
3. dcs sends `RESUME`. Container resumes.
4. Stop heartbeats from inside the container (simulate controller
   starvation): dcs's HEARTBEAT_ACK timeout fires after ≥3×1min
   (worst case 3 min); dcs writes `RESET_HINT`. Container's
   controller resets. (Alternative: kill -STOP on controller process
   — broker probe (Phase 4) catches HTTP-layer wedge in 15s, RPC
   heartbeat catches controller wedge in 3 min. Both layers
   fire independently.)
5. Send `DRAIN`. Container ACKs, finishes in-flight, exits 0 within
   N seconds. dcs `docker rm`s without complaint.

---

## 8. Heartbeat cadence — layer-specific, not uniform

| Layer | Cadence | ≥3-miss threshold | Detection latency |
|---|---|---|---|
| Inter-CPU sync (master ↔ slave HEARTBEAT) | **5–10s** | 30s | ≤30s |
| Container ↔ dcs HEARTBEAT | **1 min** | 3 min | ≤3 min |
| JOIN handshake retries (transient state only) | **1–2s** | per-message timeout 5s | ≤5s |
| Phase 4 broker HTTP probes (existing) | **5s** | 3 (existing) | 15s |

**Why different cadences:**

- Sync layer failure is sub-cluster coordination; seconds matter.
- Container controller failure develops slowly anyway; broker probes
  cover the fast-path HTTP-wedge case.
- JOIN retries fire only during transient JOINING state; once ACTIVE,
  drop to steady cadence.
- Phase 4 probes don't change — they're an independent signal for
  HTTP-layer wedge.

**Two universal rules across all layers:**

1. **Threshold is ≥3 missed beats** before declaring lost. Single-
   miss alarms are noise.
2. **Jitter the cadence** (each participant picks a random offset 0–
   10% of its own period). Prevents synchronized-reset storms when a
   shared dependency (pg, nats) blips.

**Optional refinement (not in initial Phase 6.1 scope):** adaptive
cadence — fast when uncertain (5–10s after JOIN, after one missed
beat), slow when stable (drop to 30s or 1 min after ACTIVE for ≥5
min). Adds ~20 lines per side. Defer until soak shows fixed cadence
is wasting I/O.

---

## 9. Migration plan

Order matters: lock the principle at the inter-CPU layer first, then
propagate outward.

| Sub-phase | Scope | Sessions | Depends on |
|---|---|---|---|
| 6.1 | Inter-CPU sync via RPC queues (replaces bit-mask handshake) | 1–2 | Phase 4 soak complete |
| 6.2 | Per-node infra restart (uses 6.1 verbs) | 1 | 6.1 |
| 6.3 | Bidirectional master-loss detection | 0 (free fallout of 6.1) | 6.1 |
| 6.4 | Container ↔ dcs RPC queues (subsumes luajit_base hardening) | 2 | 6.1 stable in soak |

**Backwards compatibility:** none required. Phase 6 is a rewrite, not
an extension. The bit-mask paths in `user_functions.lua` and
`dcs_dsl.lua` are deleted, not deprecated. Per
`feedback_no_band_aid_over_architecture`: either commit to the
rewrite or leave the system visibly broken — we are committing.

**Rollback:** standard git revert of the relevant commits + bootstrap
rebuild + cluster restart. No persistent on-disk format change other
than the new `peer_state.<cpu_id>` rows (additive, ignored by old
code).

---

## 10. Open questions

These are deferred to the implementation session for 6.1; flag them
when starting code.

1. **Bootstrap_hash in JOIN_REQ.** Should slave compute and send a
   hash of its own bootstrap.db so master can detect catalog
   mismatch? Useful but adds complexity. Defer to 6.1 stretch goal.
2. **Master grace period after restart.** Should master wait N seconds
   before processing JOIN_REQs (to avoid pounding from simultaneously-
   restarting slaves)? Recommendation: yes, 2s — long enough for
   master's own setup, short enough to not slow first join.
3. **Queue read concurrency.** Lua single-threaded; processing one
   JOIN_REQ blocks all RPC for its duration. Recommend: cap RPC
   handler at <50ms; if heavier work needed, queue it for the
   chain-tree tick loop to pick up.
4. **`ready_bits` future.** Today it encodes "node has finished its
   setup phase." Orthogonal to sync. Keep as bit-field, or extend
   into a verb (`READY` from slave, `ALL_READY` broadcast)? Defer to
   6.4 since it interacts more with container readiness than sync.
5. **DRAIN propagation.** When master sends DRAIN to a slave, should
   slave also DRAIN its containers? Probably yes — slave forwards to
   each container_q, waits for all PAUSED acks, then exits 0.

---

## 11. What this design does not address

- **Multi-master / leader election.** Out of scope. Today's master is
  configuration-fixed (cpu_01). If multi-master becomes a goal, that's
  a separate phase (Raft or similar).
- **Clock skew between master and slave.** Epochs are based on local
  boot timestamps; if clocks drift severely, epoch comparisons could
  misbehave. NTP assumed. Not a Phase 6 concern.
- **Network partition tolerance.** If pg becomes unreachable, all
  RPC stops; cluster goes through full reset+rejoin once pg returns.
  Same as today.
- **Postgres as a single point of failure.** RPC queues live in pg.
  Acceptable for Phase 6; pg HA is a separate problem.

---

## 12. Estimate

- Phase 6.1: 1–2 sessions (~8–16h of focused work).
- Phase 6.2: 1 session (~4–8h).
- Phase 6.3: zero new code (acceptance test only).
- Phase 6.4: 2 sessions (~8–16h).

Total: ~5 sessions of code + soak. Substantial but bounded.
