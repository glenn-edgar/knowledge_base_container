# observability + DCS — continue plan

**Last updated 2026-04-26 (evening — full session).** Phase 4 closed
out (all 5 live tests passed); Phase 6 designed (RPC sync rewrite);
strategic v3-platform roadmap locked across a long chat session;
file-store discovery captured.

## Where this file lives

`~/knowledge_base_assembly/luajit_programs_and_containers/building_blocks/observability/continue.md`

A second authoritative document is the Phase 6 design at:
`~/knowledge_base_assembly/luajit_programs_and_containers/building_blocks/nanodatacenter_dcs/PHASE6_DESIGN.md`

Read **PHASE6_DESIGN.md §0 first** — it is the canonical summary of
all strategic locks set in this session.

---

## Phase 4 — CLOSED. All 5 live tests passed 2026-04-26

Broker-active HTTP probes are live in production. End-to-end pipeline
validated.

### Test results

| # | Test | Result |
|---|---|---|
| 1 | Health observation | PASS — `test_app_01` is the only container with `probe.configured=true`; all other 14 containers report `probe: null`. Inert-by-default contract holds. |
| 2 | 5-min soak baseline | PASS — `RestartCount=0`, `WATCHDOG mentions=0`, `fail_streak=0`, `last_status=200` throughout, `last_ok_ts` advancing on 5s cadence. (User accepted 5min vs 10min for wall-time savings.) |
| 3 | Stuck-process | PASS — SIGSTOP `exception_web` worker → WATCHDOG fired at T+85s with correct format `broker probe stuck (streak=12, last=context deadline exceeded) fail=3/3`, container respawned in ~95s. Edge: SIGSTOPed worker delays docker-rm grace period — exactly the cooperative-tear-down gap that Phase 6.4 `DRAIN` verb addresses. |
| 4 | No-route | PASS — synthetic container on isolated network → broker emits `route=no_route`, `fail_streak=0` (broker correctly skips counting). Code review at `user_functions.lua:1301-1302` confirms WATCHDOG branch is gated on `route ~= "no_route"`. Design Q4 verified. |
| 5 | Broker-outage gate | PASS — 8s `docker pause` of broker: Phase 4 `is_fresh()` gate held WATCHDOG (counter paused at 1/3, resumed at 2/3 only after broker recovered); Phase 3b "staying quiet" gate held VERIFY_SYSTEM_CONTAINERS_HEALTHY (logged "broker stale" at 1Hz, no ERR_MONITOR_TRIP). Cluster did NOT cascade despite >5s outage. **Memory `feedback_broker_outage_threshold` was OUTDATED** — Phase 3b absorbs outages silently. Memory updated. |

### Phase 4 commits in this branch

```
09f093c8  phase 4 guinea pig: probe block on test_app.exceptions_ui
8c3e08f9  continue.md: 2026-04-25 evening session handoff (phase 4 code complete)
50269ea4  dcs_host: phase 4 -- consume broker probe state (dcs.lua side)
bc30cc9c  docker_host_broker: phase 4 -- broker-active HTTP probes (broker side)
fa52f8b9  docker_host_broker: phase 4 design -- wire protocol + catalog schema
```

### Image tags as of session end

```
nanodatacenter/docker-host-broker:latest              42de2749f094   (= phase4)
nanodatacenter/docker-host-broker:phase4              42de2749f094
nanodatacenter/docker-host-broker:phase4-prior        d50f4d390bc3
nanodatacenter/docker-host-broker:phase2              03a38a7be462   (real Phase 2 rollback)
nanodatacenter/docker-host-broker:phase1c-rollback    19b98ef5c035

nanodatacenter/test-app:latest                        1657d951ab57   (= phase4, has /health endpoint)
nanodatacenter/test-app:phase4                        1657d951ab57
```

`:phase4` tags pinned this session for rollback / known-good reference.

### Cluster state at session end

* All 4 infra containers Up healthy.
* `docker-host-broker` Up (Phase 4 binary, broker_version 0.2.0-phase2).
* `test_app_01` Up — image has /health, container has probe labels,
  broker is probing and reporting `route=bridge`, `ok=true`. Container
  ID at session end: `841a39dd0810` (post-Test-5 respawn).
* `dcs.lua` Up on cpu_01 (master) and cpu_02 (slave). Both bounced
  this session to load the new bootstrap.db with the probe block.
* 5 app containers running: observability_01, dcs_console_01,
  robot_manager_01, ros_mission_planner_ii_01, test_app_01.
* `sys_ready=true`, `node_op=true` cluster-wide.

---

## Strategic v3 platform roadmap (locked 2026-04-26 evening)

**The platform is the product.** Apps (test_app, robot_manager,
ros_mission_planner_ii, observability, dcs_console) are validation
cases. The North Star is **"an edge-distributed container architecture
driven by knowledge-base namespace, where app designers focus only on
their procedure and the platform handles bundling + maintenance."**

This is **v3** of the same conceptual project. v1 = 2017 sprinkler.
v2 = redis-go. v3 = this (pg + KB namespace).

### Hardware + scale (locked)

* Pi 4+ class CPUs, Debian Linux, 8GB+ RAM, **USB3 SSD or better.**
  No SD-card class. WSL2 is dev-only.
* 8 nodes per cluster v1, up to 16 v2. Beyond = federation, not
  sharding.
* Strict all-N quorum. Fixed master (cpu_01). MQTT-registered
  devices are app-level, not platform.
* Master loss = site outage. Operator re-provisions manually.

### Maintenance discipline (locked)

* **One startup path.** Planned shutdown = whole-cluster shutdown.
  No degraded operation. Bell/AT&T 1990 lesson: heterogeneous
  recovery paths cascade.

### Three-tier configuration (step 4 model)

| Tier | Read when | Source |
|---|---|---|
| Site config | First, before pg | JSON file (`site.json` per cpu) |
| Secrets | After site config | `secrets.env` file |
| Tunables + catalog | After pg up | KB rows |
| Protocol-correctness | Compile time | Code |

The JSON-file pattern is v2's `Get_site_data` ported to Lua.

### Seven-step path to v1 done

1. **Solidify system/node-control RPC** (Phase 6.1+6.2+6.3+planned-
   DRAIN). 8-verb protocol, master ↔ slave RPC queues. ~1-2 sessions.
2. **Container base + RPC methods** (Phase 6.4). Container ↔ dcs RPC.
   ~2 sessions.
3. **Condense for build.** Directory layout to one place that builds
   cleanly. Manual deploy via scp/sshfs from master. Automation
   deferred. ~1 session.
4. **KB-driven everything.** bootstrap.db → live-from-pg, tunables →
   KB, site config → JSON file, secrets → file. Catalog migrates
   into the file store. Cloud-integration prerequisite. ~2 sessions.
5. **App-container build documentation.** DSL contract self-
   documenting. ~1 session.
6. **Log-analysis web UI by KB namespace tree.** Tree-shaped
   LOG+RULE+EXCEPTION refactor (storage + UI). Cloud-integration
   driver: log/exception analysis runs cloud-side. ~3 sessions.
7. **v1 done = maintenance mode.** Code complete + few-day local soak
   + standalone soak-node deployed via manual deploy + 30-day
   adversarial soak running with at least one validation app
   (recommend `observability_01`). Soak result not gating; feeds
   maintenance updates.

Estimated: ~10-12 sessions to reach v1 done.

### What v1 does NOT include

* Multi-master / leader election. Postgres single-master.
* Per-app KB RBAC. Trust boundary is pg auth.
* External vaults / HSMs for secrets.
* Federation / cloud-bridge. Triggers when site outgrows; that's v2.
* Hot-reload of config beyond what KB-rows-at-startup gives.
* App designer self-service tooling (UI, CLI, error-message polish).
  Documentation only in v1.

---

## Phase 6 design — see `PHASE6_DESIGN.md`

Phase 6 = sync layer rewrite via RPC queues. Replaces today's
polled-bit-mask sync (which 2026-04-26 dcs.lua bounce empirically
proved is fragile — master alone could not converge after restart
because slave's stale `cluster_sync_bit_1` was never re-asserted).

### Sub-phase summary

* **6.1** Inter-CPU sync via RPC queues + 3-way handshake (with
  `cluster_join_ack`-equivalent verb). 8 verbs. Bootstrap.db
  hydration moves into JOIN handshake.
* **6.2** Per-node infra restart instead of full system teardown.
  Uses RPC `RESET_HINT` + container `PAUSE`/`RESUME` for cooperative
  pause-during-infra-restart.
* **6.3** Bidirectional master-loss detection — *automatic fallout*
  of 6.1's HEARTBEAT/HEARTBEAT_ACK round-trip.
* **6.4** Container ↔ dcs RPC queues. Closes the chain-tree-controller-
  starvation gap (`feedback_chain_tree_no_blocking_io`). Subsumes
  the previously-queued luajit_base controller hardening.

### Open questions deferred to implementation session

Per `PHASE6_DESIGN.md` §10: bootstrap_hash in JOIN_REQ; master grace
period after restart; queue read concurrency; ready_bits future;
DRAIN propagation specifics.

---

## File store discovery (2026-04-26 evening)

Late in the session: discovered the **file store** that landed in
commit `6a63eec3` — content-addressable file/blob storage in pg.
**Important architectural primitive that wasn't in my mental model.**

### Status

* ✅ Schema in pg: `<db>_doc_class`, `<db>_fs_blob` (sha256 PK),
  `<db>_fs_node` (path→blob).
* ✅ DSL wired (`kb:add_doc_class{...}` via
  `construct_data_tables.lua:30,69,157,202`).
* ✅ Runtime + commissioning APIs implemented.
* ✗ `doc_class`, `fs_blob`, `fs_node` tables all 0 rows in live pg.
  **Schema-only, no live usage.**
* ✗ No automated test coverage.

### Why it matters for the strategic plan

* **Step 4 (KB-driven catalog)** uses the file store, not bespoke
  pg rows. Catalog migrates into a `kb:add_doc_class{...}` namespace.
* **Step 5 (app-container docs)** has a second DSL surface beyond
  `definitions.lua`: app designers can ship static assets via
  `add_doc_class` without rebuilding container images.
* **Cloud integration** gets a federation-by-construction layer:
  sha256-keyed blobs deduplicate across sites trivially. No conflict
  resolution.

### Smoke test queued (gates catalog migration)

Before adopting file store for catalog, write smoke test (~30 lines
of Lua + tiny fixture):

1. `add_doc_class{namespace="test.smoke", source_dir="/tmp/smoke"}`
2. `load_dir(...)` from a 2-3 file fixture (text, binary, nested dir)
3. `doc_get(...)` content matches input
4. Re-load same dir — verify `fs_blob` count doesn't grow (sha256 dedup)
5. Modify one file, re-load — `fs_node` updates, old blob orphaned
6. `fs_blob_sweep(...)` orphan reclamation
7. `extract_dir(...)` byte-for-byte roundtrip
8. `doc_purge_entity(...)` deletion

### Two open puzzles

* `knowledge_base_documents` (plural) vs `knowledge_base_document`
  (singular) — both tables exist; one is the JSONB doc table, other
  is unknown. Confirm intent during smoke session.
* Stream store (`kb_stream_store.lua`) — same construct-but-unused?
  Worth checking when looking at file store next session.

See `project_file_store.md` memory for full detail and locations.

---

## Memory updates this session

| File | Change |
|---|---|
| `feedback_user_driven_testing.md` | UPDATED — testing mode is per-session; user can flip to assistant-driven with stop-after-each-major-step. |
| `feedback_one_reset_path.md` | NEW — Bell/AT&T 1990 discipline; one reset path; 3-way handshake. Empirically validated. |
| `feedback_broker_outage_threshold.md` | UPDATED — Phase 3b absorbed outages silently in monitor state; the 5s threshold is now log-noise, not cascade. Validated 2026-04-26. |
| `project_v3_platform_roadmap.md` | NEW — full strategic locks: hardware, scale, quorum, master, maintenance, 7-step roadmap. |
| `feedback_three_tier_config.md` | NEW — JSON file (boot) / KB (live) / code (protocol) split. v2 Get_site_data pattern. |
| `project_file_store.md` | NEW — content-addressable file store status, gaps, smoke test queued. |

---

## Bring-up checklist for next session

1. **Read in this order:**
   - This file (`observability/continue.md`).
   - `nanodatacenter_dcs/PHASE6_DESIGN.md` § 0 (strategic locks).
   - `project_v3_platform_roadmap.md` and `feedback_three_tier_config.md`
     in memory.
   - If touching file store: `project_file_store.md`.
   - If touching Phase 6: `feedback_one_reset_path.md`.

2. **Verify cluster state:**
   ```bash
   docker ps --format "table {{.Names}}\t{{.Status}}"
   curl -sS http://127.0.0.1:9100/v1/health
   tail -10 ~/knowledge_base_assembly/luajit_programs_and_containers/building_blocks/nanodatacenter_dcs/deployment/cpu_01/error.log
   ```
   Expect: 9 cluster containers Up + buildkit + broker; broker reports
   healthy; cpu_01 dcs.lua showing `sys_ready=true node_op=true`.

3. **Confirm Phase 4 still working:**
   ```bash
   curl -sS http://127.0.0.1:9100/v1/state/containers \
     | python3 -c "import json,sys; d=json.load(sys.stdin); cs=d.get('containers',d); cs=cs.get('containers',cs) if isinstance(cs,dict) else cs; cs=list(cs.values()) if isinstance(cs,dict) else cs;
   [print(f\"{c['name']}: probe={c.get('probe')}\") for c in cs if 'test_app' in c.get('name','')]"
   ```
   Expect: `test_app_01` with `probe.configured=true, ok=true`.

---

## Recommended next session

**File store smoke test, before any other work.** Reasons:

* It's the gate before catalog migration (step 4).
* It's small, focused, and validates a real architectural primitive.
* It exercises the existing DSL and runtime APIs end-to-end.
* It surfaces the two open puzzles (`knowledge_base_documents` vs
  `knowledge_base_document`; stream store status).
* It's well-bounded — one session, clear acceptance criteria.

After file-store smoke test passes, the next major chunk is **Phase
6.1 (inter-CPU sync RPC implementation).** That's a 1-2 session
build:

1. Schema: `peer_state.<cpu_id>` rows (master-side observability).
2. RPC verb implementations on master and slave.
3. State machine rewrites (`sync_master`, `sync_slave` columns of
   chain-tree).
4. Acceptance test: reproduce the 2026-04-26 master-bounce-hang on
   the new code → cluster should reconverge in <30s without manual
   slave bounce.

---

## Risks / things to watch out for

* **Memories age.** This continue.md was written tonight; by next
  session some claims about file:line citations or table names may be
  stale. Cross-check before acting on memory claims.
* **`feedback_broker_outage_threshold` taught the lesson:** check
  whether old memories still match current code before relying on
  them. Phase 3b silently fixed the cascade. The same pattern may be
  hiding in other "deferred" memory items.
* **The strategic discussion was long.** If something in this doc
  contradicts something else in this doc, the locks in
  `PHASE6_DESIGN.md §0` and `project_v3_platform_roadmap.md` win;
  the body sections were edited surgically, not rewritten.
* **Phase 4 left the cluster in a good state** but `:phase4` images
  are not yet pushed to a registry — they exist only on this dev
  laptop. If the laptop dies, rebuild from source.

---

## End of 2026-04-26 session
