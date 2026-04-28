# Phase 6.1 — code complete, ready for acceptance test

**Author:** assistant, 2026-04-28.
**Status:** code complete, NOT yet exercised against the live cluster.
**You run the acceptance tests; assistant analyzes pasted logs.**

---

## What changed

Phase 6.1 replaces the bit-mask + `cluster_go` sync handshake with
**message-passing over per-CPU pg-backed RPC queues**. The 2026-04-26
master-bounce hang (master alone could not converge because slave's
stale `cluster_sync_bit_1` was never re-asserted) is the failure class
this fixes; with no shared mutable bits, the bug is structurally
impossible to reproduce.

### Files added (new code)

| Path | Purpose | LOC |
|---|---|---|
| `building_blocks/knowledge_base/postgres/data_structures/kb_sync_queue.lua` | Runtime push/drain/peek/count/purge for sync verb queues. Single-statement INSERT and SELECT-DELETE-CTE. <3ms/op. | ~180 |
| `building_blocks/knowledge_base/postgres/construct_kb/construct_sync_queue.lua` | DDL + DSL `kb:add_sync_queue{queue_name="..."}`. UNLOGGED per-queue tables. | ~130 |
| `nanodatacenter_dcs/runtime/dcs_host/sync_rpc.lua` | All 7 verb handlers, master per-peer state machine, slave own-state machine, rpc_scheduler tick, slave heartbeat tick, KB writeback, budget telemetry. | ~440 |
| `nanodatacenter_dcs/construction/subsystems/sync_queues.lua` | Declares `master_q` + `cpu_<id>_q` queues; declares `peer_state_<cpu_id>` and `rpc_budget_summary` status fields. | ~80 |
| `nanodatacenter_dcs/construction/tests/test_sync_queue.lua` | 14-step smoke test for kb_sync_queue. Already passing 2026-04-28 (100 push+drain in 9.6ms; 0.09ms/op avg). | ~150 |

### Files modified

| Path | Change |
|---|---|
| `building_blocks/knowledge_base/postgres/construct_kb/construct_data_tables.lua` | Wire `Construct_Sync_Queue` into facade + add `add_sync_queue` delegate. |
| `nanodatacenter_dcs/construction/build_kb.lua` | Add `"sync_queues"` to SUBSYSTEMS list. |
| `nanodatacenter_dcs/construction/subsystems/cpu_bootstrap.lua` | Add `master_cpu` + `peers` (sorted) to bootstrap.config. |
| `nanodatacenter_dcs/construction/subsystems/readiness_sync.lua` | Drop `cluster_sync_bits` (replaced by RPC). Keep `ready_bits` (orthogonal). |
| `nanodatacenter_dcs/construction/subsystems/site_scalars.lua` | Drop `cluster_go` status field. |
| `nanodatacenter_dcs/runtime/dcs_host/user_functions.lua` | Require + instantiate `sync_rpc`; register handlers via `:install_handlers(R)`. Delete `SET/CLEAR_OWN_SYNC_BIT`, `VERIFY_SYNC_QUORUM_OR_TIMEOUT`, `WRITE/CLEAR_CLUSTER_GO`, `CLEAR_ALL_CLUSTER_SYNC_BITS`, `VERIFY_CLUSTER_GO`. Add `OPEN_PG_CONNECTION` helper. |
| `nanodatacenter_dcs/runtime/chain_tree/dcs_dsl.lua` | Rewrite `sync_master_sm` (`bring_up_infra` → `await_active` (parallel sched + verify) → `handoff`). Rewrite `sync_slave_sm` (`wait_infra` → `join` (parallel sched + heartbeat + verify) → `handoff`). Drop `CLEAR_OWN_SYNC_BIT` from node_control teardown. |
| `nanodatacenter_dcs/runtime/chain_tree/dcs.json` + `dcs_debug.yaml` | Auto-regenerated (261 nodes). |

### Architectural decisions locked this session

- **Transport: pg queues (Option B), NOT NATS.** See `project_phase6_transport.md` + `project_kb_sync_queue.md` memories. NATS revisited only at Phase 6.4 for container fan-out.
- **Handler budget: <50ms with INFO@30ms + SCADA exception @50ms.** Master serializes one slave per walker tick (round-robin cursor). Heartbeat ±10% jitter. See `feedback_phase6_handler_budget` memory.
- **Master grace 2s** before processing JOIN_REQs after start.
- **Bootstrap_hash in JOIN_REQ deferred** to step 4 (catalog migration).

---

## How to run the acceptance test

**This is destructive of the current Phase 4 cluster state.** All 4
infra containers stay up; the dcs.lua processes (cpu_01 + cpu_02) bounce
and re-handshake.

### Step 1 — full cluster rebuild

The pg KB schema changed (new `<db>_sync_queue_class` + `<db>_sync_msg__*`
tables; `cluster_sync_bits` + `cluster_go` rows removed). You need a
fresh build_kb run, which DROPS+RECREATES tables and **must run with
dcs.lua processes stopped**.

```bash
# 1. Stop both dcs.lua processes cleanly. Watchdog scripts respawn,
#    so you need to stop them at the watchdog level too. Pattern that
#    has worked in prior sessions:
pkill -TERM -f "start_dcs.sh\|dcs.lua" && sleep 5
# Confirm no dcs.lua processes left:
pgrep -af "dcs.lua"   # should print nothing

# 2. Rebuild KB schema (touches pg-vector live container).
cd ~/knowledge_base_assembly/luajit_programs_and_containers/building_blocks/nanodatacenter_dcs/construction
POSTGRES_PASSWORD=$(docker exec pg-vector printenv POSTGRES_PASSWORD) ./build_kb.sh

# 3. Slice per-CPU bootstrap.db (writes deployment/cpu_01/bootstrap.db etc.).
POSTGRES_PASSWORD=$(docker exec pg-vector printenv POSTGRES_PASSWORD) ./slice_bootstrap.sh

# 4. Restart dcs.lua processes per your usual deployment pattern
#    (start_dcs.sh in deployment/cpu_01 and cpu_02; or whatever the
#    multi-CPU dev setup uses).
```

If `build_kb.sh` errors out, paste the full output and I'll diagnose.

### Step 2 — Test 1: fresh boot reach ACTIVE within 15s

Watch the cpu_01 + cpu_02 error.log for the new sync_rpc messages:

```bash
# In one terminal:
tail -f ~/knowledge_base_assembly/luajit_programs_and_containers/building_blocks/nanodatacenter_dcs/deployment/cpu_01/error.log
# In another:
tail -f ~/knowledge_base_assembly/luajit_programs_and_containers/building_blocks/nanodatacenter_dcs/deployment/cpu_02/error.log
```

**Expected sequence on cpu_02 (slave):**
```
sync_slave: wait_infra (entering)
sync_slave: join (entering)
JOIN_REQ sent (cpu_id=cpu_02 epoch=<ts>)
peer cpu_02 -> ACTIVE (epoch=<ts>)         <-- on master log
slave cpu_02 -> ACTIVE (epoch=<ts>)         <-- on slave log
sync_slave: handoff (entering)
```

**Expected on cpu_01 (master):**
```
sync_master: bring_up_infra (entering)
MASTER_SYNC_INIT: epoch=<ts> grace=2s peers=1
sync_master: await_active (entering)
peer cpu_02 -> ACTIVE (epoch=<ts>)
sync_master: handoff (entering)
```

**Pass criteria:** both reach `node state: monitor (entering)` within ~15s
of process start. `sys_ready=true node_op=true` in the standard tick log.

### Step 3 — Test 2: SIGTERM master only (THE 2026-04-26 BUG)

```bash
# From a third terminal, kill master's dcs.lua only:
pkill -TERM -f "deployment/cpu_01.*dcs.lua"
```

**Expected:**
- Master's watchdog respawns it within 2s.
- Slave's HEARTBEAT_ACK timer notices missing ACKs.
- After 3 missed (~15s), slave logs `FAIL-STOP: 3 missed HEARTBEAT_ACKs`
  and exits 0; slave's watchdog respawns it.
- Both re-enter sync; new epochs; cluster reaches ACTIVE again.
- **Total recovery time: ≤30s, no manual slave bounce.** This is the
  exact failure mode that hung the cluster on 2026-04-26.

### Step 4 — Test 3: SIGTERM slave only

```bash
pkill -TERM -f "deployment/cpu_02.*dcs.lua"
```

**Expected:**
- Slave watchdog respawns it within 2s.
- Master's `peer_state_cpu_02` row stays at `ACTIVE` briefly, then
  master's missed HEARTBEAT_ACKs (note: master doesn't currently fail-
  stop on missed slave HBs in 6.1 — that's 6.3 territory; current
  behavior is master keeps own state until slave rejoins with a NEW epoch).
- Slave sends fresh JOIN_REQ with new epoch; master ACKs; slave CONFIRMs;
  ACTIVE within ~10s.
- Total recovery: ≤15s.

### Step 5 — Test 4: SIGTERM both simultaneously

```bash
pkill -TERM -f "dcs.lua"
```

**Expected:** both watchdogs respawn within 2s. JOIN handshake
converges within ~30s.

### Step 6 — Inspect peer_state + budget summary

```bash
docker exec pg-vector psql -U gedgar -d knowledge_base -c \
  "SELECT path, data FROM knowledge_base_status
    WHERE path LIKE '%.peer_state_%' OR path LIKE '%.rpc_budget_summary'
    ORDER BY path;"
```

Expect:
- `peer_state_cpu_01` and `peer_state_cpu_02` rows with `state=ACTIVE`,
  recent `last_heartbeat_at`.
- `rpc_budget_summary` with `max_ms < 50` (handler budget held),
  `violations = 0`, `warnings = 0` ideally.

If `violations > 0`: paste the dcs error.log lines containing
`WARN budget violation` so I can see which handler blew budget.

---

## Rollback procedure

If anything goes wrong:

```bash
# Stop dcs.lua again
pkill -TERM -f "dcs.lua"

# Revert all Phase 6.1 commits (HEAD~N..HEAD where N covers them).
# Adjust N once commits land.
git log --oneline -20  # find the Phase 6.1 commits
git revert <oldest-phase-6-1-sha>..<newest-phase-6-1-sha>

# Rebuild + restart
cd ~/.../nanodatacenter_dcs/construction
POSTGRES_PASSWORD=... ./build_kb.sh
POSTGRES_PASSWORD=... ./slice_bootstrap.sh
# restart dcs.lua processes
```

The `:phase4` docker images are still tagged so the broker / test_app
state is unchanged either way. Pg-vector container itself doesn't
change.

---

## What's NOT in 6.1 (deferred per design)

- **Per-node infra restart** (Phase 6.2): full system teardown still
  happens on infra loss. The new RPC verbs (PAUSE/RESUME) aren't wired
  into the system_control monitor state yet. Add in 6.2.
- **`bootstrap_hash` in JOIN_REQ** (Q1 / step 4): not implemented.
  Slave sends only `{cpu_id, epoch}`. Master accepts any epoch.
  Catalog mismatch detection comes when catalog moves to file store.
- **Container-layer RPC** (Phase 6.4): app containers still use Phase 4
  HTTP probes only. No PAUSE/CONTAINER_READY/HEARTBEAT verbs at
  container layer yet.
- **NATS for container fan-out** (locked decision): revisit in 6.4 only.

---

## Open issues to flag during testing

1. **Slave cleanup of stale queue entries on restart.** If a slave's
   prior epoch left stale messages in master_q (HEARTBEAT with old
   epoch), master's handler ignores them (epoch mismatch check). They
   accumulate until the next master restart drops them implicitly via
   the rebuild. Not a correctness issue, but worth noting.

2. **Master's epoch updates only on MASTER_SYNC_INIT.** A master that
   never bounces uses one epoch forever. That's correct (master's
   epoch identifies the master's lifetime; slaves don't compare it
   except to display in logs). If multi-master ever becomes a thing,
   this will need revisit.

3. **No DRAIN handler on slave side yet.** Slaves can DRAIN themselves
   via the master sending RESET_HINT. Cooperative tear-down is 6.4.

---

## Memory pointers

- `project_kb_sync_queue.md` — new module rationale + permanence.
- `project_phase6_transport.md` — pg vs NATS decision.
- `feedback_phase6_handler_budget.md` — <50ms cap + round-robin scheduler.
- `feedback_one_reset_path.md` — discipline this implements.
- `feedback_no_band_aid_over_architecture.md` — why no backwards-compat shims.
- `nanodatacenter_dcs/PHASE6_DESIGN.md` — full design spec; §0 is the
  canonical strategic-locks summary.
