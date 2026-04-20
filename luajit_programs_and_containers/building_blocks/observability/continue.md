# observability container — continue plan

Session 2026-04-19 ended mid-Task 4. Phases 1-7 committed + pushed.
Phase 8 (UIs) + Phase 9 (e2e verify) still open.

## Where we are

Current commit: `90660680` (task 4 phase 7). Master branch, pushed.

Observability container is alive on master CPU:
- `docker ps` shows `observability_01 Up`
- All 4 processes running (controller reports `apps=4/4`)
- `log_analyzer: tick=N logs=65` — connected, discovering all KB_LOGs
- `exception_analyzer: tick=N tracked=149` — connected, snapshotting hit_counts
- `exception_web` + `log_web` still Phase-5 shell pages on :19007 and :19008

No alarms firing (no writer is pushing samples into KB_LOGs yet).
That's expected and orthogonal to the analyzer pipeline itself.

## What's done (committed, pushed)

| phase | commit   | artifact |
|-------|----------|----------|
| 1     | 1db8a441 | SCADA nested SYS_EXCEPTION (15 status children + signatures jsonb). SCADA API: raise / ack / clear / shelve / suppress / unshelve. Legacy compat wrappers preserved. |
| 2     | 6fa56210 | KB_LOG / KB_RULE construction DSL. Rollups table. auto_health pattern. 7 rule kinds accepted. |
| 3     | 6fa56210 | 14 v1 log signals declared across site/cpu/container scopes. ~128 seed rules. |
| 4     | 046ecd85 | runtime/dcs_host/kb_log.lua + kb_rule.lua (DBI-based). |
| 5     | ce5976b9 | observability container skeleton: 4 processes (shells), 2 ports 19007/19008. Build speedups (43s→22.5s for build_kb, 20s→0.4s for slice). |
| 6     | 29a83e7f | log_analyzer real logic: discovery, live_stats (welford+MA+envelope+slope+cusum), 7-kind rule evaluation, tier-1/2/3 rollup compaction, retention trim. |
| 7     | 90660680 | exception_analyzer real logic: shelve-expiry sweep, rolling 5-min flap-rate window, flap-flood auto-shelve. |

## What's left — Phase 8 + Phase 9

### Phase 8: web UIs (the biggest remaining chunk)

Split into 4 sub-phases, each shippable:

**Phase 8a — exception_web views 1-2.**
Site Overview homepage + Active Alarms table. Proves the
openresty + pgmoon + htmx pattern works end-to-end. Reads
SYS_EXCEPTION state children from knowledge_base_status.
Navigate to `/ui/observability/exception_web/` via the
dcs_console gateway.

**Phase 8b — exception_web views 3-5.**
Alarm Detail (with signature-dedup jsonb rendering + ack/shelve
form handlers), Alarm Journal (append-only transitions view),
Shelved Alarms tab with manual-unshelve control.

**Phase 8c — uPlot vendoring + log_web view 7 (Log Detail).**
Drop uPlot.iife.min.js + uPlot.min.css into openresty_base's
prebuilt_static/ (or into observability/container/log_web/).
Write the single-log strip-chart view: raw samples + ma_short +
ma_long + envelope band + rule-trip markers. Backend endpoint
`/api/log/<path>/series?window_s=N` picks the tier.

**Phase 8d — log_web views 6, 8, 9.**
Live Operational (grid of mini strip charts across all operational
logs), Archival Browser (long-range multi-log overlay),
Rule Inventory (flat KB_RULE table with inline enable/suppress
toggles).

### Phase 9: e2e verification + topology

Full `build_kb → slice → stage → start` cycle with everything wired.
Pick a handful of rules to trip intentionally (maybe write a small
sample-generator script that pushes artificial values) and confirm
the SCADA path end-to-end: sample pushed → rule fires → SYS_EXCEPTION
flips to UNACK_ACTIVE → exception_web shows the alarm → operator acks →
state transitions to ACK_ACTIVE → operator shelves → shelve expires
via exception_analyzer janitor → state back to NORMAL.

## Deferred (orthogonal to Task 4 completion)

**Writer wiring.** No writer currently pushes samples into KB_LOGs.
The analyzer just sits idle waiting. Wiring is separate work:
  * host_sampler (runtime/dcs_host/) — currently writes to
    monitor.samples; could be adapted to also push to per-metric
    KB_LOG rings.
  * Or a sidecar translator in log_analyzer that reads
    monitor.samples and decomposes into per-signal KB_LOGs.
  * DCS tick loop in dcs.lua — can push tick_duration_ms +
    ticks_per_burst directly.
  * pg_connector — can sample roundtrip ms and push.
Recommend: after Phase 8 proves the UI/analyzer stack, do writer
wiring as a final integration step (Phase 10 effectively).

**Meta-exception on flood.** exception_analyzer's auto-shelve
logs to stderr but doesn't raise `alarm_flood_detected`. Need to
declare that exception first (in site_scalars or similar), then
raise from check_flap_floods.

**RTN_UNACK aging.** Purely cosmetic; UI can compute "unacked for
X hours" from last_rtn_ts without needing aging state.

**TimescaleDB migration.** Planned path documented in
project_dcs_task4_design.md. Trigger when rollup table scale
degrades query performance. Not needed for v1.

## How to resume

1. **Confirm state survived overnight.** Both CPU watchdogs should
   still be running; check `docker ps` for observability_01 +
   4 app containers. If CPUs died (laptop sleep etc.), restart
   with `./start.sh` in each deployment/cpu_XX/ dir.

2. **Verify no regression.** `docker logs observability_01 --tail 10`
   should still show heartbeats from both analyzers. If not, rebuild
   + redeploy (full cycle in §How to rebuild below).

3. **Read project_dcs_task4_design.md** for the locked design. Don't
   re-derive; don't rethink. If a proposed change hits a load-bearing
   piece (SCADA state enum, kind vocabulary, label names), push back
   against the change before accepting.

4. **Start Phase 8a.** Model the openresty app layout on the existing
   dcs_console/admin/ (which we've already built and verified). Use
   pgmoon + `env` directives for env vars. Keep the browser-refresh
   pattern — no live polling.

## How to rebuild (fast path)

```bash
# Image rebuild (only if observability/container/ changed)
cd ~/knowledge_base_assembly/luajit_programs_and_containers/building_blocks/observability/container
./docker_build.sh                    # ~5s on warm cache

# KB + deploy (only if definitions.lua / topology.lua / subsystems/ changed)
cd ../../nanodatacenter_dcs/construction
./build_kb.sh > /tmp/build_kb.log 2>&1     # ~22s (was 43s before phase 5 speedup)
./slice_bootstrap.sh > /tmp/slice.log 2>&1 # ~0.4s (was 20s before phase 5 speedup)
./stage_deploy.sh > /dev/null

# Force container restart with new spec
docker rm -f observability_01

# Restart DCS so node_control re-reads the fresh container_definition spec.
# BOTH cpu_01 and cpu_02 watchdogs needed (sys_ready requires the pair):
# Terminal 1:  cd ../deployment/cpu_01 && ./start.sh
# Terminal 2:  cd ../deployment/cpu_02 && ./start.sh
```

**Known WSL2/bash quirk**: if a terminal was inside deployment/cpu_XX/
when slice_bootstrap ran, its cwd is now a stale inode. `cd ~ && cd
~/.../cpu_XX` to re-enter.

## Key design pointers (for AI context next session)

Read these memories at session start:
  * `project_dcs_task4_design.md` — the locked design. SCADA SYS_EXCEPTION
    shape, KB_LOG shape with 3 kinds, rule vocabulary, display plan.
  * `project_dcs_infra_roadmap.md` — 5-task infra gate; Task 4 is the
    last hard gate before app work.
  * `feedback_coordinator_startup_wipe.md` — principle the observability
    container follows (idempotent; don't clear state at startup).
  * `feedback_nginx_env_directive.md` — openresty needs `env NAME;` at
    main context for os.getenv() to see env vars in workers. Matters
    for Phase 8 when the web servers need pgmoon.

## Open files relevant to Phase 8

**Pattern sources (read these first):**
  * `dcs_console/container/admin/` — openresty + pgmoon + htmx pattern.
    Most of Phase 8 is mechanical translation of this pattern for the
    SYS_EXCEPTION / KB_LOG / KB_RULE data shapes.
  * `dcs_console/container/gateway/conf/nginx.conf` — env directives,
    env-from-/etc/hosts trick for host.docker.internal.

**Targets:**
  * `observability/container/exception_web/conf/nginx.conf` — replace
    the stub with real server block(s).
  * `observability/container/exception_web/lua/` — new dir; views
    (site_overview.lua, active_alarms.lua, alarm_detail.lua, ...).
  * `observability/container/log_web/conf/nginx.conf` — same.
  * `observability/container/log_web/lua/` — new dir; chart views +
    uPlot vendored asset.
  * `observability/container/log_web/static/uPlot.iife.min.js` +
    `uPlot.min.css` — vendored from https://github.com/leeoniya/uPlot
    releases (single file each, ~40 KB).
