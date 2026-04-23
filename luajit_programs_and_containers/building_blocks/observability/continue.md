# observability container — continue plan

Last updated end of **2026-04-23**. Overnight run surfaced real
issues; see "Current problems" below. Longer roadmap still captured
at the bottom; tree-control + scoped-namespace work promoted out of
"last UI item" on 2026-04-23.

## Current problems (prioritized)

### Breaking now

1. **cpu_02 `dcs.lua` is silently dead.** PID 98925 still running but
   pg connection severed ~11 min ago. Every heartbeat write fails;
   no samples flow; `system_ready` goes false because cpu_02's
   heartbeat bit isn't advancing. Fix: `kill -KILL 98925`; start.sh
   respawns it with current code.

2. **cpu_02 zombie is running pre-fix code.** Even if its connection
   recovered it'd still write live_stats to the wrong table. The
   respawn in (1) fixes both.

### Time bombs (will recur)

3. **`dcs.lua` has no pg-reconnect logic.** Same dead-connection bug
   the analyzers had. I fixed log_analyzer + exception_analyzer
   (commit 87999ceb) but not dcs.lua. pg_connector gives dcs.lua one
   connection at startup with no self-heal. Every build_kb, pg
   idle-timeout, WSL2 network blip kills it. Same `conn_alive` +
   reconnect pattern from log_analyzer/main.lua needs to land in
   pg_connector.lua or dcs.lua's tick loop.

4. **`build_kb` against a live cluster is destructive.** DROP CASCADE
   on `knowledge_base` + satellites severs every active pg connection
   and wipes `CONTAINER_REGISTRY` rows. We've hit this 3 times now
   (overnight, cpu_02 today, yesterday). Two fixes possible:
     - Pre-flight check: refuse to drop while any connection holds
       locks on knowledge_base tables, unless `--force`.
     - Make analyzers / dcs.lua universally reconnect-capable (partly
       done -- analyzers only).

5. **Welford warmup is unfiltered.** First samples (often 0 or
   placeholder values) skew running mean/variance forever. Observed:
   tick_duration_ms mean=197ms with stddev=673ms -- 3x mean. Already
   caused false `tick_overrun` fire on first 500ms+ burst because the
   mean was pulled low by warmup. Fix: skip first N samples or gate
   rule evaluation on `welford.n >= 10`.

### Incomplete / deferred

6. **50 of 69 KB_LOGs have no writer.** Container-resource metrics
   (`container_cpu_pct` etc.) declared × 9 containers. host_sampler
   produces the JSON blob in `monitor.samples` but nothing decomposes
   into per-metric rings. WSL2 cgroup visibility gap compounds --
   some would be empty even with a decomposer on this host.

7. **Build-system modularization for outside review.** Single
   canonical entrypoint, clear separation between "rebuild image" /
   "rebuild KB" / "redeploy", top-to-bottom doc for first-time
   reviewer. User priority between reconnect fix and tree-control.

8. **Tree-control + base-context scoping across all inventory views
   (promoted 2026-04-23).** All 7 inventory views currently render
   flat lists (`/alarms` 161 alarms, `/rules` 138 rules, `/live` 20+
   charts, etc.). Needs a shared sidebar tree + `?base=` query param
   so every list query gets `WHERE path <@ <base>::ltree`. Same
   primitive generalizes to cloud rollup (one more level in the
   tree) and future permission scoping. See roadmap section for
   layered roll-out.

### UI gaps (medium-term)

9. **Analyzer / host-process health is not surfaced in the UI.**
   Dead pg connections manifest as lines in a log file, not alarms.
   The thing that would raise the alarm is the thing that's dead.
   Needs a liveness ping from analyzers to KB; separate heartbeat
   SYS_EXCEPTION that stale-sample-gap can trip.

10. **`system_ready=false` has no diagnostic trail in admin UI.**
    Requires grepping error.log to find which CPU's heartbeat is
    stale. Admin UI should surface per-CPU heartbeat age.

## Session log

Session 2026-04-20 ended very productive. User happy with the state of
the system. Locked-in plan for the next few sessions below; longer
roadmap captured at the bottom.

## Where we are (end of 2026-04-20)

Master branch, commits pushed through **`54adee87`** (WIP perf
writers). Recent commits worth knowing:

| commit    | what |
|-----------|------|
| d16b945a  | task 4 phase 9 e2e — Task 4 COMPLETE |
| 36ec294b  | reconcile + watchdog columns in node_control |
| 530a14d2  | observability + gateway redirect fix |
| f7722f4d  | prefix-aware URLs via X-Forwarded-Prefix |
| 54adee87  | WIP perf writers in dcs.lua tick loop (display blocked) |

State today:
- All UI views render through gateway iframe correctly with
  prefix-aware URLs.
- exception_web layout reviewed + approved by user.
- log_web reviewed; user wants charts to actually plot data.
- WIP writer (commit 54adee87) lands valid samples into per-CPU
  KB_LOG rings every burst (verified 32 valid samples per metric on
  both cpu_01 and cpu_02 within 30s of restart). **Chart still
  shows 0 points** because of the monotonic-vs-wall-clock timestamp
  bug (see "Tomorrow" item 1 below).
- node_control reconcile + HTTP watchdog live and tested
  (commit 36ec294b). `docker rm -f` or `docker pause` of any
  assigned container triggers auto-recovery + SYS_EXCEPTION raise.

## Tomorrow's plan (2026-04-21)

**Goal:** finish wiring node_control performance logs end-to-end so
the UI shows real charts, then run the system overnight to populate
the tier-1 (1min) rollup table and exercise the longer-window views.

Order of operations:

1. **Fix the wall-clock timestamp bug in kb_log.** `kb_log.lua:66`
   `now_s()` wraps `ptime.now_sec()` which is CLOCK_MONOTONIC. Change
   to `os.time()` for sample timestamps (the chart filter needs
   wall-clock-comparable values). Same fix probably also needed in
   `kb_exception.lua:69` for `last_raised_ts` so the alarm journal
   sorts by real time. Quick sanity check: existing samples already
   in the ring will become stale and get evicted naturally as the
   ring fills with new wall-clock-stamped samples.

2. **Verify writers + display.** Restart cpu_01 + cpu_02 watchdogs.
   Within ~30s, the per-CPU KB_LOG.tick_duration_ms / ticks_per_burst
   / pg_roundtrip_ms rings should hold real samples with epoch
   timestamps. Confirm `log_web/detail?path=...&window_s=300` renders
   a populated uPlot chart for at least one of the metrics on each
   CPU. (The wiring already creates separate logs per CPU
   automatically — `cpu_log_root = "system.site.<S>.cpu.<id>.KB_LOG"`
   in dcs.lua line 226-230 of commit 54adee87.)

3. **Add any missing perf signals.** Today only `tick_duration_ms`,
   `ticks_per_burst`, `pg_roundtrip_ms` are wired. Candidates for
   adding (declaration in `construction/subsystems/cpu_*.lua` may
   already cover some):
     - `containers_running` (count from docker.ls vs assignments)
     - `reconcile_check_ms` (duration of one RECONCILE pass)
     - `watchdog_probe_ms` (per-container probe latency, or aggregate)
   Decide based on what falls out naturally during the writer fix.

4. **Run overnight.** Both DCS watchdogs detached via `nohup ... & disown`.
   Tier-1 (1min) rollup compaction in log_analyzer should populate
   knowledge_base_rollups by morning, enabling the longer-window
   chart views (window_s 1800 / 3600 → tier-1; 7d → tier-2).

## Roadmap after tomorrow

User's stated principle: **fix infra/namespace problems before app
development; otherwise back-fitting gets exponentially worse.** This
shapes the order of the remaining infra items.

3. **Modularize the build system + make presentable for outside
   review.** The build flow today (build_kb → slice_bootstrap →
   stage_deploy → docker_build per container → start.sh per CPU) is
   functional but ad-hoc. Wants a single canonical entrypoint,
   clearer separation between "rebuild image" / "rebuild KB" /
   "redeploy", documented top-to-bottom story for someone seeing
   the project for the first time. Target audience: external
   reviewer (collaborator, employer, open-source contributor).

4. **Tree-control + base-context scoping across ALL inventory views
   (promoted 2026-04-23).** Originally framed as "scope the log_detail
   namespace display"; user elevated the scope on 2026-04-23 after
   seeing flat lists hurting every inventory view. Today 7 views
   (exception: /alarms /journal /shelved; log: /live /rules /archival
   /detail picker) each enumerate their full inventory flat. With
   ~161 tracked SYS_EXCEPTIONs + 69 KB_LOGs + 138 rules, operators
   can't narrow to "just cpu_01" or "just container X" without
   scrolling. Tree-control fix applies one unified pattern to all
   seven:
     - Sidebar tree renders once per page from
       `SELECT DISTINCT path FROM knowledge_base WHERE label IN
        ('SYS_EXCEPTION','KB_LOG','KB_RULE')`, grouped by ltree
       prefix; plain `<details><summary>` (no JS widget).
     - `?base=<ltree>` query param picks base context; default is
       site root.
     - Every inventory query gets an implicit `WHERE path <@
       <base>::ltree` clause using the existing GiST index.
     - Same base propagates through detail drill-ins via mk_url.
   Cloud rollup becomes natural: each site posts its tree to the
   aggregator at `cloud.fleet.<site_code>.<local-path>`; same UI code
   handles deeper tree with zero special-case.
   Permissions hook: subtree scoping maps to future auth roles for
   free (operator X sees only cpu_01; fleet manager sees everything).
   Layered roll-out:
     - v1 (~2h): URL ?base=, relpath() helper, flat sidebar list of
       common bases.
     - v2: real expandable tree using `<details><summary>`.
     - v3: cloud edge mounts each site at `cloud.fleet.<site_code>`.
   Backend: every list query gets `WHERE path <@ <base>::ltree`.
   Optional: fnv1a32 hash of full path as a permanent permalink for
   tickets/voicemail/SMS where the structured path can't be passed.

5. **Application development.** Only AFTER (3) and (4) are done.
   Apps are robot_manager, ros_mission_planner_ii (currently stubs)
   plus future entrants.

## Smaller deferred items (not blocking the roadmap)

**Add Shelve action to log_web/rules.** Today the rule-inventory page
has Enable/Disable + Suppress (which is conceptually shelve for a
rule). User wants explicit Shelve N min on individual rules, matching
the SCADA pattern in exception_web/alarm_detail. Reuses
`kb_rule.shelve()` already in runtime/dcs_host/kb_rule.lua. Pattern:
exception_web/alarm_detail.lua:171-177 (form() helper for
Shelve 5m / 1h / 1d buttons).

**Meta-exception `alarm_flood_detected`.** exception_analyzer's
auto-shelve logs to stderr but doesn't raise the alarm. Declare in
`catalogs/topology.lua` first, then raise from `check_flap_floods`.

**RTN_UNACK aging.** Cosmetic only.

**TimescaleDB migration.** Documented in project_dcs_task4_design.md.
Trigger when rollup query degradation appears.

## How to rebuild (fast path)

```bash
# Image rebuild (only if observability/container/ changed)
cd ~/knowledge_base_assembly/luajit_programs_and_containers/building_blocks/observability/container
./docker_build.sh

# Gateway rebuild (only if dcs_console/container/ changed)
cd ../../dcs_console/container
./docker_build.sh

# KB + deploy (only if construction/ changed)
cd ../../nanodatacenter_dcs/construction
./build_kb.sh > /tmp/build_kb.log 2>&1
./slice_bootstrap.sh > /tmp/slice.log 2>&1
./stage_deploy.sh > /dev/null

# Chain-tree rebuild (if dcs_dsl.lua / user_functions.lua / dcs.lua)
cd ../runtime/chain_tree && ./build_dsl.sh

# Force container respawn — reconcile picks up within 10-15s
docker rm -f observability_01
docker rm -f dcs_console_01
```

For a host-process change (dcs.lua, user_functions.lua, kb_log.lua
etc.), kill the dcs.lua child — start.sh's while-loop respawns it
immediately with the new code:
```bash
pkill -KILL -f 'deployment/cpu.*/runtime/dcs_host/dcs\.lua'
```

If start.sh itself was killed (laptop sleep, etc.):
```bash
cd ~/knowledge_base_assembly/luajit_programs_and_containers/building_blocks/nanodatacenter_dcs/deployment/cpu_01
nohup ./start.sh > /tmp/cpu_01.out 2>&1 & disown
cd ../cpu_02
nohup ./start.sh > /tmp/cpu_02.out 2>&1 & disown
```

## Key memory pointers (for AI context next session)

- `project_dcs_task4_design.md` — locked observability design.
- `project_dcs_infra_roadmap.md` — infra arc closed; pivot-to-apps
  unblocked but DELIBERATELY HELD until namespace + build
  modularization land (per user's "fix infra before apps" principle).
- `project_dcs_node_control_supervision.md` — reconcile + watchdog.
- `feedback_nginx_proxy_redirect_trap.md` — diagnostic order for
  proxied web app "refused" symptoms.
- `feedback_infra_before_apps.md` (write tomorrow) — the principle
  driving roadmap order: namespace + build modularization happen
  first, app dev waits.

## Key files for tomorrow's work

- `runtime/dcs_host/kb_log.lua:66` — `now_s()` change to `os.time()`.
- `runtime/dcs_host/kb_exception.lua:69` — same change for alarm ts.
- `runtime/dcs_host/dcs.lua` lines added in 54adee87 — the writer
  hook in the tick loop (already wired correctly per-CPU).
- `observability/container/log_web/lua/log_detail.lua` — chart
  filter at line 90 (`(os.time() - ts_sample) <= window_s`); should
  start passing samples through once kb_log writes wall-clock ts.
- `construction/subsystems/cpu_*.lua` — KB_LOG declarations (verify
  the three perf signals are declared on every CPU; add any missing).
