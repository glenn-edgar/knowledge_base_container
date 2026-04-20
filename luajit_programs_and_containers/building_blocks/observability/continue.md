# observability container — continue plan

Session 2026-04-20 finished Task 4 (all 9 phases) and landed two
follow-on improvements. Next session: investigate the UI display for
the exception_web and log_web servers.

## Where we are

Master branch, commits pushed through **`530a14d2`** (redirect fix).

Recent commit trail:
| commit    | what |
|-----------|------|
| d16b945a  | task 4 phase 9 e2e verified — Task 4 COMPLETE |
| 36ec294b  | node_control: continuous reconcile + HTTP watchdog |
| 530a14d2  | fix: observability + gateway redirects relative + namespaced |

Observability container (`observability_01`) is DCS-managed on cpu_01
at ports 19007 (exception_web) and 19008 (log_web). All 4 processes
(exception_analyzer, log_analyzer, exception_web, log_web) run under
the chain-tree controller; controller reports `apps=4/4`. cpu_02
still runs `ros_mission_planner_ii_01` + `robot_manager_01`.

Reconcile + watchdog from commit 36ec294b are live in node_control's
monitor_st — any container `docker rm -f`'d or hung gets auto-recovered
with SYS_EXCEPTION raised (`container_respawned` / `container_hung`).

## Next session — UI display investigation

The user wants to **investigate the UI display for the exception and
log web servers**. The pages render (all 9 views return 200) but the
user hasn't yet reviewed the actual content against what SCADA alarm
summary / log detail UIs should look like. Likely areas of focus:

- **exception_web**: density of information in the alarm tables, how
  the 5-state enum is visually distinguished, whether the signature-
  dedup display is useful, priority colour conventions, whether
  `/detail` renders the 15 status children in a scannable layout.
- **log_web**: whether the uPlot charts are actually readable, the
  grid of mini strip charts in Live Ops, whether the rule inventory's
  138-row table is usable, whether the log detail view shows the
  signal + envelope + rule-trip markers clearly.
- **interaction through the gateway**: now that the iframe redirects
  work (commit 530a14d2), sanity-check that `/action` POST handlers
  (ack / shelve / etc.) still land correctly via the namespaced
  `/ui/observability_01/exception_web/action` path. The redirect
  fix does `Location: /referer` rewriting; actions.lua does
  `ngx.redirect(referer, 303)` — may need additional checking.
- **known oddity**: SYS_EXCEPTION `last_raised_ts` is being stored as
  monotonic-clock seconds (e.g. 258823) instead of wall-clock epoch.
  Caused by `ptime.now_sec()` in `kb_exception.lua:69` returning
  CLOCK_MONOTONIC instead of CLOCK_REALTIME. Pre-existing; not
  introduced by the reconcile/watchdog work. May be worth fixing
  before the UI investigation since the alarm journal sorts on this.

No writer is currently pushing samples into KB_LOGs — the uPlot
charts will therefore display empty series. Writer wiring is the
other orthogonal task (see "Deferred" below).

### UI tools for inspection

Use the DCS gateway path, not direct ports (direct ports had the
redirect bug and may still have latent issues):
- `http://127.0.0.1:19003/` — gateway landing
- Click `observability_01 / exception_web` tab
- Click `observability_01 / log_web` tab

Admin UI for intervention is at `http://127.0.0.1:19004/`.

## Task 4 — DONE summary

All 9 phases complete, all committed + pushed:
| phase | commit   | artifact |
|-------|----------|----------|
| 1     | 1db8a441 | SCADA nested SYS_EXCEPTION. |
| 2     | 6fa56210 | KB_LOG / KB_RULE construction DSL. |
| 3     | 6fa56210 | 14 v1 log signals + ~128 seed rules. |
| 4     | 046ecd85 | kb_log.lua + kb_rule.lua runtime libs. |
| 5     | ce5976b9 | container skeleton + build-time speedup. |
| 6     | 29a83e7f | log_analyzer real logic. |
| 7     | 90660680 | exception_analyzer real logic. |
| 8a    | 06946b97 | Site Overview + Active Alarms. |
| 8b    | a9742359 | Detail + Actions + Journal + Shelved. |
| 8c    | d08762b2 | uPlot vendoring + Log Detail. |
| 8d    | f2642c56 | Live Ops + Archival + Rule Inventory. |
| 9     | d16b945a | topology integration + e2e verified. |

Plus post-completion improvements:
| 36ec294b | reconcile + watchdog columns in node_control. |
| 530a14d2 | redirect fix (absolute_redirect off + proxy_redirect). |

## Deferred (orthogonal to Task 4 / UI investigation)

**Writer wiring.** No writer currently pushes samples into KB_LOGs.
Charts will show empty series until this lands. Recommended approach:
adapt `runtime/dcs_host/host_sampler.lua` (currently writes to
monitor.samples) to also push to per-metric KB_LOG rings. Or sidecar
translator in log_analyzer. The 14 declared signals are in
`construction/subsystems/node_monitor.lua` + similar.

**Monotonic-vs-wall-clock timestamps.** `kb_exception.lua:69`
`now_s()` uses `ptime.now_sec()` which appears to be CLOCK_MONOTONIC.
Alarm timestamps are small integers (~258823) instead of epoch seconds.
Fix: inspect posix_time.lua or switch `now_s()` to `os.time()`.

**Meta-exception `alarm_flood_detected`.** exception_analyzer's
auto-shelve logs to stderr but doesn't raise the alarm. Need to
declare in `catalogs/topology.lua` first, then raise from
`check_flap_floods`.

**RTN_UNACK aging.** Purely cosmetic; UI can compute "unacked for
X" from last_rtn_ts.

**TimescaleDB migration.** Documented in project_dcs_task4_design.md.
Trigger on rollup query degradation.

## How to rebuild (fast path)

```bash
# Image rebuild (only if observability/container/ changed)
cd ~/knowledge_base_assembly/luajit_programs_and_containers/building_blocks/observability/container
./docker_build.sh                    # ~5s on warm cache

# Gateway rebuild (only if dcs_console/container/ changed)
cd ../../dcs_console/container
./docker_build.sh                    # ~5s on warm cache

# KB + deploy (only if definitions.lua / topology.lua / subsystems/ changed)
cd ../../nanodatacenter_dcs/construction
./build_kb.sh > /tmp/build_kb.log 2>&1     # ~22s
./slice_bootstrap.sh > /tmp/slice.log 2>&1 # ~0.4s
./stage_deploy.sh > /dev/null

# Chain-tree rebuild (if dcs_dsl.lua / user_functions.lua changed)
cd ../runtime/chain_tree && ./build_dsl.sh

# Force container respawn — reconcile will pick up within 10-15s,
# no need to restart DCS watchdogs anymore:
docker rm -f observability_01
docker rm -f dcs_console_01
```

If DCS watchdogs themselves died (laptop sleep etc.):
```bash
cd ~/knowledge_base_assembly/luajit_programs_and_containers/building_blocks/nanodatacenter_dcs/deployment/cpu_01
nohup ./start.sh > /tmp/cpu_01.out 2>&1 & disown
cd ../cpu_02
nohup ./start.sh > /tmp/cpu_02.out 2>&1 & disown
```

**Known WSL2/bash quirk**: if a terminal was inside deployment/cpu_XX/
when slice_bootstrap ran, its cwd is now a stale inode. `cd ~ && cd
~/.../cpu_XX` to re-enter.

## Key memory pointers (for AI context next session)

- `project_dcs_task4_design.md` — locked design (SCADA + KB_LOG +
  KB_RULE). Use to judge UI display decisions against spec.
- `project_dcs_infra_roadmap.md` — infra arc closed; pivot to apps
  unblocked. UI polish here is application work, not infra.
- `project_dcs_node_control_supervision.md` — reconcile + watchdog
  behaviour (in case a container respawn fires during investigation).
- `feedback_nginx_proxy_redirect_trap.md` — if a UI refuses via
  browser, check redirect URLs before chasing firewall / network.
- `feedback_coordinator_startup_wipe.md` — observability design
  principle (idempotent, don't clear state at startup).
- `feedback_nginx_env_directive.md` — openresty `env NAME;` needed
  at main context for pgmoon env vars.

## Key files for the UI investigation

**exception_web:**
- `observability/container/exception_web/lua/site_overview.lua`
- `observability/container/exception_web/lua/active_alarms.lua`
- `observability/container/exception_web/lua/alarm_detail.lua`
- `observability/container/exception_web/lua/alarm_journal.lua`
- `observability/container/exception_web/lua/shelved.lua`
- `observability/container/exception_web/lua/helpers.lua`
- `observability/container/exception_web/lua/render.lua` — shared
  page frame + CSS.

**log_web:**
- `observability/container/log_web/lua/live.lua`
- `observability/container/log_web/lua/log_detail.lua` (has uPlot)
- `observability/container/log_web/lua/archival.lua`
- `observability/container/log_web/lua/rules.lua`
- `observability/container/log_web/lua/render.lua` — shared page frame
  (identical CSS to exception_web's version; candidate for shared
  module if we want to deduplicate).

**uPlot vendored assets:**
- `observability/container/log_web/static/uPlot.iife.min.js`
- `observability/container/log_web/static/uPlot.min.css`
