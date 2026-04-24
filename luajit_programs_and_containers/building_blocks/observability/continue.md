# observability container — continue plan

Last updated end of **2026-04-24**. Session covered a big batch of
observability polish + some hard lessons about Docker Desktop's
platform flakiness. Pi 4 deployment remains the target.

## Location of this file

`~/knowledge_base_assembly/luajit_programs_and_containers/building_blocks/observability/continue.md`

## 2026-04-24 session commits (newest first)

| commit   | what |
|----------|------|
| `0fe32b90` | `START_ASSIGNED_CONTAINERS` stamps `last_restart_ts` — first-boot containers now get watchdog's 30s grace |
| `666639eb` | Auto-clear threshold/slope/rate rules + container_hung on successful probe |
| `6b4dbb2e` | `sample_gap` rules self-clear when gap closes (SCADA RTN_UNACK transition) |
| `11063ca4` | `push_sample` 6→3 pg ops + analyzer heartbeats throttled 1Hz → 0.2Hz |
| `62063d05` | Consolidated D+A+C+B fix (heartbeats + stalled rules / batch docker / container-stats writer / export endpoints) |

Plus side-fixes in `62063d05`:
- Fixed `check_sample_gaps` comparing monotonic-clock `now` to wall-clock `live.last_ts` — no sample_gap rule would ever have fired until this fix.
- Fixed `tonumber(str:gsub(...))` crash in docker-stats parser (gsub's replacement-count was being interpreted as a base).
- Raised `tick_overrun` threshold 500ms → 1500ms (master CPU baseline is 600-900ms; 500 was under the noise floor).

## What landed, categorized

### Observability-of-observability (D)
- Site-level `log_analyzer_heartbeat` and `exception_analyzer_heartbeat` KB_LOGs (`diagnostic` kind, `auto_health=false`).
- `SYS_EXCEPTION log_analyzer_stalled` / `exception_analyzer_stalled` at priority 1.
- `sample_gap` rules (60s threshold, 120s cooldown) on each heartbeat.
- Heartbeat cadence throttled to 0.2 Hz in both analyzer tick loops.
- **Verified via synthetic outage** (put observability_01 under 180s maintenance lease + wait 88s, clear lease → catch-up fire): `log_analyzer_stalled` transitions to UNACK_ACTIVE with `last_error = "sample_gap: 125s since last sample"`.

### Batch docker + parallel probes (A)
- New `docker.list_running()` — one `docker ps` instead of N `docker inspect`.
- New `probe_many()` helper in `user_functions.lua` — shell-background parallel curls via `&` + `wait`. Wall-time ≈ max(slowest curl) regardless of N.
- Measured result on Snapdragon: reconcile_check_ms 250→65ms, watchdog_probe_ms 500→70-100ms.

### Container-metric writer via docker stats (C)
- `sample_containers_via_docker_stats()` fallback triggers when cgroup path is disabled (Docker Desktop / WSL2).
- Shells out `docker stats --no-stream --format '{{json .}}'` once per SAMPLE_CONTAINERS tick (60s cadence).
- Decomposes into 4 per-metric KB_LOG rings per container (cpu_pct, mem_rss_mb, disk_read_kbps, disk_write_kbps).
- Delta-tracks cumulative block-bytes → rate via wall-clock divisor.
- **Verified populating**: 7 containers × 4 metrics with sensible values (pg-vector 8% CPU, idle near 0%).

### Export endpoints (B)
- `/export?path=<ltree>&tier=raw|1min|1hour|1day&format=csv|json&window_s=N` in `log_web`.
- Streams rows from `knowledge_base_stream` (raw) or `knowledge_base_rollups` (tier).
- Content-Disposition prompts browser download; `curl -O` also works.

### Alarm hygiene (huge noise reduction)
- `sample_gap` rules call `kb_exc.clear()` when gap closes — alarm transitions UNACK_ACTIVE → RTN_UNACK on its own.
- `rules.evaluate` (threshold/slope/rate/z_score/etc.) same treatment: clear target when not tripped.
- Watchdog handler clears `container_hung` on every successful probe.
- **Measured effect**: 76 stuck alarms post-reboot collapsed to 2 after first fix, 0 after second.

### Connection resilience
- `pg_connector.is_alive()` helper (liveness probe via `SELECT 1`).
- `dcs.lua` tick loop now auto-reconnects on dead pg connection (10s liveness check).
- Both analyzers had their own reconnect loops added earlier in session (commit `87999ceb` from the start-of-session wall-clock fix).
- Boot-grace: `START_ASSIGNED_CONTAINERS` stamps `last_restart_ts[asg.name] = now` so first-time spawns get the 30s watchdog grace.

### pg write-amplification reduction
- `push_sample` collapsed 6 pg ops → 3:
  1. UPDATE stream with subquery (was SELECT + UPDATE).
  2. Multi-row INSERT ON CONFLICT for last_sample_ts + last_value.
  3. Single INSERT ON CONFLICT with inline increment for sample_count_total.
- Heartbeat writes throttled to every 5th tick.
- **Honest note**: I misread docker stats' CPU % formula earlier in the session. On Snapdragon pg-vector at 8% = 0.08 cores (8% of one), NOT 1 core of 12. Pi 4 projection was 12× too pessimistic. System was never CPU-constrained. The collapse is still useful cleanup but wasn't urgent.

## Current problems (as of end-of-session)

### Unresolved — Docker Desktop platform issues

**Not DCS bugs.** Go away on Pi 4 with native Docker.

1. **Infra containers exit in sync.** pg-vector, nats-js-ram, mosquitto-ram-ws_main received SIGTERM together at least twice during this session (exit codes 0/0/1/0 within 5s of each other). Cause: Docker Desktop backend restart or WSL2 reset. Now mitigated by setting `restart: unless-stopped` on all four (was `no` on three of them).

2. **vpnkit port-forwarder flakiness.** `curl 127.0.0.1:<port>` and `psql -h localhost` both intermittently fail even when the target container is healthy. Watchdog's probe uses `http://127.0.0.1:<port>/` so it gets false-positive "container hung" readings during forwarder glitches. Respawns that container unnecessarily, which adds load, compounding the forwarder stress.

3. **Duplicated DCS watchdogs after session handovers.** Multiple `pkill` / `nohup` cycles today left behind zombie `start.sh` + `dcs.lua` pairs. Each pair ran reconcile + watchdog independently, thrashing containers. Fixed by explicit PID-kill cleanup; worth adding a systemd/supervisor-style single-instance guard someday.

### Architectural gap exposed by (2) above

**Watchdog probing via 127.0.0.1:port is wrong long-term.** The probe shouldn't depend on the host's external port map — it should reach the container via its Docker internal network IP. Two benefits:
- Immune to vpnkit/port-forwarder problems (bypasses them entirely).
- Works identically on Docker Desktop, native Linux, Pi 4.

Not landed today; documented here as next major improvement to the watchdog.

### SCADA semantics gap (fixed today but worth noting)

Before today, every rule's fire path called `kb_exc.raise` but never `kb_exc.clear`. Once an alarm fired, it stayed `UNACK_ACTIVE` until operator intervention — even after the condition cleared. All rules (`threshold`, `slope_trend`, `rate_of_change`, `z_score`, `envelope_drift`, `cusum`, `sample_gap`) had this anti-pattern. Today's fix covers all of them. Now:
- Condition tripped → `raise` → UNACK_ACTIVE
- Condition gone → `clear` → RTN_UNACK (operator ack → NORMAL)

## Open problems queue (deferred)

1. **Watchdog probe via container IP, not 127.0.0.1:port** — see architectural gap above. Next major supervisor improvement.
2. **Single-instance guard on DCS watchdogs** — prevents zombie `start.sh` pairs from dueling.
3. **Build-system modularization for outside review** (already noted; unchanged).
4. **Tree-control + `?base=` scoping across all inventory views** (already noted).
5. **NATS-for-samples pilot** on the 3 high-cadence dcs.lua metrics (already scoped).
6. **pgmoon migration** of dcs_host — low priority now that the performance picture is corrected.
7. **Cloud aggregator service** (Go) — for fleet rollup. Future.

## Rebuild sequence (cheat-sheet)

```bash
# Rebuild observability image
cd ~/knowledge_base_assembly/luajit_programs_and_containers/building_blocks/observability/container
./docker_build.sh

# Rebuild dcs_console image
cd ~/knowledge_base_assembly/luajit_programs_and_containers/building_blocks/dcs_console/container
./docker_build.sh

# Rebuild KB from topology (only if construction/ changed)
cd ~/knowledge_base_assembly/luajit_programs_and_containers/building_blocks/nanodatacenter_dcs/construction
./build_kb.sh
./slice_bootstrap.sh
./stage_deploy.sh

# Rebuild chain-tree IR (only if dcs_dsl.lua changed)
cd ~/knowledge_base_assembly/luajit_programs_and_containers/building_blocks/nanodatacenter_dcs/runtime/chain_tree
./build_dsl.sh

# Apply dcs.lua / user_functions.lua changes (no rebuild, just restart)
pkill -KILL -u $USER -f 'deployment/cpu_0[12]/runtime/dcs_host/dcs\.lua'
```

## Cold-start from scratch

```bash
# 1. Kill DCS watchdogs + dcs.lua (clean stop)
pkill -KILL -u $USER -f 'deployment/cpu_0[12]/(start\.sh|runtime/dcs_host/dcs\.lua)'
# If any duplicate start.sh remains, kill by PID:
#   ps -eo pid,cmd | grep 'start.sh'

# 2. Start infra (auto-start after restart policy fix, but belt+braces)
docker start pg-vector nats-js-ram mosquitto-ram-ws_main kv-bridge

# 3. (Optional) Remove app containers so node_control recreates fresh
docker rm -f observability_01 dcs_console_01 test_app_01 robot_manager_01 ros_mission_planner_ii_01

# 4. (Optional) Reset any stale alarm state
source ~/.config/nanodatacenter/secrets.env
PGPASSWORD="$PG_PASSWORD" psql -h localhost -U gedgar -d knowledge_base -tAc "
UPDATE knowledge_base_status SET data = jsonb_build_object('value', 'NORMAL')
 WHERE path::text LIKE '%SYS_EXCEPTION%.state'
   AND (data::jsonb->>'value') IN ('UNACK_ACTIVE', 'ACK_ACTIVE', 'RTN_UNACK')"

# 5. Launch DCS watchdogs (use setsid to detach cleanly)
( cd ~/knowledge_base_assembly/luajit_programs_and_containers/building_blocks/nanodatacenter_dcs/deployment/cpu_01 && setsid nohup ./start.sh > /tmp/cpu_01.out 2>&1 < /dev/null & )
( cd ~/knowledge_base_assembly/luajit_programs_and_containers/building_blocks/nanodatacenter_dcs/deployment/cpu_02 && setsid nohup ./start.sh > /tmp/cpu_02.out 2>&1 < /dev/null & )

# 6. Wait for sys_ready = 1
until [ "$(PGPASSWORD=$PG_PASSWORD psql -h localhost -U gedgar -d knowledge_base -tAc "SELECT data::jsonb->>'value' FROM knowledge_base_status WHERE path::text='system.site.moonbase.alpha.dcs.KB_STATUS_FIELD.system_ready'" 2>/dev/null | tr -d ' ')" = "1" ]; do sleep 3; done
echo "system_ready = 1"
```

## Access points

| what | URL |
|---|---|
| DCS gateway (main UI) | `http://127.0.0.1:19003/` |
| DCS admin | `http://127.0.0.1:19004/` |
| observability exception_web | via gateway tab OR `http://127.0.0.1:19007/overview` |
| observability log_web | via gateway tab OR `http://127.0.0.1:19008/detail` |
| test_app | `http://127.0.0.1:19001/` |
| robot_manager (stub) | `http://127.0.0.1:19006/` |
| ros_mission_planner_ii (stub) | `http://127.0.0.1:19005/` |

## Next session — suggested opening moves

1. **If Docker Desktop is misbehaving again**: `wsl --shutdown` from PowerShell, then re-launch Docker Desktop. Clears vpnkit state.
2. Review this file's "open problems queue" and pick next item.
3. The next major landable item — my vote — is **watchdog probe via container IP** (solves Docker Desktop flakiness AND is better on Pi 4). ~2-3 hours.

## Pre-apps roadmap (reminder — from earlier sessions)

1. Reconcile + dcs.lua reconnect ✓ (earlier in session)
2. Build-system modularization (outside review)
3. Tree-control + base-context scoping (UI primitive across all inventory views)
4. Namespace cleanup (merges with #3)
5. Application development

User's infra-before-apps principle (`feedback_infra_before_apps.md`) remains the north star.
