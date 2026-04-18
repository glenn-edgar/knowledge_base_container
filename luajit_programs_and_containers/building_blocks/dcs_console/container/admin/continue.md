# dcs_console :: admin slot -- Operator UI Continuation Plan

Living design doc for the DCS operator UI. When this stabilises, promote
the "Architecture" + "Contract" + "SCADA practices" sections to a
README.md; this file keeps session notes + open questions.

## Phase 1 (2026-04-18) -- shell DONE

Reusable shell vendored into `openresty_base` (shared across every user
container that needs an operator surface):

- `assets/dcs_shell/shell.css` -- mobile-first layout (drawer < 700px
  full-width, >= 700px 18em overlay). Traffic-light classes, stale-time
  decoration, status-list dl layout, header operator-name slot.
- `assets/dcs_shell/shell.js` -- drawer toggle, tree navigation,
  hash persistence, HX-Trigger-After-Settle `shell:context` listener,
  status popup with SSE lifecycle, operator identity prompt, alarm
  toggle, stale-time ticker, default-view auto-load.
- `assets/htmx/htmx.min.js` + `sse.js` -- 1.9.12 vendored.

Admin slot is the first consumer; any new user container pulls the
shell in with one `<link>` + three `<script>` tags and writes a tree.

## Why we built this shape (SCADA lessons)

Classic SCADA systems target 1920x1080 operator consoles; the first
time you need to triage an alarm from a phone in a field or a
warehouse, that regret costs hours. Decisions that shaped the shell:

1. **Mobile-first** -- one hamburger button, everything else behind it.
   Scales from 320px phone to 1440px desktop without horizontal scroll.
2. **Single pattern, all user containers** -- operators learn the nav
   once; it transfers across admin, planner_ui, manager_ui, and
   everything we ship later.
3. **Server as source of truth** -- title, status-popup endpoints, and
   alarm badge all come from the server on each fragment response
   (via `HX-Trigger-After-Settle`). No client-side state machine.
4. **Dumb shell, smart views** -- shell plumbing never changes; every
   phase from here on is additive (new views fill in, no shell churn).

## Four operator modes (the frame we design against)

Every pane should declare which mode it serves. Views that try to do
all four end up serving none well.

| Mode | ~% of shift | Operator question | Our UI's answer |
|---|---|---|---|
| **Passive monitoring** | 90% | "Is anything wrong?" | Traffic-light indicators, at-a-glance counts, no scrolling. |
| **Active investigation** | 7% | "What happened / where?" | Drill-down views, stale-data indicators, timestamps, recent events. |
| **Intervention** | 2% | "Fix this." | Confirmation-gated actions with operator identity + audit trail. |
| **Retrospective** | 1% | "What can I learn?" | History views with filtering and scoping. |

Phases 2-5 cover monitoring + investigation. Intervention (phase 6+) is
a separate risk tier.

## DCS operator task catalog (mapped to menu branches)

### Monitoring

| # | Task | Menu path | Data |
|---|---|---|---|
| M1 | Site glance -- "is everything green?" | `System / Overview` | system_ready, cluster_go, ready_bits, active-exception count, CPUs up/expected |
| M2 | CPU heartbeat freshness | `CPUs / cpu_NN / heartbeat` | bit_mask_table.heartbeat ts vs wall clock |
| M3 | Container fleet roster | `Containers` | CONTAINER_REGISTRY rows |
| M4 | Infra pulse | `Infra / <service>` | container running + shallow probe |
| M5 | Alarm badge (every view) | header `[ⓘ]` badge | count(SYS_EXCEPTION WHERE status=true AND acknowledged=false) |

### Investigation

| # | Task | Menu path | Data |
|---|---|---|---|
| I1 | What is this CPU doing? | `CPUs / cpu_NN / summary` | assignments, operational flag, ready/sync bits, resource samples tail |
| I2 | Why did this container restart? | `Containers / X / status` | status row, events stream, restart_count, started_ts, last_error |
| I3 | What's in this exception? | `Exceptions / Active` row detail | SYS_EXCEPTION row + status: type, instance, description, last_error, trace_b64 |
| I4 | Is this getting worse? (trend) | status popup sparkline | recent N samples from relevant stream |
| I5 | What's the CPU topology? | `System / Overview` graph | topology-derived CPU + container graph |
| I6 | What happened in the last hour? | `Exceptions / History`, `CPUs / cpu_NN / timeline` | recent exception rows, node_control events |

### Retrospective

| # | Task | Menu path | Data |
|---|---|---|---|
| R1 | Full exception history w/ filtering | `Exceptions / History` | all SYS_EXCEPTION rows + filters |
| R2 | Restart history for a container | `Containers / X / history` | events stream full contents |
| R3 | Shift handoff summary | `System / Overview` scoped window | "since <time>: N exceptions, M restarts, P teardowns" |

### Intervention (deferred past phase 5)

| # | Task | Risk |
|---|---|---|
| X1 | Ack exception (silence alarm, keep record) | low -- first mutation we add |
| X2 | Clear exception (mark resolved) | low |
| X3 | Restart a container (recovery after crash / config change) | medium, confirm-by-name |
| X4 | Pause monitoring on a CPU (maintenance) | medium |
| X5 | Trigger full teardown of a CPU | HIGH, two-step + audit |
| X6 | Change a setpoint (poll_interval, thresholds) | low-medium, audit |
| X7 | **Stop container for maintenance / Start back** (operator-initiated pause) | medium, reversible |

X7 is distinct from X3: an operator-initiated reversible pause so a
container can be worked on without node_control treating the stop as a
crash (which would trigger a SYS_EXCEPTION and restart attempts). The
UI surfaces a Stop / Start toggle on `Containers / X / status`; under
the hood it sets the container's `unmonitor_until` timestamp in
`knowledge_base_status`, which node_control's monitor state honours
as "do not flag this one for now". Lease defaults to
`unmonitor_lease_default_s` (site-level status field, already in the
KB schema); operator can extend or clear it.

Natural home in the buildout: Phase 5 (after exceptions) or Phase 6
(with intervention suite). Per the user request 2026-04-18, this is
promoted from "X3 variant" to a first-class intervention task.

## SCADA practices adopted in Phase 2

### Cheap + high value (bake in now)

1. **Traffic-light colours only** -- green (ok) / yellow (stale or warning)
   / red (fault) / grey (unknown). Always paired with an icon + text so it
   works for colour-blind operators and screenshots.
2. **Stale-data indicators** -- every timestamp shows "Ns ago"; after
   threshold (default 30s) flips to grey + "STALE" label. Drives
   operator trust.
3. **UTC on the wire, local at the browser** -- server emits ISO-8601
   UTC, shell.js formats to local on render. No timezone ambiguity.
4. **Alarm badge on every view** -- `[ⓘ]` button shows active-exception
   count site-wide regardless of current view. Already plumbed via
   shell:context event; server just computes it.
5. **"Last event" footer** -- every view has a one-line "last registry
   change: 12s ago" footer so operators can tell whether the UI is
   stuck.
6. **Explicit empty states** -- not "no rows" in a blank box; "No
   active exceptions. Last cleared 3h ago." Operators hate not knowing
   if the UI is broken or data is really zero.
7. **Operator identity** -- simple prompt on first visit, stored in
   `localStorage["dcs.operator"]`, rendered in header. Used for audit
   trail when mutations arrive.
8. **Audio alarm toggle** -- 🔔 / 🔕 in header, default silent. When
   enabled, short beep on badge-count increase.

### Adopted in Phase 4+ (exceptions)

9. Ack is explicit, distinct from clear (two-step mental model).
10. Optional audio alarm on new exceptions (toggle built in Phase 2,
    wired in Phase 4).
11. Audit log of every mutation with operator identity.

### Deferred past v1

12. Trending charts (SVG sparklines inline OK when cheap).
13. Shift-handoff report generation (needs audit-log volume first).
14. Role-based access control (trusted LAN v1).
15. Multi-site aggregation.
16. Alarm rationalisation / priorities.

## Phase 2 scope (narrow + deep) -- LOCKED

Build `System / Overview` end-to-end with every SCADA-hygiene pattern
baked in, because it's the default landing view and sets the polish bar
for every subsequent phase.

**Deliverables:**
- Real fragment renderer hitting pg (pgmoon + /etc/hosts IP resolution).
- Traffic-light status rendering with icon + colour + text.
- Stale-data decoration on every timestamp (client-side 1s ticker).
- Alarm-count badge populated via server-side COUNT query.
- Status popup SSE stream showing the same data refreshed every 2s
  while the popup is open.
- Footer "last event" widget.
- Explicit empty / zero states.

**Not in Phase 2:**
- Other menu branches (CPUs / Containers / Exceptions / Infra) stay
  on the placeholder renderer for now; Phase 3+ fills them.
- No TCP probes of nats/mqtt/kv_bridge in the status popup -- those go
  in Phase 6 (Infra) where they belong.
- No mutations.

## Open questions -- LOCKED answers for Phase 2

Four decisions surfaced during design; these are the answers that
drive implementation. Tweak after seeing it work.

1. **Operator identity prompt on first visit?** -- YES. Simple
   `localStorage["dcs.operator"]`; prompt once via a small startup
   dialog. Displayed in header. No verification; trust the operator.
   Cost: low. Value (when mutations arrive): high. Audit always
   resolvable, even if the answer is "night-shift-laptop".
2. **Audio alarm toggle?** -- YES, build the toggle in Phase 2, wire
   the actual beep in Phase 4. Default silent (mobile-friendly);
   operator opts in. Icon: 🔔 (enabled) / 🔕 (muted).
3. **Stale threshold location?** -- Per-widget via
   `data-stale-after="N"` attribute; 30s default when attribute
   missing. Different signals go stale at different rates
   (heartbeat 15s, resource samples 300s, etc.).
4. **Default landing view?** -- Auto-load `System / Overview` when
   hash is empty. Nothing useful happens at the placeholder state, and
   the overview answers the main operator question instantly. Override
   via `<meta name="dcs-default-view" content="...">` if an app wants
   a different landing point.

## Architecture (phase 1 + phase 2 forward)

### Three layers

| Layer | Lives in | What it owns | Rebuilt when |
|---|---|---|---|
| Shared shell | `openresty_base/container/prebuilt_web_assets/dcs_shell/` | CSS + JS + htmx libs. Reusable verbatim. | openresty_base rebuild |
| Per-app static HTML | `<app>/container/<slot>/html/index.html` | Shell template + hand-rolled (or server-rendered) tree | app image rebuild |
| Per-app dynamic endpoints | `<app>/container/<slot>/lua/*.lua` + `conf/nginx.conf` | `/fragment/*`, `/status/*`, `/sse/*` routes | app image rebuild |

### App contract

A container that mounts the shell must:

1. Serve `index.html` at `GET /` -- a copy of the shell skeleton with
   a `<ul class="tree">` inside `#shell-drawer`. Every leaf carries
   `data-fragment="<relative-path>"` (no leading slash).
2. Include shell + htmx via **relative** hrefs. nginx.conf aliases
   `/assets/` to `/usr/local/openresty/site/assets/`.
3. Implement each leaf's fragment endpoint. Every response must:
   - Return an HTML fragment for `#shell-content`.
   - Set
     `HX-Trigger-After-Settle: {"shell:context": {"title":"...", "status_url":"...", "status_stream_url":"...", "badge":"..."}}`
     -- any subset; missing fields disable the corresponding UI
     element.
4. Optionally implement `GET <status_url>` (one-shot HTML) and
   `GET <status_stream_url>` (`text/event-stream` with
   `event: update\ndata: <html>\n\n` frames) for views that opt in.

### Data flow for one click

```
User taps leaf
   -> shell.js click handler
htmx.ajax(GET, "fragment/cpu/cpu_01/summary", "#shell-content")
   -> admin nginx on :19004 (direct or via gateway proxy)
content_by_lua_file lua/fragment.lua
   -> dispatches to lua/views/<path>.lua
   -> returns HTML fragment (swapped into #shell-content)
   -> sets HX-Trigger-After-Settle header
   -> browser receives response
shell.js shell:context handler
   -> updates #shell-title, [ⓘ] state, stashes status_url + stream_url,
      renders/hides badge
   -> if badge increased AND audio toggle on: short beep
```

### URL / persistence

- Top-level URL: `http://.../ui/dcs_console_01/admin/` (via gateway)
  or `:19004/` (direct).
- Current view: `location.hash = "#view=<fragment-path>"`. Restored
  on refresh. Bookmarkable.
- All internal paths relative (no leading `/`). Works identically
  direct and behind the gateway's prefix-stripping proxy. Hard rule.

## Phase progression

| Phase | What gets real | Status |
|---|---|---|
| 1 | Shell + stub tree + stub fragments + stub SSE | DONE 2026-04-18 |
| 2 | `System / Overview` real; all SCADA patterns baked in (traffic lights, stale, badge, identity, alarm toggle) | in progress |
| 3 | `CPUs` menu real; tree goes pg-dynamic | planned |
| 4 | `Containers` menu real; registry table + per-container detail | planned |
| 5 | `Exceptions` menu real; ack/clear actions (first mutations) | planned |
| 6 | `Infra` menu real; pg / nats / mqtt / kv-bridge probes | planned |
| 7+ | Full intervention suite (container restart, CPU teardown) | planned |

Each phase after 2 is additive and independent.

## File layout (after phase 2)

```
dcs_console/container/admin/
  conf/nginx.conf              -- locations + env + mime.types
  html/index.html              -- shell skeleton + server-rendered tree
  lua/
    fragment.lua               -- dispatcher: /fragment/<path>
    status.lua                 -- dispatcher: /status/<path>
    sse.lua                    -- dispatcher: /sse/<path>
    shell_helpers.lua          -- pg connect + ctx header + badge count + html helpers
    views/
      system_overview.lua      -- phase 2 view
      placeholder.lua          -- fallback for not-yet-implemented views
    sse/
      system_overview.lua      -- phase 2 live stream
      placeholder.lua          -- fallback stub stream
```

## Operator identity handshake (Phase 2+)

First visit (hash empty):
1. `shell.js` reads `localStorage["dcs.operator"]`.
2. If missing, shows a small dialog "Operator name?"; stores whatever
   is typed (defaulting to "unknown" if empty).
3. Populates `#operator-name` in the header.
4. Injects `hx-headers='{"X-Operator":"..."}'` on `<body>` so every
   htmx request carries the operator identity. (Phase 2 uses the
   header for nothing; Phase 6+ audit-logs it on mutations.)

## Alarm sound handshake (Phase 2+)

1. `shell.js` reads `localStorage["dcs.alarm_enabled"]`.
2. Header shows 🔕 (muted, default) or 🔔 (enabled) toggle.
3. When enabled + `shell:context` badge count > previous badge count:
   play one short beep (a vendored or data-URI base64 wav).
4. Phase 4 (exceptions) adds the beep trigger; Phase 2 just builds
   the toggle + state persistence.

## Known tradeoffs accepted in phase 2

- Tree is hand-rolled; phase 3 makes it pg-driven. Fine for a 2-CPU
  site.
- Status popup doesn't stack with main content -- one popup at a time.
  Users who need two dashboards open the admin twice in the gateway's
  outer tab strip.
- Clicking a tree leaf closes the drawer; there is no "open in new
  tab". Same answer: the gateway's outer tab strip.
- No keyboard shortcuts yet (Ctrl+Tab etc.); mobile-first, keyboard
  nav comes later.
