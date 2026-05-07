# logxform — Transducer-Based Log/Event Pipeline (DEFERRED)

> **Status: considered and rejected, 2026-05-07.**
>
> This design was evaluated as the LuaJIT implementation of the anomalisa
> architecture (decimation → windowing → Welford stats → bucket aggregation →
> anomaly gates) generalized to ChainTree's three-tier deployment. It is sound,
> the decisions below are locked, and the spec is preserved as a future-implementer
> reference. We are not building it now.
>
> **Why not now:**
> - The abstraction pays off when ≥2 pipelines share xforms (decimate + welford +
>   zscore in one, bucket + spike in another). Today we have at most one concrete
>   pipeline in mind, so the ~14-xform surface area + protocol + compose +
>   adapters is overkill — an inline loop is cheaper to write and read.
> - The 3-arity reducer protocol is idiomatic in Clojure but slightly awkward in
>   Lua (closures-over-upvalues for state allocates more than an explicit state
>   struct), and we don't yet have the second consumer to amortize that cost.
> - Live/replay parity is the strongest argument for the abstraction. We don't
>   yet have a recorded-stream replay workflow or anomaly debugging cadence
>   that would exercise it.
>
> **When to revive (triggers):**
> 1. A second log/event pipeline appears that would share at least 2 stages
>    (e.g. one welford-based anomaly stream + one bucket-based spike stream).
> 2. We adopt a recorded-stream replay workflow for anomaly debugging — at
>    that point time-injection (`ev.ts`, never `os.time()`) becomes
>    load-bearing and must be enforced uniformly across pipelines.
> 3. The bare-Cortex-M port (`xform_t` chain, preallocated, no closures —
>    see "Out of scope") becomes a real target. The LuaJIT version is the
>    semantic ground truth for that port; building it first de-risks the C port.
>
> **How to revive (sequencing — do not skip steps):**
> 1. Re-read the **Core decisions** section below. Those 10 items are locked
>    for a reason (bare-MCU port + live/replay parity). Do not silently work
>    around them.
> 2. Implement in the order from "Hand-off notes for Claude Code":
>    `protocol.lua` → `compose.lua` → `init.lua` → stateless xforms
>    (`map`/`filter`/`take`) with protocol tests → `welford` + `bucket` →
>    sources/sinks last. Use `iter` source + `collect` sink for all pre-NATS tests.
> 3. Keep the module under `commissioning_software/` (its natural home is
>    near the kb/ and engines/ trees, not platform_containers/observability/
>    — observability is a *consumer*, not the owner of the runtime).
> 4. The `kb_anomalies` PostgreSQL table is assumed to exist; coordinate the
>    schema migration separately before sinking to it.
>
> The remainder of this document is the original spec, preserved verbatim
> as the implementation reference.
>
> ---

## Goal

Build a LuaJIT module (`logxform`, rename freely) that processes log and event streams using composable transducers. The pipeline ingests events from NATS subjects, MQTT topics, SQLite tails, or file replays; runs them through fused stateful transformations (decimation, windowing, Welford stats, bucket aggregation, anomaly detection); and sinks results to NATS, SQLite, KV, or a log file. This is the LuaJIT implementation of the anomalisa architecture, generalized to ChainTree's three-tier deployment model.

The same transducer chain must run identically against live NATS streams in production and against recorded SQLite logs or CBOR-stream files during testing, with no source-specific code in the transducers themselves.

## Core decisions

These are locked. Do not revisit during implementation.

1. **Reducer protocol is 3-arity.** A reducer `rf` is a single callable invoked at three arities: `rf()` returns the initial accumulator, `rf(acc, x)` is the per-input step, `rf(acc)` performs completion (flush buffered state, return final acc). This matches Clojure's transducer protocol and gives windowing transducers a clean place to flush on stream end.

2. **Transducer signature is `xf(rf) -> rf'`.** A transducer takes a reducer and returns a new reducer with a transformation wrapped in. Composition is plain function composition: `compose(a, b, c)(rf) = a(b(c(rf)))`. Pipeline reading order matches `compose` argument order — `compose(decimate(10), welford_window(60), to_nats)` reads left-to-right as data flow.

3. **State lives in upvalues, not in the accumulator.** Stateful transducers (window, dedupe, take, partition, Welford, bucket) capture state in closure upvalues over the returned reducer. The `acc` carries downstream-visible accumulation only. This keeps composition predictable and sources/sinks agnostic.

4. **Early termination via `reduced` wrapper.** A reducer can wrap its return with `reduced(x)` to signal "no more inputs needed." The runner checks for this on each step and short-circuits to completion. Implements `take(n)` and similar.

5. **One source, one sink, one chain per `run`.** `logxform.run(source, xform, sink)` drives one source through one composed transducer to one sink. Fan-out is achieved by composing a `tee(sink_a, sink_b)` transducer at the tail, not by multi-sink dispatch in the runner.

6. **Sources and sinks are adapter modules, not transducers.** A source exposes `source:next() -> ev | nil` and `source:close()`. A sink is a reducer (the terminal `rf`). Adapters live in `logxform.sources.*` and `logxform.sinks.*` namespaces.

7. **Time is injected, never wall-clock-implicit.** Every event carries a `ts` field (epoch ms). Windowing and bucket transducers read `ev.ts`, never `os.time()`. This is non-negotiable: replay against historical SQLite must produce identical output to a live run.

8. **CBOR is the canonical event format on the wire.** Events from NATS/MQTT arrive as CBOR-decoded LuaJIT tables. Transducers operate on tables, never on bytes. Decoding lives in source adapters.

9. **No new dependencies beyond the existing ChainTree LuaJIT runtime.** Use the existing FFI bindings for NATS, MQTT, luadbi (PostgreSQL/SQLite), dkjson, and the CBOR module already in the runtime. No new wheels.

10. **No class system.** Reducers are functions; transducers are functions returning functions; composition is function composition. If you reach for OOP, stop and reread this section.

## Module layout

```
logxform/
  init.lua            -- public API: run, transduce, compose, reduced, into
  protocol.lua        -- reducer protocol helpers, reduced wrapper
  compose.lua         -- compose(...)
  xforms/
    map.lua           -- mapping(f)
    filter.lua        -- filtering(pred)
    take.lua          -- take(n), take_while(pred)
    drop.lua          -- drop(n), drop_while(pred)
    dedupe.lua        -- dedupe(), dedupe_by(keyfn)
    decimate.lua      -- decimate(n) keep every nth
    partition.lua     -- partition_by(keyfn), partition_all(n)
    window.lua        -- sliding_window(n), tumbling_window(n)
    welford.lua       -- welford(), welford_window(n)
    bucket.lua        -- bucket_by_hour(), bucket_by(keyfn)
    zscore.lua        -- zscore_gate(threshold, min_n)
    spike.lua         -- pct_spike_gate(threshold)
    topk.lua          -- per_key_max(keyfn, threshold)
    tee.lua           -- tee(sink_a, sink_b, ...)
  sources/
    nats.lua          -- subscribe subject, yield decoded events
    mqtt.lua          -- subscribe topic
    sqlite_tail.lua   -- tail a table by rowid
    file_replay.lua   -- JSONL or CBOR-stream file
    iter.lua          -- wrap any Lua iterator
  sinks/
    nats.lua          -- publish to subject
    sqlite.lua        -- insert into table
    kv.lua            -- update KV with atomic check-and-set
    log.lua           -- write to log file
    collect.lua       -- collect into table (testing)
    callback.lua      -- invoke user fn (testing, integration)
  test/
    ...
```

## Reducer protocol (exact)

```lua
-- rf()        -> initial acc
-- rf(acc, x)  -> new acc (possibly reduced(...))
-- rf(acc)     -> final acc (flush; called exactly once)
```

`protocol.lua` provides a `reducer{init=fn, step=fn, complete=fn}` constructor that builds a multi-arity callable. Sinks built with this constructor compose uniformly. Default `init` returns `nil`; default `complete` returns `acc` unchanged.

## Welford transducer spec

`welford()` is stateful (upvalues hold `n, mean, M2`). On each step:

```
n     = n + 1
delta = x - mean
mean  = mean + delta / n
delta2 = x - mean
M2    = M2 + delta * delta2
```

Emit `{n=n, mean=mean, var=(n>1 and M2/(n-1) or 0), x=input}` downstream. The input-passthrough field `x` lets downstream gates see both the value and the running stats. For `n < 2`, var is 0 — let downstream gates enforce `min_n`.

`welford_window(N)` maintains a ring buffer of the last N samples and updates incrementally. Use Chan's parallel-Welford merge to subtract leaving samples without numerical drift. Acceptance test: drift after 10×N samples must be under 1e-9; if exceeded, fall back to recompute-from-buffer.

## Bucket aggregation spec

`bucket_by_hour(fold_fn, init_fn)` is stateful. Per step:

1. Compute `hour = math.floor(ev.ts / 3600000)`.
2. If `hour ~= current_hour` and `current_hour ~= nil`, emit the closed bucket `{hour=current_hour, agg=current_agg}` downstream via the wrapped reducer, then reset `current_agg = init_fn()`.
3. Set `current_hour = hour` and update `current_agg = fold_fn(current_agg, ev)`.

On `complete`, emit the open bucket if any, then call `complete` on the wrapped reducer. Defaults: `init_fn = function() return {count=0} end`, `fold_fn = function(a, _) a.count = a.count + 1; return a end`.

`bucket_by(keyfn, fold_fn, init_fn)` is the generalization for non-time keys (e.g. `kb_path`). Holds a table of open accumulators by key; never auto-closes — closure happens only on `complete`. Caller is responsible for memory if key cardinality is unbounded.

## Three anomaly modes (anomalisa parity)

1. **`zscore_gate(threshold, min_n)`** — expects `welford` upstream (input is a Welford emit). Forwards input only when `n >= min_n` and `math.abs(x - mean) / math.sqrt(var) > threshold`. Defaults: `threshold = 2.0`, `min_n = 3`.

2. **`pct_spike_gate(pct)`** — expects bucketed counts (input is a `bucket_by_hour` emit). Forwards bucket only when `(curr.count - prev.count) / prev.count > pct`. Holds previous count in upvalue. Defaults: `pct = 0.5`.

3. **`per_key_max(keyfn, threshold)`** — tracks per-key running max in a table upvalue. Forwards input when its `keyfn(ev)` value exceeds `threshold` and is a new per-key max.

All three are pure transducers — they do not publish or alert. Compose with a sink (typically `kv` with atomic check-and-set on alert key for dedupe, then `tee` to `nats` publish) to fire alerts.

## Source adapters

Each source module exports `open(opts) -> source` where `source` provides:

- `source:next() -> ev | nil` — blocks until next event or stream end.
- `source:close()` — release resources.

NATS source uses the existing FFI binding's callback-driven RPC pattern: bridge callbacks into a Lua coroutine queue that `next()` pops from. SQLite tail polls `rowid > last_seen` at configurable interval (default 500ms). File replay is a coroutine that yields decoded records.

## Sink adapters

Each sink module exports `open(opts) -> rf` where `rf` is a 3-arity reducer.

- `nats.open{subject, encode="cbor"}` — `step` publishes, `complete` flushes connection.
- `sqlite.open{db, table, columns}` — `step` inserts (prepared statement, parameter binding), `complete` commits and finalizes.
- `kv.open{store, key_fn, ttl_ms, dedupe=true}` — atomic check-and-set; with `dedupe=true`, step is a no-op if key present within TTL. Returns whether the set landed.
- `log.open{path, format="jsonl"}` — append-only file writer.
- `collect.open()` — `step` appends to internal table, `complete` returns table. For tests.

## Runner (reference implementation)

```lua
function logxform.run(source, xform, sink)
  local rf = xform(sink)
  local acc = rf()
  while true do
    local ev = source:next()
    if ev == nil then break end
    acc = rf(acc, ev)
    if protocol.is_reduced(acc) then
      acc = protocol.unreduced(acc)
      break
    end
  end
  acc = rf(acc)  -- complete
  source:close()
  return acc
end
```

`logxform.into(t, xform, iter)` is a convenience for tests: drives an iterator into a collected table.

## Integration with the knowledge base

Events with a `kb_path` field (ltree path string) are routed by `bucket_by(function(ev) return ev.kb_path end, ...)` for per-subtree aggregation. Anomaly alerts are written to the KV store under `alerts:<kb_path>:<ts>` (atomic CAS for dedupe) and simultaneously inserted into the PostgreSQL `kb_anomalies` table for ltree-indexed querying. Schema migration for `kb_anomalies` is out of scope — assume the table exists with columns `(ts bigint, kb_path ltree, kind text, payload jsonb)`.

## Test plan

1. **Protocol** — every transducer respects 3-arity contract; `complete` called exactly once; `reduced` short-circuits before subsequent steps.
2. **Determinism** — same input sequence + same xform produces byte-identical output across runs.
3. **Replay parity** — a recorded NATS stream replayed via `file_replay` produces the same alerts as the original live run.
4. **Welford correctness** — compare against a numpy reference on a 10k-sample fixture; max abs error under 1e-12.
5. **Bucket boundary** — events at `ts = hour_boundary - 1` and `ts = hour_boundary + 1` close the right bucket.
6. **Memory** — `welford_window(N)` holds bounded memory under streaming load. Measure with `collectgarbage("count")` before/after 1M events; growth under 1KB.
7. **Composition equivalence** — `compose(a, b, c)(rf)` produces same output as `a(b(c(rf)))` applied manually on a 1000-event fixture.
8. **Reduced propagation** — `take(10)` followed by an expensive map proves the map is never called for inputs past the 10th.

## Out of scope

- Distributed transducers across nodes (single-process only).
- Transducers over byte streams (CBOR decoding lives in source adapters).
- A bare-Cortex-M C port. That is a separate spec (`xform_t { void *state; step_fn step; flush_fn flush; xform_t *next; }` chain, preallocated, no closures). Reference this document as the semantic ground truth.
- Behavior tree integration. Drive Programs may consume `logxform` outputs via NATS subscription, but `logxform` does not import ChainTree runtime.
- Schema management for downstream PostgreSQL tables.
- Hot-reload of running pipelines.

## Hand-off notes for Claude Code

Start with `protocol.lua`, `compose.lua`, and `init.lua`. Then implement `xforms/map`, `xforms/filter`, `xforms/take` and write protocol tests against them — these exercise the full reducer contract without state. Then `xforms/welford` and `xforms/bucket`, which are the load-bearing stateful pieces. Sources and sinks last; use `iter` source and `collect` sink for all pre-NATS tests.

Stop and ask if a design choice from "Core decisions" appears to conflict with implementation reality. Do not silently work around it — those choices are load-bearing for the bare-MCU port and the live/replay parity guarantee.
