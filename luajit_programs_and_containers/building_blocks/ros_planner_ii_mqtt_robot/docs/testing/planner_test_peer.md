# `planner_test_peer.lua`

Reusable mission-side peer extracted from `test_mock_planner.lua` +
`test_random_paths.lua`. Each new e2e scenario is now ~30 lines on
top of this module.

## Shape

```lua
local peer = require("planner_test_peer").new{
    robot   = "rover_1",
    site    = "moonbase.alpha.surface_ops",
    host    = "localhost", port = 1883,
    verbose = false,
}

-- bring rover live (handshake)
peer:bring_robot_live(15)

-- dispatch
peer:send_command{ packet_type = T.PATH_LINE, params = {...} }
local test_ids = peer:send_batch(commands)   -- assigns seq + test_id

-- collect
local stats = peer:wait_for_dones(#commands, 60)
-- stats = { ack=N, hb=N, done=N, ok=N, fail=N }

peer:close()
```

## Lifecycle methods

| Method | Blocks? | Purpose |
|---|---|---|
| `:bring_robot_live(timeout_s)` | yes (until live) | Wait for `link_announce`, send `link_bridge_ack`, wait for `link_confirm`. Returns `true` on success, `nil, errstring` on timeout. |
| `:pump_for(duration_s)` | yes | Long-running keepalive (heartbeats every 1s). Used by `test_mock_planner` to keep a live link while another script runs scenarios. |
| `:pump_until(predicate, timeout_s)` | yes | Generic pump-until loop: ticks until `predicate(self)` returns truthy, or timeout. |
| `:tick(poll_ms)` | no | Single-step pump — drains one batch of MQTT messages, sends a heartbeat if due. Use this when composing peers (e.g. driving multiple rovers from one process). |
| `:close()` | no | Disconnect + destroy the MQTT client. |

## Dispatch methods

| Method | Purpose |
|---|---|
| `:send_command(cmd)` | Publish ONE command packet to `…/rpc`. |
| `:send_batch(cmds, opts)` | Assign sequential `seq` + `test_id` (default base 2000) to each command in the table, dispatch all in burst. Returns the list of test_ids in order. |

## Collection methods

| Method | Purpose |
|---|---|
| `:wait_for_dones(n, timeout_s)` | Pump until N `kb_done` events received (or timeout). Returns the stats table. |

Direct access to collected events:

- `peer.dones` — list of done records ordered by arrival
- `peer.ack_count`, `peer.hb_count` — running counters
- `peer.live`, `peer.capabilities_seen` — handshake state booleans

## Optional callbacks

For streaming-aware tests, pass these to `new`:

```lua
local peer = require("planner_test_peer").new{
    robot = "rover_1",
    on_done      = function(done) print("done:", done.test_id, done.success) end,
    on_telemetry = function(hb)   ... end,
}
```

If `on_done` is set, the default verbose print is suppressed (the
callback is the renderer).

## Composability

The peer is intentionally synchronous and tickable, NOT
threaded/coroutine-based, so it composes cleanly:

```lua
-- Drive two rovers from one process
local peer_1 = ...new{ robot = "rover_1", ... }
local peer_2 = ...new{ robot = "rover_2", ... }
peer_1:bring_robot_live(15)
peer_2:bring_robot_live(15)

while not done do
    peer_1:tick(50)
    peer_2:tick(50)
    -- ... orchestration logic ...
end
```

## Why it lives in `dongle_base`

The peer is generic — it speaks Planner II MQTT, not anything
class-specific. Class images inherit it via `dongle_base` so any
scenario test runs from inside the container too:

```bash
docker exec rover_1 luajit /opt/dongle_base/test_harness/test_random_paths.lua \
    --self-host --robot rover_1 --mode paths_only --count 3
```
