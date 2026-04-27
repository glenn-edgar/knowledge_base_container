# Init-check worker

The shortest worker in the system. Three ticks, set a status bitmask,
declare success.

## Behaviour

```lua
M.main.WKR_INIT_CHECK_MAIN = function(h, bf, n, eid)
    if eid ~= CFL_TIMER_EVENT then return CFL_CONTINUE end
    local bb = h.blackboard
    bb.worker_alive = true
    if bb.exec_start then
        bb.exec_start = false; bb.exec_active = true
        bb.ticks_remaining = 3
    end
    if bb.exec_active then
        bb.ticks_remaining = bb.ticks_remaining - 1
        if bb.ticks_remaining <= 0 then
            bb.exec_active = false
            bb["worker_init_check.bitmask"] = 0x0F
            bb.worker_success = true
            return CFL_DISABLE
        end
    end
end
```

`bitmask = 0x0F` means *battery | motors | sensors | comms* — all four
self-check bits pass. There's no real check today; it's the
"yes I'm alive" handshake the planner uses to confirm the robot can
start receiving real packets.

## Why three ticks

Just enough to exercise the watchdog, the heartbeat counter, and the
completion path before the next packet arrives. A zero-tick complete
would fast-path so quickly that the harness might never see a
heartbeat for the worker.
