# Dwell-only workers

`pass_gate`, `inspection_scan`, `idle`, `operation` are all stubs that
just dwell for a fixed sim-time. None of them have a real sensor /
infrastructure model behind them yet — the system simulates the *time
cost* of these operations, not their effect.

| Worker | Dwell (sim s) | Bitmask on success |
|---|---|---|
| `pass_gate`       | 1.5 | 0x0F |
| `inspection_scan` | 1.0 | 0x01 (`reading_ready`) |
| `idle`            | 0.5 | 0x01 (`parked`) |
| `operation`       | 2.0 | 0x01 (`action_complete`) |

## Pattern

```lua
M.one_shot.WKR_X_INIT = function(h)
    h.blackboard.exec_active = true
    h.blackboard._x_start = h.blackboard._hal:sim_time()
end

M.main.WKR_X_MAIN = function(h, bf, n, eid)
    if eid ~= defs.CFL_TIMER_EVENT then return defs.CFL_CONTINUE end
    local bb = h.blackboard
    bb.worker_alive = true
    if bb._hal:sim_time() - bb._x_start >= DWELL then
        bb["worker_x.bitmask"] = MASK
        bb.worker_success = true
        return defs.CFL_DISABLE
    end
    return defs.CFL_CONTINUE
end
```

## Why these are stubs

Each of these has a real-world counterpart that would touch a sensor or
external system:

- **`inspection_scan`** would drive a range / colour / camera sensor
  and return readings.
- **`pass_gate`** would coordinate with gate infrastructure (request →
  open ack → cross → close).
- **`operation`** is a placeholder for site-specific "do a thing"
  packets.
- **`idle`** is short on purpose: it's the planner's "park here" packet,
  not a real wait.

These are the obvious places to extend the worker library; nothing in
the architecture forces them to stay dwell-only. The MQTT protocol can
already carry arbitrary `params` — workers just need to use them.
