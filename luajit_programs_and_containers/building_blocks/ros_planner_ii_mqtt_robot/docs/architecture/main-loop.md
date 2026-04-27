# Main tick loop — `mqtt_robot_main.lua`

The single process the planner sees. Loads config, builds a HAL handle,
wires up a ChainTree runtime, instantiates `robot_controller` and
`link_client`, and then sits in a tick loop.

## Boot sequence

1. **Argument check** — needs a config path (`rover_1_config.json`).
2. **`mqtt_robot_config.load`** — parses the config, opens the MQTT
   transport, claims a slot in shared state, returns a populated `cfg`.
3. **HAL** — `robot_hal.new({ dir = robot_dir, seed = sim_seed })`. The HAL
   loads `physics_config.json` + `sim_map.json` from the same directory as
   the robot config and returns a handle wrapping `libphysics.so`.
4. **ChainTree remote** — `ct_loader.load(cfg.remote_json)` reads the
   compiled `remote.json` (built by `build.sh` from `remote_dsl.lua`).
   Worker functions get registered through `fn_registry` and validated
   before runtime start.
5. **`robot_controller.new`** — wraps the ChainTree handle, the transport,
   and the HAL into the dispatcher.
6. **`link_client.new`** — owns link lifecycle (announce → ack → live);
   when the planner is lost it calls `ctrl:abort_all()` to flush both the
   Lua worker queue and the C path queue.

## Per-tick work

```
while not blackboard.shutdown_requested do
    hal:step(0.1)                    -- 200 Hz inner loop, 100 ms sim slice
    lc:tick()                        -- link_client (announce/ack/heartbeat)
    if lc:is_live() then
        ctrl:tick()                  -- drain RPC, schedule workers, complete
        for kb in active_tests do    -- enqueue CFL_TIMER_EVENT for each
            event_queue += { kb.root_node, CFL_TIMER_EVENT }
        end
        drain event_queue            -- engine.execute_event → workers run
    end
    tick_count++
    if tick_count % 10 == 0 then     -- 1 Hz bitmask publish
        publish_bitmask(active_kb, raw_bitmask)
    end
    if sim_t - last_save_t >= 30 then save_energy() end
    usleep(WALL_SLEEP_US)            -- pacing knob; SPEED_FACTOR scales this
end
```

## Tunables

| Constant / env | Default | Effect |
|---|---|---|
| `CT_TICK_SIM_S` | `0.10` s | Sim time advanced per outer tick. Inner C step is `0.005` s, so each tick is 20 inner steps. |
| `SPEED_FACTOR` env or `cfg.speed_factor` | `1.0` | Wall sleep is `(0.10 / SPEED_FACTOR) * 1e6` μs. `5x` is the e2e default. |
| `BITMASK_PUBLISH_EVERY` | `10` ticks | 1 Hz bitmask publish onto `status_pub` topic. |
| `ENERGY_SAVE_SIM_S` | `30.0` s | Persist energy through `mqtt_robot_config.save_energy`. |

## Shutdown

A `packet_type = 255` RPC sets `blackboard.shutdown_requested = true`. The
loop exits, `link_client:shutdown()` runs, and `cfg.cleanup` writes the
final energy back. SIGPIPE is ignored at startup so a broker-dropped pipe
doesn't kill the process.
