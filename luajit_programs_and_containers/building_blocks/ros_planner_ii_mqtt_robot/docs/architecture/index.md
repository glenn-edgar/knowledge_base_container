# Architecture overview

```
+-------------+      MQTT     +----------------+    Lua call    +-------------+
|  Planner    | <-----------> | mqtt_robot_main|--------------> | controller  |
| (Planner II)|   /rpc /link  |   tick loop    |                | (continuous |
+-------------+   /stream_bus +-------+--------+                |  motion)    |
                                      |                         +------+------+
                                      | ChainTree event_queue          |
                                      v                                |
                              +--------------+                         |
                              |  ChainTree   |  per-VN workers (Lua)   |
                              |   engine     |<------------------------+
                              +------+-------+                         |
                                     |                                 |
                                     | Lua function calls              | push_line/spline
                                     v                                 | request_stop
                              +--------------+   FFI call       +------v------+
                              |  robot_hal   |----------------> | libphysics  |
                              | (sim or real)|                  | (C plant)   |
                              +--------------+                  +-------------+
```

## Three time domains

| Domain | Tick rate | Owner |
|---|---|---|
| **Wall clock** | scaled by `SPEED_FACTOR` env / config | `usleep` at the bottom of the main tick loop |
| **Sim clock**  | advances by `phys_step(0.1)` per tick, inner-loop at 5 ms | C inside `libphysics.so` |
| **ChainTree tick** | 10 Hz (one event per active KB per tick) | `mqtt_robot_main.lua` event-queue drain |

`SPEED_FACTOR=5.0` means 1 second of sim wall-clocks in 0.2 s — useful for
batch random-path tests.

## What lives where

- **C** owns the inner loop: chassis dynamics, motor PID, pure-pursuit
  follower, segment FIFO, stations, tools, battery.
- **Lua** is the outer-loop dispatcher: parses RPC packets, decides whether a
  command flows directly to the C queue or has to wait for the robot to
  stop, runs ChainTree workers that poll C for completion, and emits MQTT
  stream events.
- **`mqtt_robot_main.lua`** is the only thing that owns the tick loop. It
  steps physics, then ticks the link client, then ticks the controller,
  then drains the ChainTree event queue.

## Why this split

The original design had Lua choreographing motion turn-by-turn. That meant
any segment boundary became a stop. Pushing the segment queue into C lets
pure-pursuit chase whatever's at the head of the queue and pop it when
done — the robot keeps moving across virtual-node boundaries for free.

Non-motion VNs (deliver_part, paint_sample, recharge, idle, …) are gated:
the controller holds them in `worker_queue` until `queue_depth() == 0` and
`is_stopped()` are both true.

See [Continuous-motion design](continuous-motion.md) for detail.

## Components

- [Main tick loop](main-loop.md) — `mqtt_robot_main.lua`
- [Controller](controller.md) — `robot_controller.lua`
- [HAL selector](hal.md) — `robot_hal.lua`
- [Continuous-motion design](continuous-motion.md) — invariants the system
  is built around
- [Control / physics pipe](pipe.md) — bidirectional FIFO between the
  ChainTree (10 Hz) and the physics inner loop (200 Hz → MCU)
