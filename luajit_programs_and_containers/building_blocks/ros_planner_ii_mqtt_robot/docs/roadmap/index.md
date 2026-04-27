# Roadmap

Sequenced design + implementation plans for the four work tracks
ahead. These are *plans*, not status; the ones that have shipped get
moved into `architecture/` or `physics/` as appropriate.

| Track | Plan | Goal |
|---|---|---|
| 2 | [Pipe implementation](pipe-implementation.md) | Spec the bidirectional FIFO between ChainTree (10 Hz) and physics (200 Hz → MCU). Spec is in [architecture/pipe.md](../architecture/pipe.md). |
| 1 | (TBD) ChainTree supervisor | Add supervisor KBs that own watchdog rollups, fault triage policy, energy budget, mission-progress book-keeping. |
| 3 | (TBD) Realistic actuator model | Replace `F = τ/r` shortcut with brushed-DC ODE: `Kt`, `Kv`, `R`, gearbox ratio + efficiency, wheel slip, battery sag. |
| 4 | (TBD) Robot config schema | Motor catalog + gearbox + wheel + battery descriptors so `rover_1` vs `rover_2` differ only by JSON. |

The natural ordering is **2 → 4 → 3 → 1**: the pipe lands first
(everyone consumes events afterward), then config gives the new
physics knobs to read, then the physics gets richer, then the
supervisor consumes the richer event stream.
