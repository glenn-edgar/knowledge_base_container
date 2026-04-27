# Running

## Manual launch

```
luajit mqtt_robot_main.lua rover_1_config.json
```

Needs:

- `LUA_PATH` containing the LuaJIT pipeline + the MQTT base + this dir:
  ```
  LUA_PATH="$BB/chain_tree_luajit/lua_dsl/luajit_pipeline/?.lua;\
           $BB/knowledge_base/mqtt/?.lua;\
           $BB/knowledge_base/mqtt/lib/?.lua;\
           ./?.lua;;"
  ```
- `LD_LIBRARY_PATH` containing the MQTT base (where the linked .so for
  `mqtt_pubsub` lives).
- A reachable MQTT broker at `mqtt_host:mqtt_port`.

The wrapper `building_blocks/ros_scripts/start_robot.sh` handles
`LUA_PATH` / `LD_LIBRARY_PATH` for you.

## Speed factor

Wall pacing is `0.10 / SPEED_FACTOR` seconds per ChainTree tick. So:

| `SPEED_FACTOR` | Wall sleep per tick | Sim:wall ratio |
|---|---|---|
| `1.0`  | 100 ms | 1× real time |
| `5.0`  | 20 ms  | 5× real time (tests default) |
| `10.0` | 10 ms  | 10× — used by `run_tests.sh --speed 10` (also default there) |

Setting `SPEED_FACTOR=0` is allowed (no sleep) but then the loop will
saturate the CPU. Don't.

## Energy persistence

Every 30 sim seconds, the controller pushes the current energy reading
through `link_client:set_energy` and `mqtt_robot_config.save_energy`.
On a clean shutdown (`packet_type = 255` or SIGINT propagated through
the host), `cfg.cleanup(final_energy.remaining)` writes the final value
back to the slot store. On a kill-9 the reading is up to 30 s stale.

## Live tail

The robot is talkative on stderr. Useful filters:

```
grep -E "MQTT_ROBOT|VN\[|DONE|FAULT" /tmp/rover_1.log
grep "→ live" /tmp/rover_1.log               # link established
grep "planner lost" /tmp/rover_1.log         # link drop
```

## Multiple robots, one broker

Each robot has its own `rover_<id>_config.json` and its own topic
prefix. Run as many as the broker can keep up with — there's no global
state shared between them.
