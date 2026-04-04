# ChainTree C Robot — Continue From Here

## Session Summary (2026-04-03)

### What's built and working
- **Standalone C robot** (src/main_standalone.c): 12/12 tests pass through bridge, both JSON and CBOR wire
- **ChainTree C robot** (src/main.c): compiles, runs, processes CBOR packets through full ChainTree pipeline
- **DSL** (dsl/robot_mqtt_dsl.lua): 13 KBs (controller + 12 workers), each worker is a separate function
- **Manual CBOR tests pass**: all 12 packet types dispatch → worker start/complete → energy deduct

### Manual test proof (all working)
```
[main:inject] CBOR 28 bytes → controller node 0, event_id=0x0014
[ct:dispatch] pkt: {"packet_type":1,"seq":0,"test_id":1}
[ct:dispatch] activated worker KB 1 for packet_type=1
worker_init_check: started
worker_init_check: completed
[ct:completion] success pkt=1 energy=9950/10000
```

### Bridge test issue (NOT a codec problem)
- Both CBOR codecs (lua_cbor.c and cfl_cbor_packets) produce identical RFC 8949 bytes
- We captured the bridge output: valid CBOR, decodes correctly
- The bridge test fails with cJSON hook conflict inside cfl_cbor_sink_main_fn
- This happens ONLY when robot runs as background process in test script
- Manual mosquitto_pub with identical CBOR bytes works every time
- Root cause: likely cJSON global hook state corruption from test script environment, NOT our code

### Next session TODO

1. **Debug bridge test**: the cJSON hook conflict needs investigation
   - The CBOR sink calls reset_cjson_hooks() then cfl_json_parse calls set_heap()
   - When robot runs as background process, something corrupts the hook state
   - May need to bypass cfl_json_parse entirely and decode CBOR in user function

2. **Make main.c lean**: move robot-specific logic (energy, recharge) into DSL
   - Energy deduction → DSL push function (translates to MQTT or Thread)
   - Recharge → DSL handles energy restore
   - main.c becomes generic: poll MQTT, inject events, run tick, publish status

3. **DSL push function**: new DSL primitive for outbound MQTT/CBOR packets
   - `ct:asm_cbor_push(data, topic)` → translated to MQTT publish or Thread send
   - Worker KBs use push for heartbeat, ack, kb_done
   - Transport-agnostic: same DSL works for MQTT and Thread robots

4. **Individual worker complexity**:
   - worker_recharge: DSL push for energy restore notification
   - worker_path_spline: actual spline computation from command params
   - worker_pass_gate: RPC open/close gate sequence
   - Each worker grows independently — no shared template

### Key discoveries

**Function name hashing**: binary image hash tables use transformed names:
- Main: `lowercase(DSL_NAME) + "_main"` → e.g., "ctrl_completion_main_main"
- One-shot: `lowercase(DSL_NAME) + "_one_shot"` → e.g., "worker_term_one_shot"
- Boolean: `lowercase(DSL_NAME) + "_boolean"` → e.g., "cbor_rpc_dispatch_boolean"

**Blackboard types**: C binary image supports uint16, int32, uint32, float, uint64. No bool or string.

**CBOR codec compatibility**: lua_cbor.c and cfl_cbor_packets produce identical RFC 8949 CBOR. The decoder handles both correctly. The problem is cJSON hook conflict, not codec mismatch.

**DSL worker pattern**: `asm_wait_time` + `asm_terminate` replaces custom C main functions. Workers are now pure DSL with only init one-shots in C.

### File layout
```
ros_planner_ii_robot/
  libs/
    mqtt_pubsub.c/.h        ← streaming ring buffer pub/sub
    cbor_codec.c/.h          ← JSON↔CBOR codec
  src/
    main.c                   ← ChainTree main loop (current)
    main_standalone.c        ← standalone main loop (12/12 tests pass)
    robot_config.c/.h        ← JSON config loader
    robot_state.c/.h         ← energy, pose, worker state
    robot_mqtt.c/.h          ← MQTT transport, wire format, protocol messages
    robot_protocol.h         ← packet types, energy costs, durations
    robot_context.h          ← global context for user function MQTT access
    ct_user_functions.c/.h   ← ChainTree user functions
    json_extract.c/.h        ← lightweight JSON field extraction
  dsl/
    robot_mqtt_dsl.lua       ← DSL: controller + 12 individual worker KBs
    robot_handle_image.h     ← compiled binary image (recompile after DSL change)
    robot_handle_blackboard.h ← blackboard field offsets
    continue.md              ← this file
  config/
    robot_config.json        ← sample config (CBOR wire format)
  test_c_robot.sh            ← end-to-end test script
  Makefile                   ← builds with ChainTree libraries
```

### Build commands
```bash
# Recompile DSL (from chain_tree_c directory)
cd ../chain_tree_c
./s_build_json.sh ../ros_planner_ii_robot/dsl/robot_mqtt_dsl.lua ../ros_planner_ii_robot/dsl
./s_build_headers_binary.sh ../ros_planner_ii_robot/dsl/robot_mqtt_dsl.json ../ros_planner_ii_robot/dsl robot_handle

# Build robot
cd ../ros_planner_ii_robot
make clean && make

# Manual test (send CBOR packet)
./robot_main config/robot_config.json &
python3 -c "
import sys
buf = bytearray([0xa3, 0x6b]) + b'packet_type' + bytearray([0x01, 0x63]) + b'seq' + bytearray([0x00, 0x67]) + b'test_id' + bytearray([0x01])
sys.stdout.buffer.write(buf)
" | mosquitto_pub -t "moonbase/alpha/surface_ops/robots/rover_1/rpc" -s -q 1
```
