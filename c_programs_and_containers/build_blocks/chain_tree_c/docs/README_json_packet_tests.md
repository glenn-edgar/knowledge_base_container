# JSON Packet Pipeline Tests

Integration tests for the JSON packet streaming subsystem. All tests use the binary image runtime with embedded `.ctb` images.

## Test Summary

| Test | Name | What It Tests |
|------|------|---------------|
| 0 | `json_telemetry_test` | Single generator → sink. Emit queues JSON text, sink parses and extracts x/y/z. |
| 1 | `json_multi_generator_test` | 3 generators (different sensors) → single sink. Verifies fan-in. |
| 2 | `json_verify_test` | 3 generators with different x values → verify boolean (range filter) → verified sink. |
| 3 | `json_drone_control_test` | JSON controlled nodes: 4 flight mode servers + sequencing client. |
| 4 | `json_drone_exception_test` | Same as test 3, but fly_straight raises exception → catch_all handler. |

## Running

```bash
cd dsl_tests/dsl_tests_c/json_packet_test
make                # builds all libraries + test binary
./main 0            # through ./main 4
```

## Architecture

### Pipeline

```
DSL Lua table → JSON text (node_dict) → CFL_JSON_EMIT → event queue → CFL_JSON_SINK → parse → user boolean
```

### CFL Functions

| DSL Name | Type | C Symbol | Purpose |
|----------|------|----------|---------|
| `CFL_JSON_EMIT` | one-shot | `cfl_json_emit_one_shot_fn` | Queue JSON text from node_dict |
| `CFL_JSON_SINK` | main | `cfl_json_sink_main_fn` | Parse JSON, call user boolean with `cfl_json_packet_t*` |
| `CFL_JSON_TRANSFORM` | main | `cfl_json_transform_main_fn` | Receive → user boolean modifies → emit output |
| `CFL_JSON_DISPATCH` | main | `cfl_json_dispatch_main_fn` | Route by "type" field via FNV-1a hash |
| `CFL_JSON_CONTROLLED_NODE_*` | main + one-shots | `cfl_json_controlled_node_*_fn` | Server: receive request, run column, send response |
| `CFL_JSON_CLIENT_CONTROLLED_NODE_*` | main + one-shots | `cfl_json_client_controlled_node_*_fn` | Client: send request, wait for response |

### DSL Helpers (`lua_dsl/lua_support/json_streaming.lua`)

- `asm_json_emit_oneshot(packet_data, event_column, event_name)` — emit JSON packet
- `asm_json_sink(aux_function, aux_data, event_name)` — consume JSON packets
- `asm_json_transform(aux_function, aux_data, input_event, output_event, output_column)` — transform pipeline
- `asm_json_dispatch(aux_data, event_name, output_event, routes, default_column)` — route by type
- `make_json_control_port(event_name)` — create port for controlled nodes
- `json_controlled_node(api_name, column_name, aux_fn, aux_data, req_port, resp_port)` — server
- `json_client_controlled_node(api_name, aux_fn, aux_data, req_port, resp_port)` — client

### User Boolean Interface

All user booleans receive `cfl_json_packet_t*` as event_data. Extract fields with:

```c
cfl_json_get_string(pkt, "topic", &topic);
cfl_json_get_float(pkt, "payload.x", &x);
cfl_json_get_int(pkt, "seq", &seq);
```

### Memory Model

- Emit `text_buffer`: arena-allocated, lives for node lifetime
- Sink: per-event JSON parse on heap, freed after boolean returns
- Server `response_text`: heap-allocated by user finalizer, freed by client after consumption
- Client `request_text`: heap-allocated at init, freed in client term

## Files

```
dsl_tests/dsl_tests_c/json_packet_test/
  json_packet_test.lua           — DSL: 5 test KBs
  main.c                         — test harness, function registration
  user_json_functions.c/h        — sink/verify/transform booleans (tests 0-2)
  user_json_drone_functions.c/h  — server monitors, client completions, finalizers (tests 3-4)
  Makefile                       — builds against runtime_binary, runtime_functions, json_packets, cbor_packets
```

## Dependencies

- `json_packets/` — cJSON-based JSON packet library
- `runtime_binary/` — binary image runtime
- `runtime_functions/` — CFL function implementations
- `cbor_packets/` — linked for CBOR function symbols in runtime_functions
