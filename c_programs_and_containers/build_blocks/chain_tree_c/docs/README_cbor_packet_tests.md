# CBOR Packet Pipeline Tests

Integration tests for CBOR packet streaming. Mirrors the JSON packet tests but with CBOR on the wire. User boolean functions receive identical `cfl_json_packet_t*` — the CBOR encode/decode is transparent.

## Test Summary

| Test | Name | What It Tests |
|------|------|---------------|
| 0 | `cbor_telemetry_test` | Single generator → sink. JSON→CBOR at emit, CBOR→JSON at sink. |
| 1 | `cbor_multi_generator_test` | 3 generators → single sink. Verifies fan-in with CBOR wire. |
| 2 | `cbor_verify_test` | 3 generators → verify boolean (range filter) → verified sink. CBOR wire. |
| 3 | `cbor_drone_control_test` | CBOR controlled nodes: 4 flight mode servers + sequencing client. |
| 4 | `cbor_drone_exception_test` | Same as test 3, but fly_straight raises exception → catch_all handler. |

## Running

```bash
cd dsl_tests/dsl_tests_c/cbor_packet_test
make                # builds all libraries + test binary
./main 0            # through ./main 4
```

## Architecture

### Pipeline

```
DSL Lua table → JSON text (node_dict) → CFL_CBOR_EMIT (JSON→CBOR) → event queue (CBOR bytes)
    → CFL_CBOR_SINK (CBOR→JSON) → parse → user boolean (cfl_json_packet_t*)
```

### CFL Functions

| DSL Name | Type | C Symbol | Purpose |
|----------|------|----------|---------|
| `CFL_CBOR_EMIT` | one-shot | `cfl_cbor_emit_one_shot_fn` | JSON text → CBOR bytes → event queue |
| `CFL_CBOR_SINK` | main | `cfl_cbor_sink_main_fn` | CBOR bytes → JSON text → parse → user boolean |
| `CFL_CBOR_CONTROLLED_NODE_*` | main + one-shots | `cfl_cbor_controlled_node_*_fn` | Server: CBOR request → JSON for monitor |
| `CFL_CBOR_CLIENT_CONTROLLED_NODE_*` | main + one-shots | `cfl_cbor_client_controlled_node_*_fn` | Client: JSON → CBOR request, CBOR → JSON response |

### DSL Helpers (`lua_dsl/lua_support/cbor_streaming.lua`)

- `asm_cbor_emit_oneshot(packet_data, event_column, event_name)` — emit CBOR-encoded packet
- `asm_cbor_sink(aux_function, aux_data, event_name)` — consume CBOR packets
- `make_cbor_control_port(event_name)` — create port for controlled nodes
- `cbor_controlled_node(api_name, column_name, aux_fn, aux_data, req_port, resp_port)` — server
- `cbor_client_controlled_node(api_name, aux_fn, aux_data, req_port, resp_port)` — client

### CBOR Library (`cbor_packets/`)

Minimal CBOR codec (RFC 8949 subset) with JSON↔CBOR transcoding:

- **Encoder**: uint, int, float64, bool, null, text string, map, array. Writes to caller buffer.
- **Decoder**: Pull-style iteration with typed accessors. EXCEPTION on malformed input.
- **Transcoder**: `cfl_json_text_to_cbor()` / `cfl_cbor_to_json_text()`. Uses cJSON internally, resets hooks to stdlib.
- **String keys**: Current implementation uses text string keys (not FNV-1a hash keys).
- **13 standalone tests** in `cbor_packets/test/test_cbor.c`.

### Memory Model

- Node structs on **arena** via `cfl_allocate_state` + `cfl_smart_arena_alloc` — safe for CFL_RESET
- CBOR byte buffers on **heap** — pointers stored in arena struct
- Emit buffer: heap-allocated once, reused across reset cycles, never freed by term (event queue lifetime)
- Server `cbor_buffer`: heap-allocated at init, freed in term after encoding response
- Client `cbor_request`: heap-allocated at init, freed in term
- Per-event decoded JSON: heap-allocated, freed immediately after user boolean

### User Boolean Interface

Identical to JSON tests — user booleans receive `cfl_json_packet_t*`:

```c
bool cbor_telem_sink_boolean_fn(void *handle, unsigned node_index,
    unsigned event_type, unsigned event_id, void *event_data) {
    if (!event_data) return false;
    cfl_json_packet_t *pkt = (cfl_json_packet_t *)event_data;

    cfl_json_get_string(pkt, "topic", &topic);
    cfl_json_get_float(pkt, "payload.x", &x);
    // ...
}
```

For controlled node monitors, event_data is `char*` JSON text (decoded from CBOR):

```c
bool cbor_fly_straight_monitor_boolean_fn(void *handle, ..., void *event_data) {
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;
    cfl_cbor_server_controlled_node_t *server = cfl_get_cbor_server_node(rt, node_index);
    cfl_json_packet_t *pkt = cfl_json_parse(&server->heap_iface, (const char *)event_data, -1);
    // extract fields, free pkt
}
```

## Files

```
dsl_tests/dsl_tests_c/cbor_packet_test/
  cbor_packet_test.lua           — DSL: 5 test KBs
  main.c                         — test harness, function registration
  user_cbor_functions.c/h        — sink/verify booleans (tests 0-2)
  user_cbor_drone_functions.c/h  — server monitors, client completions, finalizers (tests 3-4)
  Makefile                       — builds against all libraries

cbor_packets/
  include/cfl_cbor_packets.h     — public API
  src/cfl_cbor_encode.c          — encoder
  src/cfl_cbor_decode.c          — decoder
  src/cfl_cbor_transcode.c       — JSON↔CBOR transcoder
  test/test_cbor.c               — 13 standalone tests
  lib/libcfl_cbor_packets.a      — output library
```

## Dependencies

- `cbor_packets/` — CBOR codec + transcoder
- `json_packets/` — cJSON (used by transcoder and for user boolean packet parsing)
- `runtime_binary/` — binary image runtime
- `runtime_functions/` — CFL function implementations
