# CBOR Packet Test — Status and Continuation Plan

## What's Done (5/5 tests passing)

```bash
cd dsl_tests/dsl_tests_c/cbor_packet_test
./main 0   # cbor_telemetry_test      — single generator → sink (CBOR wire)
./main 1   # cbor_multi_generator_test — 3 generators → single sink (CBOR wire)
./main 2   # cbor_verify_test          — verify boolean filters by x range (CBOR wire)
./main 3   # cbor_drone_control_test   — CBOR controlled nodes, 4 flight modes
./main 4   # cbor_drone_exception_test — exception in server → catch_all handler (CBOR wire)
```

All 5 tests mirror the JSON packet tests (`json_packet_test/`) but use CBOR on the wire.

### Infrastructure built

- **cbor_packets/** — CBOR encode/decode library + JSON↔CBOR transcoder (string keys, ~300 lines)
  - `cfl_cbor_encode.c` — RFC 8949 subset encoder (uint, int, float64, bool, null, text, map, array)
  - `cfl_cbor_decode.c` — Pull-style decoder with type accessors
  - `cfl_cbor_transcode.c` — JSON text ↔ CBOR bytes via cJSON (resets hooks to stdlib for safety)
  - 13 standalone tests in `cbor_packets/test/test_cbor.c`

- **runtime_functions/** — 6 new CFL functions:
  - `cfl_cbor_emit_one_shot_fn` — JSON text from DSL → CBOR bytes → event queue
  - `cfl_cbor_sink_main_fn` — CBOR bytes → JSON text → `cfl_json_packet_t` → user boolean
  - `cfl_cbor_controlled_node_{init,main,term}` — CBOR server (request CBOR→JSON for monitor, response JSON→CBOR for client)
  - `cfl_cbor_client_controlled_node_{init,main,term}` — CBOR client (request JSON→CBOR, response CBOR→JSON for completion)

- **lua_dsl/lua_support/cbor_streaming.lua** — DSL mixin:
  - `asm_cbor_emit_oneshot` — packet defined as Lua table, emitted as CBOR
  - `asm_cbor_sink` — receive CBOR, user boolean sees `cfl_json_packet_t`
  - `make_cbor_control_port`, `cbor_controlled_node`, `cbor_client_controlled_node`

- **Engine** — `CFL_FUNCTION_ID_CBOR_CONTROLLED_NODE_MAIN (8)` in both runtime variants, exception boundary support

### Key design decisions

- **String keys in CBOR** — no FNV-1a hash keys yet. Simpler for initial validation.
- **JSON→CBOR at emit, CBOR→JSON at sink** — user boolean functions receive identical `cfl_json_packet_t` interface as JSON tests. Zero user code changes.
- **cJSON hooks reset** — transcoder resets cJSON to stdlib malloc/free before each operation, since CFL JSON packets set global hooks to the CFL heap.

### Memory model

- Node structs on **arena** via `cfl_allocate_state` + `cfl_smart_arena_alloc` — safe for CFL_RESET loops
- CBOR byte buffers on **heap** via `cfl_heap_malloc_pointer` — pointers stored in arena struct
- Emit buffer: lives for node lifetime (event queue may hold reference after term)
- Server `cbor_buffer`: heap-allocated at init, freed in term after encoding response
- Client `cbor_request`: heap-allocated at init, freed in term
- Server `response_text`: heap-allocated by user finalizer, freed after CBOR encode in term
- Client response: decoded JSON text heap-allocated per-event, freed immediately after user boolean

### Bug fixes during development

- **JSON controlled node memory leaks** — fixed in this session:
  - `cfl_json_client_controlled_node_term` now frees `request_text` from heap
  - `cfl_json_client_controlled_node_main` now frees `response_text` (event_data) after user boolean consumes it

---

## Next: FNV-1a Integer Hash Keys

Replace string keys with FNV-1a u32 integer keys for the Thread mesh path where bandwidth matters:

```
Current:  {"command": "fly_straight", "distance": 100.0}    → CBOR with string keys
Next:     {0xA084479F: 0xB3F2C1D8, 0x12345678: 100.0}      → CBOR with hash keys
```

### Steps

1. **Field registry** (`cbor_packets/src/cfl_field_registry.c`) — FNV-1a u32 hash ↔ string name bidirectional lookup
2. **Hash-keyed transcoder** — `cfl_json_to_cbor_hashed()` / `cfl_cbor_hashed_to_json()` using field registry
3. **Registry population** — from DSL field definitions, collision-detected at generation time
4. **Integration tests** — round-trip with hash keys, verify decoded JSON matches original

### After that: Wire protocol

```
Serial packet: [msg_type:u8][seq:u8][src_node:u16][payload_len:u16][payload:cbor][crc16:u16]
```

For Thread mesh nodes (256KB flash / 64KB RAM) where string keys waste bandwidth.

---

## Build Commands

```bash
# DSL compilation
cd chain_tree_c
./s_build_json.sh dsl_tests/dsl_tests_c/cbor_packet_test/cbor_packet_test.lua dsl_tests/dsl_tests_c/cbor_packet_test
./s_build_headers_binary.sh dsl_tests/dsl_tests_c/cbor_packet_test/cbor_packet_test.json dsl_tests/dsl_tests_c/cbor_packet_test chaintree_handle

# Build and run
cd dsl_tests/dsl_tests_c/cbor_packet_test
make                    # builds all libraries + test binary
make run                # build + execute test 0
./main 0                # through ./main 4

# Standalone CBOR library tests
cd cbor_packets/test
make run                # 13 encode/decode/transcode tests

# Clean everything
cd dsl_tests/dsl_tests_c/cbor_packet_test
make clean-all          # cleans app + all libraries
```
