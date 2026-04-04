# JSON Packet Test — Continuation Plan

## What's Done (5/5 tests passing)

```bash
cd dsl_tests/dsl_tests_c/json_packet_test
./main 0   # json_telemetry_test      — single generator → sink
./main 1   # json_multi_generator_test — 3 generators → single sink
./main 2   # json_verify_test          — verify boolean filters by range
./main 3   # json_drone_control_test   — JSON controlled nodes, 4 flight modes
./main 4   # json_drone_exception_test — exception in server → catch_all handler
```

### Infrastructure built
- **json_packets/** — cJSON vendored, `libcfl_json_packets.a` (parse, build, extract, dispatch, serialize)
- **runtime_functions/** — 16 CFL functions: emit, sink, transform, dispatch, JSON controlled node server/client (init/main/term)
- **lua_dsl/lua_support/json_streaming.lua** — DSL mixin: `asm_json_emit_oneshot`, `asm_json_sink`, `asm_json_transform`, `asm_json_dispatch`, `make_json_control_port`, `json_controlled_node`, `json_client_controlled_node`
- **Engine** — `CFL_FUNCTION_ID_JSON_CONTROLLED_NODE_MAIN (7)` in both runtime variants, exception boundary support

### Key lessons (from tests 1-5)
- CFL naming pipeline: DSL `"CFL_JSON_SINK"` → stage3 `"cfl_json_sink_main"` → C `cfl_json_sink_main_fn`
- Smart allocators (`cfl_allocate_state` + `cfl_smart_arena_alloc`) survive CFL_RESET loops
- Heap for dynamic text (request/response), arena for fixed structs and buffers
- `cfl_additional_arena_alloc` for sub-allocations in same arena lifetime
- Server init must be called before accessing server arena state
- User booleans must guard `if (!event_data) return false;`

### Memory fixes (applied during CBOR development)
- Client term now frees `request_text` from heap (was leaking)
- Client main now frees `response_text` (event_data from server) after user boolean consumes it (was leaking)

## Completed: CBOR Packet Tests

The CBOR packet test suite (`cbor_packet_test/`) mirrors all 5 JSON tests with CBOR on the wire. See `cbor_packet_test/continue.md` for details.

---

## Next: CBOR Codec Library and JSON↔CBOR Transcoding

### Context

The ChainTree C robot (Pi Zero 2) and Thread bridge both need CBOR. The planner speaks JSON on NATS. Translation happens at the robot boundary:

```
Pi path:     Planner → NATS(JSON) → C Robot(JSON→CBOR internally)
Thread path: Planner → NATS(JSON) → Bridge → Dongle(JSON→CBOR) → Mesh(CBOR) → Robot
```

Both paths share the same CBOR codec and the same FNV-1a integer hash key scheme. Strings never appear on the wire in CBOR — only `u32` hash keys from the field registry.

### Wire protocol (from Thread bridge plan)

```
Serial packet: [msg_type:u8][seq:u8][src_node:u16][payload_len:u16][payload:cbor][crc16:u16]
```

CBOR payload uses FNV-1a u32 integer keys instead of string keys:
```
JSON:  {"command": "fly_straight", "distance": 100.0, "heading": 90.0}
CBOR:  {0xA084479F: 0xB3F2C1D8, 0x12345678: 100.0, 0x9ABCDEF0: 90.0}
       (hash keys)   (hash val)   (hash key)          (hash key)
```

### CBOR subset needed

The robot protocol uses a small subset of CBOR (RFC 8949):

| CBOR Type | Major | Use |
|-----------|-------|-----|
| Unsigned int | 0 | Hash keys, enum values, seq numbers, bit fields |
| Negative int | 1 | Rare — error codes |
| Byte string | 2 | Not needed initially |
| Text string | 3 | Not needed on wire (hashes replace strings) |
| Array | 4 | Ordered parameter lists |
| Map | 5 | All messages — integer-keyed maps |
| Float (half/single/double) | 7 | Sensor values, coordinates, energy |
| Bool (true/false) | 7 | Flags, status bits |
| Null | 7 | Optional fields |

No tags, no indefinite-length, no bignum, no nested depth beyond 2. This is ~300 lines of C.

### Step 1: CBOR Codec Library (`cbor_packets/`)

Mirror the `json_packets/` pattern:

```
cbor_packets/
├── Makefile
├── include/
│   └── cfl_cbor_packets.h       # public API
├── src/
│   ├── cfl_cbor_encode.c        # CBOR encoder (struct → bytes)
│   ├── cfl_cbor_decode.c        # CBOR decoder (bytes → struct)
│   └── cfl_cbor_packets.c       # high-level API (build, extract, transcode)
├── build/                        # generated
└── lib/
    └── libcfl_cbor_packets.a     # output
```

**API design** — same heap interface pattern as `cfl_json_packets`:

```c
/* ── Heap interface (same as json_packets) ── */
typedef cfl_json_heap_interface_t cfl_cbor_heap_interface_t;

/* ── Encode: build CBOR into caller buffer ── */
typedef struct cfl_cbor_encoder cfl_cbor_encoder_t;

cfl_cbor_encoder_t *cfl_cbor_encoder_create(uint8_t *buf, size_t buflen);
void     cfl_cbor_encode_map_begin(cfl_cbor_encoder_t *enc, unsigned count);
void     cfl_cbor_encode_uint(cfl_cbor_encoder_t *enc, uint32_t val);
void     cfl_cbor_encode_int(cfl_cbor_encoder_t *enc, int32_t val);
void     cfl_cbor_encode_float(cfl_cbor_encoder_t *enc, double val);
void     cfl_cbor_encode_bool(cfl_cbor_encoder_t *enc, bool val);
void     cfl_cbor_encode_null(cfl_cbor_encoder_t *enc);
void     cfl_cbor_encode_array_begin(cfl_cbor_encoder_t *enc, unsigned count);
size_t   cfl_cbor_encoder_finish(cfl_cbor_encoder_t *enc);

/* ── Decode: iterate CBOR bytes ── */
typedef struct cfl_cbor_decoder cfl_cbor_decoder_t;
typedef enum { CBOR_UINT, CBOR_INT, CBOR_FLOAT, CBOR_BOOL,
               CBOR_NULL, CBOR_MAP, CBOR_ARRAY, CBOR_END } cfl_cbor_type_t;

cfl_cbor_decoder_t *cfl_cbor_decoder_create(const uint8_t *buf, size_t len);
cfl_cbor_type_t cfl_cbor_decode_next(cfl_cbor_decoder_t *dec);
uint32_t cfl_cbor_decode_uint(cfl_cbor_decoder_t *dec);
int32_t  cfl_cbor_decode_int(cfl_cbor_decoder_t *dec);
double   cfl_cbor_decode_float(cfl_cbor_decoder_t *dec);
bool     cfl_cbor_decode_bool(cfl_cbor_decoder_t *dec);
unsigned cfl_cbor_decode_container_count(cfl_cbor_decoder_t *dec);

/* ── Transcode: JSON text ↔ CBOR bytes ── */
/* Requires a field registry for hash↔name translation */
typedef struct cfl_field_registry cfl_field_registry_t;

size_t cfl_json_to_cbor(const char *json_text, uint8_t *cbor_buf, size_t cbor_buflen,
                         const cfl_field_registry_t *registry);
char  *cfl_cbor_to_json(const uint8_t *cbor_buf, size_t cbor_len,
                         cfl_cbor_heap_interface_t *heap,
                         const cfl_field_registry_t *registry);
```

### Step 2: Field Registry (`cbor_packets/src/cfl_field_registry.c`)

Maps FNV-1a u32 hash ↔ string name. Used by the transcoder to convert JSON string keys to CBOR integer keys and back.

```c
typedef struct {
    uint32_t hash;
    const char *name;
} cfl_field_entry_t;

typedef struct cfl_field_registry {
    cfl_field_entry_t *entries;
    unsigned count;
    unsigned capacity;
} cfl_field_registry_t;

cfl_field_registry_t *cfl_field_registry_create(unsigned initial_capacity);
void     cfl_field_registry_destroy(cfl_field_registry_t *reg);
void     cfl_field_registry_add(cfl_field_registry_t *reg, const char *name);
const char *cfl_field_registry_lookup_name(const cfl_field_registry_t *reg, uint32_t hash);
uint32_t cfl_field_registry_lookup_hash(const cfl_field_registry_t *reg, const char *name);
```

The registry is populated at startup from the KB DSL field definitions. FNV-1a hashing uses the same algorithm already in the ChainTree binary image loader (`runtime_binary/src/cfl_fnv1a.c`).

### Step 3: Standalone Tests (`cbor_packets/test/`)

```
test/
├── Makefile
├── test_cbor_encode.c      # encode primitives, maps, arrays
├── test_cbor_decode.c      # decode and verify round-trip
├── test_cbor_transcode.c   # JSON→CBOR→JSON round-trip with field registry
└── test_field_registry.c   # registry add, lookup, collision detection
```

Tests to verify:
1. **Encode/decode round-trip** — uint, int, float, bool, null, nested map, array
2. **Size limits** — buffer overflow detection, max nesting depth
3. **Transcode** — JSON text → CBOR bytes → JSON text, with field registry
4. **Protocol messages** — encode/decode actual robot protocol messages:
   - Mission command: `{command, waypoints[], energy_budget}`
   - Telemetry: `{robot_id, pose{x,y,z,heading}, battery, timestamp}`
   - Ack: `{seq, status, error_code}`
   - Bitmask: `{raw, fields{heartbeat, active, error}}`

### Step 4: CFL Transcode Functions (`runtime_functions/`)

New CFL one-shot and main functions for JSON↔CBOR transcoding in the ChainTree pipeline:

```c
/* ── CFL_CBOR_ENCODE — one-shot: JSON text → CBOR bytes on arena ── */
void cfl_cbor_encode_one_shot_fn(void *handle, unsigned node_index);

/* ── CFL_CBOR_DECODE — one-shot: CBOR bytes → JSON text on arena ── */
void cfl_cbor_decode_one_shot_fn(void *handle, unsigned node_index);

/* ── CFL_CBOR_SINK — main: receives CBOR events, decodes, calls user boolean ── */
unsigned cfl_cbor_sink_main_fn(void *handle, unsigned bool_function_index,
    unsigned node_index, unsigned event_type, unsigned event_id, void *event_data);
```

These integrate with the existing streaming pipeline:
```
JSON emit → [CFL_CBOR_ENCODE] → CBOR on wire → [CFL_CBOR_DECODE] → JSON sink
```

### Step 5: Integration Test (Test 6) — JSON↔CBOR Round-Trip

```lua
-- Test 6: json_cbor_roundtrip_test
-- Generator emits JSON telemetry → encode to CBOR → decode back to JSON → verify sink
-- Field registry populated from DSL field names
-- Verify: decoded JSON matches original within float tolerance
```

### Step 6: Integration Test (Test 7) — CBOR Controlled Nodes

```lua
-- Test 7: cbor_drone_control_test
-- Same as test 4 but request/response go through CBOR encode/decode
-- Client sends JSON → encode to CBOR → server receives CBOR → decode to JSON
-- Server sends JSON response → encode → client receives → decode → verify
```

This proves the full path: planner sends JSON, robot processes CBOR internally, response comes back as JSON.

---

## Build Order and Dependencies

```
Step 1: cbor_packets/ library        ← no dependencies (self-contained)
Step 2: field registry               ← FNV-1a from runtime_binary/src/cfl_fnv1a.c
Step 3: standalone tests             ← steps 1-2
Step 4: CFL transcode functions      ← steps 1-2 + runtime_functions pattern
Step 5: integration test (round-trip) ← steps 1-4
Step 6: integration test (controlled) ← step 5 + existing JSON controlled nodes
```

Steps 1-2 can be done in a single session. Step 3 validates before integrating.
Steps 4-6 follow the same pattern as the JSON packet tests.

---

## Design Decisions

### Why not vendor TinyCBOR or QCBOR?

The protocol needs ~10 CBOR types with max nesting depth 2. A full CBOR library (TinyCBOR: 3K SLOC, QCBOR: 8K SLOC) is overkill. A focused ~300 line encoder/decoder:
- Fits the 32KB ARM Cortex-M target
- No external dependencies beyond libc
- EXCEPTION-based error handling (matches ChainTree conventions)
- Heap interface pattern (matches `cfl_json_packets`)
- Avoids pulling in CMake, test frameworks, or build system conflicts

### Why integer hash keys instead of string keys?

Thread mesh nodes have 256KB flash / 64KB RAM (XIAO MG24). String keys waste bandwidth and RAM. FNV-1a u32 hashes are:
- 4 bytes per key vs 10-20 bytes per string key
- Computed at compile time by the DSL (same as ChainTree binary image)
- Collision-detected at generation time (same as CTB1 format)
- Already used throughout ChainTree (function lookup, event routing, blackboard fields)

### Why field registry instead of hardcoded hash constants?

The transcoder needs bidirectional lookup (hash→name for CBOR→JSON, name→hash for JSON→CBOR). A registry:
- Generated from the same DSL that defines the protocol
- Shared between encode and decode paths
- Extensible — new fields added to DSL automatically appear in registry
- Testable — collision detection at registry build time

### Where does the registry come from at runtime?

Two options depending on deployment:
1. **Embedded**: Registry baked into the `.ctb` binary image as a new section (like BBRD/CREC)
2. **Dynamic**: Registry populated from NATS KV at startup (robot reads field definitions from KB exporter)

Start with embedded (simpler, no NATS dependency for testing). Add dynamic later for production.

---

## File Locations After Completion

```
chain_tree_c/
  cbor_packets/
    include/cfl_cbor_packets.h
    include/cfl_field_registry.h
    src/cfl_cbor_encode.c
    src/cfl_cbor_decode.c
    src/cfl_cbor_packets.c
    src/cfl_field_registry.c
    test/test_cbor_encode.c
    test/test_cbor_decode.c
    test/test_cbor_transcode.c
    test/test_field_registry.c
    test/Makefile
    Makefile
    lib/libcfl_cbor_packets.a

  runtime_functions/
    include/cfl_cbor_functions.h      (new)
    src/cfl_cbor_functions.c          (new)
    src/cfl_function_loader.c         (add CBOR registrations)

  lua_dsl/lua_support/
    json_streaming.lua                (add CBOR DSL helpers)

  dsl_tests/dsl_tests_c/json_packet_test/
    json_packet_test.lua              (add tests 6-7)
    user_json_cbor_functions.c/h      (new user functions)
    main.c                            (register CBOR user functions)
    Makefile                          (link cbor_packets)
```
