# Avro DSL Code Generator

A LuaJIT-based DSL for generating C header files from Avro-like schema definitions. Designed for embedded systems with minimal runtime overhead.

## Approach

### Design Philosophy

1. **Build-time generation** — LuaJIT reads schema definitions and emits static C headers. No runtime parsing or schema interpretation on the target.

2. **Zero-copy encoding** — Structs are memory-mapped directly to wire format. Encode/decode are simple `memcpy` operations.

3. **Per-record packet sizing** — Each record type gets its own packet structure sized exactly to its payload. No wasted space from max-sized buffers.

4. **User-controlled allocation** — Generated code operates on caller-provided pointers. User decides stack, static, heap, or arena allocation.

5. **No dispatch table** — User handles routing via switch statement or direct calls. Maximum flexibility, no imposed runtime structure.

6. **Position-based indexing** — Record index is determined by declaration order in the schema. Dense tables, no sparse 256-entry arrays.

### Wire Format

```
┌─────────┬─────────┬──────────────┐
│ index:8 │ len:16  │ payload      │
└─────────┴─────────┴──────────────┘
```

- `index` — Record type (0-based position in schema)
- `len` — Payload size in bytes
- `payload` — Struct data (little-endian, packed)

### Generated Output

Each `.lua` schema produces a `.h` file containing:

- Enum definitions
- Fixed-size array typedefs
- Struct definitions
- Record definitions
- Inline encode/decode functions
- Per-record packet types
- Record descriptors (for introspection)

## DSL Commands

### File Structure

```lua
require("avro_dsl").export_globals()

FILE("name")              -- Start schema file
INCLUDE("header.h")       -- Add #include directive
-- ... definitions ...
GENERATE()                -- Emit .h file
GENERATE("path/out.h")    -- Emit to specific path
```

### Enums

```lua
ENUM("sensor_state")
    VALUE("IDLE", 0)
    VALUE("SAMPLING", 1)
    VALUE("ERROR", 2)
END_ENUM()
```

Generates:
```c
typedef enum {
    SENSOR_STATE_IDLE = 0,
    SENSOR_STATE_SAMPLING = 1,
    SENSOR_STATE_ERROR = 2
} sensor_state_t;
```

### Fixed Arrays

```lua
FIXED("mac_addr", 6)      -- uint8_t[6]
FIXED("uuid", 16)         -- uint8_t[16]
```

Generates:
```c
typedef uint8_t mac_addr_t[6];
typedef uint8_t uuid_t[16];
```

### Fixed Strings

```lua
STRING("label", 16)       -- char[16]
```

Generates:
```c
typedef char label_t[16];
```

### Structs (Helper Types)

Non-dispatchable structures for composition:

```lua
STRUCT("packet_header")
    FIELD("device_id", "uint16")
    FIELD("seq", "uint16")
    FIELD("timestamp", "uint32")
END_STRUCT()
```

Generates:
```c
typedef struct {
    uint16_t device_id;
    uint16_t seq;
    uint32_t timestamp;
} packet_header_t;
```

### Records (Dispatchable Types)

Message types with index and packet wrapper:

```lua
RECORD("temp_reading")          -- index assigned by position
    FIELD("header", "packet_header")
    FIELD("celsius", "float")
    FIELD("state", "sensor_state")
END_RECORD()
```

Generates:
```c
// Data struct
typedef struct {
    packet_header_t header;
    float celsius;
    sensor_state_t state;
} temp_reading_t;

// Wire packet (exact size)
typedef struct {
    uint8_t  index;
    uint16_t length;
    temp_reading_t data;
} temp_reading_packet_t;

// Encode helper
static inline void temp_reading_packet_encode(
    temp_reading_packet_t* pkt, 
    const temp_reading_t* src);

// Raw encode/decode
static inline size_t temp_reading_encode(
    const temp_reading_t* src, uint8_t* buf);
static inline void temp_reading_decode(
    const uint8_t* buf, temp_reading_t* dst);
```

### Field Types

| DSL Type | C Type |
|----------|--------|
| `int8` | `int8_t` |
| `uint8` | `uint8_t` |
| `int16` | `int16_t` |
| `uint16` | `uint16_t` |
| `int32` | `int32_t` |
| `uint32` | `uint32_t` |
| `int64` | `int64_t` |
| `uint64` | `uint64_t` |
| `float` | `float` |
| `double` | `double` |
| `bool` | `bool` |
| `"type_name"` | `type_name_t` |

### Arrays

```lua
FIELD("temps", "float", 8)    -- float[8]
FIELD("readings", "temp_reading", 4)  -- temp_reading_t[4]
```

## C Runtime Interfacing

### Encoding a Packet

```c
#include "sensor_msgs.h"

// Allocate packet (stack, static, or dynamic)
temp_reading_packet_t pkt;

// Populate data
temp_reading_t data = {
    .header = { .device_id = 42, .seq = 1, .timestamp = 1000000 },
    .celsius = 23.5f,
    .state = SENSOR_STATE_SAMPLING,
};

// Encode (sets index and length automatically)
temp_reading_packet_encode(&pkt, &data);

// Send: &pkt, sizeof(pkt) or (3 + pkt.length)
transmit(&pkt, sizeof(pkt));
```

### Decoding a Packet

```c
// Receive into buffer
uint8_t buf[128];
size_t len = receive(buf, sizeof(buf));

// Read header
uint8_t index = buf[0];
uint16_t payload_len = *(uint16_t*)&buf[1];
uint8_t* payload = &buf[3];

// Dispatch by index
switch (index) {
    case 0: {  // temp_reading
        temp_reading_t* r = (temp_reading_t*)payload;
        handle_temp(r);
        break;
    }
    case 1: {  // pressure_reading
        pressure_reading_t* r = (pressure_reading_t*)payload;
        handle_pressure(r);
        break;
    }
}
```

### Direct Type Access

When type is known at compile time:

```c
// Encode
temp_reading_packet_t pkt;
temp_reading_packet_encode(&pkt, &data);

// Access data directly (no cast needed)
printf("temp = %.2f\n", pkt.data.celsius);
```

### Record Introspection

```c
// Iterate all record types
for (int i = 0; i < SENSOR_MSGS_RECORD_COUNT; i++) {
    const avro_record_desc_t* rec = &sensor_msgs_records[i];
    printf("index=%u name=%s size=%u\n", 
           rec->index, rec->name, rec->size);
}
```

### Memory Strategies

**Stack allocation:**
```c
void process(void) {
    temp_reading_packet_t pkt;  // automatic
    // ...
}
```

**Static allocation:**
```c
static temp_reading_packet_t pkt;  // single instance, reused
```

**Union for shared buffer:**
```c
static union {
    temp_reading_packet_t temp;
    pressure_reading_packet_t pressure;
    sensor_batch_packet_t batch;
} pkt_buf;
```

**Arena/pool:**
```c
temp_reading_packet_t* pkt = arena_alloc(&arena, sizeof(*pkt));
```

## Build

```bash
# Generate header from schema
luajit sensor_msgs.lua

# Compile
gcc -Wall -Wextra -O2 -o app main.c

# Or use make
make
```

## File Summary

| File | Purpose |
|------|---------|
| `avro_dsl.lua` | DSL generator |
| `sensor_msgs.lua` | Example schema |
| `sensor_msgs.h` | Generated header |
| `main.c` | Example usage |
| `Makefile` | Build rules |