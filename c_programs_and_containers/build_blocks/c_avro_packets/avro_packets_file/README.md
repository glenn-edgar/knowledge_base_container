# AVRO DSL - Embedded Schema Code Generator

## Purpose

**avro_dsl.lua** is a LuaJIT-based code generator that produces C data structures and binary schema files from a simple declarative DSL. It is designed for **embedded systems** and **distributed control networks** where:

- Memory is constrained (32KB - 8GB targets)
- Data must be exchanged over IP sockets between heterogeneous nodes
- Schema validation must occur without string comparisons
- Binary schemas must be loadable at runtime or linked into firmware
- Wire formats must be platform-independent (fixed endianness, no pointers)

The generator replaces traditional approaches like Protobuf or Avro with a lightweight, embedded-friendly solution that produces:

1. **C header files** with typed structs, enums, and wire packet helpers
2. **Binary schema files** loadable at runtime for dynamic validation
3. **Embedded binary headers** for linking schema data directly into firmware

All outputs use **FNV-1a 32-bit hashes** for schema identification, enabling fast validation without string operations - critical for real-time embedded systems.

---

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [DSL Reference](#dsl-reference)
- [Output Files](#output-files)
- [Wire Protocol](#wire-protocol)
- [Binary Schema Format](#binary-schema-format)
- [Test Program](#test-program)
- [Building](#building)
- [32-bit Cross Compilation](#32-bit-cross-compilation)

---

## Installation

### Requirements

- **LuaJIT** (recommended) or **Lua 5.3+**
- **GCC** or compatible C compiler
- Optional: `arm-linux-gnueabihf-gcc` and `qemu-user` for 32-bit ARM testing

### Files

| File | Description |
|------|-------------|
| `avro_dsl.lua` | Code generator module |
| `test_schema.lua` | Example schema definition |
| `main.c` | Comprehensive test program |
| `Makefile` | Build automation |

---

## Quick Start

### 1. Define a Schema

Create a schema file (e.g., `my_schema.lua`):

```lua
#!/usr/bin/env luajit
local avro = require("avro_dsl")
avro.export_globals()

FILE("my_schema")

INCLUDE_BRACKET("stdint.h")
INCLUDE_BRACKET("stdbool.h")

ENUM("status")
    VALUE("OK", 0)
    VALUE("ERROR", 1)
END_ENUM()

RECORD("message")
    FIELD("id", "uint16")
    FIELD("status", "status")
    FIELD("value", "float")
END_RECORD()

GENERATE_ALL("my_schema")
```

### 2. Generate Outputs

```bash
luajit my_schema.lua
```

This produces:
- `my_schema.h` - C header with types and wire packet helpers
- `my_schema_bin.h` - Embedded binary schema as `const uint8_t[]`
- `my_schema.bin` - Loadable binary schema file

### 3. Use in C Code

```c
#include "my_schema.h"

// Create and send a packet
message_packet_t pkt;
message_t* msg = message_packet_init(&pkt, NODE_ID);
msg->id = 42;
msg->status = STATUS_OK;
msg->value = 3.14f;

send(sock, &pkt, sizeof(pkt), 0);

// Receive and validate
recv(sock, &pkt, sizeof(pkt), 0);
const message_t* data = message_packet_verify(&pkt);
if (data) {
    printf("Received id=%d, value=%f\n", data->id, data->value);
}
```

---

## DSL Reference

### File Definition

| Command | Description |
|---------|-------------|
| `FILE(name)` | Start a new schema file definition |
| `INCLUDE_BRACKET(header)` | Add `#include <header>` |
| `INCLUDE_STRING(header)` | Add `#include "header"` |

### Type Definitions

#### Enumerations

```lua
ENUM("name")
    VALUE("LABEL1", 0)
    VALUE("LABEL2", 1)
END_ENUM()
```

Generates:
```c
typedef enum {
    NAME_LABEL1 = 0,
    NAME_LABEL2 = 1
} name_t;
```

#### Fixed-Size Arrays

```lua
FIXED("mac_addr", 6)    -- 6-byte array
FIXED("uuid", 16)       -- 16-byte array
```

Generates:
```c
typedef uint8_t mac_addr_t[6];
typedef uint8_t uuid_t[16];
```

#### Fixed-Length Strings

```lua
STRING("sensor_name", 32)   -- 32-char buffer with length tracking
```

Generates:
```c
typedef struct {
    char buffer[32];
    uint16_t length;
    uint16_t max_length;
} sensor_name_t;
```

#### Opaque Pointers

```lua
POINTER("user_data")    -- void* wrapper for runtime data
```

Generates:
```c
typedef struct {
    void *ptr;
} user_data_t;
```

**Note:** Pointer types are excluded from wire packets (not socket-safe).

#### Structures (Non-Record)

```lua
STRUCT("config")
    FIELD("rate", "uint16")
    FIELD("enabled", "bool")
END_STRUCT()
```

Generates a typedef but **no wire packet helpers**.

#### Records (Wire-Transmittable)

```lua
RECORD("sensor_reading")
    FIELD("sensor_id", "uint16")
    FIELD("value", "float")
    FIELD("timestamp", "uint32")
END_RECORD()
```

Generates:
- `sensor_reading_t` struct
- `sensor_reading_packet_t` wire packet struct
- `sensor_reading_packet_init()` helper
- `sensor_reading_packet_verify()` helper

### Field Types

| DSL Type | C Type | Size |
|----------|--------|------|
| `int8` | `int8_t` | 1 |
| `uint8` | `uint8_t` | 1 |
| `int16` | `int16_t` | 2 |
| `uint16` | `uint16_t` | 2 |
| `int32` | `int32_t` | 4 |
| `uint32` | `uint32_t` | 4 |
| `int64` | `int64_t` | 8 |
| `uint64` | `uint64_t` | 8 |
| `float` | `float` | 4 |
| `double` | `double` | 8 |
| `bool` | `bool` | 1 |
| `<enum_name>` | `<enum_name>_t` | 4 |
| `<fixed_name>` | `<fixed_name>_t` | varies |
| `<struct_name>` | `<struct_name>_t` | varies |

### Array Fields

```lua
FIELD("samples", "float", 10)    -- float samples[10]
FIELD("data", "uint8", 256)      -- uint8_t data[256]
```

### Generation Commands

| Command | Description |
|---------|-------------|
| `GENERATE(path)` | Generate `.h` file only |
| `GENERATE_BINARY(path)` | Generate `.bin` file only |
| `GENERATE_BINARY_HEADER(path)` | Generate `_bin.h` file only |
| `GENERATE_ALL(base_path)` | Generate all three outputs |

---

## Output Files

### C Header File (`.h`)

Contains:

| Section | Description |
|---------|-------------|
| **File Metadata** | `SCHEMA_HASH`, `RECORD_COUNT`, `SCHEMA_FILE` defines |
| **Enums** | Typedef'd enumerations with prefixed values |
| **Fixed Arrays** | `typedef uint8_t name_t[size]` |
| **Fixed Strings** | Struct with buffer, length, max_length |
| **Pointers** | `void*` wrapper structs |
| **Structs** | Non-record structures |
| **Records** | Wire-transmittable structures |
| **Wire Header** | 16-byte packet header type |
| **Wire Packets** | Per-record packet types |
| **Packet Helpers** | `_packet_init()` and `_packet_verify()` functions |
| **Dispatch Helper** | Generic `_packet_dispatch()` function |
| **Size Arrays** | `record_sizes[]` and `packet_sizes[]` |

### Binary Header File (`_bin.h`)

```c
#define SCHEMA_HASH    0xXXXXXXXXU
#define BIN_SIZE       NNN
#define RECORD_COUNT   N

static const uint8_t schema_bin[NNN] = {
    0xXX, 0xXX, ...
};
```

Use for linking schema data directly into firmware without filesystem access.

### Binary Schema File (`.bin`)

Loadable at runtime via `fread()` or received over network. Format:

| Offset | Size | Field |
|--------|------|-------|
| 0 | 4 | Magic (`0x41565244` = "AVRD") |
| 4 | 2 | Version (1) |
| 6 | 2 | Record count |
| 8 | 4 | Schema hash (FNV-1a of filename) |
| 12 | 4 | Total size |
| 16 | N | Schema name (null-terminated) |
| ... | ... | Enums, fixed, strings, pointers, structs, records |

---

## Wire Protocol

### Cross-Platform Compatibility

The generator produces **two struct variants** for each record:

| Type | Purpose | Packing |
|------|---------|---------|
| `record_t` | Native use on local platform | Natural alignment (may have padding) |
| `record_wire_t` | Wire transmission | `#pragma pack(1)` (no padding) |

**Why two types?**
- Native structs are faster to access (aligned memory)
- Wire structs have identical byte layout on all platforms
- Conversion helpers copy between them

```c
// Native struct (fast, may have padding)
sensor_reading_t native = { .sensor_id = 42, .value = 23.5f };

// Convert to wire format for transmission
sensor_reading_wire_t wire;
sensor_reading_to_wire(&native, &wire);
send(sock, &pkt, sizeof(pkt), 0);

// Receive and convert back
sensor_reading_from_wire(&received_wire, &native);
```

### Wire Header (16 bytes, packed)

```c
typedef struct {
    double      timestamp;     // 8: Set by transport layer
    uint32_t    schema_hash;   // 4: FNV-1a hash of schema filename
    uint16_t    seq;           // 2: Sequence number
    uint8_t     source_node;   // 1: Originating node ID
    uint8_t     index;         // 1: Record type index (0-based)
} schema_wire_header_t;
```

**Design rationale:**
- **No pointers** - safe for socket transmission
- **Hash-based identification** - no string comparisons
- **Fixed size** - predictable memory layout
- **Naturally aligned** - no packing needed, works on all architectures

### Packet Structure

```c
#pragma pack(push, 1)
typedef struct {
    schema_wire_header_t header;  // 16 bytes
    record_wire_t        data;    // Packed, variable size
} record_packet_t;
#pragma pack(pop)
```

**Static assertions** verify sizes at compile time:
```c
_Static_assert(sizeof(sensor_data_wire_header_t) == 16, "Wire header must be 16 bytes");
_Static_assert(sizeof(sensor_reading_wire_t) == 14, "sensor_reading_wire_t size mismatch");
```

If sizes don't match expected values, compilation fails - catching cross-platform issues early.

### Send Pattern

```c
record_packet_t pkt;
record_t* data = record_packet_init(&pkt, my_node_id);

// Fill in data fields
data->field1 = value1;
data->field2 = value2;

// Transport layer sets timestamp and seq before sending
pkt.header.timestamp = get_time();
pkt.header.seq = next_seq++;

send(sock, &pkt, sizeof(pkt), 0);
```

### Receive Pattern

```c
uint8_t buffer[MAX_PACKET_SIZE];
recv(sock, buffer, sizeof(buffer), 0);

// Generic dispatch
uint8_t source_node;
const void* data;
int idx = schema_packet_dispatch(buffer, &source_node, &data);

switch (idx) {
    case 0: handle_record0((const record0_t*)data); break;
    case 1: handle_record1((const record1_t*)data); break;
    // ...
    default: handle_error(); break;
}

// Or type-specific verify
const record0_t* r0 = record0_packet_verify((const record0_packet_t*)buffer);
if (r0) {
    // Valid record0 packet
}
```

---

## Binary Schema Format

The binary schema enables runtime introspection without parsing C headers.

### Type Tags

| Tag | Type |
|-----|------|
| 1 | int8 |
| 2 | uint8 |
| 3 | int16 |
| 4 | uint16 |
| 5 | int32 |
| 6 | uint32 |
| 7 | int64 |
| 8 | uint64 |
| 9 | float |
| 10 | double |
| 11 | bool |
| 20 | enum |
| 21 | fixed |
| 22 | string |
| 23 | pointer |
| 30 | struct |
| 31 | record |

### Field Descriptor (in binary)

| Size | Field |
|------|-------|
| N | Name (null-terminated) |
| 1 | Type tag |
| 2 | Offset within struct |
| 2 | Field size |
| 2 | Array count (0 = scalar) |

---

## Test Program

### Overview

`main.c` is a comprehensive test suite that validates all aspects of the generated code. It performs **72 assertions** across **10 test categories**.

### Building and Running

```bash
# Native build
gcc -Wall -Wextra -O2 -o test_sensor main.c
./test_sensor sensor_data.bin

# Or via Makefile
make test
```

### Test Categories

#### Test 1: Record Types and Field Access

**Purpose:** Verify that generated struct types compile correctly and fields are accessible.

**Validates:**
- `sensor_reading_t` field assignment and retrieval
- `alarm_event_t` enum field handling
- `config_update_t` mixed type fields
- `heartbeat_t` compact struct layout
- Correct `sizeof()` for each record type

#### Test 2: Wire Header and Schema Hash

**Purpose:** Verify the wire header structure and schema validation.

**Validates:**
- `sizeof(sensor_data_wire_header_t) == 16` (natural alignment)
- `SENSOR_DATA_SCHEMA_HASH` matches expected FNV-1a value
- `sensor_data_verify_header()` accepts valid hash
- `sensor_data_verify_header()` rejects invalid hash

#### Test 3: Packet Init and Verify

**Purpose:** Verify per-record packet initialization and type-safe verification.

**Validates:**
- `sensor_reading_packet_init()` sets correct header fields
- `alarm_event_packet_init()` sets index = 1
- `config_update_packet_init()` sets index = 2
- `heartbeat_packet_init()` sets index = 3
- `*_packet_verify()` returns data pointer for valid packets
- `*_packet_verify()` returns NULL for wrong packet type
- `*_packet_verify()` returns NULL for corrupted hash

#### Test 4: Generic Packet Dispatch

**Purpose:** Verify the switch-based packet dispatch mechanism.

**Validates:**
- `sensor_data_packet_dispatch()` returns correct index for each record type
- Source node extraction works correctly
- Data pointer points to correct offset
- Returns -1 for invalid schema hash
- Returns -1 for out-of-range index

#### Test 5: Size Arrays

**Purpose:** Verify the generated size lookup tables.

**Validates:**
- `sensor_data_record_sizes[i]` matches `sizeof(record_t)` for all records
- `sensor_data_packet_sizes[i]` matches `sizeof(record_packet_t)` for all packets
- Arrays have correct count (`SENSOR_DATA_RECORD_COUNT` entries)

#### Test 6: Embedded Binary Schema

**Purpose:** Verify the embedded binary schema in `_bin.h` is valid and parseable.

**Validates:**
- `sizeof(sensor_data_schema_bin) == SENSOR_DATA_BIN_SIZE`
- Binary header magic is `0x41565244` ("AVRD")
- Binary header version is 1
- Record count matches `SENSOR_DATA_RECORD_COUNT`
- Schema hash matches `SENSOR_DATA_SCHEMA_HASH`
- Total size field matches actual size
- Full binary schema parses without errors
- All enums, fixed arrays, strings, and records are present

#### Test 7: Enum Values

**Purpose:** Verify generated enum constants have correct values.

**Validates:**
- `SENSOR_TYPE_TEMPERATURE == 0`
- `SENSOR_TYPE_HUMIDITY == 1`
- `SENSOR_TYPE_PRESSURE == 2`
- `SENSOR_TYPE_FLOW == 3`
- `ALARM_LEVEL_NONE == 0`
- `ALARM_LEVEL_WARNING == 1`
- `ALARM_LEVEL_CRITICAL == 2`

#### Test 8: Fixed Types

**Purpose:** Verify fixed-size type definitions.

**Validates:**
- `sizeof(mac_addr_t) == 6`
- `sizeof(uuid_t) == 16`
- `sensor_name_t` buffer size and length fields work correctly

#### Test 9: Simulated Socket Round-Trip

**Purpose:** End-to-end test of packet creation, transmission, and reception.

**Validates:**
- Packet init creates valid wire format
- Header verification succeeds after "transmission"
- Dispatch returns correct record index
- All data fields survive round-trip intact
- Both generic dispatch and type-specific verify work

#### Test 10: Cross-Platform Wire Format

**Purpose:** Verify wire structs have identical sizes on all platforms.

**Validates:**
- Wire header is exactly 16 bytes
- All `_wire_t` structs match expected packed sizes
- All `_packet_t` structs are header + wire payload
- `_to_wire()` and `_from_wire()` conversions preserve data
- Static asserts would catch size mismatches at compile time

**Expected wire sizes:**

| Type | Size | Calculation |
|------|------|-------------|
| `sensor_reading_wire_t` | 14 | 2+4+4+4 |
| `alarm_event_wire_t` | 18 | 2+4+4+4+4 |
| `config_update_wire_t` | 13 | 2+2+4+4+1 |
| `heartbeat_wire_t` | 10 | 4+4+1+1 |

#### Test 11: Binary File Loading

**Purpose:** Verify external `.bin` file loading and parsing.

**Validates:**
- File opens and reads successfully
- File size matches `SENSOR_DATA_BIN_SIZE`
- File contents match embedded `sensor_data_schema_bin[]`
- Binary schema parses correctly from file

---

## Building

### Makefile Targets

| Target | Description |
|--------|-------------|
| `make generate` | Run LuaJIT to generate `.h`, `_bin.h`, `.bin` |
| `make test` | Build and run native test program |
| `make test32` | Build and run 32-bit ARM test (via QEMU) |
| `make compare` | Show struct sizes for 32-bit vs 64-bit |
| `make sizes` | Display generated file sizes |
| `make clean` | Remove generated files |
| `make all` | Generate and test (default) |

### Manual Build

```bash
# Generate all outputs
luajit test_schema.lua

# Compile test program
gcc -Wall -Wextra -O2 -o test_sensor main.c

# Run tests
./test_sensor sensor_data.bin
```

---

## 32-bit Cross Compilation

For testing on 32-bit ARM from a 64-bit ARM host (e.g., Snapdragon):

### Install Cross-Compiler

```bash
sudo apt install gcc-arm-linux-gnueabihf qemu-user
```

### Build and Run

```bash
# Via Makefile
make test32

# Manual
arm-linux-gnueabihf-gcc -Wall -Wextra -O2 -march=armv7-a \
    -mfloat-abi=hard -static -o test_sensor_arm32 main.c

qemu-arm -L /usr/arm-linux-gnueabihf ./test_sensor_arm32 sensor_data.bin
```

### Compare Architectures

```bash
make compare
```

Example output:
```
=== 64-bit native ===
  sizeof(void*)   = 8 (64-bit pointers)
  sizeof(sensor_reading_t) = 16

=== 32-bit ARM ===
  sizeof(void*)   = 4 (32-bit pointers)
  sizeof(sensor_reading_t) = 16
```

Note: Record sizes should be identical since they use fixed-width types. The wire header is always 16 bytes regardless of platform.

---

## License

MIT License - See source files for details.

---

## Related Projects

- **ChainTree** - Unified control architecture using this schema system
- **s_engine** - S-expression runtime with FNV-1a hash-based dispatch

---

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2025-01 | Initial release with .h, _bin.h, .bin generation |