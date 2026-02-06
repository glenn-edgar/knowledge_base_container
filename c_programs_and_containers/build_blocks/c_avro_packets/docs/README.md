# Avro DSL for Embedded C

A Lua-based DSL (Domain Specific Language) that generates C structures and binary packets for reliable message exchange in embedded systems. Inspired by Apache Avro but redesigned for resource-constrained environments.

## Overview

This tool generates:
- **Schema header** (`_msgs.h`) - C structures, packet wrappers, inline encode/verify functions, and schema-level constants
- **Data header** (`_data.h`) - Embedded binary blob for ROM storage
- **Binary file** (`.bin`) - Packet data for file-based operations

## Quick Start

```bash
# Generate schema header only
lua sensor_msgs.lua

# Generate schema + binary data
lua sensor_data.lua
```

### Schema Definition (sensor_msgs.lua)
```lua
require("avro_dsl").export_globals()

FILE("sensor_msgs")
INCLUDE_BRACKET("stdint.h")

FIXED("mac_addr", 6)

RECORD("sensor_reading")
    FIELD("sensor_id", "uint16")
    FIELD("value", "float")
    FIELD("timestamp", "double")
END_RECORD()

GENERATE()
```

### Data Definition (sensor_data.lua)
```lua
require("avro_dsl").export_globals()
dofile("sensor_msgs.lua")

DATA_FILE("sensor_data")

INSTANCE("sensor_reading", "default_reading")
    SET("sensor_id", 0x0001)
    SET("value", 25.5)
    SET("timestamp", 0.0)
END_INSTANCE()

GENERATE_DATA()
```

### C Usage
```c
#include "sensor_msgs.h"

// Encode a packet
sensor_reading_packet_t pkt;
sensor_reading_t* data = sensor_reading_packet_encode(&pkt, "sensor_msgs", node_id);
data->sensor_id = 0x0042;
data->value = 23.5f;
data->timestamp = get_time();
send(fd, &pkt, SENSOR_READING_PACKET_SIZE);

// Verify and access received packet
uint16_t source;
const sensor_reading_t* rx = sensor_reading_packet_verify(buffer, "sensor_msgs", &source);
if (rx) {
    printf("Value: %f\n", rx->value);
}
```

## Design Philosophy

### Why Not Standard Avro?

Apache Avro is excellent for big data, log storage, and schema evolution in distributed systems. However, it was designed with different constraints:

| Aspect | Apache Avro | This Implementation |
|--------|-------------|---------------------|
| Target | JVM, Big Data | Embedded C, 32KB MCUs |
| Schema | JSON | Lua DSL → C headers |
| Wire Format | Variable-length, compressed | Fixed-layout, packed structs |
| Access | Parse/decode | Direct memory-mapped |
| Use Case | Log storage, Kafka | Real-time message exchange |

### Core Design Decisions

#### 1. C Structures Instead of JSON

Standard Avro uses JSON schemas and binary encoding with variable-length integers. This implementation generates `__attribute__((packed))` C structures that match the wire format exactly.

**Rationale:** On a 32KB ARM Cortex-M microcontroller, there is no room for a JSON parser or variable-length decoder. Direct struct access eliminates parsing overhead entirely.

```c
// Wire buffer can be cast directly to struct pointer
const sensor_reading_t* data = (sensor_reading_t*)buffer;
float value = data->value;  // Direct access, no decoding
```

#### 2. Fixed Layout, No Compression

Every field is always present at a fixed offset. No optional fields, no variable-length encoding, no compression.

**Rationale:** 
- Predictable memory allocation (know packet size at compile time)
- Constant-time field access
- No encode/decode CPU overhead
- Packets are for real-time message exchange, not archival storage

```c
// Packet sizes known at compile time
#define SENSOR_READING_PACKET_SIZE 42
uint8_t buffer[SENSOR_READING_PACKET_SIZE];
```

#### 3. One Instance Per Record Type

Binary files contain exactly one packet per record type defined in the schema. All records must be populated.

**Rationale:** This eliminates the need for a secondary indexing scheme. Packet offsets are determined entirely by the schema:

```c
// Offsets computed from schema, not data
#define SENSOR_HEADER_PACKET_OFFSET 0
#define SENSOR_READING_PACKET_OFFSET 28
#define DEVICE_CONFIG_PACKET_OFFSET 70

// File operations need only the schema header
#include "sensor_msgs.h"  // No _data.h required

uint8_t buf[SENSOR_READING_PACKET_SIZE];
read(fd, buf, SENSOR_READING_PACKET_SIZE);
const sensor_reading_t* data = sensor_reading_packet_verify(buf, "sensor_msgs", &src);
```

#### 4. Verification Before Access

Every packet includes a header with:
- **Schema hash** (32-bit DJB2) - Detects schema mismatch
- **Record index** - Verifies packet type
- **Payload length** - Validates data integrity

**Rationale:** In distributed embedded systems, packets may arrive corrupted, from incompatible firmware versions, or be misrouted. Verification catches these errors before data access.

```c
// Returns NULL if verification fails
const sensor_reading_t* data = sensor_reading_packet_verify(
    buffer, 
    "sensor_msgs",  // Schema name (hashed at runtime)
    &source_node);

if (data) {
    // Safe to access - schema, type, and length verified
}
```

#### 5. Timestamp in Header

The packet header includes a `double` timestamp field.

**Rationale:** These packets are used for sensor streaming and control systems (e.g., drone flight control, industrial automation). Timestamps are essential for:
- Measurement correlation
- Latency detection
- Control loop timing
- Data fusion from multiple sensors

Unlike log storage systems where arrival time may suffice, real-time control requires knowing when the measurement was taken at the source.

## Packet Structure

```
+------------------+
| Packet Header    |  20 bytes
|   schema_hash    |  4 bytes (uint32)
|   timestamp      |  8 bytes (double)
|   seq            |  2 bytes (uint16)
|   source_node    |  2 bytes (uint16)
|   length         |  2 bytes (uint16)
|   index          |  2 bytes (uint16)
+------------------+
| Payload          |  Variable (record-specific)
|   (C struct)     |
+------------------+
```

## Generated API

For each record type, the DSL generates:

### Types and Constants
```c
typedef struct __attribute__((packed)) { ... } sensor_reading_t;
typedef struct __attribute__((packed)) { ... } sensor_reading_packet_t;

#define SENSOR_READING_SIZE 22          // Payload size
#define SENSOR_READING_INDEX 1          // Record type index
#define SENSOR_READING_PACKET_SIZE 42   // Header + payload
#define SENSOR_READING_PACKET_OFFSET 28 // Offset in binary file
```

### Functions
```c
// Initialize packet header, return pointer to payload
sensor_reading_t* sensor_reading_packet_encode(
    sensor_reading_packet_t* pkt,
    const char* schema_name,
    uint16_t source_node);

// Verify header, return payload pointer or NULL
const sensor_reading_t* sensor_reading_packet_verify(
    const void* buffer,
    const char* schema_name,
    uint16_t* source_node);

// Get packet size
size_t sensor_reading_packet_length(void);

// Copy with verification
sensor_reading_t* sensor_reading_packet_copy(
    void* dst,
    const void* src);
```

## Type System

### Primitive Types
| DSL Type | C Type | Size |
|----------|--------|------|
| `uint8` | `uint8_t` | 1 |
| `uint16` | `uint16_t` | 2 |
| `uint32` | `uint32_t` | 4 |
| `uint64` | `uint64_t` | 8 |
| `int8` | `int8_t` | 1 |
| `int16` | `int16_t` | 2 |
| `int32` | `int32_t` | 4 |
| `int64` | `int64_t` | 8 |
| `float` | `float` | 4 |
| `double` | `double` | 8 |
| `bool` | `bool` | 1 |

### Composite Types
```lua
FIXED("mac_addr", 6)           -- uint8_t mac_addr_t[6]
POINTER("callback_data")       -- void* callback_data_t
RECORD("inner") ... END_RECORD() -- Nested struct
```

## Use Cases

This implementation was developed for:

1. **Sensor Networks** - Streaming measurements from distributed sensors
2. **Drone Control** - Flight controller ↔ ground station communication
3. **Industrial Automation** - PLC and controller message exchange
4. **ChainTree Control Systems** - Behavior tree node coordination

## Target Platforms

- **Minimum:** ARM Cortex-M0+ with 32KB flash, 8KB RAM
- **Typical:** ARM Cortex-M4/M7, ESP32, Raspberry Pi
- **Architecture:** Little-endian (ARM32, ARM64, x86)

## Files

| File | Description |
|------|-------------|
| `avro_dsl.lua` | DSL generator (LuaJIT/Lua 5.3+ compatible) |
| `cfl_avro_support.h` | Runtime hash function (inline) |
| `cfl_avro_support.c` | Runtime hash function (linkable) |
| `sensor_msgs.lua` | Example schema definition |
| `sensor_data.lua` | Example data definition |
| `main.c` | Test/example program |

## License

MIT License

## A Note to Avro Purists

This is not Apache Avro. It borrows the concept of schema-defined binary serialization but makes fundamental tradeoffs for embedded systems:

- **No schema evolution** - Firmware updates are coordinated
- **No variable-length encoding** - Predictability over compression
- **No JSON** - C structures are the schema
- **No optional fields** - All fields always present

These are deliberate design choices for reliable, low-latency message exchange on resource-constrained microcontrollers. For log storage, big data, or schema evolution requirements, use standard Apache Avro.