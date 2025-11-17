MessagePack Arena Library
A zero-copy, read-only MessagePack implementation designed for embedded systems with resource constraints. Data structures are pre-compiled into Flash/ROM using a Python code generator, enabling fast lookups with minimal RAM usage.
Features

Zero-Copy Access - Reads directly from Flash/ROM, no deserialization overhead
Minimal RAM Usage - Only small arena descriptor lives in RAM
Hash-Based Map Keys - O(n) lookups using FNV-1a 64-bit hashes with compile-time constants
Type-Safe API - Explicit type checking for all value extractions
Subtree Operations - Extract and copy portions of data structures
Code Generation - Python tool converts JSON/MessagePack/dicts to C arrays
ARM-Optimized - 4-byte aligned structures, .rodata section placement
No Dynamic Allocation - All data pre-allocated at compile time

Architecture
┌─────────────────────────────────────────────────────┐
│  Flash/ROM (.rodata section)                        │
│  ┌───────────────────────────────────────────────┐  │
│  │  device_config_buffer[]                       │  │
│  │  ┌─────────────────────────────────────────┐  │  │
│  │  │  Node Area (20 bytes × N nodes)         │  │  │
│  │  │  - Type, flags, counts                  │  │  │
│  │  │  - Offsets to children/data             │  │  │
│  │  │  - Inline values (int/float/hash)       │  │  │
│  │  └─────────────────────────────────────────┘  │  │
│  │  ┌─────────────────────────────────────────┐  │  │
│  │  │  String Pool (null-terminated strings)  │  │  │
│  │  │  - All string/binary data               │  │  │
│  │  └─────────────────────────────────────────┘  │  │
│  └───────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────┘
                         │
                         ▼
                  ┌──────────────┐
                  │ MsgPackArena │ (RAM: ~32 bytes)
                  │  - buffer*   │
                  │  - size      │
                  │  - root      │
                  └──────────────┘
Node Structure (20 bytes, 4-byte aligned)
ctypedef struct {
    uint8_t type;              // MSGPACK_TYPE_*
    uint8_t flags;             // Reserved
    uint16_t element_count;    // Array/Map size
    uint32_t data_offset;      // Offset to string/binary data
    uint32_t child_offset;     // Offset to first child node
    union {
        int64_t i64;           // Signed integer
        uint64_t u64;          // Unsigned integer / hash
        double f64;            // Floating point
        struct {
            uint32_t size;     // String/binary length
            uint32_t reserved;
        } sized;
    } value;
} MsgPackNode;  // Total: 20 bytes
Quick Start
1. Generate Data from JSON
bash# Install Python generator
pip install msgpack  # Optional, for .msgpack input

# Generate from JSON
python msgpack_gen.py config.json -n device_config -o src/

# Output:
#   src/device_config_data.h
#   src/device_config_data.c
#   src/shared_msgpack_runtime.h
#   src/shared_msgpack_runtime.c
Input (config.json):
json{
  "device_id": "DEV-12345",
  "firmware_version": 100,
  "network": {
    "wifi_ssid": "Company-IoT",
    "server_url": "https://api.example.com"
  }
}
2. Use in Your Code
c#include "msgpack_arena.h"
#include "device_config_data.h"
#include "shared_msgpack_runtime.h"

void setup() {
    // Initialize arena
    device_config_init();
    
    // Get root node
    const MsgPackNode* root = device_config_root();
    
    // Access values using hashes
    const MsgPackNode* device_id = msgpack_map_get_str(
        &device_config_arena, root, "device_id");
    
    size_t len;
    const char* id = msgpack_get_string(&device_config_arena, device_id, &len);
    printf("Device ID: %.*s\n", (int)len, id);
    
    // Navigate nested structures
    const MsgPackNode* network = msgpack_map_get_str(
        &device_config_arena, root, "network");
    
    const MsgPackNode* ssid = msgpack_map_get_str(
        &device_config_arena, network, "wifi_ssid");
    
    const char* ssid_str = msgpack_get_string(&device_config_arena, ssid, &len);
    printf("WiFi SSID: %.*s\n", (int)len, ssid_str);
}
3. Use Compile-Time Hash Constants (Faster)
c// Runtime generates hash macros
#include "shared_msgpack_runtime.h"

// Direct hash lookup (no string hashing at runtime)
const MsgPackNode* device_id = msgpack_map_get(
    &device_config_arena, root, HASH_DEVICE_ID);
API Reference
Initialization
c// Initialize arena from read-only buffer
bool msgpack_arena_init(MsgPackArena* arena, const void* buffer, size_t size);

// Get root node
const MsgPackNode* msgpack_arena_root(const MsgPackArena* arena);
Navigation
c// Get node at offset
const MsgPackNode* msgpack_get_node(const MsgPackArena* arena, uint32_t offset);

// Map lookup by hash
const MsgPackNode* msgpack_map_get(const MsgPackArena* arena, 
                                   const MsgPackNode* map,
                                   uint64_t key_hash);

// Map lookup by string (hashes at runtime)
const MsgPackNode* msgpack_map_get_str(const MsgPackArena* arena,
                                       const MsgPackNode* map,
                                       const char* key);

// Array access by index
const MsgPackNode* msgpack_array_get(const MsgPackArena* arena,
                                     const MsgPackNode* array,
                                     uint16_t index);
Value Extraction
c// Extract typed values (returns false if type mismatch)
bool msgpack_get_int(const MsgPackArena* arena, const MsgPackNode* node, int64_t* out);
bool msgpack_get_uint(const MsgPackArena* arena, const MsgPackNode* node, uint64_t* out);
bool msgpack_get_float(const MsgPackArena* arena, const MsgPackNode* node, float* out);
bool msgpack_get_double(const MsgPackArena* arena, const MsgPackNode* node, double* out);
bool msgpack_get_bool(const MsgPackArena* arena, const MsgPackNode* node, bool* out);

// Get string/binary (returns pointer into buffer, no copy)
const char* msgpack_get_string(const MsgPackArena* arena, 
                               const MsgPackNode* node, 
                               size_t* len);

const uint8_t* msgpack_get_binary(const MsgPackArena* arena, 
                                  const MsgPackNode* node, 
                                  size_t* len);
Subtree Operations
c// Calculate size of subtree (for allocation)
size_t msgpack_subtree_size(const MsgPackArena* arena, const MsgPackNode* node);

// Copy subtree to another arena allocator
bool msgpack_subtree_copy(const MsgPackArena* src_arena,
                         const MsgPackNode* src_node,
                         ArenaAllocator* dest_arena,
                         uint32_t* out_offset);

// Extract subtree to new arena
bool msgpack_subtree_extract(const MsgPackArena* src_arena,
                            const MsgPackNode* src_node,
                            MsgPackArena* dest_arena,
                            ArenaAllocator* dest_allocator);
Debugging
c// Convert node to JSON-like string
bool msgpack_to_string(const MsgPackArena* arena,
                      const MsgPackNode* node,
                      char* buffer,
                      size_t buffer_size,
                      size_t* out_len);

// Print node (uses 1KB stack buffer)
void msgpack_print_node(const MsgPackArena* arena, const MsgPackNode* node);

// Validate arena
bool msgpack_validate(const MsgPackArena* arena);
Hash Functions
c// FNV-1a 64-bit hash (matches Python generator)
uint64_t msgpack_hash64(const char* str);
uint64_t msgpack_hash64_n(const char* str, size_t len);
Code Generator Usage
Command-Line
bash# Single configuration (generates data + runtime)
python msgpack_gen.py config.json -n my_config -o output/

# Multiple configurations (shared runtime)
python msgpack_gen.py config1.json -n config1 --data-only -o output/
python msgpack_gen.py config2.json -n config2 --data-only -o output/
python msgpack_gen.py --runtime-only --runtime-name shared_runtime -o output/

# From Python dict
python msgpack_gen.py -d '{"key": "value"}' -n my_data

# Verbose output
python msgpack_gen.py config.json -v
Python Library
pythonfrom msgpack_gen import MsgPackCodeGenerator, RuntimeGenerator

# Single config
gen = MsgPackCodeGenerator("device_config")
gen.load_json("config.json")
gen.generate_all("output/")
gen.print_stats()

# Multiple configs with shared runtime
runtime = RuntimeGenerator()

for name, data in configs.items():
    gen = MsgPackCodeGenerator(name)
    gen.load_dict(data)
    gen.generate_data_only("output/")
    runtime.merge(gen)

runtime.generate("output/", "shared_msgpack_runtime")
Advanced Examples
Complex Nested Structures
c// config.json:
// {
//   "sensors": [
//     {"type": "temperature", "enabled": true, "threshold": 25.5},
//     {"type": "humidity", "enabled": true, "threshold": 60.0}
//   ]
// }

const MsgPackNode* sensors = msgpack_map_get_str(&arena, root, "sensors");

for (uint16_t i = 0; i < sensors->element_count; i++) {
    const MsgPackNode* sensor = msgpack_array_get(&arena, sensors, i);
    
    const MsgPackNode* type_node = msgpack_map_get_str(&arena, sensor, "type");
    size_t len;
    const char* type = msgpack_get_string(&arena, type_node, &len);
    
    const MsgPackNode* threshold_node = msgpack_map_get_str(&arena, sensor, "threshold");
    double threshold;
    msgpack_get_double(&arena, threshold_node, &threshold);
    
    printf("Sensor %d: %.*s, threshold: %.1f\n", i, (int)len, type, threshold);
}
Error Handling
cconst MsgPackNode* value = msgpack_map_get_str(&arena, root, "key");
if (!value) {
    // Key not found
    return ERROR_KEY_NOT_FOUND;
}

int64_t num;
if (!msgpack_get_int(&arena, value, &num)) {
    // Type mismatch
    return ERROR_TYPE_MISMATCH;
}
Subtree Extraction
c// Extract a subtree to work with independently
const MsgPackNode* network = msgpack_map_get_str(&arena, root, "network");

size_t size = msgpack_subtree_size(&arena, network);
uint8_t* buffer = malloc(size);

ArenaAllocator dest_alloc;
arena_init(&dest_alloc, buffer, size);

MsgPackArena network_arena;
msgpack_subtree_extract(&arena, network, &network_arena, &dest_alloc);

// Now work with network_arena independently
Performance Characteristics
OperationTime ComplexityNotesMap lookupO(n)Linear scan through key-value pairsArray accessO(1)Direct offset calculationValue extractionO(1)Direct pointer returnSubtree copyO(n)Proportional to subtree sizeInitializationO(1)Sets pointers only
Memory Usage:

Flash/ROM: Full data structure (nodes + strings)
RAM: 32 bytes for MsgPackArena structure
Stack: Minimal (function call overhead only)

Limitations

Map keys must be strings - Converted to 64-bit hashes
No dynamic modification - Read-only data structures
Maximum 65,535 elements per array/map (16-bit count)
Maximum 4GB buffer size (32-bit offsets)
Linear map search - Not a hash table (okay for small maps)
No schema validation - Assumes correctly generated data

Integration
Build Configuration
makefile# Makefile
CFLAGS += -I./include
SOURCES += src/msgpack_arena.c \
           src/device_config_data.c \
           src/shared_msgpack_runtime.c

# For ARM targets
CFLAGS += -mthumb -mcpu=cortex-m4
LDFLAGS += -Wl,--section-start=.rodata=0x08000000
CMake
cmakeadd_library(msgpack_arena
    src/msgpack_arena.c
    src/device_config_data.c
    src/shared_msgpack_runtime.c
)

target_include_directories(msgpack_arena PUBLIC include)
target_compile_options(msgpack_arena PRIVATE -Wall -Wextra)
Testing
c// test_msgpack.c
#include "msgpack_arena.h"
#include "device_config_data.h"
#include <assert.h>

void test_basic_access() {
    device_config_init();
    const MsgPackNode* root = device_config_root();
    assert(root != NULL);
    assert(root->type == MSGPACK_TYPE_MAP);
    
    const MsgPackNode* id = msgpack_map_get_str(
        &device_config_arena, root, "device_id");
    assert(id != NULL);
    
    size_t len;
    const char* str = msgpack_get_string(&device_config_arena, id, &len);
    assert(strncmp(str, "DEV-12345", len) == 0);
}
License
(Add your license here)
Contributing
(Add contribution guidelines here)
Credits

FNV Hash: Fowler-Noll-Vo hash function
MessagePack: Inspired by MessagePack specification
