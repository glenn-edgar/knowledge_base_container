# msgpack_gen.py - MessagePack Code Generator

A Python code generator that creates C data structures and runtime code for embedded systems using MessagePack format with arena allocators. Generates position-independent, read-only data structures suitable for flash/ROM storage with minimal RAM overhead.

[![Python](https://img.shields.io/badge/Python-3.7+-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Platform](https://img.shields.io/badge/Platform-ARM%20%7C%20x86%20%7C%20RISC--V-orange.svg)](https://github.com)

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Command-Line Usage](#command-line-usage)
- [Library Usage](#library-usage)
- [Production Preprocessor](#production-preprocessor)
- [API Reference](#api-reference)
- [Examples](#examples)
- [Integration](#integration)
- [Troubleshooting](#troubleshooting)
- [Architecture](#architecture)

## Features

### Core Features
- ✅ **Offset-based addressing** - Position-independent data structures
- ✅ **Read-only data** - Suitable for flash/ROM storage
- ✅ **String hashing** - Fast lookups using FNV-1a 64-bit hashes
- ✅ **Shared runtime** - Multiple configs can share single runtime
- ✅ **Zero RAM overhead** - Data stays in flash
- ✅ **ARM-aligned** - 4-byte alignment for ARM processors
- ✅ **Subtree extraction** - Copy portions to RAM when needed

### Input Formats
- JSON files (`.json`)
- MessagePack binary files (`.msgpack`, `.mp`)
- Python dict/list objects
- Auto-detection of format

### Output
- **Data files**: `{name}_data.h`, `{name}_data.c`
- **Runtime files**: `{name}_runtime.h`, `{name}_runtime.c` (optional, can be shared)

### Use Cases
- **Embedded systems** - Configuration storage in flash
- **IoT devices** - Device, sensor, network configs
- **Multi-environment builds** - Dev, test, production configs
- **Firmware builds** - Customer-specific configurations
- **Data persistence** - Serializable structures

## Installation

### Basic Installation

```bash
# Clone or download msgpack_gen.py
curl -O https://raw.githubusercontent.com/yourrepo/msgpack_gen.py

# No dependencies for JSON (built-in)
# Optional: Install msgpack for binary format support
pip install msgpack
```

### Requirements

- Python 3.7+
- `msgpack` module (optional, for MessagePack binary format)

```bash
pip install msgpack  # Optional
```

### File Structure

```
your_project/
├── msgpack_gen.py              # Main generator module
├── msgpack_arena.h             # C runtime header (required)
├── msgpack_arena.c             # C runtime implementation (required)
├── arena_alloc.h               # Arena allocator header (required)
├── arena_alloc.c               # Arena allocator implementation (required)
└── config.json                 # Your configuration data
```

## Quick Start

### 1. Command-Line (Simplest)

```bash
# Generate from JSON file
python msgpack_gen.py config.json

# This creates:
#   msgpack_data_data.h
#   msgpack_data_data.c
#   msgpack_data_runtime.h
#   msgpack_data_runtime.c
```

### 2. Python Library

```python
from msgpack_gen import MsgPackCodeGenerator

# Create generator
gen = MsgPackCodeGenerator("my_config")

# Load data
gen.load_json("config.json")

# Generate all files
gen.generate_all("output/")
```

### 3. Use in C Code

```c
#include "msgpack_data_data.h"
#include "msgpack_data_runtime.h"

int main(void) {
    // Initialize
    msgpack_data_init();
    
    // Get root node
    const MsgPackNode* root = msgpack_data_root();
    
    // Access values
    const MsgPackNode* value = msgpack_map_get_str(&msgpack_data_arena, 
                                                    root, "device_name");
    
    return 0;
}
```

## Command-Line Usage

### Basic Commands

```bash
# From JSON file (auto-detects format)
python msgpack_gen.py config.json

# From MessagePack binary
python msgpack_gen.py data.msgpack -f msgpack

# From Python dict string
python msgpack_gen.py -d '{"key": "value", "num": 42}'

# Custom variable name
python msgpack_gen.py config.json -n device_config

# Specify output directory
python msgpack_gen.py config.json -o generated/

# Verbose output
python msgpack_gen.py config.json -v
```

### Advanced Commands

```bash
# Generate data files only (no runtime)
python msgpack_gen.py config.json --data-only -n config1 -o output/

# Generate runtime only
python msgpack_gen.py --runtime-only --runtime-name shared_runtime -o output/

# Multiple configs with shared runtime
python msgpack_gen.py config1.json -n config1 --data-only -o output/
python msgpack_gen.py config2.json -n config2 --data-only -o output/
python msgpack_gen.py config3.json -n config3 --data-only -o output/
python msgpack_gen.py --runtime-only --runtime-name shared_runtime -o output/
```

### Command-Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `input` | Input file or dict string | Required |
| `-d, --dict` | Treat input as Python dict/list | False |
| `-f, --format` | Input format (json/msgpack/auto) | auto |
| `-n, --name` | Variable name prefix | msgpack_data |
| `-o, --output` | Output directory | . |
| `--data-only` | Generate only data files | False |
| `--runtime-only` | Generate only runtime files | False |
| `--runtime-name` | Runtime file name | msgpack_runtime |
| `-v, --verbose` | Verbose output | False |

## Library Usage

### Basic Example

```python
from msgpack_gen import MsgPackCodeGenerator

# Create generator with custom name
gen = MsgPackCodeGenerator("device_config", verbose=True)

# Load from different sources
gen.load_json("config.json")           # From JSON file
# OR
gen.load_msgpack("config.msgpack")     # From MessagePack binary
# OR
gen.load_dict({"key": "value"})        # From Python dict
# OR
gen.load_auto("config.json")           # Auto-detect format

# Generate files
files = gen.generate_all("output/")

# Print statistics
gen.print_stats()

# Get statistics programmatically
stats = gen.get_stats()
print(f"Total size: {stats.total_bytes} bytes")
print(f"Nodes: {stats.node_count}")
print(f"Hashed keys: {stats.hashed_keys}")
```

### Method Chaining

```python
from msgpack_gen import MsgPackCodeGenerator

# Fluent API
gen = (MsgPackCodeGenerator("config")
       .load_json("config.json")
       .generate_all("output/"))
```

### Generate to String (In-Memory)

```python
from msgpack_gen import MsgPackCodeGenerator
from io import StringIO

gen = MsgPackCodeGenerator("inline")
gen.load_dict({"key": "value"})

# Generate to string instead of file
header_code = gen.generate_data_header()
source_code = gen.generate_data_source()

print(header_code)
```

### Multiple Configurations

```python
from msgpack_gen import MsgPackCodeGenerator

configs = {
    "device_config": {"device_id": "DEV-001", "version": 1},
    "sensor_config": {"enabled": True, "rate": 1000},
    "network_config": {"ssid": "IoT", "timeout": 5000}
}

for name, data in configs.items():
    gen = MsgPackCodeGenerator(name)
    gen.load_dict(data)
    gen.generate_all("output/")
    print(f"Generated {name}: {gen.get_stats()}")
```

## Production Preprocessor

For production builds with multiple configurations sharing a single runtime:

### Using RuntimeGenerator Class

```python
from msgpack_gen import MsgPackCodeGenerator, RuntimeGenerator

# Create shared runtime
runtime = RuntimeGenerator()

# Generate multiple configs (data only)
configs = {
    "device_config": {...},
    "sensor_config": {...},
    "network_config": {...}
}

for name, data in configs.items():
    gen = MsgPackCodeGenerator(name)
    gen.load_dict(data)
    gen.generate_data_only("output/")  # Data files only
    runtime.merge(gen)                 # Collect hashes

# Generate single shared runtime
runtime.generate("output/", "shared_runtime")

print(f"Generated {len(configs)} configs with {runtime.get_hash_count()} unique hashes")
```

### Using ProductionPreprocessor Class

```python
from production_preprocessor import ProductionPreprocessor

# Create preprocessor
prep = ProductionPreprocessor(
    output_dir="build/generated",
    runtime_name="shared_msgpack_runtime",
    verbose=True
)

# Add configurations
prep.add_config("device_config", device_data)
prep.add_config("sensor_config", sensor_data)
prep.add_config_from_file("network_config", "network.json")

# Finalize - generates shared runtime
result = prep.finalize()

# Print summary
prep.print_summary()
```

### Production Build Script Example

```python
#!/usr/bin/env python3
"""build_configs.py - Production build script"""

from msgpack_gen import MsgPackCodeGenerator, RuntimeGenerator
import json
from pathlib import Path

def build_all_configs():
    """Build all product configurations"""
    
    config_dir = Path("configs")
    output_dir = Path("generated")
    
    runtime = RuntimeGenerator()
    
    # Process all JSON files in configs/
    for config_file in config_dir.glob("*.json"):
        name = config_file.stem
        print(f"Processing {name}...")
        
        gen = MsgPackCodeGenerator(name, verbose=True)
        gen.load_json(str(config_file))
        gen.generate_data_only(str(output_dir))
        runtime.merge(gen)
    
    # Generate shared runtime
    print("\nGenerating shared runtime...")
    runtime.generate(str(output_dir), "product_runtime")
    
    print(f"\n✓ Generated {len(list(config_dir.glob('*.json')))} configs")
    print(f"✓ Total unique hashes: {runtime.get_hash_count()}")

if __name__ == "__main__":
    build_all_configs()
```

## API Reference

### MsgPackCodeGenerator Class

#### Constructor

```python
MsgPackCodeGenerator(var_name: str = "msgpack_data", verbose: bool = False)
```

**Parameters:**
- `var_name` - Variable name prefix for generated code
- `verbose` - Enable verbose output

#### Loading Methods

```python
load_json(filepath: str) -> MsgPackCodeGenerator
load_json_string(json_str: str) -> MsgPackCodeGenerator
load_msgpack(filepath: str) -> MsgPackCodeGenerator
load_msgpack_bytes(data: bytes) -> MsgPackCodeGenerator
load_dict(data: Union[dict, list]) -> MsgPackCodeGenerator
load_auto(filepath: str) -> MsgPackCodeGenerator
```

#### Generation Methods

```python
# Generate all files (data + runtime)
generate_all(output_dir: str = ".") -> Dict[str, str]

# Generate only data files
generate_data_only(output_dir: str = ".") -> Dict[str, str]

# Generate individual files
generate_data_header(output: Union[str, StringIO] = None) -> str
generate_data_source(output: Union[str, StringIO] = None) -> str
```

#### Statistics Methods

```python
get_stats() -> GenerationStats
print_stats() -> None
```

#### Utility Methods

```python
reset() -> None
get_log_messages() -> List[str]
clear_log() -> None
```

### RuntimeGenerator Class

#### Constructor

```python
RuntimeGenerator()
```

#### Methods

```python
# Merge hashes from a generator
merge(generator: MsgPackCodeGenerator) -> None

# Manually add hash
add_hash(key: str, hash_value: Optional[int] = None) -> None

# Get hash count
get_hash_count() -> int

# Generate runtime files
generate(output_dir: str = ".", runtime_name: str = "msgpack_runtime") -> Dict[str, str]

# Generate individual files
generate_header(output: Union[str, StringIO] = None, runtime_name: str = "msgpack_runtime") -> str
generate_source(output: Union[str, StringIO] = None, runtime_name: str = "msgpack_runtime") -> str
```

### Utility Functions

```python
hash_string_64(s: str) -> int
```

FNV-1a 64-bit hash function (matches C implementation).

## Examples

### Example 1: Simple Configuration

**config.json:**
```json
{
  "device_name": "Sensor-001",
  "firmware_version": 123,
  "network": {
    "ssid": "MyNetwork",
    "timeout": 5000
  },
  "sensors": ["temperature", "humidity", "pressure"]
}
```

**Generate:**
```bash
python msgpack_gen.py config.json -n device_config
```

**Use in C:**
```c
#include "device_config_data.h"
#include "device_config_runtime.h"

void setup(void) {
    device_config_init();
    
    const MsgPackNode* root = device_config_root();
    
    // Access using hash macros (fast)
    const MsgPackNode* name = msgpack_map_get(&device_config_arena, 
                                             root, 
                                             HASH_DEVICE_NAME);
    
    // Or using string (computes hash at runtime)
    const MsgPackNode* version = msgpack_map_get_str(&device_config_arena, 
                                                     root, 
                                                     "firmware_version");
}
```

### Example 2: Multiple Environments

**Python:**
```python
from msgpack_gen import MsgPackCodeGenerator

environments = {
    "dev": {
        "debug": True,
        "server": "http://localhost:8000",
        "log_level": "verbose"
    },
    "prod": {
        "debug": False,
        "server": "https://api.example.com",
        "log_level": "error"
    }
}

for env, config in environments.items():
    gen = MsgPackCodeGenerator(f"{env}_config")
    gen.load_dict(config)
    gen.generate_all(f"output/{env}/")
```

**C Makefile:**
```makefile
# Development build
firmware_dev.elf: main.c output/dev/dev_config_data.c
	$(CC) -DENV_DEV -o $@ $^

# Production build
firmware_prod.elf: main.c output/prod/prod_config_data.c
	$(CC) -DENV_PROD -o $@ $^
```

### Example 3: Shared Runtime (Production)

**Python:**
```python
from msgpack_gen import MsgPackCodeGenerator, RuntimeGenerator

runtime = RuntimeGenerator()

configs = ["device", "sensor", "network", "system"]

for name in configs:
    gen = MsgPackCodeGenerator(f"{name}_config")
    gen.load_json(f"configs/{name}.json")
    gen.generate_data_only("output/")
    runtime.merge(gen)

# Single shared runtime for all configs
runtime.generate("output/", "shared_runtime")
```

**C Code:**
```c
// All configs use same runtime
#include "device_config_data.h"
#include "sensor_config_data.h"
#include "network_config_data.h"
#include "shared_runtime.h"  // Single runtime for all

void init_all(void) {
    device_config_init();
    sensor_config_init();
    network_config_init();
    
    // All hash macros from all configs are in shared_runtime.h
    // HASH_DEVICE_NAME, HASH_SSID, HASH_THRESHOLD, etc.
}
```

### Example 4: Integration with Build System

**CMakeLists.txt:**
```cmake
# Generate configs at build time
add_custom_command(
    OUTPUT ${CMAKE_BINARY_DIR}/generated/device_config_data.c
    COMMAND python3 ${CMAKE_SOURCE_DIR}/msgpack_gen.py 
            ${CMAKE_SOURCE_DIR}/config.json 
            -n device_config 
            -o ${CMAKE_BINARY_DIR}/generated
    DEPENDS ${CMAKE_SOURCE_DIR}/config.json
    COMMENT "Generating MessagePack configuration"
)

add_executable(firmware
    main.c
    ${CMAKE_BINARY_DIR}/generated/device_config_data.c
    ${CMAKE_BINARY_DIR}/generated/device_config_runtime.c
)
```

**Makefile:**
```makefile
# Generate configs before compiling
generated/%.c: configs/%.json msgpack_gen.py
	python3 msgpack_gen.py $< -n $(basename $(notdir $<)) -o generated/

firmware.elf: main.c generated/device_config_data.c
	$(CC) -o $@ $^ -Igenerated/
```

## Integration

### Arduino

```cpp
// Place msgpack_gen.py generated files in sketch folder
#include "device_config_data.h"
#include "device_config_runtime.h"

void setup() {
    Serial.begin(115200);
    
    device_config_init();
    const MsgPackNode* root = device_config_root();
    
    // Use configuration
    const MsgPackNode* rate = msgpack_map_get_str(&device_config_arena, 
                                                  root, "sample_rate");
}
```

### ESP-IDF

```cmake
# components/config/CMakeLists.txt
idf_component_register(
    SRCS 
        "device_config_data.c"
        "device_config_runtime.c"
        "msgpack_arena.c"
        "arena_alloc.c"
    INCLUDE_DIRS "."
)
```

### PlatformIO

```ini
; platformio.ini
[env:device]
extra_scripts = pre:generate_config.py

; generate_config.py
Import("env")
env.Execute("python msgpack_gen.py config.json -o src/")
```

## Troubleshooting

### No output files generated

**Problem:** Files not created after running generator.

**Solutions:**
```bash
# 1. Check output directory exists and is writable
ls -ld output/
chmod 755 output/

# 2. Run with verbose flag
python msgpack_gen.py config.json -v

# 3. Check for errors
python msgpack_gen.py config.json 2>&1 | tee error.log

# 4. Use absolute path
python msgpack_gen.py config.json -o $(pwd)/output/

# 5. Test with minimal example
python -c "from msgpack_gen import MsgPackCodeGenerator; \
           gen = MsgPackCodeGenerator('test', verbose=True); \
           gen.load_dict({'key': 'value'}); \
           gen.generate_all('.')"
```

### Import errors

**Problem:** `ImportError: No module named 'msgpack_gen'`

**Solutions:**
```bash
# Option 1: Run from same directory
cd /path/to/msgpack_gen/
python msgpack_gen.py config.json

# Option 2: Add to PYTHONPATH
export PYTHONPATH=/path/to/msgpack_gen:$PYTHONPATH

# Option 3: Install as module
pip install -e /path/to/msgpack_gen/
```

### MessagePack format not supported

**Problem:** `RuntimeError: msgpack module not available`

**Solution:**
```bash
pip install msgpack
```

### Permission denied

**Problem:** Cannot write to output directory.

**Solution:**
```bash
# Fix permissions
chmod 755 output/

# Or write to user directory
python msgpack_gen.py config.json -o ~/generated/
```

### Hash collisions

**Problem:** Two different strings have the same hash.

**Note:** With 64-bit FNV-1a hashes, collisions are extremely rare (< 10^-15 probability for typical datasets).

**If needed, check for collisions:**
```python
from msgpack_gen import hash_string_64

keys = ["key1", "key2", "key3"]
hashes = {key: hash_string_64(key) for key in keys}

# Check for duplicates
if len(hashes) != len(set(hashes.values())):
    print("Collision detected!")
```

## Architecture

### Data Flow

```
Input Data (JSON/MessagePack/Dict)
         ↓
   MsgPackCodeGenerator
         ↓
   Build Tree (Nodes + Strings)
         ↓
   Hash Map Keys (FNV-1a 64-bit)
         ↓
   Generate C Code
         ↓
   ┌─────────────┬──────────────┐
   ↓             ↓              ↓
Data Header  Data Source  Runtime Files
  (.h)         (.c)       (.h + .c)
```

### File Structure

```
Generated Files:
├── {name}_data.h          # Data structure header
│   ├── Buffer declaration
│   ├── Arena declaration
│   ├── Init/access functions
│   └── Size constants
│
├── {name}_data.c          # Data structure source
│   ├── Buffer definition (nodes + strings)
│   ├── Arena instance
│   └── Init/access implementations
│
├── {name}_runtime.h       # Runtime header (optional shared)
│   ├── Hash value macros
│   ├── Hash mapping structures
│   └── Lookup function declarations
│
└── {name}_runtime.c       # Runtime source (optional shared)
    ├── Hash mapping table
    └── Lookup implementations
```

### Memory Layout

```
Flash/ROM:
┌─────────────────────────────────────────┐
│ {name}_buffer[] (const)                 │
│ ┌─────────────────────────────────────┐ │
│ │ Nodes (24 bytes each)               │ │
│ │ ├─ Type, flags, count               │ │
│ │ ├─ Data offset                      │ │
│ │ ├─ Child offset                     │ │
│ │ └─ Value (union)                    │ │
│ └─────────────────────────────────────┘ │
│ ┌─────────────────────────────────────┐ │
│ │ String Pool                         │ │
│ │ "string1\0string2\0..."             │ │
│ └─────────────────────────────────────┘ │
└─────────────────────────────────────────┘

RAM:
┌─────────────────────────────────────────┐
│ MsgPackArena arena (24 bytes)           │
│ ├─ buffer pointer → Flash               │
│ ├─ size                                 │
│ └─ root_offset                          │
└─────────────────────────────────────────┘
Total RAM: 24 bytes only!
```

## Performance

### Memory Overhead

- **Per config**: ~24 bytes RAM (arena struct)
- **Per node**: 24 bytes flash (node struct)
- **Strings**: Variable (null-terminated in flash)
- **No malloc**: Zero fragmentation

### Access Speed

- **Hash lookup**: O(n) where n = map size
- **Array index**: O(1)
- **String deref**: O(1) pointer arithmetic

### Compilation Size

- **Code**: ~2-5 KB (depends on configuration)
- **Data**: Varies by content
- **Example**: 100-item config ≈ 3-4 KB total

## License

MIT License - See LICENSE file for details.

## Contributing

Contributions welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Add tests for new features
4. Submit a pull request

## Support

- **Issues**: [GitHub Issues](https://github.com/yourrepo/issues)
- **Documentation**: [Wiki](https://github.com/yourrepo/wiki)
- **Examples**: See `examples/` directory

## Related Projects

- **arena_alloc** - Arena allocator for embedded systems
- **msgpack_arena** - MessagePack runtime for arena allocators

---

**Made with ❤️ for embedded systems developers**

