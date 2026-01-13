```markdown
# Hierarchical Bitmap Example - Irrigation Valve System

## Overview

This example demonstrates the complete workflow of the Hierarchical Bitmap system using a realistic irrigation control scenario. The example covers all three buffer types (OR_LATCH, OR_MASK, AND) and shows practical error detection patterns.

**What this example demonstrates:**
- Creating and initializing a hierarchical bitmap instance
- Working with all three buffer types
- Using the controller pattern for PLC bank mapping
- Shadow buffer writes and propagation
- Latch and mask operations
- Error detection and collection with monitoring node attribution

---

## The Irrigation System

### Hardware Architecture

```
Irrigation Control System
├── VALVE_STATUS Tree (OR_LATCH and OR_MASK buffers)
│   ├── STATION_1_VALVE_STATUS (4 banks)
│   ├── STATION_2_VALVE_STATUS (3 banks)
│   ├── STATION_3_VALVE_STATUS (3 banks)
│   └── STATION_4_VALVE_STATUS (3 banks)
│
└── VALVE_STATE Tree (AND buffer)
    ├── STATION_1_VALVE_STATE (4 banks)
    ├── STATION_2_VALVE_STATE (3 banks)
    ├── STATION_3_VALVE_STATE (3 banks)
    └── STATION_4_VALVE_STATE (3 banks)
```

**Each valve bank has:**
- 8 alarm bits (ALARM_LATCHED buffer)
- 8 alarm enable bits (ALARM_MASK buffer)
- 8 ready status bits (AND_LATCHED buffer)

### Schema Definition

From `irrigation_valves.lua`:

```lua
-- Three buffer types
S.buffer("ALARM_LATCHED", "OR_LATCH")  -- Alarms that latch until cleared
S.buffer("ALARM_MASK", "OR_MASK")      -- Alarms with enable/disable control
S.buffer("AND_LATCHED", "AND")         -- Ready status (all must be true)

-- Leaf classes
S.class("Valve_Bank_Leaf", {ALARM_LATCHED = 8, ALARM_MASK = 8, AND_LATCHED = 0})
  S.bits("ALARM_LATCHED", "overcurrent", "stuck_open", "stuck_closed", "leak",
                          "overtemp", "comm_fail", "low_pressure", "high_pressure")
  S.bits("ALARM_MASK", "overcurrent", "stuck_open", "stuck_closed", "leak",
                       "overtemp", "comm_fail", "low_pressure", "high_pressure")
S.end_class()

S.class("AND_Valve_Bank_Leaf", {ALARM_LATCHED = 0, ALARM_MASK = 0, AND_LATCHED = 8})
  S.bits("AND_LATCHED", "powered", "calibrated", "enabled", "ready",
                        "comm_ok", "pressure_ok", "flow_ok", "position_ok")
S.end_class()
```

---

## Building and Running

### Prerequisites

```bash
# Install LuaJIT (for code generation)
sudo apt-get install luajit

# Or on macOS
brew install luajit
```

### Generate Headers

```bash
# From the repository root
cd examples
luajit ../codegen.lua irrigation_valves.lua test_out/
```

**Generates:**
- `test_out/generated_irrigation_valves.h` - Constants and enums
- `test_out/generated_irrigation_valves_data.h` - Static data tables

### Compile

```bash
gcc -o test_example \
    test_example.c \
    ../cfl_hbit.c \
    ../cfl_hbit_support.c \
    -I.. \
    -Itest_out \
    -Wall -Wextra -O2
```

### Run

```bash
./test_example
```

**Expected output:**
```
========================================
Hierarchical Bit Map My Example
========================================

Testing OR Latch Test
...

Testing OR MASK Test
...

Testing AND Mask Test
...

Testing Simple Walker Based Error Handling
...
```

---

## Test Breakdown

### Test 1: OR_LATCH Buffer

**Purpose:** Demonstrate latching alarm behavior

```c
void test_or_latch_test(cfl_hbit_instance_t* inst)
```

**What it does:**

1. **Setup**
   ```c
   // Find the ALARM_LATCHED buffer and VALVE_STATUS root node
   uint16_t bit_space_id = IRRIGATION_VALVES_BUF_ALARM_LATCHED;
   int16_t top_node = cfl_hbit_find_node_path(inst, "VALVE_STATUS");
   
   // Create controller for VALVE_STATUS tree
   cfl_hbit_controller_t* ctrl = cfl_hbit_controller_create(inst, top_node, bit_space_id);
   
   // Clear all bits and latches
   cfl_hbit_controller_clear_all(ctrl);
   cfl_hbit_clear_controller_latches(ctrl);
   cfl_hbit_sync_and_propagate(inst);
   ```

2. **Set alarms in 4 stations** (child-relative indexing)
   ```c
   cfl_hbit_controller_set_child_bit(ctrl, 0, 0);  // Station 1, bit 0
   cfl_hbit_controller_set_child_bit(ctrl, 1, 1);  // Station 2, bit 1
   cfl_hbit_controller_set_child_bit(ctrl, 2, 2);  // Station 3, bit 2
   cfl_hbit_controller_set_child_bit(ctrl, 3, 3);  // Station 4, bit 3
   cfl_hbit_sync_and_propagate(inst);
   ```

3. **Clear alarms** (alarms clear, but latches remain)
   ```c
   cfl_hbit_controller_clear_child_bit(ctrl, 0, 0);
   cfl_hbit_controller_clear_child_bit(ctrl, 1, 1);
   cfl_hbit_controller_clear_child_bit(ctrl, 2, 2);
   cfl_hbit_controller_clear_child_bit(ctrl, 3, 3);
   cfl_hbit_sync_and_propagate(inst);
   // Current bits clear, but latched bits remain set!
   ```

4. **Clear latches** (operator acknowledgment)
   ```c
   cfl_hbit_controller_clear_latch_child_bit(ctrl, 0, 0);
   cfl_hbit_controller_clear_latch_child_bit(ctrl, 1, 1);
   cfl_hbit_controller_clear_latch_child_bit(ctrl, 2, 2);
   cfl_hbit_controller_clear_latch_child_bit(ctrl, 3, 3);
   cfl_hbit_sync_and_propagate(inst);
   ```

5. **Verify propagation**
   ```c
   cfl_hbit_controller_set_child_bit(ctrl, 0, 0);
   cfl_hbit_sync_and_propagate(inst);
   
   // Check that alarm propagated to root
   bool alarm = cfl_hbit_controller_read_bit(ctrl, 0);
   printf("Top node alarm: %d (expected 1)\n", alarm);
   ```

**Key Concepts:**
- Latching prevents missed transient alarms
- Latches persist after source clears
- Explicit acknowledgment required (clear_latch)
- Child-relative indexing maps to PLC banks

---

### Test 2: OR_MASK Buffer

**Purpose:** Demonstrate masked alarm propagation

```c
void test_or_mask_test(cfl_hbit_instance_t* inst)
```

**What it does:**

1. **Setup**
   ```c
   uint16_t bit_space_id = IRRIGATION_VALVES_BUF_ALARM_MASK;
   int16_t top_node = cfl_hbit_find_node_path(inst, "VALVE_STATUS");
   cfl_hbit_controller_t* ctrl = cfl_hbit_controller_create(inst, top_node, bit_space_id);
   
   // Clear all bits and masks (disable all propagation)
   cfl_hbit_controller_clear_all(ctrl);
   cfl_hbit_clear_controller_masks(ctrl);
   cfl_hbit_sync_and_propagate(inst);
   ```

2. **Set alarms** (but masks are disabled, so won't propagate)
   ```c
   cfl_hbit_controller_set_child_bit(ctrl, 0, 0);
   cfl_hbit_controller_set_child_bit(ctrl, 1, 1);
   cfl_hbit_controller_set_child_bit(ctrl, 2, 2);
   cfl_hbit_controller_set_child_bit(ctrl, 3, 3);
   cfl_hbit_sync_and_propagate(inst);
   // Leaf bits set, but parent bits remain 0 (masked out)
   ```

3. **Disable masks** (explicitly block propagation)
   ```c
   cfl_hbit_controller_set_mask_child_bit(ctrl, 0, 0, false);  // Disable
   cfl_hbit_controller_set_mask_child_bit(ctrl, 1, 1, false);
   cfl_hbit_controller_set_mask_child_bit(ctrl, 2, 2, false);
   cfl_hbit_controller_set_mask_child_bit(ctrl, 3, 3, false);
   cfl_hbit_sync_and_propagate(inst);
   // Still masked, parent bits remain 0
   ```

4. **Enable masks** (allow propagation)
   ```c
   cfl_hbit_controller_set_mask_child_bit(ctrl, 0, 0, true);  // Enable
   cfl_hbit_controller_set_mask_child_bit(ctrl, 1, 1, true);
   cfl_hbit_controller_set_mask_child_bit(ctrl, 2, 2, true);
   cfl_hbit_controller_set_mask_child_bit(ctrl, 3, 3, true);
   cfl_hbit_sync_and_propagate(inst);
   // Now alarms propagate up tree
   ```

5. **Clear alarms**
   ```c
   cfl_hbit_controller_clear_child_bit(ctrl, 0, 0);
   cfl_hbit_controller_clear_child_bit(ctrl, 1, 1);
   cfl_hbit_controller_clear_child_bit(ctrl, 2, 2);
   cfl_hbit_controller_clear_child_bit(ctrl, 3, 3);
   cfl_hbit_sync_and_propagate(inst);
   ```

6. **Verify masked propagation**
   ```c
   cfl_hbit_controller_set_child_bit(ctrl, 0, 0);
   cfl_hbit_sync_and_propagate(inst);
   
   bool alarm = cfl_hbit_controller_read_bit(ctrl, 0);
   printf("Top node alarm: %d (expected 1)\n", alarm);
   ```

**Key Concepts:**
- Masks control which alarms propagate
- Useful for maintenance mode (suppress alarms during service)
- Runtime enable/disable of specific alarm monitoring
- Propagation formula: `parent = (child1 & mask1) | (child2 & mask2) | ...`

---

### Test 3: AND Buffer

**Purpose:** Demonstrate AND logic for ready status

```c
void test_and_test(cfl_hbit_instance_t* inst)
```

**What it does:**

1. **Setup**
   ```c
   uint16_t bit_space_id = IRRIGATION_VALVES_BUF_AND_LATCHED;
   int16_t top_node = cfl_hbit_find_node_path(inst, "VALVE_STATE");
   cfl_hbit_controller_t* ctrl = cfl_hbit_controller_create(inst, top_node, bit_space_id);
   
   // Fill all bits with 1s (everything ready)
   cfl_hbit_controller_fill_all(ctrl, 0xFF);
   cfl_hbit_sync_and_propagate(inst);
   ```

2. **Clear ready bits in 4 stations** (one device not ready)
   ```c
   cfl_hbit_controller_clear_child_bit(ctrl, 0, 0);  // Station 1 not ready
   cfl_hbit_controller_clear_child_bit(ctrl, 1, 1);  // Station 2 not ready
   cfl_hbit_controller_clear_child_bit(ctrl, 2, 2);  // Station 3 not ready
   cfl_hbit_controller_clear_child_bit(ctrl, 3, 3);  // Station 4 not ready
   cfl_hbit_sync_and_propagate(inst);
   // Parent bits also clear (AND logic)
   ```

3. **Set ready bits** (devices now ready)
   ```c
   cfl_hbit_controller_set_child_bit(ctrl, 0, 0);
   cfl_hbit_controller_set_child_bit(ctrl, 1, 1);
   cfl_hbit_controller_set_child_bit(ctrl, 2, 2);
   cfl_hbit_controller_set_child_bit(ctrl, 3, 3);
   cfl_hbit_sync_and_propagate(inst);
   // All children ready → parent ready
   ```

4. **Clear one ready bit** (breaks AND chain)
   ```c
   cfl_hbit_controller_clear_child_bit(ctrl, 0, 0);
   cfl_hbit_sync_and_propagate(inst);
   
   bool ready = cfl_hbit_controller_read_bit(ctrl, 0);
   printf("Top node ready: %d (expected 0)\n", ready);
   // One child not ready → parent not ready
   ```

**Key Concepts:**
- AND logic: parent set only if ALL children set
- Useful for "all ready" or "all healthy" indicators
- Single failure propagates up tree
- Propagation formula: `parent = child1 & child2 & child3 & ...`

---

### Test 4: Error Detection and Analysis

**Purpose:** Demonstrate comprehensive error collection with monitoring node attribution

```c
void test_simple_walker_based_error_handling(cfl_hbit_instance_t* inst)
```

**What it does:**

1. **Setup**
   ```c
   // Find buffer by name (runtime lookup)
   uint16_t bit_space_id = irrigation_valves_find_buffer("ALARM_LATCHED");
   
   int16_t top_node = cfl_hbit_find_node_path(inst, "VALVE_STATUS");
   cfl_hbit_controller_t* ctrl = cfl_hbit_controller_create(inst, top_node, bit_space_id);
   
   // Clear everything
   cfl_hbit_controller_clear_all(ctrl);
   cfl_hbit_clear_controller_latches(ctrl);
   cfl_hbit_sync_and_propagate(inst);
   ```

2. **Set alarms in 4 stations**
   ```c
   cfl_hbit_controller_set_child_bit(ctrl, 0, 0);  // Station 1, bit 0
   cfl_hbit_controller_set_child_bit(ctrl, 1, 1);  // Station 2, bit 1
   cfl_hbit_controller_set_child_bit(ctrl, 2, 2);  // Station 3, bit 2
   cfl_hbit_controller_set_child_bit(ctrl, 3, 3);  // Station 4, bit 3
   cfl_hbit_sync_and_propagate(inst);
   ```

3. **Define monitoring nodes** (which station each alarm belongs to)
   ```c
   uint8_t temp_bit_index = 0;
   uint16_t monitoring_nodes[4];
   
   // Get the station node for each child
   monitoring_nodes[0] = cfl_hbit_controller_get_node_bit(ctrl, 0, 0, &temp_bit_index);
   monitoring_nodes[1] = cfl_hbit_controller_get_node_bit(ctrl, 1, 1, &temp_bit_index);
   monitoring_nodes[2] = cfl_hbit_controller_get_node_bit(ctrl, 2, 2, &temp_bit_index);
   monitoring_nodes[3] = cfl_hbit_controller_get_node_bit(ctrl, 3, 3, &temp_bit_index);
   ```

4. **Count error bits** (fast scan with tree pruning)
   ```c
   uint32_t error_bits = cfl_hbit_count_error_bits(
       inst, 
       top_node, 
       bit_space_id, 
       false);  // Don't apply mask
   
   printf("Number of error bits: %d\n", error_bits);
   ```

5. **Collect all errors with monitoring attribution**
   ```c
   cfl_hbit_error_bits_t* errors = cfl_hbit_count_error_bits_and_get_bits(
       inst, 
       top_node, 
       bit_space_id,
       4,                // 4 monitoring nodes
       monitoring_nodes, 
       true);            // Apply mask
   ```

6. **Process error collection**
   ```c
   if (errors) {
       printf("Found %u error bits:\n", errors->count);
       
       for (uint32_t i = 0; i < errors->count; i++) {
           cfl_hbit_error_bit_t* err = &errors->error_bits[i];
           const cfl_hbit_node_t* node = &inst->config->nodes[err->node];
           
           printf("  Error at node %u (hash 0x%08X), bit %u, monitoring node %u\n",
                  err->node, node->path_hash, err->index, err->monitoring_node);
       }
       
       // Print grouped by node
       cfl_hbit_print_error_bits_by_node(inst, errors);
       
       // Free memory
       cfl_hbit_error_bits_destroy(inst, errors);
   }
   ```

**Key Concepts:**
- Two-pass algorithm: count then allocate
- Monitoring node attribution: which subsystem does error belong to
- First match wins: error attributed to first ancestor found in monitoring list
- Error collection includes: node index, bit index, monitoring node
- Tree pruning optimizes scan (O(log n) instead of O(n))

---

## Key Patterns Demonstrated

### 1. Instance Lifecycle

```c
// Create with custom allocator
static void* my_alloc(size_t size, void* ctx) {
    return malloc(size);
}

static void my_free(void* ptr, void* ctx) {
    free(ptr);
}

static const cfl_hbit_allocator_t g_alloc = { my_alloc, my_free, NULL };

// Create instance
cfl_hbit_instance_t* inst = cfl_hbit_create(
    &g_alloc,
    (const cfl_hbit_config_t*)&irrigation_valves_config);

// Use instance...

// Destroy when done
cfl_hbit_destroy(inst);
```

### 2. Shadow Buffer Pattern

```c
// Phase 1: Accumulate writes in shadow
cfl_hbit_controller_set_child_bit(ctrl, 0, 0);
cfl_hbit_controller_set_child_bit(ctrl, 1, 1);
cfl_hbit_controller_set_child_bit(ctrl, 2, 2);

// Phase 2: Atomic sync and propagate
cfl_hbit_sync_and_propagate(inst);

// Phase 3: Read consistent state
bool alarm = cfl_hbit_controller_read_bit(ctrl, 0);
```

### 3. Controller Pattern for PLC Banks

```c
// Create controller for station
cfl_hbit_controller_t* ctrl = cfl_hbit_controller_create(
    inst, station_node, buffer_id);

// Child-relative indexing (matches PLC data layout)
cfl_hbit_controller_set_child_bit(ctrl, child_idx, bit_idx);

// Flat indexing (iterate all bits)
for (uint16_t i = 0; i < ctrl->total_bits; i++) {
    if (cfl_hbit_controller_read_bit(ctrl, i)) {
        // Process error
    }
}

cfl_hbit_controller_destroy(ctrl);
```

### 4. Runtime Node Lookup

```c
// By path string
int16_t node = cfl_hbit_find_node_path(inst, "VALVE_STATUS.STATION_2");

// By generated hash constant (faster)
int16_t node = cfl_hbit_find_node(inst, IRRIGATION_VALVES_HASH_VALVE_STATUS);

// Buffer by name
uint16_t buf = irrigation_valves_find_buffer("ALARM_LATCHED");

// Or use generated enum (compile-time constant, fastest)
uint16_t buf = IRRIGATION_VALVES_BUF_ALARM_LATCHED;
```

### 5. Debug Printing

```c
// Print node state (current, latched, mask)
cfl_hbit_print_node_state(inst, buffer_id, node_id, "Label");

// Print error collection grouped by node
cfl_hbit_print_error_bits_by_node(inst, errors);
```

---

## Expected Output Walkthrough

### Test 1 Output: OR_LATCH

```
Testing OR Latch Test

Found ALARM_LATCHED bitspace at 0

Initial state:
 [node 0, buf 0, aggregate]:
  current: 0x00 (0000 0000)
  latched: 0x00 (0000 0000)

Setting child bits:
 [node 0, buf 0, aggregate]:
  current: 0x0F (0000 1111)
  latched: 0x0F (0000 1111)

Clearing child bits:
 [node 0, buf 0, aggregate]:
  current: 0x00 (0000 0000)
  latched: 0x0F (0000 1111)  ← Latches remain!

Clearing latch child bits:
 [node 0, buf 0, aggregate]:
  current: 0x00 (0000 0000)
  latched: 0x00 (0000 0000)  ← Now cleared

reading bit top node 0 bit index 0 value 1 expected 1
 [node 0, buf 0, aggregate]:
  current: 0x01 (0000 0001)
  latched: 0x01 (0000 0001)
```

### Test 2 Output: OR_MASK

```
Testing OR MASK Test

Found OR_MASK bitspace at 1

Initial state:
 [node 0, buf 1, aggregate]:
  current: 0x00 (0000 0000)
  mask:    0x00 (0000 0000)  ← All masked out

Setting child bits:
 [node 0, buf 1, aggregate]:
  current: 0x00 (0000 0000)  ← Doesn't propagate (masked)
  mask:    0x00 (0000 0000)

Clearing mask child bits:
 [node 0, buf 1, aggregate]:
  current: 0x00 (0000 0000)
  mask:    0x00 (0000 0000)

Setting mask child bits:
 [node 0, buf 1, aggregate]:
  current: 0x0F (0000 1111)  ← Now propagates!
  mask:    0x0F (0000 1111)

Clearing child bits:
 [node 0, buf 1, aggregate]:
  current: 0x00 (0000 0000)
  mask:    0x0F (0000 1111)

reading bit top node 0 bit index 0 value 1 expected 1
 [node 0, buf 1, aggregate]:
  current: 0x01 (0000 0001)
  mask:    0x0F (0000 1111)
```

### Test 3 Output: AND

```
Testing AND Mask Test

Found AND_LATCHED bitspace at 2

Initial state:
 [node 18, buf 2, aggregate]:
  current: 0xFF (1111 1111)  ← All ready

Setting child bits:
 [node 18, buf 2, aggregate]:
  current: 0xF0 (1111 0000)  ← Some not ready

Clearing child bits:
 [node 18, buf 2, aggregate]:
  current: 0xFF (1111 1111)  ← All ready again

reading bit top node 18 bit index 0 value 0 expected 0
 [node 18, buf 2, aggregate]:
  current: 0xFE (1111 1110)  ← One not ready
```

### Test 4 Output: Error Detection

```
Testing Simple Walker Based Error Handling

Found OR_LATCH bitspace at 0

Initial state:
 [node 0, buf 0, aggregate]:
  current: 0x00 (0000 0000)
  latched: 0x00 (0000 0000)

Setting child bits:
 [node 0, buf 0, aggregate]:
  current: 0x0F (0000 1111)
  latched: 0x0F (0000 1111)

Number of error bits: 4

Found 4 error bits:
  Error at node 2 (hash 0x6B2D261A), bit 0 monitoring node 2
  Error at node 7 (hash 0xA3769025), bit 1 monitoring node 7
  Error at node 11 (hash 0xA98D269C), bit 2 monitoring node 11
  Error at node 15 (hash 0xA147788F), bit 3 monitoring node 15

Node 2 (hash 0x6B2D261A): bits [0] (1 bits)
Node 7 (hash 0xA3769025): bits [1] (1 bits)
Node 11 (hash 0xA98D269C): bits [2] (1 bits)
Node 15 (hash 0xA147788F): bits [3] (1 bits)
```

---

## Extending the Example

### Add More Tests

```c
void test_maintenance_mode(cfl_hbit_instance_t* inst) {
    // Enter maintenance on specific station
    uint16_t station_node = cfl_hbit_find_node_path(inst, 
        "VALVE_STATUS.STATION_2_VALVE_STATUS");
    
    // Disable all alarm propagation
    cfl_hbit_controller_t* ctrl = cfl_hbit_controller_create(
        inst, station_node, IRRIGATION_VALVES_BUF_ALARM_MASK);
    
    cfl_hbit_clear_controller_masks(ctrl);
    cfl_hbit_sync_and_propagate(inst);
    
    // Set alarms - won't propagate
    cfl_hbit_controller_set_child_bit(ctrl, 0, 0);
    cfl_hbit_sync_and_propagate(inst);
    
    // Check that parent doesn't see alarm
    bool alarm = cfl_hbit_read_bit(inst, 
        IRRIGATION_VALVES_BUF_ALARM_MASK,
        IRRIGATION_VALVES_NODE_VALVE_STATUS, 0);
    
    printf("System alarm during maintenance: %d (expected 0)\n", alarm);
    
    cfl_hbit_controller_destroy(ctrl);
}
```

### Simulate PLC Updates

```c
void simulate_plc_update(cfl_hbit_instance_t* inst) {
    // Read from "PLC" (simulated data)
    uint8_t plc_data[8] = {
        0x01,  // Valve 1: overcurrent
        0x04,  // Valve 2: stuck_closed
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00
    };
    
    // Write directly to node using shadow_write
    uint16_t node = cfl_hbit_find_node_path(inst,
        "VALVE_STATUS.STATION_1_VALVE_STATUS.BANK_1_VALVE_STATUS");
    
    cfl_hbit_shadow_write(inst, 
        IRRIGATION_VALVES_BUF_ALARM_LATCHED,
        node, plc_data, 8);
    
    cfl_hbit_sync_and_propagate(inst);
}
```

---

## Common Modifications

### 1. Change Schema
```bash
# Edit irrigation_valves.lua
# - Add more stations
# - Add more valve banks per station
# - Add more alarm types

# Regenerate headers
luajit ../codegen.lua irrigation_valves.lua test_out/

# Recompile
make clean && make
```

### 2. Use Different Allocator
```c
// Arena allocator for embedded systems
static uint8_t arena[4096];
static size_t arena_offset = 0;

static void* arena_alloc(size_t size, void* ctx) {
    if (arena_offset + size > sizeof(arena)) return NULL;
    void* ptr = arena + arena_offset;
    arena_offset += size;
    return ptr;
}

static void arena_free(void* ptr, void* ctx) {
    // Arena doesn't support individual frees
}

static const cfl_hbit_allocator_t arena_alloc = { 
    arena_alloc, arena_free, NULL 
};
```

### 3. Add Performance Timing
```c
#include <time.h>

void benchmark_propagation(cfl_hbit_instance_t* inst) {
    struct timespec start, end;
    
    clock_gettime(CLOCK_MONOTONIC, &start);
    
    for (int i = 0; i < 10000; i++) {
        cfl_hbit_sync_and_propagate(inst);
    }
    
    clock_gettime(CLOCK_MONOTONIC, &end);
    
    double elapsed = (end.tv_sec - start.tv_sec) + 
                     (end.tv_nsec - start.tv_nsec) / 1e9;
    
    printf("10000 propagations: %.3f ms (%.3f µs each)\n",
           elapsed * 1000, elapsed * 100);
}
```

---

## Troubleshooting

### "Failed to create instance"
- Check that generated headers are up to date
- Verify allocator callbacks are valid
- Ensure sufficient memory available

### "ERROR: could not find node"
- Verify path string matches DSL definition exactly
- Check that DSL was regenerated after changes
- Use generated constants instead of runtime lookup

### Unexpected propagation behavior
- Print node state with `cfl_hbit_print_node_state()`
- Verify buffer type matches intention (OR vs AND)
- Check that `sync_and_propagate()` called after writes

### Memory leaks
- Always call `cfl_hbit_destroy()` on instance
- Always call `cfl_hbit_controller_destroy()` on controllers
- Always call `cfl_hbit_error_bits_destroy()` on error collections

---

## Summary

This example demonstrates:

✓ **Complete workflow** - DSL → Codegen → Runtime  
✓ **All buffer types** - OR_LATCH, OR_MASK, AND  
✓ **Controller pattern** - PLC bank mapping  
✓ **Error detection** - Collection with monitoring attribution  
✓ **Debug helpers** - Print functions for development  
✓ **Best practices** - Shadow buffers, cleanup, error checking  

**Next Steps:**
- Modify schema for your hardware
- Add your own test scenarios
- Integrate into your control loop
- Profile performance on target hardware

---

**Questions?** See [DSL.md](../DSL.md), [RUNTIME.md](../RUNTIME.md), or [THEORY.md](../THEORY.md) for more details.
```