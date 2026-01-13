```markdown
# Hierarchical Bitmap System - Theory of Operation

## 1. The Problem: Managing Distributed Status at Scale

### Why Hierarchical Bitmaps?

Modern control systems face a fundamental challenge: **how do you efficiently monitor thousands of status bits across hundreds of devices without overwhelming the CPU?**

Consider an industrial irrigation system with:
- 120 valve banks (each with 8 alarm bits = 960 alarm bits)
- 40 pump stations (each with 6 status bits = 240 status bits)
- 30 flow sensors (each with 4 error bits = 120 error bits)

**Total: 1,320+ status bits to monitor every control cycle (10-100ms)**

#### Traditional Approaches Fail

**Linear scanning** (checking every bit):
```c
// Scan 1,320 bits every cycle - SLOW!
for (int i = 0; i < 1320; i++) {
    if (check_bit(i)) {
        handle_error(i);
    }
}
```
- **O(n) complexity** - scales poorly
- **No priority** - critical alarms wait in line
- **CPU intensive** - wastes cycles when no errors exist

**Hash table lookups**:
- Requires dynamic memory allocation
- Poor cache locality  
- Still requires scanning to find "any error" condition

### The Hierarchical Bitmap Solution

Hierarchical bitmaps solve this by organizing bits into a **tree structure** where:

#### A. Quick Global Check (O(1))
```c
// Check if ANY error exists in entire system
bool has_errors = cfl_hbit_read_bit(inst, ALARM_BUFFER, ROOT_NODE, 0);

// One bit check replaces scanning 1,320 bits!
if (!has_errors) return;  // Common case: fast exit
```

**How?** Parent nodes automatically OR/AND their children's bits. The root summarizes the entire system in a single bit.

#### B. Fast Error Location (O(log n))
```c
// Drill down the tree to find errors
if (cfl_hbit_read_bit(inst, ALARM, STATION_2, 0)) {
    if (cfl_hbit_read_bit(inst, ALARM, STATION_2_ZONE_A, 0)) {
        if (cfl_hbit_read_bit(inst, ALARM, VALVE_12, 0)) {
            // Found it: VALVE_12 has error
            handle_valve_error(VALVE_12);
        }
    }
}
```

**Tree depth = log₂(n)** for balanced trees. Find 1 error among 1,024 devices in 10 bit checks instead of 1,024 scans.

#### C. Hierarchical Error Handlers

Attach handlers at appropriate tree levels:

```c
// Handle entire station offline
if (cfl_hbit_read_bit(inst, COMM_FAIL, STATION_2, 0)) {
    station_offline_handler(STATION_2);
    return;  // Don't check individual valves
}

// Handle zone-level alarms
if (cfl_hbit_read_bit(inst, PRESSURE_ALARM, STATION_2_ZONE_A, 0)) {
    zone_pressure_handler(STATION_2_ZONE_A);
}

// Handle individual device alarms
if (cfl_hbit_read_bit(inst, LEAK, VALVE_12, 0)) {
    valve_leak_handler(VALVE_12);
}
```

**Error handling matches system architecture** - don't handle individual valves if the entire station is offline.

#### D. Tree-Based Organization

The bitmap **is** the tree structure:

```
                    ROOT (1 bit)
                    /          \
            STATION_1(1)    STATION_2(1)
            /      \         /        \
       ZONE_A(1) ZONE_B(1) ZONE_A(1) ZONE_B(1)
         /   \      /   \     /   \     /   \
    V1(8) V2(8) V3(8) V4(8) V5(8) V6(8) V7(8) V8(8)
```

- **Leaves** = actual hardware bits (8 bits per valve)
- **Parents** = automatic OR/AND aggregation
- **Root** = entire system summary

---

## 2. Buffer Types: Three Propagation Modes

Each hierarchical bitmap tree operates with one of three **buffer types** that control how bits propagate up the tree:

### OR_LATCH - Latching Alarms

**Behavior:** 
- Parent bit = OR of all children's bits
- **Latches on 0→1 transition** and remains set
- Must be **manually cleared** even if source clears

**Use Case:** Critical alarms that require acknowledgment

```c
// Valve detects overcurrent
set_bit(VALVE_12, OVERCURRENT);  
sync_and_propagate();

// Immediately propagates up tree
ZONE_A[overcurrent] = 1    // Latched
STATION_2[overcurrent] = 1 // Latched  
ROOT[overcurrent] = 1      // Latched

// Even if overcurrent clears at valve:
clear_bit(VALVE_12, OVERCURRENT);
sync_and_propagate();

// Latches remain set!
ZONE_A[overcurrent] = 1    // Still latched
STATION_2[overcurrent] = 1 // Still latched

// Must explicitly clear latch
clear_latch(ZONE_A, OVERCURRENT);
```

**Latch Buffer Layout:**
```
current[...]   - Active bit values (updated by propagation)
latched[...]   - Latched bits (OR'd with current on each sync)
```

### OR_MASK - Masked Propagation

**Behavior:**
- Parent bit = OR of (children's bits AND their masks)
- Each bit has a corresponding **mask bit**
- Only masked bits propagate up tree

**Use Case:** Alarms with enable/disable control

```c
// Valve has leak, but leak monitoring is disabled
set_bit(VALVE_12, LEAK_ALARM);
set_mask_bit(VALVE_12, LEAK_ALARM, false);  // Disable this alarm
sync_and_propagate();

// Does NOT propagate (masked out)
ZONE_A[leak] = 0     // Blocked by mask
STATION_2[leak] = 0  // Not propagated
ROOT[leak] = 0       // Not propagated

// Enable leak monitoring
set_mask_bit(VALVE_12, LEAK_ALARM, true);
sync_and_propagate();

// NOW it propagates
ZONE_A[leak] = 1     // Unmasked
STATION_2[leak] = 1  // Propagated
ROOT[leak] = 1       // Propagated
```

**Mask Buffer Layout:**
```
current[...]   - Active bit values (updated by propagation)
mask[...]      - Enable/disable mask (controls propagation)
```

**Propagation Logic:**
```c
parent_bit = (child1_bit & child1_mask) | (child2_bit & child2_mask) | ...
```

### AND - Ready/Status Indicators

**Behavior:**
- Parent bit = AND of all children's bits
- Parent set **only if ALL children are set**

**Use Case:** "All ready" or "all healthy" indicators

```c
// Check if entire zone is ready to operate
set_bit(VALVE_12, READY);
set_bit(VALVE_13, READY);
set_bit(VALVE_14, READY);  // All valves ready
set_bit(VALVE_15, READY);
sync_and_propagate();

// AND propagates up
ZONE_A[ready] = 1        // All children ready
STATION_2[ready] = ?     // Depends on ZONE_B

// One valve goes not-ready
clear_bit(VALVE_14, READY);
sync_and_propagate();

// AND collapses
ZONE_A[ready] = 0        // One child not ready
STATION_2[ready] = 0     // Propagates down
ROOT[ready] = 0          // Entire system not ready
```

**AND Buffer Layout:**
```
current[...]   - Active bit values (updated by propagation)
```

**Propagation Logic:**
```c
parent_bit = child1_bit & child2_bit & child3_bit & ...
```

---

## 2.5 Runtime Node and Buffer Lookup

The DSL generates **hash-based lookup tables** for runtime string-to-index resolution.

### Node Lookup by Path String

Every node has a unique **hierarchical path** defined in the DSL:

```lua
-- DSL definition creates path
S.node("VALVE_STATUS", "Root")
  S.node("STATION_2_VALVE_STATUS", "Station")
    S.node("BANK_2_VALVE_STATUS", "Valve")
    S.end_node()
  S.end_node()
S.end_node()
```

**Runtime lookup:**
```c
// Find node by full path string
int16_t node = cfl_hbit_find_node_path(inst, 
    "VALVE_STATUS.STATION_2_VALVE_STATUS.BANK_2_VALVE_STATUS");

// Or use generated hash constant (faster - no string hashing)
int16_t node = cfl_hbit_find_node(inst, 
    IRRIGATION_HASH_VALVE_STATUS_STATION_2_VALVE_STATUS_BANK_2_VALVE_STATUS);
```

### Buffer Lookup by Name

```c
// Find buffer index by name
int16_t buf = irrigation_valves_find_buffer("ALARM_MASK");

// Or use generated enum (compile-time constant)
uint16_t buf = IRRIGATION_VALVES_BUF_ALARM_MASK;
```

### Why 32-bit Hashes Instead of Strings?

**String comparison is expensive:**
```c
// Slow: O(n) strcmp for each lookup
for (int i = 0; i < node_count; i++) {
    if (strcmp(path, node_paths[i]) == 0) return i;
}
```

**Hash comparison is fast:**
```c
// Fast: O(log n) binary search on integers
uint32_t hash = fnv1a_hash(path);  // O(n) - done once
int lo = 0, hi = node_count - 1;
while (lo <= hi) {
    int mid = (lo + hi) / 2;
    if (hash_table[mid].hash == hash) return hash_table[mid].index;
    if (hash_table[mid].hash < hash) lo = mid + 1;
    else hi = mid - 1;
}
```

**Code generation pre-computes hashes:**
```c
// Generated at compile time - no runtime cost
#define IRRIGATION_HASH_VALVE_STATUS_STATION_2 0x4065F0D9U
```

**Lookup performance:**
- String comparison: **O(m×n)** where m = string length, n = node count
- Hash comparison: **O(m + log n)** - hash once, binary search
- Direct enum: **O(1)** - compile-time constant

---

## 3. Hierarchical Bitmaps Are NOT All-Encompassing

### Anti-Pattern: The Monolithic Status Tree

**DON'T** try to model your entire system in one giant tree:

```
❌ BAD: Everything in one tree
                     FACTORY
                    /   |   \   \
              HVAC  POWER SAFETY PRODUCTION
               |      |     |        |
            [100s of nodes deep...]
```

**Problems:**
- Tree becomes unmanageably deep (6+ levels)
- Unrelated subsystems coupled together
- Propagation overhead grows
- Different update rates conflict
- Hard to reason about dependencies

---

## 4. Design Pattern: Multiple Focused Trees

### Best Practice: Function-Specific Trees

Create **separate hierarchical bitmap trees** for distinct functional domains:

#### Example: Irrigation System

**Tree 1: PLC Communication Status**
```
PLC_COMM_STATUS (OR_LATCH)
├── PLC_1_COMM
│   ├── LINK_STATUS
│   ├── WATCHDOG
│   └── CRC_ERRORS
├── PLC_2_COMM
└── PLC_3_COMM
```
**Purpose:** Track communication health with all PLCs

**Tree 2: Valve Alarms** (from PLCs)
```
VALVE_ALARMS (OR_LATCH)
├── STATION_1
│   ├── BANK_A (8 valves × 8 alarm bits)
│   └── BANK_B
└── STATION_2
```
**Purpose:** Track physical valve faults reported by PLCs

**Tree 3: Irrigation Zones Ready**
```
ZONES_READY (AND)
├── ZONE_1
│   ├── PRESSURE_OK
│   ├── FLOW_OK
│   └── VALVES_READY
├── ZONE_2
└── ZONE_3
```
**Purpose:** Track which zones are ready to irrigate

#### A. Each Tree Represents a Specific Function

**Examples of good functional decomposition:**

| Tree | Buffer Type | Purpose | Update Rate |
|------|-------------|---------|-------------|
| `PLC_COMM` | OR_LATCH | Communication faults | 100ms |
| `VALVE_ALARMS` | OR_MASK | Physical valve faults | 1s |
| `PUMP_STATUS` | AND | Pump readiness | 500ms |
| `ZONE_ENABLES` | OR_MASK | Manual enable/disable | On change |
| `FLOW_SENSORS` | OR_LATCH | Flow measurement errors | 2s |

**Benefits:**
- Each tree updates at its natural rate
- Clear ownership and responsibility
- Independent testing and debugging
- Isolated failure domains

#### B. Application Logic Connects Trees

**Higher-order decisions combine trees:**

```c
// Application logic synthesizes multiple trees
bool can_irrigate_zone_1() {
    // Check communication is healthy
    bool plc_ok = !cfl_hbit_read_bit(plc_comm_inst, COMM_FAIL, PLC_1, 0);
    
    // Check no valve alarms in this zone
    bool valves_ok = !cfl_hbit_read_bit(valve_alarm_inst, ALARM, ZONE_1, 0);
    
    // Check zone is ready
    bool ready = cfl_hbit_read_bit(zone_ready_inst, READY, ZONE_1, 0);
    
    // Check zone is enabled
    bool enabled = cfl_hbit_read_bit(zone_enable_inst, ENABLE, ZONE_1, 0);
    
    return plc_ok && valves_ok && ready && enabled;
}
```

**This application logic could feed a higher-level tree:**

```c
// Update high-level irrigation control tree
if (can_irrigate_zone_1()) {
    set_bit(irrigation_control_inst, ZONE_1_AVAILABLE);
} else {
    clear_bit(irrigation_control_inst, ZONE_1_AVAILABLE);
}
```

**Tree Hierarchy:**
```
Level 1: Hardware Trees (PLC_COMM, VALVE_ALARMS, SENSORS)
            ↓ (read by application logic)
Level 2: Application Logic (decision rules, interlocks)
            ↓ (writes to higher tree)
Level 3: Control Trees (IRRIGATION_CONTROL, SCHEDULING)
```

### Guidelines for Tree Decomposition

✓ **One tree per data source** (each PLC bank gets its own tree)  
✓ **One tree per functional domain** (alarms, status, enables separate)  
✓ **Similar update rates** within a tree  
✓ **3-5 levels deep maximum** per tree  
✓ **Clear boundaries** between trees  

✗ **Don't mix unrelated hardware** in one tree  
✗ **Don't mix different update rates** in one tree  
✗ **Don't create trees deeper than 5 levels**  
✗ **Don't try to model everything** in one mega-tree  

---

## 5. Runtime Structure: The Instance

The hierarchical bitmap runtime is managed by a C structure instance:

```c
cfl_hbit_instance_t* inst = cfl_hbit_create(&allocator, &config);
```

### Instance Memory Layout

```
┌─────────────────────────────────────────┐
│ cfl_hbit_instance_t                     │
├─────────────────────────────────────────┤
│ config (ROM pointer)                    │  ← Generated by DSL
│ allocator (callbacks)                   │  ← User-provided
│ ram (allocated block)                   │  ← Size computed by codegen
│   ├─ current[buf0, buf1, buf2]         │  ← Active bit values
│   ├─ shadow[buf0, buf1, buf2]          │  ← Pending writes
│   ├─ latched[buf0] (OR_LATCH only)     │  ← Latched bits
│   ├─ mask[buf1] (OR_MASK only)         │  ← Enable masks
│   └─ dirty_nodes[bitmap]               │  ← Changed node tracking
└─────────────────────────────────────────┘
```

### A. Shadow and Current Buffers

**All writes go to shadow buffers:**

```c
// Write to shadow (not visible yet)
cfl_hbit_shadow_set_bit(inst, ALARM_BUF, VALVE_12, OVERCURRENT);
cfl_hbit_shadow_set_bit(inst, ALARM_BUF, VALVE_15, LEAK);
cfl_hbit_shadow_set_bit(inst, ALARM_BUF, PUMP_3, OVERLOAD);

// Reads still see old values (shadow not active)
bool alarm = cfl_hbit_read_bit(inst, ALARM_BUF, VALVE_12, OVERCURRENT);  
// Returns false - shadow not synced yet
```

**Why shadow buffers?**
- **Atomic updates**: All writes take effect simultaneously
- **Consistent reads**: Application sees stable state during processing
- **Staged propagation**: Collect all changes before propagating up tree

### B. Writes Go to Shadow

```c
// During control cycle: accumulate changes in shadow
for (int plc = 0; plc < NUM_PLCS; plc++) {
    uint8_t* alarm_data = read_plc_alarms(plc);
    cfl_hbit_shadow_write(inst, ALARM_BUF, plc_nodes[plc], alarm_data, 8);
}

// Shadow is dirty, current is unchanged
// Application reads stable current buffer
```

### C. Sync and Propagate at Tick Boundary

```c
void control_loop() {
    // 1. Sync: Shadow → Current (atomic update)
    // 2. Propagate: Leaf → Root (tree traversal)
    cfl_hbit_sync_and_propagate(inst);
    
    // Now all writes are visible and propagated
    
    // 3. Application logic reads current state
    if (cfl_hbit_read_bit(inst, ALARM, ROOT, 0)) {
        handle_global_alarm();
    }
    
    // 4. Accumulate new changes in shadow for next tick
    poll_hardware_and_write_to_shadow();
}
```

**Sync and Propagate Algorithm:**

```c
void cfl_hbit_sync_and_propagate(inst) {
    // 1. SYNC: Copy shadow → current for all dirty leaves
    for each dirty_leaf {
        memcpy(current[leaf], shadow[leaf], size);
        
        // Update latch (OR_LATCH only)
        if (buffer_type == OR_LATCH) {
            latched[leaf] |= current[leaf];  // Latch on 0→1 edge
        }
    }
    
    // 2. Mark all ancestors of dirty leaves
    for each dirty_leaf {
        node = parent[dirty_leaf];
        while (node != ROOT) {
            mark_dirty(node);
            node = parent[node];
        }
    }
    
    // 3. PROPAGATE: Walk tree bottom-up, recompute parents
    for node in reverse_order {  // Children before parents
        if (!is_dirty(node)) continue;
        
        if (buffer_type == OR_LATCH) {
            current[node] = 0;
            latched[node] = 0;
            for each child {
                current[node] |= current[child];
                latched[node] |= latched[child];
            }
        }
        else if (buffer_type == OR_MASK) {
            current[node] = 0;
            for each child {
                current[node] |= (current[child] & mask[child]);
            }
        }
        else if (buffer_type == AND) {
            current[node] = 0xFF;  // Start with all 1s
            for each child {
                current[node] &= current[child];
            }
        }
    }
    
    // 4. Clear dirty flags
    clear_all_dirty();
}
```

### D. Mask and Latch Operations

#### Mask (OR_MASK buffers only)

**Mask acts as logical AND:**

```c
// Disable leak alarm at specific valve
cfl_hbit_set_mask_bit(inst, ALARM_BUF, VALVE_12, LEAK, false);

// Propagation: parent = OR of (child & mask)
parent_bit = (valve_12_leak & 0) | (valve_13_leak & 1) | ...
           = 0 | valve_13_leak | ...  // VALVE_12 leak doesn't propagate
```

**Use cases:**
- Maintenance mode (suppress alarms during service)
- Conditional monitoring (enable/disable checks based on state)
- Alarm filtering (suppress nuisance alarms)

#### Latch (OR_LATCH buffers only)

**Latch performs logical OR on 0→1 edge:**

```c
// Valve detects overcurrent (0 → 1 transition)
cfl_hbit_shadow_set_bit(inst, ALARM_BUF, VALVE_12, OVERCURRENT);
cfl_hbit_sync_and_propagate(inst);

// Current and latched both set
current[VALVE_12][overcurrent] = 1;
latched[VALVE_12][overcurrent] = 1;  // Latched!

// Overcurrent clears at valve (1 → 0 transition)
cfl_hbit_shadow_clear_bit(inst, ALARM_BUF, VALVE_12, OVERCURRENT);
cfl_hbit_sync_and_propagate(inst);

// Current clears, but latch remains
current[VALVE_12][overcurrent] = 0;
latched[VALVE_12][overcurrent] = 1;  // Still latched!

// Application reads latched state
bool alarm = cfl_hbit_read_latched_bit(inst, ALARM_BUF, VALVE_12, OVERCURRENT);
// Returns true - requires acknowledgment

// Operator clears latch
cfl_hbit_clear_latch_bit(inst, ALARM_BUF, VALVE_12, OVERCURRENT);

// Now fully cleared
latched[VALVE_12][overcurrent] = 0;
```

**Use cases:**
- Critical alarms requiring acknowledgment
- Fault logging (alarm occurred at some point)
- Operator notification (don't miss transient events)

---

## 6. Controller Structure: PLC Bank Mapping

The `cfl_hbit_controller_t` structure provides a **PLC-centric view** of the bitmap tree for systems where **multiple PLCs each control a bank of devices**.

### Motivation: The PLC Bank Pattern

**Common industrial architecture:**
```
┌─────────────────────────────────────────────────────────┐
│                    SCADA / HMI System                   │
└──────────────────┬──────────────────────────────────────┘
                   │
        ┌──────────┴──────────┬────────────┬──────────────┐
        │                     │            │              │
    ┌───▼───┐           ┌───▼───┐    ┌───▼───┐      ┌───▼───┐
    │ PLC 1 │           │ PLC 2 │    │ PLC 3 │      │ PLC 4 │
    └───┬───┘           └───┬───┘    └───┬───┘      └───┬───┘
        │                   │            │              │
   ┌────┴────┐         ┌────┴────┐  ┌────┴────┐    ┌────┴────┐
   │ Bank A  │         │ Bank B  │  │ Bank C  │    │ Bank D  │
   │ 8 valves│         │ 8 valves│  │ 8 valves│    │ 8 valves│
   │ 8 bits  │         │ 8 bits  │  │ 8 bits  │    │ 8 bits  │
   │ each    │         │ each    │  │ each    │    │ each    │
   └─────────┘         └─────────┘  └─────────┘    └─────────┘
```

**Each PLC:**
- Controls one **bank** (group of valves/devices)
- Reports status as **packed byte array** (e.g., 8 valves × 8 bits = 64 bits = 8 bytes)
- Updates at its own rate (100ms - 1s)

### Controller Creation

```c
// Create controller for STATION_2 node in ALARM buffer
cfl_hbit_controller_t* ctrl = cfl_hbit_controller_create(
    inst, 
    IRRIGATION_NODE_VALVE_STATUS_STATION_2,  // Parent node
    IRRIGATION_BUF_ALARM);                   // Buffer to control
```

**What happens during creation:**

1. **Identify second-level children** (immediate children of parent):
```
STATION_2 (parent)
├── BANK_1 (child 0) ← Second level from bottom
│   ├── VALVE_1 (leaf)
│   ├── VALVE_2 (leaf)
│   └── VALVE_3 (leaf)
├── BANK_2 (child 1) ← Second level from bottom
│   ├── VALVE_1 (leaf)
│   └── VALVE_2 (leaf)
└── BANK_3 (child 2) ← Second level from bottom
    └── VALVE_1 (leaf)
```

2. **Collect all leaf descendants** under each child
3. **Create two mapping modes**:
   - **Flat bitmap** (all bits linearly indexed from parent)
   - **Child-relative bitmap** (bits indexed per child node)

### Two Mapping Modes

#### Mode 1: Flat Bitmap Indexing

**Maps bits linearly from parent perspective:**

```c
// Get which leaf node and bit for flat index 42
uint8_t bit;
int16_t node = cfl_hbit_controller_get_bitmap_node(ctrl, 42, &bit);

// Example: Index 42 might map to:
//   node = VALVE_STATUS.STATION_2.BANK_2.VALVE_1
//   bit = 2
```

**Visual representation:**

```
Parent: STATION_2 ALARM buffer
        Flat index: 0  1  2  3  4  5  6  7 | 8  9 10 11 12 13 14 15 | 16 17 18 19 ...
                    ↓  ↓  ↓  ↓  ↓  ↓  ↓  ↓   ↓  ↓  ↓  ↓  ↓  ↓  ↓  ↓    ↓  ↓  ↓  ↓
        Child 0:    [BANK_1.VALVE_1------] | [BANK_1.VALVE_2------] | [BANK_1.VALVE_3-...]
        Child 1:    [BANK_2.VALVE_1------] | [BANK_2.VALVE_2------]
        Child 2:    [BANK_3.VALVE_1------]
                    
Each valve has 8 bits, so:
  Index 0-7:   BANK_1.VALVE_1 bits 0-7
  Index 8-15:  BANK_1.VALVE_2 bits 0-7
  Index 16-23: BANK_1.VALVE_3 bits 0-7
  Index 24-31: BANK_2.VALVE_1 bits 0-7
  ...
```

**Use case:** Iterate all bits under a parent
```c
// Scan all alarm bits in STATION_2
for (uint16_t i = 0; i < ctrl->total_bits; i++) {
    if (cfl_hbit_controller_read_bit(ctrl, i)) {
        uint8_t bit;
        int16_t node = cfl_hbit_controller_get_bitmap_node(ctrl, i, &bit);
        printf("Alarm at node %d, bit %d\n", node, bit);
    }
}
```

#### Mode 2: Child-Relative Indexing

**Maps bits relative to each child node:**

```c
// Get which leaf node and bit for child 1, bit 18
uint8_t bit;
int16_t node = cfl_hbit_controller_get_node_bit(
    ctrl,
    1,   // Child index (BANK_2)
    18,  // Bit within that child
    &bit);

// Returns:
//   node = VALVE_STATUS.STATION_2.BANK_2.VALVE_3
//   bit = 2 (because 18 = 2*8 + 2)
```

**Visual representation:**

```
Child 0 (BANK_1):
    Relative index: 0  1  2  3  4  5  6  7 | 8  9 10 11 12 13 14 15 | 16 17 18 ...
                    [    VALVE_1         ] | [    VALVE_2         ] | [VALVE_3...]
                    
Child 1 (BANK_2):
    Relative index: 0  1  2  3  4  5  6  7 | 8  9 10 11 12 13 14 15
                    [    VALVE_1         ] | [    VALVE_2         ]
                    
Child 2 (BANK_3):
    Relative index: 0  1  2  3  4  5  6  7
                    [    VALVE_1         ]
```

**Use case:** Process PLC data banks directly

```c
// Read 8 bytes from PLC 2 (controls BANK_2)
uint8_t plc_data[8];
read_modbus(PLC_2_ADDRESS, plc_data, 8);

// Write to shadow buffer using child-relative mapping
for (int byte_idx = 0; byte_idx < 8; byte_idx++) {
    for (int bit_idx = 0; bit_idx < 8; bit_idx++) {
        uint16_t child_bit = byte_idx * 8 + bit_idx;
        
        if (plc_data[byte_idx] & (1 << bit_idx)) {
            cfl_hbit_controller_set_child_bit(ctrl, 1, child_bit);  // Child 1 = BANK_2
        } else {
            cfl_hbit_controller_clear_child_bit(ctrl, 1, child_bit);
        }
    }
}
```

### Diagram: Controller Mapping

```
┌──────────────────────────────────────────────────────────────────────┐
│                        STATION_2 (Parent Node)                       │
└────────────────┬─────────────────┬─────────────────┬─────────────────┘
                 │                 │                 │
       ┌─────────▼────────┐ ┌─────▼────────┐ ┌─────▼────────┐
       │  BANK_1 (child 0)│ │ BANK_2 (ch 1)│ │ BANK_3 (ch 2)│
       └─────────┬────────┘ └─────┬────────┘ └─────┬────────┘
                 │                 │                 │
        ┌────────┴────────┐        │         ┌──────▼──────┐
        │        │        │        │         │             │
    ┌───▼──┐ ┌──▼──┐ ┌──▼──┐  ┌──▼──┐  ┌──▼──┐      
    │VALVE1│ │VALVE2│ │VALVE3│  │VALVE1│  │VALVE1│
    │8 bits│ │8 bits│ │8 bits│  │8 bits│  │8 bits│
    └──────┘ └──────┘ └──────┘  └──────┘  └──────┘

FLAT BITMAP MAPPING (from parent):
┌──────────────────────────────────────────────────────────────────────┐
│ Flat Index:  0...7  8..15 16..23 24..31 32..39                      │
│              ▼      ▼     ▼      ▼      ▼                            │
│ Maps to:     V1     V2    V3     V1     V1                           │
│ (child):    (ch0)  (ch0) (ch0)  (ch1)  (ch2)                        │
└──────────────────────────────────────────────────────────────────────┘

CHILD-RELATIVE MAPPING (per child):
┌──────────────────────────────────────────────────────────────────────┐
│ Child 0 (BANK_1):                                                    │
│   Child Index: 0...7   8..15  16..23                                │
│   Maps to:     V1      V2     V3                                    │
│                                                                      │
│ Child 1 (BANK_2):                                                    │
│   Child Index: 0...7                                                │
│   Maps to:     V1                                                   │
│                                                                      │
│ Child 2 (BANK_3):                                                    │
│   Child Index: 0...7                                                │
│   Maps to:     V1                                                   │
└──────────────────────────────────────────────────────────────────────┘

USAGE PATTERN:
┌─────────────────────────────────────────────────────────────────────┐
│  // PLC 1 controls BANK_1 (child 0)                                │
│  uint8_t bank1_data[3*8]; // 3 valves × 8 bits                    │
│  read_plc(PLC_1, bank1_data, 24);                                 │
│                                                                     │
│  for (int bit = 0; bit < 24; bit++) {                             │
│      if (test_bit(bank1_data, bit)) {                             │
│          cfl_hbit_controller_set_child_bit(ctrl, 0, bit); // ←────┤
│      }                                                             │
│  }                                     Child-relative indexing     │
│                                                                     │
│  // Later: scan all alarms in STATION_2                           │
│  for (int i = 0; i < ctrl->total_bits; i++) {                     │
│      if (cfl_hbit_controller_read_bit(ctrl, i)) { // ←────────────┤
│          uint8_t bit;                                              │
│          int16_t node = get_bitmap_node(ctrl, i, &bit);           │
│          handle_alarm(node, bit);        Flat indexing            │
│      }                                                             │
│  }                                                                 │
└─────────────────────────────────────────────────────────────────────┘
```

### Why This Mapping Matters

**C structures follow DSL ordering** - the tree structure in the DSL is preserved in memory layout:

```lua
-- DSL defines this order
S.node("STATION_2", "StationAggregate")
  S.node("BANK_1", "BankAggregate")    -- First child
    S.node("VALVE_1", "Valve")          -- First leaf
    S.end_node()
    S.node("VALVE_2", "Valve")          -- Second leaf
    S.end_node()
  S.end_node()
  S.node("BANK_2", "BankAggregate")    -- Second child
    S.node("VALVE_1", "Valve")          -- Third leaf
    S.end_node()
  S.end_node()
S.end_node()
```

**Generated node array preserves this depth-first order:**

```c
static const node_t nodes[] = {
    // index 0: STATION_2 (parent)
    // index 1: BANK_1 (child 0)
    // index 2: VALVE_1 (leaf 0 under child 0)
    // index 3: VALVE_2 (leaf 1 under child 0)
    // index 4: BANK_2 (child 1)
    // index 5: VALVE_1 (leaf 2 under child 1)
    ...
};
```

**Controller can compute mappings efficiently** because children are contiguous in both DSL order and memory layout.

---

## 7. Error Detection and Analysis

Error processing will be described in detail in the **Runtime Operations Manual** (separate document).

**Brief overview of error processing workflow:**

1. **Detection** - Controller identifies which bits are set
2. **Location** - Maps flat/child indices back to specific nodes
3. **Collection** - Gathers all error bits with monitoring node attribution
4. **Analysis** - Application logic processes errors by priority/severity
5. **Handling** - Appropriate action taken (alarm, shutdown, logging)
6. **Acknowledgment** - Latch clearing after operator review

For complete documentation on error collection functions and monitoring node attribution, see **RUNTIME.md**.

---

## Summary: When and Why to Use Hierarchical Bitmaps

### Use Hierarchical Bitmaps When:

✓ **Monitoring many distributed status bits** (hundreds to thousands)  
✓ **Need fast "any error" check** (O(1) global health check)  
✓ **Hardware organized hierarchically** (stations → banks → devices)  
✓ **Multiple PLCs or data sources** (one tree per bank)  
✓ **Different buffer behaviors needed** (alarms latch, status doesn't)  
✓ **System has natural aggregation levels** (zone, station, bank)  

### Don't Use Hierarchical Bitmaps When:

✗ **Few bits to monitor** (<50 bits - just use arrays)  
✗ **Flat structure** (no natural hierarchy)  
✗ **Complex bit relationships** (bits interact in non-hierarchical ways)  
✗ **Dynamic structure** (tree changes frequently at runtime)  
✗ **String-heavy processing** (need rich metadata, not just bits)  

### Key Advantages:

1. **Performance** - O(1) global check, O(log n) error location
2. **Memory efficiency** - Packed bits, shared parent nodes
3. **Code clarity** - Structure matches physical system
4. **Type safety** - Compile-time constants for all identifiers
5. **Zero runtime overhead** - Tree structure in ROM
6. **Flexible buffer types** - OR_LATCH, OR_MASK, AND for different needs
7. **Scalable** - Add nodes without changing code structure

### Design Philosophy:

> **"The map is the territory"**
>
> Your hierarchical bitmap tree should mirror your physical system architecture. If you can draw your hardware as a tree, you can model it as a hierarchical bitmap. Keep trees focused, let application logic connect them, and leverage the natural aggregation hierarchy for efficient monitoring and control.

---

**Next:** See **RUNTIME.md** for detailed runtime API documentation and error processing workflows.
```