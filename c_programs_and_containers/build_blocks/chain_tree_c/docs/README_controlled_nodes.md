# Controlled Node Patterns

Controlled nodes allow external clients to enable/disable subtrees at runtime, with optional exception handling for error recovery.

## Basic Controlled Node

A controlled node container wraps child columns that can be activated by client commands:

```lua
ct:define_controlled_node_container("drone_controller")

    ct:define_controlled_node("fly_up", false)
        ct:asm_log_message("flying up")
        ct:asm_wait_time(3.0)
        ct:asm_log_message("altitude reached")
    ct:end_controlled_node()

    ct:define_controlled_node("fly_down", false)
        ct:asm_log_message("descending")
        ct:asm_wait_time(3.0)
        ct:asm_log_message("landed")
    ct:end_controlled_node()

    ct:define_controlled_node("fly_straight", false)
        ct:asm_log_message("cruising")
        ct:asm_halt()
    ct:end_controlled_node()

ct:end_controlled_node_container()
```

The second parameter to `define_controlled_node` is `auto_start` — set to `false` so nodes wait for client activation.

## Client Control

A separate column acts as the client, enabling and disabling controlled nodes by reference:

```lua
local client_col = ct:define_column("client", nil, nil, nil, nil, nil, true)

    ct:asm_log_message("activating fly_up")
    ct:asm_enable_nodes({fly_up_node})
    ct:asm_wait_time(5.0)

    ct:asm_log_message("switching to fly_straight")
    ct:asm_disable_nodes({fly_up_node})
    ct:asm_enable_nodes({fly_straight_node})
    ct:asm_wait_time(10.0)

    ct:asm_log_message("activating fly_down")
    ct:asm_disable_nodes({fly_straight_node})
    ct:asm_enable_nodes({fly_down_node})
    ct:asm_wait_time(5.0)

    ct:asm_terminate_system()

ct:end_column(client_col)
```

## Client Controlled Node

For programmatic control via a boolean function:

```lua
ct:define_client_controlled_node("mission_controller", "MISSION_BOOLEAN_FN")
    ct:define_controlled_node("waypoint_1", false)
        -- ...
    ct:end_controlled_node()
    ct:define_controlled_node("waypoint_2", false)
        -- ...
    ct:end_controlled_node()
ct:end_client_controlled_node()
```

The boolean function receives events and returns `true` to indicate completion. It can enable/disable child nodes programmatically based on mission state.

## With Exception Handling

Controlled nodes can be wrapped in exception handlers for error recovery:

```lua
ct:define_exception_handler("flight_handler")

    ct:define_main_column("flight_main")
        ct:define_controlled_node_container("flight_modes")
            ct:define_controlled_node("takeoff", true)
                ct:asm_log_message("taking off")
                ct:asm_wait_time(3.0)
                -- If sensor fails here, exception propagates to handler
            ct:end_controlled_node()
        ct:end_controlled_node_container()
    ct:end_main_column()

    ct:define_recovery_column("emergency_land")
        ct:asm_log_message("emergency landing")
        ct:asm_wait_time(2.0)
    ct:end_recovery_column()

    ct:define_finalize_column("shutdown")
        ct:asm_log_message("motors off")
    ct:end_finalize_column()

ct:end_exception_handler()
```

## JSON Controlled Nodes

JSON controlled nodes replace the Avro-coupled variant with pure JSON text for request/response data. No schema hashes, handler IDs, or wire headers.

### Port Definition

```lua
-- JSON ports only need an event name (no file/record/handler_id)
local request_port  = ct:make_json_control_port("fly_straight_request")
local response_port = ct:make_json_control_port("fly_straight_response")
```

### Server (Dead Node)

```lua
local container = ct:controlled_node_container("control_container")

local server = ct:json_controlled_node(
    "api_fly_straight",         -- alias for client binding
    "fly_straight_column",      -- column name in tree
    "FLY_STRAIGHT_MONITOR",     -- user boolean (receives JSON request text)
    {},                         -- aux_data
    request_port, response_port
)
    ct:asm_log_message("fly straight: executing")
    ct:asm_wait_time(2)
    ct:asm_one_shot_handler("FLY_STRAIGHT_FINAL", {})  -- sets response text
    ct:asm_terminate()
ct:end_column(server)

ct:end_column(container)
```

### Client (Initiator)

```lua
-- aux_data is serialized to JSON at DSL time as the request text
ct:json_client_controlled_node(
    "api_fly_straight",                 -- matches server alias
    "ON_FLY_STRAIGHT_COMPLETE",         -- user boolean (receives JSON response)
    { command = "fly_straight",         -- becomes request_text JSON
      distance = 100.0,
      altitude = 50.0 },
    request_port, response_port
)
```

### Server Finalizer (Setting Response)

User one-shot functions set the response text using the `cfl_json_server_set_response_text()` helper:

```c
void fly_straight_final_one_shot_fn(void *handle, unsigned node_index) {
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;
    cfl_json_server_set_response_text(rt, node_index,
        "{\"status\":\"complete\",\"command\":\"fly_straight\"}");
}
```

### Key Differences from Avro Controlled Nodes

| Aspect | Avro | JSON |
|--------|------|------|
| Port | `cfl_port_t` (schema_hash, handler_id, event_id, packet_pointer, data_pointer) | `cfl_json_port_t` (event_id only) |
| Packet validation | `cfl_packet_matches_port` + schema hash | None needed — event_id routing |
| Source node | `cfl_avro_get_source_node()` from wire header | Client sets `client_node_index` on server struct |
| Request data | Pre-built Avro binary packet | JSON text string (char*) serialized at DSL time |
| Response data | Avro packet via `response_port.packet_pointer` | JSON text string via `cfl_json_server_set_response_text()` |
| Container | `CFL_CONTROLLED_NODE_CONTAINER_*` (reused as-is) | Same — format-agnostic |
| DSL functions | `controlled_node()`, `client_controlled_node()` | `json_controlled_node()`, `json_client_controlled_node()` |
| DSL ports | `make_control_port(file, record, handler_id, event)` | `make_json_control_port(event_name)` |

### Memory Model

- Server struct: arena-allocated via `cfl_smart_arena_alloc`, survives reset
- Client `request_text`: heap-allocated copy from node_dict, fixed for KB lifetime
- Server `response_text`: heap-allocated by user finalizer one-shot, sent to client on term
- Both structs include `cfl_json_heap_interface_t` for JSON parse/build operations

## CBOR Controlled Nodes

CBOR controlled nodes use the same client/server pattern as JSON but encode request/response data as CBOR bytes on the wire. User boolean functions still receive JSON text — the CBOR encode/decode is transparent.

### Port Definition

```lua
local request_port  = ct:make_cbor_control_port("fly_straight_request")
local response_port = ct:make_cbor_control_port("fly_straight_response")
```

### Server (Dead Node)

```lua
local container = ct:controlled_node_container("control_container")

local server = ct:cbor_controlled_node(
    "api_fly_straight",         -- alias for client binding
    "fly_straight_column",      -- column name in tree
    "FLY_STRAIGHT_MONITOR",     -- user boolean (receives JSON text decoded from CBOR)
    {},                         -- aux_data
    request_port, response_port
)
    ct:asm_log_message("fly straight: executing")
    ct:asm_wait_time(2)
    ct:asm_one_shot_handler("FLY_STRAIGHT_FINAL", {})
    ct:asm_terminate()
ct:end_column(server)

ct:end_column(container)
```

### Client (Initiator)

```lua
ct:cbor_client_controlled_node(
    "api_fly_straight",                 -- matches server alias
    "ON_FLY_STRAIGHT_COMPLETE",         -- user boolean (receives JSON text decoded from CBOR)
    { command = "fly_straight",         -- serialized to JSON, then CBOR at runtime
      distance = 100.0,
      altitude = 50.0 },
    request_port, response_port
)
```

### Server Finalizer

Same helper pattern as JSON — set response as JSON text, the CBOR term function encodes it:

```c
void fly_straight_final_one_shot_fn(void *handle, unsigned node_index) {
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;
    cfl_cbor_server_set_response_text(rt, node_index,
        "{\"status\":\"complete\",\"command\":\"fly_straight\"}");
}
```

### Data Flow

```
Client init:  Lua table → JSON text (DSL) → CBOR bytes (heap)
Client main:  Send CBOR request → server
Server main:  CBOR bytes → JSON text → user boolean monitor
Server term:  JSON response text → CBOR bytes → send to client
Client main:  CBOR bytes → JSON text → user boolean completion
```

### Key Differences from JSON Controlled Nodes

| Aspect | JSON | CBOR |
|--------|------|------|
| Wire format | JSON text (`char*`) | CBOR bytes (`cfl_cbor_buffer_t*`) |
| User boolean input | JSON text directly | JSON text (decoded from CBOR) |
| Response helper | `cfl_json_server_set_response_text()` | `cfl_cbor_server_set_response_text()` |
| Engine function ID | `CFL_FUNCTION_ID_JSON_CONTROLLED_NODE_MAIN (7)` | `CFL_FUNCTION_ID_CBOR_CONTROLLED_NODE_MAIN (8)` |
| DSL functions | `json_controlled_node()` | `cbor_controlled_node()` |
| DSL ports | `make_json_control_port()` | `make_cbor_control_port()` |

### Memory Model

- Server/client structs: arena-allocated via `cfl_smart_arena_alloc`, survives CFL_RESET
- Client `cbor_request`: heap-allocated at init, freed in client term
- Server `cbor_buffer`: heap-allocated at init, freed in server term after encoding response
- Server `response_text`: heap-allocated by user finalizer, freed after CBOR encode in server term
- Per-event decoded JSON text: heap-allocated, freed immediately after user boolean

## Use Cases

- **Drone flight modes**: takeoff, cruise, land, emergency — client switches between modes
- **Robot arm sequences**: pick, place, home — external planner controls sequence
- **Manufacturing steps**: setup, process, inspect — operator controls progression
- **Test harnesses**: enable/disable test phases from a coordinator
- **JSON-based robot control**: Pi Zero 2 / NATS robots using JSON protocol instead of Avro
- **CBOR-based embedded control**: Thread mesh / constrained devices where CBOR saves bandwidth

## Test Reference

### Avro Controlled Nodes (incremental_binary/)
- **Test 27 (twenty_seventh_test)**: Client controlled drone flight patterns
- **Test 28 (twenty_eighth_test)**: Client controlled node with exceptions

### JSON Controlled Nodes (json_packet_test/)
- **Test 4 (json_drone_control_test)**: JSON client/server drone control, 4 flight modes
- **Test 5 (json_drone_exception_test)**: JSON drone with exception handling

### CBOR Controlled Nodes (cbor_packet_test/)
- **Test 4 (cbor_drone_control_test)**: CBOR client/server drone control, 4 flight modes
- **Test 5 (cbor_drone_exception_test)**: CBOR drone with exception handling

See [README_incremental_binary.md](README_incremental_binary.md) for the full Avro test list.
