# Streaming Pipeline Tutorial

ChainTree's streaming subsystem provides a typed packet pipeline for embedded wire protocols. Packets flow through tap → filter → transform → sink nodes, with schema verification at each stage.

## Concepts

- **Port**: Typed connection point defined by schema hash, handler ID, and event ID
- **Packet**: Fixed-layout C struct with an `avro_packet_header_t` prefix (schema hash, sequence number, timestamp, source node)
- **Tap**: Generates packets on a timer or event
- **Sink**: Receives and processes packets
- **Filter**: Passes or blocks packets based on a boolean function
- **Transform**: Reads input packet, produces output packet
- **Collect**: Aggregates packets from multiple sources

## DSL Usage

### Define Ports

```lua
-- make_port(schema_hash, handler_id, event_id)
local port_sensor = ct:make_port("sensor_packet_hash", 0, "CFL_SECOND_EVENT")
local port_motor  = ct:make_port("motor_packet_hash", 1, "CFL_TIMER_EVENT")
```

### Packet Generator (Tap)

```lua
ct:asm_streaming_emit_packet(
    "SENSOR_GENERATOR",          -- user boolean function (generates packet)
    { sample_rate = 10 },        -- function data
    event_column,                -- column that receives the event
    port_sensor                  -- output port
)
```

The boolean function is called each tick. It fills the packet and returns `true` to emit, `false` to skip.

### Packet Sink

```lua
ct:asm_streaming_sink_packet(
    "SENSOR_HANDLER",            -- user boolean function (processes packet)
    { log_level = 1 },           -- function data
    port_sensor                  -- input port (must match generator's port)
)
```

### Packet Filter

```lua
ct:asm_streaming_filter_packet(
    "RANGE_CHECK",               -- user boolean: true=pass, false=block
    { min = 0, max = 100 },
    port_sensor                  -- input port
)
```

### Packet Transform

```lua
ct:asm_streaming_transform_packet(
    "CELSIUS_TO_FAHRENHEIT",     -- user boolean function
    { offset = 32 },
    port_sensor,                 -- input port
    port_motor,                  -- output port
    output_event_column          -- column for output events
)
```

### Packet Collector

```lua
ct:asm_streaming_collect_packets(
    "AGGREGATE_READINGS",        -- user boolean function
    {},
    { port_sensor_a, port_sensor_b },  -- input ports (multiple)
    "COLLECTION_COMPLETE",       -- output event name
    output_event_column          -- column for output events
)
```

### Verified Sink

```lua
ct:asm_streaming_verify_packet(
    "VERIFY_SENSOR",             -- user boolean function
    {},
    port_sensor,                 -- input port
    verify_fn, reset_flag, timeout, error_fn, error_data
)
```

## Complete Pipeline Example

```lua
local function streaming_test(ct, kb_name)
    ct:start_test(kb_name)

    local col = ct:define_column("pipeline", nil, nil, nil, nil, nil, true)

        -- Define ports
        local port_raw = ct:make_port("raw_sensor", 0, "CFL_SECOND_EVENT")
        local port_filtered = ct:make_port("filtered_sensor", 1, "CFL_SECOND_EVENT")

        -- Generator → produces raw sensor packets
        local gen_col = ct:define_column("gen_events")
            ct:asm_streaming_emit_packet("SENSOR_GEN", {}, gen_col, port_raw)
        ct:end_column(gen_col)

        -- Filter → passes only valid readings
        ct:asm_streaming_filter_packet("RANGE_FILTER", {min=0, max=1000}, port_raw)

        -- Sink → processes filtered packets
        ct:asm_streaming_sink_packet("DATA_LOGGER", {}, port_raw)

        ct:asm_wait_time(10.0)
        ct:asm_terminate_system()

    ct:end_column(col)

    ct:end_test()
end
```

## User Function Signatures

All streaming user functions are boolean functions:

```c
// Generator — fill packet, return true to emit
bool sensor_gen_fn(void *handle, unsigned node_index,
                   unsigned event_type, unsigned event_id, void *event_data);

// Sink — process received packet
bool data_logger_fn(void *handle, unsigned node_index,
                    unsigned event_type, unsigned event_id, void *event_data);

// Filter — return true to pass, false to block
bool range_filter_fn(void *handle, unsigned node_index,
                     unsigned event_type, unsigned event_id, void *event_data);
```

The packet pointer is accessed via `event_data` when `event_id` matches the port's event.

## Avro Packet Format

Packets use a fixed header followed by application-defined fields:

```c
typedef struct {
    uint32_t schema_hash;    // identifies packet type
    uint32_t seq;            // sequence number (auto-incremented)
    double   timestamp;      // set by runtime
    unsigned source_node;    // generating node index
} avro_packet_header_t;

// Application packet extends the header:
typedef struct {
    avro_packet_header_t header;
    float temperature;
    float pressure;
    uint32_t sensor_id;
} sensor_packet_t;
```

See [avro/README_avro_commands.md](avro/README_avro_commands.md) and [avro/README_c_avro_packtes.md](avro/README_c_avro_packtes.md) for the Avro packet DSL and format details.
