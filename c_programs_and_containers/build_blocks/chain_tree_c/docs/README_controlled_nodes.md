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

## Use Cases

- **Drone flight modes**: takeoff, cruise, land, emergency — client switches between modes
- **Robot arm sequences**: pick, place, home — external planner controls sequence
- **Manufacturing steps**: setup, process, inspect — operator controls progression
- **Test harnesses**: enable/disable test phases from a coordinator

## Test Reference

- **Test 23 (twenty_seventh_test)**: Client controlled drone flight patterns
- **Test 24 (twenty_eighth_test)**: Client controlled node with exceptions

See [README_incremental_binary.md](README_incremental_binary.md) for the full test list.
