# Virtual Nodes

Virtual nodes (VNs) are the atomic actions a robot can execute. Each VN type is defined in the KB with a packet type ID, JSON schema, bitmask fields, and pose fields.

## VN Types (Lunar Rover)

| VN | Type ID | Category | Pose Fields |
|----|---------|----------|-------------|
| init_check | 1 | System | none |
| path_spline | 2 | Navigation | delta_x, delta_y, delta_heading |
| path_line | 3 | Navigation | delta_x, delta_y, delta_heading |
| path_wall | 4 | Navigation | delta_x, delta_y, delta_heading |
| path_rotate | 5 | Navigation | delta_heading |
| deliver_part | 6 | Task | delta_arm_angle |
| paint_sample | 7 | Task | delta_arm_angle |
| load_shipping | 8 | Task | delta_arm_angle |
| pass_gate | 9 | Task | delta_x, delta_y, delta_heading |
| inspection_scan | 10 | Task | none |
| idle | 11 | System | none |
| recharge | 12 | System | none |

## Hub-Side Execution

The hub runtime executes every VN with the same state machine:

```
idle → send_command → wait_ack (5s) → active (bitmaps) → wait_kb_done (10s) → done
```

No VN-specific code on the hub side. The only difference per VN is the `packet_type_id` included in the command JSON.

## Robot-Side Execution

The robot has a ChainTree with one worker KB per VN type. The controller dispatches incoming commands to the appropriate worker based on `packet_type_id`. Each worker:

1. Receives command parameters via blackboard
2. Executes the action (simulated tick countdown or real hardware)
3. Reports delta pose on completion
4. Sends KB_DONE with success/failure

## Adding a New VN

1. Add definition to `construct_surface_ops.lua`:
```lua
kb:add_info_node("vn_type", "new_action",
    { packet_type_id = 13 },
    { description = "...", json_schema = {...}, bitmask = {...}, pose_fields = {...} })
```

2. Add capability to robot class infra:
```lua
virtual_nodes = { ..., "new_action" }
```

3. Add worker to `ros_planner_ii_mqtt_robot/remote_dsl.lua`:
```lua
worker_new_action = make_worker("worker_new_action", "WKR_NEW_ACTION_MAIN", "WKR_NEW_ACTION_INIT")
```

4. Add worker functions to `remote_user_functions.lua`

No planner-side code changes needed.
