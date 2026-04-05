# Robots

Test robot used by the planner test suite. Independent robot processes
(NATS robot, MQTT robot) live in their own top-level directories:

- `../ros_planner_ii_nats_robot/` — standalone NATS robot (Pi Zero 2 template)
- `../ros_planner_ii_mqtt_robot/` — standalone MQTT robot (ESP32/Pico template)
- `../ros_planner_ii_mqtt_bridge/` — NATS-MQTT bridge (multi-robot)

Managed by `../start_planner_system.sh`.
