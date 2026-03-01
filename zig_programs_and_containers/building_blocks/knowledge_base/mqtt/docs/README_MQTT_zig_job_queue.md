# mqtt_queue — Zig MQTT Queue Library

Reliable queued messaging over MQTT v3.1.1 using persistent sessions.
Wraps the Mosquitto C client library (libmosquitto).

## Prerequisites

```bash
sudo apt install libmosquitto-dev
```

## Build

```bash
zig build                          # build .a and .so → zig-out/lib/
zig build -Doptimize=ReleaseFast   # optimized build
zig build test                     # run unit tests
zig build run-test                 # run integration test (requires broker)
```

## Output

| Artifact              | Path                           |
|-----------------------|--------------------------------|
| Static library        | `zig-out/lib/libmqtt_queue.a`  |
| Shared library        | `zig-out/lib/libmqtt_queue.so` |
| Integration test      | `zig-out/bin/mqtt_queue_test`  |