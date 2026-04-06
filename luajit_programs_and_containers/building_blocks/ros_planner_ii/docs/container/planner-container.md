# Planner Container

Single generic image: `nanodatacenter/ros-planner:latest` (150MB)

## Build

```bash
cd third_party_containers/ros_planner
./docker_build.sh
```

Stages Lua modules from `building_blocks/` into local `lua/` directory, copies prebuilt `.so` files, runs `docker build`. Fast (~5 seconds).

## Run

```bash
# Minimal — KB provides all connection details:
docker run --name surface-ops-planner \
  --restart unless-stopped --network host \
  -v /home/gedgar/Sqlite_Data:/data:ro \
  -e SQLITE_DB=/data/surface_ops.db \
  nanodatacenter/ros-planner:latest

# Different domain:
./docker_run.sh warehouse_ops

# With env var override:
MQTT_HOST=10.0.1.5 ./docker_run.sh
```

## Startup Flow

`bootstrap_container.lua`:

1. Query extracted SQLite for NATS/MQTT/site (KB infrastructure)
2. Build runtime KB if missing (`construct_surface_ops.lua`)
3. Build remote.json if missing (robot ChainTree DSL)
4. Export KB to NATS KV (34 keys)
5. `dofile()` planner_server.lua (runs in same process)

## Prebuilt Libraries

`prebuilt_libs/` contains aarch64 `.so` files copied from host:

- libnats.so, libnats_key_store.so, libnats_job_queue.so, libnats_pubsub.so, libnats_rpc.so, libnats_kb_store.so
- libmqtt_pubsub.so, liblua_cbor.so, libmqtt_kv_store.so, libmqtt_queue.so, libmqtt_luajit_adapter.so
- ltree.so (SQLite extension)

Same architecture as host — no in-container compilation.

## Environment Variables

| Variable | Required | Source |
|----------|----------|--------|
| SQLITE_DB | Yes | Path to extracted SQLite DB |
| MQTT_HOST | No | Override KB mqtt host |
| MQTT_PORT | No | Override KB mqtt port |
| NATS_SERVER | No | Override KB nats server |
| VMRT_KB_SITE | No | Override KB domain site |
