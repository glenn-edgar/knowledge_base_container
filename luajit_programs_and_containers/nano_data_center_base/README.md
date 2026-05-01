# nano_data_center_base

Platform tree for the nano data center. App-agnostic. Site-agnostic.

This is the **default cluster** as of 2026-05-01. The previous tree at
`../building_blocks/` is retained for historical reference and is no
longer used in production.

## Layout

```
nano_data_center_base/
├── luajit/
│   ├── luajit_base/                container image: nanodatacenter/luajit-base:latest
│   └── openresty_base/             container image: nanodatacenter/openresty-base:latest
├── commissioning_software/
│   ├── infrastructure/             docker_host_broker (Go) — host docker + /proc adapter
│   ├── engines/                    chain_tree, s_expression
│   ├── kb/                         knowledge_base + dsl (postgres, sqlite3, nats, mqtt)
│   ├── system_node_control/        DCS host process (was building_blocks/nanodatacenter_dcs/)
│   ├── orchestrator/               graph + startup/shutdown helpers
│   └── validation/test_app/        Phase 6.4b smoke target
├── platform_containers/            DCS-managed core containers
│   ├── ops_container/              (placeholder; was nanodatacenter_gateway/)
│   ├── dcs_console/
│   ├── gateway/                    (placeholder for federated reverse proxy)
│   ├── observability/
│   └── system_api/                 (placeholder)
├── support_procedures/runbooks/
│   └── start_planner_system.sh
└── development/                    (gitignored; per-CPU staging during dev)
```

## Wire-up: `NDC_BASE` env var

All build/run scripts consume `NDC_BASE` (this directory's absolute path).
If unset, scripts climb relative to their own location to discover it.
Set it explicitly when running outside the tree.

## Build + boot (single CPU dev cluster on this laptop)

Prereq: pg-vector, nats-js-ram, mosquitto-ram-ws_main, kv-bridge already up
(via `commissioning_software/system_node_control/construction/install_infra.sh`).

```bash
export NDC_BASE=/home/gedgar/knowledge_base_assembly/luajit_programs_and_containers/nano_data_center_base

# 1. Build base images (can be parallelized; ordered here for clarity).
( cd $NDC_BASE/luajit/luajit_base/container             && bash docker_build.sh )
( cd $NDC_BASE/luajit/openresty_base/container          && bash docker_build.sh )
( cd $NDC_BASE/commissioning_software/infrastructure/docker_host_broker/container && bash docker_build.sh )
( cd $NDC_BASE/commissioning_software/validation/test_app/container               && bash docker_build.sh )
( cd $NDC_BASE/platform_containers/dcs_console/container        && bash docker_build.sh )
( cd $NDC_BASE/platform_containers/observability/container      && bash docker_build.sh )

# 2. Start docker-host-broker (NOT managed by node_control; bootstrap container).
#    SITE must match topology.site exactly, HTTP_ADDR must bind 0.0.0.0,
#    PG_DSN required so master can read snapshots.
PG_PW=$(docker exec pg-vector printenv POSTGRES_PASSWORD)
docker run -d --name docker-host-broker \
    --restart unless-stopped \
    -v /var/run/docker.sock:/var/run/docker.sock:ro \
    -v /proc:/host/proc:ro \
    --network planner-net \
    -p 127.0.0.1:9100:9100 \
    -e SITE=moonbase.alpha.dcs \
    -e NATS_URL=nats://nats-js-ram:4222 \
    -e HTTP_ADDR=0.0.0.0:9100 \
    -e PG_DSN="host=pg-vector port=5432 dbname=knowledge_base user=gedgar password=$PG_PW" \
    nanodatacenter/docker-host-broker:latest

# 3. Build KB + slice + stage.
cd $NDC_BASE/commissioning_software/system_node_control/construction
bash build_kb.sh
bash slice_bootstrap.sh
bash stage_deploy.sh --mode=dev

# 4. Edit env.sh in each per-CPU dir if needed (POSTGRES_PASSWORD, etc).

# 5. Boot.
DEP=$NDC_BASE/commissioning_software/system_node_control/deployment
( cd $DEP/cpu_01 && setsid nohup ./start.sh </dev/null >/dev/null 2>&1 & disown )
sleep 3
( cd $DEP/cpu_02 && setsid nohup ./start.sh </dev/null >/dev/null 2>&1 & disown )

# 6. Stop.
$DEP/cpu_01/stop.sh
$DEP/cpu_02/stop.sh
```

## Convergence target

```
master cpu_01:  sys_ready=true   node_op=true
slave  cpu_02:  sys_ready=false  node_op=true   (sys_ready is master-only by design)
```

Plus 5 DCS-managed containers up: `test_app_01`, `observability_01`,
`dcs_console_01`, `robot_manager_01`, `ros_mission_planner_ii_01`.

## Notes

- `development/` is gitignored. Per-CPU staging directories
  (`commissioning_software/system_node_control/deployment/cpu_*/`) are also
  gitignored because they hold generated `bootstrap.db`, `start.sh`, and
  operator-edited `env.sh`.
- Site config (currently `moonbase.alpha.dcs`) lives in
  `commissioning_software/system_node_control/construction/catalogs/topology.lua`.
  Site-tier overrides will live in the sibling `nano_data_center_instance/`
  repo when site/instance separation is enforced.
- `support_procedures/runbooks/commissioning.md` is queued but unwritten —
  three modes to document: first-time, subsequent boot, re-commission.
