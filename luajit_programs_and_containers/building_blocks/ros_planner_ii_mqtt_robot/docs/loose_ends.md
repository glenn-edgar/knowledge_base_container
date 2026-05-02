# Loose ends

End-of-arc snapshot 2026-05-02. The container stack, L6 wire,
test-harness extraction, Phase-2 contract, mkdocs site, and
small-Pi deploy tooling are all in. This page lists what's known to
be incomplete and where the next pass should pick up.

## Phase-2 robot_controller (not yet implemented)

- Contract spec: ✅ `docs/controller/contract.md`.
- In-process fixture: ✅ `robot_controller_test_peer.lua` + 37/37 test.
- Real client: ❌ `robot_base/supervisor/upward_peer.lua` is still a
  no-op stub. Methods `register / tick / on_shutdown` return without
  doing transport.
- Real service: ❌ no container, no implementation. The
  `robot_controller` daemon is a future project.

**Recommended next step:** wire the stub `upward_peer.lua` against
the in-process test peer in a loopback test (`test_upward_peer_contract.lua`)
to confirm the harness rejects no-op responses with clear errors.
That makes the harness self-validating before any wire transport
shows up.

## Track B (Pico hardware port)

- Architecture proven on Linux + containers.
- Not yet started. Requires Pi Pico (~$5) + Pi Debug Probe (~$12).
- Track A scaffolding (bus_kernel + ext_bus headers, 3-fn contract)
  already platform-neutral; Zephyr port is a "fork-and-implement"
  exercise, not a redesign.
- See `docs/porting/new_hardware_target.md` for the bring-up sequence.

## ESP32 dongle prototype (parallel project)

- Locked 2026-05-02: 4-phase plan in
  `project_esp32_dongle_prototype` memory.
- Phase 1 (Arduino firmware + physics on Waveshare board): not yet
  started.
- Decoupled from this rover repo until Phase 3 (Zephyr port).

## mixed_* test-suite formalization

- L6 e2e validated via `test_random_paths --mode mixed`: the existing
  random scenario builder generates `deliver_part`, `paint_sample`,
  `inspection_scan`, `idle` interleaved with paths. Cmds=10 ack=10
  done=10 (10 ok / 0 fail) on 2026-05-02.
- BUT: there's no DEDICATED scenario test for each tool verb in
  isolation. We rely on randomness rolling those verbs eventually.
- **Suggested follow-up:** add `test_tool_scenarios.lua` exercising
  each L6 opcode explicitly (BEGIN_GRIP / RELEASE / DOCK / CHARGE /
  TOOL_MOVE) plus their failure modes (recharge-when-not-at-charger,
  grip-with-no-payload, etc). ~1 session using `planner_test_peer`.

## Tarball deploy: tunables.bin arch portability

- `make-deploy-tarball.sh` ships a pre-built `tunables.bin`. It's a
  flat C struct emitted by `build_drive_base_tunables.lua`, which is
  arch-independent at the byte level (LuaJIT FFI writes match
  packed-struct layout on x86_64 + aarch64).
- BUT: there's no compile-time guard against future tunables-schema
  drift between the host that built the tarball and the target. If
  schema_version bumps, the included `.bin` becomes wrong.
- **Mitigation:** `Makefile` has a `make tunables` target that
  rebuilds on the Pi. Documented in the bundle README.
- **Suggested follow-up:** make `start_rover.sh` validate
  schema_version via a luajit one-liner before running, fail-stop
  if mismatched.

## Tarball deploy: libmqtt_pubsub arch portability

- The bundle ships `native_libs/libmqtt_pubsub.so` from the
  build host. On a Pi target this can be wrong arch.
- **Mitigation:** `Makefile`'s `make pubsub` target rebuilds on the
  target. The README documents replacing the prebuilt with the local
  build.
- **Suggested follow-up:** add a `start_rover.sh` arch check
  (`file libmqtt_pubsub.so` → bail if mismatch).

## Container vtable refactor (deferred)

- `robot_sim/main.c` `#include`s class-specific
  `drive_base_robot.h`. That's why `main.c` lives in
  `lunar_rover-class` instead of `dongle_base`.
- A second class (factory_arm, lidar_pod) will force the issue:
  extract a `class_register_logical_robots(ctx)` symbol, lift `main.c`
  into `dongle_base`, classes ship just the vtable impl.
- Documented in `docs/containers/adding_a_new_class.md`.
- **Trigger:** when a second class is added.

## comm_manifest_ffi.lua nondeterminism

- Generator (`luajit comm_manifest.lua`) emits table addresses
  in the output, leading to noisy diffs on every regeneration:
  `pkt.data.dongles[0] = table: 0x01ea873e1018` → different hex on
  every run.
- The .bin / .h outputs ARE deterministic; only the `_ffi.lua`
  pretty-printer leaks the address.
- **Mitigation today:** treat `_ffi.lua` as a regenerated artifact;
  don't re-commit hex-only changes (see `git checkout --` pattern in
  recent commits).
- **Suggested follow-up:** patch the avro_dsl generator to print a
  symbolic placeholder (e.g. `<table-N>`) instead of the actual
  address.

## Image registry (no push yet)

- `mycorp/...` tags are local-only. No push to a registry.
- v3 platform's container_registry (per `project_dcs_registry_integration`)
  is the long-term destination.
- **Trigger:** when v3 platform stands up the registry.

## Pi 4 / 5 aarch64 image build (not yet smoked)

- Multi-stage Dockerfile is platform-neutral by construction (rebuilds
  C from source). aarch64 is expected to work; never run on real
  hardware.
- **Trigger:** when a Pi 4+ test rig is available. ~30 min smoke.

## Operations docs not yet refreshed for L6 / harness extraction

- `docs/operations/testing.md` predates `planner_test_peer.lua` and
  the contract harness; section text still describes the pre-extraction
  shapes.
- New material lives under `docs/testing/`. The two are inconsistent.
- **Suggested follow-up:** retire `docs/operations/testing.md` (link
  it to `docs/testing/index.md`) or rewrite it to match the new
  layout. Mechanical, ~30 min.

## Roadmap doc is stale

- `docs/roadmap/index.md` predates the 3-layer container split, L6,
  and Phase-2 contract. Reads as if those are open work.
- **Suggested follow-up:** rewrite or delete. ~30 min.

## Self-host vs two-process e2e timing variance

- Observed on `--count 5 --wait 60`: two-process flow occasionally
  reports `done=5/6` (last command times out) while self-host
  reports `done=6/6`. Same parameters, different timing characteristics
  due to broker contention.
- Not a defect — both are functional. Worth noting if test stability
  becomes important.
- **Suggested follow-up:** baseline with longer `--wait` (90s) for
  CI flows; default the `run_tests.sh` invocation that way.

## Out of scope for this rover specifically

- Mission planner improvements, fleet manager, distributed KB, etc.
  live in `ros_planner_ii` and `nano_data_center_base`. Not this
  repo's problem.
- The full sim_map (paint_fixture, assembly_fixture stations) is
  exercised in `test_physics.lua` but not in mqtt e2e — those workers
  exist but no scenario yet drives a rover to them through MQTT.
