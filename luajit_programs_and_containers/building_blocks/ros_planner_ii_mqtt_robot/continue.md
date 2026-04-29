# continue.md — 2026-04-28 late evening session handoff

## What landed this session

### Bug fix: multi-dongle N=100 stress flake (libcomm)

The known flake from the prior session was misdiagnosed. continue.md
predicted a blocking-write/pty buffer issue and proposed making
robot_sim's master_fd non-blocking. strace immediately showed that
hypothesis was wrong — robot_sim's writes returned in microseconds,
reads simply waited 1.0 s between incoming PINGs. The bottleneck was
on the chain_tree side.

**Real root cause:** `manifest.c` documents that `manifest_validate`
aliases the caller's blob and never copies it. The Lua test allocated
the manifest with `ffi.new()` inside `bring_up_two_dongles()` — local
cdata, dropped on function return. LuaJIT GC eventually freed it, and
`g_manifest` became a dangling pointer. After GC, comm_poll's
`bus_id = g_manifest->dongles[i].bus_local_ids[0]` read garbage (saw
`bus=2` instead of `bus=1`), `router_find_by_triple(dongle=1, bus=2,
addr=1)` returned NULL, and every response was silently dropped. Once
GC fired, EVERY subsequent response missed routing — that's why the
test settled at exactly 1 PING/sec (the test's `poll_until` deadline)
and never recovered.

**Fix:** copy the validated manifest into static storage inside
libcomm. ~10 LoC in `libcomm/comm.c`:

```c
static comm_manifest_v1_wire_t        g_manifest_storage;
static const comm_manifest_v1_wire_t *g_manifest = 0;
...
// in comm_init and comm_init_with_dongles:
g_manifest_storage = *m;
g_manifest         = &g_manifest_storage;
```

**Verification:**
- Before fix: 5/10 pass at N=100, 50% flake
- After fix: 20/20 at N=100, 5/5 at N=500 (serialized 1.25 s, interleaved 0.69 s)
- `bash run_tests.sh --skip-e2e`: all 184 unit checks green
- Bumped `STRESS_N` 20 → 100 in `test_comm_pty_multi_dongle.lua` so the
  bug stays caught

**Memory updates:**
- `feedback_pty_blocking_write_deadlock.md` — postscript noting the 2c.75
  flake was NOT this pattern; pattern still real for future bidirectional
  pty/serial code
- `feedback_verify_handoff_hypothesis.md` — new lesson: verify handoff
  hypotheses with evidence (strace/log instrumentation) before
  implementing the proposed fix

### Architecture session: dongle decomposition + embedded port plan

Multi-question design session covering the full path from today's Linux
waypoint to the embedded targets. Locked the architecture profile;
deferred specific code work to tomorrow.

**Four-role decomposition (per application dongle):**

```
external_bus_thread        priority HIGH
       ↓ frames in
dongle_manager_thread      priority MED  (HELLO/IDENT, identity, lifecycle, frame routing)
       ↓ internal-bus messages
internal_bus_thread        priority MED  (fanout to per-logical-robot inboxes)
       ↓
logical_robot_thread × N   priority LOW  (drive_base, gripper, etc — one per logical robot)
```

Rationale: Linux could fold the manager into the external-bus loop, but
keeping all four roles as separate threads gives 1:1 mapping to the
embedded RTOS shape. Internal-bus IS its own thread (originally argued
against this for Linux; reversed for embedded portability — k_msgq +
routing thread is the canonical Zephyr publish/subscribe shape).
Logical-robot threads exist even when tickless — a thread blocked on
`k_msgq_get(K_FOREVER)` costs only its stack on Zephyr/FreeRTOS.

**Three boundaries to hold:**

```
┌────────────────────────────────────────────────────┐
│ application threads + frame layer + manifest       │  PORTABLE C
├────────────────────────────────────────────────────┤
│ bus_kernel.h  (threads, msgq, timers, mutex)       │  per-RTOS, ~150 LoC each
├────────────────────────────────────────────────────┤
│ ext_bus contract: tx() / rx() / rx_wait()          │  ← UPPER BOUNDARY
├────────────────────────────────────────────────────┤
│ ext_bus driver: DMA, ISR, FIFO, errata             │  per-silicon, ~150-400 LoC
└────────────────────────────────────────────────────┘
```

Three-function ext_bus contract:

```c
size_t ext_bus_tx(const uint8_t *bytes, size_t n);                 // non-blocking
size_t ext_bus_rx(uint8_t *out, size_t cap);                       // non-blocking
int    ext_bus_rx_wait(uint32_t timeout_ms);                       // returns 0 / -ETIMEDOUT
```

**Discipline:** above the contract, no caller is allowed to assume
anything about where bytes live in memory. The driver owns alignment,
cacheability, DMA-vs-CPU ordering. If application code ever needs to
know "is this DMA?" — the contract is wrong.

**Three primary embedded targets, all on Zephyr:**

| Slot | Target | Silicon | RAM | RTOS |
|------|--------|---------|-----|------|
| 1 | Linux waypoint (today) | x86_64 / aarch64 | huge | pthreads |
| 2 | Pico | RP2040 (M0+ ×2 @ 133 MHz) | 264 KB | Zephyr |
| 3 | Pico 2 | RP2350 (M33 ×2 @ 150 MHz) | 520 KB | Zephyr |
| 4 | XIAO Silabs | MGM240 / EFR32MG24 (M33 @ 78 MHz) | 256 KB | Zephyr |

Two `bus_kernel.h` backends total: pthreads (today) + Zephyr.

**Targets considered and DEFERRED:**
- Teensy 4.1 (iMXRT1062) + FreeRTOS — capable but adds FreeRTOS backend
  AND cache-coherent DMA. Defer until 3-Zephyr-target story is proven.
- Teensy 3.6 (K66) — same family, simpler silicon, also deferred.
- ST Nucleo-144 (F429ZI / F767ZI / H743ZI) — highest pin count of any
  evaluated option but adds nothing to the three-target lineup.
- 32K-RAM tier (likely EFR32BG22) — only after `#define`-ification
  refactor; documented as a future consideration.

**Static-everything discipline:** every static array size becomes a
`#define` overridable per build target. `#define`-ification audit must
land before any embedded port — cheap now, expensive later.

```c
#define COMM_HANDLES_MAX        32
#define COMM_SLAVES_MAX         64
#define COMM_DONGLES_MAX        4
#define TRANSPORT_UART_TX_SIZE  1024
#define TRANSPORT_UART_RX_SIZE  2048
#define LOGICAL_ROBOT_MAX       8
#define MSGQ_DEPTH_DEFAULT      16
#define BUS_THREAD_STACK_BYTES  1536
```

**DMA pattern (every embedded target):**
- RX: double-buffered circular DMA + idle-line interrupt. ISR posts
  head pointer to thread via semaphore. NO per-byte interrupts.
- TX: DMA-driven with completion-IRQ to chain next chunk if pending.
- ISR does ONLY ring + semaphore. Never frame work, never CRC.
- RX ring sized for worst-case IRQ-latency × line-rate × 2 (≥2 KB).
- TX ring sized for max-frame-size × 4 (~1 KB).
- Software CRC (frame.c) on every target. Hardware CRC peripherals
  vary across silicon; not worth porting.

**Logged in memory:** `project_dongle_architecture.md`.

## Plan of action — tomorrow

Three tracks, ordered by dependency. Track A is the gate before any
silicon work; B and C can run in parallel after A.

### Track A: portability refactor (Linux waypoint)

The `#define`-ification + abstraction layer that makes ports mechanical.

1. **Audit all hardcoded sizes in libcomm/.** Replace with `#define`s in
   a new `bus_config.h`. Defaults match today's values. Targets:
   `COMM_HANDLES_MAX`, `COMM_SLAVES_MAX`, `COMM_DONGLES_MAX`,
   `COMM_BUSES_MAX`, `COMM_PAYLOAD_MAX`, `TRANSPORT_UART_TX_SIZE`,
   `TRANSPORT_UART_RX_SIZE`, any others discovered. Document each.
   Verify `bash run_tests.sh --skip-e2e` still green.

2. **Extract `bus_kernel.h` interface.** Three primitives at minimum:
   `bus_thread`, `bus_msgq`, `bus_timer`. Optional fourth: `bus_mutex`.
   Pure interface — no Linux-isms in the header. Document each
   primitive's contract (blocking vs non-blocking, queue depth
   semantics, timer drift behavior).

3. **Implement `bus_kernel_linux.c` (pthreads backend).** Move the
   existing thread/queue plumbing from wherever it currently lives
   (chain_tree side and the test harness) into this file. Keep the
   surface API identical.

4. **Sketch `bus_kernel_zephyr.c` (Zephyr backend).** Skeleton only —
   doesn't need to compile under Linux. Each function maps to one or
   two Zephyr kernel calls (`k_thread_create`, `k_msgq_put/get`,
   `k_timer_init`). This forces porting decisions to surface NOW
   instead of at port time.

5. **Refactor `transport_uart.c` into `ext_bus_linux_pty.c`** that
   implements the three-function contract:

   ```c
   size_t ext_bus_tx(const uint8_t *bytes, size_t n);
   size_t ext_bus_rx(uint8_t *out, size_t cap);
   int    ext_bus_rx_wait(uint32_t timeout_ms);
   ```

   Lift SLIP / CRC / framing above this line. comm_poll's pump becomes
   "drain ext_bus_tx → call ext_bus_rx → feed decoder."

6. **Run loopback contract test.** Wire `ext_bus_tx` → `ext_bus_rx`
   directly in a fixture (no real bus) and verify the frame layer
   passes through with zero modification. This same test will run
   unchanged on every silicon target — it's the boundary's
   regression gate.

**Done when:** all 184 unit checks still green, plus a new
`test_ext_bus_contract.lua` (or `.c`) exercising the contract via
loopback.

### Track B: Pico bring-up (RP2040 + Zephyr)

Start in parallel with Track A's later steps once `bus_kernel.h` exists.

1. **Set up Zephyr workspace + west tooling.** West init for a workspace
   that builds for `rpi_pico` board target. Confirm `samples/hello_world`
   blinks an LED.

2. **Write `bus_kernel_zephyr.c` properly.** Implement the four
   primitives against k_thread / k_msgq / k_timer / k_mutex. Compile
   the dongle's portable code on Zephyr; expect linker errors that
   identify any remaining Linux-isms above the contract. Fix them.

3. **Write `ext_bus_rp2040_uart_dma.c`** — the per-silicon driver.
   Pattern: one UART (UART0 by default), DMA channel for RX, second
   for TX, idle-line interrupt fires `k_sem_give` from ISR. Don't use
   PIO yet; reserve PIO for if the protocol ever needs RS-485 / DE-line
   toggling.

4. **Loopback contract test on hardware.** TX pin → RX pin (jumper),
   run the loopback fixture from Track A.6. Same test, different
   backend. Should pass byte-for-byte.

5. **Two-Pico interop.** One Pico runs robot_sim shape, one Pico runs
   chain_tree shape, connected via UART. PING test mirroring Phase B
   on Linux today. This is the real "embedded waypoint exists" gate.

### Track C: design follow-ups (no code)

Surface decisions now, before they bite at port time.

1. **Internal-bus message format.** What's a "message" between dongle
   manager and a logical_robot? Header (mcu / cmd / seq / payload_len)
   + bounded-size payload? Reuse `frame_meta_t` shape? Decide once,
   write down, lock.

2. **Logical-robot lifecycle.** Init / tick / on-message / shutdown
   contract. One callback table per logical-robot type? Static-table
   registration?

3. **Identity storage on embedded.** Today robot_sim takes argv. On
   Pico/Pico 2/MGM240 there's no argv — identity has to live somewhere
   on-chip. Options: Zephyr settings subsystem / dedicated flash sector
   / OTP fuses (if available). Pick per-target.

4. **External-bus speed targets.** "Sustain N msg/s with worst-case
   end-to-end ext_bus → logical_robot dispatch latency of M ms."
   Without the number, ring sizes and thread priorities are guesses.
   Probably 1 Mbps, ~50 msg/s sustained, ~5 ms worst-case latency —
   but write it down.

5. **Logical-robot count per dongle in v1.** Probably 1 (drive_base
   only), maybe 2 (drive_base + battery monitor). Affects msgq depths
   and stack sizing.

## How to resume

```bash
cd /home/gedgar/knowledge_base_assembly/luajit_programs_and_containers/building_blocks/ros_planner_ii_mqtt_robot
make
bash run_tests.sh --skip-e2e   # all 184 checks should be green
```

Quick sanity at N=100 multi-dongle stress:

```bash
for i in 1 2 3 4 5; do luajit test_comm_pty_multi_dongle.lua 2>/dev/null | grep summary; done
# expect: 5x "[summary] 18 passed, 0 failed"
```

Open with **Track A.1** — the audit. ~30 minutes of grep + decision-making
+ doc, plus a re-run of the test suite. Once that's in, everything else
on Track A is mechanical refactor.

## Uncommitted state at session end

```
modified:   libcomm/comm.c                   (manifest copy fix — the load-bearing change)
modified:   test_comm_pty_multi_dongle.lua   (STRESS_N 20→100, comment update)
modified:   libcomm.so                       (rebuild artifact)
modified:   comm_manifest_ffi.lua            (also a rebuild artifact, untouched by edits)
```

Suggested commit message:

> libcomm: copy validated manifest into static storage to fix N=100 multi-dongle flake
>
> manifest_validate aliases the caller's blob; LuaJIT-allocated cdata
> got GC'd mid-test, dangling g_manifest caused router lookup to read
> garbage bus_id and silently drop responses. Copy the validated
> manifest into a static comm_manifest_v1_wire_t storage so the lib
> is robust against caller memory-management mistakes. Also bump
> STRESS_N 20→100 to keep the bug caught if it ever returns.
