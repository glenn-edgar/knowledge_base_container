# Planner-to-Target Interface Architecture
## MicroPython Execution Target with BLE/Thread/WiFi Transport

---

## Table of Contents

1. [Purpose and Scope](#1-purpose-and-scope)
2. [System Architecture](#2-system-architecture)
   - 2.1 [Host and Target Roles](#21-host-and-target-roles)
   - 2.2 [Communication Model](#22-communication-model)
   - 2.3 [Data Flow Diagram](#23-data-flow-diagram)
3. [Target Execution Model](#3-target-execution-model)
   - 3.1 [Single-Leaf-Per-Global-Tick](#31-single-leaf-per-global-tick)
   - 3.2 [Monitor Tick (10ms)](#32-monitor-tick-10ms)
   - 3.3 [Guard Check](#33-guard-check)
   - 3.4 [Cleanup Guarantee](#34-cleanup-guarantee)
   - 3.5 [Execution Lifecycle Diagram](#35-execution-lifecycle-diagram)
4. [Transport Abstraction Layer](#4-transport-abstraction-layer)
   - 4.1 [Design Principle](#41-design-principle)
   - 4.2 [Abstract Transport Interface](#42-abstract-transport-interface)
   - 4.3 [BLE Transport (Initial)](#43-ble-transport-initial)
   - 4.4 [Thread Transport (Future)](#44-thread-transport-future)
   - 4.5 [WiFi Transport (Future)](#45-wifi-transport-future)
   - 4.6 [Transport Constraints Summary](#46-transport-constraints-summary)
5. [Protocol Specification](#5-protocol-specification)
   - 5.1 [Message Format](#51-message-format)
   - 5.2 [Command Messages (Host to Target)](#52-command-messages-host-to-target)
   - 5.3 [Response Messages (Target to Host)](#53-response-messages-target-to-host)
   - 5.4 [Status Stream Messages (Target to Host, Unsolicited)](#54-status-stream-messages-target-to-host-unsolicited)
   - 5.5 [Message Demultiplexing](#55-message-demultiplexing)
   - 5.6 [Sequence Numbers and Idempotency](#56-sequence-numbers-and-idempotency)
6. [Command Vocabulary](#6-command-vocabulary)
   - 6.1 [System Commands](#61-system-commands)
   - 6.2 [Motor Commands](#62-motor-commands)
   - 6.3 [Sensor Commands](#63-sensor-commands)
   - 6.4 [DriveBase Commands](#64-drivebase-commands)
   - 6.5 [Tree Control Commands](#65-tree-control-commands)
   - 6.6 [Extending the Vocabulary](#66-extending-the-vocabulary)
7. [Status Stream Specification](#7-status-stream-specification)
   - 7.1 [Status Fields](#71-status-fields)
   - 7.2 [Compact Encoding](#72-compact-encoding)
   - 7.3 [Rate Control](#73-rate-control)
   - 7.4 [Host-Side Blackboard Mapping](#74-host-side-blackboard-mapping)
8. [Target-Side MicroPython Architecture](#8-target-side-micropython-architecture)
   - 8.1 [Program Structure](#81-program-structure)
   - 8.2 [Coroutine Model](#82-coroutine-model)
   - 8.3 [Command Dispatcher](#83-command-dispatcher)
   - 8.4 [Leaf Node Pattern](#84-leaf-node-pattern)
   - 8.5 [Tree Executor](#85-tree-executor)
9. [Host-Side Interface](#9-host-side-interface)
   - 9.1 [BLE Listener and Blackboard Updater](#91-ble-listener-and-blackboard-updater)
   - 9.2 [Command Sender](#92-command-sender)
   - 9.3 [Planner Integration](#93-planner-integration)
10. [Error Handling](#10-error-handling)
    - 10.1 [Transport Errors](#101-transport-errors)
    - 10.2 [Execution Errors](#102-execution-errors)
    - 10.3 [Reconnection Protocol](#103-reconnection-protocol)
11. [Multi-Target Support](#11-multi-target-support)
12. [Design Constraints and Rationale](#12-design-constraints-and-rationale)

---

## 1. Purpose and Scope

This document defines the standardized interface between the host-side planner (LuaJIT + SQLite KB + ChainTree runtime) and one or more execution targets running MicroPython (initially LEGO SPIKE Prime with Pybricks).

The interface is transport-agnostic. The initial transport is BLE (Bluetooth Low Energy) using the Nordic UART Service. Future transports include 802.15.4 Thread and WiFi TCP/UDP. All transports implement the same abstract interface so that the planner, protocol, and target-side program are unchanged when the transport changes.

The interface has two data paths:

- **Command/Response** (host to target, synchronous) — the planner sends a command, the target executes it, the target sends a response.
- **Status Stream** (target to host, asynchronous) — the target pushes sensor and execution state to the host at a fixed interval, independent of command traffic.

---

## 2. System Architecture

### 2.1 Host and Target Roles

| Component | Runs On | Role |
|-----------|---------|------|
| Planner | Host (PC, RPi) | Queries KB, selects actions, sends goal/tree commands |
| ChainTree Runtime | Host | RPC server, manages dead nodes, execution framework |
| SQLite KB | Host | World state, domain knowledge, plan storage |
| Blackboard Updater | Host | Parses status stream, writes to KB/blackboard fields |
| Transport Driver | Host | BLE/Thread/WiFi connection management |
| Tree Executor | Target (SPIKE hub) | Runs shallow BT, global ticks, leaf selection |
| Monitor Loop | Target | 10ms poll loop inside active leaf |
| Status Stream | Target | Periodic sensor push, independent of tree executor |
| Command Handler | Target | Receives and dispatches host commands |

### 2.2 Communication Model

The target maintains two concurrent output streams on a single serial channel:

1. **Status lines** — unsolicited, periodic, prefixed `S:`
2. **Response lines** — solicited, one per command, prefixed `R:`

The host sends:

3. **Command lines** — prefixed `C:`, each carrying a sequence number

All messages are newline-delimited. The host demultiplexes inbound lines by prefix.

### 2.3 Data Flow Diagram

```
HOST                                          TARGET
────                                          ──────

┌────────────────┐                            ┌────────────────────────┐
│ Planner        │                            │ tree_executor()        │
│ (LuaJIT+KB)    │                            │   global tick:         │
│                │    C: command + seq_id      │     root → leaf       │
│ ───────────────┼──────────────────────────►  │   monitor ticks:      │
│                │                            │     10ms poll          │
│                │    R: response + seq_id     │     guard_check()     │
│                │  ◄──────────────────────── │     until done/error   │
│                │                            │                        │
│ Blackboard     │    S: sensor status         │ status_stream()        │
│ Updater        │  ◄──────────────────────── │   100ms push           │
│ (async)        │    (continuous, 10 Hz)      │   (always running)     │
└────────────────┘                            └────────────────────────┘
        │                                              │
        ▼                                              ▼
┌────────────────┐                            ┌────────────────────────┐
│ Transport      │◄ ─ ─ BLE / Thread / WiFi ─►│ Transport              │
│ Driver         │      (newline-delimited)    │ Driver                 │
└────────────────┘                            └────────────────────────┘
```

---

## 3. Target Execution Model

### 3.1 Single-Leaf-Per-Global-Tick

The target runs a shallow behavior tree. On each global tick:

1. Traversal starts at the root node.
2. Composite nodes (sequence, selector) evaluate children according to their rules.
3. Exactly one leaf node is selected for execution.
4. The leaf executes to completion (SUCCESS or FAILURE) before the next global tick begins.
5. No other leaf runs during this time.

This guarantees:
- **Deterministic execution order** — no concurrency within the tree.
- **No shared-state races** — only one actuator command is active at a time.
- **Correct for MicroPython** — single-threaded, cooperative multitasking only.

### 3.2 Monitor Tick (10ms)

Once a leaf node begins execution, it enters a monitor loop that polls at 10ms intervals:

```python
async def leaf_execute(self):
    self.start_action()          # send motor command, wait=False
    try:
        while True:
            fault = guard_check()
            if fault:
                return FAILURE, fault
            if self.done_condition():
                return SUCCESS, None
            if self.elapsed_ms() > self.timeout_ms:
                return FAILURE, "timeout"
            await wait(10)       # 10ms monitor tick
    finally:
        self.cleanup()           # always stop hardware
```

The monitor tick handles:
- **Completion detection** — motor.done(), drivebase.done(), sensor threshold reached.
- **Fault detection** — via shared guard_check() (see 3.3).
- **Timeout** — configurable per leaf, prevents indefinite blocking.

The 10ms rate matches Pybricks motor control loop granularity. Faster polling wastes CPU cycles with no benefit. Slower polling risks missing transient events (stall detection, bump).

### 3.3 Guard Check

Safety checks are centralized in a single function called on every monitor tick by every leaf node. This prevents duplicating safety logic across leaves.

```python
def guard_check():
    """Shared safety checks. Returns fault string or None."""
    if hub.battery.voltage() < LOW_BATTERY_MV:
        return "low_battery"
    acc = hub.imu.acceleration(Axis.Z)
    if abs(acc) > BUMP_THRESHOLD:
        return "bump"
    if abs(hub.imu.tilt()[0]) > TILT_LIMIT:
        return "tilt"
    # Add transport-level guard:
    if transport.seconds_since_last_rx() > COMM_TIMEOUT_S:
        return "comm_timeout"
    return None
```

Guard thresholds are configurable via host command (see Section 6.1). Defaults are set at program startup.

The guard_check function is the target-side equivalent of ChainTree's exception handler pattern. It runs inside the active leaf's context so that failures route through the leaf's cleanup path.

### 3.4 Cleanup Guarantee

Every leaf node must guarantee that hardware is in a safe state when it exits, whether by success, failure, or exception. The `try/finally` pattern enforces this:

```python
finally:
    self.cleanup()  # stop motors, brake drivebase, release gripper
```

The cleanup method is leaf-specific. A motor leaf calls `motor.stop()`. A drivebase leaf calls `drivebase.brake()`. A gripper leaf releases torque hold. The global tick does not resume until cleanup completes.

### 3.5 Execution Lifecycle Diagram

```
Global Tick N
│
├─ Traverse tree root → select Leaf B
│
├─ Leaf B: start_action()
│   │
│   ├─ Monitor tick 1 (10ms): guard_check() ok, not done
│   ├─ Monitor tick 2 (10ms): guard_check() ok, not done
│   ├─ ...
│   ├─ Monitor tick K (10ms): guard_check() ok, done!
│   │
│   ├─ finally: cleanup()
│   └─ return SUCCESS
│
├─ Update node status
│
Global Tick N+1
│
├─ Traverse tree root → select Leaf C
│   ...
```

---

## 4. Transport Abstraction Layer

### 4.1 Design Principle

The transport layer is a replaceable module that provides exactly two operations:

- **write_line(text)** — send a newline-terminated string to the peer
- **read_line()** — receive a newline-terminated string from the peer (may block or return None)

All protocol logic (prefixes, sequence numbers, JSON encoding) lives above the transport layer. The transport knows nothing about message content.

### 4.2 Abstract Transport Interface

**Target side (MicroPython):**

```python
class Transport:
    def write_line(self, text):
        """Send text + newline to host. Must not block."""
        raise NotImplementedError

    async def read_line(self):
        """Receive one line from host. Returns str or None if no data."""
        raise NotImplementedError

    def is_connected(self):
        """True if transport link is active."""
        raise NotImplementedError

    def seconds_since_last_rx(self):
        """Seconds since last received data. For comm timeout guard."""
        raise NotImplementedError
```

**Host side (LuaJIT):**

```lua
-- transport.lua abstract interface
local Transport = {}
Transport.__index = Transport

function Transport:write_line(text)   error("not implemented") end
function Transport:read_line()        error("not implemented") end  -- returns string or nil
function Transport:is_connected()     error("not implemented") end
function Transport:close()            error("not implemented") end
```

### 4.3 BLE Transport (Initial)

**Target (SPIKE Prime / Pybricks):**

Pybricks exposes BLE as a Nordic UART Service (NUS). `print()` writes to the TX characteristic (hub → host, notify). `stdin` reads from the RX characteristic (host → hub, write).

```python
import usys

class BleTransport(Transport):
    def __init__(self):
        self._last_rx_ms = 0

    def write_line(self, text):
        # print() writes to BLE TX notify characteristic
        print(text)

    async def read_line(self):
        # Non-blocking read from BLE RX
        # Pybricks stdin is line-buffered over BLE
        data = usys.stdin.buffer.read(256)
        if data:
            self._last_rx_ms = time_ms()
            return data.decode().strip()
        return None

    def is_connected(self):
        return True  # Pybricks manages BLE connection internally

    def seconds_since_last_rx(self):
        return (time_ms() - self._last_rx_ms) / 1000.0
```

**Constraints:**
- MTU: 20 bytes default, up to ~240 negotiated. Messages exceeding MTU are fragmented automatically by the BLE stack.
- Throughput: ~20 KB/s practical.
- Latency: 15-30ms per round trip.
- Notification rate: 10-30 per second sustainable.

**Host:**

The host BLE driver uses the platform's BLE library (e.g., `bleak` on Python, or a C library via LuaJIT FFI) to connect to the Pybricks NUS service and read/write characteristics.

### 4.4 Thread Transport (Future)

802.15.4 Thread (OpenThread) provides IPv6 mesh networking. Transport would be UDP datagrams over Thread.

```python
class ThreadTransport(Transport):
    def __init__(self, host_addr, port):
        self._sock = usocket.socket(usocket.AF_INET6, usocket.SOCK_DGRAM)
        self._sock.setblocking(False)
        self._peer = (host_addr, port)

    def write_line(self, text):
        self._sock.sendto((text + "\n").encode(), self._peer)

    async def read_line(self):
        try:
            data, addr = self._sock.recvfrom(512)
            self._last_rx_ms = time_ms()
            return data.decode().strip()
        except OSError:
            return None
```

**Constraints:**
- MTU: 1280 bytes (IPv6 minimum). Status messages fit in one datagram.
- Latency: 10-50ms depending on mesh depth.
- Reliability: UDP, no guaranteed delivery. Sequence numbers handle lost messages.

### 4.5 WiFi Transport (Future)

TCP stream over WiFi. Most reliable transport option.

```python
class WifiTransport(Transport):
    def __init__(self, host_addr, port):
        self._sock = usocket.socket()
        self._sock.connect((host_addr, port))
        self._sock.setblocking(False)
        self._buf = b""

    def write_line(self, text):
        self._sock.send((text + "\n").encode())

    async def read_line(self):
        try:
            chunk = self._sock.recv(512)
            if chunk:
                self._buf += chunk
                self._last_rx_ms = time_ms()
            if b"\n" in self._buf:
                line, self._buf = self._buf.split(b"\n", 1)
                return line.decode().strip()
        except OSError:
            pass
        return None
```

**Constraints:**
- MTU: 1460 bytes (TCP MSS). No message size concern.
- Latency: 1-5ms on local network.
- Reliability: TCP guarantees delivery and ordering.

### 4.6 Transport Constraints Summary

| Property | BLE (NUS) | Thread (UDP) | WiFi (TCP) |
|----------|-----------|--------------|------------|
| MTU | 20-240 bytes | 1280 bytes | 1460 bytes |
| Latency | 15-30ms | 10-50ms | 1-5ms |
| Reliability | Notify (best-effort) | UDP (best-effort) | TCP (guaranteed) |
| Range | ~10m | ~100m mesh | LAN |
| Power | Low | Low | High |
| MicroPython support | Pybricks built-in | Board-specific | Board-specific |
| Max message rate | 10-30/s | 50-100/s | 100+/s |

The protocol (Section 5) handles unreliable transports via sequence numbers and idempotent commands. The transport layer does not retry — the protocol layer does.

---

## 5. Protocol Specification

### 5.1 Message Format

All messages are single-line, newline-delimited (`\n`), UTF-8 encoded. Each line has a one-character prefix followed by a colon, then a JSON object:

```
<prefix>:<json>\n
```

| Prefix | Direction | Meaning |
|--------|-----------|---------|
| `C` | Host → Target | Command |
| `R` | Target → Host | Response to a command |
| `S` | Target → Host | Status stream (unsolicited) |

Maximum message length: 240 bytes (fits in negotiated BLE MTU). Status messages that exceed this are split into multiple `S:` lines with a `part` field (see 7.2).

### 5.2 Command Messages (Host to Target)

```json
C:{"c":"<command>","p":<params>,"id":<seq>}
```

| Field | Type | Description |
|-------|------|-------------|
| `c` | string | Command name from the vocabulary (Section 6) |
| `p` | array or object | Command parameters (command-specific) |
| `id` | integer | Sequence number, monotonically increasing |

**Examples:**

```
C:{"c":"run_angle","p":["A",500,360],"id":1}
C:{"c":"straight","p":[500],"id":2}
C:{"c":"stop_all","p":[],"id":3}
C:{"c":"set_goal","p":{"goal":"deliver"},"id":4}
```

### 5.3 Response Messages (Target to Host)

```json
R:{"id":<seq>,"s":"<status>","d":<data>}
```

| Field | Type | Description |
|-------|------|-------------|
| `id` | integer | Echoes the command's sequence number |
| `s` | string | `"ok"`, `"err"`, `"busy"` |
| `d` | object or null | Result data (command-specific) |

**Examples:**

```
R:{"id":1,"s":"ok","d":{"angle":360}}
R:{"id":2,"s":"err","d":{"fault":"stall"}}
R:{"id":3,"s":"ok","d":null}
```

### 5.4 Status Stream Messages (Target to Host, Unsolicited)

```json
S:{"t":<tick>,"m":<motor_data>,"s":<sensor_data>,"f":<fault_flags>}
```

The target sends these at a fixed interval (default 100ms, configurable). They are never a response to a command. See Section 7 for the full field specification.

### 5.5 Message Demultiplexing

The host reads lines from the transport and routes by prefix:

```lua
local line = transport:read_line()
if line then
    local prefix = line:sub(1, 1)
    local payload = line:sub(3)  -- skip "X:"
    if prefix == "S" then
        blackboard_update(json.decode(payload))
    elseif prefix == "R" then
        response_handler(json.decode(payload))
    end
end
```

The target reads lines and routes similarly (only `C:` prefix expected inbound).

### 5.6 Sequence Numbers and Idempotency

Each command carries a monotonically increasing sequence number (`id`). The target tracks the last executed `id`. If a command arrives with an `id` less than or equal to the last executed, it is a duplicate (retransmit from unreliable transport) and is responded to with the cached previous result without re-execution.

This makes all commands idempotent at the protocol level. The host can safely retransmit on timeout without risking double execution.

```python
_last_id = 0
_last_response = None

def handle_command(msg):
    global _last_id, _last_response
    if msg["id"] <= _last_id:
        # Duplicate — return cached response
        transport.write_line("R:" + json.dumps(_last_response))
        return
    result = dispatch(msg["c"], msg["p"])
    _last_id = msg["id"]
    _last_response = {"id": msg["id"], "s": result.status, "d": result.data}
    transport.write_line("R:" + json.dumps(_last_response))
```

---

## 6. Command Vocabulary

Commands are the RPC interface. Each command maps to a Pybricks API call or a tree control operation. The vocabulary is extensible — new commands are added by registering a handler in the target's dispatcher.

### 6.1 System Commands

| Command | Params | Description |
|---------|--------|-------------|
| `ping` | `[]` | Heartbeat. Returns `{"s":"ok"}`. |
| `stop_all` | `[]` | Emergency stop. All motors coast, drivebase stops. |
| `get_config` | `[]` | Returns hub identity, firmware version, battery voltage. |
| `set_guard` | `{"k":"<key>","v":<value>}` | Set guard threshold (e.g., `"low_battery_mv"`, `"bump_g"`, `"tilt_deg"`, `"comm_timeout_s"`). |
| `set_status_rate` | `{"ms":<interval>}` | Set status stream interval in ms (min 50, max 1000). |

### 6.2 Motor Commands

| Command | Params | Description |
|---------|--------|-------------|
| `run` | `["<port>", <speed_dps>]` | Run at constant speed. Returns immediately. |
| `run_angle` | `["<port>", <speed>, <angle>]` | Run relative angle. Blocking in tree (monitor ticks poll `done()`). |
| `run_target` | `["<port>", <speed>, <target_angle>]` | Run to absolute angle. Blocking in tree. |
| `run_time` | `["<port>", <speed>, <time_ms>]` | Run for duration. Blocking in tree. |
| `run_stall` | `["<port>", <speed>]` | Run until stalled. Returns stall angle. |
| `stop` | `["<port>"]` | Stop motor (coast). |
| `brake` | `["<port>"]` | Passive brake. |
| `hold` | `["<port>"]` | Active hold at current angle. |
| `reset_angle` | `["<port>", <angle>]` | Reset angle reference. |
| `dc` | `["<port>", <duty_pct>]` | Raw duty cycle (-100 to 100). |

Port values: `"A"` through `"F"`.

### 6.3 Sensor Commands

| Command | Params | Description |
|---------|--------|-------------|
| `read_color` | `["<port>"]` | Returns `{"h":<hue>,"s":<sat>,"v":<val>}`. |
| `read_distance` | `["<port>"]` | Returns `{"mm":<distance>}`. |
| `read_force` | `["<port>"]` | Returns `{"n":<newtons>,"pressed":<bool>}`. |
| `read_imu` | `[]` | Returns `{"heading":<deg>,"pitch":<deg>,"roll":<deg>}`. |

Sensor data is also available in the status stream (Section 7). Direct reads are for on-demand queries outside the normal polling cycle.

### 6.4 DriveBase Commands

| Command | Params | Description |
|---------|--------|-------------|
| `db_setup` | `["<left>","<right>",<wheel_dia>,<axle_track>]` | Configure drivebase. Call once at startup. |
| `db_straight` | `[<distance_mm>]` | Drive straight. Blocking in tree. |
| `db_turn` | `[<angle_deg>]` | Turn in place. Blocking in tree. |
| `db_curve` | `[<radius_mm>, <angle_deg>]` | Drive arc. Blocking in tree. |
| `db_drive` | `[<speed_mms>, <turn_rate_dps>]` | Continuous drive. Returns immediately. |
| `db_stop` | `[]` | Stop drivebase (coast). |
| `db_brake` | `[]` | Passive brake drivebase. |
| `db_reset` | `[]` | Reset distance and angle counters. |
| `db_use_gyro` | `[<bool>]` | Enable/disable gyro heading correction. |

### 6.5 Tree Control Commands

These commands control the target-side tree executor from the host-side planner.

| Command | Params | Description |
|---------|--------|-------------|
| `set_goal` | `{"goal":"<goal_name>"}` | Set the current mission goal. The tree executor uses this to select branches. |
| `activate_branch` | `{"branch":"<name>"}` | Enable a named branch (dead node) in the tree. |
| `deactivate_branch` | `{"branch":"<name>"}` | Disable a named branch. |
| `set_tree` | `{"tree":"<tree_id>"}` | Switch to a different pre-loaded tree. |
| `pause_tree` | `[]` | Pause tree execution after current leaf completes. |
| `resume_tree` | `[]` | Resume tree execution. |

### 6.6 Extending the Vocabulary

New commands are registered in the target's dispatcher table:

```python
_dispatch_table = {}

def register_command(name, handler):
    _dispatch_table[name] = handler

def dispatch(command_name, params):
    handler = _dispatch_table.get(command_name)
    if handler is None:
        return Result("err", {"reason": "unknown_command"})
    return handler(params)
```

Custom leaf nodes register their commands at startup. The vocabulary is not fixed — it is the union of all registered handlers.

---

## 7. Status Stream Specification

### 7.1 Status Fields

The status stream pushes a snapshot of all monitored state at a fixed interval. The target reads all sensors and packs them into a single `S:` message.

```json
S:{"t":12345,"m":{"A":[360,50,0],"B":[0,0,1]},"db":[500,45,200,0],"u":152,"f":0,"bat":7850}
```

| Field | Type | Description |
|-------|------|-------------|
| `t` | int | Tick counter (monotonic, wraps at 2^31). For ordering and staleness detection. |
| `m` | object | Motor state per port. Value is `[angle_deg, speed_dps, stalled_bool]`. Only ports with initialized motors are included. |
| `db` | array or null | DriveBase state: `[distance_mm, angle_deg, speed_mms, done_bool]`. Null if no drivebase configured. |
| `u` | int or null | Ultrasonic distance in mm. Null if no sensor. |
| `cl` | array or null | Color sensor: `[hue, sat, val]`. Null if no sensor. |
| `fc` | array or null | Force sensor: `[force_cN, pressed_bool]`. Force in centinewtons for integer encoding. Null if no sensor. |
| `imu` | array or null | IMU: `[heading_deg_x10, pitch_deg_x10, roll_deg_x10]`. Scaled by 10 for integer encoding. |
| `f` | int | Fault flags (bitmask). See below. |
| `bat` | int | Battery voltage in mV. |

### 7.2 Compact Encoding

To stay within BLE MTU limits:

- **Short field names** — single or two-character keys.
- **Integer encoding** — floats are scaled to integers (heading × 10, force in cN). Avoids JSON float serialization overhead.
- **Array encoding for fixed-structure data** — motor state as `[angle, speed, stalled]` not `{"angle":360,"speed":50,"stalled":false}`.
- **Omit null sensors** — only include ports/sensors that are physically connected.

A typical status message with 2 motors + drivebase + ultrasonic + battery is ~120 bytes. Fits in one negotiated-MTU BLE packet.

If a status message exceeds 200 bytes (many sensors attached), split into two `S:` lines:

```
S:{"t":12345,"m":{"A":[360,50,0],"B":[0,0,1]},"bat":7850,"f":0,"pt":1}
S:{"t":12345,"db":[500,45,200,0],"u":152,"cl":[132,94,26],"pt":2}
```

The `pt` (part) field indicates multi-part status. The host reassembles by matching `t` values.

### 7.3 Rate Control

Default status rate: 100ms (10 Hz).

The host can change the rate via `set_status_rate` command. Minimum 50ms (20 Hz), maximum 1000ms (1 Hz). The target clamps to these bounds.

For BLE, 10 Hz is the recommended rate. For WiFi, 20 Hz is feasible. For Thread, 10 Hz is safe.

### 7.4 Host-Side Blackboard Mapping

The host-side blackboard updater maps status stream fields to KB entries or ChainTree blackboard fields:

| Status Field | Blackboard / KB Path |
|---|---|
| `m.A[0]` | `blackboard.motor_a_angle` or `planner.problem.functions.motor_angle.a` |
| `m.A[2]` | `blackboard.motor_a_stalled` |
| `db[0]` | `blackboard.drivebase_distance` |
| `u` | `blackboard.ultrasonic_distance` |
| `f` | `blackboard.fault_flags` |
| `bat` | `blackboard.battery_voltage` |

The updater writes on every received `S:` message. The planner reads these values with zero latency — no BLE round-trip needed for sensor data.

**Fault flags bitmask:**

| Bit | Meaning |
|-----|---------|
| 0 | Low battery |
| 1 | Motor A stalled |
| 2 | Motor B stalled |
| 3 | Bump detected (IMU) |
| 4 | Tilt exceeded |
| 5 | Communication timeout |
| 6 | Drivebase stalled |
| 7 | Sensor disconnect |

---

## 8. Target-Side MicroPython Architecture

### 8.1 Program Structure

The target runs a single MicroPython program with three concurrent coroutines managed by Pybricks `multitask`:

```python
from pybricks.tools import multitask, run_task

async def main():
    await multitask(
        status_stream_loop(),    # push sensor state to host
        command_listener_loop(), # receive and dispatch host commands
        tree_executor_loop()     # run the behavior tree
    )

run_task(main())
```

### 8.2 Coroutine Model

```
┌──────────────────────────────────────────────────────────┐
│  Pybricks multitask (cooperative scheduling)              │
│                                                           │
│  ┌─────────────────┐  ┌──────────────────┐  ┌─────────┐ │
│  │ status_stream    │  │ command_listener  │  │ tree    │ │
│  │ await wait(100)  │  │ await read_line() │  │ executor│ │
│  │ read sensors     │  │ parse + dispatch  │  │ global  │ │
│  │ write S: line    │  │ write R: line     │  │ tick    │ │
│  │ (never blocks    │  │ (yields when no   │  │ monitor │ │
│  │  long)           │  │  data available)  │  │ tick    │ │
│  └─────────────────┘  └──────────────────┘  └─────────┘ │
└──────────────────────────────────────────────────────────┘
```

All three coroutines yield cooperatively via `await wait()`. No preemption. A leaf's 10ms monitor tick yields to allow status_stream and command_listener to run.

### 8.3 Command Dispatcher

```python
import json

_dispatch_table = {}

def register_command(name, handler):
    _dispatch_table[name] = handler

async def command_listener_loop():
    while True:
        line = await transport.read_line()
        if line and line.startswith("C:"):
            try:
                msg = json.loads(line[2:])
                result = dispatch(msg["c"], msg.get("p", []))
                resp = {"id": msg["id"], "s": result[0], "d": result[1]}
                transport.write_line("R:" + json.dumps(resp))
            except Exception as e:
                transport.write_line(
                    'R:{"id":0,"s":"err","d":{"reason":"parse_error"}}')
        await wait(5)  # yield to other coroutines
```

### 8.4 Leaf Node Pattern

Every leaf node follows the same structure:

```python
class LeafNode:
    def __init__(self, name, timeout_ms=10000):
        self.name = name
        self.timeout_ms = timeout_ms
        self._start_ms = 0

    def start_action(self):
        """Override: initiate the hardware action (non-blocking)."""
        raise NotImplementedError

    def done_condition(self):
        """Override: return True when action is complete."""
        raise NotImplementedError

    def cleanup(self):
        """Override: ensure hardware is in safe state."""
        raise NotImplementedError

    def elapsed_ms(self):
        return time_ms() - self._start_ms

    async def execute(self):
        self._start_ms = time_ms()
        self.start_action()
        try:
            while True:
                fault = guard_check()
                if fault:
                    return FAILURE, fault
                if self.done_condition():
                    return SUCCESS, None
                if self.elapsed_ms() > self.timeout_ms:
                    return FAILURE, "timeout"
                await wait(10)
        finally:
            self.cleanup()
```

**Concrete example — drive straight:**

```python
class DriveStraightLeaf(LeafNode):
    def __init__(self, drivebase, distance_mm, timeout_ms=15000):
        super().__init__("drive_straight", timeout_ms)
        self.db = drivebase
        self.distance_mm = distance_mm

    def start_action(self):
        self.db.straight(self.distance_mm, wait=False)

    def done_condition(self):
        return self.db.done()

    def cleanup(self):
        self.db.brake()
```

### 8.5 Tree Executor

The tree executor runs global ticks in a loop. The tree is a simple data structure — nested dicts or a small class hierarchy:

```python
SUCCESS = 0
FAILURE = 1
RUNNING = 2

async def tree_executor_loop():
    while True:
        # Global tick: traverse from root, find one leaf, execute it
        result, detail = await tree_root.tick()

        # Report result in next status message
        _tree_status["last_leaf"] = tree_root.last_active_leaf
        _tree_status["last_result"] = result
        _tree_status["detail"] = detail

        # Brief yield before next global tick
        await wait(1)
```

Trees are pre-loaded at startup. The host sends `set_tree` or `activate_branch` commands to reconfigure which tree or branches are active. The tree executor does not build trees dynamically — it selects among pre-defined structures.

---

## 9. Host-Side Interface

### 9.1 BLE Listener and Blackboard Updater

The host runs a persistent listener on the BLE connection that reads all inbound lines and routes them:

```lua
-- host_ble_listener.lua
local json = require('dkjson')

function start_listener(transport, blackboard, response_queue)
    -- Runs in a coroutine or callback
    while true do
        local line = transport:read_line()
        if line then
            local prefix = line:sub(1, 1)
            local payload = json.decode(line:sub(3))

            if prefix == "S" then
                update_blackboard(blackboard, payload)
            elseif prefix == "R" then
                response_queue:put(payload)
            end
        end
        coroutine.yield()  -- or sleep briefly
    end
end

function update_blackboard(bb, status)
    -- Motor state
    if status.m then
        for port, vals in pairs(status.m) do
            bb:set("motor_" .. port:lower() .. "_angle",   vals[1])
            bb:set("motor_" .. port:lower() .. "_speed",   vals[2])
            bb:set("motor_" .. port:lower() .. "_stalled", vals[3])
        end
    end
    -- DriveBase
    if status.db then
        bb:set("drivebase_distance", status.db[1])
        bb:set("drivebase_angle",    status.db[2])
        bb:set("drivebase_speed",    status.db[3])
        bb:set("drivebase_done",     status.db[4])
    end
    -- Sensors
    if status.u   then bb:set("ultrasonic_distance", status.u) end
    if status.bat then bb:set("battery_voltage",     status.bat) end
    if status.f   then bb:set("fault_flags",         status.f) end
    bb:set("status_tick", status.t)
end
```

### 9.2 Command Sender

```lua
-- host_command.lua
local json = require('dkjson')

local CommandSender = {}
CommandSender.__index = CommandSender

function CommandSender.new(transport, response_queue, timeout_ms)
    return setmetatable({
        transport = transport,
        response_queue = response_queue,
        timeout_ms = timeout_ms or 2000,
        _seq = 0,
    }, CommandSender)
end

function CommandSender:send(command, params)
    self._seq = self._seq + 1
    local msg = json.encode({c = command, p = params, id = self._seq})
    self.transport:write_line("C:" .. msg)

    -- Wait for response with matching id
    local deadline = clock_ms() + self.timeout_ms
    while clock_ms() < deadline do
        local resp = self.response_queue:get_nonblocking()
        if resp and resp.id == self._seq then
            return resp.s, resp.d
        end
        coroutine.yield()
    end
    return "err", {reason = "timeout"}
end
```

### 9.3 Planner Integration

The planner interacts with the target through the command sender and reads state from the blackboard. It does not interact with the transport directly.

```lua
-- planner_target_interface.lua
-- This is the API the planner uses. Transport-agnostic.

local TargetInterface = {}
TargetInterface.__index = TargetInterface

function TargetInterface.new(command_sender, blackboard)
    return setmetatable({
        cmd = command_sender,
        bb = blackboard,
    }, TargetInterface)
end

-- Read current world state (from status stream, no BLE round-trip)
function TargetInterface:get_state()
    return {
        battery_mv     = self.bb:get("battery_voltage"),
        fault_flags    = self.bb:get("fault_flags"),
        ultrasonic_mm  = self.bb:get("ultrasonic_distance"),
        drivebase_done = self.bb:get("drivebase_done"),
        status_tick    = self.bb:get("status_tick"),
    }
end

-- Send action command (blocks until response or timeout)
function TargetInterface:execute_action(command, params)
    return self.cmd:send(command, params)
end

-- Set mission goal on target
function TargetInterface:set_goal(goal_name)
    return self.cmd:send("set_goal", {goal = goal_name})
end

-- Emergency stop
function TargetInterface:emergency_stop()
    return self.cmd:send("stop_all", {})
end
```

---

## 10. Error Handling

### 10.1 Transport Errors

| Error | Detection | Recovery |
|-------|-----------|----------|
| BLE disconnect | `transport.is_connected()` returns false | Target: `guard_check()` fires `comm_timeout`, tree stops. Host: reconnect loop, re-query state. |
| Message corruption | JSON parse failure | Drop message, log error. Next status/response proceeds normally. |
| Message loss (UDP/BLE) | Response timeout on host. Stale `t` on status. | Host retransmits command (idempotent). Stale status detected by tick counter gap. |

### 10.2 Execution Errors

| Error | Detection | Response |
|-------|-----------|----------|
| Motor stall | `motor.stalled()` in monitor tick | Leaf returns FAILURE with `"stall"`. Guard_check also catches via fault flags. |
| Bump | IMU accelerometer threshold in guard_check | Leaf returns FAILURE with `"bump"`. All motors stopped in cleanup. |
| Tilt | IMU tilt threshold in guard_check | Leaf returns FAILURE with `"tilt"`. |
| Timeout | `elapsed_ms > timeout_ms` in monitor tick | Leaf returns FAILURE with `"timeout"`. |
| Low battery | Voltage threshold in guard_check | Leaf returns FAILURE with `"low_battery"`. Host planner can issue controlled shutdown. |

All execution errors are reported in two ways:
1. Immediately in the `R:` response to the active command (if one is active).
2. In the next `S:` status message via the `f` fault flags bitmask.

### 10.3 Reconnection Protocol

After a transport reconnect, the host must not assume the previous state is valid:

1. Host sends `ping` to confirm connection.
2. Host sends `get_config` to verify target identity.
3. Host waits for at least one `S:` status message to populate the blackboard.
4. Host reads blackboard and replans from current state.

The target does not need to do anything special on reconnect. Its tree executor continues running (or is stopped by the comm_timeout guard). The host drives recovery.

---

## 11. Multi-Target Support

The architecture supports multiple targets by instantiating one transport + command sender + blackboard updater per target. The planner holds a map of target interfaces:

```lua
local targets = {
    spike_1 = TargetInterface.new(cmd_sender_1, blackboard_1),
    spike_2 = TargetInterface.new(cmd_sender_2, blackboard_2),
}
```

Each target has its own:
- Transport connection (separate BLE peripheral, Thread address, or TCP socket)
- Blackboard namespace (prefixed by target ID)
- Command sequence counter
- Status stream

The planner's action schemas reference target IDs:

```
action: navigate
  target: spike_1
  rpc: db_straight
```

Coordination between targets (e.g., "spike_1 picks up object, spike_2 drives to meeting point") is handled at the planner level by interleaving actions across targets in the plan sequence.

---

## 12. Design Constraints and Rationale

| Constraint | Rationale |
|---|---|
| Single leaf per global tick | MicroPython is single-threaded. Concurrent leaf execution would require threading or complex interleaving. One-at-a-time is deterministic and debuggable. |
| 10ms monitor tick | Matches Pybricks motor control loop. Faster wastes CPU, slower misses stall events. |
| JSON protocol (not binary) | MicroPython has `json` built in. Human-readable for debugging. BLE bandwidth is sufficient for command/response + 10 Hz status. Binary encoding adds complexity for negligible gain at these message rates. |
| Status stream separate from responses | Decouples sensor polling from command latency. The host always has fresh state regardless of command traffic. |
| Transport abstraction | BLE is the initial transport but Thread and WiFi are planned. The protocol must not depend on BLE-specific features (MTU, connection intervals). |
| Idempotent commands with sequence numbers | BLE notify and UDP are unreliable. Retransmit safety requires idempotency. The cost is one integer per message. |
| Centralized guard_check() | Safety logic must not be duplicated in every leaf. A single function is auditable and guarantees consistent behavior across all actions. |
| Cleanup in finally block | Hardware must be in a safe state after every leaf exit. The `finally` pattern is the only reliable way to guarantee this in the presence of exceptions and faults. |
| Host drives recovery, target is stateless | The target does not need to know about goals, plans, or world state history. It executes commands and reports status. All intelligence lives on the host where LuaJIT and SQLite are available. |
| Compact status encoding (short keys, integer scaling) | BLE MTU is limited. A verbose JSON status message could exceed 240 bytes with many sensors. Compact encoding keeps typical messages under 150 bytes. |
