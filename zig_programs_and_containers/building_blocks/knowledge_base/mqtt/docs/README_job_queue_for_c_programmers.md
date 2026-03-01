# mqtt_queue — Zig MQTT Queue Library

A Zig port of the C `mqtt_queue` library. Provides reliable queued messaging
over MQTT v3.1.1 using persistent sessions, wrapping the Mosquitto C client
library (libmosquitto).

This README is written for C programmers who are new to Zig.

---

## Prerequisites

```bash
sudo apt install libmosquitto-dev
```

You also need the Zig compiler (this project was tested with Zig 0.13).
Download from https://ziglang.org/download/

---

## Project Structure

```
mqtt_queue_zig/
├── build.zig               ← Build script (replaces Makefile)
├── build.zig.zon           ← Package manifest (like package.json or Cargo.toml)
├── src/
│   ├── root.zig            ← Public API entry point (like your main .h file)
│   └── mqtt_queue.zig      ← Full implementation (like your .c + .h combined)
└── test/
    └── mqtt_queue_test.zig ← Integration test driver (like your test/mqtt_queue_test.c)
```

After building, Zig creates:

```
zig-out/
└── lib/
    ├── libmqtt_queue.a     ← Static library
    └── libmqtt_queue.so    ← Shared library
```

---

## File-by-File Explanation

### `build.zig` — The Build Script

In C you write a `Makefile`. In Zig, the build system is written in Zig itself.
This file tells the compiler:

- What to build (static lib, shared lib, test executable)
- What C libraries to link (`libmosquitto`, `libc`)
- How modules connect (the test executable imports the library as a named module)

There is no `cmake`, `autoconf`, or `pkg-config` involved. The Zig build system
handles all of it.

Key lines and what they mean:

```zig
static_lib.linkSystemLibrary("mosquitto");  // equivalent to: -lmosquitto
static_lib.linkLibC();                       // equivalent to: -lc
```

```zig
// This is how the test executable can `@import("mqtt_queue")` even though
// the source file lives in a different directory. In C you'd use -I flags.
test_exe.root_module.addImport("mqtt_queue", &static_lib.root_module);
```

### `build.zig.zon` — Package Manifest

A small metadata file declaring the package name, version, and which directories
belong to it. Think of it like the metadata section of a `CMakeLists.txt`.

### `src/root.zig` — Public API Surface

In C, your `.h` file defines the public API. In Zig, `root.zig` serves that
role. It re-exports the types and functions from `mqtt_queue.zig` that
external consumers should see:

```zig
pub const Publisher = mqtt_queue.Publisher;
pub const Reader    = mqtt_queue.Reader;
pub const Config    = mqtt_queue.Config;
pub const libInit   = mqtt_queue.libInit;
// ... etc
```

Anything not re-exported here is internal to the library.

### `src/mqtt_queue.zig` — The Implementation

This is the main library file. It replaces both `mqtt_queue.h` and
`mqtt_queue.c` from the C version. Zig does not separate declarations from
definitions — everything lives in one file.

### `test/mqtt_queue_test.zig` — Integration Test

Equivalent to `test/mqtt_queue_test.c`. Connects to a live Mosquitto broker,
publishes messages, tests persistent sessions, and drains queued messages.

---

## Building and Running

```bash
zig build                          # build .a and .so
zig build -Doptimize=ReleaseFast   # optimized build (like -O2)
zig build test                     # run unit tests (no broker needed)
zig build run-test                 # run integration test (broker must be running)
```

All output goes to `zig-out/`. There is no need to create a `build/` directory
or run `make clean` — Zig handles caching automatically.

---

## C-to-Zig Concept Map

This section maps C patterns in the original code to their Zig equivalents.

### No Header Files

In C you maintain a `.h` and a `.c` separately. In Zig there is only one file.
Public functions are marked with `pub`. Everything else is private by default.

```c
// C: mqtt_queue.h
int mqtt_publisher_connect(mqtt_publisher_t *pub, int timeout_ms);
```

```zig
// Zig: inside Publisher struct
pub fn connect(self: *Self, timeout_ms: u64) Error!void { ... }
```

### Structs with Methods

In C, functions take a pointer to the struct as the first argument. In Zig,
structs can contain functions. When the first parameter is `*Self`, Zig lets
you call it with dot syntax:

```c
// C
mqtt_publisher_t pub;
mqtt_publisher_init(&pub, &cfg);
mqtt_publisher_connect(&pub, 5000);
mqtt_publisher_publish(&pub, topic, payload, 1, false);
```

```zig
// Zig
var publisher = try Publisher.init(cfg);
try publisher.connect(5000);
try publisher.publish(topic, payload, .at_least_once, false);
```

### Error Handling

C uses return codes (`0` for success, `-1` for failure). Zig uses error unions.
The `try` keyword propagates errors upward (like checking the return value and
returning early).

```c
// C
int rc = mqtt_publisher_connect(&pub, 5000);
if (rc != 0) {
    mqtt_publisher_destroy(&pub);
    return -1;
}
```

```zig
// Zig — try returns the error automatically if connect fails
try publisher.connect(5000);
```

### Resource Cleanup with `defer`

In C you manually call cleanup functions, often using `goto cleanup`. In Zig,
`defer` runs a statement when the current scope exits, regardless of how it
exits (return, error, etc.):

```c
// C
mqtt_publisher_t pub;
mqtt_publisher_init(&pub, &cfg);
// ... use pub ...
mqtt_publisher_disconnect(&pub);  // must remember to call this
mqtt_publisher_destroy(&pub);     // must remember to call this
```

```zig
// Zig — cleanup happens automatically when scope exits
var publisher = try Publisher.init(cfg);
defer publisher.deinit();
try publisher.connect(5000);
defer publisher.disconnect();
// ... use publisher ...
// deinit and disconnect are called automatically here
```

### Null Pointers → Optionals

C uses `NULL` pointers. Zig uses optionals, written with `?`. You must
explicitly handle the null case — the compiler won't let you forget:

```c
// C
const char *username = NULL;       // might be null
if (username) { ... }
```

```zig
// Zig
username: ?[:0]const u8 = null,    // might be null
if (cfg.username) |user| { ... }   // compiler forces you to unwrap
```

### Enums Instead of Magic Numbers

The C code uses bare integers for QoS levels (0, 1, 2). The Zig version uses
a typed enum so the compiler catches mistakes:

```c
// C
mqtt_publisher_publish(&pub, topic, payload, 1, false);  // what is 1?
```

```zig
// Zig
try publisher.publish(topic, payload, .at_least_once, false);  // self-documenting
```

### Memory Allocation

C uses `malloc`/`free`/`calloc`. Zig passes an explicit allocator, which means:

- No hidden global state
- Easy to swap allocators (arena, testing, etc.)
- The testing allocator detects memory leaks automatically

```c
// C
mqtt_msg_t *m = calloc(1, sizeof(*m));
m->topic = strdup(topic);
// ... later ...
free(m->topic);
free(m);
```

```zig
// Zig
const node = try allocator.create(Message);
node.topic = try allocator.dupe(u8, topic);
// ... later ...
allocator.free(node.topic);
allocator.destroy(node);
```

### Linked List Iteration

```c
// C
for (mqtt_msg_t *m = msgs; m; m = m->next) {
    printf("Topic: %s\n", m->topic);
}
```

```zig
// Zig
var cur = msgs;
while (cur) |m| {
    print("Topic: {s}\n", .{m.topic});
    cur = m.next;
}
```

### Calling C Libraries

Zig can call C libraries directly with `@cImport`. No bindings generator
or FFI layer is needed:

```zig
const c = @cImport({
    @cInclude("mosquitto.h");
});

// Then call C functions directly:
_ = c.mosquitto_lib_init();
const mosq = c.mosquitto_new(client_id, clean_session, null);
```

The build system links the C library automatically:

```zig
static_lib.linkSystemLibrary("mosquitto");
static_lib.linkLibC();
```

### Callbacks and Calling Conventions

Mosquitto callbacks are C function pointers. In Zig, you annotate them with
`callconv(.C)` so the compiler uses the C calling convention:

```c
// C
static void pub_on_connect(struct mosquitto *mosq, void *userdata, int rc) { ... }
```

```zig
// Zig
fn pubOnConnect(mosq: ?*c.struct_mosquitto, userdata: ?*anyopaque, rc: c_int) callconv(.C) void { ... }
```

### Thread Synchronisation

The C version uses `pthread_mutex_t` and `pthread_cond_t`. The Zig version
uses `std.Thread.Mutex` and `std.Thread.Condition`, which are cross-platform
and part of the standard library:

```c
// C
pthread_mutex_lock(&pub->lock);
pthread_cond_timedwait(&pub->connect_cond, &pub->lock, &ts);
pthread_mutex_unlock(&pub->lock);
```

```zig
// Zig
self.mutex.lock();
defer self.mutex.unlock();   // unlock happens automatically
self.connect_cond.timedWait(&self.mutex, timeout_ns) catch {};
```

### Strings

C strings are `char *` with a null terminator. Zig has two string types:

- `[]const u8` — a slice (pointer + length), no null terminator needed
- `[:0]const u8` — a slice that is also null-terminated (needed when passing to C)

The library API uses `[:0]const u8` for parameters that get forwarded to C
functions (topics, hostnames), and `[]const u8` for payloads where length is
already known.

### Logging

C uses `printf` and `fprintf(stderr, ...)`. Zig uses `std.log`, which is
structured, scoped, and filterable:

```c
// C
printf("[publisher] on_connect rc=%d\n", rc);
fprintf(stderr, "[publisher] connect timeout\n");
```

```zig
// Zig
log.info("[publisher] on_connect rc={d}", .{rc});
log.err("[publisher] connect timeout", .{});
```

---

## Key Architectural Difference: Userdata Pointer

The C version passes `&pub` (a pointer to the caller's struct) into
`mosquitto_new` during init. This works because C's `mqtt_publisher_init`
receives a pointer that the caller already owns.

In Zig, `init` returns a struct by value. The address of the local variable
inside `init` becomes invalid after return. So the Zig version passes `null`
to `mosquitto_new` and sets the userdata later in `connect`, where `self` is
a stable pointer to the caller's struct:

```zig
pub fn init(cfg: Config) Error!Self {
    // ...
    const mosq = c.mosquitto_new(cid, cfg.clean_session, null);  // null here
    // ...
}

pub fn connect(self: *Self, timeout_ms: u64) Error!void {
    c.mosquitto_user_data_set(self.mosq, @ptrCast(self));  // stable pointer here
    // ...
}
```

This is the single most important difference between the C and Zig versions.
If you port other callback-heavy C code to Zig, watch for this pattern.
