# Runtime

Multi-VM infrastructure. Level 1 — shared by everything.

## Boundary
- **Provides:** Pipes, threads, VMs, transport abstraction
- **Knows about:** Threading, IPC, VM lifecycle
- **Does NOT know about:** Planning, packets, robots, behavior trees

## Contents
- `vmrt.h / vmrt.c` — C lib: SPSC ringbuffer, bidirectional pipe, thread launcher
- `Makefile` — builds libvmrt.so
- `vmrt_ffi.lua` — LuaJIT FFI bindings
- `transport.lua` — generic hub↔remote transport (ringbuf, loopback, future: serial)
- `hub_vm.lua` — generic hub thread harness
- `remote_vm.lua` — generic remote thread harness
