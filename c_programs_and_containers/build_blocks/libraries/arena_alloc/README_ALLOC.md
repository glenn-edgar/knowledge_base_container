# Arena Allocator

A lightweight, portable arena/region allocator for embedded systems and cross-platform applications. Features offset-based addressing for position-independent, serializable data structures.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![C Standard](https://img.shields.io/badge/C-C11-blue.svg)](https://en.wikipedia.org/wiki/C11_%28C_standard_revision%29)
[![Platform](https://img.shields.io/badge/Platform-ARM%20%7C%20x86%20%7C%20RISC--V-green.svg)](https://github.com)

## 🎯 Features

- **Offset-based addressing** - 32-bit offsets instead of pointers
- **Position-independent** - Relocate entire arena without breaking references
- **Serializable** - Save/load arena as-is, offsets remain valid
- **ARM-aligned** - Automatic 4-byte alignment for ARM processors
- **Zero malloc** - Optional static buffer mode (no dynamic allocation)
- **Virtual class pattern** - OOP-style API with vtable
- **Cross-platform** - Works on x86, ARM, RISC-V, AVR
- **Embedded-friendly** - Suitable for Arduino, ESP32, STM32, etc.
- **No GNU extensions** - Pure C11, works with all compilers

## 📋 Quick Start
```c
#include "arena_alloc.h"

int main(void) {
    // Create arena
    ArenaAllocator* arena = arena_create(4096);
    
    // Allocate memory (returns offset, not pointer)
    uint32_t offset = ARENA_ALLOC(arena, 100);
    
    // Convert offset to pointer when needed
    void* ptr = ARENA_TO_PTR(arena, offset);
    
    // Use the memory
    memset(ptr, 0, 100);
    
    // Cleanup
    ARENA_DESTROY(arena);
    
    return 0;
}

