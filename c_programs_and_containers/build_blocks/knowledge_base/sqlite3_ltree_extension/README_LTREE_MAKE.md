# SQLite ltree Extension - Build System

Build system documentation for compiling, testing, and installing the SQLite ltree extension.

## Quick Start

```bash
make              # Build library and test program
make test         # Run test suite
sudo make install # Install system-wide
```

## Prerequisites

### Required Tools

- **GCC** - GNU Compiler Collection
- **Make** - GNU Make 3.81 or later
- **SQLite3** - Version 3.8.0 or later with development headers

### Installing Prerequisites

**Ubuntu/Debian:**
```bash
sudo apt update
sudo apt install build-essential libsqlite3-dev sqlite3
```

**Red Hat/Fedora:**
```bash
sudo dnf install gcc make sqlite-devel
```

**macOS:**
```bash
xcode-select --install
brew install sqlite3
```

## Build Targets

### Primary Targets

#### `make` or `make all`
Builds both the extension library and test program.

**Output:**
- `ltree.so` (Linux) or `ltree.dylib` (macOS)
- `test_ltree` (executable)

```bash
make
```

#### `make test`
Builds the extension and test program, then runs the complete test suite.

```bash
make test
```

**Expected Output:**
```
Building ltree extension...
Built: ltree.so
Building test program...
Built: test_ltree

Running tests...

SQLite ltree Extension Test Suite
==================================
Extension loaded successfully: ./ltree.so

=== Testing Exact Matching ===
✓ PASS: Exact match - full path
✓ PASS: Exact match - no match
...
Test Results: 28 passed, 0 failed
```

#### `make install`
Installs the extension library to the system library directory.

**Default Location:** `/usr/local/lib/`

**Requires:** sudo/root privileges

```bash
sudo make install
```

#### `make uninstall`
Removes the installed extension from the system.

```bash
sudo make uninstall
```

#### `make clean`
Removes all build artifacts, object files, and temporary files.

```bash
make clean
```

**Removes:**
- `ltree.so` / `ltree.dylib`
- `test_ltree`
- `*.o` (object files)
- `*~` (backup files)

#### `make help`
Displays build system usage information.

```bash
make help
```

## Customizing the Build

### Installation Paths

Change where the extension is installed:

```bash
# Install to /opt/myapp/lib
sudo make install PREFIX=/opt/myapp LIBDIR=/opt/myapp/lib

# Install to user directory (no sudo needed)
make install PREFIX=$HOME/.local LIBDIR=$HOME/.local/lib
```

### Compiler Options

Override compiler flags in your environment:

```bash
# Debug build with symbols
make CFLAGS="-Wall -Wextra -g -fPIC -I."

# Optimized build
make CFLAGS="-Wall -Wextra -O3 -fPIC -I."

# With additional warnings
make CFLAGS="-Wall -Wextra -Wpedantic -O2 -fPIC -I."
```

### Cross-Compilation

Set the compiler for cross-compilation:

```bash
# ARM target
make CC=arm-linux-gnueabihf-gcc

# MIPS target  
make CC=mips-linux-gnu-gcc
```

## Build Configuration

### Makefile Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `CC` | `gcc` | C compiler |
| `CFLAGS` | `-Wall -Wextra -O2 -fPIC -I.` | Compiler flags |
| `LDFLAGS` | `-shared` (Linux)<br>`-shared -dynamiclib` (macOS) | Linker flags |
| `PREFIX` | `/usr/local` | Installation prefix |
| `LIBDIR` | `$(PREFIX)/lib` | Library installation directory |
| `EXT_SUFFIX` | `.so` (Linux)<br>`.dylib` (macOS) | Extension file suffix |

### Compiler Flags Explained

- `-Wall` - Enable all common warnings
- `-Wextra` - Enable extra warnings
- `-O2` - Optimization level 2 (balanced speed/size)
- `-fPIC` - Position Independent Code (required for shared libraries)
- `-I.` - Include current directory for headers

### Linker Flags

- `-shared` - Create shared library (Linux)
- `-dynamiclib` - Create dynamic library (macOS)
- `-lsqlite3` - Link against SQLite library (test program)
- `-ldl` - Link against dynamic loading library (test program)

## Platform-Specific Notes

### Linux

- Extension suffix: `.so`
- Standard linker flags work out of the box
- May need `libsqlite3-dev` package

### macOS

- Extension suffix: `.dylib`
- Adds `-dynamiclib` to linker flags
- SQLite typically pre-installed, but may need Homebrew version for headers

### Windows (MinGW/Cygwin)

Not directly supported by this Makefile. Recommended approach:

```bash
# Under MinGW
gcc -Wall -Wextra -O2 -fPIC -shared -o ltree.dll ltree_sqlite.c

# Under Cygwin
make  # Should work with minimal modifications
```

## Build Workflow

### Development Workflow

```bash
# Initial build
make

# Test changes
make clean
make test

# After major changes
make clean all
make test

# Install for system testing
sudo make install

# Uninstall after testing
sudo make uninstall
```

### Production Build

```bash
# Clean build with tests
make clean
make test

# Install to production location
sudo make install PREFIX=/opt/production LIBDIR=/opt/production/lib/sqlite
```

### Distribution Build

```bash
# Create optimized, stripped binary
make clean
make CFLAGS="-Wall -Wextra -O3 -fPIC -I."
strip ltree.so

# Create archive
tar czf ltree-extension-1.0.0.tar.gz ltree.so README_ltree.md Makefile
```

## Build Artifacts

### Generated Files

| File | Description | Size (typical) |
|------|-------------|----------------|
| `ltree.so` / `ltree.dylib` | Extension library | ~50-100 KB |
| `test_ltree` | Test executable | ~20-40 KB |

### Intermediate Files (cleaned by `make clean`)

- `*.o` - Object files
- `*~` - Editor backup files

## Troubleshooting

### Build Fails: "sqlite3.h not found"

**Solution:** Install SQLite development headers

```bash
# Ubuntu/Debian
sudo apt install libsqlite3-dev

# Red Hat/Fedora
sudo dnf install sqlite-devel

# macOS
brew install sqlite3
export CFLAGS="-I/opt/homebrew/opt/sqlite/include $CFLAGS"
```

### Build Fails: "command not found: make"

**Solution:** Install build tools

```bash
# Ubuntu/Debian
sudo apt install build-essential

# Red Hat/Fedora
sudo dnf groupinstall "Development Tools"

# macOS
xcode-select --install
```

### Test Fails: "error while loading shared libraries"

**Solution:** Add library directory to LD_LIBRARY_PATH

```bash
export LD_LIBRARY_PATH=.:$LD_LIBRARY_PATH
make test
```

### Permission Denied on Install

**Solution:** Use sudo for system directories

```bash
sudo make install
```

Or install to user directory:

```bash
make install PREFIX=$HOME/.local LIBDIR=$HOME/.local/lib
```

### Wrong Architecture on macOS

**Solution:** Specify architecture explicitly

```bash
make CFLAGS="-Wall -Wextra -O2 -fPIC -I. -arch arm64"  # Apple Silicon
make CFLAGS="-Wall -Wextra -O2 -fPIC -I. -arch x86_64" # Intel
```

## Advanced Build Scenarios

### Static Analysis

Run static analysis tools during build:

```bash
# Clang static analyzer
scan-build make

# Cppcheck
cppcheck --enable=all ltree_sqlite.c
make
```

### Profiling Build

Build with profiling support:

```bash
make clean
make CFLAGS="-Wall -Wextra -O2 -g -pg -fPIC -I."
make test
gprof test_ltree gmon.out > analysis.txt
```

### Sanitizer Builds

Build with address or memory sanitizers:

```bash
# Address sanitizer
make clean
make CFLAGS="-Wall -Wextra -g -fsanitize=address -fPIC -I." \
     LDFLAGS="-shared -fsanitize=address"
make test

# Undefined behavior sanitizer
make clean
make CFLAGS="-Wall -Wextra -g -fsanitize=undefined -fPIC -I." \
     LDFLAGS="-shared -fsanitize=undefined"
make test
```

### Verbose Build

See full compiler commands:

```bash
make clean
make V=1
```

Or examine without building:

```bash
make -n
```

## Integration with Larger Projects

### Using as Submakefile

```makefile
# In parent Makefile
.PHONY: ltree
ltree:
	$(MAKE) -C extensions/ltree

clean-ltree:
	$(MAKE) -C extensions/ltree clean
```

### CMake Integration

```cmake
# CMakeLists.txt
add_custom_target(ltree
    COMMAND make
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/extensions/ltree
)
```

### Automating Tests in CI/CD

```bash
# CI build script
#!/bin/bash
set -e

cd extensions/ltree
make clean
make test

if [ $? -eq 0 ]; then
    echo "✓ ltree extension tests passed"
    exit 0
else
    echo "✗ ltree extension tests failed"
    exit 1
fi
```

## Makefile Structure

### Phony Targets

All action targets are marked `.PHONY` to ensure they run regardless of file state:

```makefile
.PHONY: all clean test install uninstall help
```

### Automatic Variables

- `$@` - Target name
- `$<` - First prerequisite
- `$^` - All prerequisites

### Pattern Rules

The Makefile uses explicit rules for clarity, but could be optimized with patterns:

```makefile
%.o: %.c
	$(CC) $(CFLAGS) -c -o $@ $<
```

## Performance Notes

### Build Times

Typical build times on modern hardware:

- **Clean build:** < 1 second
- **Test execution:** < 1 second
- **Total workflow:** < 5 seconds

### Parallel Builds

The extension is small, so parallel builds provide minimal benefit:

```bash
make -j4  # Build with 4 parallel jobs
```

## Best Practices

### Regular Testing

Always run tests after modifications:

```bash
make clean && make test
```

### Version Control

Commit only source files, not build artifacts:

```gitignore
# .gitignore
ltree.so
ltree.dylib
test_ltree
*.o
*~
```

### Release Checklist

1. Update version documentation
2. `make clean`
3. `make test` - verify all tests pass
4. Build release binary: `make CFLAGS="-Wall -Wextra -O3 -fPIC -I."`
5. `strip ltree.so` - remove debug symbols
6. Create distribution archive
7. Tag release in version control

## Related Files

- `ltree_sqlite.c` - Extension implementation
- `test_ltree.c` - Test suite
- `README_ltree.md` - User documentation

## Support

For build issues:
1. Check compiler and SQLite versions
2. Review error messages carefully
3. Try `make clean` before rebuilding
4. Verify all prerequisites are installed
5. Check platform-specific notes above

## License

Part of the ChainTree distributed control system architecture.
