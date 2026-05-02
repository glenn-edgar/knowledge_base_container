# Unit tests

Compact, no broker, no rover. Driven through `run_tests.sh --skip-e2e`.

## C tests

Each binary lives at the top of the source tree alongside the
Makefile; `make all` builds them. They write `[ PASS / FAIL ]` lines
that the harness scrapes via `grep -c '^  PASS'`.

A new C test follows the pattern:

```c
static int passed = 0, failed = 0;
#define ASSERT(cond, name) do { \
    if (cond) { printf("  PASS  %s\n", name); passed++; } \
    else      { printf("  FAIL  %s\n", name); failed++; } } while (0)

int main(void) {
    /* ... drive subject under test ... */
    printf("\n%s  %d passed, %d failed\n",
           failed == 0 ? "[PASS]" : "[FAIL]", passed, failed);
    return failed == 0 ? 0 : 1;
}
```

Add to `Makefile` as a new target + to `run_tests.sh` step 2 with the
log scraping pattern.

## Lua tests

`test_physics.lua` (sim-only, no FFI to libcomm) and
`test_robot_controller_contract.lua` (pure Lua, no FFI at all) follow
the same `PASS/FAIL` print convention so `run_tests.sh` can scrape
counts.

A new Lua test file:

```lua
local pass, fail = 0, 0
local function check(name, ok, detail)
    if ok then
        pass = pass + 1; io.stderr:write("  PASS  " .. name .. "\n")
    else
        fail = fail + 1; io.stderr:write("  FAIL  " .. name .. "  " .. tostring(detail) .. "\n")
    end
end

-- ... drive subject ...

io.stderr:write(string.format("\n%s  %d passed, %d failed\n",
    fail == 0 and "[PASS]" or "[FAIL]", pass, fail))
os.exit(fail == 0 and 0 or 1)
```

Wire into `run_tests.sh` step 2 with a `cd "$SCRIPT_DIR" && luajit
<file>.lua` invocation.

## What unit tests catch (and don't)

**Catch:** wire-format drift (frame, manifest), per-component
behavior (drive_base lifecycle, ext_bus contract), state-machine
transitions (logical_robot, supervisor), contract conformance
(robot_controller fixture).

**Don't catch:** integration timing, broker outages, container-build
gotchas, libcomm/libphysics arch mismatches, real serial wire issues.
Those need [e2e](e2e.md) or [containers](../containers/index.md) testing.
