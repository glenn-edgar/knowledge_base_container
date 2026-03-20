-- test_driver.lua
-- Main entry point for the serial test driver.
--
-- Usage:
--   luajit test_driver.lua                        -- run with defaults
--   luajit test_driver.lua --script burst          -- select a script
--   luajit test_driver.lua --cycles 10             -- limit cycles
--   luajit test_driver.lua --verbose               -- verbose logging
--   luajit test_driver.lua --dry-run               -- no real hardware
--   luajit test_driver.lua --config myconfig.lua   -- custom config

local ffi = require("ffi")

---------------------------------------------------------------------------
-- Parse command-line arguments
---------------------------------------------------------------------------
local function parse_args(argv)
    local args = {
        script   = "command_sequence",
        cycles   = nil,         -- nil = unlimited
        verbose  = false,
        dry_run  = false,
        config   = nil,
    }
    local i = 1
    while i <= #argv do
        local a = argv[i]
        if a == "--script" then
            i = i + 1; args.script = argv[i]
        elseif a == "--cycles" then
            i = i + 1; args.cycles = tonumber(argv[i])
        elseif a == "--verbose" or a == "-v" then
            args.verbose = true
        elseif a == "--dry-run" then
            args.dry_run = true
        elseif a == "--config" then
            i = i + 1; args.config = argv[i]
        elseif a == "--help" or a == "-h" then
            print([[
Serial Test Driver - LuaJIT
Usage: luajit test_driver.lua [options]

Options:
  --script NAME    Script to run (default: command_sequence)
                   Available: heartbeat, burst_pattern, throughput_flood,
                   sequenced, multi_port_demo, command_sequence,
                   binary_frames, stress_ramp
  --cycles N       Stop after N cycles (default: unlimited)
  --verbose, -v    Verbose logging
  --dry-run        Use stub ports (no hardware)
  --config FILE    Load port config from a Lua file
  --help, -h       Show this help
]])
            os.exit(0)
        end
        i = i + 1
    end
    return args
end

---------------------------------------------------------------------------
-- Stub port for dry-run testing
---------------------------------------------------------------------------
local StubPort = {}
StubPort.__index = StubPort

function StubPort.new(name)
    return setmetatable({ _name = name, _open = true, _bytes = 0 }, StubPort)
end
function StubPort:write(data)
    self._bytes = self._bytes + #data
    return #data
end
function StubPort:read(n)   return "" end
function StubPort:flush()   end
function StubPort:close()   self._open = false end
function StubPort:is_open() return self._open end
function StubPort:label()   return self._name .. " (stub)" end
function StubPort:stats()   return { bytes_written = self._bytes } end

---------------------------------------------------------------------------
-- Load port configuration
---------------------------------------------------------------------------
local function load_default_config()
    return {
        usb = {
            type = "serial",
            port = "COM3",
            baud = 115200,
            label = "USB-Serial",
        },
        bt = {
            type = "serial",
            port = "COM5",
            baud = 115200,
            label = "BT-SPP",
        },
        ble = {
            type = "ble",
            device_name  = "MyBLEDevice",
            service_uuid = "6e400001-b5a3-f393-e0a9-e50e24dcca9e",  -- Nordic UART
            char_uuid    = "6e400002-b5a3-f393-e0a9-e50e24dcca9e",  -- TX char
            write_type   = 1,  -- write-without-response for throughput
            label        = "BLE-UART",
            backend      = "bridge",
        },
    }
end

local function load_config(config_path)
    if config_path then
        local chunk, err = loadfile(config_path)
        if not chunk then
            io.stderr:write("config load error: " .. tostring(err) .. "\n")
            os.exit(1)
        end
        return chunk()
    end
    return load_default_config()
end

---------------------------------------------------------------------------
-- Open ports from config
---------------------------------------------------------------------------
local function open_ports(config, dry_run)
    local ports = {}

    for name, cfg in pairs(config) do
        if dry_run then
            ports[name] = StubPort.new(cfg.label or name)
            print(string.format("  [%s] stub port", name))
        else
            if cfg.type == "serial" then
                local SerialPort = require("win32_serial")
                local port, err = SerialPort.open(cfg.port, {
                    baud           = cfg.baud or 115200,
                    databits       = cfg.databits or 8,
                    parity         = cfg.parity or "none",
                    stopbits       = cfg.stopbits or "1",
                    read_timeout_ms  = cfg.read_timeout_ms or 100,
                    write_timeout_ms = cfg.write_timeout_ms or 100,
                    label          = cfg.label or name,
                })
                if not port then
                    io.stderr:write(string.format(
                        "  [%s] FAILED to open %s: %s\n",
                        name, cfg.port, tostring(err)))
                    -- Continue with remaining ports
                else
                    ports[name] = port
                    print(string.format("  [%s] opened %s @ %d baud",
                        name, cfg.port, cfg.baud or 115200))
                end

            elseif cfg.type == "ble" then
                local BlePort = require("ble_port")
                local port, err = BlePort.open({
                    address      = cfg.address,
                    device_name  = cfg.device_name,
                    scan_timeout = cfg.scan_timeout or 5000,
                    service_uuid = cfg.service_uuid,
                    char_uuid    = cfg.char_uuid,
                    write_type   = cfg.write_type or 0,
                    label        = cfg.label or name,
                    backend      = cfg.backend or "native",
                    pipe_name    = cfg.pipe_name,
                })
                if not port then
                    io.stderr:write(string.format(
                        "  [%s] FAILED to open BLE: %s\n",
                        name, tostring(err)))
                else
                    ports[name] = port
                    print(string.format("  [%s] BLE connected (%s)",
                        name, cfg.device_name or "addr"))
                end

            else
                io.stderr:write(string.format(
                    "  [%s] unknown port type: %s\n", name, tostring(cfg.type)))
            end
        end
    end

    return ports
end

---------------------------------------------------------------------------
-- Close all ports
---------------------------------------------------------------------------
local function close_ports(ports)
    for name, port in pairs(ports) do
        pcall(function() port:close() end)
        print(string.format("  [%s] closed", name))
    end
end

---------------------------------------------------------------------------
-- Logging
---------------------------------------------------------------------------
local function make_logger(verbose)
    local start_ms = nil

    return function(msg)
        if not start_ms then
            -- Lazy init to avoid FFI timing issues at require time
            local ffi = require("ffi")
            local k32 = ffi.load("kernel32")
            local t = ffi.new("LARGE_INTEGER")
            k32.QueryPerformanceCounter(t)
            local f = ffi.new("LARGE_INTEGER")
            k32.QueryPerformanceFrequency(f)
            start_ms = tonumber(t.QuadPart) * 1000.0 / tonumber(f.QuadPart)
        end

        -- Timestamp relative to start
        local k32 = ffi.load("kernel32")
        local t = ffi.new("LARGE_INTEGER")
        k32.QueryPerformanceCounter(t)
        local f = ffi.new("LARGE_INTEGER")
        k32.QueryPerformanceFrequency(f)
        local now = tonumber(t.QuadPart) * 1000.0 / tonumber(f.QuadPart)
        local elapsed = now - start_ms

        io.write(string.format("[%10.1f ms] %s\n", elapsed, msg))
        io.flush()
    end
end

---------------------------------------------------------------------------
-- Main
---------------------------------------------------------------------------
local function main()
    local args = parse_args(arg or {})

    print("===========================================")
    print("  Serial Test Driver")
    print("  LuaJIT " .. jit.version)
    print("===========================================")
    print()

    -- Load config
    print("Loading port config...")
    local config = load_config(args.config)

    -- Open ports
    print("Opening ports" .. (args.dry_run and " (DRY RUN)" or "") .. "...")
    local ports = open_ports(config, args.dry_run)

    local port_count = 0
    for _ in pairs(ports) do port_count = port_count + 1 end
    if port_count == 0 then
        io.stderr:write("\nERROR: No ports opened. Exiting.\n")
        os.exit(1)
    end
    print(string.format("\n%d port(s) ready.\n", port_count))

    -- Load script
    local scripts = require("scripts")
    local script_fn = scripts[args.script]
    if not script_fn then
        io.stderr:write(string.format(
            "ERROR: Unknown script '%s'\nAvailable: ", args.script))
        local names = {}
        for k, v in pairs(scripts) do
            if type(v) == "function" then names[#names + 1] = k end
        end
        table.sort(names)
        io.stderr:write(table.concat(names, ", ") .. "\n")
        close_ports(ports)
        os.exit(1)
    end

    local script_data = script_fn()
    print(string.format("Script '%s': %d entries", args.script, #script_data))

    -- Create engine
    local ScriptEngine = require("script_engine")
    local log = make_logger(args.verbose)

    local engine = ScriptEngine.new(script_data, ports, {
        log      = log,
        loop     = true,
        verbose  = args.verbose,
        on_cycle = function(n)
            log(string.format("=== cycle %d complete ===", n))
        end,
    })

    -- Run
    print(string.format("Running (cycles=%s, verbose=%s)...\n",
        args.cycles and tostring(args.cycles) or "unlimited",
        tostring(args.verbose)))
    print("Press Ctrl+C to stop.\n")

    local ok, err = pcall(function()
        engine:run(args.cycles)
    end)

    if not ok then
        print("\nStopped: " .. tostring(err))
    end

    -- Stats
    local st = engine:stats()
    print(string.format("\n--- Results ---"))
    print(string.format("  Messages sent: %d", st.messages))
    print(string.format("  Errors:        %d", st.errors))
    print(string.format("  Cycles:        %d", st.cycles))

    -- Cleanup
    print("\nClosing ports...")
    close_ports(ports)
    print("Done.")
end

main()