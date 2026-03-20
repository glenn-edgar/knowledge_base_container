-- scripts.lua
-- Example message scripts for the serial test driver.
-- Each function returns an array of script entries.

local S = require("script_engine").S

local M = {}

---------------------------------------------------------------------------
-- Basic heartbeat: send a status ping, wait, repeat
---------------------------------------------------------------------------
function M.heartbeat(interval_ms)
    interval_ms = interval_ms or 1000
    return {
        S.label("heartbeat"),
        S.msg("PING\r\n"),
        S.delay(interval_ms),
    }
end

---------------------------------------------------------------------------
-- Burst + pause: send a burst of messages, then pause
---------------------------------------------------------------------------
function M.burst_pattern(burst_count, pause_ms)
    burst_count = burst_count or 5
    pause_ms    = pause_ms or 2000
    local script = { S.label("burst_start") }

    for i = 1, burst_count do
        script[#script + 1] = S.msg(string.format("MSG_%03d\r\n", i))
    end

    script[#script + 1] = S.delay(pause_ms)
    return script
end

---------------------------------------------------------------------------
-- Throughput test: continuous binary payloads of a given size
-- (no delays, maximum sustained throughput)
---------------------------------------------------------------------------
function M.throughput_flood(payload_size, delay_between_ms)
    payload_size     = payload_size or 64
    delay_between_ms = delay_between_ms or 0
    local payload    = string.rep("X", payload_size) .. "\r\n"

    local script = { S.label("throughput_flood") }
    script[#script + 1] = S.msg(payload)
    if delay_between_ms > 0 then
        script[#script + 1] = S.delay(delay_between_ms)
    end
    return script
end

---------------------------------------------------------------------------
-- Sequenced messages with incrementing counter
---------------------------------------------------------------------------
function M.sequenced(delay_ms)
    delay_ms = delay_ms or 500
    return {
        S.label("sequenced"),
        S.set("seq", 0),
        S.template("SEQ=%06d\r\n", { "seq" }),
        S.set("seq", 1),  -- note: static increment (see test_driver for dynamic)
        S.delay(delay_ms),
    }
end

---------------------------------------------------------------------------
-- Multi-port demo: send different messages to different ports
---------------------------------------------------------------------------
function M.multi_port_demo(delay_ms)
    delay_ms = delay_ms or 1000
    return {
        S.label("multi_port_demo"),
        -- USB gets one message
        S.msg("USB_HELLO\r\n",   { port = "usb" }),
        -- Bluetooth SPP gets another
        S.msg("BT_HELLO\r\n",    { port = "bt" }),
        -- BLE gets a third
        S.msg("BLE_HELLO\r\n",   { port = "ble" }),
        -- All ports get this one
        S.msg("ALL_SYNC\r\n"),
        S.delay(delay_ms),
    }
end

---------------------------------------------------------------------------
-- Protocol simulation: command-response pattern
-- Sends a command, short delay for device response, then next command
---------------------------------------------------------------------------
function M.command_sequence(delay_ms)
    delay_ms = delay_ms or 200
    return {
        S.label("init_phase"),
        S.msg("AT\r\n"),
        S.delay(delay_ms),
        S.msg("AT+VERSION?\r\n"),
        S.delay(delay_ms),
        S.msg("AT+STATUS?\r\n"),
        S.delay(delay_ms),

        S.label("config_phase"),
        S.msg("AT+MODE=1\r\n"),
        S.delay(delay_ms),
        S.msg("AT+BAUD=115200\r\n"),
        S.delay(delay_ms),

        S.label("data_phase"),
        S.msg("DATA:SENSOR_A=1234\r\n"),
        S.msg("DATA:SENSOR_B=5678\r\n"),
        S.msg("DATA:SENSOR_C=9012\r\n"),
        S.delay(delay_ms),

        S.msg("AT+IDLE\r\n"),
        S.delay(1000),
    }
end

---------------------------------------------------------------------------
-- Hex payload test: raw binary frames
---------------------------------------------------------------------------
function M.binary_frames(delay_ms)
    delay_ms = delay_ms or 500
    return {
        S.label("binary_frames"),
        -- STX + payload + ETX
        S.hex("02 48 45 4C 4C 4F 03"),
        S.delay(delay_ms),
        -- Modbus-style frame
        S.hex("01 03 00 00 00 0A C5 CD"),
        S.delay(delay_ms),
        -- Custom framing
        S.hex("AA 55 01 02 03 04 FF"),
        S.delay(delay_ms),
    }
end

---------------------------------------------------------------------------
-- Stress ramp: increasing message sizes
---------------------------------------------------------------------------
function M.stress_ramp(min_size, max_size, step, delay_ms)
    min_size = min_size or 8
    max_size = max_size or 256
    step     = step or 8
    delay_ms = delay_ms or 100

    local script = { S.label("stress_ramp") }
    for size = min_size, max_size, step do
        local payload = string.rep("A", size) .. "\r\n"
        script[#script + 1] = S.msg(payload)
        script[#script + 1] = S.delay(delay_ms)
    end
    return script
end

return M