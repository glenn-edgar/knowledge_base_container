-- config_example.lua
-- Example port configuration for the serial test driver.
-- Copy this to your own config and adjust port names, baud rates, etc.
--
-- Usage: luajit test_driver.lua --config config_example.lua

return {
    ---------------------------------------------------------------------------
    -- USB Serial  (shows up as COMn when you plug in a USB-to-serial adapter
    -- or a device with USB CDC/ACM like ESP32, STM32, etc.)
    ---------------------------------------------------------------------------
    usb = {
        type     = "serial",
        port     = "COM3",          -- adjust to your actual COM port
        baud     = 115200,
        databits = 8,
        parity   = "none",
        stopbits = "1",
        read_timeout_ms  = 100,
        write_timeout_ms = 100,
        label    = "USB-Serial",
    },

    ---------------------------------------------------------------------------
    -- Bluetooth SPP  (Classic Bluetooth Serial Port Profile)
    -- Pair the BT device in Windows first.  It will appear as a COM port
    -- in Device Manager > Ports.  Outgoing port is the one you want.
    ---------------------------------------------------------------------------
    bt = {
        type     = "serial",
        port     = "COM5",          -- BT SPP outgoing COM port
        baud     = 115200,
        databits = 8,
        parity   = "none",
        stopbits = "1",
        read_timeout_ms  = 200,     -- BT can be slower
        write_timeout_ms = 200,
        label    = "BT-SPP",
    },

    ---------------------------------------------------------------------------
    -- BLE (Bluetooth Low Energy)
    -- Uses GATT characteristic writes.  Requires either:
    --   a) ble_helper.dll (native backend)
    --   b) ble_bridge.py running as a named-pipe server (bridge backend)
    --
    -- Common BLE UART services:
    --   Nordic UART (NUS):
    --     Service:  6e400001-b5a3-f393-e0a9-e50e24dcca9e
    --     TX Char:  6e400002-b5a3-f393-e0a9-e50e24dcca9e  (write to this)
    --     RX Char:  6e400003-b5a3-f393-e0a9-e50e24dcca9e  (notifications)
    --
    --   TI UART:
    --     Service:  0000ffe0-0000-1000-8000-00805f9b34fb
    --     Char:     0000ffe1-0000-1000-8000-00805f9b34fb
    ---------------------------------------------------------------------------
    ble = {
        type         = "ble",
        -- Connect by name (scans for this device):
        device_name  = "MyBLEDevice",
        scan_timeout = 5000,          -- ms to scan before giving up
        -- OR connect by address:
        -- address   = 0xAABBCCDDEEFF,

        -- GATT target
        service_uuid = "6e400001-b5a3-f393-e0a9-e50e24dcca9e",
        char_uuid    = "6e400002-b5a3-f393-e0a9-e50e24dcca9e",
        write_type   = 1,  -- 0 = with response, 1 = without response

        -- Backend selection
        backend      = "bridge",      -- "native" or "bridge"
        pipe_name    = "\\\\.\\pipe\\ble_bridge",  -- for bridge backend

        label        = "BLE-UART",
    },

    ---------------------------------------------------------------------------
    -- You can add more ports.  The name (key) is used in scripts to
    -- target specific ports:
    --   S.msg("hello", { port = "usb2" })
    ---------------------------------------------------------------------------
    -- usb2 = {
    --     type  = "serial",
    --     port  = "COM7",
    --     baud  = 9600,
    --     label = "USB-Debug",
    -- },
}