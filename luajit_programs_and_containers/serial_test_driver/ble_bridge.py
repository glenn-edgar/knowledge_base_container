#!/usr/bin/env python3
"""
ble_bridge.py - Named-pipe BLE bridge for the LuaJIT serial test driver.

Provides a Win32 named-pipe server that accepts simple text commands
from the LuaJIT ble_port.lua bridge backend, translating them into
BLE GATT operations via the 'bleak' library.

Requires:
    pip install bleak

Usage:
    python ble_bridge.py                          # default pipe name
    python ble_bridge.py --pipe \\.\pipe\my_ble   # custom pipe name
    python ble_bridge.py --verbose                 # debug logging

Protocol (line-based, text over named pipe):
    Client sends:
        CONNECT name=<device_name> timeout=<ms>
        CONNECT addr=<hex_address>
        WRITE svc=<uuid> chr=<uuid> type=<0|1> data=<hex>
        READ svc=<uuid> chr=<uuid> max=<bytes>
        CLOSE
    Server replies:
        OK [details]
        ERR <message>
"""

import asyncio
import argparse
import logging
import sys
import struct
import ctypes
import ctypes.wintypes as wt
from pathlib import Path

try:
    from bleak import BleakClient, BleakScanner
    from bleak.backends.characteristic import BleakGATTCharacteristic
except ImportError:
    print("ERROR: 'bleak' library required.  Install with: pip install bleak")
    sys.exit(1)


log = logging.getLogger("ble_bridge")

# -------------------------------------------------------------------------
# Win32 Named Pipe helpers
# -------------------------------------------------------------------------
kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)

PIPE_ACCESS_DUPLEX     = 0x00000003
PIPE_TYPE_BYTE         = 0x00000000
PIPE_READMODE_BYTE     = 0x00000000
PIPE_WAIT              = 0x00000000
PIPE_UNLIMITED_INSTANCES = 255
INVALID_HANDLE_VALUE   = ctypes.c_void_p(-1).value
BUFFER_SIZE            = 4096

CreateNamedPipeW   = kernel32.CreateNamedPipeW
ConnectNamedPipe   = kernel32.ConnectNamedPipe
DisconnectNamedPipe = kernel32.DisconnectNamedPipe
ReadFile           = kernel32.ReadFile
WriteFile          = kernel32.WriteFile
CloseHandle        = kernel32.CloseHandle
FlushFileBuffers   = kernel32.FlushFileBuffers


def create_pipe(pipe_name: str):
    """Create a named pipe and return its handle."""
    h = CreateNamedPipeW(
        pipe_name,
        PIPE_ACCESS_DUPLEX,
        PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_WAIT,
        PIPE_UNLIMITED_INSTANCES,
        BUFFER_SIZE,
        BUFFER_SIZE,
        0,
        None,
    )
    if h == INVALID_HANDLE_VALUE:
        raise OSError(f"CreateNamedPipe failed: {ctypes.get_last_error()}")
    return h


def pipe_read_line(h) -> str:
    """Read one line (up to \\n) from the pipe."""
    chars = []
    buf = ctypes.create_string_buffer(1)
    n_read = wt.DWORD(0)
    while True:
        ok = ReadFile(h, buf, 1, ctypes.byref(n_read), None)
        if not ok or n_read.value == 0:
            break
        c = buf.raw[:1]
        if c == b"\n":
            break
        chars.append(c.decode("ascii", errors="replace"))
    return "".join(chars).strip()


def pipe_write(h, text: str):
    """Write a string to the pipe, appending \\n."""
    data = (text + "\n").encode("ascii")
    written = wt.DWORD(0)
    WriteFile(h, data, len(data), ctypes.byref(written), None)
    FlushFileBuffers(h)


# -------------------------------------------------------------------------
# BLE command handlers
# -------------------------------------------------------------------------
class BleSession:
    def __init__(self):
        self.client: BleakClient | None = None
        self.loop = asyncio.new_event_loop()

    def _run(self, coro):
        return self.loop.run_until_complete(coro)

    def connect_by_name(self, name: str, timeout_ms: int) -> str:
        async def _do():
            log.info(f"Scanning for '{name}' (timeout={timeout_ms}ms)...")
            device = await BleakScanner.find_device_by_name(
                name, timeout=timeout_ms / 1000.0
            )
            if device is None:
                raise RuntimeError(f"Device '{name}' not found")
            log.info(f"Found {device.name} [{device.address}]")
            self.client = BleakClient(device.address)
            await self.client.connect()
            log.info("Connected.")
        self._run(_do())
        return "OK connected"

    def connect_by_addr(self, addr_hex: str) -> str:
        async def _do():
            # Convert hex like 'AABBCCDDEEFF' to 'AA:BB:CC:DD:EE:FF'
            addr_clean = addr_hex.replace(":", "").replace("-", "")
            addr_fmt = ":".join(
                addr_clean[i:i+2] for i in range(0, len(addr_clean), 2)
            )
            log.info(f"Connecting to {addr_fmt}...")
            self.client = BleakClient(addr_fmt)
            await self.client.connect()
            log.info("Connected.")
        self._run(_do())
        return "OK connected"

    def write(self, svc_uuid: str, chr_uuid: str,
              data_hex: str, write_type: int) -> str:
        if self.client is None:
            return "ERR not connected"
        data = bytes.fromhex(data_hex)
        response = write_type == 0

        async def _do():
            await self.client.write_gatt_char(
                chr_uuid, data, response=response
            )
        self._run(_do())
        return f"OK wrote {len(data)} bytes"

    def read(self, svc_uuid: str, chr_uuid: str, max_bytes: int) -> str:
        if self.client is None:
            return "ERR not connected"

        async def _do():
            return await self.client.read_gatt_char(chr_uuid)
        result = self._run(_do())
        hex_str = result.hex().upper()
        return f"OK data={hex_str}"

    def close(self):
        if self.client:
            try:
                self._run(self.client.disconnect())
            except Exception:
                pass
            self.client = None
        return "OK closed"


# -------------------------------------------------------------------------
# Command parser
# -------------------------------------------------------------------------
def parse_kv(parts: list[str]) -> dict[str, str]:
    """Parse key=value pairs from a command line."""
    kv = {}
    for p in parts:
        if "=" in p:
            k, v = p.split("=", 1)
            kv[k.strip()] = v.strip()
    return kv


def handle_command(session: BleSession, line: str) -> str:
    """Parse and execute a command, returning a response string."""
    parts = line.split()
    if not parts:
        return "ERR empty command"

    cmd = parts[0].upper()
    kv = parse_kv(parts[1:])

    try:
        if cmd == "CONNECT":
            if "name" in kv:
                timeout = int(kv.get("timeout", "5000"))
                return session.connect_by_name(kv["name"], timeout)
            elif "addr" in kv:
                return session.connect_by_addr(kv["addr"])
            else:
                return "ERR CONNECT requires name= or addr="

        elif cmd == "WRITE":
            svc  = kv.get("svc", "")
            chr_ = kv.get("chr", "")
            wt_  = int(kv.get("type", "0"))
            data = kv.get("data", "")
            if not svc or not chr_:
                return "ERR WRITE requires svc= and chr="
            return session.write(svc, chr_, data, wt_)

        elif cmd == "READ":
            svc  = kv.get("svc", "")
            chr_ = kv.get("chr", "")
            maxb = int(kv.get("max", "512"))
            if not svc or not chr_:
                return "ERR READ requires svc= and chr="
            return session.read(svc, chr_, maxb)

        elif cmd == "CLOSE":
            return session.close()

        else:
            return f"ERR unknown command: {cmd}"

    except Exception as e:
        log.exception(f"Command error: {cmd}")
        return f"ERR {e}"


# -------------------------------------------------------------------------
# Main server loop
# -------------------------------------------------------------------------
def serve(pipe_name: str):
    """Run the named-pipe server, handling one client at a time."""
    log.info(f"BLE Bridge starting on {pipe_name}")

    while True:
        log.info("Creating pipe, waiting for client...")
        h = create_pipe(pipe_name)
        connected = ConnectNamedPipe(h, None)
        # ConnectNamedPipe returns 0 if client already connected (race)
        # GetLastError == 535 (ERROR_PIPE_CONNECTED) is also OK

        log.info("Client connected.")
        session = BleSession()

        try:
            while True:
                line = pipe_read_line(h)
                if not line:
                    log.info("Client disconnected (empty read).")
                    break
                log.debug(f"<< {line}")
                resp = handle_command(session, line)
                log.debug(f">> {resp}")
                pipe_write(h, resp)

                if line.strip().upper() == "CLOSE":
                    break

        except Exception as e:
            log.exception("Session error")

        finally:
            session.close()
            DisconnectNamedPipe(h)
            CloseHandle(h)
            log.info("Session ended.\n")


def main():
    parser = argparse.ArgumentParser(description="BLE Bridge for LuaJIT")
    parser.add_argument(
        "--pipe", default=r"\\.\pipe\ble_bridge",
        help="Named pipe path (default: \\\\.\\pipe\\ble_bridge)"
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    try:
        serve(args.pipe)
    except KeyboardInterrupt:
        log.info("Shutting down.")


if __name__ == "__main__":
    main()