#!/usr/bin/env python3
import argparse
import ipaddress
import json
import socket
import struct
import time
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple, Optional, Dict, Any

class BaseBrokerScanner(ABC):
    """Base class for broker scanners."""
    
    def __init__(self, port: Optional[int] = None, timeout: float = 1.0, 
                 workers: int = 256, debug: bool = False):
        """
        Initialize the base scanner.
        
        Args:
            port: TCP port to scan
            timeout: Per-connection timeout in seconds
            workers: Maximum concurrent connections
            debug: Enable debug output
        """
        self.port = port if port is not None else self.default_port
        self.timeout = timeout
        self.workers = workers
        self.debug = debug
        self.subnet = self._detect_local_subnet()
    
    @property
    @abstractmethod
    def default_port(self) -> int:
        """Default port for this broker type."""
        pass
    
    @property
    @abstractmethod
    def broker_name(self) -> str:
        """Name of the broker type."""
        pass
    
    def _get_local_ipv4(self) -> str:
        """Best-effort way to get the primary local IPv4."""
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
        except Exception:
            return socket.gethostbyname(socket.gethostname())
        finally:
            s.close()
    
    def _detect_local_subnet(self) -> ipaddress.IPv4Network:
        """Automatically detect the local subnet as /24."""
        ip = self._get_local_ipv4()
        try:
            net = ipaddress.ip_network(f"{ip}/24", strict=False)
        except ValueError:
            net = ipaddress.ip_network("192.168.1.0/24", strict=False)
        return net
    
    @abstractmethod
    def detect_broker(self, ip: str) -> Optional[Any]:
        """Detect if a broker is running at the given IP."""
        pass
    
    @abstractmethod
    def print_results(self, results: List[Any]) -> None:
        """Print scan results in a formatted way."""
        pass
    
    def scan(self) -> List[Any]:
        """Scan the local subnet for brokers."""
        results = []
        targets = [str(h) for h in self.subnet.hosts()]
        
        print(f"[i] Scanning {len(targets)} hosts for {self.broker_name} brokers...")

        with ThreadPoolExecutor(max_workers=self.workers) as ex:
            fut_map = {ex.submit(self.detect_broker, ip): ip for ip in targets}
            completed = 0
            for fut in as_completed(fut_map):
                completed += 1
                if completed % 50 == 0:
                    print(f"[i] Progress: {completed}/{len(targets)} hosts scanned")
                
                try:
                    res = fut.result()
                    if res:
                        results.append(res)
                        print(f"[+] Found {self.broker_name} broker at {res[0]}:{self.port}")
                except Exception as e:
                    if self.debug:
                        print(f"[DEBUG] Error processing result: {e}")

        return results
    
    def get_scan_info(self) -> str:
        """Return scan configuration information."""
        return (f"Broker: {self.broker_name} | Port: {self.port} | "
                f"Subnet: {self.subnet} | Timeout: {self.timeout}s | "
                f"Workers: {self.workers}")


class NATSScanner(BaseBrokerScanner):
    """Scanner specifically for NATS brokers."""
    
    @property
    def default_port(self) -> int:
        return 4222
    
    @property
    def broker_name(self) -> str:
        return "NATS"
    
    def detect_broker(self, ip: str) -> Optional[Tuple[str, Dict]]:
        """
        Try to connect to ip:port and read the NATS INFO banner.
        NATS immediately sends an INFO line upon connection.
        """
        sock = None
        try:
            # Create socket with timeout
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(self.timeout)
            
            if self.debug:
                print(f"[DEBUG] Connecting to {ip}:{self.port}...")
            
            # Connect to the server
            sock.connect((ip, self.port))
            
            if self.debug:
                print(f"[DEBUG] Connected to {ip}:{self.port}, waiting for NATS INFO...")
            
            # NATS immediately sends INFO on connect, but might need a moment
            # Try multiple reads in case data comes in chunks
            data = b""
            attempts = 0
            max_attempts = 3
            
            while attempts < max_attempts:
                try:
                    sock.settimeout(0.3)  # Short timeout for each read
                    chunk = sock.recv(4096)
                    if chunk:
                        data += chunk
                        if self.debug:
                            print(f"[DEBUG] Received chunk from {ip}: {chunk[:100]}...")
                        # If we see INFO, we likely have enough
                        if b"INFO " in data and b"\r\n" in data:
                            break
                    else:
                        break  # Connection closed
                except socket.timeout:
                    attempts += 1
                    if data:  # If we have some data, might be enough
                        break
                    if attempts < max_attempts:
                        time.sleep(0.1)  # Brief pause before retry
            
            if self.debug and data:
                print(f"[DEBUG] Total data from {ip}: {data[:200]}...")
            
            if not data:
                if self.debug:
                    print(f"[DEBUG] No data received from {ip}")
                return None
            
            # Check if this looks like NATS
            if data.startswith(b"INFO "):
                try:
                    # Extract JSON from INFO line
                    # NATS format: INFO {json}\r\n
                    info_line = data.decode('utf-8', 'replace')
                    
                    # Find the JSON part (between INFO and \r\n)
                    if info_line.startswith("INFO "):
                        json_start = info_line.find('{')
                        json_end = info_line.find('}')
                        
                        if json_start != -1 and json_end != -1 and json_end > json_start:
                            json_str = info_line[json_start:json_end+1]
                            info = json.loads(json_str)
                            if self.debug:
                                print(f"[DEBUG] NATS detected at {ip}: v{info.get('version', 'unknown')}")
                            return (ip, info)
                        else:
                            if self.debug:
                                print(f"[DEBUG] INFO found but no valid JSON at {ip}")
                            return (ip, {"version": "unknown", "raw": info_line[:100]})
                            
                except json.JSONDecodeError as e:
                    if self.debug:
                        print(f"[DEBUG] JSON decode error for {ip}: {e}")
                    return (ip, {"version": "unknown", "parse_error": str(e)})
                except Exception as e:
                    if self.debug:
                        print(f"[DEBUG] Error parsing NATS response from {ip}: {e}")
                    return (ip, {"version": "unknown", "error": str(e)})
            
            if self.debug:
                print(f"[DEBUG] Not NATS - no INFO banner from {ip}")
                        
        except socket.timeout:
            if self.debug:
                print(f"[DEBUG] Timeout connecting to {ip}:{self.port}")
        except ConnectionRefusedError:
            if self.debug:
                print(f"[DEBUG] Connection refused by {ip}:{self.port}")
        except OSError as e:
            if e.errno == 113:  # No route to host
                pass
            elif self.debug:
                print(f"[DEBUG] OS error connecting to {ip}: {e}")
        except Exception as e:
            if self.debug:
                print(f"[DEBUG] Exception connecting to {ip}: {e}")
        finally:
            if sock:
                try:
                    sock.close()
                except:
                    pass
        
        return None
    
    def print_results(self, results: List[Tuple[str, Dict]]) -> None:
        """Print NATS scan results."""
        if not results:
            print("\nNo NATS brokers detected.")
            print("\nTroubleshooting tips:")
            print("  1. Check if NATS is running: ps aux | grep nats")
            print("  2. Check if port is open: netstat -tlnp | grep :4222")
            print("  3. Try manual connection: telnet localhost 4222")
            print("  4. NATS should immediately send 'INFO {...}' on connect")
            print("  5. Try with --debug flag for more details")
            return

        print(f"\n{'='*50}")
        print(f"Detected {len(results)} NATS broker(s):")
        print(f"{'='*50}")
        
        for ip, info in sorted(results, key=lambda x: x[0]):
            ver = info.get("version", "?")
            name = info.get("server_name", info.get("name", "unnamed"))
            server_id = info.get("server_id", "unknown")
            if len(server_id) > 8:
                server_id = server_id[:8] + "..."
            print(f"  {ip}:{self.port} - NATS v{ver} ({name}) ID:{server_id}")
            if self.debug and "parse_error" in info:
                print(f"    Parse error: {info['parse_error']}")


class MQTTScanner(BaseBrokerScanner):
    """Scanner specifically for MQTT brokers."""
    
    @property
    def default_port(self) -> int:
        return 1883
    
    @property
    def broker_name(self) -> str:
        return "MQTT"
    
    def _encode_remaining_length(self, length: int) -> bytes:
        """Encode MQTT remaining length field."""
        result = bytearray()
        while True:
            byte = length % 128
            length //= 128
            if length > 0:
                byte |= 0x80
            result.append(byte)
            if length == 0:
                break
        return bytes(result)
    
    def _decode_remaining_length(self, data: bytes, start_idx: int = 1) -> Tuple[int, int]:
        """
        Decode MQTT remaining length field.
        Returns (remaining_length, bytes_consumed).
        """
        multiplier = 1
        value = 0
        index = start_idx
        
        while index < len(data):
            byte = data[index]
            value += (byte & 127) * multiplier
            if (byte & 128) == 0:
                return value, index - start_idx + 1
            multiplier *= 128
            index += 1
            if multiplier > 128 * 128 * 128:
                raise ValueError("Invalid remaining length")
        
        raise ValueError("Incomplete remaining length")
    
    def _build_connect_packet(self, client_id: str = "scanner", 
                             protocol_version: int = 4) -> bytes:
        """
        Build MQTT CONNECT packet for different protocol versions.
        """
        # Protocol name and level
        if protocol_version == 3:
            proto_name = b"MQIsdp"
            proto_level = 3
        elif protocol_version == 4:
            proto_name = b"MQTT"
            proto_level = 4
        elif protocol_version == 5:
            proto_name = b"MQTT"
            proto_level = 5
        else:
            raise ValueError(f"Unknown protocol version: {protocol_version}")
        
        # Connect flags: Clean session, no will, no username/password
        connect_flags = 0b00000010
        keepalive = 60
        
        # Build variable header
        variable_header = bytearray()
        # Protocol name
        variable_header.extend(struct.pack("!H", len(proto_name)))
        variable_header.extend(proto_name)
        # Protocol level
        variable_header.append(proto_level)
        # Connect flags
        variable_header.append(connect_flags)
        # Keep alive
        variable_header.extend(struct.pack("!H", keepalive))
        
        # MQTT 5.0 properties
        if protocol_version == 5:
            variable_header.append(0)  # No properties
        
        # Build payload (just client ID)
        payload = bytearray()
        cid_bytes = client_id.encode('utf-8')
        payload.extend(struct.pack("!H", len(cid_bytes)))
        payload.extend(cid_bytes)
        
        # Build fixed header
        packet_type = 0x10  # CONNECT
        remaining_length = len(variable_header) + len(payload)
        
        # Combine everything
        packet = bytearray([packet_type])
        packet.extend(self._encode_remaining_length(remaining_length))
        packet.extend(variable_header)
        packet.extend(payload)
        
        return bytes(packet)
    
    def _parse_connack(self, data: bytes) -> Optional[int]:
        """Parse CONNACK packet and return the return code."""
        if len(data) < 4:
            return None
        
        # Check packet type
        if (data[0] & 0xF0) != 0x20:  # CONNACK
            return None
        
        try:
            remaining_len, bytes_consumed = self._decode_remaining_length(data, 1)
            header_len = 1 + bytes_consumed
            
            if len(data) < header_len + remaining_len:
                return None
            
            # CONNACK variable header
            # Byte 1: Connect Acknowledge Flags
            # Byte 2: Connect Return code (what we want)
            return data[header_len + 1]
            
        except Exception as e:
            if self.debug:
                print(f"[DEBUG] Error parsing CONNACK: {e}")
            return None
    
    def _try_mqtt_connection(self, ip: str, protocol_version: int) -> Optional[int]:
        """Try to connect with a specific MQTT protocol version."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(self.timeout)
            sock.connect((ip, self.port))
            
            try:
                # Send CONNECT packet
                connect_packet = self._build_connect_packet(
                    client_id=f"scan_{int(time.time()) % 10000}",
                    protocol_version=protocol_version
                )
                sock.sendall(connect_packet)
                
                if self.debug:
                    print(f"[DEBUG] Sent MQTT v{protocol_version} CONNECT to {ip}:{self.port}")
                    print(f"[DEBUG] Packet: {connect_packet.hex()}")
                
                # Wait a bit for broker to process
                time.sleep(0.1)
                
                # Read response (with timeout)
                sock.settimeout(0.5)
                data = b''
                attempts = 0
                
                while attempts < 3:
                    try:
                        chunk = sock.recv(1024)
                        if chunk:
                            data += chunk
                            if self.debug:
                                print(f"[DEBUG] Received chunk from {ip}: {chunk.hex()}")
                            # Check if we have a CONNACK
                            if len(data) >= 4 and (data[0] & 0xF0) == 0x20:
                                break
                        else:
                            break
                    except socket.timeout:
                        attempts += 1
                        if data:  # We have some data, try to parse it
                            break
                        time.sleep(0.1)
                
                if self.debug and data:
                    print(f"[DEBUG] Total received from {ip}: {data.hex()}")
                
                if data:
                    # Try to parse CONNACK
                    return_code = self._parse_connack(data)
                    if return_code is not None:
                        return return_code
                    
                    # If we got something that looks like MQTT packet
                    if data[0] in [0x20, 0x30, 0x40, 0x50]:
                        return 255  # Unknown but likely MQTT
                
            finally:
                sock.close()
                
        except ConnectionRefusedError:
            if self.debug:
                print(f"[DEBUG] Connection refused by {ip}:{self.port}")
        except socket.timeout:
            if self.debug:
                print(f"[DEBUG] Timeout with {ip}:{self.port} (proto {protocol_version})")
        except Exception as e:
            if self.debug:
                print(f"[DEBUG] Error with {ip}:{self.port} (proto {protocol_version}): {e}")
        
        return None
    
    def detect_broker(self, ip: str) -> Optional[Tuple[str, int]]:
        """
        Try to detect MQTT broker by sending CONNECT and looking for CONNACK.
        Tries multiple protocol versions for compatibility.
        """
        # Try different protocol versions in order of popularity
        for proto_ver in [4, 3, 5]:  # MQTT 3.1.1, 3.1, 5.0
            return_code = self._try_mqtt_connection(ip, proto_ver)
            if return_code is not None:
                if self.debug:
                    print(f"[DEBUG] MQTT detected at {ip} (proto {proto_ver}, code {return_code})")
                return (ip, return_code)
        
        return None
    
    def print_results(self, results: List[Tuple[str, int]]) -> None:
        """Print MQTT scan results."""
        if not results:
            print("\nNo MQTT brokers detected.")
            print("\nTroubleshooting tips:")
            print("  1. Check if broker is running: ps aux | grep mosquitto")
            print("  2. Check if port is open: netstat -tlnp | grep :1883")
            print("  3. Try manual test: mosquitto_pub -h localhost -t test -m hello")
            print("  4. Check firewall: sudo iptables -L | grep 1883")
            print("  5. Try with --debug flag for detailed output")
            print("  6. Try increasing --timeout to 2.0 or 3.0")
            print("  7. Try different port: --port 8883 (for TLS)")
            return

        print(f"\n{'='*50}")
        print(f"Detected {len(results)} MQTT broker(s):")
        print(f"{'='*50}")
        
        return_code_meanings = {
            0: "✓ Connection Accepted",
            1: "Unacceptable Protocol Version",
            2: "Identifier Rejected",
            3: "Server Unavailable",
            4: "Bad Username/Password",
            5: "Not Authorized",
            254: "Detected (Unexpected Response)",
            255: "Detected (Unknown Format)"
        }
        
        for ip, return_code in sorted(results, key=lambda x: x[0]):
            status = return_code_meanings.get(return_code, f"Return Code: {return_code}")
            print(f"  {ip}:{self.port} - MQTT [{status}]")


def main():
    parser = argparse.ArgumentParser(
        description="Scan local network for NATS or MQTT message brokers.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --mode nats                    # Scan for NATS brokers (port 4222)
  %(prog)s --mode mqtt                    # Scan for MQTT brokers (port 1883)
  %(prog)s --mode mqtt --port 8883        # Scan for MQTT on custom port
  %(prog)s --mode mqtt --debug            # Enable debug output
  %(prog)s --mode mqtt --timeout 2.0      # Increase timeout for slow networks
  
Common MQTT Ports:
  1883 - Standard MQTT
  8883 - MQTT over TLS/SSL
  8080 - MQTT over WebSockets
  8884 - MQTT over WebSockets with TLS/SSL
  
Common NATS Ports:
  4222 - Client connections
  6222 - Routing port for clustering
  8222 - HTTP management port
        """
    )
    
    parser.add_argument(
        "--mode", 
        choices=["nats", "mqtt"], 
        default="mqtt",
        help="Broker type to scan for (default: mqtt)"
    )
    parser.add_argument(
        "--port", 
        type=int, 
        default=None,
        help="TCP port to scan (defaults: NATS=4222, MQTT=1883)"
    )
    parser.add_argument(
        "--timeout", 
        type=float, 
        default=1.0,
        help="Connection timeout in seconds (default: 1.0)"
    )
    parser.add_argument(
        "--workers", 
        type=int, 
        default=256,
        help="Maximum concurrent connections (default: 256)"
    )
    parser.add_argument(
        "--debug", 
        action="store_true",
        help="Enable debug output for troubleshooting"
    )
    
    args = parser.parse_args()
    
    try:
        # Create appropriate scanner
        if args.mode == "nats":
            scanner = NATSScanner(
                port=args.port,
                timeout=args.timeout,
                workers=args.workers,
                debug=args.debug
            )
        else:  # mqtt
            scanner = MQTTScanner(
                port=args.port,
                timeout=args.timeout,
                workers=args.workers,
                debug=args.debug
            )
        
        # Display configuration
        print(f"\n{'='*50}")
        print(f" BROKER SCANNER")
        print(f"{'='*50}")
        print(f"[i] {scanner.get_scan_info()}")
        print(f"{'='*50}\n")
        
        # Perform scan
        start_time = time.time()
        results = scanner.scan()
        elapsed = time.time() - start_time
        
        # Display results
        scanner.print_results(results)
        print(f"\n[i] Scan completed in {elapsed:.2f} seconds")
        
    except KeyboardInterrupt:
        print("\n[!] Scan interrupted by user")
    except Exception as e:
        print(f"[!] Error: {e}")
        if args.debug:
            import traceback
            traceback.print_exc()


if __name__ == "__main__":
    main()