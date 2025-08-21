# Broker Scanner

A Python-based network scanner for discovering NATS and MQTT message brokers on your local network. The scanner automatically detects your local subnet and probes each host to identify running message brokers.

## Features

- **Dual Protocol Support**: Detects both NATS and MQTT brokers
- **Automatic Subnet Detection**: Automatically identifies your local network range
- **Concurrent Scanning**: Uses threading for fast parallel scanning
- **Multiple Protocol Versions**: Supports MQTT 3.1, 3.1.1, and 5.0
- **Debug Mode**: Detailed output for troubleshooting
- **Zero Dependencies**: Uses only Python standard library

## Requirements

- Python 3.6 or higher
- Network access to scan local subnet
- Appropriate permissions to create network connections

## Installation

Simply download the script and make it executable:

```bash
wget https://raw.githubusercontent.com/yourusername/broker-scanner/main/scanner.py
chmod +x scanner.py
```

Or clone the repository:

```bash
git clone https://github.com/yourusername/broker-scanner.git
cd broker-scanner
```

## Usage

### Basic Commands

```bash
# Scan for MQTT brokers (default)
python3 scanner.py --mode mqtt

# Scan for NATS brokers
python3 scanner.py --mode nats

# Enable debug output
python3 scanner.py --mode mqtt --debug

# Scan custom port
python3 scanner.py --mode mqtt --port 8883

# Increase timeout for slow networks
python3 scanner.py --mode mqtt --timeout 2.0
```

### Command Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `--mode` | Broker type to scan for (`nats` or `mqtt`) | `mqtt` |
| `--port` | TCP port to scan | NATS: 4222, MQTT: 1883 |
| `--timeout` | Connection timeout in seconds | 1.0 |
| `--workers` | Maximum concurrent connections | 256 |
| `--debug` | Enable debug output | False |

### Examples

```bash
# Scan for MQTT brokers with debug information
python3 scanner.py --mode mqtt --debug

# Scan for NATS brokers on custom port
python3 scanner.py --mode nats --port 4223

# Scan with increased timeout for slow networks
python3 scanner.py --mode mqtt --timeout 3.0

# Scan with fewer concurrent connections
python3 scanner.py --mode mqtt --workers 50

# Scan for both NATS and MQTT brokers
python3 scanner.py --mode nats && python3 scanner.py --mode mqtt
```

## How It Works

### NATS Detection
The scanner connects to each IP on the specified port and waits for the NATS INFO message. NATS servers immediately send an INFO JSON payload upon connection, which contains server metadata like version and server ID.

### MQTT Detection
The scanner sends an MQTT CONNECT packet and waits for a CONNACK response. It tries multiple protocol versions (3.1, 3.1.1, and 5.0) to ensure compatibility with different broker implementations.

## Output

### Successful Detection

```
==================================================
 BROKER SCANNER
==================================================
[i] Mode: MQTT | Port: 1883 | Subnet: 192.168.1.0/24 | Timeout: 1.0s | Workers: 256
==================================================

[i] Scanning 254 hosts for MQTT brokers...
[+] Found MQTT broker at 192.168.1.10:1883
[+] Found MQTT broker at 192.168.1.25:1883

==================================================
Detected 2 MQTT broker(s):
==================================================
  192.168.1.10:1883 - MQTT [✓ Connection Accepted]
  192.168.1.25:1883 - MQTT [Not Authorized]
```

### Return Codes (MQTT)

| Code | Meaning |
|------|---------|
| 0 | Connection Accepted |
| 1 | Unacceptable Protocol Version |
| 2 | Identifier Rejected |
| 3 | Server Unavailable |
| 4 | Bad Username/Password |
| 5 | Not Authorized |

## Common Ports

### MQTT Ports
- **1883** - Standard MQTT
- **8883** - MQTT over TLS/SSL
- **8080** - MQTT over WebSockets
- **8884** - MQTT over WebSockets with TLS/SSL

### NATS Ports
- **4222** - Client connections
- **6222** - Routing port for clustering
- **8222** - HTTP management port

## Troubleshooting

### No Brokers Detected

1. **Verify broker is running:**
   ```bash
   # For MQTT (Mosquitto)
   ps aux | grep mosquitto
   sudo systemctl status mosquitto
   
   # For NATS
   ps aux | grep nats
   sudo systemctl status nats-server
   ```

2. **Check port is open:**
   ```bash
   # Check listening ports
   sudo netstat -tlnp | grep :1883  # MQTT
   sudo netstat -tlnp | grep :4222  # NATS
   ```

3. **Test connectivity manually:**
   ```bash
   # MQTT
   mosquitto_sub -h localhost -t test -v
   
   # NATS
   telnet localhost 4222
   ```

4. **Check firewall rules:**
   ```bash
   sudo iptables -L | grep 1883
   sudo ufw status
   ```

5. **Run with debug flag:**
   ```bash
   python3 scanner.py --mode mqtt --debug
   ```

### Timeout Issues

If scanning is timing out, try:
- Increasing timeout: `--timeout 3.0`
- Reducing workers: `--workers 50`
- Checking network connectivity

### Permission Issues

Some systems may limit the number of concurrent connections. If you see errors, try:
- Running with fewer workers: `--workers 50`
- Increasing system limits: `ulimit -n 4096`

## Architecture

The scanner uses an object-oriented design with:

- **`BaseBrokerScanner`**: Abstract base class with common scanning logic
- **`NATSScanner`**: Specialized implementation for NATS protocol
- **`MQTTScanner`**: Specialized implementation for MQTT protocol

Each scanner:
1. Detects the local subnet automatically
2. Creates a thread pool for concurrent scanning
3. Probes each host in parallel
4. Identifies brokers based on protocol-specific handshakes
5. Reports results with detailed status information

## Security Considerations

- This tool only performs read operations (banner grabbing)
- No authentication credentials are used or exposed
- Scans only the local subnet by default
- Uses minimal protocol handshakes to identify services

## Limitations

- Scans only IPv4 networks
- Limited to /24 subnet (254 hosts)
- Cannot detect brokers behind firewalls that block connections
- Does not detect brokers using non-standard protocols
- Cannot authenticate or interact with discovered brokers

## Contributing

Contributions are welcome! Please feel free to submit issues or pull requests.

## License

MIT License - See LICENSE file for details

## Disclaimer

This tool is for network administration and debugging purposes only. Only scan networks you own or have explicit permission to test. The authors are not responsible for any misuse or damage caused by this tool.

## Author

[Your Name]

## Version History

- **1.0.0** - Initial release with NATS and MQTT support
- **1.1.0** - Added support for multiple MQTT protocol versions
- **1.2.0** - Improved error handling and debug output

