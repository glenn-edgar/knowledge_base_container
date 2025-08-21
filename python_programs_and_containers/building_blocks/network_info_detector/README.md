# NetworkInfoDetector

A comprehensive Python class for detecting network information including public IP addresses, local IP addresses, and subnet masks across different platforms.

## Features

- **Multiple Public IP Detection Methods**: Uses multiple services to ensure reliable public IP detection
- **Local IP Detection**: Identifies the local IP address of the primary network interface
- **Subnet Mask Detection**: Automatically detects subnet masks on Linux, macOS, and Windows
- **Cross-Platform Support**: Works on Linux, macOS, and Windows systems
- **Network Interface Enumeration**: Lists all available network interfaces
- **Robust Error Handling**: Graceful fallbacks when methods fail
- **Clean API**: Easy-to-use class interface with descriptive method names

## Installation

### Prerequisites

```bash
pip install requests
```

The class also uses standard library modules that come with Python:
- `socket`
- `urllib.request`
- `json`
- `ipaddress`
- `subprocess`
- `platform`
- `re`

## Usage

### Basic Usage

```python
from network_info_detector import NetworkInfoDetector

# Create an instance
detector = NetworkInfoDetector()

# Display all network information
detector.display_network_info()
```

### Advanced Usage

```python
from network_info_detector import NetworkInfoDetector

# Create instance with custom timeout
detector = NetworkInfoDetector(timeout=10)

# Get individual pieces of information
public_ip = detector.get_public_ip_via_ipify_api()
local_ip = detector.get_local_ip_via_socket_connection()
netmask = detector.get_netmask_for_local_ip()

print(f"Public IP: {public_ip}")
print(f"Local IP: {local_ip}")
print(f"Netmask: {netmask}")

# Get all network information as a dictionary
network_info = detector.get_all_network_info()
```

### Using Individual Methods

```python
detector = NetworkInfoDetector()

# Try different public IP detection methods
ip1 = detector.get_public_ip_via_ipify_api()      # Uses api.ipify.org
ip2 = detector.get_public_ip_via_ident_service()  # Uses ident.me

# Get local network information
local_ip = detector.get_local_ip_via_socket_connection()
netmask = detector.get_netmask_for_local_ip()

# Get network interfaces
interfaces = detector.get_network_interfaces_info()
```

## Class Methods

### Public IP Detection

| Method | Description | Service Used |
|--------|-------------|--------------|
| `get_public_ip_via_ipify_api()` | Retrieves public IP using ipify API | api.ipify.org |
| `get_public_ip_via_ident_service()` | Retrieves public IP using ident.me | ident.me |

### Local Network Detection

| Method | Description |
|--------|-------------|
| `get_local_ip_via_socket_connection()` | Gets local IP by creating a socket connection |
| `get_netmask_for_local_ip()` | Detects subnet mask for the local IP |
| `get_network_interfaces_info()` | Lists all network interfaces with their IPs |

### Utility Methods

| Method | Description |
|--------|-------------|
| `get_all_network_info()` | Returns dictionary with all network information |
| `display_network_info()` | Prints formatted network information to console |

## Example Output

```
============================================================
NETWORK INFORMATION DETECTOR
============================================================

📡 PUBLIC IP ADDRESSES:
----------------------------------------
  Method 1 (ipify API):     203.0.113.42
  Method 2 (ident.me):      203.0.113.42
  ✓ Confirmed Public IP:    203.0.113.42

🏠 LOCAL NETWORK INFORMATION:
----------------------------------------
  Local IP Address:         192.168.1.100
  Subnet Mask:              255.255.255.0

🔌 NETWORK INTERFACES:
----------------------------------------
  eth0: 192.168.1.100
  wlan0: 192.168.1.101

============================================================
```

## Platform-Specific Netmask Detection

The class uses different methods to detect netmasks based on the operating system:

### Linux
- Primary: `ip addr show` command
- Fallback: `ifconfig` command

### macOS
- Uses `ifconfig` with hexadecimal netmask conversion

### Windows
- Uses `ipconfig /all` command

### Fallback Method
If platform-specific commands fail, the class estimates the netmask based on IP address class:
- Class A (10.x.x.x): 255.0.0.0
- Class B (172.16-31.x.x): 255.255.0.0
- Class C (192.168.x.x): 255.255.255.0

## Error Handling

The class handles various error scenarios:

- **Network timeouts**: Configurable timeout for all network operations
- **Service unavailability**: Falls back to alternative methods
- **Command failures**: Uses fallback detection methods
- **Invalid IP addresses**: Returns descriptive error messages

## Requirements

- Python 3.6+
- `requests` library for HTTP requests
- Administrative privileges may be required on some systems for netmask detection

## Common Use Cases

### Network Diagnostics Tool
```python
detector = NetworkInfoDetector()
info = detector.get_all_network_info()

# Log network configuration
with open('network_log.txt', 'w') as f:
    f.write(f"Public IP: {info['public_ip']['confirmed']}\n")
    f.write(f"Local IP: {info['local_ip']}\n")
    f.write(f"Netmask: {info['netmask']}\n")
```

### VPN Connection Checker
```python
detector = NetworkInfoDetector()

# Check before VPN
public_ip_before = detector.get_public_ip_via_ipify_api()

# ... connect to VPN ...

# Check after VPN
public_ip_after = detector.get_public_ip_via_ipify_api()

if public_ip_before != public_ip_after:
    print("VPN is active!")
```

### Network Configuration Validator
```python
detector = NetworkInfoDetector()
local_ip = detector.get_local_ip_via_socket_connection()
netmask = detector.get_netmask_for_local_ip()

# Validate network configuration
import ipaddress
network = ipaddress.IPv4Network(f"{local_ip}/{netmask}", strict=False)
print(f"Network: {network}")
print(f"Broadcast: {network.broadcast_address}")
print(f"Number of hosts: {network.num_addresses}")
```

## Limitations

- Public IP detection requires internet connectivity
- Netmask detection may require system command availability
- Some methods may not work in containerized environments
- VPN or proxy connections may affect IP detection results

## Contributing

Feel free to submit issues, fork the repository, and create pull requests for any improvements.

## License

This project is provided as-is for educational and commercial use.

## Author

Generated from original code by gedgar

## Changelog

### Version 1.0.0
- Initial release with comprehensive network detection capabilities
- Cross-platform netmask detection
- Multiple public IP detection methods
- Network interface enumeration
- Formatted output display

