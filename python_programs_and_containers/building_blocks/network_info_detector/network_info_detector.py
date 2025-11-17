import requests
import socket
import urllib.request
import json
import ipaddress
import subprocess
import platform
import re


class NetworkInfoDetector:
    """
    A class to detect network information including public IP, local IP, and netmask.
    Provides multiple methods for IP detection to ensure reliability.
    """
    
    def __init__(self, timeout=5):
        """
        Initialize the NetworkInfoDetector with configurable timeout.
        
        Args:
            timeout (int): Timeout in seconds for network requests
        """
        self.timeout = timeout
        self.public_ip = None
        self.local_ip = None
        self.netmask = None
        
    def get_public_ip_via_ipify_api(self):
        """
        Retrieve public IP address using the ipify.org API service.
        
        Returns:
            str: Public IP address or error message
        """
        try:
            response = requests.get('https://api.ipify.org?format=json', timeout=self.timeout)
            response.raise_for_status()
            ip_data = response.json()
            self.public_ip = ip_data['ip']
            return self.public_ip
        except requests.RequestException as e:
            return f"ipify API Error: {e}"
    
    def get_public_ip_via_ident_service(self):
        """
        Retrieve public IP address using the ident.me service.
        
        Returns:
            str: Public IP address or error message
        """
        try:
            with urllib.request.urlopen('https://ident.me', timeout=self.timeout) as response:
                self.public_ip = response.read().decode('utf-8').strip()
                return self.public_ip
        except urllib.error.URLError as e:
            return f"ident.me Service Error: {e}"
    
    def get_local_ip_via_socket_connection(self):
        """
        Retrieve local IP address by creating a socket connection.
        This gets the IP address of the interface used for external connections.
        
        Returns:
            str: Local IP address or error message
        """
        try:
            # Create a UDP socket (doesn't actually send data)
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.settimeout(self.timeout)
            # Connect to Google's DNS (doesn't actually establish connection for UDP)
            s.connect(('8.8.8.8', 80))
            self.local_ip = s.getsockname()[0]
            s.close()
            return self.local_ip
        except socket.error as e:
            return f"Socket Connection Error: {e}"
    
    def get_network_interfaces_info(self):
        """
        Get detailed information about all network interfaces including IP and netmask.
        
        Returns:
            dict: Dictionary containing interface information
        """
        interfaces = {}
        
        try:
            # Get all network interfaces
            for interface_name in socket.if_nameindex():
                interface = interface_name[1]
                try:
                    # Try to get IPv4 info for this interface
                    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    ip_info = socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET)
                    
                    for info in ip_info:
                        ip_addr = info[4][0]
                        if not ip_addr.startswith('127.'):  # Skip loopback
                            interfaces[interface] = {'ip': ip_addr}
                            
                except Exception:
                    pass
                    
        except Exception as e:
            interfaces['error'] = str(e)
            
        return interfaces
    
    def get_netmask_for_local_ip(self):
        """
        Determine the netmask for the local IP address.
        Uses platform-specific commands to get accurate netmask information.
        
        Returns:
            str: Netmask or error message
        """
        if not self.local_ip:
            self.get_local_ip_via_socket_connection()
            
        system = platform.system()
        
        try:
            if system == "Linux":
                return self._get_netmask_linux()
            elif system == "Darwin":  # macOS
                return self._get_netmask_macos()
            elif system == "Windows":
                return self._get_netmask_windows()
            else:
                return self._estimate_netmask_from_ip()
                
        except Exception as e:
            return f"Netmask Detection Error: {e}"
    
    def _get_netmask_linux(self):
        """Get netmask on Linux systems using ip command."""
        try:
            result = subprocess.run(['ip', 'addr', 'show'], 
                                    capture_output=True, text=True, timeout=self.timeout)
            
            # Parse output to find the interface with our local IP
            lines = result.stdout.split('\n')
            for i, line in enumerate(lines):
                if self.local_ip in line and '/' in line:
                    # Extract CIDR notation
                    match = re.search(rf'{re.escape(self.local_ip)}/(\d+)', line)
                    if match:
                        cidr = int(match.group(1))
                        # Convert CIDR to netmask
                        self.netmask = str(ipaddress.IPv4Network(f'0.0.0.0/{cidr}').netmask)
                        return self.netmask
                        
        except (subprocess.SubprocessError, FileNotFoundError):
            pass
            
        # Fallback to ifconfig if ip command not available
        try:
            result = subprocess.run(['ifconfig'], 
                                    capture_output=True, text=True, timeout=self.timeout)
            
            lines = result.stdout.split('\n')
            for line in lines:
                if 'netmask' in line.lower() and self.local_ip in result.stdout:
                    match = re.search(r'netmask\s+(\d+\.\d+\.\d+\.\d+)', line, re.IGNORECASE)
                    if match:
                        self.netmask = match.group(1)
                        return self.netmask
                        
        except (subprocess.SubprocessError, FileNotFoundError):
            pass
            
        return self._estimate_netmask_from_ip()
    
    def _get_netmask_macos(self):
        """Get netmask on macOS systems using ifconfig."""
        try:
            result = subprocess.run(['ifconfig'], 
                                    capture_output=True, text=True, timeout=self.timeout)
            
            # Parse ifconfig output
            lines = result.stdout.split('\n')
            found_interface = False
            
            for line in lines:
                if self.local_ip in line:
                    found_interface = True
                if found_interface and 'netmask' in line.lower():
                    # Extract netmask (on macOS it's in hex format)
                    match = re.search(r'netmask\s+(0x[0-9a-fA-F]+)', line)
                    if match:
                        hex_mask = match.group(1)
                        # Convert hex to dotted decimal
                        hex_value = int(hex_mask, 16)
                        self.netmask = str(ipaddress.IPv4Address(hex_value))
                        return self.netmask
                        
        except (subprocess.SubprocessError, FileNotFoundError):
            pass
            
        return self._estimate_netmask_from_ip()
    
    def _get_netmask_windows(self):
        """Get netmask on Windows systems using ipconfig."""
        try:
            result = subprocess.run(['ipconfig', '/all'], 
                                    capture_output=True, text=True, timeout=self.timeout)
            
            lines = result.stdout.split('\n')
            found_interface = False
            
            for line in lines:
                if self.local_ip in line:
                    found_interface = True
                if found_interface and 'subnet mask' in line.lower():
                    match = re.search(r':\s*(\d+\.\d+\.\d+\.\d+)', line)
                    if match:
                        self.netmask = match.group(1)
                        return self.netmask
                        
        except (subprocess.SubprocessError, FileNotFoundError):
            pass
            
        return self._estimate_netmask_from_ip()
    
    def _estimate_netmask_from_ip(self):
        """
        Estimate netmask based on IP address class (fallback method).
        
        Returns:
            str: Estimated netmask based on IP class
        """
        if not self.local_ip:
            return "Unable to determine netmask"
            
        try:
            ip = ipaddress.IPv4Address(self.local_ip)
            
            # Check for private IP ranges
            if ip.is_private:
                first_octet = int(self.local_ip.split('.')[0])
                second_octet = int(self.local_ip.split('.')[1])
                
                # Common private network ranges
                if first_octet == 10:
                    self.netmask = "255.0.0.0"  # Class A private
                elif first_octet == 172 and 16 <= second_octet <= 31:
                    self.netmask = "255.255.0.0"  # Class B private
                elif first_octet == 192 and second_octet == 168:
                    self.netmask = "255.255.255.0"  # Class C private
                else:
                    self.netmask = "255.255.255.0"  # Default for private
            else:
                # For public IPs, we can't determine the actual netmask
                self.netmask = "Unable to determine (public IP)"
                
            return self.netmask
            
        except ValueError:
            return "Invalid IP address"
    
    def get_all_network_info(self):
        """
        Retrieve all available network information using multiple methods.
        
        Returns:
            dict: Dictionary containing all network information
        """
        info = {
            'public_ip': {
                'ipify_api': self.get_public_ip_via_ipify_api(),
                'ident_service': self.get_public_ip_via_ident_service()
            },
            'local_ip': self.get_local_ip_via_socket_connection(),
            'netmask': self.get_netmask_for_local_ip(),
            'network_interfaces': self.get_network_interfaces_info()
        }
        
        # Set the most reliable public IP
        if self.public_ip:
            info['public_ip']['confirmed'] = self.public_ip
            
        return info
    
    def display_network_info(self):
        """Display all network information in a formatted manner."""
        print("=" * 60)
        print("NETWORK INFORMATION DETECTOR")
        print("=" * 60)
        
        info = self.get_all_network_info()
        
        print("\n📡 PUBLIC IP ADDRESSES:")
        print("-" * 40)
        print(f"  Method 1 (ipify API):     {info['public_ip']['ipify_api']}")
        print(f"  Method 2 (ident.me):      {info['public_ip']['ident_service']}")
        if 'confirmed' in info['public_ip']:
            print(f"  ✓ Confirmed Public IP:    {info['public_ip']['confirmed']}")
        
        print("\n🏠 LOCAL NETWORK INFORMATION:")
        print("-" * 40)
        print(f"  Local IP Address:         {info['local_ip']}")
        print(f"  Subnet Mask:              {info['netmask']}")
        
        if info['network_interfaces'] and 'error' not in info['network_interfaces']:
            print("\n🔌 NETWORK INTERFACES:")
            print("-" * 40)
            for interface, details in info['network_interfaces'].items():
                if isinstance(details, dict) and 'ip' in details:
                    print(f"  {interface}: {details['ip']}")
        
        print("\n" + "=" * 60)


def main():
    """Main function to demonstrate the NetworkInfoDetector class."""
    # Create an instance of the detector
    detector = NetworkInfoDetector(timeout=5)
    
    # Display all network information
    detector.display_network_info()
    
    # You can also access individual pieces of information
    print("\n📊 INDIVIDUAL ACCESS EXAMPLE:")
    print("-" * 40)
    print(f"Just the local IP: {detector.local_ip}")
    print(f"Just the netmask:  {detector.netmask}")
    print(f"Just the public IP: {detector.public_ip}")


if __name__ == "__main__":
    main()

