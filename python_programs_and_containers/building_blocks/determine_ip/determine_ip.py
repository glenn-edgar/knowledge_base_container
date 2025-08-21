import requests
import socket
import urllib.request
import json

def method1_requests():
    """Use requests library to get public IP"""
    try:
        response = requests.get('https://api.ipify.org?format=json', timeout=5)
        response.raise_for_status()
        ip_data = response.json()
        return ip_data['ip']
    except requests.RequestException as e:
        return f"Method 1 Error: {e}"

def method2_urllib():
    """Use urllib to get public IP"""
    try:
        with urllib.request.urlopen('https://ident.me', timeout=5) as response:
            return response.read().decode('utf-8')
    except urllib.error.URLError as e:
        return f"Method 2 Error: {e}"

def method3_socket():
    """Get local IP (not always public) using socket"""
    try:
        # Create a socket connection to an external server
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(5)
        # Doesn't actually connect, just sets up for getting local IP
        s.connect(('8.8.8.8', 80))
        local_ip = s.getsockname()[0]
        s.close()
        return local_ip
    except socket.error as e:
        return f"Method 3 Error: {e}"

def main():
    print("Attempting to get public IP address...\n")
    
    # Try Method 1: Using ipify API
    print("Method 1 (ipify API):")
    ip1 = method1_requests()
    print(f"IP: {ip1}\n")
    
    # Try Method 2: Using ident.me
    print("Method 2 (ident.me):")
    ip2 = method2_urllib()
    print(f"IP: {ip2}\n")
    
    # Try Method 3: Using socket (local IP, may differ from public)
    print("Method 3 (socket - may show local IP):")
    ip3 = method3_socket()
    print(f"IP: {ip3}\n")

if __name__ == "__main__":
    main()
