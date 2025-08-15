"""
KVStoreReader - Reads retained key/value messages for a given topic pattern
from a Mosquitto MQTT broker running locally.

Requires: paho-mqtt >= 2.0
    pip install paho-mqtt
"""

import time
import threading
from typing import Optional, Dict
import paho.mqtt.client as mqtt


class KVStoreReader:
    """
    Reads retained messages from MQTT broker as key-value pairs.
    Retained messages act as a simple KV store in MQTT.
    """
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 1883,
        client_id: str = "kv-reader",
        keepalive: int = 60,
        use_mqttv5: bool = False,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ) -> None:
        self.host = host
        self.port = port
        self.keepalive = keepalive
        self.use_mqttv5 = use_mqttv5

        # Thread safety
        self._values: Dict[str, str] = {}
        self._values_lock = threading.Lock()
        self._connected = False
        self._connect_event = threading.Event()
        self._subscribe_event = threading.Event()

        # Use Callback API v2 to avoid deprecation warnings
        if use_mqttv5:
            self._client = mqtt.Client(
                callback_api_version=mqtt.CallbackAPIVersion.VERSION2,  # Fixed parameter name
                client_id=client_id,
                protocol=mqtt.MQTTv5,
                clean_start=True,  # v5 uses clean_start
            )
        else:
            self._client = mqtt.Client(
                callback_api_version=mqtt.CallbackAPIVersion.VERSION2,  # Fixed parameter name
                client_id=client_id,
                protocol=mqtt.MQTTv311,  # Explicitly set protocol
                clean_session=True,  # v3.1.1 uses clean_session
            )

        if username is not None:
            self._client.username_pw_set(username=username, password=password)

        # Assign callbacks with correct signatures for VERSION2
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message = self._on_message
        self._client.on_subscribe = self._on_subscribe

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        """
        Callback for connection (VERSION2 API).
        Parameters: (client, userdata, flags, reason_code, properties)
        """
        if self.use_mqttv5:
            # MQTT v5 uses reason codes
            if hasattr(reason_code, 'is_failure'):
                self._connected = not reason_code.is_failure
            else:
                self._connected = (reason_code == 0)
        else:
            # MQTT v3.1.1 uses numeric codes
            self._connected = (reason_code == 0)
        
        if self._connected:
            print(f"[Connected] Successfully connected to {self.host}:{self.port}")
        else:
            print(f"[Connection Failed] reason_code={reason_code}")
        
        self._connect_event.set()

    def _on_disconnect(self, client, userdata, disconnect_flags, reason_code, properties):
        """
        Callback for disconnection (VERSION2 API).
        Parameters: (client, userdata, disconnect_flags, reason_code, properties)
        Note the extra disconnect_flags parameter in VERSION2.
        """
        self._connected = False
        self._connect_event.clear()
        print(f"[Disconnected] reason_code={reason_code}")

    def _on_message(self, client, userdata, msg):
        """
        Callback for received messages.
        Stores/overwrites the latest retained value for each topic.
        """
        # Decode payload
        payload = msg.payload.decode(errors="replace")
        
        # Store with thread safety
        with self._values_lock:
            if msg.retain:
                # Only store retained messages (for KV store pattern)
                self._values[msg.topic] = payload
                print(f"[Retained] {msg.topic} => {payload[:50]}{'...' if len(payload) > 50 else ''}")
            else:
                # Optionally handle non-retained messages
                print(f"[Non-retained] {msg.topic} => {payload[:50]}{'...' if len(payload) > 50 else ''}")

    def _on_subscribe(self, client, userdata, mid, reason_codes, properties):
        """
        Callback for subscription acknowledgment (VERSION2 API).
        Parameters: (client, userdata, mid, reason_codes, properties)
        """
        print(f"[Subscribed] mid={mid}, reason_codes={reason_codes}")
        self._subscribe_event.set()

    def connect(self, timeout: float = 5.0) -> bool:
        """
        Connect to the MQTT broker.
        
        Args:
            timeout: Maximum time to wait for connection (seconds)
        
        Returns:
            True if connected successfully, False otherwise
        """
        self._connect_event.clear()
        
        try:
            # Connect to broker
            self._client.connect(self.host, self.port, keepalive=self.keepalive)
            
            # Start the network loop
            self._client.loop_start()
            
            # Wait for connection to complete
            if not self._connect_event.wait(timeout):
                print(f"[Error] Connection timeout after {timeout} seconds")
                self._client.loop_stop()
                return False
            
            if not self._connected:
                print(f"[Error] Failed to connect to {self.host}:{self.port}")
                self._client.loop_stop()
                return False
            
            return True
            
        except Exception as e:
            print(f"[Error] Connection failed: {e}")
            return False

    def disconnect(self) -> None:
        """Disconnect from the MQTT broker."""
        try:
            self._client.loop_stop()
            self._client.disconnect()
        except Exception as e:
            print(f"[Error] Disconnect failed: {e}")
        finally:
            self._connected = False
            self._connect_event.clear()

    def read_pattern(self, pattern: str, timeout: float = 2.0, qos: int = 1) -> Dict[str, str]:
        """
        Subscribe to `pattern` (may include wildcards like 'kv/system/#'),
        collect retained values for up to `timeout` seconds, and return them.
        
        Args:
            pattern: MQTT topic pattern (can include wildcards + and #)
            timeout: Time to wait for retained messages (seconds)
            qos: Quality of Service level for subscription
        
        Returns:
            Dictionary of topic -> value for all retained messages matching pattern
        """
        if not self._connected:
            raise RuntimeError("Not connected. Call connect() first.")
        
        # Clear previous values
        with self._values_lock:
            self._values.clear()
        
        # Subscribe to pattern
        self._subscribe_event.clear()
        result, mid = self._client.subscribe(pattern, qos=qos)
        
        if result != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError(f"Subscribe failed with result={result}")
        
        # Wait for subscription acknowledgment
        if not self._subscribe_event.wait(timeout=2.0):
            print(f"[Warning] SUBACK not received for pattern '{pattern}'")
        
        # Wait for retained messages to arrive
        # Retained messages are sent immediately after successful subscription
        time.sleep(timeout)
        
        # Return copy of collected values
        with self._values_lock:
            return dict(self._values)

    def read_single(self, topic: str, timeout: float = 1.0) -> Optional[str]:
        """
        Read a single retained value for a specific topic.
        
        Args:
            topic: Exact MQTT topic (no wildcards)
            timeout: Time to wait for the retained message
        
        Returns:
            The retained value if found, None otherwise
        """
        values = self.read_pattern(topic, timeout=timeout)
        return values.get(topic)

    def read_all(self, base_topic: str = "#", timeout: float = 3.0) -> Dict[str, str]:
        """
        Read all retained messages under a base topic.
        
        Args:
            base_topic: Base topic pattern (default "#" for all)
            timeout: Time to wait for messages
        
        Returns:
            Dictionary of all retained messages
        """
        return self.read_pattern(base_topic, timeout=timeout)

    def is_connected(self) -> bool:
        """Check if currently connected to broker."""
        return self._connected and self._client.is_connected()


def main():
    """Demo showing how to read retained messages as KV store."""
    
    print("=== MQTT KV Store Reader Demo ===\n")
    
    # Create reader instance
    reader = KVStoreReader(
        host="localhost",
        port=1883,
        client_id="kv-reader-demo",
        use_mqttv5=False  # Use v3.1.1 for compatibility
    )
    
    # Connect to broker
    print("Connecting to broker...")
    if not reader.connect(timeout=5.0):
        print("Failed to connect to broker. Is Mosquitto running?")
        return 1
    
    try:
        # First, let's publish some retained messages to test
        print("\n1. Publishing test retained messages...")
        publisher = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id="kv-publisher",
            clean_session=True
        )
        publisher.connect("localhost", 1883)
        
        # Publish some test KV pairs
        test_data = {
            "kv/example/config/host": "192.168.1.100",
            "kv/example/config/port": "8080",
            "kv/example/config/enabled": "true",
            "kv/example/status/uptime": "3600",
            "kv/example/status/connections": "42",
            "kv/system/version": "1.2.3",
            "kv/system/build": "2024.12.20",
        }
        
        for topic, value in test_data.items():
            info = publisher.publish(topic, value, qos=1, retain=True)
            info.wait_for_publish()
            print(f"  ✓ Published: {topic} = {value}")
        
        publisher.disconnect()
        time.sleep(0.5)
        
        # Read all values under kv/example/
        print("\n2. Reading all values under 'kv/example/#':")
        values = reader.read_pattern("kv/example/#", timeout=2.0)
        if values:
            print("Retrieved KV entries:")
            for topic, value in sorted(values.items()):
                print(f"  {topic} => {value}")
        else:
            print("  No retained messages found")
        
        # Read specific config values
        print("\n3. Reading config values under 'kv/example/config/+':")
        config_values = reader.read_pattern("kv/example/config/+", timeout=1.0)
        if config_values:
            print("Configuration:")
            for topic, value in sorted(config_values.items()):
                key = topic.split('/')[-1]
                print(f"  {key}: {value}")
        
        # Read a single value
        print("\n4. Reading single value 'kv/system/version':")
        version = reader.read_single("kv/system/version", timeout=1.0)
        if version:
            print(f"  System version: {version}")
        else:
            print("  Version not found")
        
        # Read all retained messages
        print("\n5. Reading ALL retained messages on broker:")
        all_values = reader.read_all("#", timeout=2.0)
        if all_values:
            print(f"Found {len(all_values)} retained messages:")
            for topic, value in sorted(all_values.items())[:10]:  # Show first 10
                print(f"  {topic} => {value[:50]}{'...' if len(value) > 50 else ''}")
            if len(all_values) > 10:
                print(f"  ... and {len(all_values) - 10} more")
        else:
            print("  No retained messages found on broker")
        
        print("\n✓ Demo completed successfully!")
        
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        return 130
    except Exception as e:
        print(f"\n✗ Error: {e}")
        return 1
        
    finally:
        print("\nDisconnecting...")
        reader.disconnect()
    
    return 0


if __name__ == "__main__":
    exit(main())