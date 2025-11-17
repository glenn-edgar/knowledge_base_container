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


def write_test_data():
    """
    Write test data to MQTT broker as retained messages.
    This simulates populating a KV store.
    """
    print("=== Writing Test Data to MQTT KV Store ===\n")
    
    # Create publisher with connection verification
    publisher_connected = threading.Event()
    
    def on_connect(client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            publisher_connected.set()
            print("✓ Publisher connected to broker")
        else:
            print(f"✗ Publisher connection failed: {reason_code}")
    
    publisher = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id="kv-writer",
        clean_session=True
    )
    publisher.on_connect = on_connect
    
    try:
        # Connect and start the network loop
        print("Connecting publisher to localhost:1883...")
        publisher.connect("localhost", 1883)
        publisher.loop_start()
        
        # Wait for connection confirmation
        if not publisher_connected.wait(timeout=5.0):
            print("✗ Failed to connect to broker. Is Mosquitto running?")
            return False
        
        # Define test data to publish
        test_data = {
            # Configuration data
            "kv/example/config/host": "192.168.1.100",
            "kv/example/config/port": "8080",
            "kv/example/config/enabled": "true",
            "kv/example/config/timeout": "30",
            "kv/example/config/retry_count": "3",
            
            # Status data
            "kv/example/status/uptime": "3600",
            "kv/example/status/connections": "42",
            "kv/example/status/last_error": "none",
            "kv/example/status/cpu_usage": "15.7",
            
            # System information
            "kv/system/version": "1.2.3",
            "kv/system/build": "2024.12.20",
            "kv/system/hostname": "mqtt-server-01",
            "kv/system/os": "Linux 5.15.0",
            
            # Application data
            "kv/app/users/count": "1250",
            "kv/app/users/active": "523",
            "kv/app/database/connected": "true",
            "kv/app/database/pool_size": "10",
            
            # Some nested structure
            "kv/sensors/temperature/living_room": "22.5",
            "kv/sensors/temperature/bedroom": "20.1",
            "kv/sensors/humidity/living_room": "45",
            "kv/sensors/humidity/bedroom": "50",
        }
        
        print(f"\nPublishing {len(test_data)} retained messages...")
        success_count = 0
        
        for topic, value in test_data.items():
            info = publisher.publish(topic, value, qos=1, retain=True)
            if info.rc != mqtt.MQTT_ERR_SUCCESS:
                print(f"  ✗ Failed to queue: {topic}")
                continue
            
            try:
                info.wait_for_publish(timeout=2.0)
                print(f"  ✓ {topic} = {value}")
                success_count += 1
            except (ValueError, RuntimeError) as e:
                print(f"  ✗ Failed: {topic} - {e}")
        
        print(f"\n✓ Successfully published {success_count}/{len(test_data)} messages")
        return success_count == len(test_data)
        
    except Exception as e:
        print(f"\n✗ Error writing test data: {e}")
        return False
        
    finally:
        # Clean shutdown
        print("Closing publisher connection...")
        publisher.loop_stop()
        publisher.disconnect()
        print("Publisher disconnected\n")


def demonstrate_kvstore_reader():
    """
    Demonstrate the KVStoreReader class functionality.
    This shows all the different ways to read from the KV store.
    """
    print("=== Demonstrating KVStoreReader Class ===\n")
    
    # Create reader instance
    print("1. Creating KVStoreReader instance...")
    reader = KVStoreReader(
        host="localhost",
        port=1883,
        client_id="kv-reader-demo",
        use_mqttv5=False,  # Use v3.1.1 for compatibility
        keepalive=60
    )
    
    # Test connection
    print("\n2. Testing connection to broker...")
    if not reader.connect(timeout=5.0):
        print("✗ Failed to connect to broker")
        return False
    
    print(f"✓ Connected successfully")
    print(f"  Connection status: {reader.is_connected()}")
    
    try:
        # Demonstrate reading all values under a pattern
        print("\n3. Reading all values under 'kv/example/#' (wildcards):")
        print("-" * 50)
        example_values = reader.read_pattern("kv/example/#", timeout=2.0)
        if example_values:
            print(f"Found {len(example_values)} entries:")
            for topic, value in sorted(example_values.items()):
                category = topic.split('/')[2] if len(topic.split('/')) > 2 else 'unknown'
                key = topic.split('/')[-1]
                print(f"  [{category}] {key}: {value}")
        else:
            print("  No retained messages found under kv/example/#")
        
        # Demonstrate reading with single-level wildcard
        print("\n4. Reading config values with 'kv/example/config/+' (single-level wildcard):")
        print("-" * 50)
        config_values = reader.read_pattern("kv/example/config/+", timeout=1.0)
        if config_values:
            print("Configuration parameters:")
            for topic, value in sorted(config_values.items()):
                param = topic.split('/')[-1]
                print(f"  {param} = {value}")
        else:
            print("  No config values found")
        
        # Demonstrate reading a single specific value
        print("\n5. Reading single value 'kv/system/version' (exact topic):")
        print("-" * 50)
        version = reader.read_single("kv/system/version", timeout=1.0)
        if version:
            print(f"  System version: {version}")
            
            # Also try reading other system values
            build = reader.read_single("kv/system/build", timeout=1.0)
            hostname = reader.read_single("kv/system/hostname", timeout=1.0)
            if build:
                print(f"  Build date: {build}")
            if hostname:
                print(f"  Hostname: {hostname}")
        else:
            print("  Version not found")
        
        # Demonstrate reading sensor data with pattern
        print("\n6. Reading sensor data 'kv/sensors/+/+' (multiple wildcards):")
        print("-" * 50)
        sensor_values = reader.read_pattern("kv/sensors/+/+", timeout=1.5)
        if sensor_values:
            # Organize by sensor type
            sensors = {}
            for topic, value in sensor_values.items():
                parts = topic.split('/')
                if len(parts) >= 4:
                    sensor_type = parts[2]
                    location = parts[3]
                    if sensor_type not in sensors:
                        sensors[sensor_type] = {}
                    sensors[sensor_type][location] = value
            
            for sensor_type, locations in sorted(sensors.items()):
                print(f"  {sensor_type.capitalize()}:")
                for location, value in sorted(locations.items()):
                    print(f"    {location}: {value}")
        else:
            print("  No sensor data found")
        
        # Demonstrate reading all retained messages
        print("\n7. Reading ALL retained messages on broker with '#':")
        print("-" * 50)
        all_values = reader.read_all("#", timeout=2.0)
        if all_values:
            print(f"Total retained messages on broker: {len(all_values)}")
            
            # Group by top-level topic
            topics_by_prefix = {}
            for topic in all_values.keys():
                prefix = topic.split('/')[0] if '/' in topic else topic
                topics_by_prefix[prefix] = topics_by_prefix.get(prefix, 0) + 1
            
            print("Message distribution:")
            for prefix, count in sorted(topics_by_prefix.items()):
                print(f"  {prefix}/: {count} messages")
            
            # Show a few examples
            print("\nFirst 5 messages:")
            for topic, value in list(sorted(all_values.items()))[:5]:
                print(f"  {topic} = {value[:50]}{'...' if len(value) > 50 else ''}")
        else:
            print("  No retained messages found on broker")
        
        # Demonstrate pattern for app-specific data
        print("\n8. Reading application metrics 'kv/app/+/+':")
        print("-" * 50)
        app_values = reader.read_pattern("kv/app/+/+", timeout=1.0)
        if app_values:
            print("Application metrics:")
            for topic, value in sorted(app_values.items()):
                parts = topic.split('/')
                if len(parts) >= 4:
                    category = parts[2]
                    metric = parts[3]
                    print(f"  {category}.{metric} = {value}")
        else:
            print("  No application metrics found")
        
        print("\n✓ Demonstration completed successfully!")
        return True
        
    except Exception as e:
        print(f"\n✗ Error during demonstration: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        # Always disconnect
        print("\n9. Cleaning up...")
        print(f"  Final connection status: {reader.is_connected()}")
        reader.disconnect()
        print("  Reader disconnected")


def main():
    """Main function to run the complete demo."""
    print("=" * 60)
    print(" MQTT KV Store Reader - Complete Demo")
    print("=" * 60)
    print()
    
    # Step 1: Write test data
    write_success = write_test_data()
    if not write_success:
        print("✗ Failed to write test data. Exiting.")
        return 1
    
    # Small delay to ensure all messages are processed
    print("Waiting for messages to settle...")
    time.sleep(1.0)
    print()
    
    # Step 2: Demonstrate reading the data
    read_success = demonstrate_kvstore_reader()
    if not read_success:
        print("✗ Demonstration failed.")
        return 1
    
    print("\n" + "=" * 60)
    print(" Demo completed! All connections closed.")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    exit(main())