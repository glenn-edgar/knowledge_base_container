"""
KVStoreWriter - Writes retained key/value messages to a Mosquitto MQTT broker.
Follows the same pattern as KVStoreReader for consistency.

Requires: paho-mqtt >= 2.0
    pip install paho-mqtt
"""

import time
import threading
from typing import Optional, Dict, Any, List, Tuple
import paho.mqtt.client as mqtt


class KVStoreWriter:
    """
    Writes retained messages to MQTT broker as key-value pairs.
    Retained messages act as a simple KV store in MQTT.
    """
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 1883,
        client_id: str = "kv-writer",
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
        self._publish_results: Dict[int, Tuple[bool, str]] = {}
        self._results_lock = threading.Lock()
        self._connected = False
        self._connect_event = threading.Event()
        self._publish_events: Dict[int, threading.Event] = {}
        self._events_lock = threading.Lock()

        # Use Callback API v2 to avoid deprecation warnings
        if use_mqttv5:
            self._client = mqtt.Client(
                callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                client_id=client_id,
                protocol=mqtt.MQTTv5,
                clean_start=True,  # v5 uses clean_start
            )
        else:
            self._client = mqtt.Client(
                callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                client_id=client_id,
                protocol=mqtt.MQTTv311,  # Explicitly set protocol
                clean_session=True,  # v3.1.1 uses clean_session
            )

        if username is not None:
            self._client.username_pw_set(username=username, password=password)

        # Assign callbacks with correct signatures for VERSION2
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_publish = self._on_publish

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
        """
        self._connected = False
        self._connect_event.clear()
        print(f"[Disconnected] reason_code={reason_code}")

    def _on_publish(self, client, userdata, mid, reason_code, properties):
        """
        Callback for publish completion (VERSION2 API).
        Parameters: (client, userdata, mid, reason_code, properties)
        Called when a message has been successfully sent to the broker.
        """
        # Check if publish was successful
        success = True
        if self.use_mqttv5:
            # MQTT v5 uses reason codes
            if hasattr(reason_code, 'is_failure'):
                success = not reason_code.is_failure
            else:
                success = (reason_code == 0)
        else:
            # For MQTT v3.1.1, reason_code is actually just the mid again in VERSION2
            # The publish is considered successful if we get the callback
            success = True
        
        with self._results_lock:
            self._publish_results[mid] = (success, f"reason_code={reason_code}")
        
        # Signal the event for this message ID
        with self._events_lock:
            if mid in self._publish_events:
                self._publish_events[mid].set()

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

    def write_single(
        self, 
        topic: str, 
        value: Any, 
        qos: int = 1, 
        retain: bool = True,
        timeout: float = 2.0
    ) -> bool:
        """
        Write a single key/value pair to the MQTT broker.
        
        Args:
            topic: MQTT topic (the "key" in KV store)
            value: Value to store (will be converted to string)
            qos: Quality of Service level (0, 1, or 2)
            retain: Whether to retain the message (should be True for KV store)
            timeout: Time to wait for publish confirmation (seconds)
        
        Returns:
            True if successfully published, False otherwise
        """
        if not self._connected:
            raise RuntimeError("Not connected. Call connect() first.")
        
        # Convert value to string if needed
        value_str = str(value) if not isinstance(value, (str, bytes)) else value
        
        # Create event for this publish
        publish_event = threading.Event()
        
        # Publish the message
        result = self._client.publish(topic, value_str, qos=qos, retain=retain)
        
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            print(f"[Error] Failed to queue message for {topic}: rc={result.rc}")
            return False
        
        # Store the event for this message ID
        with self._events_lock:
            self._publish_events[result.mid] = publish_event
        
        # Wait for publish to complete
        if not publish_event.wait(timeout):
            print(f"[Timeout] Publish timeout for {topic}")
            return False
        
        # Check result
        with self._results_lock:
            success, _ = self._publish_results.get(result.mid, (False, "Unknown"))
            # Clean up
            self._publish_results.pop(result.mid, None)
        
        with self._events_lock:
            self._publish_events.pop(result.mid, None)
        
        if success:
            print(f"[Written] {topic} => {value_str[:50]}{'...' if len(value_str) > 50 else ''}")
        
        return success

    def write_batch(
        self,
        kv_pairs: Dict[str, Any],
        qos: int = 1,
        retain: bool = True,
        timeout: float = 10.0,
        parallel: bool = True
    ) -> Tuple[int, List[str]]:
        """
        Write multiple key/value pairs to the MQTT broker.
        
        Args:
            kv_pairs: Dictionary of topic/value pairs to write
            qos: Quality of Service level for all messages
            retain: Whether to retain messages (should be True for KV store)
            timeout: Total time to wait for all publishes (seconds)
            parallel: If True, publish all then wait; if False, publish one by one
        
        Returns:
            Tuple of (success_count, list_of_failed_topics)
        """
        if not self._connected:
            raise RuntimeError("Not connected. Call connect() first.")
        
        if not kv_pairs:
            return 0, []
        
        failed_topics = []
        success_count = 0
        
        if parallel:
            # Publish all messages first, then wait for confirmations
            publish_info = []
            events = {}
            
            for topic, value in kv_pairs.items():
                value_str = str(value) if not isinstance(value, (str, bytes)) else value
                result = self._client.publish(topic, value_str, qos=qos, retain=retain)
                
                if result.rc != mqtt.MQTT_ERR_SUCCESS:
                    print(f"[Error] Failed to queue {topic}: rc={result.rc}")
                    failed_topics.append(topic)
                else:
                    event = threading.Event()
                    with self._events_lock:
                        self._publish_events[result.mid] = event
                    publish_info.append((result.mid, topic, event))
                    events[result.mid] = (topic, event)
            
            # Wait for all publishes with timeout
            start_time = time.time()
            for mid, topic, event in publish_info:
                remaining_time = max(0.1, timeout - (time.time() - start_time))
                if event.wait(remaining_time):
                    with self._results_lock:
                        success, _ = self._publish_results.get(mid, (False, "Unknown"))
                        if success:
                            success_count += 1
                            print(f"[Batch Written] {topic}")
                        else:
                            failed_topics.append(topic)
                        self._publish_results.pop(mid, None)
                else:
                    print(f"[Batch Timeout] {topic}")
                    failed_topics.append(topic)
                
                with self._events_lock:
                    self._publish_events.pop(mid, None)
        else:
            # Sequential publishing
            per_message_timeout = timeout / len(kv_pairs)
            for topic, value in kv_pairs.items():
                if self.write_single(topic, value, qos, retain, per_message_timeout):
                    success_count += 1
                else:
                    failed_topics.append(topic)
        
        return success_count, failed_topics

    def delete_single(self, topic: str, timeout: float = 2.0) -> bool:
        """
        Delete a key from the KV store by publishing an empty retained message.
        
        Args:
            topic: MQTT topic to delete
            timeout: Time to wait for deletion confirmation
        
        Returns:
            True if successfully deleted, False otherwise
        """
        if not self._connected:
            raise RuntimeError("Not connected. Call connect() first.")
        
        # Publishing empty retained message removes the retained message
        result = self._client.publish(topic, "", qos=1, retain=True)
        
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            print(f"[Error] Failed to delete {topic}: rc={result.rc}")
            return False
        
        # Create and wait for event
        delete_event = threading.Event()
        with self._events_lock:
            self._publish_events[result.mid] = delete_event
        
        if not delete_event.wait(timeout):
            print(f"[Timeout] Delete timeout for {topic}")
            return False
        
        # Check result
        with self._results_lock:
            success, _ = self._publish_results.get(result.mid, (False, "Unknown"))
            self._publish_results.pop(result.mid, None)
        
        with self._events_lock:
            self._publish_events.pop(result.mid, None)
        
        if success:
            print(f"[Deleted] {topic}")
        
        return success

    def delete_batch(
        self,
        topics: List[str],
        timeout: float = 10.0
    ) -> Tuple[int, List[str]]:
        """
        Delete multiple keys from the KV store.
        
        Args:
            topics: List of MQTT topics to delete
            timeout: Total time to wait for all deletions
        
        Returns:
            Tuple of (success_count, list_of_failed_topics)
        """
        if not self._connected:
            raise RuntimeError("Not connected. Call connect() first.")
        
        failed_topics = []
        success_count = 0
        
        # Publish all delete messages (empty retained)
        publish_info = []
        
        for topic in topics:
            result = self._client.publish(topic, "", qos=1, retain=True)
            
            if result.rc != mqtt.MQTT_ERR_SUCCESS:
                print(f"[Error] Failed to queue delete for {topic}")
                failed_topics.append(topic)
            else:
                event = threading.Event()
                with self._events_lock:
                    self._publish_events[result.mid] = event
                publish_info.append((result.mid, topic, event))
        
        # Wait for confirmations
        start_time = time.time()
        for mid, topic, event in publish_info:
            remaining_time = max(0.1, timeout - (time.time() - start_time))
            if event.wait(remaining_time):
                with self._results_lock:
                    success, _ = self._publish_results.get(mid, (False, "Unknown"))
                    if success:
                        success_count += 1
                        print(f"[Batch Deleted] {topic}")
                    else:
                        failed_topics.append(topic)
                    self._publish_results.pop(mid, None)
            else:
                print(f"[Delete Timeout] {topic}")
                failed_topics.append(topic)
            
            with self._events_lock:
                self._publish_events.pop(mid, None)
        
        return success_count, failed_topics

    def update_single(
        self,
        topic: str,
        value: Any,
        qos: int = 1,
        timeout: float = 2.0
    ) -> bool:
        """
        Update an existing key's value (alias for write_single with retain=True).
        
        Args:
            topic: MQTT topic to update
            value: New value
            qos: Quality of Service level
            timeout: Time to wait for update confirmation
        
        Returns:
            True if successfully updated, False otherwise
        """
        return self.write_single(topic, value, qos, retain=True, timeout=timeout)

    def clear_pattern(
        self,
        pattern: str,
        reader_instance: Optional['KVStoreReader'] = None,
        timeout: float = 10.0
    ) -> Tuple[int, List[str]]:
        """
        Clear all keys matching a pattern.
        Requires a KVStoreReader instance to find matching topics.
        
        Args:
            pattern: MQTT topic pattern (can include wildcards)
            reader_instance: KVStoreReader instance to find matching topics
            timeout: Total timeout for operation
        
        Returns:
            Tuple of (deleted_count, list_of_failed_topics)
        """
        if reader_instance is None:
            raise ValueError("reader_instance is required to find topics matching pattern")
        
        if not self._connected:
            raise RuntimeError("Not connected. Call connect() first.")
        
        # Use reader to find all topics matching pattern
        print(f"[Clear] Finding topics matching '{pattern}'...")
        matching_topics = reader_instance.read_pattern(pattern, timeout=2.0)
        
        if not matching_topics:
            print(f"[Clear] No topics found matching '{pattern}'")
            return 0, []
        
        print(f"[Clear] Found {len(matching_topics)} topics to delete")
        topics_to_delete = list(matching_topics.keys())
        
        # Delete all matching topics
        return self.delete_batch(topics_to_delete, timeout=timeout - 2.0)

    def is_connected(self) -> bool:
        """Check if currently connected to broker."""
        return self._connected and self._client.is_connected()


def demo_writer():
    """Demonstrate the KVStoreWriter class functionality."""
    print("=== KVStoreWriter Demo ===\n")
    
    # Create writer instance
    writer = KVStoreWriter(
        host="localhost",
        port=1883,
        client_id="kv-writer-demo",
        use_mqttv5=False
    )
    
    # Connect to broker
    print("Connecting to broker...")
    if not writer.connect(timeout=5.0):
        print("Failed to connect to broker. Is Mosquitto running?")
        return 1
    
    try:
        # 1. Write single values
        print("\n1. Writing single values:")
        writer.write_single("demo/config/host", "192.168.1.1")
        writer.write_single("demo/config/port", 8080)
        writer.write_single("demo/config/enabled", True)
        
        # 2. Write batch of values
        print("\n2. Writing batch of values:")
        batch_data = {
            "demo/status/cpu": "45.2",
            "demo/status/memory": "78.5",
            "demo/status/disk": "62.1",
            "demo/status/network": "up",
            "demo/status/services": "healthy",
        }
        success, failed = writer.write_batch(batch_data, parallel=True)
        print(f"  Batch write: {success} successful, {len(failed)} failed")
        
        # 3. Update a value
        print("\n3. Updating a value:")
        writer.update_single("demo/config/port", 9090)
        
        # 4. Delete single value
        print("\n4. Deleting single value:")
        writer.delete_single("demo/config/enabled")
        
        # 5. Delete batch
        print("\n5. Deleting batch of values:")
        to_delete = ["demo/status/network", "demo/status/services"]
        success, failed = writer.delete_batch(to_delete)
        print(f"  Batch delete: {success} successful, {len(failed)} failed")
        
        # 6. Clear pattern (requires reader)
        print("\n6. Clearing pattern (requires KVStoreReader):")
        # This would require a reader instance
        # reader = KVStoreReader(host="localhost", port=1883)
        # reader.connect()
        # success, failed = writer.clear_pattern("demo/status/#", reader)
        # reader.disconnect()
        
        print("\n✓ Demo completed successfully!")
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        return 1
    
    finally:
        print("\nDisconnecting...")
        writer.disconnect()
    
    return 0


if __name__ == "__main__":
    exit(demo_writer())