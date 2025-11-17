"""
TopicSubscriber - Subscribes to MQTT topics and issues callbacks for received messages.
Follows the same pattern as KVStoreReader and KVStoreWriter for consistency.

Requires: paho-mqtt >= 2.0
    pip install paho-mqtt
"""

import time
import threading
from typing import Optional, Dict, Callable, Any, List, Tuple
from dataclasses import dataclass
from datetime import datetime
import paho.mqtt.client as mqtt


@dataclass
class MessageInfo:
    """Data class for message information passed to callbacks."""
    topic: str
    payload: str
    qos: int
    retain: bool
    timestamp: datetime
    raw_payload: bytes


class TopicSubscriber:
    """
    Subscribes to MQTT topics and executes callbacks when messages are received.
    Supports multiple topic patterns with individual or shared callbacks.
    """
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 1883,
        client_id: str = "topic-subscriber",
        keepalive: int = 60,
        use_mqttv5: bool = False,
        username: Optional[str] = None,
        password: Optional[str] = None,
        auto_reconnect: bool = True,
        reconnect_delay: float = 5.0,
    ) -> None:
        self.host = host
        self.port = port
        self.keepalive = keepalive
        self.use_mqttv5 = use_mqttv5
        self.auto_reconnect = auto_reconnect
        self.reconnect_delay = reconnect_delay

        # Thread safety
        self._callbacks: Dict[str, List[Callable[[MessageInfo], None]]] = {}
        self._callbacks_lock = threading.Lock()
        self._subscriptions: Dict[str, int] = {}  # topic -> qos
        self._subscriptions_lock = threading.Lock()
        self._connected = False
        self._connect_event = threading.Event()
        self._running = False
        self._stats = {
            "messages_received": 0,
            "callbacks_executed": 0,
            "errors": 0,
            "last_message_time": None
        }
        self._stats_lock = threading.Lock()

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
            
            # Resubscribe to all topics after reconnection
            with self._subscriptions_lock:
                for topic, qos in self._subscriptions.items():
                    result, mid = self._client.subscribe(topic, qos=qos)
                    if result == mqtt.MQTT_ERR_SUCCESS:
                        print(f"[Resubscribed] {topic} (QoS {qos})")
                    else:
                        print(f"[Resubscribe Failed] {topic}: result={result}")
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
        
        # Auto-reconnect if enabled and still running
        if self.auto_reconnect and self._running:
            print(f"[Reconnecting] Will attempt reconnection in {self.reconnect_delay} seconds...")
            threading.Timer(self.reconnect_delay, self._attempt_reconnect).start()

    def _attempt_reconnect(self):
        """Attempt to reconnect to the broker."""
        if self._running and not self._connected:
            try:
                print("[Reconnecting] Attempting to reconnect...")
                self._client.reconnect()
            except Exception as e:
                print(f"[Reconnect Failed] {e}")
                if self._running:
                    threading.Timer(self.reconnect_delay, self._attempt_reconnect).start()

    def _on_message(self, client, userdata, msg):
        """
        Callback for received messages.
        Executes registered callbacks for matching topics.
        """
        # Update statistics
        with self._stats_lock:
            self._stats["messages_received"] += 1
            self._stats["last_message_time"] = datetime.now()
        
        # Decode payload
        try:
            payload = msg.payload.decode(errors="replace")
        except Exception as e:
            print(f"[Decode Error] Failed to decode payload: {e}")
            payload = str(msg.payload)
        
        # Create message info
        msg_info = MessageInfo(
            topic=msg.topic,
            payload=payload,
            qos=msg.qos,
            retain=msg.retain,
            timestamp=datetime.now(),
            raw_payload=msg.payload
        )
        
        print(f"[Message] {msg.topic} => {payload[:100]}{'...' if len(payload) > 100 else ''} (QoS {msg.qos}, Retain: {msg.retain})")
        
        # Find and execute matching callbacks
        callbacks_to_execute = []
        with self._callbacks_lock:
            for pattern, callback_list in self._callbacks.items():
                if self._topic_matches(msg.topic, pattern):
                    callbacks_to_execute.extend(callback_list)
        
        # Execute callbacks outside of lock
        for callback in callbacks_to_execute:
            try:
                callback(msg_info)
                with self._stats_lock:
                    self._stats["callbacks_executed"] += 1
            except Exception as e:
                print(f"[Callback Error] Error in callback for {msg.topic}: {e}")
                with self._stats_lock:
                    self._stats["errors"] += 1

    def _on_subscribe(self, client, userdata, mid, reason_codes, properties):
        """
        Callback for subscription acknowledgment (VERSION2 API).
        Parameters: (client, userdata, mid, reason_codes, properties)
        """
        print(f"[Subscribed] mid={mid}, reason_codes={reason_codes}")

    def _topic_matches(self, topic: str, pattern: str) -> bool:
        """
        Check if a topic matches a pattern (including wildcards).
        
        Args:
            topic: The actual topic
            pattern: The pattern to match (may include + and # wildcards)
        
        Returns:
            True if topic matches pattern, False otherwise
        """
        # Use paho's built-in topic matching
        return mqtt.topic_matches_sub(pattern, topic)

    def connect(self, timeout: float = 5.0) -> bool:
        """
        Connect to the MQTT broker.
        
        Args:
            timeout: Maximum time to wait for connection (seconds)
        
        Returns:
            True if connected successfully, False otherwise
        """
        self._connect_event.clear()
        self._running = True
        
        try:
            # Connect to broker
            self._client.connect(self.host, self.port, keepalive=self.keepalive)
            
            # Start the network loop
            self._client.loop_start()
            
            # Wait for connection to complete
            if not self._connect_event.wait(timeout):
                print(f"[Error] Connection timeout after {timeout} seconds")
                self._client.loop_stop()
                self._running = False
                return False
            
            if not self._connected:
                print(f"[Error] Failed to connect to {self.host}:{self.port}")
                self._client.loop_stop()
                self._running = False
                return False
            
            return True
            
        except Exception as e:
            print(f"[Error] Connection failed: {e}")
            self._running = False
            return False

    def disconnect(self) -> None:
        """Disconnect from the MQTT broker."""
        self._running = False
        try:
            self._client.loop_stop()
            self._client.disconnect()
        except Exception as e:
            print(f"[Error] Disconnect failed: {e}")
        finally:
            self._connected = False
            self._connect_event.clear()

    def subscribe(
        self,
        topic: str,
        callback: Callable[[MessageInfo], None],
        qos: int = 1,
        replace: bool = False
    ) -> bool:
        """
        Subscribe to a topic with a callback function.
        
        Args:
            topic: MQTT topic pattern (can include wildcards + and #)
            callback: Function to call when message is received
            qos: Quality of Service level (0, 1, or 2)
            replace: If True, replace existing callbacks for this topic
        
        Returns:
            True if subscription successful, False otherwise
        """
        if not self._connected:
            raise RuntimeError("Not connected. Call connect() first.")
        
        # Add callback to registry
        with self._callbacks_lock:
            if replace or topic not in self._callbacks:
                self._callbacks[topic] = [callback]
            else:
                self._callbacks[topic].append(callback)
        
        # Subscribe if not already subscribed
        with self._subscriptions_lock:
            if topic not in self._subscriptions:
                result, mid = self._client.subscribe(topic, qos=qos)
                
                if result != mqtt.MQTT_ERR_SUCCESS:
                    print(f"[Error] Subscribe failed for {topic}: result={result}")
                    # Remove callback on failure
                    with self._callbacks_lock:
                        if topic in self._callbacks:
                            self._callbacks[topic].remove(callback)
                            if not self._callbacks[topic]:
                                del self._callbacks[topic]
                    return False
                
                self._subscriptions[topic] = qos
                print(f"[Subscribed] {topic} (QoS {qos})")
            else:
                print(f"[Already Subscribed] {topic}, adding callback")
        
        return True

    def subscribe_many(
        self,
        subscriptions: List[Tuple[str, Callable[[MessageInfo], None], int]],
        replace: bool = False
    ) -> Tuple[int, List[str]]:
        """
        Subscribe to multiple topics with callbacks.
        
        Args:
            subscriptions: List of (topic, callback, qos) tuples
            replace: If True, replace existing callbacks
        
        Returns:
            Tuple of (success_count, list_of_failed_topics)
        """
        if not self._connected:
            raise RuntimeError("Not connected. Call connect() first.")
        
        success_count = 0
        failed_topics = []
        
        for topic, callback, qos in subscriptions:
            if self.subscribe(topic, callback, qos, replace):
                success_count += 1
            else:
                failed_topics.append(topic)
        
        return success_count, failed_topics

    def unsubscribe(self, topic: str) -> bool:
        """
        Unsubscribe from a topic and remove all its callbacks.
        
        Args:
            topic: MQTT topic pattern to unsubscribe from
        
        Returns:
            True if unsubscribed successfully, False otherwise
        """
        if not self._connected:
            raise RuntimeError("Not connected. Call connect() first.")
        
        # Remove from subscriptions
        with self._subscriptions_lock:
            if topic not in self._subscriptions:
                print(f"[Warning] Not subscribed to {topic}")
                return False
            
            result, mid = self._client.unsubscribe(topic)
            
            if result != mqtt.MQTT_ERR_SUCCESS:
                print(f"[Error] Unsubscribe failed for {topic}: result={result}")
                return False
            
            del self._subscriptions[topic]
        
        # Remove callbacks
        with self._callbacks_lock:
            if topic in self._callbacks:
                del self._callbacks[topic]
        
        print(f"[Unsubscribed] {topic}")
        return True

    def unsubscribe_all(self) -> int:
        """
        Unsubscribe from all topics.
        
        Returns:
            Number of topics unsubscribed from
        """
        topics_to_unsubscribe = []
        with self._subscriptions_lock:
            topics_to_unsubscribe = list(self._subscriptions.keys())
        
        count = 0
        for topic in topics_to_unsubscribe:
            if self.unsubscribe(topic):
                count += 1
        
        return count

    def add_callback(
        self,
        topic: str,
        callback: Callable[[MessageInfo], None]
    ) -> bool:
        """
        Add an additional callback to an existing subscription.
        
        Args:
            topic: MQTT topic pattern
            callback: Function to call when message is received
        
        Returns:
            True if callback added, False if topic not subscribed
        """
        with self._subscriptions_lock:
            if topic not in self._subscriptions:
                print(f"[Error] Not subscribed to {topic}. Use subscribe() first.")
                return False
        
        with self._callbacks_lock:
            if topic not in self._callbacks:
                self._callbacks[topic] = []
            self._callbacks[topic].append(callback)
        
        print(f"[Callback Added] Added callback for {topic}")
        return True

    def remove_callback(
        self,
        topic: str,
        callback: Callable[[MessageInfo], None]
    ) -> bool:
        """
        Remove a specific callback from a topic.
        
        Args:
            topic: MQTT topic pattern
            callback: The callback function to remove
        
        Returns:
            True if callback removed, False otherwise
        """
        with self._callbacks_lock:
            if topic not in self._callbacks:
                return False
            
            try:
                self._callbacks[topic].remove(callback)
                if not self._callbacks[topic]:
                    del self._callbacks[topic]
                print(f"[Callback Removed] Removed callback for {topic}")
                return True
            except ValueError:
                return False

    def get_subscriptions(self) -> Dict[str, int]:
        """
        Get current subscriptions.
        
        Returns:
            Dictionary of topic -> qos
        """
        with self._subscriptions_lock:
            return dict(self._subscriptions)

    def get_statistics(self) -> Dict[str, Any]:
        """
        Get subscriber statistics.
        
        Returns:
            Dictionary of statistics
        """
        with self._stats_lock:
            return dict(self._stats)

    def is_connected(self) -> bool:
        """Check if currently connected to broker."""
        return self._connected and self._client.is_connected()

    def wait_for_messages(self, timeout: Optional[float] = None) -> None:
        """
        Block and wait for messages.
        
        Args:
            timeout: Maximum time to wait (None for indefinite)
        """
        if timeout:
            time.sleep(timeout)
        else:
            try:
                while self._running and self._connected:
                    time.sleep(1)
            except KeyboardInterrupt:
                print("\n[Interrupted] Stopping message wait")


def demo_subscriber():
    """Demonstrate the TopicSubscriber class functionality."""
    print("=== TopicSubscriber Demo ===\n")
    
    # Define some callback functions
    def on_config_message(msg: MessageInfo):
        """Callback for configuration messages."""
        print(f"  [Config Callback] Received: {msg.topic} = {msg.payload}")
    
    def on_status_message(msg: MessageInfo):
        """Callback for status messages."""
        print(f"  [Status Callback] {msg.topic}: {msg.payload} at {msg.timestamp.strftime('%H:%M:%S')}")
    
    def on_any_message(msg: MessageInfo):
        """Generic callback for any message."""
        print(f"  [Generic Callback] Topic: {msg.topic}, Retained: {msg.retain}")
    
    def on_sensor_data(msg: MessageInfo):
        """Callback for sensor data."""
        try:
            value = float(msg.payload)
            print(f"  [Sensor Callback] {msg.topic.split('/')[-1]}: {value:.2f}")
        except ValueError:
            print(f"  [Sensor Callback] Invalid sensor data: {msg.payload}")
    
    # Create subscriber instance
    subscriber = TopicSubscriber(
        host="localhost",
        port=1883,
        client_id="subscriber-demo",
        auto_reconnect=True,
        reconnect_delay=5.0
    )
    
    # Connect to broker
    print("Connecting to broker...")
    if not subscriber.connect(timeout=5.0):
        print("Failed to connect to broker. Is Mosquitto running?")
        return 1
    
    try:
        # 1. Subscribe to specific topics with callbacks
        print("\n1. Subscribing to specific topics:")
        subscriber.subscribe("demo/config/+", on_config_message, qos=1)
        subscriber.subscribe("demo/status/#", on_status_message, qos=1)
        subscriber.subscribe("demo/sensors/+/temperature", on_sensor_data, qos=2)
        
        # 2. Subscribe to everything with a generic callback
        print("\n2. Adding generic subscription:")
        subscriber.subscribe("#", on_any_message, qos=0)
        
        # 3. Subscribe to multiple topics at once
        print("\n3. Subscribing to multiple topics:")
        multi_subs = [
            ("demo/alerts/+", lambda m: print(f"  [Alert] {m.payload}"), 2),
            ("demo/logs/+", lambda m: print(f"  [Log] {m.payload[:50]}..."), 0),
        ]
        success, failed = subscriber.subscribe_many(multi_subs)
        print(f"  Subscribed to {success} topics, {len(failed)} failed")
        
        # 4. Show current subscriptions
        print("\n4. Current subscriptions:")
        subs = subscriber.get_subscriptions()
        for topic, qos in subs.items():
            print(f"  {topic} (QoS {qos})")
        
        # 5. Publish some test messages (using a separate client)
        print("\n5. Publishing test messages...")
        publisher = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id="test-publisher"
        )
        publisher.connect("localhost", 1883)
        publisher.loop_start()
        time.sleep(0.5)
        
        test_messages = [
            ("demo/config/host", "192.168.1.100"),
            ("demo/config/port", "8080"),
            ("demo/status/cpu", "45.2"),
            ("demo/status/memory/used", "78.5"),
            ("demo/sensors/room1/temperature", "22.5"),
            ("demo/sensors/room2/temperature", "20.1"),
            ("demo/alerts/high", "CPU usage critical"),
            ("demo/logs/app", "Application started successfully"),
        ]
        
        for topic, payload in test_messages:
            publisher.publish(topic, payload, qos=1, retain=False)
            print(f"  Published: {topic}")
        
        publisher.loop_stop()
        publisher.disconnect()
        
        # 6. Wait for messages
        print("\n6. Waiting for messages (5 seconds)...")
        subscriber.wait_for_messages(timeout=5.0)
        
        # 7. Show statistics
        print("\n7. Statistics:")
        stats = subscriber.get_statistics()
        for key, value in stats.items():
            print(f"  {key}: {value}")
        
        # 8. Add another callback to existing subscription
        print("\n8. Adding additional callback:")
        def extra_config_callback(msg: MessageInfo):
            print(f"  [Extra Config] Processing {msg.topic}")
        
        subscriber.add_callback("demo/config/+", extra_config_callback)
        
        # 9. Unsubscribe from a topic
        print("\n9. Unsubscribing from demo/logs/+:")
        subscriber.unsubscribe("demo/logs/+")
        
        print("\n✓ Demo completed successfully!")
        
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        return 130
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1
        
    finally:
        print("\nCleaning up...")
        # Unsubscribe from all topics
        count = subscriber.unsubscribe_all()
        print(f"  Unsubscribed from {count} topics")
        
        # Disconnect
        subscriber.disconnect()
        print("  Disconnected")
    
    return 0


if __name__ == "__main__":
    exit(demo_subscriber())

