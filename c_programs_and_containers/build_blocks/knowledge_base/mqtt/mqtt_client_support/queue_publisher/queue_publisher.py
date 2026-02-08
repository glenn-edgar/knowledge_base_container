"""
JSON queue-style publisher for Mosquitto.
Publishes JSON to a topic (non-retained) to be consumed by subscribers.

Requires: paho-mqtt >= 2.0
    pip install paho-mqtt
"""

from typing import Optional, Any, Dict, Tuple, List
import json
import time
import threading
import paho.mqtt.client as mqtt


class QueuePublisher:
    """
    Publishes JSON messages to a topic. Use QoS=1 for at-least-once delivery.
    Consumers should use persistent sessions / shared subscriptions for queue-like behavior.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 1883,
        client_id: str = "queue-pub",
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
        self._connected = False
        self._connect_event = threading.Event()
        self._connect_rc: Optional[int] = None

        # Create client with correct parameters for v3.1.1 or v5
        if use_mqttv5:
            # NOTE: do NOT pass clean_start in the constructor for MQTT v5
            self._client = mqtt.Client(
                callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                client_id=client_id,
                protocol=mqtt.MQTTv5,
            )
        else:
            self._client = mqtt.Client(
                callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                client_id=client_id,
                protocol=mqtt.MQTTv311,   # Explicitly set v3.1.1
                clean_session=True,       # For MQTT v3.1.1
            )

        # Set credentials if provided
        if username is not None:
            self._client.username_pw_set(username=username, password=password)

        # Set up callbacks
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_publish = self._on_publish

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        """Callback for when the client receives a CONNACK from the broker."""
        if self.use_mqttv5:
            # MQTT v5 uses ReasonCodes objects
            if hasattr(reason_code, "is_failure"):
                self._connected = not reason_code.is_failure
                self._connect_rc = getattr(reason_code, "value", int(reason_code))
            else:
                self._connected = (reason_code == 0)
                self._connect_rc = reason_code
        else:
            self._connected = (reason_code == 0)
            self._connect_rc = reason_code

        if self._connected:
            print(f"[Connected] Successfully connected to {self.host}:{self.port}")
        else:
            print(f"[Connection Failed] reason_code={reason_code}")

        self._connect_event.set()

    def _on_disconnect(self, client, userdata, disconnect_flags, reason_code, properties):
        """Callback for when the client disconnects from the broker."""
        self._connected = False
        self._connect_event.clear()
        print(f"[Disconnected] reason_code={reason_code}")

    def _on_publish(self, client, userdata, mid, reason_code, properties):
        """Callback for when a message is published (VERSION2 API)."""
        # Hook available for logging/metrics; no state needed here.
        # reason_code is meaningful on MQTT v5; for v3.1.1 it's not used.
        pass

    def connect(
        self,
        session_expiry: Optional[int] = None,
        clean_start: Optional[bool] = None,
        timeout: float = 5.0,
    ) -> bool:
        """
        Connect to the MQTT broker.

        For MQTT v5:
        - session_expiry: Session expiry interval in seconds (0 = no persistence, None = omit property)
        - clean_start: Whether to start with a clean session; default True.
          True  -> MQTT_CLEAN_START_FIRST_ONLY (stateless)
          False -> MQTT_CLEAN_START_NO (resume existing session if any)

        For MQTT v3.1.1:
        - These parameters are ignored (clean_session was set in __init__)

        Returns True if connection successful, False otherwise.
        """
        self._connect_event.clear()

        try:
            if self.use_mqttv5:
                from paho.mqtt.properties import Properties
                from paho.mqtt.packettypes import PacketTypes

                props = Properties(PacketTypes.CONNECT)
                if session_expiry is not None:
                    props.SessionExpiryInterval = int(session_expiry)

                # Map boolean clean_start to Paho constants
                if clean_start is None:
                    clean_start_flag = mqtt.MQTT_CLEAN_START_FIRST_ONLY
                else:
                    clean_start_flag = (
                        mqtt.MQTT_CLEAN_START_FIRST_ONLY if clean_start
                        else mqtt.MQTT_CLEAN_START_NO
                    )

                self._client.connect(
                    self.host,
                    self.port,
                    keepalive=self.keepalive,
                    clean_start=clean_start_flag,
                    properties=props,
                )
            else:
                # MQTT v3.1.1 - clean_session already set in constructor
                self._client.connect(self.host, self.port, keepalive=self.keepalive)

            # Start the network loop
            self._client.loop_start()

            # Wait for connection to complete
            if not self._connect_event.wait(timeout):
                print(f"[Error] Connection timeout after {timeout} seconds")
                self._client.loop_stop()
                return False

            return self._connected

        except Exception as e:
            print(f"[Error] Connection failed: {e}")
            return False

    def disconnect(self) -> None:
        """Disconnect from the MQTT broker."""
        try:
            if self._connected:
                # Simple, clean disconnect; properties optional for v5
                self._client.disconnect()
            # Stop loop after disconnect to avoid races
            self._client.loop_stop()
        except Exception as e:
            print(f"[Error] Disconnect failed: {e}")
        finally:
            self._connected = False
            self._connect_event.clear()

    def publish_json(
        self,
        topic: str,
        data: Dict[str, Any],
        qos: int = 1,
        retain: bool = False,
        ensure_ascii: bool = False,
        timeout: float = 5.0,
        message_expiry: Optional[int] = None,
    ) -> bool:
        """
        Publish a JSON message to 'topic' (non-retained by default).

        Args:
            topic: MQTT topic to publish to
            data: Dictionary to be JSON encoded
            qos: Quality of Service level (0, 1, or 2)
            retain: Whether to retain the message on the broker
            ensure_ascii: Whether to escape non-ASCII characters in JSON
            timeout: Timeout for publish confirmation (seconds)
            message_expiry: MQTT v5 Message Expiry Interval (seconds). If set,
                            broker can drop stale jobs automatically.

        Returns:
            True if published successfully, False otherwise
        """
        if not self._connected:
            raise RuntimeError("Not connected. Call connect() first.")

        try:
            # Serialize to JSON
            payload = json.dumps(data, ensure_ascii=ensure_ascii, separators=(",", ":"))

            # Add properties for MQTT v5 if requested
            properties = None
            if self.use_mqttv5:
                from paho.mqtt.properties import Properties
                from paho.mqtt.packettypes import PacketTypes

                properties = Properties(PacketTypes.PUBLISH)
                properties.PayloadFormatIndicator = 1  # UTF-8 string
                properties.ContentType = "application/json"
                if message_expiry is not None:
                    properties.MessageExpiryInterval = int(message_expiry)

            # Publish the message
            msg_info = self._client.publish(
                topic,
                payload=payload,
                qos=qos,
                retain=retain,
                properties=properties if self.use_mqttv5 else None,
            )

            # Wait for publish to complete (for QoS > 0)
            if qos > 0:
                msg_info.wait_for_publish(timeout=timeout)
                if not msg_info.is_published():
                    print(f"[Error] Message not published within {timeout} seconds")
                    return False

            # Check return code
            if msg_info.rc != mqtt.MQTT_ERR_SUCCESS:
                print(f"[Error] Publish failed with rc={msg_info.rc}")
                return False

            return True

        except Exception as e:
            print(f"[Error] Failed to publish JSON: {e}")
            return False

    def publish_batch(
        self,
        topic: str,
        data_list: List[Dict[str, Any]],
        qos: int = 1,
        retain: bool = False,
        delay_between: float = 0.0,
        message_expiry: Optional[int] = None,
    ) -> Tuple[int, int]:
        """
        Publish multiple JSON messages in sequence.

        Args:
            topic: MQTT topic to publish to
            data_list: List of dictionaries to publish
            qos: Quality of Service level
            retain: Whether to retain messages
            delay_between: Delay between publishes (seconds)
            message_expiry: MQTT v5 Message Expiry Interval for each message

        Returns:
            Tuple of (successful_count, failed_count)
        """
        successful = 0
        failed = 0

        for i, data in enumerate(data_list):
            try:
                if self.publish_json(
                    topic,
                    data,
                    qos=qos,
                    retain=retain,
                    timeout=5.0,
                    message_expiry=message_expiry,
                ):
                    successful += 1
                    print(f"  ✓ Published message {i+1}/{len(data_list)}")
                else:
                    failed += 1
                    print(f"  ✗ Failed to publish message {i+1}/{len(data_list)}")

                if delay_between > 0 and i < len(data_list) - 1:
                    time.sleep(delay_between)

            except Exception as e:
                failed += 1
                print(f"  ✗ Error publishing message {i+1}: {e}")

        return successful, failed

    def is_connected(self) -> bool:
        """Check if the client is currently connected."""
        return self._connected


def main():
    """Test driver: publishes JSON messages to a work topic."""

    print("=== MQTT Queue Publisher Demo ===\n")

    # Create publisher instance
    qp = QueuePublisher(
        host="localhost",
        port=1883,
        client_id="queue-writer",
        use_mqttv5=False,  # Use v3.1.1 for compatibility
    )

    # Connect to broker
    print("Connecting to broker...")
    if not qp.connect(timeout=5.0):
        print("Failed to connect to broker. Is Mosquitto running?")
        return 1

    try:
        # Prepare test messages
        jobs = [
            {"job_id": 1, "op": "compress", "args": {"file": "a.bin"}},
            {"job_id": 2, "op": "resize", "args": {"image": "img.jpg", "w": 640, "h": 480}},
            {"job_id": 3, "op": "checksum", "args": {"file": "a.bin"}},
        ]

        # Publish messages individually
        print("\nPublishing individual messages:")
        for job in jobs:
            success = qp.publish_json("work/items/task", job, qos=1)
            if success:
                print(f"  ✓ Published job {job['job_id']}: {job['op']}")
            else:
                print(f"  ✗ Failed to publish job {job['job_id']}")
            time.sleep(0.1)

        # Test batch publishing
        print("\nTesting batch publish:")
        batch_jobs = [
            {"job_id": 101, "op": "backup", "args": {"path": "/data"}},
            {"job_id": 102, "op": "scan", "args": {"target": "network"}},
            {"job_id": 103, "op": "report", "args": {"format": "pdf"}},
        ]

        successful, failed = qp.publish_batch(
            "work/items/task",
            batch_jobs,
            qos=1,
            delay_between=0.05,
        )

        print(f"\nBatch results: {successful} successful, {failed} failed")

        # Test with QoS 2 (exactly once)
        print("\nTesting QoS 2 publish:")
        critical_job = {"job_id": 999, "op": "critical_update", "args": {"target": "database"}}
        if qp.publish_json("work/items/critical", critical_job, qos=2):
            print("  ✓ Critical job published with QoS 2")

        print(f"\n✓ Successfully published all test messages to broker")

    except Exception as e:
        print(f"\n✗ Error during publishing: {e}")
        return 1

    finally:
        print("\nDisconnecting...")
        qp.disconnect()
        print("✓ Demo completed!")

    return 0


if __name__ == "__main__":
    exit(main())
