"""
QueueReader - reliable queued message demo using MQTT v3.1.1 (clean_session=False)
Broker: localhost:1883 (Mosquitto)
Requires: paho-mqtt >= 1.6 (works with 2.x too)
    pip install paho-mqtt
"""

import time
import threading
from typing import Optional, List, Tuple
import paho.mqtt.client as mqtt
import json


class QueueReader:
    def __init__(
        self,
        host: str = "localhost",
        port: int = 1883,
        client_id: str = "queue-reader",
        keepalive: int = 60,
        username: Optional[str] = None,
        password: Optional[str] = None,
        clean_session: bool = False,   # <- v3.1.1 persistent session
    ) -> None:
        self.host = host
        self.port = port
        self.keepalive = keepalive
        self._messages: List[Tuple[str, str]] = []
        self._lock = threading.Lock()  # Thread safety for message list

        # Use Callback API v2 to avoid deprecation warnings
        self._client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,  # Fixed parameter name
            client_id=client_id,
            clean_session=clean_session,   # v3.1.1 persistence knob
        )
        if username is not None:
            self._client.username_pw_set(username=username, password=password)

        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message
        self._client.on_subscribe = self._on_subscribe
        self._client.on_disconnect = self._on_disconnect

        self._connected = False
        self._connect_event = threading.Event()
        self._suback_event = threading.Event()
        self._suback_pending = 0
        self._session_present = False

    # ---- callbacks ----
    def _on_connect(self, client, userdata, flags, reason_code, properties):
        # For CallbackAPIVersion.VERSION2 with MQTT v3.1.1/v5
        if hasattr(reason_code, 'is_failure'):  # MQTT v5
            self._connected = not reason_code.is_failure
            session_present = flags.session_present if hasattr(flags, 'session_present') else False
        else:  # MQTT v3.1.1
            self._connected = (reason_code == 0)
            session_present = flags.get('session present', False) if isinstance(flags, dict) else False
        
        self._session_present = session_present
        print(f"[on_connect] reason_code={reason_code}, session_present={session_present}")
        self._connect_event.set()

    def _on_disconnect(self, client, userdata, disconnect_flags, reason_code, properties):
        # Updated for CallbackAPIVersion.VERSION2 - now has 6 parameters total
        print(f"[on_disconnect] reason_code={reason_code}")
        self._connected = False
        self._connect_event.clear()

    def _on_message(self, client, userdata, msg):
        payload = msg.payload.decode(errors="replace")
        with self._lock:
            self._messages.append((msg.topic, payload))

    def _on_subscribe(self, client, userdata, mid, reason_codes, properties):
        # Updated for CallbackAPIVersion.VERSION2
        print(f"[on_subscribe] mid={mid}, reason_codes={reason_codes}")
        self._suback_pending -= 1
        if self._suback_pending <= 0:
            self._suback_event.set()

    # ---- ops ----
    def connect(self, timeout: float = 5.0) -> bool:
        """Connect to broker and wait for CONNACK."""
        self._connect_event.clear()
        try:
            self._client.connect(self.host, self.port, keepalive=self.keepalive)
            self._client.loop_start()
            # Wait for connection to be established
            if not self._connect_event.wait(timeout):
                print(f"[connect] Timeout waiting for connection")
                return False
            return self._connected
        except Exception as e:
            print(f"[connect] Error: {e}")
            return False

    def disconnect(self) -> None:
        """Disconnect from broker and stop loop."""
        try:
            self._client.loop_stop()
            self._client.disconnect()
        except Exception as e:
            print(f"[disconnect] Error: {e}")
        finally:
            self._connected = False
            self._connect_event.clear()

    def ensure_subscription(self, topic: str, qos: int = 1, timeout: float = 2.0) -> bool:
        """Subscribe and block until SUBACK or timeout. Returns True on success."""
        if not self._connected:
            print("[ensure_subscription] Not connected")
            return False
        
        self._suback_event.clear()
        self._suback_pending += 1
        
        try:
            result, mid = self._client.subscribe(topic, qos=qos)
            if result != mqtt.MQTT_ERR_SUCCESS:
                self._suback_pending -= 1
                print(f"[ensure_subscription] subscribe failed with result={result}")
                return False
            
            # Wait for SUBACK
            if not self._suback_event.wait(timeout):
                print("[ensure_subscription] timeout waiting for SUBACK")
                self._suback_pending -= 1
                return False
            
            return True
        except Exception as e:
            print(f"[ensure_subscription] Error: {e}")
            self._suback_pending -= 1
            return False

    def read_queue(self, topic: str, qos: int = 1, timeout: float = 5.0) -> List[Tuple[str, str]]:
        """Subscribe (or resubscribe) and collect messages for up to timeout seconds."""
        with self._lock:
            self._messages.clear()
        
        # Only subscribe if we don't have a persistent session or if explicitly needed
        if not self._session_present:
            if not self.ensure_subscription(topic, qos=qos, timeout=2.0):
                raise RuntimeError("SUBACK not received; subscription not active")
        else:
            print(f"[read_queue] Using existing subscription from persistent session")
        
        # Collect messages for the specified timeout
        time.sleep(timeout)
        
        with self._lock:
            return list(self._messages)

    def is_connected(self) -> bool:
        """Check if client is currently connected."""
        return self._connected

    def clear_messages(self) -> None:
        """Clear the message buffer."""
        with self._lock:
            self._messages.clear()


def main():
    """Demo showing persistent session with queued messages."""
    TOPIC = "work/items/task"
    BROKER = "localhost"
    PORT = 1883
    CLIENT_ID = "worker-1"

    print("=== MQTT Queue Reader Demo ===\n")

    # --- 1) First connect: create persistent session and REGISTER subscription
    print("1. Creating persistent session and registering subscription...")
    reader = QueueReader(
        host=BROKER, 
        port=PORT,
        client_id=CLIENT_ID,          # FIXED client id is crucial
        clean_session=False,           # v3.1.1 persistent session
    )
    
    if not reader.connect():
        raise SystemExit("Failed to connect to broker")
    
    try:
        if not reader.ensure_subscription(TOPIC, qos=1, timeout=2.0):
            raise SystemExit("Failed to SUBSCRIBE (no SUBACK). Check broker/ACLs.")
        print("✓ Subscription registered in persistent session")
    finally:
        reader.disconnect()
        print("✓ Disconnected (session persisted)\n")

    # --- 2) While reader is offline, publish QoS1 jobs (these should be queued)
    print("2. Publishing messages while consumer is offline...")
    pub = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id="queue-publisher",
        clean_session=True
    )
    
    try:
        pub.connect(BROKER, PORT, keepalive=60)
        pub.loop_start()
        time.sleep(0.5)  # Give publisher time to connect
        
        jobs = [
            {"job_id": 301, "op": "compress", "args": {"file": "a.bin"}},
            {"job_id": 302, "op": "resize",   "args": {"image": "img.jpg", "w": 640, "h": 480}},
            {"job_id": 303, "op": "checksum", "args": {"file": "a.bin"}},
        ]
        
        for j in jobs:
            payload = json.dumps(j, separators=(",", ":"))
            info = pub.publish(TOPIC, payload, qos=1, retain=False)
            info.wait_for_publish()
            print(f"  ✓ Published job {j['job_id']}")
        
        time.sleep(0.5)  # Ensure all messages are sent
        print(f"✓ Published {len(jobs)} jobs while consumer offline\n")
    finally:
        pub.loop_stop()
        pub.disconnect()

    # --- 3) Reconnect SAME client id with clean_session=False to resume & drain
    print("3. Reconnecting to retrieve queued messages...")
    reader = QueueReader(
        host=BROKER,
        port=PORT,
        client_id=CLIENT_ID,          # SAME client id
        clean_session=False,           # must be False to RESUME session
    )
    
    if not reader.connect():
        raise SystemExit("Failed to reconnect to broker")
    
    try:
        msgs = reader.read_queue(TOPIC, qos=1, timeout=3.0)
        print(f"\n✓ Retrieved {len(msgs)} queued messages:")
        for t, p in msgs:
            try:
                job = json.loads(p)
                print(f"  Topic: {t}")
                print(f"    Job ID: {job.get('job_id')}")
                print(f"    Operation: {job.get('op')}")
                print(f"    Args: {job.get('args')}")
            except json.JSONDecodeError:
                print(f"  Topic: {t} => {p}")
    finally:
        reader.disconnect()
        print("\n✓ Demo completed successfully!")


if __name__ == "__main__":
    main()