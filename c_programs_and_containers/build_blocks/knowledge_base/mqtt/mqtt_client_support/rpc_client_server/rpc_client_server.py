import json
import uuid
import time
import threading
from typing import Callable, Dict, Any, Optional
from datetime import datetime
import paho.mqtt.client as mqtt


class MQTTRPCServer:
    """MQTT RPC Server for handling remote procedure calls over MQTT"""

    def __init__(
        self,
        broker_host: str,
        broker_port: int = 1883,
        client_id: Optional[str] = None,
        service_name: str = "rpc_service",
        username: Optional[str] = None,
        password: Optional[str] = None,
        qos: int = 1,
    ):
        """
        Args:
            broker_host: MQTT broker hostname
            broker_port: MQTT broker port
            client_id: Unique client identifier
            service_name: Name of the RPC service (used in topic structure)
            username/password: (optional) broker auth
            qos: subscription/publish QoS (default 1)
        """
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.client_id = client_id or f"rpc_server_{uuid.uuid4().hex[:8]}"
        self.service_name = service_name
        self.qos = qos

        # Topic patterns
        self.request_topic = f"rpc/{service_name}/request/+"
        self.response_topic_base = f"rpc/{service_name}/response"

        # Registered methods
        self.methods: Dict[str, Callable] = {}

        # MQTT client (Paho v2 API + MQTT 3.1.1)
        self.client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=self.client_id,
            protocol=mqtt.MQTTv311,
            clean_session=True,
        )
        if username is not None:
            self.client.username_pw_set(username=username, password=password)

        # Callbacks (VERSION2 signatures)
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.on_disconnect = self._on_disconnect
        self.client.on_subscribe = self._on_subscribe

        self.connected = False
        self._stop_flag = False
        self._subscribed = threading.Event()

    # ---- callbacks ----
    def _on_connect(self, client, userdata, flags, reason_code, properties):
        """VERSION2 connect callback: (client, userdata, flags, reason_code, properties)"""
        ok = (reason_code == 0) if not hasattr(reason_code, "is_failure") else not reason_code.is_failure
        self.connected = ok
        print(f"[Server on_connect] rc={reason_code}, connected={ok}")
        if ok:
            # Subscribe to request topic
            res, mid = client.subscribe(self.request_topic, qos=self.qos)
            if res != mqtt.MQTT_ERR_SUCCESS:
                print(f"[Server] subscribe error: {res}")

    def _on_subscribe(self, client, userdata, mid, reason_codes, properties):
        print(f"[Server on_subscribe] mid={mid}, reason_codes={reason_codes}")
        self._subscribed.set()

    def _on_disconnect(self, client, userdata, disconnect_flags, reason_code, properties):
        """VERSION2 disconnect callback"""
        print(f"[Server on_disconnect] rc={reason_code}")
        self.connected = False

    def _on_message(self, client, userdata, msg):
        """Handle incoming RPC requests (request topic: rpc/{service}/request/{client_id})"""
        try:
            parts = msg.topic.split("/")
            if len(parts) < 4:
                return
            client_id = parts[3]

            request = json.loads(msg.payload.decode("utf-8"))
            threading.Thread(
                target=self._process_request,
                args=(request, client_id),
                daemon=True,
            ).start()
        except Exception as e:
            print(f"[Server] Error processing message: {e}")

    # ---- core ----
    def _process_request(self, request: Dict[str, Any], client_id: str):
        """Process RPC request and send response to rpc/{service}/response/{client_id}"""
        response = {"id": request.get("id"), "jsonrpc": "2.0"}

        try:
            # Basic JSON-RPC 2.0 shape check
            if request.get("jsonrpc") != "2.0" or "method" not in request:
                response["error"] = {"code": -32600, "message": "Invalid Request"}
            else:
                method_name = request["method"]
                params = request.get("params", {})

                if method_name not in self.methods:
                    response["error"] = {"code": -32601, "message": f"Method '{method_name}' not found"}
                else:
                    method = self.methods[method_name]
                    if isinstance(params, dict):
                        result = method(**params)
                    elif isinstance(params, list):
                        result = method(*params)
                    else:
                        result = method()
                    response["result"] = result
        except Exception as e:
            response["error"] = {"code": -32603, "message": f"Internal error: {e}"}

        # Publish response (non-retained)
        response_topic = f"{self.response_topic_base}/{client_id}"
        self.client.publish(response_topic, json.dumps(response), qos=self.qos, retain=False)

    # ---- public API ----
    def register_method(self, name: str, method: Callable):
        self.methods[name] = method
        print(f"[Server] Registered method: {name}")

    def register_methods(self, methods: Dict[str, Callable]):
        for name, method in methods.items():
            self.register_method(name, method)

    def start(self, wait_for_subscribe: bool = True, sub_timeout: float = 3.0):
        print(f"[Server] Starting RPC Server '{self.service_name}'...")
        self._subscribed.clear()
        self.client.connect(self.broker_host, self.broker_port, keepalive=60)
        self.client.loop_start()
        if wait_for_subscribe and not self._subscribed.wait(sub_timeout):
            print("[Server] Warning: SUBACK not received yet; requests may be missed until it arrives")

    def stop(self):
        print("[Server] Stopping RPC Server...")
        self._stop_flag = True
        try:
            self.client.disconnect()
        finally:
            self.client.loop_stop()

    def wait(self):
        try:
            while not self._stop_flag:
                time.sleep(0.25)
        except KeyboardInterrupt:
            self.stop()


class MQTTRPCClient:
    """MQTT RPC Client for making remote procedure calls over MQTT"""

    def __init__(
        self,
        broker_host: str,
        broker_port: int = 1883,
        client_id: Optional[str] = None,
        service_name: str = "rpc_service",
        timeout: float = 30.0,
        username: Optional[str] = None,
        password: Optional[str] = None,
        qos: int = 1,
    ):
        """
        Args:
            broker_host/broker_port: broker location
            client_id: unique client id (auto if None)
            service_name: RPC service namespace
            timeout: default call timeout (seconds)
            username/password: (optional) broker auth
            qos: subscription/publish QoS (default 1)
        """
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.client_id = client_id or f"rpc_client_{uuid.uuid4().hex[:8]}"
        self.service_name = service_name
        self.timeout = timeout
        self.qos = qos

        # Topics
        self.request_topic = f"rpc/{service_name}/request/{self.client_id}"
        self.response_topic = f"rpc/{service_name}/response/{self.client_id}"

        # Pending requests
        self.pending_requests: Dict[str, threading.Event] = {}
        self.responses: Dict[str, Any] = {}
        self._lock = threading.Lock()

        # MQTT client
        self.client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=self.client_id,
            protocol=mqtt.MQTTv311,
            clean_session=True,
        )
        if username is not None:
            self.client.username_pw_set(username=username, password=password)

        # Callbacks
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.on_disconnect = self._on_disconnect
        self.client.on_subscribe = self._on_subscribe

        self.connected = False
        self._connect_event = threading.Event()
        self._subscribed = threading.Event()

        # For async id return
        self._request_counter = 0

    # ---- callbacks ----
    def _on_connect(self, client, userdata, flags, reason_code, properties):
        ok = (reason_code == 0) if not hasattr(reason_code, "is_failure") else not reason_code.is_failure
        self.connected = ok
        print(f"[Client on_connect] rc={reason_code}, connected={ok}")
        if ok:
            res, mid = client.subscribe(self.response_topic, qos=self.qos)
            if res != mqtt.MQTT_ERR_SUCCESS:
                print(f"[Client] subscribe error: {res}")

        self._connect_event.set()

    def _on_subscribe(self, client, userdata, mid, reason_codes, properties):
        print(f"[Client on_subscribe] mid={mid}, reason_codes={reason_codes}")
        self._subscribed.set()

    def _on_disconnect(self, client, userdata, disconnect_flags, reason_code, properties):
        print(f"[Client on_disconnect] rc={reason_code}")
        self.connected = False
        self._connect_event.clear()

    def _on_message(self, client, userdata, msg):
        """Handle RPC responses"""
        try:
            response = json.loads(msg.payload.decode("utf-8"))
            request_id = response.get("id")
            if not request_id:
                return
            with self._lock:
                ev = self.pending_requests.get(request_id)
                if ev is not None:
                    self.responses[request_id] = response
                    ev.set()
        except Exception as e:
            print(f"[Client] Error processing response: {e}")

    # ---- ops ----
    def connect(self, timeout: float = 5.0, wait_for_suback: bool = True) -> None:
        print(f"[Client] Connecting to {self.broker_host}:{self.broker_port} ...")
        self._connect_event.clear()
        self._subscribed.clear()
        self.client.connect(self.broker_host, self.broker_port, keepalive=60)
        self.client.loop_start()

        if not self._connect_event.wait(timeout):
            self.client.loop_stop()
            raise ConnectionError("Timeout waiting for MQTT connection")

        if wait_for_suback and not self._subscribed.wait(timeout):
            print("[Client] Warning: SUBACK not received yet; responses may be missed until it arrives")

    def disconnect(self) -> None:
        print("[Client] Disconnecting ...")
        try:
            self.client.disconnect()
        finally:
            self.client.loop_stop()

    def call(self, method: str, params: Any = None, timeout: Optional[float] = None) -> Any:
        """
        Make a synchronous RPC call.
        Raises TimeoutError on timeout, Exception on JSON-RPC error.
        """
        if not self.connected:
            raise ConnectionError("Not connected to broker")

        with self._lock:
            self._request_counter += 1
            request_id = f"{self.client_id}_{self._request_counter}"

        request = {"jsonrpc": "2.0", "method": method, "id": request_id}
        if params is not None:
            request["params"] = params

        ev = threading.Event()
        with self._lock:
            self.pending_requests[request_id] = ev

        # Publish request (non-retained)
        info = self.client.publish(self.request_topic, json.dumps(request), qos=self.qos, retain=False)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            with self._lock:
                self.pending_requests.pop(request_id, None)
            raise RuntimeError(f"Publish failed: rc={info.rc}")

        # Wait for response
        wait_s = timeout if timeout is not None else self.timeout
        if not ev.wait(wait_s):
            with self._lock:
                self.pending_requests.pop(request_id, None)
                self.responses.pop(request_id, None)
            raise TimeoutError(f"RPC call '{method}' timed out after {wait_s}s")

        with self._lock:
            response = self.responses.pop(request_id, None)
            self.pending_requests.pop(request_id, None)

        if not response:
            raise RuntimeError("No response received")

        if "error" in response:
            err = response["error"]
            raise Exception(f"RPC Error {err.get('code')}: {err.get('message')}")

        return response.get("result")

    def call_async(self, method: str, params: Any = None, callback: Optional[Callable] = None) -> str:
        """
        Fire-and-forget async call. Callback is invoked as callback(error, result).
        Returns the request id prefix for reference.
        """
        def _runner():
            try:
                result = self.call(method, params)
                if callback:
                    callback(None, result)
            except Exception as e:
                if callback:
                    callback(e, None)

        with self._lock:
            next_id = self._request_counter + 1
        t = threading.Thread(target=_runner, daemon=True)
        t.start()
        return f"{self.client_id}_{next_id}"


# Example usage
if __name__ == "__main__":
    # Example server methods
    def add(a, b): return a + b
    def multiply(a, b): return a * b
    def get_server_time(): return datetime.now().isoformat()
    def echo(message): return f"Echo: {message}"

    # Server
    server = MQTTRPCServer(
        broker_host="localhost",
        broker_port=1883,
        service_name="math_service",
    )
    server.register_methods({
        "add": add,
        "multiply": multiply,
        "get_time": get_server_time,
        "echo": echo,
    })
    server.start()

    # Client (normally separate process)
    client = MQTTRPCClient(
        broker_host="localhost",
        broker_port=1883,
        service_name="math_service",
        timeout=10.0,
    )

    try:
        client.connect()
        time.sleep(0.5)  # small settle

        print(f"5 + 3 = {client.call('add', {'a': 5, 'b': 3})}")
        print(f"4 * 7 = {client.call('multiply', [4, 7])}")
        print(f"Server time: {client.call('get_time')}")
        print(client.call('echo', {'message': 'Hello RPC!'}))

        def handle_result(err, res):
            print("Async error:" if err else "Async result:", err or res)

        client.call_async("add", {"a": 10, "b": 20}, handle_result)
        time.sleep(1.5)

    finally:
        client.disconnect()
        server.stop()
