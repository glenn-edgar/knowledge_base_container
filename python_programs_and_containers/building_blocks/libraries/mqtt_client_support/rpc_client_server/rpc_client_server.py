import json
import uuid
import time
import threading
from typing import Callable, Dict, Any, Optional
from datetime import datetime, timedelta
import paho.mqtt.client as mqtt


class MQTTRPCServer:
    """MQTT RPC Server for handling remote procedure calls over MQTT"""
    
    def __init__(self, broker_host: str, broker_port: int = 1883, 
                 client_id: Optional[str] = None, 
                 service_name: str = "rpc_service"):
        """
        Initialize MQTT RPC Server
        
        Args:
            broker_host: MQTT broker hostname
            broker_port: MQTT broker port
            client_id: Unique client identifier
            service_name: Name of the RPC service (used in topic structure)
        """
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.client_id = client_id or f"rpc_server_{uuid.uuid4().hex[:8]}"
        self.service_name = service_name
        
        # Topic patterns
        self.request_topic = f"rpc/{service_name}/request/+"
        self.response_topic_base = f"rpc/{service_name}/response"
        
        # Registered methods
        self.methods: Dict[str, Callable] = {}
        
        # MQTT client setup
        self.client = mqtt.Client(client_id=self.client_id)
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.on_disconnect = self._on_disconnect
        
        self.connected = False
        self._stop_flag = False
        
    def _on_connect(self, client, userdata, flags, rc):
        """Handle MQTT connection"""
        if rc == 0:
            print(f"RPC Server connected to broker at {self.broker_host}:{self.broker_port}")
            self.connected = True
            # Subscribe to request topic
            client.subscribe(self.request_topic)
            print(f"Subscribed to {self.request_topic}")
        else:
            print(f"Failed to connect, return code: {rc}")
            
    def _on_disconnect(self, client, userdata, rc):
        """Handle MQTT disconnection"""
        self.connected = False
        if rc != 0:
            print(f"Unexpected disconnection (rc: {rc})")
            
    def _on_message(self, client, userdata, msg):
        """Handle incoming RPC requests"""
        try:
            # Extract client_id from topic
            topic_parts = msg.topic.split('/')
            if len(topic_parts) < 4:
                return
                
            client_id = topic_parts[3]
            
            # Parse request
            request = json.loads(msg.payload.decode('utf-8'))
            
            # Process request in thread to avoid blocking
            threading.Thread(
                target=self._process_request,
                args=(request, client_id),
                daemon=True
            ).start()
            
        except Exception as e:
            print(f"Error processing message: {e}")
            
    def _process_request(self, request: Dict[str, Any], client_id: str):
        """Process RPC request and send response"""
        response = {
            "id": request.get("id"),
            "jsonrpc": "2.0"
        }
        
        try:
            method_name = request.get("method")
            params = request.get("params", {})
            
            if method_name not in self.methods:
                response["error"] = {
                    "code": -32601,
                    "message": f"Method '{method_name}' not found"
                }
            else:
                # Execute method
                method = self.methods[method_name]
                if isinstance(params, dict):
                    result = method(**params)
                elif isinstance(params, list):
                    result = method(*params)
                else:
                    result = method()
                    
                response["result"] = result
                
        except Exception as e:
            response["error"] = {
                "code": -32603,
                "message": f"Internal error: {str(e)}"
            }
            
        # Send response
        response_topic = f"{self.response_topic_base}/{client_id}"
        self.client.publish(
            response_topic,
            json.dumps(response),
            qos=1
        )
        
    def register_method(self, name: str, method: Callable):
        """
        Register a method for RPC calls
        
        Args:
            name: Method name for RPC calls
            method: Callable to execute
        """
        self.methods[name] = method
        print(f"Registered method: {name}")
        
    def register_methods(self, methods: Dict[str, Callable]):
        """Register multiple methods at once"""
        for name, method in methods.items():
            self.register_method(name, method)
            
    def start(self):
        """Start the RPC server"""
        print(f"Starting RPC Server '{self.service_name}'...")
        self.client.connect(self.broker_host, self.broker_port, 60)
        self.client.loop_start()
        
    def stop(self):
        """Stop the RPC server"""
        print("Stopping RPC Server...")
        self._stop_flag = True
        self.client.loop_stop()
        self.client.disconnect()
        
    def wait(self):
        """Keep server running until stopped"""
        try:
            while not self._stop_flag:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()


class MQTTRPCClient:
    """MQTT RPC Client for making remote procedure calls over MQTT"""
    
    def __init__(self, broker_host: str, broker_port: int = 1883,
                 client_id: Optional[str] = None,
                 service_name: str = "rpc_service",
                 timeout: float = 30.0):
        """
        Initialize MQTT RPC Client
        
        Args:
            broker_host: MQTT broker hostname
            broker_port: MQTT broker port
            client_id: Unique client identifier
            service_name: Name of the RPC service to connect to
            timeout: Default timeout for RPC calls in seconds
        """
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.client_id = client_id or f"rpc_client_{uuid.uuid4().hex[:8]}"
        self.service_name = service_name
        self.timeout = timeout
        
        # Topic patterns
        self.request_topic = f"rpc/{service_name}/request/{self.client_id}"
        self.response_topic = f"rpc/{service_name}/response/{self.client_id}"
        
        # Pending requests
        self.pending_requests: Dict[str, threading.Event] = {}
        self.responses: Dict[str, Any] = {}
        self._lock = threading.Lock()
        
        # MQTT client setup
        self.client = mqtt.Client(client_id=self.client_id)
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.on_disconnect = self._on_disconnect
        
        self.connected = False
        self._request_counter = 0
        
    def _on_connect(self, client, userdata, flags, rc):
        """Handle MQTT connection"""
        if rc == 0:
            print(f"RPC Client connected to broker at {self.broker_host}:{self.broker_port}")
            self.connected = True
            # Subscribe to response topic
            client.subscribe(self.response_topic)
            print(f"Subscribed to {self.response_topic}")
        else:
            print(f"Failed to connect, return code: {rc}")
            
    def _on_disconnect(self, client, userdata, rc):
        """Handle MQTT disconnection"""
        self.connected = False
        if rc != 0:
            print(f"Unexpected disconnection (rc: {rc})")
            
    def _on_message(self, client, userdata, msg):
        """Handle RPC responses"""
        try:
            response = json.loads(msg.payload.decode('utf-8'))
            request_id = response.get("id")
            
            if request_id and request_id in self.pending_requests:
                with self._lock:
                    self.responses[request_id] = response
                    event = self.pending_requests.get(request_id)
                    if event:
                        event.set()
                        
        except Exception as e:
            print(f"Error processing response: {e}")
            
    def connect(self):
        """Connect to MQTT broker"""
        print(f"Connecting RPC Client to {self.broker_host}:{self.broker_port}...")
        self.client.connect(self.broker_host, self.broker_port, 60)
        self.client.loop_start()
        
        # Wait for connection
        timeout = time.time() + 5
        while not self.connected and time.time() < timeout:
            time.sleep(0.1)
            
        if not self.connected:
            raise ConnectionError("Failed to connect to MQTT broker")
            
    def disconnect(self):
        """Disconnect from MQTT broker"""
        print("Disconnecting RPC Client...")
        self.client.loop_stop()
        self.client.disconnect()
        
    def call(self, method: str, params: Any = None, timeout: Optional[float] = None) -> Any:
        """
        Make an RPC call
        
        Args:
            method: Method name to call
            params: Method parameters (dict or list)
            timeout: Timeout for this call (uses default if None)
            
        Returns:
            Result from RPC call
            
        Raises:
            TimeoutError: If call times out
            Exception: If RPC call returns an error
        """
        if not self.connected:
            raise ConnectionError("Not connected to broker")
            
        # Generate request ID
        with self._lock:
            self._request_counter += 1
            request_id = f"{self.client_id}_{self._request_counter}"
            
        # Prepare request
        request = {
            "jsonrpc": "2.0",
            "method": method,
            "id": request_id
        }
        if params is not None:
            request["params"] = params
            
        # Setup response handling
        event = threading.Event()
        with self._lock:
            self.pending_requests[request_id] = event
            
        # Send request
        self.client.publish(
            self.request_topic,
            json.dumps(request),
            qos=1
        )
        
        # Wait for response
        call_timeout = timeout or self.timeout
        if not event.wait(call_timeout):
            with self._lock:
                self.pending_requests.pop(request_id, None)
            raise TimeoutError(f"RPC call '{method}' timed out after {call_timeout}s")
            
        # Get response
        with self._lock:
            response = self.responses.pop(request_id, None)
            self.pending_requests.pop(request_id, None)
            
        if not response:
            raise Exception("No response received")
            
        # Check for error
        if "error" in response:
            error = response["error"]
            raise Exception(f"RPC Error: {error.get('message', 'Unknown error')}")
            
        return response.get("result")
        
    def call_async(self, method: str, params: Any = None, 
                   callback: Optional[Callable] = None) -> str:
        """
        Make an asynchronous RPC call
        
        Args:
            method: Method name to call
            params: Method parameters
            callback: Optional callback function for result
            
        Returns:
            Request ID for tracking
        """
        def _async_call():
            try:
                result = self.call(method, params)
                if callback:
                    callback(None, result)
            except Exception as e:
                if callback:
                    callback(e, None)
                    
        thread = threading.Thread(target=_async_call, daemon=True)
        thread.start()
        
        return f"async_{self._request_counter}"


# Example usage
if __name__ == "__main__":
    # Example server methods
    def add(a, b):
        return a + b
    
    def multiply(a, b):
        return a * b
    
    def get_server_time():
        return datetime.now().isoformat()
    
    def echo(message):
        return f"Echo: {message}"
    
    # Server setup
    server = MQTTRPCServer(
        broker_host="localhost",
        broker_port=1883,
        service_name="math_service"
    )
    
    # Register methods
    server.register_methods({
        "add": add,
        "multiply": multiply,
        "get_time": get_server_time,
        "echo": echo
    })
    
    # Start server
    server.start()
    
    # Client example (in practice, this would be in a separate process)
    client = MQTTRPCClient(
        broker_host="localhost",
        broker_port=1883,
        service_name="math_service"
    )
    
    try:
        # Connect client
        client.connect()
        time.sleep(1)  # Allow connection to establish
        
        # Make RPC calls
        result = client.call("add", {"a": 5, "b": 3})
        print(f"5 + 3 = {result}")
        
        result = client.call("multiply", [4, 7])
        print(f"4 * 7 = {result}")
        
        result = client.call("get_time")
        print(f"Server time: {result}")
        
        result = client.call("echo", {"message": "Hello RPC!"})
        print(f"Echo result: {result}")
        
        # Async call example
        def handle_result(error, result):
            if error:
                print(f"Async error: {error}")
            else:
                print(f"Async result: {result}")
                
        client.call_async("add", {"a": 10, "b": 20}, handle_result)
        
        # Keep running
        time.sleep(2)
        
    finally:
        client.disconnect()
        server.stop()


