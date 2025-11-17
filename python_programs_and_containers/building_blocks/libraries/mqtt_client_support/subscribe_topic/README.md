I've created the TopicSubscriber class following the same pattern as KVStoreReader and KVStoreWriter. Here are the key features:
Core Features:

Callback-based message handling - Register functions to be called when messages arrive
Pattern matching - Support for MQTT wildcards (+ and #)
Multiple callbacks per topic - Can register multiple callbacks for the same pattern
Auto-reconnection - Automatically reconnects and resubscribes if connection is lost
Statistics tracking - Tracks messages received, callbacks executed, and errors

Main Methods:
Subscription Management:

subscribe(topic, callback, qos) - Subscribe to a topic with a callback
subscribe_many(subscriptions) - Subscribe to multiple topics at once
unsubscribe(topic) - Unsubscribe from a topic
unsubscribe_all() - Unsubscribe from all topics

Callback Management:

add_callback(topic, callback) - Add additional callback to existing subscription
remove_callback(topic, callback) - Remove a specific callback

Utilities:

wait_for_messages(timeout) - Block and wait for messages
get_subscriptions() - Get current subscriptions
get_statistics() - Get subscriber statistics

MessageInfo Data Class:
Each callback receives a MessageInfo object containing:

topic - The MQTT topic
payload - Decoded string payload
raw_payload - Original bytes payload
qos - Quality of Service level
retain - Whether message was retained
timestamp - When message was received

Key Design Features:

Thread-safe operations - All callback and subscription management is thread-safe
Automatic resubscription - After reconnection, automatically resubscribes to all topics
Error isolation - Callback errors don't affect other callbacks or the subscriber
Flexible callback management - Add/remove callbacks dynamically
Pattern matching - Uses MQTT's built-in topic matching for wildcards

Example Usage:
python# Create subscriber
subscriber = TopicSubscriber(host="localhost", auto_reconnect=True)
subscriber.connect()

# Define callbacks
def on_temperature(msg):
    print(f"Temperature: {msg.payload}°C")

def on_status(msg):
    print(f"Status update: {msg.topic} = {msg.payload}")

# Subscribe to topics
subscriber.subscribe("sensors/+/temperature", on_temperature)
subscriber.subscribe("system/status/#", on_status)

# Wait for messages
subscriber.wait_for_messages(timeout=60)  # Wait 60 seconds

# Or run indefinitely
# subscriber.wait_for_messages()  # Ctrl+C to stop

# Cleanup
subscriber.disconnect()
This class provides a robust, production-ready MQTT subscriber with comprehensive callback management, perfect for building event-driven applications, monitoring systems, or IoT data collectors.

