import paho.mqtt.client as mqtt
import socket
import time
import os

# Configuration
BROKER = "localhost"  # Try "host.docker.internal" or container IP (e.g., 172.17.0.2) if localhost fails
#BROKER = "192.168.1.158"
PORT = 1883          # Standard MQTT port
TOPIC = "my/test/topic"
MESSAGE = "Hello, MQTT!"
OUTPUT_FILE = "mqtt_output.txt"

# Check if broker is reachable
def check_connectivity(host, port):
    print("Checking broker connectivity...")
    try:
        sock = socket.create_connection((host, port), timeout=5)
        sock.close()
        print(f"{host} [{host}] {port} open")
        return True
    except (socket.timeout, socket.gaierror, ConnectionRefusedError) as e:
        print(f"Error: Cannot connect to broker at {host}:{port}. Ensure the Mosquitto container is running and port {port} is mapped.")
        print("Try setting BROKER to 'host.docker.internal' or the container IP (check with 'docker inspect mosquitto').")
        return False

# Callback for when the subscriber connects
def on_connect_sub(client, userdata, flags, reason_code, properties):
    print("Starting subscriber...")
    print("Subscriber connected with result code", reason_code)
    client.subscribe(TOPIC, qos=0)

# Callback for when a message is received
def on_message(client, userdata, msg):
    print("Subscriber output:")
    print(f"{msg.topic} {msg.payload.decode()}")
    with open(userdata["output_file"], "a") as f:
        f.write(f"{msg.topic} {msg.payload.decode()}\n")
    client.disconnect()

# Main function to run the test
def main():
    # Check broker connectivity
    if not check_connectivity(BROKER, PORT):
        return

    # Start subscriber
    sub_client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        protocol=mqtt.MQTTv5
    )
    sub_client.on_connect = on_connect_sub
    sub_client.on_message = on_message
    sub_client.user_data_set({"output_file": OUTPUT_FILE})

    try:
        sub_client.connect(BROKER, PORT, 60)
    except Exception as e:
        print(f"Error: Subscriber failed to connect: {e}")
        return

    # Start subscriber in a background thread
    sub_client.loop_start()

    # Wait to ensure the subscriber is connected
    time.sleep(5)

    # Publish a test message
    print(f"Publishing message: {MESSAGE}")
    pub_client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        protocol=mqtt.MQTTv5
    )
    try:
        pub_client.connect(BROKER, PORT, 60)
        pub_client.publish(TOPIC, MESSAGE, qos=0)
        pub_client.disconnect()
    except Exception as e:
        print(f"Error: Publisher failed to connect or publish: {e}")
        sub_client.loop_stop()
        sub_client.disconnect()
        return

    # Wait for the subscriber to receive the message
    time.sleep(2)

    # Display the subscriber's output
    print("Subscriber output:")
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, "r") as f:
            print(f.read().strip())
    else:
        print("No output captured.")

    # Stop the subscriber
    print("Stopping subscriber...")
    sub_client.loop_stop()
    sub_client.disconnect()

    # Clean up
    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)
    print("Test complete.")

if __name__ == "__main__":
    main()

