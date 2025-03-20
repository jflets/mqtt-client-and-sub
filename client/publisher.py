import paho.mqtt.client as mqtt
import time
import random
import json
import os
from dotenv import load_dotenv
import threading

# Load environment variables from .env file
load_dotenv()

# MQTT Broker Configuration from .env
MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "machine/telemetry/data")

# Initialize MQTT client
client = None
running = True  # Flag to control whether the loop should continue

# Check that environment variables are loaded correctly
print(f"MQTT Broker: {MQTT_BROKER}")

# MQTT Callbacks
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("✅ Connected to HiveMQ broker")
    else:
        print(f"❌ Connection failed with code {rc}")

def on_publish(client, userdata, mid):
    print(f"📨 Message {mid} published successfully")

# Create MQTT Client
def create_mqtt_client():
    global client
    client = mqtt.Client(client_id="machine_1")
    client.on_connect = on_connect
    client.on_publish = on_publish
    client.connect(MQTT_BROKER, MQTT_PORT, 60)

# Function to publish data
def publish_data():
    global running
    while running:
        temperature = round(random.uniform(20.0, 30.0), 2)
        vibration = round(random.uniform(0.1, 5.0), 2)

        payload = {
            "machine_id": "machine_1",
            "temperature": temperature,
            "vibration": vibration,
        }
        payload_str = json.dumps(payload)

        print(f"📡 Publishing to {MQTT_TOPIC}: {payload_str}")
        client.publish(MQTT_TOPIC, payload_str, qos=1)

        time.sleep(15)

# Function to listen for stop command
def listen_for_stop():
    global running
    while True:
        command = input("Enter 'stop' to disconnect and stop the script: ")
        if command.lower() == 'stop':
            print("🛑 Stopping publisher...")
            running = False
            if client:
                client.loop_stop()
                client.disconnect()
            break

# Start the process
if __name__ == "__main__":
    try:
        create_mqtt_client()
        client.loop_start()

        # Start the thread to listen for the stop command
        stop_thread = threading.Thread(target=listen_for_stop)
        stop_thread.start()

        # Start publishing
        publish_data()
        
    except KeyboardInterrupt:
        print("🛑 Stopping publisher due to KeyboardInterrupt...")
        running = False
        if client:
            client.loop_stop()
            client.disconnect()
