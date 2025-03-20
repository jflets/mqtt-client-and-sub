import paho.mqtt.client as mqtt
import psycopg2
import json
import os
import argparse
from dotenv import load_dotenv
import threading

# Load environment variables from .env file
load_dotenv()

# MQTT Broker Configuration from .env
MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
MQTT_LWT_TOPIC = "machine/status"

# AWS PostgreSQL Database Configuration from .env
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

# Define 4 subscribers with their custom topics
SUBSCRIBERS = [
    {"client_id": "subscriber_1", "topic": "machine_1/telemetry/data"},
    {"client_id": "subscriber_2", "topic": "machine_2/telemetry/data"},
    {"client_id": "subscriber_3", "topic": "machine/+/telemetry/data"},  # Single-level wildcard example
    {"client_id": "subscriber_4", "topic": "machine/#"},    # Multi-level wildcard at the end
    {"client_id": "shared_subscriber", "topic": "$share/group1/machine/+/telemetry/data"}  # Shared subscription
]

# Initialize connection variables
conn = None
cursor = None

# Command-line argument parsing
parser = argparse.ArgumentParser(description="MQTT Subscriber")
parser.add_argument(
    "--show-payload", action="store_true", help="Flag to print the payload"
)
args = parser.parse_args()

# Set the flag to show or hide payload based on the command-line argument
show_payload = args.show_payload

# Connect to PostgreSQL
def connect_db():
    global conn, cursor
    try:
        conn = psycopg2.connect(
            host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS, port=DB_PORT
        )
        cursor = conn.cursor()
        print("✅ Connected to AWS PostgreSQL")
    except Exception as e:
        print(f"❌ Database connection error: {e}")

# MQTT Callbacks
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"✅ Connected to MQTT Broker at {MQTT_BROKER}")
        
        # Get the topic for this subscriber
        topic = userdata["topic"]
        print(f"🔔 Subscribing to topic: {topic}")  # Add this line to print the topic
        
        # Subscribe to the main topic and the LWT topic separately
        client.subscribe(topic, 1)  # Corrected subscribe syntax for the main telemetry topic
        client.subscribe(MQTT_LWT_TOPIC, 1)  # Corrected subscribe syntax for the LWT topic
        
        print(f"✅ Subscribed to {topic} and {MQTT_LWT_TOPIC}")
        
        # Connect to the database
        connect_db()
    else:
        print(f"❌ Connection failed with code {rc}")


def on_message(client, userdata, msg):
    if show_payload:
        print(f"📩 Received message from {msg.topic}: {msg.payload.decode()}")
    
    try:
        payload = json.loads(msg.payload.decode())
        if msg.topic == MQTT_LWT_TOPIC:
            print(f"🔴 LWT Update: {payload}")
        else:
            # Insert telemetry data into PostgreSQL
            if cursor:
                cursor.execute(
                    "INSERT INTO telemetry (machine_id, temperature, vibration, timestamp) VALUES (%s, %s, %s, NOW())",
                    (payload["machine_id"], payload["temperature"], payload["vibration"]),
                )
                conn.commit()
                print("✅ Data inserted into AWS PostgreSQL")
    except Exception as e:
        print(f"❌ Error processing message: {e}")

# Function to start a subscriber
def start_subscriber(client_id, topic):
    client = mqtt.Client(client_id=client_id, clean_session=False)  # Set clean_session=False
    client.user_data_set({"topic": topic})  # Pass the topic for the subscriber
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    print(f"🚀 Starting subscriber {client_id}")
    client.loop_forever()

# Function to start multiple subscribers concurrently
def start_multiple_subscribers():
    threads = []
    for subscriber in SUBSCRIBERS:
        client_id = subscriber["client_id"]
        topic = subscriber["topic"]
        thread = threading.Thread(target=start_subscriber, args=(client_id, topic))
        thread.start()
        threads.append(thread)
    
    # Optionally, join threads if you want to wait for them to finish (although loop_forever keeps them running)
    for thread in threads:
        thread.join()

# Start all subscribers
if __name__ == "__main__":
    start_multiple_subscribers()
