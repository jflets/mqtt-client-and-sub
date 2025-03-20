import paho.mqtt.client as mqtt
import psycopg2
import json
import os
from dotenv import load_dotenv
import random
import threading

# Load environment variables from .env file
load_dotenv()

# MQTT Broker Configuration from .env
MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "machine/telemetry/data")
MQTT_LWT_TOPIC = "machine/status"

# AWS PostgreSQL Database Configuration from .env
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

# Define multiple client IDs for subscribers
SUBSCRIBERS = ["subscriber_1", "subscriber_2", "subscriber_3", "subscriber_4", "subscriber_5"]

# Initialize connection variables
conn = None
cursor = None

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
        client.subscribe([(MQTT_TOPIC, 1), (MQTT_LWT_TOPIC, 1)])
        print(f"✅ Subscribed to {MQTT_TOPIC} and {MQTT_LWT_TOPIC}")
        connect_db()
    else:
        print(f"❌ Connection failed with code {rc}")

def on_message(client, userdata, msg):
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
def start_subscriber(client_id):
    client = mqtt.Client(client_id=client_id, clean_session=False)  # Set clean_session=False
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    print(f"🚀 Starting subscriber {client_id}")
    client.loop_forever()

# Function to start multiple subscribers concurrently
def start_multiple_subscribers():
    threads = []
    for client_id in SUBSCRIBERS:
        thread = threading.Thread(target=start_subscriber, args=(client_id,))
        thread.start()
        threads.append(thread)
    
    # Optionally, join threads if you want to wait for them to finish (although loop_forever keeps them running)
    for thread in threads:
        thread.join()

# Start all subscribers
if __name__ == "__main__":
    start_multiple_subscribers()
