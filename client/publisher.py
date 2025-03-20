import paho.mqtt.client as mqtt
import psycopg2
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

# AWS PostgreSQL Database Configuration from .env
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

# Initialize connection variables
conn = None
cursor = None
client = None
running = True  # Flag to control whether the loop should continue

# Check that environment variables are loaded correctly
print(f"MQTT Broker: {MQTT_BROKER}")
print(f"DB Host: {DB_HOST}, DB Name: {DB_NAME}")

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
        print("✅ Connected to HiveMQ broker")
        # Connect to the database once MQTT connection is successful
        connect_db()

        # Ensure telemetry table exists after connecting to DB
        if cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS telemetry (
                    id SERIAL PRIMARY KEY,
                    machine_id TEXT,
                    temperature FLOAT,
                    vibration FLOAT,
                    timestamp TIMESTAMP DEFAULT NOW()
                );
            """)
            conn.commit()
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

# Function to publish and store data
def publish_and_store():
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

        # Store data in PostgreSQL after successful MQTT connection
        if cursor:
            try:
                cursor.execute(
                    "INSERT INTO telemetry (machine_id, temperature, vibration, timestamp) VALUES (%s, %s, %s, NOW())",
                    (payload["machine_id"], temperature, vibration),
                )
                conn.commit()
                print("✅ Data inserted into AWS PostgreSQL")
            except Exception as e:
                print(f"❌ Database insertion error: {e}")

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
            if cursor:
                cursor.close()
            if conn:
                conn.close()
            break

# Start the process
if __name__ == "__main__":
    try:
        create_mqtt_client()
        client.loop_start()

        # Start the thread to listen for the stop command
        stop_thread = threading.Thread(target=listen_for_stop)
        stop_thread.start()

        # Main publishing process
        publish_and_store()
        
    except KeyboardInterrupt:
        print("🛑 Stopping publisher due to KeyboardInterrupt...")
        running = False
        if client:
            client.loop_stop()
            client.disconnect()
        if cursor:
            cursor.close()
        if conn:
            conn.close()
