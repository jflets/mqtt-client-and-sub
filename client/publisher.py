import paho.mqtt.client as mqtt
import psycopg2
import time
import random
import json
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv("config/broker_config.env")
load_dotenv("db/db.env")

# MQTT Broker Configuration
MQTT_BROKER = os.getenv("MQTT_BROKER")
MQTT_PORT = int(os.getenv("MQTT_PORT"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC")

# AWS PostgreSQL Database Configuration
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

# Connect to PostgreSQL
def connect_db():
    try:
        conn = psycopg2.connect(
            host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS, port=DB_PORT
        )
        cursor = conn.cursor()
        print("✅ Connected to AWS PostgreSQL")
        return conn, cursor
    except Exception as e:
        print(f"❌ Database connection error: {e}")
        return None, None

conn, cursor = connect_db()

# Ensure telemetry table exists
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

# MQTT Callbacks
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("✅ Connected to HiveMQ broker")
    else:
        print(f"❌ Connection failed with code {rc}")

def on_publish(client, userdata, mid):
    print(f"📨 Message {mid} published successfully")

# Create MQTT Client
client = mqtt.Client(client_id="machine_1")
client.on_connect = on_connect
client.on_publish = on_publish
client.connect(MQTT_BROKER, MQTT_PORT, 60)

# Function to publish and store data
def publish_and_store():
    while True:
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

        # Store data in PostgreSQL
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

# Start the process
try:
    client.loop_start()
    publish_and_store()
except KeyboardInterrupt:
    print("🛑 Stopping publisher...")
    client.loop_stop()
    client.disconnect()
    if cursor:
        cursor.close()
        conn.close()
