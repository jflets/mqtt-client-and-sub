import paho.mqtt.client as mqtt
import psycopg2
import json
import os
from dotenv import load_dotenv

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
        client.subscribe(MQTT_TOPIC)  # Subscribe to the topic after connection
        connect_db()  # Connect to the database after successful MQTT connection
    else:
        print(f"❌ Connection failed with code {rc}")

def on_message(client, userdata, msg):
    print(f"📩 Received message from {msg.topic}: {msg.payload.decode()}")
    
    try:
        payload = json.loads(msg.payload.decode())

        # Insert data into PostgreSQL
        if cursor:
            cursor.execute(
                "INSERT INTO telemetry (machine_id, temperature, vibration, timestamp) VALUES (%s, %s, %s, NOW())",
                (payload["machine_id"], payload["temperature"], payload["vibration"]),
            )
            conn.commit()
            print("✅ Data inserted into AWS PostgreSQL")
    except Exception as e:
        print(f"❌ Error processing message: {e}")

# Create MQTT Client
client = mqtt.Client(client_id="subscriber_1")
client.on_connect = on_connect
client.on_message = on_message
client.connect(MQTT_BROKER, MQTT_PORT, 60)

# Start listening
try:
    client.loop_forever()
except KeyboardInterrupt:
    print("🛑 Stopping subscriber...")
    client.disconnect()
    if cursor:
        cursor.close()
    if conn:
        conn.close()
