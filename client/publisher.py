import paho.mqtt.client as mqtt
import time
import random
import json
import os
from dotenv import load_dotenv
import threading

# Load environment variables
load_dotenv()

# MQTT Broker Configuration
MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
MQTT_QOS = int(os.getenv("MQTT_QOS", 1))

# MQTT v5 Features
MQTT_RESPONSE_TOPIC = "machine/response"  # Response topic for request/response pattern
MQTT_CORRELATION_DATA = b"12345"  # Unique identifier for response matching
MQTT_RETAIN = True  

# Additional Metadata
MQTT_CONTENT_TYPE = "application/json"  # Describes the payload format
MQTT_PAYLOAD_FORMAT = 1  # 1 = UTF-8 encoded JSON data

# MQTT Settings
MQTT_KEEPALIVE = 60  
MQTT_LWT_TOPIC = "machine/status"  
MQTT_LWT_MESSAGE = json.dumps({"machine_id": "", "status": "offline"})
MQTT_LWT_QOS = 1
MQTT_LWT_RETAIN = True

# Global running flag
running = True  

# Flag to control logging of ACK messages
LOG_ACKS = False  # Set to True to enable ACK message logging, False to disable

# Flag to control whether payload is shown in the console
SHOW_PAYLOAD = False  # Set to False to hide the payload in the console

# Shared Subscription Settings
SHARED_SUBSCRIPTION_NAME = "my-shared-subscribers"  # Define a shared subscription name
SHARED_TOPIC = "machine/telemetry/data"  # Base topic for telemetry data
SHARED_TOPIC_SUBSCRIPTION = f"$share/{SHARED_SUBSCRIPTION_NAME}/{SHARED_TOPIC}"

print(f"MQTT Broker: {MQTT_BROKER}, QoS Level: {MQTT_QOS}")

# MQTT Callbacks
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print(f"✅ Connected to HiveMQ broker as {client._client_id.decode()}")
        # Ensure the machine is marked as online after a successful reconnect
        client.publish(
            MQTT_LWT_TOPIC,
            json.dumps({"machine_id": client._client_id.decode(), "status": "online"}),
            qos=MQTT_LWT_QOS,
            retain=MQTT_LWT_RETAIN
        )
        # Subscribe to the shared subscription topic
        client.subscribe(SHARED_TOPIC_SUBSCRIPTION, qos=MQTT_QOS)
        print(f"🗣️ Subscribed to shared subscription: {SHARED_TOPIC_SUBSCRIPTION}")
    else:
        print(f"❌ Connection failed with code {rc}")


def on_publish(client, userdata, mid):
    print(f"📨 Message {mid} published successfully")

def on_message(client, userdata, message):
    if SHOW_PAYLOAD:
        print(f"📥 Received message on {message.topic}: {message.payload.decode()} (QoS {message.qos})")
    else:
        print(f"📥 Received message on {message.topic} (Payload hidden) (QoS {message.qos})")


# Log ACK messages
def on_log(client, userdata, level, buf):
    if LOG_ACKS and any(ack in buf for ack in ["PUBACK", "PUBREC", "PUBREL", "PUBCOMP"]):
        print(f"📥 MQTT ACK: {buf}")

def create_mqtt_client(machine_id, clean_session=False):  # Added clean_session parameter
    client = mqtt.Client(client_id=f"machine_{machine_id}", protocol=mqtt.MQTTv5, clean_session=clean_session)  # Set clean_session
    client.will_set(MQTT_LWT_TOPIC, MQTT_LWT_MESSAGE.replace("offline", "online"), qos=MQTT_LWT_QOS, retain=MQTT_LWT_RETAIN)
    client.on_connect = on_connect
    client.on_publish = on_publish
    client.on_message = on_message
    client.on_log = on_log  
    client.connect(MQTT_BROKER, MQTT_PORT, MQTT_KEEPALIVE)
    return client

def simulate_internet_loss(client):
    # Randomly simulate loss of internet connection (disconnect for 5 to 15 seconds)
    loss_duration = random.randint(5, 15)
    print(f"🌐 Simulating internet loss for {loss_duration} seconds...")
    
    # Update the LWT to "offline" while simulating the loss
    client.publish(
        MQTT_LWT_TOPIC,
        json.dumps({"machine_id": client._client_id.decode(), "status": "offline"}),
        qos=MQTT_LWT_QOS,
        retain=MQTT_LWT_RETAIN
    )
    
    time.sleep(loss_duration)
    print("🌐 Internet connection restored.")
    
    # Once restored, mark the machine as "online" again
    client.publish(
        MQTT_LWT_TOPIC,
        json.dumps({"machine_id": client._client_id.decode(), "status": "online"}),
        qos=MQTT_LWT_QOS,
        retain=MQTT_LWT_RETAIN
    )


def publish_data(client, machine_id):
    global running
    while running:
        # Randomly simulate internet loss for some machines
        if random.random() < 0.2:  # 20% chance to simulate internet loss
            simulate_internet_loss(client)  # Pass the client object here

        # Generate basic telemetry data
        temperature = round(random.uniform(20.0, 30.0), 2)
        vibration = round(random.uniform(0.1, 5.0), 2)

        # Generate additional data to increase payload size
        long_string = "A" * 1000  # A 1 KB string, repeated as needed

        # Construct a payload with enough data to hit ~5 KB
        payload = {
            "machine_id": f"machine_{machine_id}",
            "temperature": temperature,
            "vibration": vibration,
            "long_string_1": long_string,
            "long_string_2": long_string,
            "long_string_3": long_string,
            "long_string_4": long_string,
            "long_string_5": long_string
        }
        payload_str = json.dumps(payload)

        # Ensure the payload is approximately 5 KB
        payload_size_kb = len(payload_str.encode('utf-8')) / 1024  # Convert to KB
        if payload_size_kb < 5:
            padding = "B" * (1024 * (5 - payload_size_kb))  # Pad with extra data
            payload_str = json.dumps({**payload, "padding": padding})

        # MQTT v5 Properties
        properties = mqtt.Properties(mqtt.PacketTypes.PUBLISH)
        properties.ResponseTopic = MQTT_RESPONSE_TOPIC
        properties.CorrelationData = MQTT_CORRELATION_DATA
        properties.ContentType = MQTT_CONTENT_TYPE
        properties.PayloadFormatIndicator = MQTT_PAYLOAD_FORMAT

        if SHOW_PAYLOAD:
            print(f"📡 Machine {machine_id} publishing to machine_{machine_id}/telemetry/data: {payload_str} (QoS {MQTT_QOS})")
        else:
            print(f"📡 Machine {machine_id} publishing to machine_{machine_id}/telemetry/data: Payload sent (QoS {MQTT_QOS})")
        
        client.publish(f"machine_{machine_id}/telemetry/data", payload_str, qos=MQTT_QOS, retain=MQTT_RETAIN, properties=properties)

        time.sleep(15)


def listen_for_stop():
    global running
    while True:
        command = input("Enter 'stop' (graceful) or 'crash' (ungraceful) to exit: ")
        if command.lower() == 'stop':
            print("🛑 Graceful disconnect...")
            running = False
            break
        elif command.lower() == 'crash':
            print("💥 Simulating a crash!")
            os._exit(1)  # Simulate an unexpected crash

if __name__ == "__main__":
    try:
        num_machines = int(input("Enter the number of machines to connect: "))
        
        # Start multiple MQTT clients for each machine
        threads = []
        clients = []
        for machine_id in range(1, num_machines + 1):
            client = create_mqtt_client(machine_id, clean_session=False)  # Set clean_session to False
            client.loop_start()  # Start the MQTT loop for each client
            clients.append(client)
            
            # Create a separate thread for publishing data
            publish_thread = threading.Thread(target=publish_data, args=(client, machine_id))
            threads.append(publish_thread)
            publish_thread.start()

        # Start the thread for graceful shutdown
        stop_thread = threading.Thread(target=listen_for_stop)
        stop_thread.start()
        
        # Wait for all publish threads to finish
        for thread in threads:
            thread.join()

    except KeyboardInterrupt:
        print("🛑 Stopping publisher due to KeyboardInterrupt...")
        running = False
        for client in clients:
            client.loop_stop()
            client.disconnect()
