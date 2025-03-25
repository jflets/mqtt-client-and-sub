# MQTT Publisher and Subscriber with PostgreSQL Integration

## Overview
This project simulates an MQTT publisher that generates telemetry data (temperature, vibration, etc.) for multiple machines. The data is sent to an MQTT broker and includes features such as Last Will and Testament (LWT) messages, shared subscriptions, and internet connectivity loss simulation. 

Subscribers are set up to listen for telemetry data from multiple machines and store it into an AWS-hosted PostgreSQL database. You can control whether to show the payload in the console and insert data into the database using command-line arguments.

## Setup Steps

### 1. Install Required Dependencies
Make sure you have Python 3 installed, then install the necessary dependencies by running:

```bash
pip install paho-mqtt psycopg2 python-dotenv
```

### 2. Create a `.env` File
Create a `.env` file in the project root directory to store sensitive information, including your MQTT broker and PostgreSQL database credentials. Here's an example:

```env
# MQTT Broker Configuration
MQTT_BROKER=localhost
MQTT_PORT=1883
MQTT_QOS=1

# PostgreSQL Database Configuration
DB_HOST=<your-db-host>
DB_PORT=5432
DB_NAME=<your-db-name>
DB_USER=<your-db-username>
DB_PASS=<your-db-password>
```

Replace the placeholders (`<your-db-host>`, `<your-db-name>`, etc.) with your actual database details. If you are using a local PostgreSQL instance, set `DB_HOST=localhost`.

### 3. Run the Publisher (Simulate MQTT Machines)
The publisher simulates multiple machines sending telemetry data. You can start it by running the following command:

```bash
python publisher.py
```

It will prompt you to enter the number of machines to connect, and then it will start publishing telemetry data for each machine. It also simulates intermittent internet connectivity loss for some machines.

To stop the publisher gracefully, type `stop`. To simulate a crash, type `crash`.

### 4. Run the Subscriber (Listen and Store Data in PostgreSQL)
The subscriber listens to telemetry data and Last Will and Testament (LWT) messages. It stores the received telemetry data into the PostgreSQL database.

To run the subscriber, use the following command:

```bash
python subscriber.py
```

You can also pass the `--show-payload` argument to print the payload of the received messages:

```bash
python subscriber.py --show-payload
```

### 5. Database Setup
Ensure your PostgreSQL database is set up with a `telemetry` table that looks like this:

```sql
CREATE TABLE telemetry (
    machine_id VARCHAR(255),
    temperature DOUBLE PRECISION,
    vibration DOUBLE PRECISION,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

This table will store the telemetry data (machine ID, temperature, vibration, and timestamp).

### 6. Customize and Test
You can modify the number of machines in the publisher, the telemetry data structure, and the subscriber's topics. Adjust the topics based on your needs, such as using single-level or multi-level wildcards for topic subscriptions.

### 7. Optional - Start Multiple Subscribers
You can start multiple subscribers that listen to different topics using the `start_multiple_subscribers()` function. This will allow you to handle different machines or telemetry data streams.

---

### Notes:
- The publisher sends telemetry data in a JSON format and can simulate internet loss for up to 15 seconds.
- The subscriber subscribes to various topics, including wildcard topics and shared subscriptions, to receive data from different sources.
- You can choose whether to display the payload in the console by using the `--show-payload` argument in the subscriber script.