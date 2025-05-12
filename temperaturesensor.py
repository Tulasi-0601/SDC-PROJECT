import boto3
import json
import random
import time

# AWS Kinesis Stream Name
STREAM_NAME = "TemperatureSensorStream"

# Initialize Kinesis Client
kinesis_client = boto3.client("kinesis", region_name="us-east-1")

def generate_temperature_data():
    """Simulate temperature readings and send data to Kinesis."""
    while True:
        # Simulating sensor ID and random temperature
        sensor_id = random.randint(1, 10)
        temperature = round(random.uniform(20.0, 40.0), 2)  # Temperature in Celsius

        # Create JSON payload
        data_payload = {
            "sensor_id": sensor_id,
            "temperature": temperature,
            "timestamp": time.time()
        }

        # Convert to JSON string
        json_data = json.dumps(data_payload)

        # Send data to Kinesis
        response = kinesis_client.put_record(
            StreamName=STREAM_NAME,
            Data=json_data,
            PartitionKey=str(sensor_id)
        )

        print(f"Sent: {json_data}, Response: {response['ResponseMetadata']['HTTPStatusCode']}")

        # Simulating real-time streaming every 2 seconds
        time.sleep(2)

# Run the sensor data stream
generate_temperature_data()
