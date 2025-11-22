from kafka import KafkaProducer
import json
import logging
import time

# Set up logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("weather_kafka_producer")

# Kafka setup
KAFKA_BROKER = 'localhost:9092'  # Adjust the broker to your Kafka instance
TOPIC = 'weather_data_topic'

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def send_to_kafka(data):
    try:
        # Sending the data to Kafka topic
        producer.send(TOPIC, value=data)
        log.info("Data sent to Kafka successfully")
    except Exception as e:
        log.error(f"Error sending data to Kafka: {e}")

# Example function to send weather data
def send_weather_data(weather_data):
    send_to_kafka(weather_data)

# Example usage (this would typically be in the loop where data is processed)
sample_data = {
    "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
    "iso_code": "CAISO",
    "temperature": 23.5,
    "humidity": 45.2,
    "wind_speed": 12.3
}
send_weather_data(sample_data)
