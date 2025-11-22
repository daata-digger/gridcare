from flask import Flask, render_template
from flask_socketio import SocketIO, emit
from kafka import KafkaConsumer
import threading
import json

app = Flask(__name__)
socketio = SocketIO(app)

# Kafka Consumer for real-time data
KAFKA_BROKER = 'localhost:9092'
TOPIC = 'weather_data_topic'

consumer = KafkaConsumer(TOPIC, bootstrap_servers=KAFKA_BROKER, value_deserializer=lambda m: json.loads(m.decode('utf-8')))

def consume_kafka_data():
    for message in consumer:
        data = message.value
        socketio.emit('weather_update', data)

@app.route('/')
def index():
    return render_template('index.html')

if __name__ == '__main__':
    thread = threading.Thread(target=consume_kafka_data)
    thread.daemon = True
    thread.start()
    
    socketio.run(app, host='0.0.0.0', port=5000)
