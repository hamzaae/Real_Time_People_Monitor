import base64
import pickle

from kafka import KafkaProducer

bootstrap_servers = 'localhost:9092'
topic = 'Topic'

# Create a Kafka producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

def send_message(message):
    try:
        print(message)
        encodded_message = str(message).encode('utf-8')
        base64_dict = base64.b64encode(encodded_message)
        producer.send(topic, value=base64_dict)
        print(f"Produced: {message} to Kafka topic: {topic}")
    except Exception as error:
        print(f"Error: {error}")