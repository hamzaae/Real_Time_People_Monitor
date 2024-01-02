# from kafka import KafkaProducer

# # Kafka broker details
# bootstrap_servers = '172.31.80.1:9092'  # Use the actual IP address if needed

# # Create a Kafka producer
# producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

# # Kafka topic to which the message will be sent
# kafka_topic = 'rtdbTopic'  # Match the Kafka topic used with kafka-console-producer.sh

# def send_message(message):
#     try:
#         # Send a message to the Kafka topic
#         producer.send(kafka_topic, value=str(message).encode('utf-8'))
#         print(f"Message sent to Kafka topic: {kafka_topic}")
#     except Exception as e:
#         print(f"Error sending message to Kafka: {e}")


# # Example message
# sample_message = "Hello, Kafka 22!"
# for i in range(10):
#     send_message(i)
# # Send the example message to Kafka
# send_message(sample_message)

# # Close the Kafka producer
# producer.close()


import json
import random
from kafka import KafkaProducer
import time
import logging
producer = KafkaProducer(bootstrap_servers=['localhost:9092'], max_block_ms=5000)
curr_time = time.time()
while True:
    if time.time() > curr_time + 60: #1 minute
        break
    try:
        res = {
            "time": time.time(),
            "zone_1": random.randint(0, 20),
            "zone_2": random.randint(0, 20),
        }
        producer.send('users_created', json.dumps(res).encode('utf-8'))
    except Exception as e:
        logging.error(f'An error occured: {e}')
        continue