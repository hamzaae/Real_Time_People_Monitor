from kafka import KafkaProducer


def send_message(message, kafka_topic):
    bootstrap_servers = '127.0.0.1:9092' 
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
    try:
        # Send a message to the Kafka topic
        producer.send(kafka_topic, value=str(message).encode('utf-8'))
        print(f"Message sent to Kafka topic: {kafka_topic}")
    except Exception as e:
        print(f"Error sending message to Kafka: {e}")
    producer.close()

