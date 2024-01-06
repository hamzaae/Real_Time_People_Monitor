import json
from kafka import KafkaProducer
from kafka import KafkaConsumer

def consume_messages(kafka_topic):
    bootstrap_servers = 'kafka:9092'
    consumer_group_id = 'rtdb_consumer_group'  
    consumer = KafkaConsumer(kafka_topic,
                             group_id=consumer_group_id,
                             bootstrap_servers=bootstrap_servers,
                             auto_offset_reset='earliest', 
                             enable_auto_commit=True,
                             value_deserializer=lambda x: json.loads(x.decode('utf-8')))

    try:
        for message in consumer:
            # Process the received message
            print(f"Received message: {message.value}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        print("Kafka consumer closed")

def send_message(message, kafka_topic):
    bootstrap_servers = 'kafka:9092' 
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
    try:
        # Send a message to the Kafka topic
        producer.send(kafka_topic, value=json.dumps(message).encode('utf-8'))
        print(f"Message sent to Kafka topic: {kafka_topic}")
    except Exception as e:
        print(f"Error sending message to Kafka: {e}")
    producer.close()
    print("Kafka producer closed")


# consume_messages("rtdbTopic")
