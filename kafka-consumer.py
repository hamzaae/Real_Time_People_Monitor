from kafka import KafkaConsumer

bootstrap_servers = '127.0.0.1:9092'


# Kafka topic from which the messages will be consumed
kafka_topic = 'rtdbTopic'  # Match the Kafka topic used with kafka-console-producer.sh

# Create a Kafka consumer
consumer = KafkaConsumer(
    kafka_topic,
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',  # Start consuming from the beginning of the topic
    group_id='my_consumer_group'   # Specify a unique consumer group id
)

# Continuously poll for new messages
for message in consumer:
    try:
        # Decode the message value and print it
        print(f"Received message: {message.value.decode('utf-8')}")
    except Exception as e:
        print(f"Error processing message: {e}")

# Close the Kafka consumer (this is typically done in a separate thread or process)
consumer.close()
