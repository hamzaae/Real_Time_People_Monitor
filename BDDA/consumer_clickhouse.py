import base64

from kafka import KafkaConsumer
import json
import datetime
import clickhouse_connect





def consumer_clickhouse():
    # Kafka broker configuration
    bootstrap_servers = 'localhost:9092'
    topic = 'firstTopic'

    # Create a Kafka consumer
    consumer = KafkaConsumer(topic,
                             group_id='my_consumer_group',
                             bootstrap_servers=bootstrap_servers,
                             value_deserializer=lambda x: x.decode('utf-8'))

    try:
        # Establish a connection to the ClickHouse database
        client = clickhouse_connect.get_client(host='localhost', username='default', port=8123)


        for message in consumer:
            try:
                # Split the message value into individual values
                values = eval(base64.b64decode(message.value))

                print(values)
                # Unpack the values into variables
                zone1 = values['zone1']
                zone2 = values['zone2']
                time = values['time']

                row1 = [1, time, zone1, 1]
                row2 = [2, time, zone2, 2]

                data = [row1, row2]
                client.insert('Recording', data)

                print("-------------------")

            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")
                continue

    except KeyboardInterrupt:
        print("Consumer stopped.")


        print("Consumer and database connections closed.")