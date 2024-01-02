import base64
import json
from kafka import KafkaConsumer
import psycopg2
from psycopg2.extras import RealDictCursor
import datetime





def consumer_postgresql():
    # Kafka broker configuration
    bootstrap_servers = 'localhost:9092'
    topic = 'firstTopic'

    # Create a Kafka consumer
    consumer = KafkaConsumer(
        topic,
        group_id='my_consumer_group',
        bootstrap_servers=bootstrap_servers,
        value_deserializer=lambda x: x.decode('utf-8')
    )

    # Establish a connection to the PostgreSQL database
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        user="postgres",
        password="readyset",
        database="market"
    )

    try:
        for message in consumer:
            print('ok******')

            # Split the message value into individual values
            values = eval(base64.b64decode(message.value))

            # Unpack the values into variables
            zone1_count = values['zone1']
            zone2_count = values['zone2']
            time_value = values['time']

            records = [
                {'id_zone': 1, 'time': time_value, 'count': zone1_count},
                {'id_zone': 2, 'time': time_value, 'count': zone2_count}
            ]

            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                for record in records:
                    cursor.execute("""
                        INSERT INTO zone_counts (id_zone, time, count)
                        VALUES (%s, %s, %s)
                    """, (record['id_zone'], record['time'], record['count']))

                conn.commit()

            print('records inserted in PostgreSQL')
            print("-------------------")

    except KeyboardInterrupt:
        print("Consumer stopped.")

    finally:
        conn.close()
        print("Consumer and database connections closed.")
