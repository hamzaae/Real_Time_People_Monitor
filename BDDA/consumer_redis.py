import base64

from kafka import KafkaConsumer
import redis
import datetime
import time



def consumer_redis():
    # Kafka broker configuration
    bootstrap_servers = 'localhost:9092'
    topic = 'Topic'

    # Create a Kafka consumer
    consumer = KafkaConsumer(topic,
                             group_id='my_consumer_group',
                             bootstrap_servers=bootstrap_servers,
                             value_deserializer=lambda x: x.decode('utf-8'))

    try:
        # Establish a connection to the redis database
        client = redis.StrictRedis(host='localhost',port=6379,db=0,decode_responses=True)
        for message in consumer:
            # Split the message value into individual values
            values = eval(base64.b64decode(message.value))

            # Unpack the values into variables
            zone1_count = values['zone1']
            zone2_count = values['zone2']
            time =values['time']

            print('final data: ',values)

            records = [{'id_zone': 1,'time':time, 'count': zone1_count},{'id_zone': 2,'time':time, 'count': zone2_count}]

            for record in records:
                key = f"record:{record['id_zone']}"
                client.hset(key, mapping=record)

            print('records inserted in redis')
            print("-------------------")



    except KeyboardInterrupt:
        print("Consumer stopped.")


        print("Consumer and database connections closed.")