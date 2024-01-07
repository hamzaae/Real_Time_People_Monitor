from hdfs import InsecureClient
import json
from kafka import KafkaConsumer


def consume_messages(kafka_topic):
    bootstrap_servers = 'kafka:9092'
    consumer_group_id = 'rtdb_consumer_group'  
    consumer = KafkaConsumer(kafka_topic,
                             group_id=consumer_group_id,
                             bootstrap_servers=bootstrap_servers,
                             auto_offset_reset='earliest', 
                             enable_auto_commit=True,
                             value_deserializer=lambda x: x.decode('utf-8'))

    try:
        # Iterate over the streamed data
        current_minute = None
        accumulated_data = []
        for message in consumer:
            data_point = eval(message.value)
            timestamp_str = data_point.get('time', '')
            data_point_minute = timestamp_str.split(':')[1]

            if current_minute is None:
                current_minute = data_point_minute
            if current_minute != data_point_minute:
                # Save the accumulated data to HDFS here
                save_to_hdfs(accumulated_data, timestamp_str.replace(' ', '_').replace(':', '-').rsplit(':', 1)[0] + '-00')

                print(len(accumulated_data), current_minute)
                current_minute = data_point_minute
                accumulated_data = []
                accumulated_data.append(data_point)
            else:
                accumulated_data.append(data_point)

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        print("Kafka consumer closed")

def save_to_hdfs(data, timestamp):
    hdfs_host = 'localhost'
    hdfs_port = 50070
    client = InsecureClient(f'http://{hdfs_host}:{hdfs_port}')

    hdfs_file_path = f'rtdbpc/data_{timestamp}.json'
    data_json = json.dumps(data)

    with client.write(hdfs_file_path, encoding='utf-8') as hdfs_file:
        hdfs_file.write(data_json)
    print(f"File uploaded to HDFS: {hdfs_file_path}")

consume_messages("rtdbTopic")

