import clickhouse_connect
from kafka import KafkaConsumer
from datetime import datetime
import json
import uuid


def convert_to_datetime(input_string):
    # Convert string to datetime object
    input_format="%Y-%m-%d %H:%M:%S"
    formatted_string = datetime.strptime(input_string, input_format)
    return formatted_string

def insert_clickhouse(data):
    client = clickhouse_connect.get_client(host='localhost', username='default',database="Market", port=8123)
    client.insert('Record', data)
    print('done')
    
def transform_dict_to_list(input_dict):
    time_value = input_dict["time"]
    result_list = [[str(uuid.uuid4()),convert_to_datetime(time_value), count,int(zone.split("_")[1]) ] for zone, count in input_dict.items() if zone != "time"]
    return result_list

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
        
        for message in consumer:
            output = transform_dict_to_list(eval(message.value))
            print(f"Received message: {output}")
            insert_clickhouse(output)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        print("Kafka consumer closed")


print("====hello from clickhouse consumer====")
consume_messages("rtdbTopic")
# streamed_data = [
#     {'time': '2024-01-03 13:15:48', 'zone_1': 8, 'zone_2': 6, 'zone_3': 3, 'zone_4': 3, 'zone_5': 2, 'zone_6': 1, 'zone_7': 8},
#     {'time': '2024-01-03 13:15:49', 'zone_1': 10, 'zone_2': 6, 'zone_3': 3, 'zone_4': 4, 'zone_5': 2, 'zone_6': 2, 'zone_7': 8},
#     {'time': '2024-01-03 13:15:51', 'zone_1': 9, 'zone_2': 6, 'zone_3': 2, 'zone_4': 4, 'zone_5': 1, 'zone_6': 2, 'zone_7': 9},
#     {'time': '2024-01-03 13:16:48', 'zone_1': 8, 'zone_2': 6, 'zone_3': 3, 'zone_4': 3, 'zone_5': 2, 'zone_6': 1, 'zone_7': 8},
#     {'time': '2024-01-03 13:16:49', 'zone_1': 10, 'zone_2': 6, 'zone_3': 3, 'zone_4': 4, 'zone_5': 2, 'zone_6': 2, 'zone_7': 8},
#     {'time': '2024-01-03 13:17:51', 'zone_1': 9, 'zone_2': 6, 'zone_3': 2, 'zone_4': 4, 'zone_5': 1, 'zone_6': 2, 'zone_7': 9},
#     {'time': '2024-01-03 13:17:55', 'zone_1': 9, 'zone_2': 6, 'zone_3': 2, 'zone_4': 4, 'zone_5': 1, 'zone_6': 2, 'zone_7': 9},
#     {'time': '2024-01-03 13:18:48', 'zone_1': 8, 'zone_2': 6, 'zone_3': 3, 'zone_4': 3, 'zone_5': 2, 'zone_6': 1, 'zone_7': 8},
#     {'time': '2024-01-03 13:18:49', 'zone_1': 10, 'zone_2': 6, 'zone_3': 3, 'zone_4': 4, 'zone_5': 2, 'zone_6': 2, 'zone_7': 8},
#     {'time': '2024-01-03 13:19:49', 'zone_1': 10, 'zone_2': 6, 'zone_3': 3, 'zone_4': 4, 'zone_5': 2, 'zone_6': 2, 'zone_7': 8}
# ]

# for i in streamed_data:
#     insert_clickhouse(transform_dict_to_list(i))
#     print("--------------------")
