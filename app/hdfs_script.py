from hdfs import InsecureClient
from datetime import datetime
import json



def save_to_hdfs(data, timestamp):
    hdfs_host = 'localhost'
    hdfs_port = 50070
    client = InsecureClient(f'http://{hdfs_host}:{hdfs_port}')

    hdfs_file_path = f'data_{timestamp}.json'
    data_json = json.dumps(data)

    with client.write(hdfs_file_path, encoding='utf-8') as hdfs_file:
        hdfs_file.write(data_json)
    print(f"File uploaded to HDFS: {hdfs_file_path}")




# Simulated streaming data
streamed_data = [
    {'time': '2024-01-03 13:15:48', 'zone_1': 8, 'zone_2': 6, 'zone_3': 3, 'zone_4': 3, 'zone_5': 2, 'zone_6': 1, 'zone_7': 8},
    {'time': '2024-01-03 13:15:49', 'zone_1': 10, 'zone_2': 6, 'zone_3': 3, 'zone_4': 4, 'zone_5': 2, 'zone_6': 2, 'zone_7': 8},
    {'time': '2024-01-03 13:15:51', 'zone_1': 9, 'zone_2': 6, 'zone_3': 2, 'zone_4': 4, 'zone_5': 1, 'zone_6': 2, 'zone_7': 9},
    {'time': '2024-01-03 13:16:48', 'zone_1': 8, 'zone_2': 6, 'zone_3': 3, 'zone_4': 3, 'zone_5': 2, 'zone_6': 1, 'zone_7': 8},
    {'time': '2024-01-03 13:16:49', 'zone_1': 10, 'zone_2': 6, 'zone_3': 3, 'zone_4': 4, 'zone_5': 2, 'zone_6': 2, 'zone_7': 8},
    {'time': '2024-01-03 13:17:51', 'zone_1': 9, 'zone_2': 6, 'zone_3': 2, 'zone_4': 4, 'zone_5': 1, 'zone_6': 2, 'zone_7': 9},
    {'time': '2024-01-03 13:17:55', 'zone_1': 9, 'zone_2': 6, 'zone_3': 2, 'zone_4': 4, 'zone_5': 1, 'zone_6': 2, 'zone_7': 9},
    {'time': '2024-01-03 13:18:48', 'zone_1': 8, 'zone_2': 6, 'zone_3': 3, 'zone_4': 3, 'zone_5': 2, 'zone_6': 1, 'zone_7': 8},
    {'time': '2024-01-03 13:18:49', 'zone_1': 10, 'zone_2': 6, 'zone_3': 3, 'zone_4': 4, 'zone_5': 2, 'zone_6': 2, 'zone_7': 8},
    {'time': '2024-01-03 13:19:49', 'zone_1': 10, 'zone_2': 6, 'zone_3': 3, 'zone_4': 4, 'zone_5': 2, 'zone_6': 2, 'zone_7': 8}
]


# Iterate over the streamed data
current_minute = None
accumulated_data = []
for data_point in streamed_data:
    
    timestamp_str = data_point.get('time', '')
    data_point_minute = timestamp_str.split(':')[1]

    if current_minute is None:
        current_minute = data_point_minute
    if current_minute != data_point_minute:
        # Save the accumulated data to HDFS here
        save_to_hdfs(accumulated_data, timestamp_str.replace(' ', '_'))

        print(len(accumulated_data), current_minute)
        current_minute = data_point_minute
        accumulated_data = []
        accumulated_data.append(data_point)
    else:
        accumulated_data.append(data_point)
# Save remaining accumulated data to HDFS here
save_to_hdfs(accumulated_data, timestamp_str.replace(' ', '_'))

