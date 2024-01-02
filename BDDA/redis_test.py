import redis
import datetime
zones = [{'id_zone': 1, 'name': 'zone_1'}, {'id_zone': 2, 'name': 'zone_2'}]




client = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)




# for zone in zones:
#     key = f"zone:{zone['id_zone']}"
#     client.hset(key, mapping=zone)
#     print(f'Data inserted for key: {key}')



values = {'time': datetime.datetime(2024, 1, 2, 11, 18, 35, 37022).strftime('%Y-%m-%d-%H-%M-%S'), 'zone1': 22, 'zone2': 17}


zone1_count = values['zone1']
zone2_count = values['zone2']
time = values['time']

records = [{'id_zone': 1,'time':time, 'count': zone1_count},{'id_zone': 2,'time':time, 'count': zone2_count}]


for record in records:
    key = f"record:{record['id_zone']}"
    client.hset(key, mapping=record)
