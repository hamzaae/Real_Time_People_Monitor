import time

import clickhouse_connect
import datetime



client = clickhouse_connect.get_client(host='localhost', username='default',port = 8123)


#
# client.command('''
#         Create Table if not exists Zone
#             (id_zone UInt32 primary key,
#              name String)
#         ENGINE MergeTree
#         order by id_zone;
# ''')
#
# client.command('''
#         Create Table if not exists Recording
#             (id_rec UInt32 primary key,
#             time DateTime,
#              n_count UInt32,
#              id_zone UInt32,
#              foreign key (id_zone) references Zone(id_zone))
#         ENGINE MergeTree
#         order by id_rec;
# ''')


# print("done")
# row1 = [1, 'Caisse']
# row2 = [2, 'Boulengerie']
# data = [row1, row2]
# client.insert('Zone', data)
#
#
# result = client.query('SELECT * FROM Zone')
# print(result.result_rows)


# row3 = [1, datetime.datetime.now(),1,1]
# row5 = [3,datetime.datetime.now(),4,2]
#
# time.sleep(1)

while True:
    row3 = [1, datetime.datetime.now(),1,1]
    row5 = [3,datetime.datetime.now(),3,2]

    data2 = [row3, row5]
    client.insert('Recording', data2)

    time.sleep(1)

    row6 = [4,datetime.datetime.now(),2,2]
    row4 = [2,datetime.datetime.now(),8,1]

    data1 = [row6,row4]
    client.insert('Recording', data1)




    row3 = [1, datetime.datetime.now(), 4, 1]
    row5 = [3, datetime.datetime.now(), 5, 2]

    data2 = [row3, row5]
    client.insert('Recording', data2)

    time.sleep(1)

    row6 = [4, datetime.datetime.now(), 5, 2]
    row4 = [2, datetime.datetime.now(), 1, 1]

    data1 = [row6, row4]
    client.insert('Recording', data1)

    print("done")

# result = client.query('SELECT * FROM Recording')
# print(result.result_rows)
