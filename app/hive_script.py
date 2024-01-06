from pyhive import hive



def connect_to_hive():
    hive_host = 'localhos'
    hive_port = 10000
    hive_database = 'rtdbpcdb'  

    conn = hive.Connection(
        host=hive_host,
        port=hive_port,
        database=hive_database,
        auth='NOSASL' # No authentication
    )
    print('Connected to Hive successfully')
    return conn


def celect_from_hive(conn):
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM your_hive_table') 
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return results

def insert_into_hive(conn, data):
    cursor = conn.cursor()
    insert_query = "INSERT INTO table (column1, column2, column3) VALUES (%s, %s, %s)"

    try:
        cursor.execute(insert_query, data)
        conn.commit()
        print('Data inserted into Hive successfully')
    except Exception as e:
        print(f"Error inserting data into Hive: {e}")
    finally:
        cursor.close()
        conn.close()

conn = connect_to_hive()
results = celect_from_hive(conn)
print("DONE-----------------------------------------")