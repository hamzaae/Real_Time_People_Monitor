from pyspark.sql import SparkSession
from pyspark.sql.streaming import DataStreamWriter
from cassandra.cluster import Cluster
import logging

# Create a Spark session
# spark = SparkSession.builder.appName("HdfsToCassandra") \
#     .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.1.0") \
#     .config('spark.sql.catalog.client', 'com.datastax.spark.connector.datasource.CassandraCatalog') \
#     .config('spark.sql.catalog.client.spark.cassandra.connection.host', 'hadoop') \
#     .config('spark.sql.extensions', 'com.datastax.spark.connector.CassandraSparkExtensions') \
# .getOrCreate()

print("0-----------------------------------------------")

spark = SparkSession.builder \
            .appName('HdfsToCassandra') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1") \
            .config('spark.cassandra.connection.host', 'cassandra') \
            .getOrCreate()

# Read JSON data from HDFS
input_data = "hdfs:///user/root/rtdbpc/data_2024-01-03_13-16-48-00.json"
df = spark.read.json(input_data)

# Perform transformations
# For example, you can select specific columns and add additional transformations here
df_transformed = df.select("time", "zone_1", "zone_2", "zone_3", "zone_4", "zone_5", "zone_6", "zone_7")
df_transformed.show()




print("1-----------------------------------------------")
def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)
    print("Keyspace created successfully!")


def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.created_zones (
        time TEXT PRIMARY KEY,
        zone_1 INT,
        zone_2 INT,
        zone_3 INT,
        zone_4 INT,
        zone_5 INT,
        zone_6 INT,
        zone_7 INT);
    """)
    print("Table created successfully!")


def insert_data(session, row):
    print("Inserting data...")

    time = row['time']
    zone_1 = row['zone_1']
    zone_2 = row['zone_2']
    zone_3 = row['zone_3']
    zone_4 = row['zone_4']
    zone_5 = row['zone_5']
    zone_6 = row['zone_6']
    zone_7 = row['zone_7']

    try:
        session.execute("""
            INSERT INTO spark_streams.created_zones(time, zone_1, zone_2, zone_3, 
                zone_4, zone_5, zone_6, zone_7)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (time, zone_1, zone_2, zone_3, zone_4, zone_5, zone_6, zone_7))
        logging.info(f"Data inserted for time: {time}")

    except Exception as e:
        logging.error(f'Could not insert data due to {e}')

def create_cassandra_connection():
    try:
        # connecting to the cassandra cluster
        cluster = Cluster(['cassandra'])

        cas_session = cluster.connect()
        print("Cassandra Connection established successfully!")

        return cas_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None

def process_partition(rows, session):
        for row in rows:
            insert_data(session, row)
            # print("row-----------------------------------------------")
            # print(type(row['time']))
            # print(type(row['zone_1']))

# Create a Cassandra session
cassandra_session = create_cassandra_connection()
print("2-----------------------------------------------")
if cassandra_session is not None:
    # Create keyspace and table
    create_keyspace(cassandra_session)
    create_table(cassandra_session)

    logging.info("Streaming is being started...")

    print("3-----------------------------------------------")

    # df_transformed.foreach(lambda row: insert_data(cassandra_session, row))

    df_transformed.foreachPartition(lambda rows: process_partition(rows, create_cassandra_connection()))

    


    # df_transformed \
    # .writeStream \
    # .outputMode("append") \
    # .foreachBatch(lambda df, epoch_id: df.write \
    #               .format("org.apache.spark.sql.cassandra") \
    #               .options(table="created_zones", keyspace="spark_streams") \
    #               .mode("append") \
    #               .save())

    # df_transformed.write \
    #     .format("org.apache.spark.sql.cassandra") \
    #     .options(table="created_zones", keyspace="spark_streams") \
    #     .mode("append") \
    #     .save()


    print("4-----------------------------------------------")

# Stop the Spark session
spark.stop()
print("BYE-----------------------------------------------")