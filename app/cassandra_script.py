from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from cassandra.cluster import Cluster
import logging

print("0-----------------------------------------------")

spark = SparkSession.builder \
            .appName('HdfsToCassandra') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1") \
            .config('spark.cassandra.connection.host', 'cassandra') \
            .getOrCreate()

print("1-----------------------------------------------")

def process_data(data_path, data):
    if "time" in data.columns:
        data = data.withColumn("time", col("time").cast("timestamp"))
    else:
        raise ValueError("Column 'time' not found in the DataFrame.")

    melted_df = data.selectExpr("time", "stack(7, 'zone_1', zone_1, 'zone_2', zone_2, \
                                'zone_3', zone_3, 'zone_4', zone_4, 'zone_5', zone_5, \
                                'zone_6', zone_6, 'zone_7', zone_7) as (zone, value)")
    melted_df = melted_df.withColumn("value", col("value").cast("double"))
    melted_df = melted_df.withColumn("id_zone", melted_df["zone"].substr(-1, 1).cast("int"))
    melted_df = melted_df.drop("zone")

    # Group by time and id_zone, then calculate max, mean, and total
    result_df = melted_df.groupBy("time", "id_zone").agg({"value": "min", "value": "max", "value": "mean", "value": "sum"})
    result_df = result_df.withColumnRenamed("min(value)", "min").withColumnRenamed("max(value)", "max").withColumnRenamed("avg(value)", "mean").withColumnRenamed("sum(value)", "total")

    # result_df.show()
    return result_df

def insert_processed_data(session, row):
    print("Inserting data...")

    time = row['time']
    id_zone = row['id_zone']
    min = row['min']
    max = row['max']
    mean = row['mean']
    total = row['total']

    try:
        session.execute("""
            INSERT INTO spark_streams.stats(time, id_zone, zone_2, zone_3, 
                zone_4, zone_5, zone_6, zone_7)
                VALUES (%s, %s, %s, %s, %s, %s)
        """, (time, id_zone, min, max, mean, total))
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
            insert_processed_data(session, row)


# Read JSON data from HDFS
input_data = "hdfs:///user/root/rtdbpc/data_2024-01-03_13-22-55-00.json"
df = spark.read.json(input_data)

# Perform transformations
df_transformed = df.select("time", "zone_1", "zone_2", "zone_3", "zone_4", "zone_5", "zone_6", "zone_7")
df_transformed.show()

df_stats = process_data(input_data, df)
df_stats.show()

print("2-----------------------------------------------")

# Create a Cassandra session
cassandra_session = create_cassandra_connection()
if cassandra_session is not None:
    logging.info("Streaming is being started...")

    print("3-----------------------------------------------")

    # df_transformed.foreachPartition(lambda rows: process_partition(rows, create_cassandra_connection()))
    df_stats.foreachPartition(lambda rows: process_partition(rows, create_cassandra_connection()))

    print("4-----------------------------------------------")

# Stop the Spark session
spark.stop()
print("BYE-----------------------------------------------")