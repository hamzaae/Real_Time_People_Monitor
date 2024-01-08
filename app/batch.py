import json
import logging
from kafka import KafkaConsumer
from hdfs import InsecureClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from cassandra.cluster import Cluster


def create_spark_session():
    spark = SparkSession.builder \
        .appName('HdfsToCassandra') \
        .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1") \
        .config('spark.cassandra.connection.host', 'cassandra') \
        .getOrCreate()
    print("Spark session created!")
    return spark

def process_data(data):
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
            INSERT INTO spark_streams.stats(time, id_zone, min, max, 
                mean, total)
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

def process_ini_data(file_name):
    # Read JSON data from HDFS
    input_data = "hdfs:///user/root/rtdbpc/" + file_name
    df = spark.read.json(input_data)

    df_transformed = df.select("time", "zone_1", "zone_2", "zone_3", "zone_4", "zone_5", "zone_6", "zone_7")
    # df_transformed.show()

    return df_transformed

def save_to_hdfs(data, timestamp):
    hdfs_host = 'localhost'
    hdfs_port = 50070
    client = InsecureClient(f'http://{hdfs_host}:{hdfs_port}')

    hdfs_file_path = f'rtdbpc/data_{timestamp}.json'
    data_json = json.dumps(data)

    with client.write(hdfs_file_path, encoding='utf-8') as hdfs_file:
        hdfs_file.write(data_json)
    print(f"File uploaded to HDFS: {hdfs_file_path}")

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
        logging.info("Streaming is being started...")
        print("Streaming is being started...")
        # Iterate over the streamed data
        current_minute = None
        accumulated_data = []
        for message in consumer:
            print("Message received!---------------------------")
            data_point = eval(message.value)
            timestamp_str = data_point.get('time', '')
            data_point_minute = timestamp_str.split(':')[1]

            if current_minute is None:
                current_minute = data_point_minute
            if current_minute != data_point_minute:
                # Save the accumulated data to HDFS here
                namee = timestamp_str.replace(' ', '_').replace(':', '-').rsplit(':', 1)[0] + '-00'
                save_to_hdfs(accumulated_data, namee)
                
                # Process the accumulated data and save it to Cassandra
                hdfs_file_name = f'data_{namee}.json'
                df = process_ini_data(hdfs_file_name)
                df_stats = process_data(df)
                df_stats.foreachPartition(lambda rows: process_partition(rows, create_cassandra_connection()))

                # Reset the current minute and accumulated data
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


if __name__ == "__main__":
    print("HI-----------------------------------------------")
    # Create a Spark session
    spark = create_spark_session()

    # Create a Cassandra session
    cassandra_session = create_cassandra_connection()
    if cassandra_session is not None:
        consume_messages("rtdbTopic")

    # Stop the Spark session
    spark.stop()
    print("BYE-----------------------------------------------")

