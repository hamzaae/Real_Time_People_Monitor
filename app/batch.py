import json
import logging
from kafka import KafkaConsumer
from hdfs import InsecureClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as F
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

    result_df.show()
    return result_df

def process_data2(data):
    df_aggregated = data.agg(
        F.min("zone_1").alias("zone_1_min"),
        F.max("zone_1").alias("zone_1_max"),
        F.sum("zone_1").alias("zone_1_total"),
        F.avg("zone_1").alias("zone_1_avg"),

        F.min("zone_2").alias("zone_2_min"),
        F.max("zone_2").alias("zone_2_max"),
        F.sum("zone_2").alias("zone_2_total"),
        F.avg("zone_2").alias("zone_2_avg"),

        F.min("zone_3").alias("zone_3_min"),
        F.max("zone_3").alias("zone_3_max"),
        F.sum("zone_3").alias("zone_3_total"),
        F.avg("zone_3").alias("zone_3_avg"),

        F.min("zone_4").alias("zone_4_min"),
        F.max("zone_4").alias("zone_4_max"),
        F.sum("zone_4").alias("zone_4_total"),
        F.avg("zone_4").alias("zone_4_avg"),

        F.min("zone_5").alias("zone_5_min"),
        F.max("zone_5").alias("zone_5_max"),
        F.sum("zone_5").alias("zone_5_total"),
        F.avg("zone_5").alias("zone_5_avg"),

        F.min("zone_6").alias("zone_6_min"),
        F.max("zone_6").alias("zone_6_max"),
        F.sum("zone_6").alias("zone_6_total"),
        F.avg("zone_6").alias("zone_6_avg"),

        F.min("zone_7").alias("zone_7_min"),
        F.max("zone_7").alias("zone_7_max"),
        F.sum("zone_7").alias("zone_7_total"),
        F.avg("zone_7").alias("zone_7_avg")
    )


    df_aggregated.show()

    return df_aggregated

def insert_processed_data(session, row):
    print("Inserting data...")
    print("row : ", row)
    time = row['time']

    for zone_id in range(1, 8):
        id_zone = zone_id
        max_value = row[f'zone_{id_zone}_max']
        mean_value = row[f'zone_{id_zone}_avg']
        total_value = row[f'zone_{id_zone}_total']
        min_value = row[f'zone_{id_zone}_min']

        try:
            session.execute("""
                INSERT INTO sparkstreams.stats(time, id_zone, min, max, 
                    mean, total)
                    VALUES (%s, %s, %s, %s, %s, %s)
            """, (time, id_zone, min_value, max_value, mean_value, total_value))

            logging.info(f"Data inserted for time: {time}, id_zone: {id_zone}")
            print(f"Data inserted for time: {time}, id_zone: {id_zone}")

        except Exception as e:
            logging.error(f'Could not insert data for id_zone {id_zone} due to {e}')
            print(f'Could not insert data for id_zone {id_zone} due to {e}')

def create_cassandra_connection():
    try:
        # connecting to the cassandra cluster
        cluster = Cluster(['cassandra'])

        cas_session = cluster.connect()
        print("Cassandra Connection established successfully!")

        return cas_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        print(f"Could not create cassandra connection due to {e}")

        return None

def process_partition(rows, session):
        for row in rows:
            insert_processed_data(session, row)

def process_ini_data(file_name):
    # Read JSON data from HDFS
    input_data = "hdfs:///user/root/rtdbpc/" + file_name
    df = spark.read.json(input_data)

    df_transformed = df.select("time", "zone_1", "zone_2", "zone_3", "zone_4", "zone_5", "zone_6", "zone_7")
    df_transformed.show()

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
                df_stats = process_data2(df)
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

