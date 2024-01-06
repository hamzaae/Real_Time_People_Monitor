from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = SparkSession.builder.appName("rtdbpc").getOrCreate()



# Show the schema to inspect column names
# data.printSchema()

def process_data(data_path):
    data = spark.read.format('json').load(data_path)

    if "time" in data.columns:
        data = data.withColumn("time", col("time").cast("timestamp"))
    else:
        raise ValueError("Column 'time' not found in the DataFrame.")

    melted_df = data.selectExpr("time", "stack(4, 'zone_1', zone_1, 'zone_2', zone_2, 'zone_3', zone_3, 'zone_4', zone_4) as (zone, value)")
    melted_df = melted_df.withColumn("value", col("value").cast("double"))
    melted_df = melted_df.withColumn("id_zone", melted_df["zone"].substr(-1, 1).cast("int"))
    melted_df = melted_df.drop("zone")

    # Group by time and id_zone, then calculate max, mean, and total
    result_df = melted_df.groupBy("time", "id_zone").agg({"value": "max", "value": "mean", "value": "sum"})
    result_df = result_df.withColumnRenamed("max(value)", "max").withColumnRenamed("avg(value)", "mean").withColumnRenamed("sum(value)", "total")

    # result_df.show()
    return result_df




from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a Spark session
spark = SparkSession.builder.appName("HdfsToHive").getOrCreate()

# Read JSON data from HDFS
input_data = "hdfs://data_2024-01-03_13-16-48.json.json"
df = spark.read.json(input_data)

# Perform transformations
# For example, you can select specific columns and add additional transformations here
df_transformed = df.select("time", "zone_1", "zone_2", "zone_3", "zone_4", "zone_5", "zone_6", "zone_7")

# Stop the Spark session
spark.stop()
