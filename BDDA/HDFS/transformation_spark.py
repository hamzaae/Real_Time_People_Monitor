from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a Spark session
spark = SparkSession.builder.appName("example").getOrCreate()

# Read JSON data into a PySpark DataFrame
data = spark.read.json("data.json")


# Show the schema to inspect column names
data.printSchema()

# Convert 'time' column to timestamp
if "time" in data.columns:
    data = data.withColumn("time", col("time").cast("timestamp"))
else:
    raise ValueError("Column 'time' not found in the DataFrame.")

# Melt the DataFrame to transform it
melted_df = data.selectExpr("time", "stack(4, 'zone_1', zone_1, 'zone_2', zone_2, 'zone_3', zone_3, 'zone_4', zone_4) as (zone, value)")
melted_df = melted_df.withColumn("value", col("value").cast("double"))
melted_df = melted_df.withColumn("id_zone", melted_df["zone"].substr(-1, 1).cast("int"))
melted_df = melted_df.drop("zone")

# Group by time and id_zone, then calculate max, mean, and total
result_df = melted_df.groupBy("time", "id_zone").agg({"value": "max", "value": "mean", "value": "sum"})
result_df = result_df.withColumnRenamed("max(value)", "max").withColumnRenamed("avg(value)", "mean").withColumnRenamed("sum(value)", "total")

# Show the result
result_df.show()
