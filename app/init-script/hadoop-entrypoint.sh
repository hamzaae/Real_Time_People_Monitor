#!/bin/bash

apt-get update
apt-get install python3-pip -y
pip3 install kafka-python hdfs cassandra-driver
# spark-submit --conf spark.pyspark.python=python3 --conf spark.pyspark.driver.python=python3 /app/batch.py
bash