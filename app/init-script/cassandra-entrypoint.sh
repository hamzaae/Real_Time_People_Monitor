#!/bin/bash

# Wait for cassandra to start
until cqlsh -e "describe cluster" ; do
  echo "Cassandra is unavailable - sleeping"
  sleep 2
done

cqlsh -e "CREATE KEYSPACE IF NOT EXISTS spark_streams WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};"

cqlsh -e "CREATE TABLE IF NOT EXISTS spark_streams.created_zones (
  time TEXT PRIMARY KEY,
  zone_1 INT,
  zone_2 INT,
  zone_3 INT,
  zone_4 INT,
  zone_5 INT,
  zone_6 INT,
  zone_7 INT
);"
