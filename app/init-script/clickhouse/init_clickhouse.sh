#!/bin/bash

clickhouse-client --multiquery --queries-file=/docker-entrypoint-initdb.d/scripts/create_tables.sql

apt-get update 
apt-get install -y python3 python3-pip
pip3 install kafka-python clickhouse-connect
python3 /app/clickhouse.py