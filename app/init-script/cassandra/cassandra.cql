-- Create Keyspace
CREATE KEYSPACE 
IF NOT EXISTS sparkstreams
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Use Keyspace
USE sparkstreams;

-- Create Zones Table
CREATE TABLE 
IF NOT EXISTS zones (
    id_zone INT PRIMARY KEY,
    name_zone TEXT
);

-- Create Stats Table
CREATE TABLE 
IF NOT EXISTS stats (
    id_zone INT,
    time TIMESTAMP,
    min DOUBLE,
    max DOUBLE,
    mean DOUBLE,
    total DOUBLE,
    PRIMARY KEY (time, id_zone)
);
