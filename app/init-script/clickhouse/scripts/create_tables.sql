-- Create Market Database
CREATE DATABASE IF NOT EXISTS Market;
USE Market;

-- Create Zone table
CREATE TABLE IF NOT EXISTS Zone
(
    id_zone UInt32,
    name_zone String
) ENGINE = MergeTree Primary Key id_zone;

-- Create Record table
CREATE TABLE IF NOT EXISTS Record
(
    id_record String,
    time DateTime,
    client_count UInt32,
    id_zone UInt32
) ENGINE = MergeTree ORDER BY time;

-- Insert data into Zone table
INSERT INTO Zone (id_zone, name_zone) VALUES (1, 'zone_1');
INSERT INTO Zone (id_zone, name_zone) VALUES (2, 'zone_2');
INSERT INTO Zone (id_zone, name_zone) VALUES (3, 'zone_3');
INSERT INTO Zone (id_zone, name_zone) VALUES (4, 'zone_4');
INSERT INTO Zone (id_zone, name_zone) VALUES (5, 'zone_5');
INSERT INTO Zone (id_zone, name_zone) VALUES (6, 'zone_6');
INSERT INTO Zone (id_zone, name_zone) VALUES (7, 'zone_7');