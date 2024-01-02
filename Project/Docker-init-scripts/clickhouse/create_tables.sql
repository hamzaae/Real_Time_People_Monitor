-- Create Market Database
CREATE Database Market ;
USE Market;

-- Create Zone table
CREATE TABLE IF NOT EXISTS Zone
(
    id_zone UInt32,
    name_zone String
) ENGINE = MergeTree ORDER BY id_zone;

-- Create Record table
CREATE TABLE IF NOT EXISTS Record
(
    id_record UInt32,
    time DateTime,
    client_count UInt32,
    id_zone UInt32
) ENGINE = MergeTree ORDER BY (id_record, id_zone);
