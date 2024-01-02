-- Create Market database
CREATE DATABASE market;

-- Switch to market database
\c market;

-- Create Zone table
CREATE TABLE IF NOT EXISTS Zone
(
    id_zone SERIAL PRIMARY KEY,
    name_zone VARCHAR(255)
);

-- Create Record table
CREATE TABLE IF NOT EXISTS Record
(
    id_record SERIAL PRIMARY KEY,
    time TIMESTAMPTZ,
    client_count INT,
    id_zone INT REFERENCES Zone(id_zone)
);
