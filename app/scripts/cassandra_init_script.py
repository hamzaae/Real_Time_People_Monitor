from cassandra.cluster import Cluster
import logging

print("0-----------------------------------------------")

def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS sparkstreams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)
    print("Keyspace created successfully!")

def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS sparkstreams.zones (
        id_zone INT PRIMARY KEY,
        name_zone TEXT);
    """)
    print("Zones Table created successfully!")

def create_processed_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS sparkstreams.stats (
    id_zone INT,
    time TIMESTAMP,
    min DOUBLE,
    max DOUBLE,
    mean DOUBLE,
    total DOUBLE,
    PRIMARY KEY (time, id_zone));
    """)
    print("Stats Table created successfully!")

def init_data(session):
    print("Inserting data...")
    zones = [[1, "zone_1"], [2, "zone_2"], [3, "zone_3"], [4, "zone_4"], [5, "zone_5"], [6, "zone_6"], [7, "zone_7"]]
    for zone in zones:
        id_zone = zone[0]
        name_zone = zone[1]
        try:
            session.execute("""
                INSERT INTO sparkstreams.zones(id_zone, name_zone)
                    VALUES (%s, %s)
            """, (id_zone, name_zone))
            logging.info(f"Data inserted for time: {name_zone}")

        except Exception as e:
            logging.error(f'Could not insert data due to {e}')

def create_cassandra_connection():
    try:
        # connecting to the cassandra cluster
        cluster = Cluster(['cassandra'])

        cas_session = cluster.connect()
        print("Cassandra Connection established successfully!")

        return cas_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None

print("2-----------------------------------------------")

# Create a Cassandra session
cassandra_session = create_cassandra_connection()
if cassandra_session is not None:
    create_keyspace(cassandra_session)
    create_table(cassandra_session)
    create_processed_table(cassandra_session)
    logging.info("Streaming is being started...")

# Stop the Spark session
print("BYE-----------------------------------------------")