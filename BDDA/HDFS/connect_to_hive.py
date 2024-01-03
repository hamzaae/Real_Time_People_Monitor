from pyhive import hive

# Set the connection parameters
hive_host = 'localhost'
hive_port = 10000
hive_database = 'project_db'  # Replace with your Hive database name

# Create a connection without a password using NOSASL authentication
conn = hive.Connection(
    host=hive_host,
    port=hive_port,
    database=hive_database,
    auth='NOSASL'  # Use NOSASL authentication mechanism
)

print('Connected to Hive successfully')

# Create a cursor
cursor = conn.cursor()

# Execute a query
cursor.execute('SELECT * FROM your_table')  # Replace 'your_table' with your actual Hive table name

# Fetch results
results = cursor.fetchall()

# Print results
for row in results:
    print(row)

# Close cursor and connection
cursor.close()
conn.close()
