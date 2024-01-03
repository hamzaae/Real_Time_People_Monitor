from hdfs import InsecureClient

hdfs_host = 'localhost'
hdfs_port = 50070

# Create an HDFS client
client = InsecureClient(f'http://{hdfs_host}:{hdfs_port}')

def hdsf_consumer():
    local_file_path = 'data.json'
    hdfs_file_path = 'data.json'  # HDFS destination

    with open(local_file_path, 'rb') as local_file:
        client.write(hdfs_file_path, local_file)

    print(f"File uploaded to HDFS: {hdfs_file_path}")