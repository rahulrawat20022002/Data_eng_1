import os
from hdfs import InsecureClient

DATA_FOLDER_PATH = "C:\\Users\\rahul\\OneDrive\\Desktop\\Data engineering 1\\data"

# --- 2. HDFS Configuration ---
# Replace 'your_namenode_ip_or_hostname' with the actual IP address or hostname of your HDFS NameNode.
# The default WebHDFS port is usually 9870 or 50070.
HDFS_NAMENODE_URL = 'http://master-node:9870' # Example: 'http://192.168.1.100:9870'
HDFS_OUTPUT_DIR = '/amz_data/' # Path in HDFS where data will be stored
HDFS_OUTPUT_FILENAME = 'data_amz.csv' # Name of the CSV file in HDFS

local_csv_path='C:\\Users\\rahul\\OneDrive\\Desktop\\Data engineering 1\\data_amazon\\amazon_extracted_products_data.csv'


# Initialize HDFS client
try:
    client = InsecureClient(HDFS_NAMENODE_URL)
    print(f"Connected to HDFS NameNode at {HDFS_NAMENODE_URL}")
except Exception as e:
    print(f"Error connecting to HDFS: {e}")
    print("Please ensure your HDFS NameNode is running and the URL/port are correct.")
    # Exit or handle the error appropriately if HDFS connection is critical
    exit()

try:
        client.makedirs(HDFS_OUTPUT_DIR, permission=755) # Create directory if it doesn't exist
        print(f"HDFS directory '{HDFS_OUTPUT_DIR}' ensured.")
except Exception as e:
        print(f"Error creating HDFS directory: {e}")

hdfs_file_path = os.path.join(HDFS_OUTPUT_DIR, HDFS_OUTPUT_FILENAME)        



try:
        # The 'overwrite=True' argument will replace the file if it already exists.
        # Use 'overwrite=False' if you want to append or handle existing files differently.
        client.upload(hdfs_file_path, local_csv_path, overwrite=True)
        print(f"Successfully uploaded '{local_csv_path}' to HDFS at '{hdfs_file_path}'")
except Exception as e:
        print(f"Error uploading to HDFS: {e}")