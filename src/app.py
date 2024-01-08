from pyspark.sql import SparkSession
from azure.storage.blob import BlobServiceClient
import os

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("PySparkSQLServerAndAzureDLGen2Example") \
    .config("spark.jars", "/usr/local/share/sqljdbc/sqljdbc_12.4/enu/jars/mssql-jdbc-12.4.2.jre8.jar") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.2.0,com.microsoft.azure:azure-storage:8.6.6") \
    .config("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem") \
    .config("fs.azure.account.key.stbiappsne.dfs.core.windows.net", os.environ.get("AZURE_STORAGE_KEY")) \
    .getOrCreate()

# Set log level
spark.sparkContext.setLogLevel("WARN")

# Azure Storage Blob client
connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
blob_service_client = BlobServiceClient.from_connection_string(connect_str)
container_name = "de-engineer-trial-intus"

def check_blob_exists(container_name, blob_name):
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(blob_name)
    try:
        blob_client.get_blob_properties()
        return True
    except Exception as e:
        return False

# Reads table fom sql db, lands in azure storage and confirms that data is written is storage
def read_and_write_table(table_name, output_folder):
    df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)
    blob_name = f"films/{output_folder}/{table_name}.csv"
    output_path = f"abfss://{container_name}@stbiappsne.dfs.core.windows.net/{blob_name}"
    df.write.mode("overwrite").csv(output_path)
    if check_blob_exists(container_name, blob_name):
        print(f"Success: Data for {table_name} written to {blob_name}")
    else:
        print(f"Error: Data for {table_name} not found in {blob_name}")

# Database connection properties
database = "FilmData"
user = "de_candidate"
password = "1ntu5-d4t4"
server = "de-engineer-trial-intus.database.windows.net"

# JDBC URL
jdbc_url = f"jdbc:sqlserver://{server}:1433;databaseName={database}"

# Connection properties
connection_properties = {
    "user": user,
    "password": password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Tables to be copied to adls
tables = {
    "actor": "actors",
    "category": "categories",
    "film": "films",
    "film_actor": "film_actors",
    "film_category": "film_categories",
    "inventory": "inventory",
    "language": "languages"
}

# Process each table
for table, folder in tables.items():
    read_and_write_table(table, folder)

# Stop the Spark session
spark.stop()
