from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit

import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DataPipeline")

try:
    logger.info("Starting the PySpark job...")

    # Initialize SparkSession with Hive support
    spark = SparkSession.builder \
        .appName("DataPipeline") \
        .config("spark.sql.catalogImplementation", "hive") \
        .enableHiveSupport() \
        .getOrCreate()

    # Step 1: Read raw data from HDFS
    raw_data_path = "hdfs://namenode_host/path/to/raw_data.csv"
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("delimiter", ",") \
        .load(raw_data_path)

    # Step 2: Transform the data (example: concatenating name fields)
    df_transformed = df.withColumn("fullname", concat(col("first_name"), lit(" "), col("last_name")))

    # Step 3: Write the transformed data to a Hive table
    spark.sql("CREATE DATABASE IF NOT EXISTS my_database")
    spark.sql("USE my_database")
    df_transformed.write.mode("overwrite").saveAsTable("processed_data")

    print("Data pipeline executed successfully!")
    spark.stop()

    logger.info("PySpark job completed successfully!")
    
except Exception as e:
    logger.error(f"Error occurred: {str(e)}")
    raise
