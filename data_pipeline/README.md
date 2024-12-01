
Data Pipeline using PySpark and Hive:

data-pipeline/
├── Jenkinsfile           # Jenkins pipeline configuration file
├── src/
├── tests/
├── airflow/
└── README.md
|__ Requirements.txt


Overview:

This project implements a data processing pipeline using PySpark, Hive, and HDFS. The pipeline:

1.Reads raw data from HDFS.
2.Transforms the data (e.g., concatenates columns).
3.Writes the processed data to a Hive table for querying and analysis.
4.Logging is integrated to track execution steps, and error handling is implemented for robust operation.

Technologies Used: 

PySpark: For distributed data processing.
Hive: For storing and querying processed data.
HDFS: For raw data storage.
Logging: For tracking and debugging pipeline execution.