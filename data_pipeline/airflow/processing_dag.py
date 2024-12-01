from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG
with DAG(
    'data_pipeline',
    default_args=default_args,
    description='Data processing pipeline with PySpark and Hive',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
) as dag:
    # Task 1: Run PySpark job
    run_pyspark = BashOperator(
        task_id='run_pyspark_job',
        bash_command='spark-submit --master yarn /path/to/ingest.py'
    )

    run_pyspark
