import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta
from airflow.models.baseoperator import chain
from datahub_airflow_plugin.entities import Dataset, Urn

# Set Thailand timezone (Asia/Bangkok)
local_tz = pendulum.timezone("Asia/Bangkok")

# Default configuration for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024, 9, 12, tz="Asia/Bangkok"),  # Set start_date with Asia/Bangkok timezone
    'retries': 1,  # Number of retries if the task fails
    'retry_delay': timedelta(minutes=1),  # Delay between retries
}

# Initialize DAG with Thailand timezone, running every 60 minutes and not allowing multiple DAG runs at the same time
dag = DAG(
    'dbt_run_dag',
    default_args=default_args,
    description='Run dbt models in Asia/Bangkok timezone',
    schedule_interval="*/59 * * * *",  # Run every 60 minutes
    catchup=False,  # Do not backfill missed DAG runs
    concurrency=4,  # Limit number of tasks running concurrently in this DAG
    max_active_runs=1,  # Limit to only one DAG run at a time
)

# Kafka first producer (data from Elasticsearch to Kafka topic)
kafka_producer = BashOperator(
    task_id='kafka_producer',
    bash_command='python3.9 /opt/airflow/dags/kafka/kafkasink/producer.py',
    pool='default_pool',
    dag=dag,
    inlets=[Dataset(platform="elasticsearch", name="data", env="PROD")],  # Source Elasticsearch Index
    outlets=[Dataset(platform="kafka", name="kafka-cluster-1.accounts", env="PROD")],  # Destination Kafka Topic
)

# Kafka sink (data from Kafka topic to PostgreSQL)
kafka_sink = BashOperator(
    task_id='kafka_sink',
    bash_command='python3.9 /opt/airflow/dags/kafka/kafkasink/consumer.py',
    pool='default_pool',
    dag=dag,
    inlets=[Dataset(platform="kafka", name="kafka-cluster-1.accounts", env="PROD")],  # Source Kafka Topic
    outlets=[Dataset(platform="postgres", name="accountsdb.public.accounts", env="PROD")],  # Destination PostgreSQL Table
)

# Task to run dbt command (process data in PostgreSQL)
dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command='cd /opt/airflow/dags/dbtProcess && dbt run',
    pool='default_pool',
    dag=dag,
    inlets=[Dataset(platform="postgres", name="accountsdb.public.accounts", env="PROD")],  # Source PostgreSQL Table
    outlets=[Dataset(platform="postgres", name="accountsdb.public.check_null_first_name", env="PROD")],  # Destination PostgreSQL Table
)

# Final Kafka producer (send data back to Kafka topic)
kafka_producer2 = BashOperator(
    task_id='kafka_producer2',
    bash_command='python3.9 /opt/airflow/dags/kafka/kafkasource/producer.py',
    dag=dag,
    inlets=[Dataset(platform="postgres", name="accountsdb.public.check_null_first_name", env="PROD")],  # Source PostgreSQL Table
    outlets=[Dataset(platform="kafka", name="kafka-cluster-1.null-firstname", env="PROD")],  # Final Kafka Topic
)

# Define the task sequence
chain(kafka_sink, dbt_run, kafka_producer2)
