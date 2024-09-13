import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta

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

# Initialize DAG with Thailand timezone, running every 5 minutes and not allowing multiple DAG runs at the same time
dag = DAG(
    'dbt_run_dag',
    default_args=default_args,
    description='Run dbt models in Asia/Bangkok timezone',
    schedule_interval="*/40 * * * *",  # Run every 5 minutes
    catchup=False,  # Do not backfill missed DAG runs
    concurrency=4,  # Limit number of tasks running concurrently in this DAG
    max_active_runs=1,  # Limit to only one DAG run at a time
)


#kafka first producer
kafka_producer = BashOperator(
    task_id='kafka_producer',
    bash_command='python3.9 /opt/airflow/dags/kafka/kafkasink/producer.py',
    pool='default_pool',
    dag = dag
)

#kafka sinks
kafka_sink = BashOperator(
    task_id='kafka_sink',
    bash_command='python3.9 /opt/airflow/dags/kafka/kafkasink/consumer.py',
    pool='default_pool',
    dag = dag
)

# Task to run dbt command
dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command='cd /opt/airflow/dags/dbtProcess && dbt run',
    pool='default_pool',  # Use default pool
    dag=dag,
)

## kafka producer
kafka_producer2 = BashOperator(
    task_id='kafka_producer2',
    bash_command='python3.9 /opt/airflow/dags/kafka/kafkasource/producer.py',
    dag=dag
)

# after sink , dbt work

kafka_sink >> dbt_run

# after dbt , kafka producer work
dbt_run >> kafka_producer2
