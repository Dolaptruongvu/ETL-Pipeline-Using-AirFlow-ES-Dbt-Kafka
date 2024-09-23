FROM apache/airflow:2.10.1

USER root

# Install necessary packages
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    libpython3-dev \
    python3-distutils \
    python3-pip \
    libpq-dev \
    && apt-get clean


# Upgrade pip and install psycopg2-binary
RUN python -m pip install --upgrade pip \
    && python -m pip install psycopg2-binary

USER airflow

# Install required Python libraries
RUN python -m pip install dbt-core==1.8.0 dbt-postgres==1.8.0 psycopg2-binary kafka-python-ng \
    && python -m pip install apache-airflow-providers-celery \
    && python -m pip install 'acryl-datahub-airflow-plugin[plugin-v2]'


# Configure PATH so that the airflow user can access installed libraries
ENV PATH="/home/airflow/.local/bin:$PATH"

# Verify that Kafka-python is installed successfully
RUN python -c "from kafka import KafkaProducer; print('Kafka-python module installed successfully')"

# Check dbt version
RUN dbt --version
