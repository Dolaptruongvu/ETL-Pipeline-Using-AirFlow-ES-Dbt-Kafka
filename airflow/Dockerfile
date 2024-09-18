FROM apache/airflow:2.5.0


USER root


RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    gcc \
    python3.9 \
    python3.9-venv \
    python3.9-dev \
    libpython3.9-dev \
    python3-distutils \
    python3-pip \
    libpq-dev \
    && apt-get clean


RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.9 1 \
    && update-alternatives --install /usr/bin/python python /usr/bin/python3.9 1


RUN python3.9 -m pip install --upgrade pip


USER airflow


RUN python3.9 -m pip install --user dbt-core==1.8.0 dbt-postgres==1.8.0 psycopg2-binary kafka-python \
    && python3.9 -m pip install --user apache-airflow-providers-celery \
    && python3.9 -m pip install --user 'acryl-datahub-airflow-plugin[plugin-v2]'


ENV PATH="/home/airflow/.local/bin:$PATH"


RUN python3.9 -c "from kafka import KafkaProducer; print('Kafka-python module installed successfully')"


RUN dbt --version
