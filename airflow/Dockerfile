# Sử dụng hình ảnh cơ bản của Airflow
FROM apache/airflow:2.5.0

# Chuyển qua user root để cài đặt các gói cần thiết
USER root

# Cài đặt Python 3.9 và các gói cần thiết
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

# Cấu hình Python 3.9 làm mặc định
RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.9 1 \
    && update-alternatives --install /usr/bin/python python /usr/bin/python3.9 1

# Cài đặt pip và dbt với phiên bản tương thích dưới quyền root
RUN python3.9 -m pip install --upgrade pip

# Đổi lại quyền cho user airflow
USER airflow

# Cài đặt dbt-core, dbt-postgres, psycopg2-binary, và kafka-python
RUN python3.9 -m pip install --user dbt-core==1.8.0 dbt-postgres==1.8.0 psycopg2-binary kafka-python

# Thêm thư mục cài đặt pip vào PATH cho user airflow
ENV PATH="/home/airflow/.local/bin:$PATH"

# Kiểm tra xem kafka-python đã được cài đặt thành công chưa
RUN python3.9 -c "from kafka import KafkaProducer; print('Kafka-python module installed successfully')"

# Kiểm tra phiên bản dbt đã được cài đặt
RUN dbt --version
