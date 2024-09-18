from kafka import KafkaProducer
import psycopg2
import json

# PostgreSQL configuration
POSTGRES_HOST = ''
POSTGRES_PORT = 21272
POSTGRES_USER = ''
POSTGRES_PASSWORD = ''
POSTGRES_DATABASE = 'accountsdb'

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = ''
KAFKA_TOPIC = 'null-firstname'

# Number of records in each batch
BATCH_SIZE = 8

# Connect to PostgreSQL
conn = psycopg2.connect(
    host=POSTGRES_HOST,
    user=POSTGRES_USER,
    port=POSTGRES_PORT,
    password=POSTGRES_PASSWORD,
    dbname=POSTGRES_DATABASE
)
cursor = conn.cursor()

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Function to fetch data in batches
def fetch_batch(offset, limit):
    query = """
    SELECT account_number 
    FROM check_null_first_name 
    ORDER BY account_number 
    LIMIT %s OFFSET %s
    """
    cursor.execute(query, (limit, offset))
    return cursor.fetchall()

# Initialize initial offset
offset = 0

while True:
    # Fetch data in batch
    rows = fetch_batch(offset, BATCH_SIZE)
    
    # Break the loop if no more data
    if not rows:
        break
    
    # Send each account_number in batch to Kafka
    for row in rows:
        account_number = row[0]
        producer.send(KAFKA_TOPIC, {'account_number': account_number})
    
    # Flush Kafka messages and ensure they are sent
    producer.flush()

    # Increase offset to fetch the next batch
    offset += BATCH_SIZE

# Close PostgreSQL and Kafka connections
cursor.close()
conn.close()
producer.close()
print("Producer stopped successfully.")