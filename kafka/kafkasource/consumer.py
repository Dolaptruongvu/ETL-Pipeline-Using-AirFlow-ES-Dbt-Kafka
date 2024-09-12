from kafka import KafkaConsumer
import psycopg2
import json

KAFKA_BOOTSTRAP_SERVERS = ''
KAFKA_TOPIC = 'accounts'
POSTGRES_HOST = ''
POSTGRES_PORT = 21272
POSTGRES_USER = ''
POSTGRES_PASSWORD = ''
POSTGRES_DATABASE = 'accountsdb'

# Connect to PostgreSQL
conn = psycopg2.connect(
    host=POSTGRES_HOST,
    user=POSTGRES_USER,
    port=POSTGRES_PORT,
    password=POSTGRES_PASSWORD,
    dbname=POSTGRES_DATABASE
)
cursor = conn.cursor()

# Create Kafka Consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Function to insert data into PostgreSQL
def insert_into_postgres(record):
    account_number = record.get('account_number')
    balance = record.get('balance')
    firstname = record.get('firstname') if record.get('firstname', "") != "" else None
    lastname = record.get('lastname')
    age = record.get('age')
    gender = record.get('gender')
    address = record.get('address')
    employer = record.get('employer')
    email = record.get('email')
    city = record.get('city')
    state = record.get('state')

    cursor.execute("""
        INSERT INTO accounts 
        (account_number, balance, firstname, lastname, age, gender, address, employer, email, city, state) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (account_number) DO NOTHING
    """, (account_number, balance, firstname, lastname, age, gender, address, employer, email, city, state))
    conn.commit()
    print(f"Inserted account {account_number} into PostgreSQL")

# Listen to messages from Kafka and insert into PostgreSQL
for message in consumer:
    record = message.value
    if record and 'account_number' in record:
        insert_into_postgres(record)

# Close PostgreSQL and Kafka connections
cursor.close()
conn.close()
