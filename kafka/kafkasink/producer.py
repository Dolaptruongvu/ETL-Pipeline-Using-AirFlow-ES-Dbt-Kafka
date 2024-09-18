import requests
from kafka import KafkaProducer
import json
import time

# Set up Elasticsearch and Kafka
ELASTICSEARCH_URL = ''
KAFKA_TOPIC = 'accounts'
KAFKA_BOOTSTRAP_SERVERS = ''

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Fetch data from Elasticsearch with scroll
def fetch_elasticsearch_data(scroll_id=None):
    if scroll_id:
        # Fetch the next batch of data using the scroll ID
        response = requests.post(f'{ELASTICSEARCH_URL}/_search/scroll', json={
            "scroll": "1m",
            "scroll_id": scroll_id
        })

    else:
        # Fetch the initial batch of data
        response = requests.post(f'{ELASTICSEARCH_URL}/data/_search?scroll=1m', json={
            "query": {
                "match_all": {}
            },
            "size": 300
        })
    
    response_data = response.json()
    # print(json.dumps(response_data, indent=2))
    return response_data

# Send data to Kafka
def send_to_kafka(records):
    for record in records:
        producer.send(KAFKA_TOPIC, key=str(record['_id']).encode('utf-8'), value=record['_source'])
        print(f"Sent record with key {record['_id']} to topic {KAFKA_TOPIC}")

# Main
if __name__ == "__main__":
    scroll_id = None

    while True:
        # Fetch data from Elasticsearch
        data = fetch_elasticsearch_data(scroll_id)
         
        # Send data to Kafka
        send_to_kafka(data['hits']['hits'])

        # Update scroll_id
        scroll_id = data['_scroll_id']

        # Check if there is no more data
        if len(data['hits']['hits']) == 0:
            break
        time.sleep(30)

    # Ensure all messages are sent
    producer.flush()
