import json
from kafka import KafkaProducer

def send_to_kafka(article_data):
    producer = KafkaProducer(bootstrap_servers='kafka1:9092', value_serializer=lambda x: json.dumps(x).encode('utf-8'))
    for article in article_data:
        producer.send('test-topic', value=article)
    producer.flush()
