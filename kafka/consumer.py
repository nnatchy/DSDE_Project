from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'test-topic',
    # bootstrap_servers=['localhost:9092'],
    bootstrap_servers=['kafka1:9092'],
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Starting the consumer...")
for message in consumer:
    print(f"Received: {message.value}")
