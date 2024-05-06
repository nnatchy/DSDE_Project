from kafka import KafkaProducer
import time
import json

producer = KafkaProducer(
    # bootstrap_servers=['localhost:9092'],
    bootstrap_servers=['kafka1:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
    request_timeout_ms=60000,  # Increase timeout to 60 seconds
    retry_backoff_ms=500,  # Increase backoff time between retries
)

for i in range(100):
    message = {'number': i}
    try:
        producer.send('test-topic', value=message).get(timeout=30)  # Wait up to 30 seconds
        print(f"Sent: {message}")
    except Exception as e:
        print(f"Failed to send message: {str(e)}")
    time.sleep(1)

producer.flush()