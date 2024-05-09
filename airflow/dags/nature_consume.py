from kafka import KafkaConsumer
import json
import time
import os
import datetime
import csv

def receive_nature_from_kafka(timeout=20):
    output_dir = "out/nature"
    output_file = os.path.join(output_dir, "output_nature.csv")
    topic_name = 'nature-topic'
    consumer = KafkaConsumer(
        topic_name,
        group_id='nature_consumer_group',  # Set a group ID for the consumer
        bootstrap_servers=['kafka1:19092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,  # Enable auto commit
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    os.makedirs(output_dir, exist_ok=True)
    print("Starting the consumer for nature...")
    data = []
    start_time = time.time()

    try:
        while True:
            message_found = False
            for message in consumer.poll(timeout_ms=10000).values():
                for msg in message:
                    data.append(msg.value)
                    print(f'receive data msg: {msg.value}')
                    message_found = True

            if message_found:
                start_time = time.time()

            if time.time() - start_time > timeout:
                print("Timeout reached. Stopping consumer.")
                break

            if not data:
                print("Waiting for data...")
                continue

    finally:
        consumer.close()

    if data:
        if isinstance(data[0], list):
            data = [item for sublist in data for item in sublist]  # Flatten the list
        keys = data[0].keys()  # Assumes that all dictionaries have the same structure
        with open(output_file, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=keys)
            writer.writeheader()
            for item in data:
                writer.writerow(item)
        print(f"Data written to CSV at {output_file}.")
    else:
        print("No data received.")

    return output_file