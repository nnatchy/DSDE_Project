from kafka import KafkaConsumer
import json
import time
import os
import csv

def receive_scopus_from_kafka(year, timeout=20):
    """
    Receives messages from Kafka topic for a specific year. The timeout resets whenever a message is received.
    :param year: The year for which to receive messages.
    :param timeout: Time to wait in seconds for new messages before giving up after the last message received.
    """
    output_dir = "out/scopus"
    output_file = os.path.join(output_dir, f'output_scopus_{year}.csv')
    topic_name = f'scopus-topic-{year}'
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=['kafka1:19092'],
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    # Ensure directory exists
    os.makedirs(output_dir, exist_ok=True)

    print(f"Starting the consumer for {year}...")
    data = []
    start_time = time.time()

    try:
        while True:
            message_found = False
            for message in consumer.poll(timeout_ms=10000).values():
                for msg in message:
                    data.append(msg.value)
                    print(f"Received: {msg.value}")
                    message_found = True

            # Reset the timer if a message was found
            if message_found:
                start_time = time.time()

            # Check if the timeout has elapsed since the last message was received
            if time.time() - start_time > timeout:
                print(f"Timeout reached after {timeout} seconds of inactivity. Stopping consumer for {year}.")
                break

            if not data:  # If no data has been received yet, continue waiting
                print(f"No data received yet for {year}. Waiting...")
                continue

    finally:
        consumer.close()  # Ensure consumer is properly closed after the loop

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