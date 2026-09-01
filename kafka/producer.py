import csv
import json
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = "flight-data"
delay = 0.01
count = 0

with open("data/flights.csv", "r", encoding="utf-8") as file:
    reader = csv.DictReader(file)

    for row in reader:
        producer.send(topic, value=row)

        count += 1

        print(
            f"Sent {count}: "
            f"{row['FL_DATE']} | "
            f"{row['ORIGIN']} -> {row['DEST']}"
        )

        time.sleep(delay)

producer.flush()
producer.close()

print()
print("Streaming finished!")
print(f"Total records: {count}")