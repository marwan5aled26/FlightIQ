import csv
import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = "flight-data"
count = 0

with open("data/flights-streaming.csv", "r", encoding="utf-8") as file:
    reader = csv.DictReader(file)
    for row in reader:
        producer.send(topic, value=row)
        count += 1
        print(f"Sent {count}: {row['FL_DATE']} | {row['ORIGIN']} -> {row['DEST']}")

producer.flush()
producer.close()
print(f"Total records sent: {count}")