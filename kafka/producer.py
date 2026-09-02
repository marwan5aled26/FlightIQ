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
        try:
            arr_delay = float(row.get('ARR_DELAY', 0) or 0)
        except:
            arr_delay = 0.0
        
        row['ARR_DELAY'] = arr_delay
        row['IS_DELAYED'] = 1 if arr_delay > 15 else 0

        producer.send(topic, value=row)
        count += 1

        print(
            f"Sent {count}: "
            f"{row['FL_DATE']} | "
            f"{row['ORIGIN']} -> {row['DEST']} | "
            f"ARR_DELAY: {row['ARR_DELAY']} | "
            f"IS_DELAYED: {row['IS_DELAYED']}"
        )

producer.flush()
producer.close()

print()
print("Streaming finished!")
print(f"Total records: {count}")