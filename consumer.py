import csv
import heapq
import json
from confluent_kafka import Consumer, KafkaError

TOPIC = "flights"
KAFKA_SERVER = "kafka:19092"
PRINT_EVERY = 50000
OUTPUT_FILE = "stream_counts.csv"

consumer = Consumer({
    "bootstrap.servers": KAFKA_SERVER,
    "group.id": "flight-tally",
    "auto.offset.reset": "earliest",
})
consumer.subscribe([TOPIC])

counts = {}
received = 0
skipped = 0
latest_time = ""
quiet_seconds = 0

def print_summary():
    top = heapq.nlargest(5, counts.items(), key=lambda item: item[1])
    print(f"--- {received:,} received | {skipped:,} skipped (no origin) | flight time {latest_time} | top 5 airport-hours ---")
    for (airport, hour), n in top:
        print(f"    {airport}  {hour}  {n} flights")

def save_counts():
    with open(OUTPUT_FILE, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["airport", "hour", "flights"])
        for (airport, hour), n in counts.items():
            writer.writerow([airport, hour, n])

print("Waiting for flights... (Ctrl+C to stop)")
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            quiet_seconds += 1
            if quiet_seconds == 10 and received > 0:
                print("(stream is quiet)")
                print_summary()
                save_counts()
            continue
        if msg.error():
            if msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                continue
            print("error:", msg.error())
            continue
        quiet_seconds = 0

        flight = json.loads(msg.value())
        received += 1
        latest_time = flight["firstseen"][:16]

        airport = flight["origin"]
        if airport == "":
            skipped += 1
        else:
            hour = flight["firstseen"][:13] + ":00"
            key = (airport, hour)
            counts[key] = counts.get(key, 0) + 1

        if received % PRINT_EVERY == 0:
            print_summary()

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
    print_summary()
    save_counts()
    print(f"Stopped. Received {received:,} | skipped {skipped:,} | tally saved to {OUTPUT_FILE}")
