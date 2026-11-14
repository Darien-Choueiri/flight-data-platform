import json
import os
import time
import pandas as pd
from confluent_kafka import Producer

DATA_FILE = os.environ.get("DATA_FILE", "flightlist_20220601_20220630.csv.gz")
SPEEDUP = float(os.environ.get("SPEEDUP", "3600"))
MAX_FLIGHTS = int(os.environ.get("MAX_FLIGHTS", "0"))
TOPIC = "flights"
KAFKA_SERVER = "kafka:19092"
PRINT_EVERY = 50000

COLUMNS = ["callsign", "icao24", "origin", "destination", "firstseen", "day"]

print(f"Reading {DATA_FILE} ...")
flights = pd.read_csv(DATA_FILE, usecols=COLUMNS, dtype=str).fillna("")
rows_read = len(flights)
print(f"Read {rows_read:,} flights from the file.")

flights["takeoff"] = pd.to_datetime(flights["firstseen"], format="%Y-%m-%d %H:%M:%S%z", utc=True)
flights = flights.sort_values("takeoff", kind="stable")

month_start = pd.to_datetime(flights["day"], format="%Y-%m-%d %H:%M:%S%z", utc=True).min()
flights["seconds_in"] = (flights["takeoff"] - month_start).dt.total_seconds().clip(lower=0)

if MAX_FLIGHTS > 0:
    flights = flights.head(MAX_FLIGHTS)

producer = Producer({"bootstrap.servers": KAFKA_SERVER})

delivered = 0
failed = 0

def on_delivery(err, msg):
    global delivered, failed
    if err is None:
        delivered += 1
    else:
        failed += 1

print(f"Replaying {len(flights):,} flights at {SPEEDUP:g}x speed ...")
start = time.time()
sent = 0
try:
    for flight in flights.itertuples(index=False):
        send_at = flight.seconds_in / SPEEDUP
        wait = send_at - (time.time() - start)
        if wait > 0:
            time.sleep(wait)

        message = json.dumps({
            "callsign": flight.callsign,
            "icao24": flight.icao24,
            "origin": flight.origin,
            "destination": flight.destination,
            "firstseen": flight.firstseen,
        })

        while True:
            try:
                producer.produce(TOPIC, value=message, on_delivery=on_delivery)
                break
            except BufferError:
                producer.poll(0.5)
        producer.poll(0)
        sent += 1

        if sent % PRINT_EVERY == 0:
            print(f"sent {sent:,} | flight time {flight.firstseen[:16]} | {time.time() - start:.0f}s real time")
except KeyboardInterrupt:
    print("Stopped early.")

producer.flush()
print(f"Done. Read {rows_read:,} from file | sent {sent:,} | Kafka confirmed {delivered:,} | failed {failed:,}")
