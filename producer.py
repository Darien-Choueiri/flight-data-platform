import glob
import json
import os
import time
import pandas as pd
from confluent_kafka import Producer

PARQUET_FILES = os.environ.get("PARQUET_FILES", "parquet_monthly/*.parquet")
SPEEDUP = float(os.environ.get("SPEEDUP", "86400"))
TOPIC = "flights"
KAFKA_SERVER = "kafka:19092"
PRINT_SECONDS = 5

COLUMNS = ["callsign", "icao24", "origin", "destination", "firstseen", "day"]
TEXT_COLUMNS = ["callsign", "icao24", "origin", "destination"]


def load_month(path):
    month = pd.read_parquet(path, columns=COLUMNS)
    month["takeoff"] = pd.to_datetime(month["firstseen"], utc=True)
    month = month.dropna(subset=["takeoff"]).sort_values("takeoff", kind="stable")
    month_start = pd.to_datetime(month["day"], utc=True).min()
    month["seconds_in"] = (month["takeoff"] - month_start).dt.total_seconds().clip(lower=0)
    month["firstseen"] = month["takeoff"].dt.strftime("%Y-%m-%d %H:%M:%S")
    month[TEXT_COLUMNS] = month[TEXT_COLUMNS].fillna("")
    return month


months = sorted(glob.glob(PARQUET_FILES))
if not months:
    print(f"No Parquet files found at {PARQUET_FILES}. Run the Spark job first.")
    raise SystemExit(1)
print(f"Found {len(months)} month(s) to stream.")

producer = Producer({"bootstrap.servers": KAFKA_SERVER})

delivered = 0
failed = 0


def on_delivery(err, msg):
    global delivered, failed
    if err is None:
        delivered += 1
    else:
        failed += 1


rows_read = 0
sent = 0
last_print = time.time()
try:
    for path in months:
        month = load_month(path)
        rows_read += len(month)
        print(f"Replaying {os.path.basename(path)}: {len(month):,} flights at {SPEEDUP:g}x speed")
        start = time.time()
        for flight in month.itertuples(index=False):
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

            if time.time() - last_print >= PRINT_SECONDS:
                print(f"sent {sent:,} | flight time {flight.firstseen[:16]}")
                last_print = time.time()
except KeyboardInterrupt:
    print("Stopped early.")

producer.flush()
print(f"Done. Read {rows_read:,} from Parquet | sent {sent:,} | Kafka confirmed {delivered:,} | failed {failed:,}")
