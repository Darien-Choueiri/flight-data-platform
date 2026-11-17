import glob
import json
import os
import time

import pyarrow.compute as pc
import pyarrow.parquet as pq
from confluent_kafka import Producer

PARQUET_FILES = os.environ.get("PARQUET_FILES", "parquet_monthly/*.parquet")
SPEEDUP = float(os.environ.get("SPEEDUP", "432000"))  # seconds of flight time per real second. 0 = no pacing
BATCH_ROWS = 50000          # flights read from Parquet into memory at a time
FLIGHTS_PER_MESSAGE = 1000  # flights packed into one Kafka message
TOPIC = "flights"
KAFKA_SERVER = "kafka:19092"
PRINT_SECONDS = 5

TEXT_COLUMNS = ["callsign", "icao24", "origin", "destination"]


def read_batches(month_path):
    # Spark saves each month as a folder of part files. Read them 50,000 flights at a time.
    for part in sorted(glob.glob(os.path.join(month_path, "*.parquet"))):
        yield from pq.ParquetFile(part).iter_batches(batch_size=BATCH_ROWS, columns=TEXT_COLUMNS + ["firstseen"])


months = sorted(glob.glob(PARQUET_FILES))
if not months:
    print(f"No Parquet files found at {PARQUET_FILES}. Run the Spark job first.")
    raise SystemExit(1)
print(f"Found {len(months)} month(s) to stream.")

producer = Producer({
    "bootstrap.servers": KAFKA_SERVER,
    "compression.type": "lz4",             # compress messages, so Kafka writes far less to disk
    "queue.buffering.max.messages": 500,   # don't pile up more than 500 unsent messages in memory
})

delivered = 0
failed = 0


def on_delivery(err, msg):
    global delivered, failed
    if err is None:
        delivered += 1
    else:
        failed += 1


flights_sent = 0
messages_sent = 0
began = time.time()
last_print = began
try:
    for path in months:
        print(f"Replaying {os.path.basename(path)} at {SPEEDUP:g}x speed")
        start = time.time()
        first_takeoff = None

        for batch in read_batches(path):
            batch = batch.filter(pc.is_valid(batch.column("firstseen")))  # drop flights with no takeoff time
            if batch.num_rows == 0:
                continue
            takeoff = batch.column("firstseen")
            if first_takeoff is None:
                first_takeoff = takeoff[0]  # the month's first flight starts the clock

            # Arrow converts each whole column at once, which is much faster than going row by row
            seconds_in = pc.seconds_between(first_takeoff, takeoff).to_pylist()
            flights = {name: pc.fill_null(batch.column(name), "").to_pylist() for name in TEXT_COLUMNS}
            flights["firstseen"] = pc.strftime(takeoff, format="%Y-%m-%d %H:%M:%S").to_pylist()

            for i in range(0, batch.num_rows, FLIGHTS_PER_MESSAGE):
                if SPEEDUP > 0:
                    # wait until the (sped-up) takeoff time of the first flight in this message
                    wait = seconds_in[i] / SPEEDUP - (time.time() - start)
                    if wait > 0:
                        time.sleep(wait)

                # one message = the next 1,000 flights, stored as one list per column
                chunk = {name: values[i:i + FLIGHTS_PER_MESSAGE] for name, values in flights.items()}
                message = json.dumps(chunk)

                while True:
                    try:
                        producer.produce(TOPIC, value=message, on_delivery=on_delivery)
                        break
                    except BufferError:  # the send queue is full, let Kafka catch up
                        producer.poll(0.5)
                producer.poll(0)
                flights_sent += len(chunk["firstseen"])
                messages_sent += 1

            if time.time() - last_print >= PRINT_SECONDS:
                print(f"sent {flights_sent:,} flights | flight time {flights['firstseen'][-1][:16]}")
                last_print = time.time()
except KeyboardInterrupt:
    print("Stopped early.")

producer.flush()
took = time.time() - began
print(f"Done. Sent {flights_sent:,} flights in {messages_sent:,} messages | Kafka confirmed {delivered:,} messages | failed {failed:,}")
print(f"Took {took:.1f} s ({flights_sent / took:,.0f} flights/s)")
