import glob
import json
import os
import time
from uuid import uuid4

import psycopg
import pyarrow.compute as pc
import pyarrow.parquet as pq
from confluent_kafka import Producer

PARQUET_FILES = os.environ.get("PARQUET_FILES", "parquet_monthly/*.parquet")
SPEEDUP = float(os.environ.get("SPEEDUP", "432000"))  # seconds of flight time per real second. 0 = no pacing
BATCH_ROWS = 50000          # flights read from Parquet into memory at a time
FLIGHTS_PER_MESSAGE = 1000  # flights packed into one Kafka message
TOPIC = "flights-replay-v2"
KAFKA_SERVER = "kafka:19092"
PRINT_SECONDS = 5

TEXT_COLUMNS = ["callsign", "icao24", "origin", "destination"]


# Validation counts every input row, including rows later skipped for missing firstseen.
rows_in = 0


def read_batches(month_path):
    global rows_in
    # Spark saves each month as a folder of part files. Read them 50,000 flights at a time.
    parts = [month_path] if os.path.isfile(month_path) else sorted(glob.glob(os.path.join(month_path, "*.parquet")))
    if not parts:
        raise ValueError(f"No Parquet part files found in {month_path}")
    for part in parts:
        for batch in pq.ParquetFile(part).iter_batches(
            batch_size=BATCH_ROWS, columns=TEXT_COLUMNS + ["firstseen"]
        ):
            rows_in += batch.num_rows
            yield batch


def display_counts(db, replay_id):
    # One statement gives the replay status and ranking the same database snapshot.
    rows = db.execute(
        """SELECT progress.last_offset, progress.complete,
                  ranked.airport_code, ranked.hour_start, ranked.flight_count
           FROM stream_replay_progress AS progress
           LEFT JOIN stream_airport_hour_counts AS ranked ON true
           WHERE progress.replay_id = %s
           ORDER BY ranked.flight_count DESC, ranked.airport_code, ranked.hour_start LIMIT 5""",
        (replay_id,),
    ).fetchall()
    if not rows:
        print("Waiting for the consumer's first database commit")
        return None, False
    offset, complete = rows[0][:2]
    label = "Final" if complete else "Live"
    print(f"{label} PostgreSQL top 5 airport-hours (UTC) | committed Kafka offset {offset}")
    for _, _, airport, hour, count in rows:
        if airport is not None:
            print(f"    {airport}  {hour:%Y-%m-%d %H:%M}  {count:,} flights")
    return offset, complete


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

replay_id = str(uuid4())
delivered = 0
failed = 0


def on_delivery(err, msg):
    global delivered, failed
    if err is None:
        delivered += 1
    else:
        failed += 1


db = psycopg.connect(autocommit=True, connect_timeout=10)
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
                chunk["replay_id"] = replay_id
                message = json.dumps(chunk)

                while True:
                    try:
                        producer.produce(TOPIC, value=message, partition=0, on_delivery=on_delivery)
                        break
                    except BufferError:  # the send queue is full, let Kafka catch up
                        producer.poll(0.5)
                producer.poll(0)
                flights_sent += len(chunk["firstseen"])
                messages_sent += 1

            if time.time() - last_print >= PRINT_SECONDS:
                print(f"sent {flights_sent:,} flights | flight time {flights['firstseen'][-1][:16]}")
                display_counts(db, replay_id)
                last_print = time.time()
except KeyboardInterrupt:
    raise SystemExit("Stopped early.")
    
producer.flush()
if failed or delivered != messages_sent:
    raise SystemExit(f"Replay incomplete: Kafka confirmed {delivered:,} of {messages_sent:,} data messages")

producer.produce(
    TOPIC,
    # The consumer can compare these row totals with its received count.
    value=json.dumps({
        "type": "replay_complete",
        "replay_id": replay_id,
        "rows_in": rows_in,
        "flights_sent": flights_sent,
    }),
    partition=0,
    on_delivery=on_delivery,
)
producer.flush()
if failed or delivered != messages_sent + 1:
    raise SystemExit("Kafka did not confirm the completion marker")
print("Replay completion marker sent.", flush=True)
took = time.time() - began
print(f"Done. Sent {flights_sent:,} flights in {messages_sent:,} messages | Kafka confirmed {delivered - 1:,} messages | failed {failed:,}")
print(f"Took {took:.1f} s ({flights_sent / took:,.0f} flights/s)")

# Kafka delivery can finish before the consumer commits the last counts.
last_offset = None
last_progress = time.monotonic()
while True:
    offset, complete = display_counts(db, replay_id)
    if complete:
        break
    if offset != last_offset:
        last_offset = offset
        last_progress = time.monotonic()
    if time.monotonic() - last_progress >= 300:
        raise SystemExit("No database progress for 5 minutes. Check the consumer logs.")
    time.sleep(PRINT_SECONDS)
db.close()
