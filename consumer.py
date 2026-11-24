import heapq
import json
import time
import psycopg
from confluent_kafka import Consumer, KafkaError

TOPIC = "flights-replay-v2"
KAFKA_SERVER = "kafka:19092"
PRINT_SECONDS = 5
QUIET_SECONDS = 10



consumer = Consumer({
    "bootstrap.servers": KAFKA_SERVER,
    "group.id": "flight-tally",
    "enable.auto.commit": False,
    "auto.offset.reset": "earliest",
})
consumer.subscribe([TOPIC])

db = psycopg.connect(autocommit=True)
changed = set()
replay_id = None
last_offset = None
first_save = True
counts = {}  # "airport,hour" -> number of flights, e.g. "KJFK,2022-06-01 14:00" -> 37
received = 0
skipped = 0
latest_time = ""
quiet_seconds = 0
first_at = None
last_at = None
last_print = time.time()


def print_summary():
    top = heapq.nlargest(5, counts.items(), key=lambda item: item[1])
    print(f"--- {received:,} received | {skipped:,} skipped (no origin) | flight time {latest_time} | top 5 airport-hours ---")
    for key, n in top:
        airport, hour = key.split(",")
        print(f"    {airport}  {hour}  {n} flights")


def print_timing():
    if first_at is not None and last_at > first_at:
        took = last_at - first_at
        print(f"First to last flight received: {took:.1f} s ({received / took:,.0f} flights/s)")


def save_counts(complete=False):
    global first_save
    # The dictionary holds cumulative totals. Replace only the keys that changed.
    with db.transaction():
        if first_save:
            db.execute("TRUNCATE stream_airport_hour_counts")
        with db.cursor() as cursor:
            cursor.executemany(
                """INSERT INTO stream_airport_hour_counts (airport_code, hour_start, flight_count)
                   VALUES (%s, %s, %s)
                   ON CONFLICT (airport_code, hour_start) DO UPDATE
                   SET flight_count = EXCLUDED.flight_count""",
                ((*key.split(",", 1), counts[key]) for key in changed),
            )
        db.execute(
            """INSERT INTO stream_replay_progress (singleton, replay_id, last_offset, complete)
               VALUES (true, %s, %s, %s)
               ON CONFLICT (singleton) DO UPDATE SET replay_id = EXCLUDED.replay_id,
                   last_offset = EXCLUDED.last_offset, complete = EXCLUDED.complete""",
            (replay_id, last_offset, complete),
        )
    changed.clear()
    first_save = False


print("Waiting for flights... (Ctrl+C to stop)")
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            quiet_seconds += 1
            if quiet_seconds == QUIET_SECONDS and received > 0:
                print(f"(stream is quiet: no new flights for {QUIET_SECONDS} s. "
                      "Waiting for the replay completion marker.)")
                print_summary()
                print_timing()
                save_counts()
                print("Tally saved to PostgreSQL")
            continue
        if msg.error():
            if msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                continue  # the producer hasn't started yet
            raise RuntimeError(msg.error())
        quiet_seconds = 0

        # one message = about 1,000 flights, stored as one list per column
        flights = json.loads(msg.value())
        if replay_id is not None and replay_id != flights["replay_id"]:
            raise ValueError("Replay changed before its completion marker. Start a fresh replay.")
        replay_id = flights["replay_id"]
        last_offset = msg.offset()
        if flights.get("type") == "replay_complete":

            rows_in = flights["rows_in"]
            sent = flights["flights_sent"]
            print(f"producer read {rows_in:,} | sent {sent:,} | consumer received {received:,}")
            if not (rows_in == sent == received):
                raise ValueError("Producer and consumer row counts do not match")
            save_counts(complete=True)
            # A restart needs the entire replay to rebuild the in-memory totals.
            for partition in consumer.commit(message=msg, asynchronous=False):
                if partition.error is not None:
                    raise RuntimeError(partition.error)
            print("Final tally saved to PostgreSQL.", flush=True)
            break
        for airport, firstseen in zip(flights["origin"], flights["firstseen"]):
            if airport == "":
                skipped += 1
            else:
                key = airport + "," + firstseen[:13] + ":00"
                counts[key] = counts.get(key, 0) + 1
                changed.add(key)
        received += len(flights["firstseen"])
        latest_time = flights["firstseen"][-1][:16]

        last_at = time.time()
        if first_at is None:
            first_at = last_at
        if last_at - last_print >= PRINT_SECONDS:
            save_counts()
            print_summary()
            last_print = last_at

except KeyboardInterrupt:
    raise SystemExit("Stopped early.")
finally:
    consumer.close()
    db.close()
    print_summary()
    print_timing()
    print(f"Stopped. Received {received:,} | skipped {skipped:,}")
