import heapq
import json
import time

from confluent_kafka import Consumer, KafkaError

TOPIC = "flights"
KAFKA_SERVER = "kafka:19092"
PRINT_SECONDS = 5
QUIET_SECONDS = 10
OUTPUT_FILE = "stream_counts.csv"


consumer = Consumer({
    "bootstrap.servers": KAFKA_SERVER,
    "group.id": "flight-tally",
    "auto.offset.reset": "earliest",
})
consumer.subscribe([TOPIC])

counts = {}  # "airport,hour" -> number of flights, e.g. "KJFK,2022-06-01 14:00" -> 37
received = 0
skipped = 0
saved = 0  # how many flights were counted the last time the tally was saved
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


def save_counts():
    global saved
    # Write in 1 MB pieces. Lots of small writes from Docker into a Windows folder are very slow.
    with open(OUTPUT_FILE, "w", buffering=1024 * 1024) as f:
        f.write("airport,hour,flights\n")
        for key, n in counts.items():
            f.write(f"{key},{n}\n")
    saved = received


print("Waiting for flights... (Ctrl+C to stop)")
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            quiet_seconds += 1
            if quiet_seconds == QUIET_SECONDS and received > 0:
                print(f"(stream is quiet: no new flights for {QUIET_SECONDS} s. "
                      "If the producer said Done, press Ctrl+C to stop.)")
                print_summary()
                print_timing()
                save_counts()
                print(f"Tally saved to {OUTPUT_FILE}")
            continue
        if msg.error():
            if msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                continue  # the producer hasn't started yet
            print("error:", msg.error())
            continue
        quiet_seconds = 0

        # one message = about 1,000 flights, stored as one list per column
        flights = json.loads(msg.value())
        if flights.get("type") == "replay_complete":

            # This is the producer in + flights out with an assert --> If this fails somethings wrong
            print(f"producer read {flights['rows_in']:,} | consumer received {received:,}")
            assert received == flights["rows_in"], f"missing {flights['rows_in'] - received:,} flights"
            
            save_counts()
            print(f"Final tally saved to {OUTPUT_FILE}", flush=True)
            break
        for airport, firstseen in zip(flights["origin"], flights["firstseen"]):
            if airport == "":
                skipped += 1
            else:
                key = airport + "," + firstseen[:13] + ":00"
                counts[key] = counts.get(key, 0) + 1
        received += len(flights["firstseen"])
        latest_time = flights["firstseen"][-1][:16]

        last_at = time.time()
        if first_at is None:
            first_at = last_at
        if last_at - last_print >= PRINT_SECONDS:
            print_summary()
            last_print = last_at

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
    print_summary()
    print_timing()
    if received > saved:
        save_counts()
    print(f"Stopped. Received {received:,} | skipped {skipped:,} | tally saved to {OUTPUT_FILE}")
