# flight-data-platform

## Before you start (once)
1. Install Docker Desktop and open it. Wait until it says "Engine running".
2. Put the data files (`flightlist_*.csv.gz`) in this folder.
   The stream replays `flightlist_20220601_20220630.csv.gz` (June 2022), so that one must be here.

## Run it (from this folder)
1. **Stream (Part 2):**
   ```
   docker compose up --build producer consumer
   ```
   Starts Kafka, then the producer replays June 2022 flights into Kafka in takeoff (event time) order,
   sped up to 1 hour of flights per second. The consumer keeps a running count of flights per origin
   airport per hour, prints the top 5 every 50,000 flights, and saves the full count to `stream_counts.csv`.
   Press Ctrl+C to stop.
2. **Batch (Part 1), in a second terminal:**
   ```
   docker compose run --rm spark
   ```
   Converts the CSV files to Parquet and runs the Spark queries.
3. **Stop and reset:**
   ```
   docker compose down
   ```
   Stops everything and wipes Kafka, so the next run starts fresh.

## Stream settings (optional)
In `docker-compose.yml`, under `producer` -> `environment`:
- `DATA_FILE`: which month to replay
- `SPEEDUP`: seconds of flight time per real second (3600 = 1 hour per second)
- `MAX_FLIGHTS`: stop after this many flights (0 = all of them)

## Old Spark-only commands (linux)
1. docker build -t flight-pipeline .
2. docker run --rm -m 4g -v "$(pwd):/app" flight-pipeline /opt/spark/bin/spark-submit team_project.py
