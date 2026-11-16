# flight-data-platform

## Before you start (once)
1. Install Docker Desktop and open it. Wait until it says "Engine running".
2. Put the data files (`flightlist_*.csv.gz`) in this folder.

## Run it (from this folder)
1. **Run everything:**
   ```
   docker compose up --build spark producer consumer
   ```
   - **Part 1 (batch):** Spark converts the CSV files to Parquet (`parquet_monthly/`) and answers the questions.
   - **Part 2 (stream):** once Spark finishes, the producer replays the flights from those Parquet files into Kafka,
     one month after another, in takeoff (event time) order, sped up to 1 day of flights per second.
     The consumer keeps a running count of flights per origin airport per hour, prints the top 5 every 5 seconds,
     and saves the full count to `stream_counts.csv`.
   - Press Ctrl+C to stop.
2. **Stop and reset:**
   ```
   docker compose down
   ```
   Stops everything and wipes Kafka, so the next run starts fresh.

## Stream settings (optional)
In `docker-compose.yml`, under `producer` -> `environment`:
- `PARQUET_FILES`: which months to stream (default: all of them).
  For one month: `parquet_monthly/flightlist_20220601_20220630.parquet`
- `SPEEDUP`: seconds of flight time per real second (86400 = 1 day per second, 3600 = 1 hour per second)
