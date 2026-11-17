# flight-data-platform

## Before you start (once)
1. Install Docker Desktop and open it. Wait until it says "Engine running".
2. Put the data files (`flightlist_*.csv.gz`, one per month) in the `data/` folder. Leave them zipped.

## Run it (from this folder)
1. **Run everything:**
   ```
   docker compose up --build
   ```
   - **Part 1 (batch):** Spark converts the CSV files to Parquet (`parquet_monthly/`), 4 months at a time,
     and answers the questions on both formats to compare their speed.
     Months that are already converted are skipped, so only the first run does the conversion.
   - **Part 2 (stream):** once Spark finishes, the producer replays the flights from those Parquet files into Kafka,
     one month after another, in takeoff (event time) order, sped up to about 5 days of flights per second.
     It reads the Parquet 50,000 flights at a time and packs 1,000 flights into each Kafka message.
     The consumer keeps a running count of flights per origin airport per hour, prints the top 5 every 5 seconds,
     and saves the full count to `stream_counts.csv`.
   - When the producer prints `Done` and the consumer says the stream is quiet, press Ctrl+C to stop.
2. **Stop and reset:**
   ```
   docker compose down
   ```
   Stops everything and wipes Kafka, so the next run starts fresh.

## Stream settings (optional)
In `docker-compose.yml`, under `producer` -> `environment`:
- `PARQUET_FILES`: which months to stream (default: all of them).
  For one month: `parquet_monthly/flightlist_20220601_20220630.parquet`
- `SPEEDUP`: seconds of flight time per real second (432000 = 5 days per second, 86400 = 1 day per second).
  `0` = no pacing, send as fast as possible.

Both the producer and consumer print how long the stream took, in flights per second.
