# flight-data-platform

## Before you start (once)
1. Install Docker Desktop and open it. Wait until it says "Engine running".
2. The data files (`flightlist_*.csv.gz`, one per month, 16 months) are already in the `data/` folder. Leave them zipped.

## Run it (from this folder)
1. **Run everything:**
   ```
   docker compose up --build
   ```
   - **Part 1 (batch):** Spark converts the CSV files to Parquet (`parquet_monthly/`), 4 months at a time,
     and answers the questions on both formats to compare their speed.
     Months that are already converted are skipped, so only the first run does the conversion.
     This part is slow and Spark can print nothing for several minutes at a time. That is normal, it has not frozen.
     The questions alone take about 10 minutes, and the first run also has to convert all 16 months before that.
   - **Part 2 (stream):** once Spark finishes, the producer replays the flights from those Parquet files into Kafka,
     one month after another, in takeoff (event time) order, sped up to about 5 days of flights per second.
     It reads the Parquet 50,000 flights at a time and packs 1,000 flights into each Kafka message.
     The consumer keeps a running count of flights per origin airport per hour, prints the top 5 every 5 seconds.
     After the Kafka replay, producer sends completion marker and consumer saves the full count to `stream_counts.csv`.
   - **Part 3 (serve):** The PostgreSQL benchmark (serving) is triggered after Kafka replay finishes. The benchmark loads the batch airport counts,
     yearly flight totals, and streaming airport hour counts in PostgreSQL. The query is ran over five iterations with each result displayed and
     reports its average execution time in milliseconds.
   - When the benchmark is done running and displays `exited with code 0`, press Ctrl+C to stop.
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

## Run benchmark separately (after batching and streaming are completed)
1. **Before you start:** Open Docker Desktop and run the commands from this folder. Make sure a completed pipeline run has generated these files:
   - `batch_exports/batch_airport_counts.csv`
   - `batch_exports/batch_year_counts.csv`
   - `stream_counts.csv`
2. **Start PostgreSQL and run the benchmark:**
   ```bash
   docker compose up -d --wait postgres
   docker compose run --build --rm --no-deps benchmark
   ```
The benchmark loads the existing CSVs into PostgreSQL without restarting Spark or Kafka. It displays the results for all four queries on each of five runs and reports the average time for each query in milliseconds.

## Other folders
These are not part of the run:
- `docs/`: notes on the data's schema.
- `qa/`: data quality helpers (row counts and validation ideas).
