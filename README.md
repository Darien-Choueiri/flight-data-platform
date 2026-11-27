# flight-data-platform

## Before you start (once)
1. Install Docker Desktop and open it. Wait until it says "Engine running".
2. The data files (`flightlist_*.csv.gz`, one per month, 16 months) are already in the `data/` folder. Leave them zipped.

## Run it (from this folder)
1. **Run everything:**
   ```
   docker compose up --build
   ```
   - **Part 1 (batch):** Spark reads the CSV files and checks every row (see [Bad rows](#bad-rows)).
     Good rows are saved as Parquet in `parquet_monthly/`, one folder per month. Bad rows are saved in `parquet_rejects/`.
     Then it answers the questions on both formats to compare their speed.
     The conversion is redone from scratch on every run.
     This part is slow and Spark can print nothing for several minutes at a time. That is normal, it has not frozen.
     The questions alone take about 10 minutes, and converting all 16 months comes before that.
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

## Bad rows
While converting, Spark checks every row. A row is rejected if:

| Reason | What's wrong |
|---|---|
| `unparseable` | The row can't be read, e.g. text where a number should be |
| `missing_required` | `icao24`, `firstseen`, `lastseen` or `day` is empty |
| `bad_icao24` | `icao24` isn't 6 hex characters |
| `lastseen_before_firstseen` | The plane was last seen before it was first seen |
| `bad_latitude` | A latitude is outside -90 to 90 |
| `bad_longitude` | A longitude is outside -180 to 180 |
| `bad_altitude` | An altitude is outside -500 to 20,000 m |

The rules are checked in this order. A row that breaks more than one only gets the first reason.
Empty coordinates and altitudes are allowed.

Rejected rows go to `parquet_rejects/` with all their original columns, plus:
- `reject_reason`: the rule from the table
- `month`: the file the row came from
- `_corrupt`: the original line (only filled in for `unparseable` rows)

At the end of Part 1, Spark prints how many rows came in, were kept and were rejected, and a count for each reason.
If kept + rejected doesn't equal the rows that came in, the run stops with an error.
If more than 5% of rows are rejected, it prints `⚠️ ALERT` but keeps going.

## Test the bad-row checks (demo only)
> **Note:** The `bad_rows/` folder is not included in this submission. Its test file was too large to submit,
> so we deleted it. We did use it in our Project Video, and the steps below show how we ran it.
> The bad-row checks themselves are still part of Part 1 and run on every file in `data/`.

`bad_rows/flightlist_badtest.csv.gz` is a made-up file of 1,000,000 flights. Every row is broken on purpose,
about 142,857 for each reason above, so none of them should be kept.
1. Copy it into `data/` (Spark only reads files from there):
   ```
   cp bad_rows/flightlist_badtest.csv.gz data/
   ```
2. Run only Part 1:
   ```
   docker compose up --build spark
   ```
   When it shows `exited with code 0`, press Ctrl+C, then run `docker compose down`.
3. In the output, every reason should have at least about 142,857 rows.
   In `parquet_rejects/`, these rows have `month` = `flightlist_badtest`.
4. Delete `data/flightlist_badtest.csv.gz` when you're done. The CSV side of the benchmark reads every file in `data/`, so while it's there the CSV numbers won't match the Parquet ones.

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
- `docs/`: notes on the data's schema.
- `qa/`: the row checks used in Part 1 (`ingest_and_validation.py`) and a row-count helper (`counts.py`, not part of the run).
- `bad_rows/`: not included in this submission because it was too large. It held the test file for the row checks that we used in the Project Video (see above). Not part of a normal run.
