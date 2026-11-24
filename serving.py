import os
import subprocess
import sys
from statistics import mean


def export_batch_counts():
    import psycopg
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.appName("ExportBatchCounts").getOrCreate()

    try:
        flights = spark.read.parquet("/app/parquet_monthly/*.parquet")

        airport_rows = []
        for role in ("origin", "destination"):
            for airport, count in flights.groupBy(role).count().collect():
                if airport is None:
                    airport = "None"
                airport_rows.append((role, airport, count))

        year_counts = (
            flights.select(F.substring("day", 1, 4).cast("int").alias("flight_year"))
            .groupBy("flight_year")
            .count()
            .orderBy("flight_year")
            .collect()
        )

        # Publish both summaries atomically after Spark finishes aggregating.
        with psycopg.connect(connect_timeout=15) as db, db.cursor() as cursor:
            cursor.execute("TRUNCATE batch_airport_counts, batch_year_counts")
            with cursor.copy(
                "COPY batch_airport_counts (airport_role, airport_code, flight_count) FROM STDIN"
            ) as copy:
                for row in airport_rows:
                    copy.write_row(row)
            with cursor.copy(
                "COPY batch_year_counts (flight_year, flight_count) FROM STDIN"
            ) as copy:
                for row in year_counts:
                    copy.write_row(tuple(row))
        print(f"Published {len(airport_rows):,} airport totals and {len(year_counts)} yearly totals to PostgreSQL.", flush=True)
    finally:
        spark.stop()


def run_sql(filename, capture_output=False):
    return subprocess.run(
        [
            "psql", "-X", "-h", "postgres",
            "-U", "flight_user", "-d", "flight_data_platform",
            "-v", "ON_ERROR_STOP=1", "-f", f"/app/database/{filename}",
        ],
        check=True,
        text=True,
        capture_output=capture_output,
    )


def benchmark():
    os.environ["LC_ALL"] = "C"  # Keep psql timing output in English with decimal points.

    runs = []
    for run in range(1, 6):
        print(f"\n=== Run {run} of 5 ===", flush=True)
        output = run_sql("run_queries.sql", capture_output=True).stdout
        print(output, end="", flush=True)

        timings = [
            float(line.split()[1])
            for line in output.splitlines()
            if line.startswith("Time:")
        ]
        if len(timings) != 4:
            raise SystemExit(f"Run {run}: expected four timings, got {len(timings)}.")
        runs.append(timings)

    print("\n=== Average over 5 runs ===")
    for query, timings in zip(("Ten busiest origin airports", "Ten busiest destination airports", "Total flights in summer for each year", "Top five airport-hours in UTC"), zip(*runs)):
        print(f"{query}: {mean(timings):.3f} ms")


if __name__ == "__main__":
    if sys.argv[1:] == ["export"]:
        export_batch_counts()
    elif sys.argv[1:] == ["benchmark"]:
        benchmark()
    else:
        raise SystemExit("Usage: serving.py export|benchmark")