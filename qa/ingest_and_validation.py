import glob
import os
import shutil
from pyspark.storagelevel import StorageLevel
from pyspark.sql.functions import col, when, lit, input_file_name, regexp_extract
from pyspark.sql.types import (StructType, StructField, StringType, DoubleType, TimestampType)

# https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.schema.html
# We should explicitly define our schema, that way spark doesn't need to spend time infering it
#   every time we have it infer, it needs to do a full pass of the data before processing anything
SCHEMA = StructType([
    StructField("callsign", StringType(), True),
    StructField("number", StringType(), True),
    StructField("icao24", StringType(), True),
    StructField("registration", StringType(), True),
    StructField("typecode", StringType(), True),
    StructField("origin", StringType(), True),
    StructField("destination", StringType(), True),
    StructField("firstseen", TimestampType(), True),
    StructField("lastseen", TimestampType(), True),
    StructField("day", TimestampType(), True),
    StructField("latitude_1", DoubleType(), True),
    StructField("longitude_1", DoubleType(), True),
    StructField("altitude_1", DoubleType(), True),
    StructField("latitude_2", DoubleType(), True),
    StructField("longitude_2", DoubleType(), True),
    StructField("altitude_2", DoubleType(), True),
    StructField("_corrupt", StringType(), True),
])


ORIGINAL_COLS = [f.name for f in SCHEMA.fields if f.name != "_corrupt"]

# This is all you need to know if a flight exists
REQUIRED = ["icao24", "firstseen", "lastseen", "day"]



def bad_latitude(c):
    return (col(c) < -90) | (col(c) > 90)
 
 
def bad_longitude(c):
    return (col(c) < -180) | (col(c) > 180)
 
 
def bad_altitude(c): 
    return (col(c) < -500) | (col(c) > 20000)


def count_rows(spark, path):
    rows = spark.read.parquet(path).count()
    print(f"{path}: {rows:,} rows")
    return rows
 

# Need to rename files for everyone elses parts to work properly
def rename_month_folders(out_dir):
    for d in glob.glob(os.path.join(out_dir, "month=*")):
        target = os.path.join(out_dir, os.path.basename(d)[len("month="):] + ".parquet")
        shutil.rmtree(target, ignore_errors=True)
        os.rename(d, target)
        open(os.path.join(target, "_SUCCESS"), "w").close()


# https://community.databricks.com/t5/community-articles/handling-the-chaos-data-quality-strategies-with-pyspark/td-p/139249
# --> Inspo
def ingest(spark, src, clean, reject, max_reject=5.0):


    df = (spark.read
          .schema(SCHEMA)
          .option("header", True)
          .option("mode", "PERMISSIVE")
          .option("columnNameOfCorruptRecord", "_corrupt")
          .csv(src))
 
    missing = None
    for c in REQUIRED:
        missing = col(c).isNull() if missing is None else missing | col(c).isNull()
 
    # Null coords/altitudes pass through (null comparisons never match a rule)
    reason = (when(col("_corrupt").isNotNull(), "unparseable")
              .when(missing, "missing_required")
              # Check 1: For bad icao24
              .when(~col("icao24").rlike("^[0-9a-fA-F]{6}$"), "bad_icao24")

              # Check 2: Make sure plane doesn't appear out of no where
              .when(col("lastseen") < col("firstseen"), "lastseen_before_firstseen")

              # Check 3: Possible latitudes
              .when(bad_latitude("latitude_1") | bad_latitude("latitude_2"), "bad_latitude")

              # Check 4: Possible longitudes
              .when(bad_longitude("longitude_1") | bad_longitude("longitude_2"), "bad_longitude")

              # Check 5: Possible altitudes
              .when(bad_altitude("altitude_1") | bad_altitude("altitude_2"), "bad_altitude")
              .otherwise(lit(None)))

 
    # Persist / spill CSV to disk after parse just in case our 8gb laptop runs out of RAM
    tagged = (df.withColumn("reject_reason", reason)
                # Grab the input file name
                .withColumn("month", regexp_extract(input_file_name(), r"([^/]+?)\.csv(\.gz)?$", 1))
                .persist(StorageLevel.DISK_ONLY))


    rows_in = tagged.count()
    print(f"{src}: {rows_in:,} rows in")

    
 
    # Good rows --> If theres no reject reason / not null per row it gets written to parquet
    #   as clean
    (tagged.filter(col("reject_reason").isNull())
           .select(*ORIGINAL_COLS, "month")
           .write.mode("overwrite").partitionBy("month").parquet(clean))
 
    # Bad rows --> Gets put into a separate path it includes a reject reason
    (tagged.filter(col("reject_reason").isNotNull())
           .write.mode("overwrite").parquet(reject))


    # We are done parsing now so we can unpersist disk level data that spills over
    tagged.unpersist()

    # Rename the files
    rename_month_folders(clean)
    rename_month_folders(reject)

    # Do the counts; assert more rows aren't going out than what came in
    rows_ok = count_rows(spark, f"{clean}/*.parquet")
    rows_bad = count_rows(spark, f"{reject}/*.parquet")

    assert rows_in == rows_ok + rows_bad, f"lost {rows_in - rows_ok - rows_bad:,} rows"

    # Puts up a warning if we reject more than 5% of rows
    reject_pct = rows_bad / rows_in * 100 if rows_in else 0
    if reject_pct > max_reject:
        print(f"⚠️  ALERT: {reject_pct:.2f}% of rows rejected "
              f"({rows_bad:,}/{rows_in:,}), threshold is {max_reject}%")


    spark.read.parquet(f"{reject}/*.parquet").groupBy("reject_reason").count().show(truncate=False)

 
    spark.read.parquet(reject).groupBy("reject_reason").count().show(truncate=False)
    return rows_in, rows_ok, rows_bad
 
