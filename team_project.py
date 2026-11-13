import glob
import os
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def get_spark_session():
    return SparkSession.builder \
        .appName("FlightDataPipelineSpark") \
        .config("spark.driver.memory", "3g") \
        .getOrCreate()

def convert_to_parquet(spark):
    os.makedirs("parquet_monthly", exist_ok=True)
    file_pattern = "flightlist_*.csv.gz"  
    file_list = sorted(glob.glob(file_pattern))

    if not file_list:
        file_pattern = "flight-data-platform/flightlist_*.csv.gz"
        file_list = sorted(glob.glob(file_pattern))

    print(f"Found {len(file_list)} monthly files to process.")
    
    for file_idx, file_path in enumerate(file_list, 1):
        base_name = os.path.basename(file_path).replace(".csv.gz", ".parquet").replace(".csv", ".parquet")
        output_parquet_path = os.path.join("parquet_monthly", base_name)
        
        if os.path.exists(output_parquet_path):
            print(f"[{file_idx}/{len(file_list)}] Parquet already exists for {os.path.basename(file_path)}, skipping.")
            continue
        
        print(f"[{file_idx}/{len(file_list)}] Converting {os.path.basename(file_path)} to Parquet...")
        
        # Read CSV with Spark and write out as Parquet
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
        df.write.parquet(output_parquet_path, mode="overwrite")

def run_benchmarks(spark):
    print("\n==================================================")
    print("  CSV Vs. Parquet Benchmarks")
    print("==================================================")
    
    parquet_path = "parquet_monthly/*.parquet"
    csv_path = "flightlist_*.csv.gz"

    # --------------------------------------------------
    # 1. PARQUET BENCHMARK FOR QUERY 1
    # --------------------------------------------------
    print("\n--- Benchmarking Spark Parquet for Query 1 ---")
    start_parquet = time.time()
    
    df_parquet = spark.read.parquet(parquet_path)
    
    top_parquet_origins = df_parquet.groupBy("origin").count().orderBy(F.desc("count")).limit(10).collect()
    top_parquet_dests = df_parquet.groupBy("destination").count().orderBy(F.desc("count")).limit(10).collect()

    parquet_time = time.time() - start_parquet
    print(f"Spark Parquet Total Time: {parquet_time:.4f} seconds")
    print("Ten busiest origin airports:")
    for row in top_parquet_origins:
        print(f"  {row['origin']}: {row['count']}")
    print("Ten busiest destination airports:")
    for row in top_parquet_dests:
        print(f"  {row['destination']}: {row['count']}")

    # --------------------------------------------------
    # 2. CSV.GZ BENCHMARK FOR QUERY 1
    # --------------------------------------------------
    print("\n--- Benchmarking Spark CSV.GZ for Query 1 ---")
    start_csv = time.time()
    
    df_csv = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)
    
    top_csv_origins = df_csv.groupBy("origin").count().orderBy(F.desc("count")).limit(10).collect()
    top_csv_dests = df_csv.groupBy("destination").count().orderBy(F.desc("count")).limit(10).collect()

    csv_time = time.time() - start_csv
    print(f"CSV.GZ Total Time: {csv_time:.4f} seconds")

    # --------------------------------------------------
    # 3. PARQUET BENCHMARK FOR QUERY 2
    # --------------------------------------------------
    print("\n--- Benchmarking Spark Parquet for Query 2 ---")
    start_parquet_q2 = time.time()
    
    df_parquet_q2 = df_parquet.withColumn("year", F.substring(F.col("day"), 1, 4))
    summer_counts_parquet = df_parquet_q2.groupBy("year").count().orderBy("year").collect()

    parquet_time_q2 = time.time() - start_parquet_q2
    print(f"Total processing time: {parquet_time_q2:.4f} seconds\n")
    print("Total flights in summer for each year (Parquet):")
    for row in summer_counts_parquet:
        print(f"  Year {row['year']}: {row['count']:,}")

    # --------------------------------------------------
    # 4. CSV.GZ BENCHMARK FOR QUERY 2
    # --------------------------------------------------
    print("\n--- Benchmarking Spark CSV.GZ for Query 2 ---")
    start_csv_q2 = time.time()
    
    df_csv_q2 = df_csv.withColumn("year", F.substring(F.col("day"), 1, 4))
    summer_counts_csv = df_csv_q2.groupBy("year").count().orderBy("year").collect()

    csv_time_q2 = time.time() - start_csv_q2
    print(f"Total processing time: {csv_time_q2:.4f} seconds\n")
    print("Total flights in summer for each year (CSV):")
    for row in summer_counts_csv:
        print(f"  Year {row['year']}: {row['count']:,}")

    # --------------------------------------------------
    # 5. STORAGE & RUNTIME REPORT
    # --------------------------------------------------
    parquet_files = glob.glob("parquet_monthly/*.parquet")
    csv_files = glob.glob("flightlist_*.csv.gz")
    parquet_size_mb = sum(os.path.getsize(f) for f in parquet_files) / (1024 * 1024) if parquet_files else 0
    csv_size_mb = sum(os.path.getsize(f) for f in csv_files) / (1024 * 1024)

    print("\n==================================================")
    print("  FINAL PERFORMANCE & STORAGE RESULTS")
    print("==================================================")
    print(f"{'Metric':<33} | {'Parquet Multi-File':<18} | {'Raw CSV.GZ Files':<18}")
    print("-" * 72)
    print(f"{'Total Size on Disk':<33} | {parquet_size_mb:>15.2f} MB | {csv_size_mb:>15.2f} MB")
    print(f"{'Total Processing Time for Query 1':<33} | {parquet_time:>15.4f} s  | {csv_time:>15.4f} s")
    print(f"{'Total Processing Time for Query 2':<33} | {parquet_time_q2:>15.4f} s  | {csv_time_q2:>15.4f} s")
    print("==================================================")

if __name__ == "__main__":
    spark = get_spark_session()
    try:
        convert_to_parquet(spark)
        run_benchmarks(spark)
    finally:
        spark.stop()