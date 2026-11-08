import glob
import os
import time
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from collections import defaultdict

#create output directory
os.makedirs("parquet_monthly", exist_ok=True)

#explicitly define data types so Pandas never misinterprets columns between files
explicit_dtypes = {
    'callsign': 'str',
    'number': 'str',        
    'icao24': 'str',
    'registration': 'str',
    'typecode': 'str',
    'origin': 'str',
    'destination': 'str',
    'firstseen': 'str',
    'lastseen': 'str',
    'day': 'str',
    'latitude_1': 'float64',
    'longitude_1': 'float64',
    'altitude_1': 'float64',
    'latitude_2': 'float64',
    'longitude_2': 'float64',
    'altitude_2': 'float64'
}

def convert_to_parquet():
    #get list of csv files to process
    file_pattern = "flightlist_*.csv.gz"  
    file_list = sorted(glob.glob(file_pattern))
    print(f"Found {len(file_list)} monthly files to process.")

    for file_idx, file_path in enumerate(file_list, 1):
        print(f"[{file_idx}/{len(file_list)}] Converting {os.path.basename(file_path)} to individual Parquet...")
        
        base_name = os.path.basename(file_path).replace(".csv.gz", ".parquet").replace(".csv", ".parquet")
        output_parquet_path = os.path.join("parquet_monthly", base_name)
        
        #remove existing file if re-running
        if os.path.exists(output_parquet_path):
            os.remove(output_parquet_path)

        writer = None
        chunk_size = 50000
        
        #pass dtype=explicit_dtypes to guarantee identical schemas across all files
        for chunk in pd.read_csv(file_path, chunksize=chunk_size, low_memory=False, dtype=explicit_dtypes):
            table = pa.Table.from_pandas(chunk)
            
            if writer is None:
                writer = pq.ParquetWriter(output_parquet_path, table.schema, compression="zstd", compression_level=3)
            
            writer.write_table(table, row_group_size=chunk_size)
            
        if writer:
            writer.close()

def run_benchmarks():
    print("\n==================================================")
    print("  CSV Vs. Parquet Benchmarks")
    print("==================================================")
    
    parquet_files = sorted(glob.glob("parquet_monthly/*.parquet"))
    csv_files = sorted(glob.glob("flightlist_*.csv.gz"))

    # --------------------------------------------------
    # 1. PARQUET BENCHMARK FOR QUERY 1
    # --------------------------------------------------
    print("\n--- Benchmarking Parquet for Query 1")
    start_parquet = time.time()
    
    parquet_origin_counts = pd.Series(dtype='int64')
    parquet_dest_counts = pd.Series(dtype='int64')

    #reading one file at a time, otherwise crashing an 8GB laptop crashes 
    for pf in parquet_files:
        df_chunk = pd.read_parquet(pf, columns=['origin', 'destination'])
        
        #accumulate counts
        parquet_origin_counts = parquet_origin_counts.add(df_chunk['origin'].value_counts(), fill_value=0)
        parquet_dest_counts = parquet_dest_counts.add(df_chunk['destination'].value_counts(), fill_value=0)

    top_parquet_origins = parquet_origin_counts.nlargest(10)
    top_parquet_dests = parquet_dest_counts.nlargest(10)

    parquet_time = time.time() - start_parquet
    
    print(f"Parquet Total Time (all 16 files iterated): {parquet_time:.4f} seconds")
    print(f"Ten busiest origin airports:\n {top_parquet_origins}")
    print(f"Ten busiest destination airpoirts:\n {top_parquet_dests}")

    # --------------------------------------------------
    # 2. RAW CSV.GZ BENCHMARK FOR QUERY 1
    # --------------------------------------------------
    print("\n--- Benchmarking CSV.GZ for Query 1 ---")
    start_csv = time.time()
    
    csv_origin_counts = pd.Series(dtype='int64')
    csv_dest_counts = pd.Series(dtype='int64')

    for cf in csv_files:
        for chunk in pd.read_csv(cf, usecols=['origin', 'destination'], chunksize=50000, low_memory=False, dtype={'origin': 'str', 'destination': 'str'}):
            csv_origin_counts = csv_origin_counts.add(chunk['origin'].value_counts(), fill_value=0)
            csv_dest_counts = csv_dest_counts.add(chunk['destination'].value_counts(), fill_value=0)

    #get the 10 origin and destination airports with most traffic
    top_csv_origins = csv_origin_counts.nlargest(10)
    top_csv_dests = csv_dest_counts.nlargest(10)

    #get time it takse to copmlete query
    csv_time = time.time() - start_csv
    print(f"Ten busiest origin airports:\n {top_csv_origins}")
    print(f"Ten busiest destination airpoirts:\n {top_csv_dests}")
    print(f"CSV.GZ Total Time: {csv_time:.4f} seconds")

    # --------------------------------------------------
    # 3. Parquet BENCHMARK FOR QUERY 2
    # --------------------------------------------------
    print("\n--- Benchmarking Parquet for Query 2 ---")
    start_parquet_q2 = time.time()
    #dictionary to hold counts per year
    summer_counts_by_year = {}
    parquet_files = sorted(glob.glob("parquet_monthly/*.parquet"))

    for pf in parquet_files:
        #find which year the dataset belongs to using filename
        filename = os.path.basename(pf)
        file_year = None
        for year in range(2019, 2023): 
            if str(year) in filename:
                file_year = year
                break
    
        df_chunk = pd.read_parquet(pf, columns=['day'])
        row_count = len(df_chunk)
        
        #accumulate counts for that year
        summer_counts_by_year[file_year] = summer_counts_by_year.get(file_year, 0) + row_count

    #measure time it takes to copmlete query
    parquet_time_q2 = time.time() - start_parquet_q2

    print(f"Total processing time: {parquet_time_q2:.4f} seconds\n")
    print("Total flights in summer for each year:")
    for year in sorted(summer_counts_by_year.keys()):
        print(f"  Summer {year}: {summer_counts_by_year[year]:,}")

    # --------------------------------------------------
    # 4. RAW CSV.GZ BENCHMARK FOR QUERY 2
    # --------------------------------------------------
    print("\n--- Benchmarking CSV.GZ for Query 2 ---")
    start_csv_q2 = time.time()
    # Dictionary to hold counts per year
    summer_counts_by_year_csv = {}
    
    for cf in csv_files:
        #check the year of the dataset using filename
        filename = os.path.basename(cf)
        file_year = None
        for year in range(2019, 2023): 
            if str(year) in filename:
                file_year = year
                break

        for chunk in pd.read_csv(cf, usecols=['day'], chunksize=50000, low_memory=False, dtype={'day': 'str'}):
            row_count = len(chunk)
            #accumulate counts for that year
            summer_counts_by_year_csv[file_year] = summer_counts_by_year_csv.get(file_year, 0) + row_count

    #get time it takes to complete query
    csv_time_q2 = time.time() - start_csv_q2

    print(f"Total processing time: {parquet_time:.4f} seconds\n")
    print("Total flights in summer for each year:")
    for year in sorted(summer_counts_by_year.keys()):
        print(f"  Summer {year}: {summer_counts_by_year[year]:,}")

    # --------------------------------------------------
    # 5. STORAGE & RUNTIME REPORT
    # --------------------------------------------------
    parquet_size_mb = sum(os.path.getsize(f) for f in parquet_files) / (1024 * 1024)
    csv_size_mb = sum(os.path.getsize(f) for f in csv_files) / (1024 * 1024)

    print("\n==================================================")
    print("  FINAL PERFORMANCE & STORAGE COMPARISON RESULTS")
    print("==================================================")
    print(f"{'Metric':<33} | {'Parquet Multi-File':<18} | {'Raw CSV.GZ Files':<18}")
    print("-" * 72)
    print(f"{'Total Size on Disk':<33} | {parquet_size_mb:>15.2f} MB | {csv_size_mb:>15.2f} MB")
    print(f"{'Total Processing Time for Query 1':<33} | {parquet_time:>15.4f} s  | {csv_time:>15.4f} s")
    print(f"{'Total Processing Time for Query 2':<33} | {parquet_time_q2:>15.4f} s  | {csv_time_q2:>15.4f} s")
    print("==================================================")

if __name__ == "__main__":
    #convert_to_parquet()
    run_benchmarks()